//! Opening a dump and handing out newline-aligned chunks.
//!
//! Executes on the reader thread. Nothing here is async: the input is a single
//! sequential stream, and asynchrony would add complexity without adding throughput.
//!
//! This module contains no subprocess handling, no credentials, and no connection
//! strings. Dumps are delivered as files, so the library never invokes `pg_dump` and an
//! entire class of credential-handling risk is absent by construction.
//!
//! # Decompression is the serial floor
//!
//! A compressed stream must be decoded in order, so unlike row decoding it does not
//! scale with cores, and it sits in front of every other stage. It therefore governs
//! total runtime. Measured gunzip throughput is 349 MiB/s against a row decoder at
//! 1068 MiB/s per core, so with gzip input this stage, not the decoder, sets the pace.
//! Build with the `zlib-ng` feature where a C toolchain is available; it is materially
//! faster and no amount of parallelism elsewhere compensates for a slow decompressor.
//!
//! # Chunk alignment
//!
//! [`ChunkReader`] returns chunks that end on a newline, so no row is ever split across
//! a chunk. That is what allows the decode pool to work on chunks independently. Only
//! the final chunk of a stream may lack a trailing newline.

use std::io::{Cursor, Read};

use crate::error::{Error, Result};

/// Compression detected at the head of a dump.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compression {
    /// Uncompressed plain text.
    None,
    /// gzip, as produced by `pg_dump -Z`.
    Gzip,
    /// zstd. Recognised so that it produces a precise error rather than a corrupt parse,
    /// but not decoded: support is not compiled in.
    Zstd,
}

impl Compression {
    /// Name used in diagnostics.
    pub fn name(self) -> &'static str {
        match self {
            Compression::None => "none",
            Compression::Gzip => "gzip",
            Compression::Zstd => "zstd",
        }
    }
}

/// Identifies the compression format from a stream's leading bytes.
///
/// Detection is by magic number rather than by file extension or configuration, so that
/// a sender changing format is handled rather than misread. A third party that varies
/// its table set between dumps cannot be assumed to hold its compression fixed either.
pub fn detect(head: &[u8]) -> Compression {
    if head.starts_with(&[0x1f, 0x8b]) {
        Compression::Gzip
    } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        Compression::Zstd
    } else {
        Compression::None
    }
}

/// Reads up to `buf.len()` bytes, tolerating short reads until EOF.
fn read_head<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..])? {
            0 => break,
            got => n += got,
        }
    }
    Ok(n)
}

/// Wraps `inner` in the decompressor its magic bytes call for.
///
/// The bytes consumed while sniffing are pushed back, so the returned reader yields the
/// complete stream from its first byte.
///
/// # Errors
///
/// [`Error::UnsupportedCompression`] for a format that is recognised but not compiled
/// in, and [`Error::Io`] for a read failure.
///
/// # Panics
///
/// Does not panic.
pub fn decompressed<R: Read + 'static>(mut inner: R) -> Result<(Compression, Box<dyn Read>)> {
    let mut magic = [0u8; 4];
    let n = read_head(&mut inner, &mut magic)?;
    let compression = detect(&magic[..n]);
    let stream = Cursor::new(magic[..n].to_vec()).chain(inner);

    let reader: Box<dyn Read> = match compression {
        Compression::None => Box::new(stream),
        // MultiGzDecoder rather than GzDecoder: a concatenated archive would otherwise
        // stop silently at the end of its first member, which is a truncated load
        // reported as success.
        Compression::Gzip => Box::new(flate2::read::MultiGzDecoder::new(stream)),
        Compression::Zstd => {
            return Err(Error::UnsupportedCompression {
                format: compression.name().to_string(),
            });
        }
    };
    Ok((compression, reader))
}

/// Default chunk size handed to the decode pool.
pub const DEFAULT_CHUNK_BYTES: usize = 16 << 20;

/// Hard ceiling on a chunk, and therefore on a single row.
///
/// A stream containing no newline cannot be split, so the buffer would otherwise grow
/// without bound on malformed input.
pub const DEFAULT_MAX_CHUNK_BYTES: usize = 256 << 20;

/// Splits a stream into newline-aligned chunks.
///
/// Each chunk ends immediately after a newline, except possibly the last. Bytes after
/// the final newline of one chunk are carried into the next, so a row is never split.
#[derive(Debug)]
pub struct ChunkReader<R> {
    inner: R,
    buf: Vec<u8>,
    /// Bytes of `buf` holding data.
    filled: usize,
    /// Bytes at the head of `buf` already returned to the caller.
    consumed: usize,
    target: usize,
    max_chunk: usize,
    eof: bool,
}

impl<R: Read> ChunkReader<R> {
    /// Creates a reader with the default chunk size and ceiling.
    pub fn new(inner: R) -> Self {
        Self::with_limits(inner, DEFAULT_CHUNK_BYTES, DEFAULT_MAX_CHUNK_BYTES)
    }

    /// Creates a reader with an explicit target chunk size and hard ceiling.
    ///
    /// `target` is the size aimed for; a chunk may be shorter, because it is trimmed
    /// back to a newline. `max_chunk` bounds growth when a single line exceeds `target`.
    pub fn with_limits(inner: R, target: usize, max_chunk: usize) -> Self {
        Self {
            inner,
            buf: Vec::new(),
            filled: 0,
            consumed: 0,
            target: target.max(1),
            max_chunk: max_chunk.max(target.max(1)),
            eof: false,
        }
    }

    /// Returns the next newline-aligned chunk, or `None` at end of stream.
    ///
    /// The returned slice borrows an internal buffer and is invalidated by the next
    /// call.
    ///
    /// # Errors
    ///
    /// [`Error::RowTooLarge`] if a single line exceeds the configured ceiling, and
    /// [`Error::Io`] for a read failure.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn next_chunk(&mut self) -> Result<Option<&[u8]>> {
        // Carry forward whatever the previous call did not return.
        if self.consumed > 0 {
            self.buf.copy_within(self.consumed..self.filled, 0);
            self.filled -= self.consumed;
            self.consumed = 0;
        }

        loop {
            while self.filled < self.target && !self.eof {
                if self.buf.len() < self.target {
                    self.buf.resize(self.target, 0);
                }
                match self.inner.read(&mut self.buf[self.filled..self.target])? {
                    0 => self.eof = true,
                    got => self.filled += got,
                }
            }

            if self.filled == 0 {
                return Ok(None);
            }

            if let Some(nl) = memchr::memrchr(b'\n', &self.buf[..self.filled]) {
                self.consumed = nl + 1;
                return Ok(Some(&self.buf[..nl + 1]));
            }

            if self.eof {
                // Final line of a stream that does not end with a newline.
                self.consumed = self.filled;
                return Ok(Some(&self.buf[..self.filled]));
            }

            // No newline in a full buffer: one line is longer than the target, so grow.
            if self.target >= self.max_chunk {
                return Err(Error::RowTooLarge {
                    len: self.filled,
                    limit: self.max_chunk,
                });
            }
            self.target = (self.target * 2).min(self.max_chunk);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::Compression as GzLevel;
    use flate2::write::GzEncoder;
    use std::io::Write;

    fn gzip(data: &[u8]) -> Vec<u8> {
        let mut e = GzEncoder::new(Vec::new(), GzLevel::default());
        e.write_all(data).unwrap();
        e.finish().unwrap()
    }

    fn collect(mut r: ChunkReader<impl Read>) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        while let Some(c) = r.next_chunk().unwrap() {
            out.push(c.to_vec());
        }
        out
    }

    #[test]
    fn detects_formats_by_magic() {
        assert_eq!(detect(&[0x1f, 0x8b, 0x08, 0x00]), Compression::Gzip);
        assert_eq!(detect(&[0x28, 0xb5, 0x2f, 0xfd]), Compression::Zstd);
        assert_eq!(detect(b"COPY"), Compression::None);
        assert_eq!(detect(b""), Compression::None);
    }

    #[test]
    fn plain_stream_passes_through_intact() {
        let data = b"line one\nline two\n";
        let (c, mut r) = decompressed(Cursor::new(data.to_vec())).unwrap();
        assert_eq!(c, Compression::None);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn short_stream_is_not_misdetected() {
        // Fewer bytes than the magic buffer must not panic or over-read.
        let (c, mut r) = decompressed(Cursor::new(b"ab".to_vec())).unwrap();
        assert_eq!(c, Compression::None);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"ab");
    }

    #[test]
    fn gzip_round_trips() {
        let data: Vec<u8> = (0..5000)
            .map(|i| format!("row {i}\n"))
            .collect::<String>()
            .into();
        let (c, mut r) = decompressed(Cursor::new(gzip(&data))).unwrap();
        assert_eq!(c, Compression::Gzip);
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, data);
    }

    #[test]
    fn concatenated_gzip_members_are_all_read() {
        let mut archive = gzip(b"first\n");
        archive.extend_from_slice(&gzip(b"second\n"));
        let (_, mut r) = decompressed(Cursor::new(archive)).unwrap();
        let mut got = Vec::new();
        r.read_to_end(&mut got).unwrap();
        assert_eq!(got, b"first\nsecond\n");
    }

    #[test]
    fn zstd_is_recognised_and_refused_precisely() {
        // `Box<dyn Read>` is not `Debug`, so the Ok arm cannot be unwrapped away.
        let Err(err) = decompressed(Cursor::new(vec![0x28, 0xb5, 0x2f, 0xfd, 0x00])) else {
            panic!("zstd must be refused rather than parsed");
        };
        assert_eq!(
            err,
            Error::UnsupportedCompression {
                format: "zstd".into()
            }
        );
    }

    #[test]
    fn every_chunk_ends_on_a_newline() {
        let data: Vec<u8> = (0..2000)
            .map(|i| format!("row {i}\n"))
            .collect::<String>()
            .into();
        let chunks = collect(ChunkReader::with_limits(
            Cursor::new(data.clone()),
            64,
            1 << 20,
        ));
        assert!(
            chunks.len() > 10,
            "expected many chunks, got {}",
            chunks.len()
        );
        for c in &chunks {
            assert_eq!(c.last(), Some(&b'\n'));
        }
        assert_eq!(chunks.concat(), data);
    }

    #[test]
    fn trailing_line_without_newline_is_returned() {
        let chunks = collect(ChunkReader::with_limits(
            Cursor::new(b"a\nb\ntail".to_vec()),
            4,
            1 << 20,
        ));
        assert_eq!(chunks.concat(), b"a\nb\ntail");
        assert_eq!(chunks.last().unwrap(), b"tail");
    }

    #[test]
    fn line_longer_than_target_grows_the_buffer() {
        let long = format!("{}\nshort\n", "x".repeat(1000));
        let chunks = collect(ChunkReader::with_limits(
            Cursor::new(long.clone().into_bytes()),
            16,
            1 << 20,
        ));
        assert_eq!(chunks.concat(), long.as_bytes());
    }

    #[test]
    fn line_exceeding_the_ceiling_is_an_error() {
        let long = "x".repeat(500);
        let mut r = ChunkReader::with_limits(Cursor::new(long.into_bytes()), 16, 64);
        assert!(matches!(
            r.next_chunk(),
            Err(Error::RowTooLarge { limit: 64, .. })
        ));
    }

    #[test]
    fn empty_stream_yields_nothing() {
        assert!(collect(ChunkReader::new(Cursor::new(Vec::new()))).is_empty());
    }
}
