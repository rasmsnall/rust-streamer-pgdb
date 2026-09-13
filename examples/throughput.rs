//! Single-threaded decode throughput on synthetic COPY TEXT data.
//!
//! Establishes the per-core floor referenced in Chapter IV of the architecture document,
//! against which the decode pool is sized. Run in release; a debug build is not
//! representative.
//!
//! ```text
//! cargo run --release --example throughput
//! cargo run --release --features zstd --example throughput
//! ```
//!
//! The second form adds a zstd measurement alongside gzip's, for comparing the two
//! formats' decompression rate: decompression is the pipeline's serial floor (see
//! `src/dump.rs`), so this is the number that matters most for a sender who can be
//! persuaded to switch compression.

use std::hint::black_box;
use std::time::Instant;

use pgdelta::copy::{Limits, decode_row, rows};
use pgdelta::dump::{ChunkReader, decompressed};
use pgdelta::scan::{Event, Scanner};

/// xorshift64. Deterministic, so runs are comparable, and cheap enough not to distort
/// the timings it feeds.
fn rng(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// Builds a COPY block body of roughly `target` bytes.
///
/// Field values carry real entropy. An earlier fixture repeated the same handful of
/// strings on every row and compressed 22x, which made gzip look four times faster than
/// it is on production data: the decoder was copying long matches rather than working.
/// Values here are varied so the gzip ratio lands in the range a real dump reaches,
/// which is what makes the decompression timing meaningful. One field in four carries an
/// escape, so the escape path is exercised rather than optimised away.
fn synthetic_rows(target: usize) -> Vec<u8> {
    const WORDS: &[&str] = &[
        "invoice",
        "shipment",
        "pending",
        "cancelled",
        "warehouse",
        "customer",
        "adjustment",
        "reconciled",
        "partial",
        "backorder",
        "credit",
        "return",
    ];
    let mut out = Vec::with_capacity(target + 1024);
    let mut s: u64 = 0x2545F4914F6CDD1D;
    let mut i: u64 = 0;
    while out.len() < target {
        // Surrogate key: sequential, as in a real table.
        out.extend_from_slice(i.to_string().as_bytes());
        out.push(b'\t');

        // High-cardinality identifier.
        let u = rng(&mut s);
        out.extend_from_slice(format!("user{:x}@example{}.com", u & 0xFFFFFFF, u % 97).as_bytes());
        out.push(b'\t');

        // Free text with an embedded token, one row in four carrying escapes.
        let w1 = WORDS[(rng(&mut s) % WORDS.len() as u64) as usize];
        let w2 = WORDS[(rng(&mut s) % WORDS.len() as u64) as usize];
        let token = rng(&mut s);
        if i.is_multiple_of(4) {
            out.extend_from_slice(format!("{w1}\\t{w2}\\nref {token:x}").as_bytes());
        } else {
            out.extend_from_slice(format!("{w1} {w2} ref {token:x}").as_bytes());
        }
        out.push(b'\t');

        // Timestamp, and NULL one row in seven.
        if i.is_multiple_of(7) {
            out.extend_from_slice(br"\N");
        } else {
            let t = rng(&mut s);
            out.extend_from_slice(
                format!(
                    "2024-{:02}-{:02} {:02}:{:02}:{:02}",
                    1 + t % 12,
                    1 + (t >> 4) % 28,
                    (t >> 9) % 24,
                    (t >> 14) % 60,
                    (t >> 20) % 60
                )
                .as_bytes(),
            );
        }
        out.push(b'\n');
        i += 1;
    }
    out
}

fn main() {
    const TARGET: usize = 256 << 20;
    let data = synthetic_rows(TARGET);
    let bytes = data.len() as f64;
    let mib = bytes / (1024.0 * 1024.0);

    // Decode: rows to fields, with escapes resolved into a reused buffer.
    let limits = Limits::default();
    let mut scratch = Vec::new();
    let mut fields = Vec::new();
    let mut count = 0u64;

    let t = Instant::now();
    for row in rows(&data) {
        decode_row(row, 4, limits, &mut scratch, &mut fields).unwrap();
        black_box(&fields);
        count += 1;
    }
    let decode = t.elapsed().as_secs_f64();

    // Scan: the sequential stage, measured over the same bytes wrapped in a block.
    let mut dump =
        b"-- Dumped by pg_dump version 17.2\nCOPY public.t (a, b, c, d) FROM stdin;\n".to_vec();
    dump.extend_from_slice(&data);
    dump.extend_from_slice(b"\\.\n");

    let t = Instant::now();
    let mut scanner = Scanner::new();
    let mut scanned = 0usize;
    for event in scanner.feed(&dump).unwrap() {
        if let Event::CopyRows(r) = event {
            scanned += r.len();
        }
    }
    scanner.finish().unwrap();
    let scan = t.elapsed().as_secs_f64();

    // Decompression: the serial stage, and therefore the floor on total runtime.
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut enc, &data).unwrap();
    let gz = enc.finish().unwrap();
    let ratio = bytes / gz.len() as f64;

    let t = Instant::now();
    let (_, reader) = decompressed(std::io::Cursor::new(gz.clone())).unwrap();
    let mut cr = ChunkReader::new(reader);
    let mut inflated = 0usize;
    while let Some(chunk) = cr.next_chunk().unwrap() {
        inflated += chunk.len();
        black_box(chunk);
    }
    let gunzip = t.elapsed().as_secs_f64();
    assert_eq!(inflated, data.len());

    // zstd, at its default level, for comparison against gzip: only meaningful when
    // built with the `zstd` feature, since that is what makes this crate able to decode
    // it at all. Compression itself needs no feature (this crate never compresses), so
    // the comparison is available even without the feature; only the "decompress through
    // this crate's own decompressed()" half is feature-gated.
    #[cfg(feature = "zstd")]
    let (zstd_size, zstd_seconds) = {
        let z = zstd::stream::encode_all(data.as_slice(), 0).unwrap();
        let t = Instant::now();
        let (_, reader) = decompressed(std::io::Cursor::new(z.clone())).unwrap();
        let mut cr = ChunkReader::new(reader);
        let mut inflated = 0usize;
        while let Some(chunk) = cr.next_chunk().unwrap() {
            inflated += chunk.len();
            black_box(chunk);
        }
        assert_eq!(inflated, data.len());
        (z.len(), t.elapsed().as_secs_f64())
    };

    println!("input            {mib:.0} MiB, {count} rows");
    println!(
        "gzip size        {:.0} MiB  ({ratio:.1}x)",
        gz.len() as f64 / (1024.0 * 1024.0)
    );
    println!("gunzip+chunk     {gunzip:.3} s   {:.0} MiB/s", mib / gunzip);
    #[cfg(feature = "zstd")]
    {
        let zstd_ratio = bytes / zstd_size as f64;
        println!(
            "zstd size        {:.0} MiB  ({zstd_ratio:.1}x)",
            zstd_size as f64 / (1024.0 * 1024.0)
        );
        println!(
            "unzstd+chunk     {zstd_seconds:.3} s   {:.0} MiB/s",
            mib / zstd_seconds
        );
    }
    #[cfg(not(feature = "zstd"))]
    println!("unzstd+chunk     (build with --features zstd to measure)");
    println!("decode           {decode:.3} s   {:.0} MiB/s", mib / decode);
    println!("scan             {scan:.3} s   {:.0} MiB/s", mib / scan);
    println!("rows passed on   {} MiB", scanned / (1024 * 1024));
    println!();
    let gz_rate = mib / gunzip;
    let dec_rate = mib / decode;
    println!(
        "48 GB:  gunzip {:.1} min (serial floor), decode {:.1} min on one core",
        49152.0 / gz_rate / 60.0,
        49152.0 / dec_rate / 60.0
    );
    println!(
        "480 GB: gunzip {:.1} min (serial floor), decode {:.1} min on one core",
        491520.0 / gz_rate / 60.0,
        491520.0 / dec_rate / 60.0
    );
    #[cfg(feature = "zstd")]
    {
        let zstd_rate = mib / zstd_seconds;
        println!(
            "48 GB:  unzstd {:.1} min (serial floor)",
            49152.0 / zstd_rate / 60.0
        );
        println!(
            "480 GB: unzstd {:.1} min (serial floor)",
            491520.0 / zstd_rate / 60.0
        );
    }
}
