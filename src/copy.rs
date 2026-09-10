//! COPY TEXT decoding.
//!
//! This is the hot path. It converts the body of a `COPY ... FROM stdin` block into rows
//! and fields, and resolves PostgreSQL's backslash escapes.
//!
//! Executes on the decode thread pool, described in Chapter IV of the architecture
//! document. Nothing here is async, and nothing here allocates: [`Rows`] and [`Fields`]
//! yield borrowed slices of the caller's buffer, and [`Field::unescape_into`] appends to
//! a buffer the caller supplies, so unescaped bytes can be written straight into an
//! Arrow builder without an intermediate allocation.
//!
//! The format is version-agnostic. COPY TEXT has not changed across the PostgreSQL major
//! versions in scope, so no version conditional appears in this module.
//!
//! # The property this module rests on
//!
//! In COPY TEXT a literal newline inside field data is emitted as the two-character
//! sequence `\n`, never as a raw `0x0A` byte. A raw newline therefore always terminates a
//! row, with no parsing context required, which is what allows the stream to be cut into
//! chunks and decoded in parallel.

use crate::error::{Error, Result};

/// The line that terminates a `COPY` block.
pub const END_OF_DATA: &[u8] = br"\.";

/// The field value denoting SQL NULL, compared against raw bytes before unescaping.
const NULL_MARKER: &[u8] = br"\N";

/// Bounds enforced while decoding, so that a malformed or hostile dump cannot exhaust
/// the process.
///
/// The dump is untrusted third-party input. These are the only barrier between a
/// corrupt block and an out-of-memory driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest single field, in bytes.
    ///
    /// PostgreSQL permits fields up to 1 GB. This is a policy ceiling well below that,
    /// chosen so one pathological `bytea` cannot dominate a batch.
    pub max_field_bytes: usize,

    /// Largest single row, in bytes.
    pub max_row_bytes: usize,

    /// Largest number of fields in a row.
    ///
    /// Defaults to PostgreSQL's own hard limit of 1600 columns per table, so a
    /// conforming dump can never trip it.
    pub max_columns: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_field_bytes: 64 << 20,
            max_row_bytes: 256 << 20,
            max_columns: 1600,
        }
    }
}

/// One field of one row, borrowed from the chunk buffer.
///
/// The distinction between [`Field::Plain`] and [`Field::Escaped`] exists so that the
/// common case costs nothing: a field containing no backslash is handed back as a slice
/// of the original buffer and never copied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field<'a> {
    /// SQL NULL. The raw field was exactly `\N`.
    Null,

    /// Contains no escape sequence. The bytes are the value verbatim.
    Plain(&'a [u8]),

    /// Contains at least one backslash. Call [`Field::unescape_into`] to resolve it.
    Escaped(&'a [u8]),
}

impl<'a> Field<'a> {
    /// Returns `true` if this field is SQL NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Field::Null)
    }

    /// Returns the field's bytes without resolving escapes.
    ///
    /// Returns `None` for [`Field::Null`]. For [`Field::Escaped`] the bytes are still in
    /// their encoded form; use [`Field::unescape_into`] to decode them.
    pub fn raw(&self) -> Option<&'a [u8]> {
        match self {
            Field::Null => None,
            Field::Plain(b) | Field::Escaped(b) => Some(b),
        }
    }

    /// Appends the field's decoded value to `out`.
    ///
    /// For [`Field::Plain`] this is a straight extend. For [`Field::Escaped`] the
    /// backslash sequences are resolved as they are copied. [`Field::Null`] appends
    /// nothing; callers must test [`Field::is_null`] separately, because an empty string
    /// and NULL are distinct values in this format.
    ///
    /// Recognised escapes are `\b`, `\f`, `\n`, `\r`, `\t`, `\v`, `\\`, an octal
    /// sequence of one to three digits, and `\x` followed by one or two hexadecimal
    /// digits. Any other backslashed character represents itself, which is PostgreSQL's
    /// documented behaviour.
    ///
    /// # Errors
    ///
    /// [`Error::TruncatedEscape`] if a backslash is the final byte of the field, and
    /// [`Error::InvalidHexEscape`] if `\x` is not followed by a hexadecimal digit.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn unescape_into(&self, out: &mut Vec<u8>) -> Result<()> {
        let bytes = match self {
            Field::Null => return Ok(()),
            Field::Plain(b) => {
                out.extend_from_slice(b);
                return Ok(());
            }
            Field::Escaped(b) => *b,
        };

        out.reserve(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            // Copy the run up to the next backslash in one go rather than byte by byte.
            match memchr::memchr(b'\\', &bytes[i..]) {
                None => {
                    out.extend_from_slice(&bytes[i..]);
                    break;
                }
                Some(off) => {
                    out.extend_from_slice(&bytes[i..i + off]);
                    i += off + 1;
                }
            }

            let Some(&c) = bytes.get(i) else {
                return Err(Error::TruncatedEscape);
            };
            i += 1;

            match c {
                b'b' => out.push(0x08),
                b'f' => out.push(0x0C),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b't' => out.push(b'\t'),
                b'v' => out.push(0x0B),
                b'x' => {
                    let mut value: u8 = 0;
                    let mut digits = 0;
                    while digits < 2 {
                        let Some(d) = bytes.get(i).and_then(|b| hex_digit(*b)) else {
                            break;
                        };
                        value = (value << 4) | d;
                        i += 1;
                        digits += 1;
                    }
                    if digits == 0 {
                        return Err(Error::InvalidHexEscape);
                    }
                    out.push(value);
                }
                b'0'..=b'7' => {
                    let mut value: u32 = u32::from(c - b'0');
                    let mut digits = 1;
                    while digits < 3 {
                        let Some(&d) = bytes.get(i) else { break };
                        if !(b'0'..=b'7').contains(&d) {
                            break;
                        }
                        value = (value << 3) | u32::from(d - b'0');
                        i += 1;
                        digits += 1;
                    }
                    // Three octal digits reach 0o777, so the low byte is taken. This
                    // matches PostgreSQL, which does not reject an over-wide sequence.
                    out.push(value as u8);
                }
                other => out.push(other),
            }
        }
        Ok(())
    }

    /// Returns the field's decoded value as a fresh `Vec`.
    ///
    /// Convenience for tests and cold paths. The hot path should call
    /// [`Field::unescape_into`] with a reused buffer instead.
    ///
    /// # Errors
    ///
    /// As [`Field::unescape_into`].
    pub fn to_vec(&self) -> Result<Option<Vec<u8>>> {
        if self.is_null() {
            return Ok(None);
        }
        let mut out = Vec::new();
        self.unescape_into(&mut out)?;
        Ok(Some(out))
    }
}

/// Decodes one hexadecimal digit, or `None` if `b` is not one.
fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Returns `true` if `line` is the `\.` end-of-data marker.
///
/// PostgreSQL escapes a backslash-period occurring in real data, so an exact match is
/// sufficient and cannot be spoofed by field contents.
pub fn is_end_of_data(line: &[u8]) -> bool {
    line == END_OF_DATA
}

/// Splits a chunk into complete rows.
///
/// Yields only newline-terminated lines. Any trailing bytes after the last newline are
/// left in [`Rows::remainder`] for the caller to carry into the next chunk, so a row is
/// never split across a boundary.
///
/// A trailing carriage return is stripped. A literal CR inside field data is escaped as
/// `\r`, so a raw `0x0D` before a newline can only be a line-ending artefact.
pub fn rows(chunk: &[u8]) -> Rows<'_> {
    Rows { rest: chunk }
}

/// Iterator over the complete rows of a chunk. See [`rows`].
#[derive(Debug, Clone)]
pub struct Rows<'a> {
    rest: &'a [u8],
}

impl<'a> Rows<'a> {
    /// Bytes following the last complete row, to be carried into the next chunk.
    pub fn remainder(&self) -> &'a [u8] {
        self.rest
    }
}

impl<'a> Iterator for Rows<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let nl = memchr::memchr(b'\n', self.rest)?;
        let mut line = &self.rest[..nl];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        self.rest = &self.rest[nl + 1..];
        Some(line)
    }
}

/// Splits a row into fields, enforcing `limits`.
///
/// A row always contains at least one field: a row of zero bytes is a single empty
/// field, which for a one-column table is the empty string rather than NULL.
pub fn fields(row: &[u8], limits: Limits) -> Fields<'_> {
    Fields {
        row,
        pos: 0,
        count: 0,
        limits,
        finished: false,
        row_checked: false,
    }
}

/// Iterator over the fields of a row. See [`fields`].
#[derive(Debug, Clone)]
pub struct Fields<'a> {
    row: &'a [u8],
    pos: usize,
    count: usize,
    limits: Limits,
    finished: bool,
    row_checked: bool,
}

impl<'a> Iterator for Fields<'a> {
    type Item = Result<Field<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.row_checked {
            self.row_checked = true;
            if self.row.len() > self.limits.max_row_bytes {
                self.finished = true;
                return Some(Err(Error::RowTooLarge {
                    len: self.row.len(),
                    limit: self.limits.max_row_bytes,
                }));
            }
        }
        if self.finished {
            return None;
        }

        let raw = match memchr::memchr(b'\t', &self.row[self.pos..]) {
            Some(off) => {
                let f = &self.row[self.pos..self.pos + off];
                self.pos += off + 1;
                f
            }
            None => {
                self.finished = true;
                &self.row[self.pos..]
            }
        };

        self.count += 1;
        if self.count > self.limits.max_columns {
            self.finished = true;
            return Some(Err(Error::TooManyColumns {
                count: self.count,
                limit: self.limits.max_columns,
            }));
        }
        if raw.len() > self.limits.max_field_bytes {
            self.finished = true;
            return Some(Err(Error::FieldTooLarge {
                len: raw.len(),
                limit: self.limits.max_field_bytes,
            }));
        }

        Some(Ok(classify(raw)))
    }
}

/// Classifies a raw field without copying it.
fn classify(raw: &[u8]) -> Field<'_> {
    if raw == NULL_MARKER {
        Field::Null
    } else if memchr::memchr(b'\\', raw).is_some() {
        Field::Escaped(raw)
    } else {
        Field::Plain(raw)
    }
}

/// Decodes a row into `out`, checking the field count against `expected`.
///
/// Each element is `None` for NULL, or a range into `scratch` holding the decoded bytes.
/// Both buffers are cleared on entry and reused across rows, so a steady-state decode
/// performs no allocation.
///
/// # Errors
///
/// [`Error::FieldCountMismatch`] if the row's field count differs from `expected`, plus
/// any error from [`Fields`] or [`Field::unescape_into`]. A mismatch is fatal by design:
/// padding or truncating the row would corrupt the table silently.
pub fn decode_row(
    row: &[u8],
    expected: usize,
    limits: Limits,
    scratch: &mut Vec<u8>,
    out: &mut Vec<Option<std::ops::Range<usize>>>,
) -> Result<()> {
    scratch.clear();
    out.clear();
    for field in fields(row, limits) {
        let field = field?;
        if field.is_null() {
            out.push(None);
        } else {
            let start = scratch.len();
            field.unescape_into(scratch)?;
            out.push(Some(start..scratch.len()));
        }
    }
    if out.len() != expected {
        return Err(Error::FieldCountMismatch {
            found: out.len(),
            expected,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(raw: &[u8]) -> Option<Vec<u8>> {
        classify(raw).to_vec().unwrap()
    }

    #[test]
    fn null_is_distinct_from_empty() {
        assert_eq!(dec(br"\N"), None);
        assert_eq!(dec(b""), Some(Vec::new()));
    }

    #[test]
    fn plain_field_is_borrowed() {
        assert!(matches!(classify(b"hello"), Field::Plain(b"hello")));
        assert!(matches!(classify(br"a\tb"), Field::Escaped(_)));
    }

    #[test]
    fn named_escapes() {
        assert_eq!(dec(br"a\tb").unwrap(), b"a\tb");
        assert_eq!(dec(br"a\nb").unwrap(), b"a\nb");
        assert_eq!(dec(br"a\rb").unwrap(), b"a\rb");
        assert_eq!(dec(br"a\\b").unwrap(), br"a\b");
        assert_eq!(dec(br"\b").unwrap(), [0x08]);
        assert_eq!(dec(br"\f").unwrap(), [0x0C]);
        assert_eq!(dec(br"\v").unwrap(), [0x0B]);
    }

    #[test]
    fn octal_and_hex_escapes() {
        assert_eq!(dec(br"\101").unwrap(), b"A");
        assert_eq!(dec(br"\1").unwrap(), [1]);
        assert_eq!(dec(br"\12").unwrap(), [0o12]);
        assert_eq!(dec(br"\x41").unwrap(), b"A");
        assert_eq!(dec(br"\x4").unwrap(), [4]);
        // Octal consumes at most three digits; the fourth is literal.
        assert_eq!(dec(br"\1011").unwrap(), b"A1");
    }

    #[test]
    fn unknown_escape_is_literal() {
        assert_eq!(dec(br"\q").unwrap(), b"q");
        assert_eq!(dec(br"\N x").unwrap(), b"N x");
    }

    #[test]
    fn malformed_escapes_are_errors() {
        assert_eq!(classify(br"a\").to_vec(), Err(Error::TruncatedEscape));
        assert_eq!(classify(br"\xz").to_vec(), Err(Error::InvalidHexEscape));
    }

    #[test]
    fn rows_yield_only_complete_lines() {
        let chunk = b"a\nb\npartial";
        let mut it = rows(chunk);
        assert_eq!(it.next(), Some(&b"a"[..]));
        assert_eq!(it.next(), Some(&b"b"[..]));
        assert_eq!(it.next(), None);
        assert_eq!(it.remainder(), b"partial");
    }

    #[test]
    fn crlf_is_tolerated() {
        let mut it = rows(b"a\tb\r\n");
        assert_eq!(it.next(), Some(&b"a\tb"[..]));
    }

    #[test]
    fn end_of_data_marker() {
        assert!(is_end_of_data(br"\."));
        assert!(!is_end_of_data(br"\.x"));
        assert!(!is_end_of_data(b"1"));
    }

    #[test]
    fn field_splitting_counts_empty_fields() {
        let got: Vec<_> = fields(b"a\t\tb", Limits::default())
            .map(|f| f.unwrap())
            .collect();
        assert_eq!(got.len(), 3);
        assert_eq!(got[1], Field::Plain(b""));
    }

    #[test]
    fn single_empty_field_for_empty_row() {
        let got: Vec<_> = fields(b"", Limits::default()).collect();
        assert_eq!(got.len(), 1);
    }

    #[test]
    fn limits_are_enforced() {
        let limits = Limits {
            max_field_bytes: 2,
            ..Default::default()
        };
        let err = fields(b"abc", limits).next().unwrap().unwrap_err();
        assert_eq!(err, Error::FieldTooLarge { len: 3, limit: 2 });

        let limits = Limits {
            max_columns: 2,
            ..Default::default()
        };
        let errs: Vec<_> = fields(b"a\tb\tc", limits).collect();
        assert!(errs[2].is_err());

        let limits = Limits {
            max_row_bytes: 2,
            ..Default::default()
        };
        assert!(fields(b"abcdef", limits).next().unwrap().is_err());
    }

    #[test]
    fn decode_row_checks_arity() {
        let mut scratch = Vec::new();
        let mut out = Vec::new();
        let row = b"1\talice\t\\N";

        decode_row(row, 3, Limits::default(), &mut scratch, &mut out).unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(&scratch[out[0].clone().unwrap()], b"1");
        assert_eq!(&scratch[out[1].clone().unwrap()], b"alice");
        assert!(out[2].is_none());

        let err = decode_row(row, 4, Limits::default(), &mut scratch, &mut out).unwrap_err();
        assert_eq!(
            err,
            Error::FieldCountMismatch {
                found: 3,
                expected: 4
            }
        );
    }

    #[test]
    fn decode_row_reuses_buffers() {
        let mut scratch = Vec::new();
        let mut out = Vec::new();
        decode_row(b"aaaa\tbbbb", 2, Limits::default(), &mut scratch, &mut out).unwrap();
        let cap = scratch.capacity();
        for _ in 0..100 {
            decode_row(b"aaaa\tbbbb", 2, Limits::default(), &mut scratch, &mut out).unwrap();
        }
        assert_eq!(scratch.capacity(), cap, "steady state must not reallocate");
    }
}
