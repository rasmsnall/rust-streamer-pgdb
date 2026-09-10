//! Parsing unescaped COPY TEXT field bytes into typed values.
//!
//! Executes on the decode thread pool. Nothing here is async, and nothing here
//! allocates except [`parse_bytea`], which appends to a caller-supplied buffer.
//!
//! Carries no Arrow dependency: these functions produce plain integers, and
//! `builders.rs` appends them to Arrow arrays. That keeps the awkward parts, which are
//! the calendar arithmetic and PostgreSQL's several output conventions, testable without
//! compiling the Delta write path.
//!
//! # Two kinds of failure
//!
//! The return type is `Result<Option<T>>`, and the two negative cases mean different
//! things.
//!
//! - `Ok(None)` means the value is **valid but not representable**. PostgreSQL's
//!   `infinity` for dates and timestamps, and `NaN` for numeric, have no encoding in the
//!   corresponding Arrow types. These become NULL and are counted in the run statistics,
//!   so the substitution is visible rather than silent.
//! - `Err` means the value is **malformed for its declared type**, such as text in an
//!   integer column. That is a structural fault: the dump disagrees with its own DDL, so
//!   it fails the load rather than degrading.
//!
//! Errors never carry the offending value, only the column and the expected type,
//! because error text reaches logs and Python tracebacks and the dump is untrusted.

use crate::error::{Error, Result};

/// Days from the Unix epoch to `0001-01-01`, used to bound plausible dates.
const MIN_SUPPORTED_DAY: i64 = -2_440_588;

/// Microseconds in one second.
const MICROS_PER_SEC: i64 = 1_000_000;
/// Microseconds in one day.
const MICROS_PER_DAY: i64 = 86_400 * MICROS_PER_SEC;

/// Builds the error for a value that does not match its declared type.
fn bad(column: &str, expected: &'static str) -> Error {
    Error::UnparsableValue {
        column: column.to_string(),
        expected,
    }
}

/// Interprets bytes as ASCII text, rejecting anything that is not valid UTF-8.
fn text<'a>(v: &'a [u8], column: &str, expected: &'static str) -> Result<&'a str> {
    std::str::from_utf8(v).map_err(|_| bad(column, expected))
}

/// Parses a signed 16-bit integer.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not an integer or does not fit.
pub fn parse_i16(v: &[u8], column: &str) -> Result<Option<i16>> {
    let s = text(v, column, "smallint")?;
    s.trim()
        .parse::<i16>()
        .map(Some)
        .map_err(|_| bad(column, "smallint"))
}

/// Parses a signed 32-bit integer.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not an integer or does not fit.
pub fn parse_i32(v: &[u8], column: &str) -> Result<Option<i32>> {
    let s = text(v, column, "integer")?;
    s.trim()
        .parse::<i32>()
        .map(Some)
        .map_err(|_| bad(column, "integer"))
}

/// Parses a signed 64-bit integer.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not an integer or does not fit.
pub fn parse_i64(v: &[u8], column: &str) -> Result<Option<i64>> {
    let s = text(v, column, "bigint")?;
    s.trim()
        .parse::<i64>()
        .map(Some)
        .map_err(|_| bad(column, "bigint"))
}

/// Parses a 32-bit float.
///
/// PostgreSQL writes `NaN`, `Infinity`, and `-Infinity`, all of which IEEE 754
/// represents, so unlike the temporal types nothing is lost here.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not a float.
pub fn parse_f32(v: &[u8], column: &str) -> Result<Option<f32>> {
    let s = text(v, column, "real")?;
    Ok(Some(match s.trim() {
        "NaN" => f32::NAN,
        "Infinity" => f32::INFINITY,
        "-Infinity" => f32::NEG_INFINITY,
        other => other.parse::<f32>().map_err(|_| bad(column, "real"))?,
    }))
}

/// Parses a 64-bit float. See [`parse_f32`].
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not a float.
pub fn parse_f64(v: &[u8], column: &str) -> Result<Option<f64>> {
    let s = text(v, column, "double precision")?;
    Ok(Some(match s.trim() {
        "NaN" => f64::NAN,
        "Infinity" => f64::INFINITY,
        "-Infinity" => f64::NEG_INFINITY,
        other => other
            .parse::<f64>()
            .map_err(|_| bad(column, "double precision"))?,
    }))
}

/// Parses a boolean, which COPY TEXT writes as `t` or `f`.
///
/// # Errors
///
/// [`Error::UnparsableValue`] for anything else.
pub fn parse_bool(v: &[u8], column: &str) -> Result<Option<bool>> {
    match v {
        b"t" | b"true" | b"T" => Ok(Some(true)),
        b"f" | b"false" | b"F" => Ok(Some(false)),
        _ => Err(bad(column, "boolean")),
    }
}

/// Parses a fixed-scale decimal into the unscaled integer Arrow stores.
///
/// `NaN`, which PostgreSQL permits in `numeric`, has no `Decimal128` encoding and yields
/// `Ok(None)`.
///
/// A value carrying more fractional digits than the declared scale is rejected unless
/// the surplus digits are all zero. Silently discarding significant digits would change
/// stored values without saying so.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not a decimal, carries an exponent, or
/// would lose significant precision.
pub fn parse_decimal(v: &[u8], scale: i8, column: &str) -> Result<Option<i128>> {
    let s = text(v, column, "numeric")?.trim();
    if s == "NaN" {
        return Ok(None);
    }
    let (negative, digits) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    if digits.is_empty() || digits.bytes().any(|b| b == b'e' || b == b'E') {
        return Err(bad(column, "numeric"));
    }

    let (int_part, frac_part) = match digits.find('.') {
        Some(i) => (&digits[..i], &digits[i + 1..]),
        None => (digits, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(bad(column, "numeric"));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(bad(column, "numeric"));
    }

    let scale = usize::try_from(scale).map_err(|_| bad(column, "numeric"))?;
    let (frac_keep, frac_drop) = if frac_part.len() > scale {
        frac_part.split_at(scale)
    } else {
        (frac_part, "")
    };
    if frac_drop.bytes().any(|b| b != b'0') {
        return Err(bad(column, "numeric"));
    }

    let mut unscaled: i128 = 0;
    for b in int_part.bytes().chain(frac_keep.bytes()) {
        unscaled = unscaled
            .checked_mul(10)
            .and_then(|n| n.checked_add(i128::from(b - b'0')))
            .ok_or_else(|| bad(column, "numeric"))?;
    }
    // Pad when the value carried fewer fractional digits than the column declares.
    for _ in frac_keep.len()..scale {
        unscaled = unscaled.checked_mul(10).ok_or_else(|| bad(column, "numeric"))?;
    }
    Ok(Some(if negative { -unscaled } else { unscaled }))
}

/// Days from the Unix epoch for a proleptic Gregorian date.
///
/// Howard Hinnant's algorithm, implemented directly rather than pulling in a calendar
/// crate for one function.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Splits a trailing ` BC` era marker from a date or timestamp.
///
/// PostgreSQL counts BC years from 1, while the proleptic Gregorian calendar counts
/// through zero, so `1 BC` is ISO year 0 and the conversion is `1 - year`.
fn split_era(s: &str) -> (&str, bool) {
    match s.strip_suffix(" BC") {
        Some(rest) => (rest.trim_end(), true),
        None => (s, false),
    }
}

/// Parses `YYYY-MM-DD`, returning the calendar fields.
fn civil_parts(s: &str) -> Option<(i64, i64, i64)> {
    let mut it = s.splitn(3, '-');
    let y = it.next()?;
    let m = it.next()?;
    let d = it.next()?;
    if y.is_empty() || !y.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if m.len() != 2 || d.len() != 2 {
        return None;
    }
    let y: i64 = y.parse().ok()?;
    let m: i64 = m.parse().ok()?;
    let d: i64 = d.parse().ok()?;
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) {
        return None;
    }
    Some((y, m, d))
}

/// Parses a date into days since the Unix epoch.
///
/// `infinity` and `-infinity` yield `Ok(None)`: Arrow's `Date32` has no encoding for
/// them. The alternative, saturating to the extreme representable day, would put a year
/// in the millions into the column and quietly corrupt every downstream aggregate, so
/// NULL is the more honest substitution. The count is reported in the run statistics.
///
/// ` BC` suffixed dates are converted to proleptic Gregorian years.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not a date.
pub fn parse_date(v: &[u8], column: &str) -> Result<Option<i32>> {
    let s = text(v, column, "date")?.trim();
    if s == "infinity" || s == "-infinity" {
        return Ok(None);
    }
    let (s, bc) = split_era(s);
    let (y, m, d) = civil_parts(s).ok_or_else(|| bad(column, "date"))?;
    let y = if bc { 1 - y } else { y };
    let days = days_from_civil(y, m, d);
    if days < MIN_SUPPORTED_DAY && !bc {
        return Err(bad(column, "date"));
    }
    i32::try_from(days)
        .map(Some)
        .map_err(|_| bad(column, "date"))
}

/// Parses `HH:MM:SS[.ffffff]` into microseconds since midnight.
fn time_micros(s: &str) -> Option<i64> {
    let (hms, frac) = match s.find('.') {
        Some(i) => (&s[..i], &s[i + 1..]),
        None => (s, ""),
    };
    let mut it = hms.splitn(3, ':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let sec: i64 = it.next().unwrap_or("0").parse().ok()?;
    // 24:00:00 is a legal PostgreSQL time meaning end of day.
    if !(0..=24).contains(&h) || !(0..60).contains(&m) || !(0..=60).contains(&sec) {
        return None;
    }
    let mut micros = 0i64;
    if !frac.is_empty() {
        if frac.len() > 6 || !frac.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut scaled: i64 = frac.parse().ok()?;
        for _ in frac.len()..6 {
            scaled *= 10;
        }
        micros = scaled;
    }
    Some((h * 3600 + m * 60 + sec) * MICROS_PER_SEC + micros)
}

/// Parses a time of day into microseconds since midnight.
///
/// A `time with time zone` value carries an offset, which is applied so that the result
/// is expressed in UTC. PostgreSQL's own documentation discourages that type; it is
/// supported here for completeness rather than endorsed.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not a time.
pub fn parse_time(v: &[u8], column: &str) -> Result<Option<i64>> {
    let s = text(v, column, "time")?.trim();
    let (body, offset) = split_offset(s);
    let micros = time_micros(body).ok_or_else(|| bad(column, "time"))?;
    let micros = micros - offset.unwrap_or(0);
    Ok(Some(micros.rem_euclid(MICROS_PER_DAY)))
}

/// Splits a trailing UTC offset such as `+01`, `-05:30`, or `+00:00:00`.
///
/// Returns the body and the offset in microseconds. A bare date has no offset, and the
/// `-` of a negative offset is distinguished from the `-` separators of the date by only
/// considering the portion after the time begins.
fn split_offset(s: &str) -> (&str, Option<i64>) {
    let search_from = s.find(' ').map(|i| i + 1).unwrap_or(0);
    let tail = &s[search_from..];
    let Some(rel) = tail.rfind(['+', '-']) else {
        return (s, None);
    };
    let at = search_from + rel;
    let sign = if s.as_bytes()[at] == b'-' { -1 } else { 1 };
    let off = &s[at + 1..];
    let mut it = off.split(':');
    let Some(h) = it.next().and_then(|h| h.parse::<i64>().ok()) else {
        return (s, None);
    };
    let m = it.next().and_then(|m| m.parse::<i64>().ok()).unwrap_or(0);
    let sec = it.next().and_then(|x| x.parse::<i64>().ok()).unwrap_or(0);
    (
        &s[..at],
        Some(sign * (h * 3600 + m * 60 + sec) * MICROS_PER_SEC),
    )
}

/// Parses a timestamp into microseconds since the Unix epoch, in UTC.
///
/// `infinity` and `-infinity` yield `Ok(None)`, for the reason given on [`parse_date`].
///
/// When `tz` is true the value carries an offset which is subtracted to reach UTC. When
/// it is false the value is naive and is **assumed to be UTC**, which is a documented
/// assumption of this library rather than a fact about the data: Delta's `timestamp` is
/// microseconds UTC, and `timestamp_ntz` would raise the protocol requirement above the
/// compatibility floor this project holds to.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the text is not a timestamp.
pub fn parse_timestamp(v: &[u8], tz: bool, column: &str) -> Result<Option<i64>> {
    let s = text(v, column, "timestamp")?.trim();
    if s == "infinity" || s == "-infinity" {
        return Ok(None);
    }
    let (s, bc) = split_era(s);
    let (body, offset) = if tz { split_offset(s) } else { (s, None) };

    let (date_part, time_part) = match body.split_once(' ') {
        Some((d, t)) => (d, t),
        None => (body, "00:00:00"),
    };
    let (y, m, d) = civil_parts(date_part).ok_or_else(|| bad(column, "timestamp"))?;
    let y = if bc { 1 - y } else { y };
    let micros = time_micros(time_part.trim()).ok_or_else(|| bad(column, "timestamp"))?;

    days_from_civil(y, m, d)
        .checked_mul(MICROS_PER_DAY)
        .and_then(|d| d.checked_add(micros))
        .and_then(|t| t.checked_sub(offset.unwrap_or(0)))
        .map(Some)
        .ok_or_else(|| bad(column, "timestamp"))
}

/// Decodes a `bytea` value, appending the bytes to `out`.
///
/// Accepts both output formats: the modern `\x` hexadecimal form, and the older escape
/// form in which non-printable bytes appear as three-digit octal sequences. The two are
/// distinguished by their leading bytes, so a dump written under either
/// `bytea_output` setting is read correctly.
///
/// Note that the input is the field **after** COPY escape resolution, so a hex value
/// arrives as `\x48690a` rather than `\\x48690a`.
///
/// # Errors
///
/// [`Error::UnparsableValue`] for an odd-length or non-hexadecimal hex body, or a
/// malformed octal escape.
pub fn parse_bytea(v: &[u8], column: &str, out: &mut Vec<u8>) -> Result<()> {
    if let Some(hex) = v.strip_prefix(br"\x") {
        if !hex.len().is_multiple_of(2) {
            return Err(bad(column, "bytea"));
        }
        out.reserve(hex.len() / 2);
        for pair in hex.chunks_exact(2) {
            let hi = hex_val(pair[0]).ok_or_else(|| bad(column, "bytea"))?;
            let lo = hex_val(pair[1]).ok_or_else(|| bad(column, "bytea"))?;
            out.push((hi << 4) | lo);
        }
        return Ok(());
    }

    let mut i = 0;
    while i < v.len() {
        if v[i] != b'\\' {
            out.push(v[i]);
            i += 1;
            continue;
        }
        i += 1;
        match v.get(i) {
            Some(b'\\') => {
                out.push(b'\\');
                i += 1;
            }
            Some(d) if d.is_ascii_digit() => {
                let oct = v.get(i..i + 3).ok_or_else(|| bad(column, "bytea"))?;
                let mut value: u32 = 0;
                for b in oct {
                    if !(b'0'..=b'7').contains(b) {
                        return Err(bad(column, "bytea"));
                    }
                    value = (value << 3) | u32::from(b - b'0');
                }
                if value > 0xFF {
                    return Err(bad(column, "bytea"));
                }
                out.push(value as u8);
                i += 3;
            }
            _ => return Err(bad(column, "bytea")),
        }
    }
    Ok(())
}

/// Decodes one hexadecimal digit.
fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const C: &str = "col";

    #[test]
    fn integers_round_trip_and_reject_overflow() {
        assert_eq!(parse_i16(b"-32768", C).unwrap(), Some(-32768));
        assert_eq!(parse_i32(b"2147483647", C).unwrap(), Some(2147483647));
        assert_eq!(parse_i64(b"-9223372036854775808", C).unwrap(), Some(i64::MIN));
        assert!(parse_i16(b"32768", C).is_err());
        assert!(parse_i32(b"abc", C).is_err());
    }

    #[test]
    fn floats_carry_their_special_values() {
        assert!(parse_f64(b"NaN", C).unwrap().unwrap().is_nan());
        assert_eq!(parse_f64(b"Infinity", C).unwrap(), Some(f64::INFINITY));
        assert_eq!(parse_f32(b"-Infinity", C).unwrap(), Some(f32::NEG_INFINITY));
        assert_eq!(parse_f64(b"1.5", C).unwrap(), Some(1.5));
        assert!(parse_f64(b"", C).is_err());
    }

    #[test]
    fn booleans_use_copy_spelling() {
        assert_eq!(parse_bool(b"t", C).unwrap(), Some(true));
        assert_eq!(parse_bool(b"f", C).unwrap(), Some(false));
        assert!(parse_bool(b"1", C).is_err());
    }

    #[test]
    fn decimals_scale_correctly() {
        assert_eq!(parse_decimal(b"123.45", 2, C).unwrap(), Some(12345));
        assert_eq!(parse_decimal(b"-123.45", 2, C).unwrap(), Some(-12345));
        assert_eq!(parse_decimal(b"7", 2, C).unwrap(), Some(700));
        assert_eq!(parse_decimal(b"0.5", 3, C).unwrap(), Some(500));
        assert_eq!(parse_decimal(b"123", 0, C).unwrap(), Some(123));
    }

    #[test]
    fn decimal_nan_becomes_null_not_an_error() {
        assert_eq!(parse_decimal(b"NaN", 2, C).unwrap(), None);
    }

    #[test]
    fn decimal_refuses_to_lose_significant_digits() {
        // Trailing zeros beyond the scale are harmless.
        assert_eq!(parse_decimal(b"1.2300", 2, C).unwrap(), Some(123));
        // Real digits beyond the scale are not.
        assert!(parse_decimal(b"1.239", 2, C).is_err());
        assert!(parse_decimal(b"1e5", 2, C).is_err());
        assert!(parse_decimal(b"abc", 2, C).is_err());
    }

    #[test]
    fn epoch_and_known_dates() {
        assert_eq!(parse_date(b"1970-01-01", C).unwrap(), Some(0));
        assert_eq!(parse_date(b"1970-01-02", C).unwrap(), Some(1));
        assert_eq!(parse_date(b"1969-12-31", C).unwrap(), Some(-1));
        assert_eq!(parse_date(b"2000-03-01", C).unwrap(), Some(11017));
        assert_eq!(parse_date(b"2024-02-29", C).unwrap(), Some(19782));
    }

    #[test]
    fn date_infinity_becomes_null() {
        assert_eq!(parse_date(b"infinity", C).unwrap(), None);
        assert_eq!(parse_date(b"-infinity", C).unwrap(), None);
    }

    #[test]
    fn bc_dates_convert_to_proleptic_years() {
        // 1 BC is ISO year 0, so it must land before 0001-01-01.
        let bc = parse_date(b"0001-01-01 BC", C).unwrap().unwrap();
        let ad = parse_date(b"0001-01-01", C).unwrap().unwrap();
        assert!(bc < ad);
        // A year 0 exists in the proleptic calendar, so the gap is exactly 366 days.
        assert_eq!(ad - bc, 366);
    }

    #[test]
    fn malformed_dates_are_errors() {
        assert!(parse_date(b"2024-13-01", C).is_err());
        assert!(parse_date(b"2024-01", C).is_err());
        assert!(parse_date(b"not a date", C).is_err());
    }

    #[test]
    fn timestamps_combine_date_and_time() {
        assert_eq!(parse_timestamp(b"1970-01-01 00:00:00", false, C).unwrap(), Some(0));
        assert_eq!(
            parse_timestamp(b"1970-01-01 00:00:01", false, C).unwrap(),
            Some(1_000_000)
        );
        assert_eq!(
            parse_timestamp(b"1970-01-02 00:00:00", false, C).unwrap(),
            Some(MICROS_PER_DAY)
        );
    }

    #[test]
    fn fractional_seconds_scale_to_micros() {
        assert_eq!(
            parse_timestamp(b"1970-01-01 00:00:00.5", false, C).unwrap(),
            Some(500_000)
        );
        assert_eq!(
            parse_timestamp(b"1970-01-01 00:00:00.123456", false, C).unwrap(),
            Some(123_456)
        );
    }

    #[test]
    fn timestamptz_offsets_are_normalised_to_utc() {
        let utc = parse_timestamp(b"2024-01-01 12:00:00+00", true, C).unwrap();
        let plus1 = parse_timestamp(b"2024-01-01 13:00:00+01", true, C).unwrap();
        assert_eq!(utc, plus1);

        let minus5 = parse_timestamp(b"2024-01-01 07:00:00-05", true, C).unwrap();
        assert_eq!(utc, minus5);

        let half = parse_timestamp(b"2024-01-01 17:30:00+05:30", true, C).unwrap();
        assert_eq!(utc, half);
    }

    #[test]
    fn naive_timestamp_ignores_a_trailing_sign() {
        // With tz false there is no offset to strip, and a bare date must still parse.
        assert_eq!(parse_timestamp(b"2024-01-01", false, C).unwrap(), parse_timestamp(b"2024-01-01 00:00:00", false, C).unwrap());
    }

    #[test]
    fn timestamp_infinity_becomes_null() {
        assert_eq!(parse_timestamp(b"infinity", false, C).unwrap(), None);
        assert_eq!(parse_timestamp(b"-infinity", true, C).unwrap(), None);
    }

    #[test]
    fn times_parse_with_and_without_fractions() {
        assert_eq!(parse_time(b"00:00:00", C).unwrap(), Some(0));
        assert_eq!(parse_time(b"01:00:00", C).unwrap(), Some(3_600_000_000));
        assert_eq!(parse_time(b"23:59:59.999999", C).unwrap(), Some(MICROS_PER_DAY - 1));
        assert!(parse_time(b"25:00:00", C).is_err());
    }

    #[test]
    fn bytea_hex_format() {
        let mut out = Vec::new();
        parse_bytea(br"\x48656c6c6f", C, &mut out).unwrap();
        assert_eq!(out, b"Hello");

        out.clear();
        parse_bytea(br"\x", C, &mut out).unwrap();
        assert!(out.is_empty());

        assert!(parse_bytea(br"\x4", C, &mut Vec::new()).is_err());
        assert!(parse_bytea(br"\xzz", C, &mut Vec::new()).is_err());
    }

    #[test]
    fn bytea_escape_format() {
        let mut out = Vec::new();
        parse_bytea(br"abc\000def", C, &mut out).unwrap();
        assert_eq!(out, b"abc\0def");

        out.clear();
        parse_bytea(br"a\\b", C, &mut out).unwrap();
        assert_eq!(out, br"a\b");

        assert!(parse_bytea(br"a\99", C, &mut Vec::new()).is_err());
        assert!(parse_bytea(br"a\z", C, &mut Vec::new()).is_err());
    }

    #[test]
    fn errors_never_carry_the_value() {
        let err = parse_i32(b"secret-customer-data", C).unwrap_err();
        let rendered = err.to_string();
        assert!(!rendered.contains("secret"), "error leaked row data: {rendered}");
        assert!(rendered.contains("col"));
    }
}
