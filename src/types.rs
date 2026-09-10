//! PostgreSQL type names to the crate's internal type model.
//!
//! Executes wherever a `CREATE TABLE` is parsed, which is the reader thread. Nothing
//! here is async and nothing here touches row data.
//!
//! This module deliberately carries no Arrow dependency. It resolves the *text* of a
//! type as `pg_dump` writes it, which is the intricate part, and `builders.rs` binds the
//! result to an Arrow `DataType`. Keeping the two apart means the parsing can be tested
//! without compiling the Delta write path.
//!
//! # Fidelity policy
//!
//! Type uncertainty degrades and never fails. An unrecognised type resolves to
//! [`PgType::Text`] with [`ResolvedType::recognised`] false, preserving the literal dump
//! text and reporting the fact in the run statistics. Across hundreds of third-party
//! tables the type zoo is wide, and one unknown type must not kill a scheduled load.
//! Nothing is lost, because text can be reinterpreted later.

/// The subset of PostgreSQL's type system this library distinguishes.
///
/// Everything not listed resolves to [`PgType::Text`]. That is a deliberate floor rather
/// than an omission: text is always a faithful representation of what the dump contained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    /// `smallint`, `int2`.
    SmallInt,
    /// `integer`, `int`, `int4`.
    Integer,
    /// `bigint`, `int8`.
    BigInt,
    /// `real`, `float4`.
    Real,
    /// `double precision`, `float8`.
    DoublePrecision,
    /// `numeric`, `decimal`, with the precision and scale the column declared.
    ///
    /// Both are `None` for an unconstrained `numeric`, which has no fixed width and
    /// therefore cannot map to a fixed-width Arrow decimal.
    Numeric {
        /// Total significant digits, when declared.
        precision: Option<u8>,
        /// Digits after the decimal point, when declared.
        scale: Option<i8>,
    },
    /// `boolean`, `bool`.
    Boolean,
    /// `date`.
    Date,
    /// `timestamp`, with or without a time zone.
    Timestamp {
        /// True for `timestamp with time zone` and `timestamptz`.
        tz: bool,
    },
    /// `time`, with or without a time zone.
    Time {
        /// True for `time with time zone` and `timetz`.
        tz: bool,
    },
    /// `bytea`.
    Bytea,
    /// Everything textual, and everything unrecognised.
    Text,
}

/// The outcome of resolving one column's declared type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedType {
    /// The type this column maps to.
    pub pg: PgType,
    /// True if the declaration carried an array suffix such as `[]` or `[3]`.
    ///
    /// Arrays are kept as their unescaped PostgreSQL literal, so an array of any element
    /// type becomes text. This avoids guessing at a nested structure the caller may not
    /// want, and the literal remains convertible downstream.
    pub is_array: bool,
    /// False when the declaration was not recognised and fell back to text.
    ///
    /// User-defined types land here by design. A dump does not distinguish an enum from
    /// a domain or a composite at the point of use, so all of them are reported rather
    /// than silently assumed to be text.
    pub recognised: bool,
    /// The declaration as written, retained for the run statistics.
    pub source: String,
}

impl ResolvedType {
    /// True if this column will be written as text, whatever its declaration said.
    ///
    /// Arrays, unconstrained `numeric`, `numeric` whose precision or scale Arrow cannot
    /// represent, and everything unrecognised all answer true.
    pub fn is_textual(&self) -> bool {
        if self.is_array || self.pg == PgType::Text {
            return true;
        }
        matches!(self.pg, PgType::Numeric { precision, scale } if !decimal_fits(precision, scale))
    }
}

/// Arrow's `Decimal128` carries at most 38 significant digits.
const MAX_DECIMAL_PRECISION: u8 = 38;

/// True if a `numeric` of this precision and scale fits a fixed-width decimal.
///
/// Arrow requires `1 <= precision <= 38` and `0 <= scale <= precision`. PostgreSQL 15 and
/// later accept declarations outside that, notably a negative scale (`numeric(5,-2)`) and
/// a scale exceeding the precision (`numeric(2,5)`), so both are checked here rather than
/// discovered later by an Arrow builder. A declaration that does not fit degrades to text,
/// which is the documented policy and keeps the literal value intact.
fn decimal_fits(precision: Option<u8>, scale: Option<i8>) -> bool {
    let Some(p) = precision else { return false };
    if !(1..=MAX_DECIMAL_PRECISION).contains(&p) {
        return false;
    }
    // An omitted scale means zero, which always fits.
    scale.is_none_or(|s| s >= 0 && i16::from(s) <= i16::from(p))
}

/// Type names that are textual and recognised as such.
///
/// `interval` is here deliberately: its PostgreSQL literal is well defined and stable,
/// and rendering it into a temporal type would require guessing at a calendar the caller
/// may not share.
const TEXTUAL: &[&str] = &[
    "text",
    "character varying",
    "varchar",
    "character",
    "char",
    "bpchar",
    "name",
    "uuid",
    "json",
    "jsonb",
    "xml",
    "inet",
    "cidr",
    "macaddr",
    "macaddr8",
    "interval",
    "money",
    "bit",
    "bit varying",
    "varbit",
    "tsvector",
    "tsquery",
    "point",
    "line",
    "lseg",
    "box",
    "path",
    "polygon",
    "circle",
    "int4range",
    "int8range",
    "numrange",
    "tsrange",
    "tstzrange",
    "daterange",
];

/// Resolves the type text of a column as `pg_dump` writes it.
///
/// Handles the forms that appear in real dumps: parameters in the middle of a name as in
/// `timestamp(3) without time zone`, parameters at the end as in `numeric(10,2)`, array
/// suffixes, and the `with`/`without time zone` variants. Matching is case-insensitive
/// and tolerant of repeated whitespace.
///
/// Never fails. An unrecognised declaration resolves to text with
/// [`ResolvedType::recognised`] false.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use pgdelta::types::{PgType, resolve};
///
/// assert_eq!(resolve("integer").pg, PgType::Integer);
/// assert_eq!(resolve("timestamp(3) without time zone").pg, PgType::Timestamp { tz: false });
/// assert!(resolve("public.my_enum").is_textual());
/// assert!(resolve("integer[]").is_array);
/// ```
pub fn resolve(sql_type: &str) -> ResolvedType {
    let source = sql_type.trim().to_string();
    let (base, is_array) = strip_array_suffix(&source);
    let (name, params) = split_params(base);
    let name = normalise(name);

    let pg = match name.as_str() {
        "smallint" | "int2" | "smallserial" | "serial2" => PgType::SmallInt,
        "integer" | "int" | "int4" | "serial" | "serial4" => PgType::Integer,
        "bigint" | "int8" | "bigserial" | "serial8" => PgType::BigInt,
        "real" | "float4" => PgType::Real,
        "double precision" | "float8" => PgType::DoublePrecision,
        "numeric" | "decimal" => {
            let (precision, scale) = numeric_params(&params);
            PgType::Numeric { precision, scale }
        }
        "boolean" | "bool" => PgType::Boolean,
        "date" => PgType::Date,
        "timestamp" | "timestamp without time zone" => PgType::Timestamp { tz: false },
        "timestamptz" | "timestamp with time zone" => PgType::Timestamp { tz: true },
        "time" | "time without time zone" => PgType::Time { tz: false },
        "timetz" | "time with time zone" => PgType::Time { tz: true },
        "bytea" => PgType::Bytea,
        other => {
            let recognised = TEXTUAL.contains(&other);
            return ResolvedType {
                pg: PgType::Text,
                is_array,
                recognised,
                source,
            };
        }
    };

    ResolvedType {
        pg,
        is_array,
        recognised: true,
        source,
    }
}

/// Removes trailing array suffixes such as `[]`, `[3]`, or `[][]`.
fn strip_array_suffix(s: &str) -> (&str, bool) {
    let mut end = s.trim_end();
    let mut found = false;
    while end.ends_with(']') {
        let Some(open) = end.rfind('[') else { break };
        // Only a dimension suffix qualifies; anything else is left alone.
        if !end[open + 1..end.len() - 1]
            .chars()
            .all(|c| c.is_ascii_digit())
        {
            break;
        }
        end = end[..open].trim_end();
        found = true;
    }
    (end, found)
}

/// Splits the first parenthesised parameter list out of a type name.
///
/// `timestamp(3) without time zone` yields `timestamp without time zone` and `3`, so the
/// parameters can be read without disturbing a multi-word name.
fn split_params(s: &str) -> (String, String) {
    let Some(open) = s.find('(') else {
        return (s.to_string(), String::new());
    };
    let Some(close) = s[open..].find(')').map(|i| open + i) else {
        return (s.to_string(), String::new());
    };
    let params = s[open + 1..close].to_string();
    let mut name = String::with_capacity(s.len());
    name.push_str(&s[..open]);
    name.push(' ');
    name.push_str(&s[close + 1..]);
    (name, params)
}

/// Lowercases and collapses runs of whitespace to single spaces.
fn normalise(s: String) -> String {
    let mut out = String::with_capacity(s.len());
    let mut space = false;
    for c in s.trim().chars() {
        if c.is_whitespace() {
            space = true;
            continue;
        }
        if space && !out.is_empty() {
            out.push(' ');
        }
        space = false;
        out.extend(c.to_lowercase());
    }
    out
}

/// Reads the precision and scale from a `numeric` parameter list.
///
/// Parsing is checked; a value that does not fit is discarded rather than wrapped, which
/// causes the column to fall back to text instead of silently changing width.
fn numeric_params(params: &str) -> (Option<u8>, Option<i8>) {
    let mut parts = params.split(',');
    let precision = parts.next().and_then(|p| p.trim().parse::<u8>().ok());
    let scale = parts.next().and_then(|s| s.trim().parse::<i8>().ok());
    match precision {
        // A declared numeric always has a scale, defaulting to zero.
        Some(_) => (precision, Some(scale.unwrap_or(0))),
        None => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg(s: &str) -> PgType {
        resolve(s).pg
    }

    #[test]
    fn integer_family() {
        assert_eq!(pg("smallint"), PgType::SmallInt);
        assert_eq!(pg("integer"), PgType::Integer);
        assert_eq!(pg("bigint"), PgType::BigInt);
        assert_eq!(pg("int2"), PgType::SmallInt);
        assert_eq!(pg("int4"), PgType::Integer);
        assert_eq!(pg("int8"), PgType::BigInt);
    }

    #[test]
    fn serial_resolves_to_its_storage_type() {
        // pg_dump writes the storage type plus a sequence default, but a hand-written
        // schema may still say serial.
        assert_eq!(pg("serial"), PgType::Integer);
        assert_eq!(pg("bigserial"), PgType::BigInt);
    }

    #[test]
    fn float_family() {
        assert_eq!(pg("real"), PgType::Real);
        assert_eq!(pg("double precision"), PgType::DoublePrecision);
        assert_eq!(pg("float8"), PgType::DoublePrecision);
    }

    #[test]
    fn numeric_carries_precision_and_scale() {
        assert_eq!(
            pg("numeric(10,2)"),
            PgType::Numeric {
                precision: Some(10),
                scale: Some(2)
            }
        );
        // A declared precision without a scale means scale zero.
        assert_eq!(
            pg("numeric(10)"),
            PgType::Numeric {
                precision: Some(10),
                scale: Some(0)
            }
        );
        assert_eq!(
            pg("numeric"),
            PgType::Numeric {
                precision: None,
                scale: None
            }
        );
        assert_eq!(pg("decimal(5,3)"), pg("numeric(5,3)"));
    }

    #[test]
    fn wide_or_unconstrained_numeric_becomes_text() {
        assert!(resolve("numeric").is_textual());
        assert!(resolve("numeric(39,2)").is_textual());
        assert!(!resolve("numeric(38,2)").is_textual());
        assert!(!resolve("numeric(1,0)").is_textual());
    }

    /// PostgreSQL 15 and later accept a scale outside `0..=precision`, which Arrow's
    /// `Decimal128` cannot express. Such a column must be routed to text here, or the
    /// builder and the schema disagree and the whole load dies on the first flush.
    #[test]
    fn numeric_scales_arrow_cannot_hold_become_text() {
        assert!(
            resolve("numeric(2,5)").is_textual(),
            "scale exceeding precision is legal in PG15+ and must degrade"
        );
        assert!(
            resolve("numeric(5,-2)").is_textual(),
            "negative scale is legal in PG15+ and must degrade"
        );
        assert!(!resolve("numeric(5,5)").is_textual(), "scale == precision fits");
        assert!(!resolve("numeric(5,0)").is_textual());
    }

    #[test]
    fn temporal_types_and_their_zones() {
        assert_eq!(pg("date"), PgType::Date);
        assert_eq!(pg("timestamp without time zone"), PgType::Timestamp { tz: false });
        assert_eq!(pg("timestamp with time zone"), PgType::Timestamp { tz: true });
        assert_eq!(pg("timestamptz"), PgType::Timestamp { tz: true });
        assert_eq!(pg("time without time zone"), PgType::Time { tz: false });
        assert_eq!(pg("time with time zone"), PgType::Time { tz: true });
    }

    #[test]
    fn parameters_in_the_middle_of_a_name() {
        assert_eq!(pg("timestamp(3) without time zone"), PgType::Timestamp { tz: false });
        assert_eq!(pg("timestamp(6) with time zone"), PgType::Timestamp { tz: true });
        assert_eq!(pg("time(0) without time zone"), PgType::Time { tz: false });
    }

    #[test]
    fn textual_types_are_recognised() {
        for t in [
            "text",
            "character varying(255)",
            "character varying",
            "character(10)",
            "uuid",
            "json",
            "jsonb",
            "inet",
            "interval",
        ] {
            let r = resolve(t);
            assert_eq!(r.pg, PgType::Text, "{t}");
            assert!(r.recognised, "{t} should be a known textual type");
        }
    }

    #[test]
    fn unknown_types_degrade_and_are_reported() {
        let r = resolve("public.order_status");
        assert_eq!(r.pg, PgType::Text);
        assert!(!r.recognised, "user-defined types must be reported");
        assert_eq!(r.source, "public.order_status");
    }

    #[test]
    fn arrays_become_text_whatever_the_element() {
        for t in ["integer[]", "text[]", "numeric(10,2)[]", "integer[3]", "integer[][]"] {
            let r = resolve(t);
            assert!(r.is_array, "{t}");
            assert!(r.is_textual(), "{t} must be written as text");
        }
        // The element type is still resolved, which keeps the stats informative.
        assert_eq!(resolve("integer[]").pg, PgType::Integer);
    }

    #[test]
    fn a_bracket_that_is_not_a_dimension_is_left_alone() {
        let r = resolve("weird[abc]");
        assert!(!r.is_array);
        assert!(!r.recognised);
    }

    #[test]
    fn case_and_whitespace_are_tolerated() {
        assert_eq!(pg("INTEGER"), PgType::Integer);
        assert_eq!(pg("  Double   Precision "), PgType::DoublePrecision);
        assert_eq!(pg("TIMESTAMP  WITHOUT  TIME  ZONE"), PgType::Timestamp { tz: false });
    }

    #[test]
    fn bytea_is_binary() {
        assert_eq!(pg("bytea"), PgType::Bytea);
        assert!(!resolve("bytea").is_textual());
    }

    #[test]
    fn overwide_numeric_parameters_do_not_wrap() {
        // 300 does not fit u8; the column must fall back to text rather than become
        // numeric(44) through silent truncation.
        let r = resolve("numeric(300,2)");
        assert!(r.is_textual());
    }

    #[test]
    fn source_text_is_retained_for_statistics() {
        assert_eq!(resolve(" Numeric(10,2) ").source, "Numeric(10,2)");
    }
}
