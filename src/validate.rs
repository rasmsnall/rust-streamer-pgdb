//! Validates a dump the way [`crate::pipeline::run`] would, without writing anything.
//!
//! Executes entirely on the calling thread: no Tokio runtime, no worker pool, no Arrow,
//! no Delta, no object store. Decoding was already measured as a distant-last bottleneck
//! even on a single core (see `docs/architecture.md`, Chapter IV, Section 3), so a
//! validator has no reason to carry the concurrency machinery that exists only to
//! overlap Parquet encoding and Delta commits with decode.
//!
//! # What this catches
//!
//! Everything a real load would fail on before it ever opens a Delta table: an
//! unterminated `COPY` block, a row whose field count disagrees with its header, a value
//! that contradicts its declared type, non-UTF-8 text, a `numeric` too wide for its
//! column, an unqualified or mismatched `pg_dump` major, and a table name that would
//! escape the output prefix ([`crate::sink::relative_path`]) or collide with the load
//! history's own reserved name ([`crate::manifest::reserved_collision`]). `infinity`,
//! `NaN`, and an unrecognised type are reported, not failed, exactly as a real load
//! reports them.
//!
//! # What this does not catch
//!
//! Schema drift against an existing Delta table: there is no target here to compare
//! against, so this cannot know whether a run would collide with one already at
//! `output_uri`. Nor can it know whether the object store itself is reachable, since none
//! is opened. Both are genuine gaps, not oversights; run [`crate::pipeline::run`] itself
//! to learn either.
//!
//! # Reuse, not reimplementation
//!
//! [`crate::types`] and [`crate::values`] are already free of any Arrow or Delta
//! dependency (see their own module docs), so this module drives them directly:
//! [`crate::scan::Scanner`] for DDL and `COPY` boundaries, [`crate::copy::rows`] and
//! [`crate::copy::decode_row`] for the same row and field splitting the real decode pool
//! uses, and [`crate::values`]'s `parse_*` functions for the same fidelity rules
//! (`infinity`/`NaN` degrade, a contradicting value fails). This module's own private
//! `validate_field` dispatches from a resolved type to the matching `parse_*` call, and
//! necessarily mirrors `builders::ColumnBuilder::append`'s own dispatch: the one piece of
//! logic this module cannot literally share with it, since that dispatch is expressed on
//! Arrow-typed builder variants. This is the same kind of duplication the codebase
//! already tolerates between `builders::arrow_type` and `builders::ColumnBuilder::new`,
//! and is guarded the same way elsewhere in this crate: by a differential test
//! (`tests/validate_matches_load.rs`) asserting this module and the real load agree on a
//! shared set of fixtures, rather than by a shared dispatch table.
//!
//! `native_arrays` eligibility is shared exactly, not mirrored: `builders::as_element` and
//! `builders::array_element_type` (`pub(crate)`) are the same functions
//! [`crate::builders::ColumnBuilder::new`] uses, so this module cannot disagree with the
//! real load about which one-dimensional arrays qualify.

use std::collections::{BTreeSet, HashMap};
use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::builders;
use crate::copy::{self, Limits};
use crate::dump::{ChunkReader, decompressed};
use crate::error::{Error, Result};
use crate::manifest;
use crate::pipeline::{CountingReader, Progress};
use crate::scan::{Event, Scanner, TableDef, TableName};
use crate::sink;
use crate::types::{self, PgType, ResolvedType};
use crate::values;

/// Settings for one validation run.
///
/// A trimmed [`crate::pipeline::LoadConfig`]: everything about decoding and type
/// resolution, nothing about where or how anything would be written, since nothing is.
#[derive(Debug, Clone, Default)]
pub struct ValidateConfig {
    /// Qualified names to validate. `None` validates every table in the dump; a `COPY`
    /// block for any other table is scanned for its terminator and otherwise skipped,
    /// exactly as [`crate::pipeline::LoadConfig::tables`] documents.
    pub tables: Option<Vec<String>>,
    /// PostgreSQL schema (namespace) names to exclude entirely. See
    /// [`crate::pipeline::LoadConfig::excluded_schemas`]: an excluded table's `CREATE
    /// TABLE` is recognised only well enough to find its end, so a DDL construct this
    /// crate cannot parse, in a schema nobody wants, cannot fail validation either.
    pub excluded_schemas: Option<Vec<String>>,
    /// Validate a too-wide `numeric` against `decimal(38,18)` instead of the default text
    /// fallback. See [`crate::pipeline::LoadConfig::wide_numeric_as_decimal`].
    pub wide_numeric_as_decimal: bool,
    /// Validate a one-dimensional array's elements against their element type instead of
    /// treating the array as opaque text. See
    /// [`crate::pipeline::LoadConfig::native_arrays`].
    pub native_arrays: bool,
    /// When set, validation fails unless the dump's `pg_dump` major matches exactly.
    pub expect_pg_major: Option<u32>,
    /// Qualified names this dump is expected to contain. Purely a report; see
    /// [`ValidationReport::missing_tables`] and [`ValidationReport::unexpected_tables`].
    pub expect_tables: Option<Vec<String>>,
    /// Bounds enforced on every field, row and column while decoding. See
    /// [`crate::pipeline::LoadConfig::limits`].
    pub limits: Limits,
    /// The prefix a real load would write beneath, checked with
    /// [`crate::sink::reject_managed_storage`] if given. Purely a string check: no
    /// object store is opened. `None` skips the check, since a validation run need not
    /// know its eventual target.
    pub output_uri: Option<String>,
}

/// What one source table's validation produced.
///
/// A trimmed [`crate::pipeline::TableStats`]: no `batches` or `delta_version`, since
/// nothing is written.
#[derive(Debug, Clone)]
pub struct TableValidation {
    /// Qualified name as written in the dump.
    pub table: String,
    /// Rows decoded from the table's `COPY` block.
    pub rows: u64,
    /// Columns where a valid but unrepresentable value (`infinity`, `NaN`) would be
    /// stored as NULL, with the count per column. Empty when nothing was substituted.
    pub null_substitutions: Vec<(String, u64)>,
    /// Columns whose declared type was not recognised and would be written as text,
    /// paired with the declaration as the dump wrote it. Empty when every type was
    /// recognised.
    pub text_fallback_columns: Vec<(String, String)>,
}

/// The outcome of a completed validation run.
///
/// A trimmed [`crate::pipeline::LoadReport`]: no `load_id`, since nothing is written to a
/// load history.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// `pg_dump` major version that wrote the dump, from `-- Dumped by pg_dump version`.
    pub dumped_by: u32,
    /// Source server major version, from `-- Dumped from database version`, when stated.
    pub from_database: Option<u32>,
    /// Compression detected on the input, or `"none"`.
    pub compression: &'static str,
    /// Bytes taken from the input, counted before decompression.
    pub bytes_read: u64,
    /// Rows decoded across every validated table.
    pub total_rows: u64,
    /// One entry per validated table, in the order their blocks closed.
    pub tables: Vec<TableValidation>,
    /// Expected names the dump did not contain, sorted. Empty when
    /// [`ValidateConfig::expect_tables`] and [`ValidateConfig::tables`] are both `None`.
    pub missing_tables: Vec<String>,
    /// Names the dump contained that [`ValidateConfig::expect_tables`] did not list,
    /// sorted. Always empty when `expect_tables` is `None`.
    pub unexpected_tables: Vec<String>,
}

/// A `COPY` block's validation state while it is open.
struct OpenBlock {
    table: TableName,
    qualified: String,
    columns: Vec<(String, ResolvedType)>,
    arity: usize,
    /// Per-column count of values that would be stored as NULL because the type cannot
    /// hold them. Same length and order as `columns`.
    substitutions: Vec<u64>,
    rows: u64,
}

/// Validates `input` and returns what a real load against it would report.
///
/// The dump is consumed in a single forward pass on the calling thread. `progress` is
/// called after each chunk and after each `COPY` block, exactly as
/// [`crate::pipeline::run`] calls it; returning `false` aborts with
/// [`Error::Interrupted`]. Pass `|_| true` to ignore it.
///
/// # Errors
///
/// Every error [`crate::pipeline::run`] can raise before it opens a Delta table:
/// [`Error::UnsafeTableName`], [`Error::UnsupportedDumpVersion`],
/// [`Error::MissingDumpVersion`], [`Error::UnterminatedCopy`],
/// [`Error::FieldCountMismatch`], [`Error::TruncatedEscape`],
/// [`Error::InvalidHexEscape`], [`Error::UnparsableValue`], [`Error::NonUtf8Text`],
/// [`Error::FieldTooLarge`], [`Error::RowTooLarge`], [`Error::TooManyColumns`],
/// [`Error::MalformedCopyHeader`], [`Error::MalformedCreateTable`],
/// [`Error::UnsupportedCompression`], [`Error::ManagedTableTarget`] (when
/// [`ValidateConfig::output_uri`] is set), [`Error::Io`], and [`Error::Interrupted`].
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks until validation finishes. Synchronous throughout: no Tokio runtime exists and
/// none is required.
///
/// # Examples
///
/// ```no_run
/// use pgdelta::validate::{validate, ValidateConfig};
///
/// let file = std::fs::File::open("day.sql")?;
/// let report = validate(std::io::BufReader::new(file), &ValidateConfig::default(), |_| true)?;
/// println!("{} tables, {} rows, would validate cleanly", report.tables.len(), report.total_rows);
/// # Ok::<(), pgdelta::Error>(())
/// ```
pub fn validate<R, F>(
    input: R,
    config: &ValidateConfig,
    mut progress: F,
) -> Result<ValidationReport>
where
    R: Read + 'static,
    F: FnMut(Progress) -> bool,
{
    if let Some(uri) = &config.output_uri {
        sink::reject_managed_storage(uri)?;
    }

    let consumed = Arc::new(AtomicU64::new(0));
    let counted = CountingReader {
        inner: input,
        count: Arc::clone(&consumed),
    };
    let (compression, reader) = decompressed(counted)?;

    // Mirrors pipeline::drive: a chunk may only outgrow the target when a single line
    // does, so the row ceiling is what should bound it.
    let max_chunk = config
        .limits
        .max_row_bytes
        .max(crate::dump::DEFAULT_CHUNK_BYTES);
    let mut chunks = ChunkReader::with_limits(reader, crate::dump::DEFAULT_CHUNK_BYTES, max_chunk);
    let mut scanner = Scanner::new();
    if let Some(schemas) = &config.excluded_schemas {
        scanner.set_excluded_schemas(schemas.iter().cloned());
    }

    let mut seen_tables: BTreeSet<String> = BTreeSet::new();
    let mut table_defs: HashMap<String, TableDef> = HashMap::new();
    let mut dumped_by: Option<u32> = None;
    let mut from_database: Option<u32> = None;
    let mut total_rows: u64 = 0;
    let mut tables: Vec<TableValidation> = Vec::new();

    let mut current: Option<OpenBlock> = None;
    let mut skipping = false;
    let mut scratch: Vec<u8> = Vec::new();
    let mut ranges: Vec<Option<std::ops::Range<usize>>> = Vec::new();

    let allowed = |name: &str| {
        config
            .tables
            .as_deref()
            .is_none_or(|t| t.iter().any(|x| x == name))
    };
    let schema_excluded = |table: &TableName| {
        config.excluded_schemas.as_deref().is_some_and(|schemas| {
            table
                .schema
                .as_deref()
                .is_some_and(|s| schemas.iter().any(|x| x == s))
        })
    };
    let wanted = |table: &TableName, qualified: &str| allowed(qualified) && !schema_excluded(table);

    while let Some(chunk) = chunks.next_chunk()? {
        let bytes_read = consumed.load(Ordering::Relaxed);

        for event in scanner.feed(chunk)? {
            match event {
                Event::DumpVersion {
                    dumped_by: by,
                    from_database: from,
                } => {
                    if let Some(expected) = config.expect_pg_major
                        && by != expected
                    {
                        return Err(Error::UnsupportedDumpVersion { found: by });
                    }
                    dumped_by = Some(by);
                    from_database = from;
                }

                Event::Table(def) => {
                    table_defs.insert(def.name.qualified(), def);
                }

                Event::CopyStart { table, columns } => {
                    if current.is_some() {
                        return Err(Error::Internal {
                            detail: "COPY block started while another was still open",
                        });
                    }
                    if manifest::reserved_collision(&table) {
                        return Err(Error::UnsafeTableName {
                            name: table.qualified(),
                        });
                    }
                    let qualified = table.qualified();
                    seen_tables.insert(qualified.clone());
                    if !wanted(&table, &qualified) {
                        skipping = true;
                        continue;
                    }
                    let def =
                        table_defs
                            .get(&qualified)
                            .ok_or_else(|| Error::MalformedCreateTable {
                                table: qualified.clone(),
                            })?;
                    let resolved = types::resolve_copy_columns(def, &columns)?;
                    let pairs: Vec<(String, ResolvedType)> =
                        columns.iter().cloned().zip(resolved).collect();
                    // The same path-traversal guard a real load applies once it is ready
                    // to open the table (inside `sink::open_table`); checked here after
                    // DDL resolution, in the same order pipeline::run reaches it, so a
                    // COPY block for a table never declared still fails with
                    // MalformedCreateTable rather than this check pre-empting it.
                    sink::relative_path(&table)?;
                    current = Some(OpenBlock {
                        table,
                        qualified,
                        arity: columns.len(),
                        substitutions: vec![0u64; pairs.len()],
                        rows: 0,
                        columns: pairs,
                    });
                }

                Event::CopyRows(payload) => {
                    if skipping {
                        continue;
                    }
                    let Some(block) = current.as_mut() else {
                        return Err(Error::Internal {
                            detail: "COPY rows arrived with no open block",
                        });
                    };
                    for row in copy::rows(&payload) {
                        copy::decode_row(
                            row,
                            block.arity,
                            config.limits,
                            &mut scratch,
                            &mut ranges,
                        )?;
                        for (i, (name, rt)) in block.columns.iter().enumerate() {
                            let value = ranges[i].as_ref().map(|r| &scratch[r.start..r.end]);
                            if validate_field(
                                rt,
                                value,
                                name,
                                config.wide_numeric_as_decimal,
                                config.native_arrays,
                            )? {
                                block.substitutions[i] += 1;
                            }
                        }
                        block.rows += 1;
                    }
                }

                Event::CopyEnd { table } => {
                    if skipping {
                        skipping = false;
                        continue;
                    }
                    let Some(OpenBlock {
                        table: opened_table,
                        qualified,
                        columns,
                        substitutions,
                        rows,
                        ..
                    }) = current.take()
                    else {
                        return Err(Error::Internal {
                            detail: "COPY block ended with none open",
                        });
                    };
                    if opened_table != table {
                        return Err(Error::Internal {
                            detail: "COPY block ended under a different name than it began",
                        });
                    }

                    total_rows += rows;
                    let null_substitutions = columns
                        .iter()
                        .zip(&substitutions)
                        .filter(|&(_, &count)| count > 0)
                        .map(|((n, _), &count)| (n.clone(), count))
                        .collect();
                    let text_fallback_columns = columns
                        .iter()
                        .filter(|(_, rt)| !rt.recognised)
                        .map(|(n, rt)| (n.clone(), rt.source.clone()))
                        .collect();
                    tables.push(TableValidation {
                        table: qualified,
                        rows,
                        null_substitutions,
                        text_fallback_columns,
                    });

                    if !progress(Progress {
                        bytes_read,
                        rows: total_rows,
                        tables_done: tables.len(),
                        table: Some(table.qualified()),
                    }) {
                        return Err(Error::Interrupted);
                    }
                }
            }
        }

        if !progress(Progress {
            bytes_read,
            rows: total_rows,
            tables_done: tables.len(),
            table: None,
        }) {
            return Err(Error::Interrupted);
        }
    }

    scanner.finish()?;
    let dumped_by = dumped_by.ok_or(Error::MissingDumpVersion)?;

    let expectation = config.expect_tables.as_ref().or(config.tables.as_ref());
    let missing_tables: Vec<String> = expectation
        .map(|names| {
            let mut absent: Vec<String> = names
                .iter()
                .filter(|name| !seen_tables.contains(*name))
                .cloned()
                .collect();
            absent.sort();
            absent.dedup();
            absent
        })
        .unwrap_or_default();
    let unexpected_tables: Vec<String> = config
        .expect_tables
        .as_ref()
        .map(|names| {
            seen_tables
                .iter()
                .filter(|seen| !names.contains(*seen))
                .cloned()
                .collect()
        })
        .unwrap_or_default();

    Ok(ValidationReport {
        dumped_by,
        from_database,
        compression: compression.name(),
        bytes_read: consumed.load(Ordering::Relaxed),
        total_rows,
        tables,
        missing_tables,
        unexpected_tables,
    })
}

/// Opens `path` and validates it. See [`validate`].
///
/// # Errors
///
/// [`Error::Io`] if `path` cannot be opened, plus every error [`validate`] can return.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks until validation finishes; see [`validate`].
///
/// # Examples
///
/// ```no_run
/// use pgdelta::validate::{validate_file, ValidateConfig};
///
/// let report = validate_file(std::path::Path::new("day.sql"), &ValidateConfig::default(), |_| true)?;
/// # let _ = report;
/// # Ok::<(), pgdelta::Error>(())
/// ```
pub fn validate_file<F>(
    path: &Path,
    config: &ValidateConfig,
    progress: F,
) -> Result<ValidationReport>
where
    F: FnMut(Progress) -> bool,
{
    let file = std::fs::File::open(path)?;
    validate(std::io::BufReader::new(file), config, progress)
}

/// Validates one field's decoded bytes against its resolved type, without building any
/// Arrow value.
///
/// Mirrors [`crate::builders::ColumnBuilder::append`]'s dispatch exactly (same order:
/// array eligibility, then a too-wide `numeric` opted into `wide_numeric_as_decimal`,
/// then [`ResolvedType::is_textual`], then the scalar types), calling the same
/// [`crate::values`] parsers, so a dump this function accepts is a dump
/// [`crate::pipeline::run`] would accept. See the module documentation for how the two
/// are kept from disagreeing.
///
/// Returns whether a non-null input was one its type cannot represent and was therefore
/// counted as a substitution (`infinity`, `NaN`), the same signal
/// [`crate::builders::ColumnBuilder::append`] returns.
///
/// # Errors
///
/// [`Error::UnparsableValue`] if the value contradicts `rt`, and [`Error::NonUtf8Text`]
/// for a text column, or an array falling back to text, carrying non-UTF-8 bytes.
///
/// # Panics
///
/// Does not panic.
fn validate_field(
    rt: &ResolvedType,
    value: Option<&[u8]>,
    column: &str,
    wide_numeric_as_decimal: bool,
    native_arrays: bool,
) -> Result<bool> {
    let Some(v) = value else { return Ok(false) };

    if rt.is_array {
        if native_arrays && rt.array_dimensions == 1 {
            let element_rt = builders::as_element(rt);
            if builders::array_element_type(&element_rt, wide_numeric_as_decimal).is_some() {
                let elements = values::parse_array_elements(v, column)?;
                let mut substituted = false;
                for elem in &elements {
                    substituted |= validate_field(
                        &element_rt,
                        elem.as_deref(),
                        column,
                        wide_numeric_as_decimal,
                        native_arrays,
                    )?;
                }
                return Ok(substituted);
            }
        }
        // Not eligible for a native list: a real load keeps the field as opaque text,
        // the unescaped `{...}` literal, validated only as UTF-8 rather than parsed as
        // an array literal. Parsing it here would reject a malformed array literal that
        // a real load would silently accept as text.
        std::str::from_utf8(v).map_err(|_| Error::NonUtf8Text {
            column: column.to_string(),
        })?;
        return Ok(false);
    }

    if let PgType::Numeric { precision, .. } = rt.pg
        && wide_numeric_as_decimal
        && types::numeric_too_wide(precision)
    {
        let parsed = values::parse_decimal(
            v,
            types::WIDE_NUMERIC_PRECISION,
            types::WIDE_NUMERIC_SCALE,
            column,
        )?;
        return Ok(parsed.is_none());
    }

    if rt.is_textual() {
        std::str::from_utf8(v).map_err(|_| Error::NonUtf8Text {
            column: column.to_string(),
        })?;
        return Ok(false);
    }

    Ok(match rt.pg {
        PgType::SmallInt => {
            values::parse_i16(v, column)?;
            false
        }
        PgType::Integer => {
            values::parse_i32(v, column)?;
            false
        }
        PgType::BigInt => {
            values::parse_i64(v, column)?;
            false
        }
        PgType::Real => {
            values::parse_f32(v, column)?;
            false
        }
        PgType::DoublePrecision => {
            values::parse_f64(v, column)?;
            false
        }
        PgType::Numeric { precision, scale } => {
            let p = precision.unwrap_or(38);
            let s = scale.unwrap_or(0);
            values::parse_decimal(v, p, s, column)?.is_none()
        }
        PgType::Boolean => {
            values::parse_bool(v, column)?;
            false
        }
        PgType::Date => values::parse_date(v, column)?.is_none(),
        PgType::Timestamp { tz } => values::parse_timestamp(v, tz, column)?.is_none(),
        PgType::Bytea => {
            values::parse_bytea(v, column, &mut Vec::new())?;
            false
        }
        // is_textual() already returned above for PgType::Text and PgType::Time: Delta
        // has no time-of-day type, so `time` is kept as literal text like any other
        // textual column, and validated the same way (UTF-8 only). See builders::arrow_type.
        PgType::Text | PgType::Time { .. } => false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn dump(body: &str) -> Vec<u8> {
        let mut d = String::from("-- Dumped from database version 17.4\n");
        d.push_str("-- Dumped by pg_dump version 17.4\n");
        d.push_str(body);
        d.into_bytes()
    }

    #[test]
    fn a_clean_dump_validates() {
        let body = "\
CREATE TABLE public.users (
    id integer,
    name text
);
COPY public.users (id, name) FROM stdin;
1\talice
2\tbob
3\t\\N
\\.
";
        let report = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap();
        assert_eq!(report.dumped_by, 17);
        assert_eq!(report.total_rows, 3);
        assert_eq!(report.tables.len(), 1);
        assert_eq!(report.tables[0].table, "public.users");
        assert!(report.tables[0].null_substitutions.is_empty());
        assert!(report.tables[0].text_fallback_columns.is_empty());
    }

    #[test]
    fn a_value_contradicting_its_type_fails() {
        let body = "\
CREATE TABLE public.t (
    id integer
);
COPY public.t (id) FROM stdin;
not_a_number
\\.
";
        let err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap_err();
        assert_eq!(
            err,
            Error::UnparsableValue {
                column: "id".to_string(),
                expected: "integer"
            }
        );
    }

    #[test]
    fn non_utf8_text_fails() {
        let mut d =
            dump("CREATE TABLE public.t (\n    name text\n);\nCOPY public.t (name) FROM stdin;\n");
        d.extend_from_slice(b"\xff\xfe\n");
        d.extend_from_slice(br"\.");
        d.push(b'\n');
        let err = validate(Cursor::new(d), &ValidateConfig::default(), |_| true).unwrap_err();
        assert_eq!(
            err,
            Error::NonUtf8Text {
                column: "name".to_string()
            }
        );
    }

    #[test]
    fn infinity_and_nan_are_counted_not_failed() {
        let body = "\
CREATE TABLE public.t (
    d date,
    n numeric(10,2)
);
COPY public.t (d, n) FROM stdin;
infinity\tNaN
\\.
";
        let report = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap();
        assert_eq!(
            report.tables[0].null_substitutions,
            vec![("d".to_string(), 1), ("n".to_string(), 1)]
        );
    }

    #[test]
    fn an_unrecognised_type_is_reported_not_failed() {
        let body = "\
CREATE TABLE public.t (
    k public.mystery_enum
);
COPY public.t (k) FROM stdin;
whatever
\\.
";
        let report = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap();
        assert_eq!(
            report.tables[0].text_fallback_columns,
            vec![("k".to_string(), "public.mystery_enum".to_string())]
        );
    }

    #[test]
    fn an_unsafe_table_name_is_rejected() {
        let body = "\
CREATE TABLE public.\"../escape\" (
    id integer
);
COPY public.\"../escape\" (id) FROM stdin;
1
\\.
";
        let err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        });
        assert!(matches!(err, Err(Error::UnsafeTableName { .. })));
    }

    #[test]
    fn a_manifest_name_collision_is_rejected() {
        let body = "\
CREATE TABLE public.t (id integer);
COPY _pgdelta_loads (id) FROM stdin;
1
\\.
";
        let err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap_err();
        assert_eq!(
            err,
            Error::UnsafeTableName {
                name: "_pgdelta_loads".to_string()
            }
        );
    }

    #[test]
    fn managed_storage_target_is_rejected_when_given() {
        let body = "CREATE TABLE public.t (id integer);\n";
        let config = ValidateConfig {
            output_uri: Some("dbfs:/user/hive/warehouse/db.db/t".to_string()),
            ..Default::default()
        };
        let err = validate(Cursor::new(dump(body)), &config, |_| true);
        assert!(matches!(err, Err(Error::ManagedTableTarget { .. })));
    }

    #[test]
    fn an_unqualified_major_is_rejected() {
        let body = "-- Dumped by pg_dump version 18.0\n";
        let err = validate(
            Cursor::new(body.as_bytes().to_vec()),
            &ValidateConfig::default(),
            |_| true,
        )
        .unwrap_err();
        assert_eq!(err, Error::UnsupportedDumpVersion { found: 18 });
    }

    #[test]
    fn an_unterminated_block_is_rejected() {
        let body = "\
CREATE TABLE public.t (id integer);
COPY public.t (id) FROM stdin;
1
2
";
        let err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap_err();
        assert_eq!(
            err,
            Error::UnterminatedCopy {
                table: "public.t".to_string()
            }
        );
    }

    #[test]
    fn table_filter_skips_unwanted_blocks_without_decoding_them() {
        let body = "\
CREATE TABLE public.users (id integer);
CREATE TABLE public.broken (id integer);
COPY public.users (id) FROM stdin;
1
\\.
COPY public.broken (id) FROM stdin;
not_a_number
\\.
";
        let config = ValidateConfig {
            tables: Some(vec!["public.users".to_string()]),
            ..Default::default()
        };
        let report = validate(Cursor::new(dump(body)), &config, |_| true).unwrap();
        assert_eq!(report.tables.len(), 1);
        assert_eq!(report.tables[0].table, "public.users");
    }

    #[test]
    fn a_native_array_element_error_is_reported_only_when_opted_in() {
        let body = "\
CREATE TABLE public.t (
    xs integer[]
);
COPY public.t (xs) FROM stdin;
{1,not_a_number,3}
\\.
";
        // Off by default: the field is opaque text, validated only as UTF-8.
        let report = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            true
        })
        .unwrap();
        assert_eq!(report.total_rows, 1);

        // Opted in: the element is parsed against its own type and fails.
        let config = ValidateConfig {
            native_arrays: true,
            ..Default::default()
        };
        let err = validate(Cursor::new(dump(body)), &config, |_| true).unwrap_err();
        assert_eq!(
            err,
            Error::UnparsableValue {
                column: "xs".to_string(),
                expected: "integer"
            }
        );
    }

    #[test]
    fn expect_tables_reports_a_table_that_stopped_arriving() {
        let body = "\
CREATE TABLE public.t (id integer);
COPY public.t (id) FROM stdin;
1
\\.
";
        let config = ValidateConfig {
            expect_tables: Some(vec!["public.t".to_string(), "public.gone".to_string()]),
            ..Default::default()
        };
        let report = validate(Cursor::new(dump(body)), &config, |_| true).unwrap();
        assert_eq!(report.missing_tables, vec!["public.gone".to_string()]);
    }

    #[test]
    fn a_callback_returning_false_interrupts() {
        let body = "\
CREATE TABLE public.t (id integer);
COPY public.t (id) FROM stdin;
1
\\.
";
        let err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
            false
        })
        .unwrap_err();
        assert_eq!(err, Error::Interrupted);
    }
}
