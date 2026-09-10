//! A durable record of every load, and the versions that make one readable as a snapshot.
//!
//! Executes on the reader thread after phase two, on the Tokio runtime. Nothing here is
//! on the hot path: it writes one row per table per run, so a few hundred rows a day.
//!
//! # Why versions rather than a pointer
//!
//! Delta has no cross-table transaction, so the commit burst is not atomic and a reader
//! during it can see a mixture of two runs (see [`crate::sink`]). The manifest does not
//! fix that, and does not pretend to. What it does is record the exact Delta version each
//! table reached in a given run, which makes a consistent set **readable after the fact**:
//!
//! ```sql
//! SELECT * FROM delta.`.../public/users` VERSION AS OF 41
//! ```
//!
//! with 41 taken from the manifest row for that load. Every table read at its recorded
//! version is by construction the set one run produced, whatever has happened since.
//!
//! The alternative, writing each run under a new identifier and flipping a pointer, gives
//! atomicity to live readers too, but forces every downstream query through a resolving
//! view and keeps several runs on disk. This shape costs nothing at read time for callers
//! who do not need consistency, and is available to those who do.
//!
//! # What it is not
//!
//! It is not a lock and not a transaction. Nothing prevents a later run from overwriting
//! a table whose version the manifest still names; that is what `VACUUM` retention governs,
//! and time travel stops working once the files are gone. See `operations.md`, Chapter IV.

use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMicrosecondArray,
};
use deltalake::arrow::datatypes::{DataType, Field, Schema as ArrowSchema, TimeUnit};

use crate::error::{Error, Result};
use crate::pipeline::LoadReport;
use crate::scan::TableName;
use crate::sink::{TableSink, WriteMode};

/// Path component the manifest lives under, relative to the output prefix.
///
/// A leading underscore keeps it out of the way of a schema directory, and no PostgreSQL
/// schema is named this in practice. [`crate::sink::relative_path`] would nonetheless
/// accept a source table of the same name, so [`reserved_collision`] checks for it.
pub const MANIFEST_NAME: &str = "_pgdelta_loads";

/// The timezone stamped on the manifest's timestamp column.
const UTC: &str = "UTC";

/// Returns the name the manifest table is written under.
fn manifest_table() -> TableName {
    TableName {
        schema: None,
        table: MANIFEST_NAME.to_string(),
    }
}

/// True if a source table would be written to the manifest's own location.
///
/// Only reachable for an unqualified source table named exactly [`MANIFEST_NAME`]. It has
/// never been observed, and silently interleaving a source table's rows with the load
/// history would be unrecoverable, so it is checked rather than assumed away.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use pgdelta::manifest::reserved_collision;
/// use pgdelta::scan::TableName;
///
/// let bare = TableName { schema: None, table: "_pgdelta_loads".into() };
/// let qualified = TableName { schema: Some("public".into()), table: "_pgdelta_loads".into() };
/// assert!(reserved_collision(&bare));
/// assert!(!reserved_collision(&qualified));
/// ```
pub fn reserved_collision(table: &TableName) -> bool {
    table.schema.is_none() && table.table == MANIFEST_NAME
}

/// The manifest's columns.
///
/// One row per table per load. Load-level facts repeat on every row of that load, which
/// is redundant and correct: this is an audit log queried by load, not a normalised model,
/// and a few hundred rows a day makes the redundancy free.
///
/// # Panics
///
/// Does not panic.
pub fn schema() -> ArrowSchema {
    ArrowSchema::new(vec![
        // Identifies the run. Microseconds since the epoch, zero padded, so it sorts.
        Field::new("load_id", DataType::Utf8, false),
        Field::new(
            "loaded_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            false,
        ),
        Field::new("table", DataType::Utf8, false),
        // "loaded" or "missing". A missing table has no version and no rows: it was
        // expected and did not arrive, and its Delta table still holds the previous run.
        Field::new("status", DataType::Utf8, false),
        Field::new("expected", DataType::Boolean, false),
        Field::new("delta_version", DataType::Int64, true),
        Field::new("rows", DataType::Int64, true),
        Field::new("batches", DataType::Int64, true),
        // Load-level, repeated.
        Field::new("dumped_by", DataType::Int32, false),
        Field::new("from_database", DataType::Int32, true),
        Field::new("compression", DataType::Utf8, false),
        Field::new("bytes_read", DataType::Int64, false),
        Field::new("total_rows", DataType::Int64, false),
        // Fidelity and drift, rendered as text so the manifest stays readable in SQL
        // without a nested type. Empty string means nothing to report.
        Field::new("schema_added", DataType::Utf8, false),
        Field::new("schema_removed", DataType::Utf8, false),
        Field::new("schema_retyped", DataType::Utf8, false),
        Field::new("null_substitutions", DataType::Utf8, false),
        Field::new("text_fallback_columns", DataType::Utf8, false),
    ])
}

/// Microseconds since the Unix epoch, for the load identifier and its timestamp.
fn now_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_micros()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Builds the batch of rows describing one load.
fn rows_for(report: &LoadReport, load_id: &str, at: i64) -> Result<RecordBatch> {
    let expected_missing = &report.missing_tables;
    let count = report.tables.len() + expected_missing.len();

    let mut load_ids = Vec::with_capacity(count);
    let mut times = Vec::with_capacity(count);
    let mut names = Vec::with_capacity(count);
    let mut statuses = Vec::with_capacity(count);
    let mut expected = Vec::with_capacity(count);
    let mut versions: Vec<Option<i64>> = Vec::with_capacity(count);
    let mut rows: Vec<Option<i64>> = Vec::with_capacity(count);
    let mut batches: Vec<Option<i64>> = Vec::with_capacity(count);
    let mut added = Vec::with_capacity(count);
    let mut removed = Vec::with_capacity(count);
    let mut retyped = Vec::with_capacity(count);
    let mut substitutions = Vec::with_capacity(count);
    let mut fallbacks = Vec::with_capacity(count);

    for stats in &report.tables {
        load_ids.push(load_id.to_string());
        times.push(at);
        names.push(stats.table.clone());
        statuses.push("loaded".to_string());
        expected.push(!report.unexpected_tables.contains(&stats.table));
        versions.push(i64::try_from(stats.delta_version).ok());
        rows.push(i64::try_from(stats.rows).ok());
        batches.push(i64::try_from(stats.batches).ok());
        added.push(stats.schema_drift.added.join(", "));
        removed.push(stats.schema_drift.removed.join(", "));
        retyped.push(
            stats
                .schema_drift
                .retyped
                .iter()
                .map(|(column, was, now)| format!("{column}: {was} -> {now}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
        substitutions.push(
            stats
                .null_substitutions
                .iter()
                .map(|(column, n)| format!("{column}={n}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
        fallbacks.push(
            stats
                .text_fallback_columns
                .iter()
                .map(|(column, declared)| format!("{column}: {declared}"))
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    // A table that was expected and did not arrive gets a row too, or the manifest would
    // say nothing about the most consequential thing that can happen to a feed.
    for name in expected_missing {
        load_ids.push(load_id.to_string());
        times.push(at);
        names.push(name.clone());
        statuses.push("missing".to_string());
        expected.push(true);
        versions.push(None);
        rows.push(None);
        batches.push(None);
        added.push(String::new());
        removed.push(String::new());
        retyped.push(String::new());
        substitutions.push(String::new());
        fallbacks.push(String::new());
    }

    let n = names.len();
    let dumped_by = i32::try_from(report.dumped_by).unwrap_or(0);
    let from_database = report.from_database.and_then(|v| i32::try_from(v).ok());
    let bytes_read = i64::try_from(report.bytes_read).unwrap_or(i64::MAX);
    let total_rows = i64::try_from(report.total_rows).unwrap_or(i64::MAX);

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(load_ids)),
        Arc::new(TimestampMicrosecondArray::from(times).with_timezone(UTC)),
        Arc::new(StringArray::from(names)),
        Arc::new(StringArray::from(statuses)),
        Arc::new(BooleanArray::from(expected)),
        Arc::new(Int64Array::from(versions)),
        Arc::new(Int64Array::from(rows)),
        Arc::new(Int64Array::from(batches)),
        Arc::new(Int32Array::from(vec![dumped_by; n])),
        Arc::new(Int32Array::from(vec![from_database; n])),
        Arc::new(StringArray::from(vec![report.compression.to_string(); n])),
        Arc::new(Int64Array::from(vec![bytes_read; n])),
        Arc::new(Int64Array::from(vec![total_rows; n])),
        Arc::new(StringArray::from(added)),
        Arc::new(StringArray::from(removed)),
        Arc::new(StringArray::from(retyped)),
        Arc::new(StringArray::from(substitutions)),
        Arc::new(StringArray::from(fallbacks)),
    ];

    RecordBatch::try_new(Arc::new(schema()), columns).map_err(|e| Error::Arrow {
        message: e.to_string(),
    })
}

/// Appends one load's record to the manifest beneath `prefix`, creating it if needed.
///
/// Returns the load identifier that was written, which is what a caller records in order
/// to find the run again.
///
/// Always appends: the manifest is a history, and overwriting it would destroy the
/// versions that make earlier loads readable.
///
/// # Errors
///
/// [`Error::Delta`] on a storage or commit failure, and [`Error::Arrow`] if the batch
/// cannot be assembled, which would be a defect here.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Async. Must run on the Tokio runtime.
pub async fn append(
    prefix: &str,
    storage_options: &HashMap<String, String>,
    report: &LoadReport,
) -> Result<String> {
    let at = now_micros();
    let load_id = format!("{at:020}");
    let batch = rows_for(report, &load_id, at)?;

    let mut sink = TableSink::open(
        prefix,
        &manifest_table(),
        &schema(),
        WriteMode::Append,
        storage_options,
    )
    .await?;
    sink.write(batch).await?;
    sink.stage().await?;
    sink.commit().await?;
    Ok(load_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sink::SchemaDrift;
    use deltalake::arrow::array::Array;

    fn report() -> LoadReport {
        LoadReport {
            load_id: None,
            dumped_by: 17,
            from_database: Some(17),
            compression: "gzip",
            bytes_read: 1234,
            total_rows: 5,
            missing_tables: vec!["public.gone".to_string()],
            unexpected_tables: vec!["public.surprise".to_string()],
            tables: vec![
                crate::pipeline::TableStats {
                    table: "public.users".to_string(),
                    schema_drift: SchemaDrift {
                        added: vec!["email".to_string()],
                        removed: vec![],
                        retyped: vec![(
                            "id".to_string(),
                            "integer".to_string(),
                            "string".to_string(),
                        )],
                    },
                    rows: 3,
                    batches: 1,
                    delta_version: 4,
                    null_substitutions: vec![("born".to_string(), 2)],
                    text_fallback_columns: vec![("kind".to_string(), "mystery_enum".to_string())],
                },
                crate::pipeline::TableStats {
                    table: "public.surprise".to_string(),
                    schema_drift: SchemaDrift::default(),
                    rows: 2,
                    batches: 1,
                    delta_version: 1,
                    null_substitutions: vec![],
                    text_fallback_columns: vec![],
                },
            ],
        }
    }

    #[test]
    fn a_row_is_written_for_every_table_including_the_missing() {
        let batch = rows_for(&report(), "0001", 42).unwrap();
        assert_eq!(batch.num_rows(), 3, "two loaded plus one missing");
        assert_eq!(batch.schema().fields().len(), schema().fields().len());
    }

    /// A missing table is the most consequential thing that can happen to a feed, so it
    /// must appear with no version rather than be absent from the record.
    #[test]
    fn a_missing_table_has_no_version() {
        let batch = rows_for(&report(), "0001", 42).unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let statuses = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let index = (0..batch.num_rows())
            .find(|i| names.value(*i) == "public.gone")
            .expect("the missing table must have a row");
        assert_eq!(statuses.value(index), "missing");
        assert!(versions.is_null(index), "a missing table has no version");
    }

    /// The recorded version is what makes a load readable as a snapshot, so it must be
    /// the version the table actually reached.
    #[test]
    fn the_recorded_version_is_the_committed_one() {
        let batch = rows_for(&report(), "0001", 42).unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let versions = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let index = (0..batch.num_rows())
            .find(|i| names.value(*i) == "public.users")
            .unwrap();
        assert_eq!(versions.value(index), 4);
    }

    #[test]
    fn an_unexpected_table_is_flagged_as_such() {
        let batch = rows_for(&report(), "0001", 42).unwrap();
        let names = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let expected = batch
            .column(4)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let index = (0..batch.num_rows())
            .find(|i| names.value(*i) == "public.surprise")
            .unwrap();
        assert!(!expected.value(index), "it was not in expect_tables");
    }

    #[test]
    fn drift_is_rendered_readably() {
        let batch = rows_for(&report(), "0001", 42).unwrap();
        let retyped = batch
            .column(15)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(
            (0..batch.num_rows()).any(|i| retyped.value(i) == "id: integer -> string"),
            "the retype should be legible in SQL without a nested type"
        );
    }

    #[test]
    fn only_a_bare_reserved_name_collides() {
        assert!(reserved_collision(&TableName {
            schema: None,
            table: MANIFEST_NAME.to_string(),
        }));
        assert!(!reserved_collision(&TableName {
            schema: Some("public".to_string()),
            table: MANIFEST_NAME.to_string(),
        }));
        assert!(!reserved_collision(&TableName {
            schema: None,
            table: "users".to_string(),
        }));
    }
}
