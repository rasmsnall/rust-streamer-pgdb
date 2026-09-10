//! Schema drift between runs, which is how a third-party feed announces a DDL change.
//!
//! Before these existed, a new column failed the whole daily load with an opaque delta-rs
//! message, because the writers took their schema from yesterday's table metadata while
//! the batches were built from today's dump.

use std::io::Cursor;

use pgdelta::pipeline::{LoadConfig, run};
use pgdelta::sink::WriteMode;
use pgdelta::{Error, Result};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-drift-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prefix(dir: &std::path::Path) -> String {
    format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
}

fn dump(body: &str) -> Vec<u8> {
    let mut d =
        String::from("-- Dumped from database version 17.4\n-- Dumped by pg_dump version 17.4\n");
    d.push_str(body);
    d.into_bytes()
}

const DAY1: &str = concat!(
    "CREATE TABLE public.t (\n    id integer,\n    name text\n);\n",
    "COPY public.t (id, name) FROM stdin;\n",
    "1\talice\n2\tbob\n",
    "\\.\n"
);

const ADD_COLUMN: &str = concat!(
    "CREATE TABLE public.t (\n    id integer,\n    name text,\n    email text\n);\n",
    "COPY public.t (id, name, email) FROM stdin;\n",
    "1\talice\ta@x.com\n",
    "\\.\n"
);

const DROP_COLUMN: &str = concat!(
    "CREATE TABLE public.t (\n    id integer\n);\n",
    "COPY public.t (id) FROM stdin;\n",
    "1\n",
    "\\.\n"
);

const RETYPE_COLUMN: &str = concat!(
    "CREATE TABLE public.t (\n    id text,\n    name text\n);\n",
    "COPY public.t (id, name) FROM stdin;\n",
    "abc\talice\n",
    "\\.\n"
);

fn load(
    dir: &std::path::Path,
    body: &str,
    mode: WriteMode,
) -> Result<pgdelta::pipeline::LoadReport> {
    let config = LoadConfig {
        output_uri: prefix(dir),
        threads: 2,
        mode,
        ..LoadConfig::default()
    };
    run(Cursor::new(dump(body)), &config, |_| true)
}

/// Reads back the committed schema and rows, so the assertion is about what a consumer
/// would actually see rather than about what the writer intended.
fn committed(dir: &std::path::Path) -> (Vec<String>, usize) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let url = deltalake::table::builder::ensure_table_uri(format!("{}/public/t", prefix(dir)))
            .unwrap();
        let mut t = deltalake::DeltaTableBuilder::from_url(url)
            .unwrap()
            .build()
            .unwrap();
        t.load().await.unwrap();
        let snapshot = t.snapshot().unwrap();
        let columns = snapshot
            .schema()
            .fields()
            .map(|f| f.name().to_string())
            .collect();
        // Count only the files the log still references, which is what a reader sees.
        use futures::TryStreamExt;
        let log_store = t.log_store();
        let files = snapshot
            .snapshot()
            .file_views(&log_store, None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len();
        (columns, files)
    })
}

#[test]
fn a_first_run_reports_no_drift() {
    let dir = tmpdir("first");
    let report = load(&dir, DAY1, WriteMode::Overwrite).unwrap();
    assert!(
        report.tables[0].schema_drift.is_empty(),
        "a table created by this run cannot have drifted: {:?}",
        report.tables[0].schema_drift
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_added_column_is_applied_and_reported() {
    let dir = tmpdir("add");
    load(&dir, DAY1, WriteMode::Overwrite).unwrap();

    let report = load(&dir, ADD_COLUMN, WriteMode::Overwrite).unwrap();
    let drift = &report.tables[0].schema_drift;
    assert_eq!(drift.added, vec!["email".to_string()], "{drift:?}");
    assert!(drift.removed.is_empty(), "{drift:?}");
    assert!(drift.retyped.is_empty(), "{drift:?}");

    let (columns, _) = committed(&dir);
    assert_eq!(
        columns,
        vec!["id".to_string(), "name".to_string(), "email".to_string()],
        "the committed schema must carry the new column"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_dropped_column_is_applied_and_reported() {
    let dir = tmpdir("drop");
    load(&dir, DAY1, WriteMode::Overwrite).unwrap();

    let report = load(&dir, DROP_COLUMN, WriteMode::Overwrite).unwrap();
    let drift = &report.tables[0].schema_drift;
    assert_eq!(drift.removed, vec!["name".to_string()], "{drift:?}");
    assert!(drift.added.is_empty(), "{drift:?}");

    let (columns, _) = committed(&dir);
    assert_eq!(columns, vec!["id".to_string()]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_retyped_column_is_applied_and_reported() {
    let dir = tmpdir("retype");
    load(&dir, DAY1, WriteMode::Overwrite).unwrap();

    let report = load(&dir, RETYPE_COLUMN, WriteMode::Overwrite).unwrap();
    let drift = &report.tables[0].schema_drift;
    assert_eq!(drift.retyped.len(), 1, "{drift:?}");
    assert_eq!(drift.retyped[0].0, "id");
    assert!(
        drift.retyped[0].1.to_lowercase().contains("integer"),
        "was: {:?}",
        drift.retyped[0].1
    );
    assert!(
        drift.retyped[0].2.to_lowercase().contains("string"),
        "now: {:?}",
        drift.retyped[0].2
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Overwrite replaces the table wholesale, so the previous run's files must be gone as
/// well as its schema. A schema change that left yesterday's files behind would produce
/// rows that do not match the declared columns.
#[test]
fn overwrite_with_a_new_schema_leaves_no_stale_files() {
    let dir = tmpdir("stale");
    load(&dir, DAY1, WriteMode::Overwrite).unwrap();
    let (_, before) = committed(&dir);
    assert!(before > 0);

    let report = load(&dir, ADD_COLUMN, WriteMode::Overwrite).unwrap();
    assert_eq!(report.tables[0].rows, 1);

    let (columns, files) = committed(&dir);
    assert_eq!(columns.len(), 3);
    assert_eq!(
        files, 1,
        "only the new run's file may remain visible after an overwrite"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Appending rows shaped one way to a table declared another way cannot be made to mean
/// anything, so it is refused by name rather than attempted.
#[test]
fn append_refuses_a_changed_schema() {
    let dir = tmpdir("append");
    load(&dir, DAY1, WriteMode::Overwrite).unwrap();

    let err = load(&dir, ADD_COLUMN, WriteMode::Append).unwrap_err();
    match err {
        Error::SchemaChanged { table, detail } => {
            assert_eq!(table, "public.t");
            assert!(detail.contains("email"), "detail was {detail:?}");
        }
        other => panic!("expected SchemaChanged, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// An unchanged schema must not be reported as drift, or every run would look like a
/// change and the signal would be worthless.
#[test]
fn an_unchanged_schema_reports_nothing() {
    let dir = tmpdir("same");
    load(&dir, DAY1, WriteMode::Overwrite).unwrap();
    let report = load(&dir, DAY1, WriteMode::Overwrite).unwrap();
    assert!(
        report.tables[0].schema_drift.is_empty(),
        "unchanged DDL reported drift: {:?}",
        report.tables[0].schema_drift
    );
    let _ = std::fs::remove_dir_all(&dir);
}
