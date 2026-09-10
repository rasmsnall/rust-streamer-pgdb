//! The load history, and the thing it exists for: reading one run back as a consistent
//! set after later runs have moved the tables on.

use std::io::Cursor;

use pgdelta::pipeline::{LoadConfig, run};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-hist-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prefix(dir: &std::path::Path) -> String {
    format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
}

/// Two tables whose row counts change between runs, so a snapshot read can be told apart
/// from a current read.
fn dump(label: &str, rows: usize) -> Vec<u8> {
    let mut d =
        String::from("-- Dumped from database version 17.4\n-- Dumped by pg_dump version 17.4\n");
    for name in ["a", "b"] {
        d.push_str(&format!(
            "CREATE TABLE public.{name} (\n    id integer,\n    tag text\n);\n"
        ));
    }
    for name in ["a", "b"] {
        d.push_str(&format!("COPY public.{name} (id, tag) FROM stdin;\n"));
        for i in 0..rows {
            d.push_str(&format!("{i}\t{label}\n"));
        }
        d.push_str("\\.\n");
    }
    d.into_bytes()
}

fn config(dir: &std::path::Path) -> LoadConfig {
    LoadConfig {
        output_uri: prefix(dir),
        threads: 2,
        ..LoadConfig::default()
    }
}

/// Reads the history back through Delta, as an operator would.
fn history(dir: &std::path::Path) -> Vec<(String, String, Option<i64>, String)> {
    use deltalake::arrow::array::{Array, Int64Array, StringArray};
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let url =
            deltalake::table::builder::ensure_table_uri(format!("{}/_pgdelta_loads", prefix(dir)))
                .unwrap();
        let mut table = deltalake::DeltaTableBuilder::from_url(url)
            .unwrap()
            .build()
            .unwrap();
        table.load().await.unwrap();

        // Read the parquet directly rather than through a query engine, so this test
        // does not pull in datafusion.
        let batches = read_all(&dir.join("_pgdelta_loads"), &table).await;
        let mut out = Vec::new();
        for batch in batches {
            let load_id = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let name = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let status = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let version = batch
                .column(5)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push((
                    load_id.value(i).to_string(),
                    name.value(i).to_string(),
                    (!version.is_null(i)).then(|| version.value(i)),
                    status.value(i).to_string(),
                ));
            }
        }
        out
    })
}

/// Reads a Delta table's visible parquet straight off disk.
///
/// These tests write to `file://`, so going through the object store would only add a
/// dev-dependency for no extra coverage. It is the log that decides which files are
/// visible, which is the part being tested.
async fn read_all(
    root: &std::path::Path,
    table: &deltalake::DeltaTable,
) -> Vec<deltalake::arrow::array::RecordBatch> {
    use deltalake::arrow::array::RecordBatch;
    use futures::TryStreamExt;

    let log_store = table.log_store();
    let snapshot = table.snapshot().unwrap();
    let files: Vec<_> = snapshot
        .snapshot()
        .file_views(&log_store, None)
        .try_collect()
        .await
        .unwrap();

    let mut batches: Vec<RecordBatch> = Vec::new();
    for file in files {
        let bytes = std::fs::read(root.join(file.path().as_ref())).unwrap();
        let reader =
            deltalake::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                bytes::Bytes::from(bytes),
            )
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            batches.push(batch.unwrap());
        }
    }
    batches
}

#[test]
fn a_load_writes_one_row_per_table() {
    let dir = tmpdir("basic");
    let report = run(Cursor::new(dump("day1", 3)), &config(&dir), |_| true).unwrap();

    let load_id = report
        .load_id
        .clone()
        .expect("a load id should be returned");
    let rows = history(&dir);
    let mine: Vec<_> = rows.iter().filter(|r| r.0 == load_id).collect();
    assert_eq!(mine.len(), 2, "one row per table: {rows:?}");
    assert!(mine.iter().all(|r| r.3 == "loaded"));
    assert!(
        mine.iter().all(|r| r.2.is_some()),
        "versions must be recorded"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The history is a history: a second run appends rather than replacing, or the versions
/// that make earlier runs readable would be destroyed.
#[test]
fn later_runs_append_rather_than_replace() {
    let dir = tmpdir("append");
    let first = run(Cursor::new(dump("day1", 3)), &config(&dir), |_| true).unwrap();
    let second = run(Cursor::new(dump("day2", 5)), &config(&dir), |_| true).unwrap();

    assert_ne!(first.load_id, second.load_id, "each run needs its own id");
    let rows = history(&dir);
    assert_eq!(rows.len(), 4, "two runs of two tables: {rows:?}");

    let first_id = first.load_id.unwrap();
    assert_eq!(
        rows.iter().filter(|r| r.0 == first_id).count(),
        2,
        "the first run's rows must survive the second"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The point of the whole exercise: after a later run has moved the tables on, the first
/// run is still readable as a set, at the versions the manifest recorded.
#[test]
fn a_recorded_load_is_still_readable_as_a_snapshot() {
    use deltalake::arrow::array::{Array, StringArray};

    let dir = tmpdir("snapshot");
    let first = run(Cursor::new(dump("day1", 3)), &config(&dir), |_| true).unwrap();
    let first_id = first.load_id.clone().unwrap();
    run(Cursor::new(dump("day2", 5)), &config(&dir), |_| true).unwrap();

    // Current state is day2.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    for (table, version) in history(&dir)
        .into_iter()
        .filter(|r| r.0 == first_id)
        .map(|r| (r.1, r.2.unwrap()))
    {
        let relative = table.replace('.', "/");
        let batches = rt.block_on(async {
            let url =
                deltalake::table::builder::ensure_table_uri(format!("{}/{relative}", prefix(&dir)))
                    .unwrap();
            let mut t = deltalake::DeltaTableBuilder::from_url(url)
                .unwrap()
                .build()
                .unwrap();
            // The manifest's version is what makes this possible.
            t.load_version(version as u64).await.unwrap();
            read_all(&dir.join(&relative), &t).await
        });

        let total: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 3, "{table} at version {version} should hold day1");
        for batch in &batches {
            let tag = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(tag.value(i), "day1", "{table} should read as the first run");
            }
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// A table that was expected and did not arrive is the most consequential thing that can
/// happen to a feed, so the history must say so rather than omit it.
#[test]
fn a_missing_table_is_recorded_without_a_version() {
    let dir = tmpdir("missing");
    let cfg = LoadConfig {
        expect_tables: Some(vec![
            "public.a".to_string(),
            "public.b".to_string(),
            "public.gone".to_string(),
        ]),
        ..config(&dir)
    };
    let report = run(Cursor::new(dump("day1", 2)), &cfg, |_| true).unwrap();
    let load_id = report.load_id.unwrap();

    let rows = history(&dir);
    let gone = rows
        .iter()
        .find(|r| r.0 == load_id && r.1 == "public.gone")
        .expect("the missing table must be recorded");
    assert_eq!(gone.3, "missing");
    assert!(gone.2.is_none(), "a missing table has no version");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_history_can_be_switched_off() {
    let dir = tmpdir("off");
    let cfg = LoadConfig {
        write_manifest: false,
        ..config(&dir)
    };
    let report = run(Cursor::new(dump("day1", 2)), &cfg, |_| true).unwrap();
    assert!(report.load_id.is_none());
    assert!(
        !dir.join("_pgdelta_loads").exists(),
        "nothing should be written when it is off"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
