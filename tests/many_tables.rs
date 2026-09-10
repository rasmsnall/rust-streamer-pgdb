//! The tail of a real run: hundreds of small tables, where per-table commit overhead
//! rather than decoding dominates.
//!
//! These assert correctness under concurrent commits. They do not assert a speed-up,
//! because a local filesystem has none of the round-trip latency the concurrency exists
//! to hide, and a timing assertion here would measure the test machine rather than the
//! change.

use std::io::Cursor;

use pgdelta::pipeline::{LoadConfig, run};

const TABLES: usize = 120;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-many-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prefix(dir: &std::path::Path) -> String {
    format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
}

/// Many tables, each with a handful of rows: the shape of the real feed's long tail.
fn many_table_dump(count: usize) -> Vec<u8> {
    let mut d =
        String::from("-- Dumped from database version 17.4\n-- Dumped by pg_dump version 17.4\n");
    for i in 0..count {
        d.push_str(&format!(
            "CREATE TABLE public.t{i} (\n    id integer,\n    label text\n);\n"
        ));
    }
    for i in 0..count {
        d.push_str(&format!("COPY public.t{i} (id, label) FROM stdin;\n"));
        for row in 0..3 {
            d.push_str(&format!("{row}\tlabel-{i}-{row}\n"));
        }
        d.push_str("\\.\n");
    }
    d.into_bytes()
}

fn load(dir: &std::path::Path, concurrency: usize) -> pgdelta::pipeline::LoadReport {
    let config = LoadConfig {
        output_uri: prefix(dir),
        threads: 4,
        commit_concurrency: concurrency,
        ..LoadConfig::default()
    };
    run(Cursor::new(many_table_dump(TABLES)), &config, |_| true).unwrap()
}

/// Every table must be committed exactly once and carry its own rows. Concurrency must
/// not let a version, or a row, land against the wrong table.
#[test]
fn every_table_commits_with_its_own_rows() {
    let dir = tmpdir("all");
    let report = load(&dir, 16);

    assert_eq!(report.tables.len(), TABLES);
    assert_eq!(report.total_rows as usize, TABLES * 3);
    for stats in &report.tables {
        assert!(
            stats.delta_version >= 1,
            "{} was not committed: version {}",
            stats.table,
            stats.delta_version
        );
        assert_eq!(stats.rows, 3, "{} has the wrong row count", stats.table);
    }

    // Names must be distinct: a concurrency bug that mixed targets would show up here.
    let mut names: Vec<&str> = report.tables.iter().map(|t| t.table.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), TABLES, "a table name was duplicated or lost");

    let _ = std::fs::remove_dir_all(&dir);
}

/// A concurrency of one is the previous behaviour, and must give the same answer.
#[test]
fn serial_and_concurrent_commits_agree() {
    let serial_dir = tmpdir("serial");
    let concurrent_dir = tmpdir("concurrent");

    let serial = load(&serial_dir, 1);
    let concurrent = load(&concurrent_dir, 32);

    assert_eq!(serial.tables.len(), concurrent.tables.len());
    assert_eq!(serial.total_rows, concurrent.total_rows);

    let mut a: Vec<(String, u64, u64)> = serial
        .tables
        .iter()
        .map(|t| (t.table.clone(), t.rows, t.delta_version))
        .collect();
    let mut b: Vec<(String, u64, u64)> = concurrent
        .tables
        .iter()
        .map(|t| (t.table.clone(), t.rows, t.delta_version))
        .collect();
    a.sort();
    b.sort();
    assert_eq!(a, b, "concurrent commits produced a different result");

    let _ = std::fs::remove_dir_all(&serial_dir);
    let _ = std::fs::remove_dir_all(&concurrent_dir);
}

/// A second run over the same target exercises the overwrite path across many tables at
/// once, which is what the daily job actually does.
#[test]
fn a_second_run_overwrites_every_table() {
    let dir = tmpdir("second");
    let first = load(&dir, 16);
    let second = load(&dir, 16);

    assert_eq!(second.tables.len(), TABLES);
    for stats in &second.tables {
        assert_eq!(stats.rows, 3);
    }
    for (before, after) in first.tables.iter().zip(&second.tables) {
        assert!(
            after.delta_version > before.delta_version,
            "{} did not advance: {} -> {}",
            after.table,
            before.delta_version,
            after.delta_version
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
