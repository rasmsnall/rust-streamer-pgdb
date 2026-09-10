//! Detecting a table that stopped arriving, one that appeared unannounced, and an output
//! prefix that a catalog manages for itself.

use std::io::Cursor;

use pgdelta::Error;
use pgdelta::pipeline::{LoadConfig, run};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-set-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prefix(dir: &std::path::Path) -> String {
    format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
}

fn dump(tables: &[&str]) -> Vec<u8> {
    let mut d =
        String::from("-- Dumped from database version 17.4\n-- Dumped by pg_dump version 17.4\n");
    for name in tables {
        d.push_str(&format!(
            "CREATE TABLE public.{name} (\n    id integer\n);\n"
        ));
    }
    for name in tables {
        d.push_str(&format!("COPY public.{name} (id) FROM stdin;\n1\n"));
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

#[test]
fn a_table_that_stopped_arriving_is_reported() {
    let dir = tmpdir("missing");
    let cfg = LoadConfig {
        expect_tables: Some(vec![
            "public.a".to_string(),
            "public.b".to_string(),
            "public.gone".to_string(),
        ]),
        ..config(&dir)
    };

    let report = run(Cursor::new(dump(&["a", "b"])), &cfg, |_| true).unwrap();

    assert_eq!(report.missing_tables, vec!["public.gone".to_string()]);
    assert!(
        report.unexpected_tables.is_empty(),
        "{:?}",
        report.unexpected_tables
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_table_that_appeared_unannounced_is_reported() {
    let dir = tmpdir("unexpected");
    let cfg = LoadConfig {
        expect_tables: Some(vec!["public.a".to_string()]),
        ..config(&dir)
    };

    let report = run(Cursor::new(dump(&["a", "surprise"])), &cfg, |_| true).unwrap();

    assert_eq!(
        report.unexpected_tables,
        vec!["public.surprise".to_string()]
    );
    assert!(
        report.missing_tables.is_empty(),
        "{:?}",
        report.missing_tables
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Neither is an error. A drifting table set is the normal condition of this feed, and
/// failing the load over it would mean a human intervening most days.
#[test]
fn a_changed_table_set_does_not_fail_the_load() {
    let dir = tmpdir("nofail");
    let cfg = LoadConfig {
        expect_tables: Some(vec!["public.gone".to_string()]),
        ..config(&dir)
    };

    let report = run(Cursor::new(dump(&["surprise"])), &cfg, |_| true).unwrap();

    assert_eq!(report.missing_tables, vec!["public.gone".to_string()]);
    assert_eq!(
        report.unexpected_tables,
        vec!["public.surprise".to_string()]
    );
    assert_eq!(report.tables.len(), 1, "the load still ran");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A name in the filter that never appears loads nothing at all, which used to be silent.
#[test]
fn the_filter_doubles_as_an_expectation() {
    let dir = tmpdir("filter");
    let cfg = LoadConfig {
        tables: Some(vec!["public.a".to_string(), "public.typo".to_string()]),
        ..config(&dir)
    };

    let report = run(Cursor::new(dump(&["a", "b"])), &cfg, |_| true).unwrap();

    assert_eq!(report.missing_tables, vec!["public.typo".to_string()]);
    assert_eq!(report.tables.len(), 1, "only the wanted table was loaded");
    assert!(
        report.unexpected_tables.is_empty(),
        "a filter is not an expectation about what else the dump holds"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A table excluded by the filter was still seen in the dump, so it must not be counted
/// as missing.
#[test]
fn a_filtered_table_is_seen_even_though_it_is_not_loaded() {
    let dir = tmpdir("seen");
    let cfg = LoadConfig {
        tables: Some(vec!["public.a".to_string()]),
        expect_tables: Some(vec!["public.a".to_string(), "public.b".to_string()]),
        ..config(&dir)
    };

    let report = run(Cursor::new(dump(&["a", "b"])), &cfg, |_| true).unwrap();

    assert!(
        report.missing_tables.is_empty(),
        "b was in the dump and must not be called missing: {:?}",
        report.missing_tables
    );
    assert_eq!(report.tables.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn nothing_is_reported_without_an_expectation() {
    let dir = tmpdir("none");
    let report = run(Cursor::new(dump(&["a"])), &config(&dir), |_| true).unwrap();
    assert!(report.missing_tables.is_empty());
    assert!(report.unexpected_tables.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// Managed storage assumes the catalog is its only writer, so the load is refused before
/// anything is opened rather than after the dump has been decoded.
#[test]
fn catalog_managed_storage_is_refused_immediately() {
    for uri in [
        "abfss://c@a.dfs.core.windows.net/__unitystorage/catalogs/x/tables/y",
        "dbfs:/user/hive/warehouse/db.db/t",
        "s3://bucket/__UnityStorage/tables/abc",
    ] {
        let cfg = LoadConfig {
            output_uri: uri.to_string(),
            ..LoadConfig::default()
        };
        // The input is deliberately unreadable: if the guard did not fire first, the
        // failure would be about the dump instead of about the target.
        let err = run(Cursor::new(b"not a dump".to_vec()), &cfg, |_| true).unwrap_err();
        match err {
            Error::ManagedTableTarget { uri: got, .. } => assert_eq!(got, uri),
            other => panic!("expected ManagedTableTarget for {uri}, got {other:?}"),
        }
    }
}

#[test]
fn an_external_location_is_allowed() {
    let dir = tmpdir("external");
    // A Volume is managed, but it holds files rather than tables, and landing the dump
    // there is the documented arrangement.
    for uri in [
        "abfss://container@account.dfs.core.windows.net/bronze/pg/",
        "/Volumes/main/raw/pg/",
        "s3://bucket/bronze/pg/",
    ] {
        assert!(
            pgdelta::sink::reject_managed_storage(uri).is_ok(),
            "{uri} should be allowed"
        );
    }
    let report = run(Cursor::new(dump(&["a"])), &config(&dir), |_| true).unwrap();
    assert_eq!(report.tables.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}
