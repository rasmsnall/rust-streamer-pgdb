//! Opening a dump by URI rather than by path.
//!
//! The cloud backends cannot be exercised here, since that needs credentials and a real
//! account. What is exercised is everything up to the point where they differ: scheme
//! recognition, the `file://` path, and that a bad location fails before the pool has
//! done any work.

use pgdelta::pipeline::{LoadConfig, run_uri};
use pgdelta::source::is_remote;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-uri-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn prefix(dir: &std::path::Path) -> String {
    format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
}

const DUMP: &str = concat!(
    "-- Dumped from database version 17.4\n-- Dumped by pg_dump version 17.4\n",
    "CREATE TABLE public.t (\n    id integer,\n    label text\n);\n",
    "COPY public.t (id, label) FROM stdin;\n",
    "1\talpha\n2\tbeta\n",
    "\\.\n"
);

fn write_dump(dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, DUMP).unwrap();
    path
}

fn config(dir: &std::path::Path) -> LoadConfig {
    LoadConfig {
        output_uri: prefix(dir),
        threads: 2,
        expect_pg_major: Some(17),
        ..LoadConfig::default()
    }
}

#[test]
fn a_bare_path_loads() {
    let dir = tmpdir("bare");
    let dump = write_dump(&dir, "day.sql");
    let out = dir.join("out");
    std::fs::create_dir_all(&out).unwrap();

    let report = run_uri(&dump.to_string_lossy(), &config(&out), |_| true).unwrap();
    assert_eq!(report.total_rows, 2);
    assert_eq!(report.tables[0].table, "public.t");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The two spellings of a local location must behave identically, or a caller who writes
/// one in configuration and the other in a test gets different results.
#[test]
fn a_file_url_loads_the_same_as_a_path() {
    let dir = tmpdir("fileurl");
    let dump = write_dump(&dir, "day.sql");
    let out = dir.join("out");
    std::fs::create_dir_all(&out).unwrap();

    let url = format!("file://{}", dump.to_string_lossy().replace('\\', "/"));
    let report = run_uri(&url, &config(&out), |_| true).unwrap();
    assert_eq!(report.total_rows, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// gzip must be detected from the bytes regardless of how the source was opened, since
/// detection is by magic number rather than by extension.
#[test]
fn a_gzipped_source_is_decompressed() {
    use std::io::Write;
    let dir = tmpdir("gz");
    let path = dir.join("day.sql.gz");
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(DUMP.as_bytes()).unwrap();
    std::fs::write(&path, encoder.finish().unwrap()).unwrap();
    let out = dir.join("out");
    std::fs::create_dir_all(&out).unwrap();

    let report = run_uri(&path.to_string_lossy(), &config(&out), |_| true).unwrap();
    assert_eq!(report.compression, "gzip");
    assert_eq!(report.total_rows, 2);
    assert_eq!(
        report.bytes_read,
        std::fs::metadata(&path).unwrap().len(),
        "bytes_read counts the compressed source"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_missing_source_fails_before_any_work() {
    let dir = tmpdir("missing");
    let out = dir.join("out");
    std::fs::create_dir_all(&out).unwrap();

    let err = run_uri(
        &dir.join("absent.sql").to_string_lossy(),
        &config(&out),
        |_| true,
    )
    .unwrap_err();
    let rendered = err.to_string();
    assert!(rendered.contains("absent.sql"), "{rendered}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Scheme recognition decides whether a location goes to the object store or the
/// filesystem, and a mistake either way is a load that cannot start.
#[test]
fn schemes_are_classified_correctly() {
    for remote in [
        "abfss://container@account.dfs.core.windows.net/pg/day.sql.gz",
        "abfs://container@account/pg/day.sql",
        "az://container/pg/day.sql",
        "gs://bucket/pg/day.sql",
        "s3://bucket/pg/day.sql",
        "s3a://bucket/pg/day.sql",
    ] {
        assert!(is_remote(remote), "{remote} should be remote");
    }
    for local in [
        "/Volumes/main/landing/pg/day.sql",
        "/tmp/day.sql",
        "day.sql",
        "./day.sql",
        "../day.sql",
        "C:/dumps/day.sql",
        r"C:\dumps\day.sql",
        "file:///tmp/day.sql",
    ] {
        assert!(!is_remote(local), "{local} should be local");
    }
}

/// A malformed cloud URL must name the location it could not resolve, rather than being
/// silently treated as a relative filename and failing with a confusing "no such file".
///
/// Deliberately a URL that cannot resolve without touching the network. An earlier version
/// of this test used a real bucket name, which made it wait out credential discovery and
/// took seventeen seconds; a test that needs egress does not belong in a unit suite.
#[test]
fn an_unresolvable_cloud_url_reports_the_location() {
    let uri = "abfss://not a valid host/pg/day.sql";
    // `Box<dyn Read>` is not Debug, so the Ok arm cannot be unwrapped for a message.
    let rendered = match pgdelta::source::open(uri, &std::collections::HashMap::new(), &handle()) {
        Ok(_) => panic!("a malformed cloud URL should not resolve"),
        Err(err) => err.to_string(),
    };
    assert!(
        rendered.contains("abfss://"),
        "the error must name the location, got: {rendered}"
    );
}

/// A runtime handle for the few calls that need one outside a load.
fn handle() -> tokio::runtime::Handle {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap()
    })
    .handle()
    .clone()
}
