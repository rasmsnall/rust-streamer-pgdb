//! Differential test: `validate::validate` and `pipeline::run` must agree.
//!
//! `validate.rs` cannot literally share its per-type dispatch with
//! `builders::ColumnBuilder::append`, since that dispatch is expressed on Arrow-typed
//! builder variants (see `src/validate.rs`'s module doc for why). This is the guard
//! against the two drifting apart: every fixture here is run through both entry points,
//! and a fixture that fails one must fail the other with the same error, and a fixture
//! that succeeds must produce the same row counts and per-column reports on both.

use std::io::Cursor;

use pgdelta::pipeline::LoadConfig;
use pgdelta::validate::{ValidateConfig, validate};
use pgdelta::{Error, Result};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-diff-{tag}-{}", std::process::id()));
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

/// What matters about a load's outcome, for comparison against a validation's outcome.
/// Deliberately narrower than either `LoadReport` or `ValidationReport`: only the fields
/// both entry points can produce identically.
#[derive(Debug, PartialEq)]
struct Outcome {
    total_rows: u64,
    null_substitutions: Vec<(String, u64)>,
    text_fallback_columns: Vec<(String, String)>,
}

fn run_load(
    dir: &std::path::Path,
    body: &str,
    wide_numeric_as_decimal: bool,
    native_arrays: bool,
) -> Result<Outcome> {
    let config = LoadConfig {
        output_uri: prefix(dir),
        threads: 2,
        wide_numeric_as_decimal,
        native_arrays,
        ..LoadConfig::default()
    };
    let report = pgdelta::pipeline::run(Cursor::new(dump(body)), &config, |_| true)?;
    // A single-table fixture is all this test uses; concatenating would hide a
    // cross-table disagreement inside an otherwise-matching total.
    assert!(report.tables.len() <= 1, "fixtures here declare one table");
    let stats = report.tables.into_iter().next();
    Ok(Outcome {
        total_rows: report.total_rows,
        null_substitutions: stats
            .as_ref()
            .map(|s| s.null_substitutions.clone())
            .unwrap_or_default(),
        text_fallback_columns: stats.map(|s| s.text_fallback_columns).unwrap_or_default(),
    })
}

fn run_validate(body: &str, wide_numeric_as_decimal: bool, native_arrays: bool) -> Result<Outcome> {
    let config = ValidateConfig {
        wide_numeric_as_decimal,
        native_arrays,
        ..ValidateConfig::default()
    };
    let report = validate(Cursor::new(dump(body)), &config, |_| true)?;
    assert!(report.tables.len() <= 1, "fixtures here declare one table");
    let stats = report.tables.into_iter().next();
    Ok(Outcome {
        total_rows: report.total_rows,
        null_substitutions: stats
            .as_ref()
            .map(|s| s.null_substitutions.clone())
            .unwrap_or_default(),
        text_fallback_columns: stats.map(|s| s.text_fallback_columns).unwrap_or_default(),
    })
}

/// Runs `body` through both entry points and asserts they agree: both succeed with the
/// same [`Outcome`], or both fail with the same [`Error`].
fn assert_agree(tag: &str, body: &str, wide_numeric_as_decimal: bool, native_arrays: bool) {
    let dir = tmpdir(tag);
    let load_result = run_load(&dir, body, wide_numeric_as_decimal, native_arrays);
    let validate_result = run_validate(body, wide_numeric_as_decimal, native_arrays);
    let _ = std::fs::remove_dir_all(&dir);

    match (load_result, validate_result) {
        (Ok(load), Ok(validate)) => {
            assert_eq!(
                load, validate,
                "{tag}: load and validate succeeded but disagreed on the outcome"
            );
        }
        (Err(load_err), Err(validate_err)) => {
            assert_eq!(
                load_err, validate_err,
                "{tag}: load and validate both failed but with different errors"
            );
        }
        (Ok(_), Err(validate_err)) => {
            panic!("{tag}: load succeeded but validate failed with {validate_err:?}")
        }
        (Err(load_err), Ok(_)) => {
            panic!("{tag}: validate succeeded but load failed with {load_err:?}")
        }
    }
}

#[test]
fn a_clean_multi_type_row_agrees() {
    let body = concat!(
        "CREATE TABLE public.t (\n",
        "    id integer,\n",
        "    name text,\n",
        "    price numeric(10,2),\n",
        "    made timestamp(3) without time zone,\n",
        "    active boolean\n",
        ");\n",
        "COPY public.t (id, name, price, made, active) FROM stdin;\n",
        "1\talice\t19.99\t2024-01-01 00:00:00\tt\n",
        "2\t\\N\t0\t2024-06-15 12:30:00\tf\n",
        "\\.\n"
    );
    assert_agree("clean", body, false, false);
}

#[test]
fn a_value_contradicting_its_type_agrees() {
    let body = concat!(
        "CREATE TABLE public.t (\n    id integer\n);\n",
        "COPY public.t (id) FROM stdin;\n",
        "not_a_number\n",
        "\\.\n"
    );
    assert_agree("bad-value", body, false, false);
}

#[test]
fn non_utf8_text_agrees() {
    let dir = tmpdir("nonutf8-load");
    let mut d =
        dump("CREATE TABLE public.t (\n    name text\n);\nCOPY public.t (name) FROM stdin;\n");
    d.extend_from_slice(b"\xff\xfe\n");
    d.extend_from_slice(br"\.");
    d.push(b'\n');

    let config = LoadConfig {
        output_uri: prefix(&dir),
        ..LoadConfig::default()
    };
    let load_err = pgdelta::pipeline::run(Cursor::new(d.clone()), &config, |_| true).unwrap_err();
    let validate_err = validate(Cursor::new(d), &ValidateConfig::default(), |_| true).unwrap_err();
    let _ = std::fs::remove_dir_all(&dir);

    assert_eq!(load_err, validate_err);
    assert!(matches!(load_err, Error::NonUtf8Text { .. }));
}

#[test]
fn an_oversized_decimal_agrees() {
    let body = concat!(
        "CREATE TABLE public.t (\n    v numeric(5,2)\n);\n",
        "COPY public.t (v) FROM stdin;\n",
        "1000.00\n",
        "\\.\n"
    );
    assert_agree("oversized-decimal", body, false, false);
}

#[test]
fn an_unrecognised_type_agrees() {
    let body = concat!(
        "CREATE TABLE public.t (\n    k public.mystery_enum\n);\n",
        "COPY public.t (k) FROM stdin;\n",
        "whatever\n",
        "\\.\n"
    );
    assert_agree("unrecognised-type", body, false, false);
}

#[test]
fn infinity_and_nan_substitutions_agree() {
    let body = concat!(
        "CREATE TABLE public.t (\n    d date,\n    n numeric(10,2)\n);\n",
        "COPY public.t (d, n) FROM stdin;\n",
        "infinity\tNaN\n",
        "\\.\n"
    );
    assert_agree("infinity-nan", body, false, false);
}

#[test]
fn wide_numeric_as_decimal_on_and_off_agree() {
    let body = concat!(
        "CREATE TABLE public.t (\n    v numeric\n);\n",
        "COPY public.t (v) FROM stdin;\n",
        "123.456\n",
        "\\.\n"
    );
    assert_agree("wide-numeric-off", body, false, false);
    assert_agree("wide-numeric-on", body, true, false);
}

#[test]
fn wide_numeric_overflow_agrees() {
    let body = concat!(
        "CREATE TABLE public.t (\n    v numeric\n);\n",
        "COPY public.t (v) FROM stdin;\n",
        "100000000000000000000\n",
        "\\.\n"
    );
    assert_agree("wide-numeric-overflow", body, true, false);
}

#[test]
fn native_arrays_on_and_off_agree_on_a_valid_array() {
    let body = concat!(
        "CREATE TABLE public.t (\n    xs integer[]\n);\n",
        "COPY public.t (xs) FROM stdin;\n",
        "{1,2,3}\n",
        "\\.\n"
    );
    assert_agree("native-arrays-off", body, false, false);
    assert_agree("native-arrays-on", body, false, true);
}

/// With `native_arrays` off the array is opaque text and this element error is invisible
/// to both entry points; with it on, both must parse the element and fail identically.
#[test]
fn native_arrays_element_error_agrees_in_both_states() {
    let body = concat!(
        "CREATE TABLE public.t (\n    xs integer[]\n);\n",
        "COPY public.t (xs) FROM stdin;\n",
        "{1,not_a_number,3}\n",
        "\\.\n"
    );
    assert_agree("native-arrays-elem-off", body, false, false);
    assert_agree("native-arrays-elem-on", body, false, true);
}

#[test]
fn an_unterminated_copy_block_agrees() {
    let dir = tmpdir("unterminated");
    let body = concat!(
        "CREATE TABLE public.t (\n    id integer\n);\n",
        "COPY public.t (id) FROM stdin;\n",
        "1\n",
        "2\n"
    );
    let config = LoadConfig {
        output_uri: prefix(&dir),
        ..LoadConfig::default()
    };
    let load_err = pgdelta::pipeline::run(Cursor::new(dump(body)), &config, |_| true).unwrap_err();
    let validate_err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
        true
    })
    .unwrap_err();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(load_err, validate_err);
}

#[test]
fn an_unsafe_table_name_agrees() {
    let dir = tmpdir("unsafe-name");
    let body = concat!(
        "CREATE TABLE public.\"../escape\" (\n    id integer\n);\n",
        "COPY public.\"../escape\" (id) FROM stdin;\n",
        "1\n",
        "\\.\n"
    );
    let config = LoadConfig {
        output_uri: prefix(&dir),
        ..LoadConfig::default()
    };
    let load_err = pgdelta::pipeline::run(Cursor::new(dump(body)), &config, |_| true).unwrap_err();
    let validate_err = validate(Cursor::new(dump(body)), &ValidateConfig::default(), |_| {
        true
    })
    .unwrap_err();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(load_err, validate_err);
    assert!(matches!(load_err, Error::UnsafeTableName { .. }));
}

#[test]
fn an_unqualified_major_agrees() {
    let dir = tmpdir("unqualified-major");
    let body = "-- Dumped by pg_dump version 18.0\n";
    let config = LoadConfig {
        output_uri: prefix(&dir),
        ..LoadConfig::default()
    };
    let load_err = pgdelta::pipeline::run(Cursor::new(body.as_bytes().to_vec()), &config, |_| true)
        .unwrap_err();
    let validate_err = validate(
        Cursor::new(body.as_bytes().to_vec()),
        &ValidateConfig::default(),
        |_| true,
    )
    .unwrap_err();
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(load_err, validate_err);
}
