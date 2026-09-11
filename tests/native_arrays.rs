//! `native_arrays`: mapping a one-dimensional PostgreSQL array to a native Arrow `List`
//! for a Databricks target, instead of the default text fallback that keeps it as
//! PostgreSQL's own `{...}` literal.

use std::io::Cursor;

use pgdelta::Result;
use pgdelta::pipeline::LoadConfig;

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-natarr-{tag}-{}", std::process::id()));
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

fn load(dir: &std::path::Path, body: &str, native_arrays: bool) -> Result<()> {
    let config = LoadConfig {
        output_uri: prefix(dir),
        threads: 2,
        native_arrays,
        ..LoadConfig::default()
    };
    pgdelta::pipeline::run(Cursor::new(dump(body)), &config, |_| true).map(|_| ())
}

fn committed_column_type(dir: &std::path::Path) -> deltalake::arrow::datatypes::DataType {
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
        use deltalake::kernel::engine::arrow_conversion::TryIntoArrow;
        let arrow_schema: deltalake::arrow::datatypes::Schema = t
            .snapshot()
            .unwrap()
            .schema()
            .as_ref()
            .try_into_arrow()
            .unwrap();
        arrow_schema.field(0).data_type().clone()
    })
}

const INT_ARRAY: &str = concat!(
    "CREATE TABLE public.t (\n    tags integer[]\n);\n",
    "COPY public.t (tags) FROM stdin;\n",
    "{1,2,3}\n",
    "\\.\n"
);

#[test]
fn off_by_default_array_stays_text() {
    let dir = tmpdir("default-off");
    load(&dir, INT_ARRAY, false).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Utf8
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn on_one_dimensional_array_becomes_a_native_list() {
    let dir = tmpdir("one-dim");
    load(&dir, INT_ARRAY, true).unwrap();
    let dt = committed_column_type(&dir);
    assert!(
        matches!(dt, deltalake::arrow::datatypes::DataType::List(ref f) if f.data_type() == &deltalake::arrow::datatypes::DataType::Int32),
        "expected List<Int32>, got {dt:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// PostgreSQL's multi-dimensional arrays are rectangular in a way this library's
/// one-dimensional array-literal reader does not parse, so they keep falling back to
/// text even with the flag on.
#[test]
fn on_a_multi_dimensional_array_still_falls_back_to_text() {
    let dir = tmpdir("multi-dim");
    let body = concat!(
        "CREATE TABLE public.t (\n    grid integer[][]\n);\n",
        "COPY public.t (grid) FROM stdin;\n",
        "{{1,2},{3,4}}\n",
        "\\.\n"
    );
    load(&dir, body, true).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Utf8
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// An element type this library cannot represent leaves the whole array column as text,
/// decided once when the column's type is resolved, rather than failing the load.
#[test]
fn on_an_unsupported_element_type_still_falls_back_to_text() {
    let dir = tmpdir("bad-element");
    let body = concat!(
        "CREATE TABLE public.t (\n    tags public.my_enum[]\n);\n",
        "COPY public.t (tags) FROM stdin;\n",
        "{a,b}\n",
        "\\.\n"
    );
    load(&dir, body, true).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Utf8
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// `wide_numeric_as_decimal` applies to an array's element exactly as it would a plain
/// column of that type.
#[test]
fn wide_numeric_as_decimal_applies_to_array_elements_too() {
    let dir = tmpdir("wide-element");
    let body = concat!(
        "CREATE TABLE public.t (\n    amounts numeric[]\n);\n",
        "COPY public.t (amounts) FROM stdin;\n",
        "{1.5,2.25}\n",
        "\\.\n"
    );
    let config = LoadConfig {
        output_uri: prefix(&dir),
        threads: 2,
        native_arrays: true,
        wide_numeric_as_decimal: true,
        ..LoadConfig::default()
    };
    pgdelta::pipeline::run(Cursor::new(dump(body)), &config, |_| true).unwrap();
    let dt = committed_column_type(&dir);
    assert!(
        matches!(
            dt,
            deltalake::arrow::datatypes::DataType::List(ref f)
                if f.data_type() == &deltalake::arrow::datatypes::DataType::Decimal128(38, 18)
        ),
        "expected List<Decimal128(38,18)>, got {dt:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
