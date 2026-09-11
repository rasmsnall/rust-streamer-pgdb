//! `wide_numeric_as_decimal`: mapping a too-wide `numeric` to `decimal(38,18)` for a
//! Databricks target, instead of the default text fallback, with overflow failing the
//! load rather than silently corrupting the stored value.

use std::io::Cursor;

use pgdelta::pipeline::LoadConfig;
use pgdelta::{Error, Result};

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pgdelta-widenum-{tag}-{}", std::process::id()));
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

fn load(dir: &std::path::Path, body: &str, wide_numeric_as_decimal: bool) -> Result<()> {
    let config = LoadConfig {
        output_uri: prefix(dir),
        threads: 2,
        wide_numeric_as_decimal,
        ..LoadConfig::default()
    };
    pgdelta::pipeline::run(Cursor::new(dump(body)), &config, |_| true).map(|_| ())
}

/// Reads back the committed Arrow type of `t`'s single column, so the assertion is about
/// what a consumer would actually see rather than about what the writer intended.
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

const UNCONSTRAINED: &str = concat!(
    "CREATE TABLE public.t (\n    v numeric\n);\n",
    "COPY public.t (v) FROM stdin;\n",
    "123.456\n",
    "\\.\n"
);

#[test]
fn off_by_default_unconstrained_numeric_stays_text() {
    let dir = tmpdir("default-off");
    load(&dir, UNCONSTRAINED, false).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Utf8
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn on_unconstrained_numeric_becomes_decimal_38_18() {
    let dir = tmpdir("unconstrained");
    load(&dir, UNCONSTRAINED, true).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Decimal128(38, 18)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn on_a_precision_over_38_becomes_decimal_38_18() {
    let dir = tmpdir("over-precision");
    let body = concat!(
        "CREATE TABLE public.t (\n    v numeric(40,5)\n);\n",
        "COPY public.t (v) FROM stdin;\n",
        "123.456\n",
        "\\.\n"
    );
    load(&dir, body, true).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Decimal128(38, 18)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A scale Arrow cannot represent at all is a different, narrower problem than "no
/// natural bound": it keeps falling back to text even with the flag on, rather than
/// silently being reinterpreted as scale 18.
#[test]
fn on_an_unrepresentable_scale_still_falls_back_to_text() {
    let dir = tmpdir("bad-scale");
    let body = concat!(
        "CREATE TABLE public.t (\n    v numeric(5,-2)\n);\n",
        "COPY public.t (v) FROM stdin;\n",
        "12300\n",
        "\\.\n"
    );
    load(&dir, body, true).unwrap();
    assert_eq!(
        committed_column_type(&dir),
        deltalake::arrow::datatypes::DataType::Utf8
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// A value that does not fit even `decimal(38,18)` is a structural disagreement between
/// the data and the type this column has been mapped to, not type uncertainty: the load
/// fails rather than silently storing a truncated or wrapped number.
#[test]
fn on_a_value_too_wide_for_decimal_38_18_fails_the_load() {
    let dir = tmpdir("overflow");
    // 21 integer digits: one more than decimal(38,18)'s 20-digit integer part allows.
    let body = concat!(
        "CREATE TABLE public.t (\n    v numeric\n);\n",
        "COPY public.t (v) FROM stdin;\n",
        "100000000000000000000\n",
        "\\.\n"
    );
    let err = load(&dir, body, true).unwrap_err();
    assert!(
        matches!(err, Error::UnparsableValue { .. }),
        "expected an overflow to be reported as a structural value error: {err:?}"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
