# pgdelta

Stream a PostgreSQL `pg_dump` plain-text dump straight into Delta Lake tables. A Rust core
with Python bindings, built for Databricks.

It removes the intermediate database from the ingestion path. Instead of restoring a dump
into a PostgreSQL server and reading it back out, the dump text is decoded directly into
Arrow and written as Delta.

```
dump file -> decode -> Arrow -> Delta
```

## What it does

- **Streams, from wherever the dump is.** A local path, or an object read straight out of
  `abfss://`, `gs://` or `s3://` with no staging copy on local disk. Memory is O(1) in dump
  size: a 480 GB dump costs the same footprint as a 48 GB one, and only wall-clock time
  scales. A byte stream that breaks part way is resumed with a ranged request.
- **Parallelises.** The reader and scanner stay sequential because DDL must be read in
  order, but row decoding and Parquet encoding fan out across a worker pool.
- **Stages everything before committing anything.** Every table's Parquet is written with
  nothing committed; only once the whole dump has been consumed cleanly is any table
  committed. A failure during that pass, which is where essentially all failures occur,
  leaves orphaned files and no visible change at all.
  This is not cross-table atomicity, which Delta cannot provide: the final commit burst is
  hundreds of independent per-table commits, and a failure partway through it leaves some
  tables on the new day and some on the old. What the design buys is shrinking that window
  from the multi-hour decode to a metadata-only burst at the end. See
  [`docs/architecture.md`](docs/architecture.md), Chapter VI.
- **Fails loudly on structure, degrades quietly on types.** A truncated `COPY` block or a
  row whose field count disagrees with its header fails the load. An unrecognised
  PostgreSQL type becomes text and is reported in the returned statistics.
- **Treats the dump as untrusted.** `#![forbid(unsafe_code)]`, path-traversal rejection on
  third-party table names, bounded field, row and column limits, and no row data in any
  error message.

## Usage

```python
import pgdelta

report = pgdelta.stream_dump_to_delta(
    "/Volumes/main/landing/pg/day.sql",
    "/Volumes/main/raw/pg/",
    mode="overwrite",
    expect_pg_major=17,
)

print(f"{len(report.tables)} tables, {report.total_rows:,} rows")
```

`expect_pg_major` is the tripwire for the source system being upgraded without notice. Set
it on any unattended feed.

## Building

```
pip install maturin
maturin build --release --features extension-module,azure,fast-gzip
pip install target/wheels/pgdelta-0.1.0-cp310-abi3-*.whl
```

The wheel is `abi3`, so one artefact loads on CPython 3.10 and later, which covers
Databricks Runtime 16.4 LTS and 17.3 LTS (both ship Python 3.12.3).

Optional Cargo features: `azure` for `abfss://` output, `fast-gzip` for the zlib-ng
decoder (needs cmake and a C toolchain), `extension-module` for the wheel build. Leave
`extension-module` off for `cargo test`, which needs to link libpython.

CI builds the manylinux artefact on every push and checks it imports and round-trips on
3.10, 3.12 and 3.13. To reproduce a release build locally without a Linux box:

```
docker run --rm -v "$PWD:/io" -w /io ghcr.io/pyo3/maturin build --release --out dist --features extension-module,azure,fast-gzip
```

## Development

```
cargo fmt --all --check
cargo clippy --all-targets --features azure,fast-gzip -- -D warnings
cargo test --features azure,fast-gzip
python tools/smoke.py          # end-to-end, against an installed wheel
```

These are exactly the gates CI runs. The toolchain is pinned in `rust-toolchain.toml` so
rustfmt and clippy agree between a laptop and the runner.

## Tools

| Tool | Purpose |
|---|---|
| `tools/smoke.py` | End-to-end round trip against an installed wheel. Loads a fixture, reads it back, and checks the values. Also runs a second dump with changed DDL to exercise schema drift. CI runs it on 3.10, 3.12 and 3.13 |
| `tools/anonymise_dump.py` | Turns a real dump into a shareable compatibility fixture. Run it where the dump lives: it opens no network connection and imports nothing outside the standard library |
| `tools/md2docx.py` | Regenerates `docs/*.docx` from the Markdown |

The anonymiser preserves every DDL construct and the shape of every value, and destroys
every data byte. Values that must stay valid to be loadable, dates, timestamps, numbers
and booleans, are regenerated as valid values of that kind rather than having their digits
scrambled. Verify before sharing:

```
python tools/anonymise_dump.py real.sql fixture.sql --max-rows 200 --audit
```

`--audit` re-reads both files and fails if any token of six or more characters from a data
field in the input survives into the output. It is a filter, not a guarantee: read the
result before it leaves your environment.

## Documentation

Markdown in `docs/` is the single source of truth. The `.docx` files are generated from it
by `python tools/md2docx.py` and are never hand-edited.

| Document | Contents |
|---|---|
| [`docs/architecture.md`](docs/architecture.md) | Pipeline stages, concurrency model, failure model, security model, assessment |
| [`docs/api.md`](docs/api.md) | Python and Rust surface, parameter semantics, returned statistics |
| [`docs/operations.md`](docs/operations.md) | Running it on Databricks, sizing, `VACUUM`, failure recovery |

## Status

Complete and implemented. Plain-format dumps only, gzip decoded on the way in. The known
gaps are listed in `docs/operations.md`, Appendix B.

## Licence

MIT. See [`LICENSE`](LICENSE).
