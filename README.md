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

- **Streams.** Memory is O(1) in dump size. A 480 GB dump costs the same footprint as a
  48 GB one; only wall-clock time scales.
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
maturin build --release
pip install target/wheels/pgdelta-0.1.0-cp310-abi3-*.whl
```

The wheel is `abi3`, so one artefact loads on CPython 3.10 and later. Optional Cargo
features: `fast-gzip` for the zlib-ng backend, `azure` for `abfss://` output.

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
