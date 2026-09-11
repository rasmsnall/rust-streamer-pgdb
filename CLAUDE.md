# rust-streamer-pgdb

Python library, implemented in Rust, that streams `pg_dump` output directly into
Delta Lake tables for use from Databricks. Status: **implemented end to end.** The
pipeline decodes in parallel, the wheel builds, and all three documents are written.

## Goals / constraints (from the user, verbatim intent)

- Rust core, Python bindings. Target consumer is Databricks.
- **Security considered first**, then whether the approach is actually the best one.
- Fast. Streaming, bounded memory. Never materialise a table.
- Minimal dependency surface; only crates that are widely used and maintained.
- **Code is not to be over-commented.** Comment the non-obvious only.

## Pipeline

```
pg_dump --format=plain
  -> child stdout pipe
  -> scanner: collects column types from CREATE TABLE, detects COPY blocks
  -> COPY TEXT decoder (tab-delimited, backslash escapes, \N = NULL, \. terminates)
  -> Arrow RecordBatch (bounded by batch_rows / batch_bytes)
  -> Delta writer (delta-rs), one Delta table per source table
```

Plain is the only `pg_dump` format that streams without `pg_restore` in the loop;
custom/directory formats are compressed archives requiring a decode pass.

pg_dump emits its pre-data section before its data section, so every `CREATE TABLE`
is seen before the `COPY` that needs it. Single pass, no seek, no buffering of the dump.

### Scanner states
- `Sql`: accumulate `CREATE TABLE` statements; watch for `COPY <tbl> (<cols>) FROM stdin;`
- `Copy`: decode rows until a line that is exactly `\.`

DDL splitting is paren-depth and quote aware; must tolerate `numeric(10,2)`,
`character varying(255)`, `timestamp(3) without time zone`, table constraints,
`GENERATED ALWAYS AS (...) STORED`, and optionally-quoted identifiers with `""` escapes.

## PostgreSQL version support

Dumps arrive **as files, daily, from a third party** whose database is PostgreSQL 17.
We have no network access to that server. That settles several things:

- **No subprocess.** We never spawn `pg_dump`, so we never need a `pg_dump` binary:
  not 17, not 16, not any. The file-input path is the *only* path.
- **No PostgreSQL of our own.** The PG16 staging server exists only to turn dump text
  into rows; this library replaces it outright. Nothing queries it, and Delta cannot
  use the indexes and constraints its post-data step builds.
- Security requirements 2, 3, 5 and 10 (argv vector, `PGPASSWORD`, `sslmode`, child
  exit status) have no subject and drop out. `dump.rs` shrinks to opening a file.

What stays version-relevant: the dump preamble carries `-- Dumped from database version
<v>` and `-- Dumped by pg_dump version <v>`. Parse both, record them in the returned
stats, and **reject a major we have not qualified.** This is the tripwire for the day
the third party upgrades to 18 without telling us, which will happen eventually, and
must fail the load loudly rather than silently mis-parse it.

COPY TEXT is stable across majors, so `copy.rs` is version-agnostic regardless.

## Threat model

The dump is **untrusted input from an external party**, arriving unattended every day.
That inverts the security emphasis: credential handling is now moot, and requirements
6, 7, 8 and 9 are the load-bearing ones. Specifically, a third party controls every
table name, column name and field byte we will ever see:

- Table names are attacker-controlled and feed a path mapping. Requirement 6 is not
  hypothetical. Validate and reject; never sanitise silently.
- Field/row/column bounds (requirement 7) are the only thing standing between a
  malformed dump and an OOM on the Databricks driver.
- A malformed dump must fail the load, never produce a short table. Silent truncation
  committed as success is the worst outcome this library has.

## Scale (measured, 2026-09)

- Dump arrives **daily** from the third party: **plain text, uncompressed, ~48 GB**,
  and growing fast.
- **~450 tables** (447-450 observed). The count varies between dumps, which is itself
  evidence that the table set drifts.
- Largest table ~60M rows, second ~13M. The remaining ~448 are comparatively small, so
  per-table commit overhead, not decoding, dominates the tail.

Consequences:

- Peak memory is already O(1) in dump size, so 480 GB costs no more footprint than
  48 GB. **Wall-clock time is the only thing that scales**, and it is what the Scaling
  strategy below attacks. The hot loop must stay zero-copy regardless (`&[u8]` slices,
  no per-field `String`, no per-row allocation).
- Writing need not be sequential. Hand finished RecordBatches to a bounded channel and a
  small writer pool so the Delta write for table N overlaps the decode of N+1. Otherwise
  ~450 commits serialise against the scan and the job goes latency-bound. Tokio is
  already in the tree via delta-rs, so this costs no new dependency.
- Expected bottleneck order: Parquet encode, then object-store upload, then decode.
- Peak memory is set by `batch_rows`/`batch_bytes` alone. 60M rows costs the same
  footprint as 60k; scale moves runtime only.
- `batch_bytes` bounds the in-memory Arrow batch, **not** the output file. Accumulate
  several batches per Parquet file, targeting ~256 MB-1 GB written, or ~450 tables
  produce small-file sprawl that degrades every downstream query.

## Scaling strategy

Requirement: **48 GB or 480 GB must not be an architectural difference.** Memory already
is not; throughput must be made not to be either.

The enabling fact: **in COPY TEXT a raw newline byte is always a row terminator.**
Newlines inside field data are escaped as `\n` and never emitted literally, so a raw
`0x0A` unambiguously ends a row with no parsing context required.

So decode parallelises safely, in a **single pass, no seek, no second read**:

- One **reader thread** pulls the stream sequentially and cuts it into ~16 MB chunks,
  backing each cut up to the last newline and carrying the remainder forward. Finding
  that boundary is a reverse `memchr`, negligible beside decoding.
- Newline-aligned chunks go over a bounded channel to a **decode pool**. Workers decode
  whole rows independently, because by construction no row spans a chunk.
- Workers emit RecordBatches to the writer pool as already described.

Throughput then scales with cores instead of being pinned to one. Backpressure from the
bounded channel keeps memory capped regardless of dump size.

Row order within a table is not preserved across chunks. Delta tables are unordered, so
this is fine; carry the chunk index if determinism is ever wanted.

The **scanner stays sequential**: `CREATE TABLE` DDL must be read in order, and COPY
block starts and ends must be observed. But DDL is a negligible fraction of the bytes;
only row interiors go to the pool.

### Known ceiling

This is a **single-node** design, scaling to the cores and network bandwidth of one
Databricks driver, good for the high hundreds of GB. Beyond that the next step is
splitting COPY-block byte ranges across Spark executors: a genuinely different
architecture needing a seekable input and a boundary-index pass. Not built now, but the
chunking above is deliberately compatible with it, since both rest on the same
newline-splittability property.

### Failure cleanup at scale

A failed phase 1 leaves orphaned Parquet proportional to how far it got. At 480 GB that
is a lot of dead bytes. Invisible to readers, but `VACUUM` must actually be scheduled or
the storage bill grows silently.

## Load semantics: all-or-nothing

Requirement from the user: **either the whole dump loads, or it fails.** No partial day.

Delta has **no cross-table transaction**: atomicity is per-table, and ~450 tables mean
~450 independent commits. Satisfied with a two-phase load:

1. **Decode and stage.** Consume the entire dump, writing every Parquet file for every
   table, committing *nothing*. Data files not referenced by a transaction log are
   invisible to Delta readers.
2. **Commit.** Only once the stream is consumed cleanly (EOF reached with every COPY
   block closed by `\.`), commit all ~450 tables.

Any failure in phase 1 leaves orphaned files and **zero visible change**. The
non-atomic window shrinks from the full multi-hour decode to the phase-2 commit burst,
which is metadata-only and the least failure-prone part of the pipeline.

This is not *strictly* atomic: a reader during phase 2 can see a mix of day N and N-1.
If a consumer ever needs strict atomicity, the pattern is a manifest table: write each
day under a new load id, then flip the whole set with one commit to a pointer table that
views resolve. Deferred until a consumer asks, because it constrains how every
downstream query must be written.

### Failure policy

- **Structural problems fail the load.** Truncated COPY block, EOF mid-block, missing
  `\.`, row/column count mismatch, unqualified `Dumped by` major, path-traversal in a
  table name. These mean the data is wrong.
- **Type uncertainty degrades, never fails.** An unrecognised Postgres type maps to
  `Utf8`, preserving the literal text, and is reported in the returned stats. Across
  ~450 third-party tables the type zoo is wide; one unknown type must not kill the daily
  load, and nothing is lost, because text can be reinterpreted later.

## Dependency budget

| Crate | Why |
|---|---|
| `pyo3` | Python bindings |
| `deltalake` (delta-rs) | Delta write path; brings arrow/parquet/object_store/tokio/chrono |
| `flate2` | gzip decode. Pure-Rust backend by default, so a wheel needs no C toolchain |
| `tokio` | Named directly by `sink.rs` and `pipeline.rs`. Pinned to what deltalake resolves |
| `futures` | Stream combinators over the Delta file listing. Same pinning |
| `memchr` | SIMD scan for `\n` / `\t` in the hot row loop |

Pinned 2026-09 (probed via cargo; crates.io index reachable): `pyo3 0.29.2`,
`deltalake 0.32.4`, `memchr 2.8.3`, `flate2 1.1.10`, `tokio 1.53.1`, `futures 0.3.34`.
Note pyo3 0.29 renamed `Python::with_gil` to `attach` and `allow_threads` to `detach`,
and it compiles cleanly under `#![forbid(unsafe_code)]`.

`/Volumes` FUSE paths need no object-store feature; `abfss://` needs deltalake's `azure`
feature. `fast-gzip` selects the zlib-ng backend and needs a C toolchain and cmake.

Use `deltalake::arrow` re-exports rather than a direct `arrow` dependency, to avoid
version skew against delta-rs's pinned arrow.

Deliberately **not** used:
- `thiserror`: hand-rolled error enum instead, ~40 lines.
- `sqlparser`: hand-rolled DDL splitter. pg_dump emits constructs sqlparser rejects,
  and its output is machine-generated and regular enough to scan directly.

## Security requirements

These are requirements, not suggestions. They were the starting point of the design.

1. `#![forbid(unsafe_code)]`.
2. Spawn `pg_dump` with an **argv vector, never a shell**. No string interpolation
   into a command line anywhere.
3. Password via the child's `PGPASSWORD` env only, **never argv**, which is
   world-readable via `/proc/<pid>/cmdline`.
4. **Scrub conninfo/secrets from every error path.** pg_dump echoes connection
   strings on stderr; that must not reach a Python traceback or a log.
5. Reject `sslmode=disable` unless explicitly opted into.
6. **Path-traversal guard on table -> output-path mapping.** `../` is a legal quoted
   Postgres identifier; an attacker-controlled table name must not escape the target
   prefix. Validate and reject, don't sanitise silently.
7. Bounded limits: max field bytes, max row bytes, max column count. A hostile or
   corrupt dump must not OOM the Databricks driver.
8. Never log row data.
9. Checked integer parsing throughout; no silent wrap.
10. Reap the child and **check its exit status**. A non-zero `pg_dump` exit must fail
    the load loudly. A silently truncated table committed as success is data-integrity
    corruption, and is the worst failure mode this library has.
11. Release the GIL (`Python::allow_threads`) around the streaming work, with periodic
    signal checks so Ctrl-C works.

## Type mapping (Postgres -> Arrow)

| Postgres | Arrow |
|---|---|
| `smallint`/`int2` | `Int16` |
| `integer`/`int4` | `Int32` |
| `bigint`/`int8` | `Int64` |
| `real` | `Float32` |
| `double precision` | `Float64` |
| `numeric(p,s)`, p<=38 | `Decimal128(p,s)` |
| `numeric` unconstrained / p>38 | `Utf8` |
| `boolean` | `Boolean` |
| `date` | `Date32` |
| `timestamp` | `Timestamp(Micros, None)` |
| `timestamptz` | `Timestamp(Micros, UTC)` |
| `time` | `Time64(Micros)` |
| `bytea` | `Binary` (decode `\x` hex; fall back to escape format) |
| `text`,`varchar`,`char`,`uuid`,`json`,`jsonb`,`inet`,enums,arrays,`interval` | `Utf8` |

Arrays and `interval` stay as their unescaped Postgres literal. This is honest, and avoids
guessing at a structure the caller may not want. Revisit only if asked.

Edge cases that must be handled explicitly: `infinity`/`-infinity` for date and
timestamp, ` BC` suffixed dates, and `NaN` for numeric.

## Databricks constraints

- Write to an **external location or a `/Volumes/...` FUSE path**, never a Unity
  Catalog *managed* table. Third-party writers can corrupt UC-managed tables.
- Schedule `VACUUM`; see open items. Daily full overwrites tombstone the prior day's
  files across every table.
- Keep the table at **reader v1 / writer v2** (no deletion vectors, no column
  mapping), so any DBR version can read the result.
- Delta `timestamp` is micros UTC. `timestamp_ntz` needs reader v3 / writer v7, which
  breaks the compatibility floor above. Naive Postgres timestamps are therefore
  assumed UTC by default; an opt-in `naive_timestamps="ntz"` may be added later, and
  the assumption must be documented at the Python API surface.

## Intended Python surface

```python
pgdelta.stream_dump_to_delta(
    dump_path=...,          # file, or a file descriptor / readable stream
    tables=["public.users"],
    excluded_schemas=["audit", "staging"],
    output_uri="/Volumes/main/raw/pg/",
    mode="overwrite" | "append" | "error",
    batch_rows=100_000,
    batch_bytes=128 << 20,
    storage_options={...},
    expect_pg_major=17,     # reject a dump from an unqualified major
    progress=callable,
)
```

Returns per-table stats. There is no DSN/subprocess entry point: the input is always
an existing dump file, which was previously described as the most secure mode of
operation and is now simply the only one.

Per-table stats include the dump's `Dumped from database version` and `Dumped by
pg_dump version`, so a caller can tell after the fact which major produced the data.

## Documentation standard

The library is intended for **unrestricted downstream use, anyone, any time**, so
documentation is a build requirement, not a closing task. It is written *with* each
item, never retrofitted.

This does **not** contradict "code is not to be over-commented". The two rules address
different things and both hold:

- **Rustdoc (`///`, `//!`) documents the API.** Mandatory on every public item.
- **Inline comments (`//`) stay rare.** Only non-obvious *why*. Never restate *what*.

### Required of every public item

- What it does, in one sentence, in the indicative mood.
- Every parameter and return value.
- **Errors:** which variants it can return and what causes them.
- **Panics:** or an explicit statement that it does not panic.
- **Blocking/async:** whether it blocks, is `async`, or must not be called from an
  async context. This is mandatory; see the concurrency model in `docs/architecture.md`.
- An example where one compiles.

Enforced with `#![warn(missing_docs)]` and `#![warn(rustdoc::broken_intra_doc_links)]`.

### Module level

Every module opens with `//!` explaining its role in the pipeline, its inputs and
outputs, and which thread or runtime it executes on.

### Python surface

Docstrings on every exported function, plus a `.pyi` type stub. The naive-timestamp
assumption must be documented at the Python surface (see Databricks constraints).

### Writing style

- **No em dashes.** Use commas, colons, parentheses, or a new sentence. This applies to
  documentation, code comments, commit messages, and error strings.
- **Formatting follows South Korean university thesis convention**, adapted for
  technical documentation rather than a thesis proper:
  - Chapters numbered with Roman numerals (`I.`, `II.`, `III.`).
  - Sections numbered `1.`, `2.`, restarting within each chapter. Subsections `A.`, `B.`
  - Front matter: table of contents, list of tables, list of figures.
  - **Table captions above the table**, formatted `<Table C-N>`, numbered by chapter.
  - **Figure captions below the figure**, formatted `[Figure C-N]`, numbered by chapter.
  - Numbered reference list at the end, appendices after it.
  - Cross-references name the chapter ("Chapter IV, Section 3"), not a symbol.
- Word output carries a serif body face, justified text, 1.6 line spacing, 3 cm margins,
  and centred page numbers.

### Long-form documents

Authored as Markdown in `docs/`, single source of truth. Word (`.docx`) is **generated**
from that Markdown by `tools/md2docx.py`, never hand-edited, so the two cannot drift.
Regenerate on any docs change with `python tools/md2docx.py`. Requires `python-docx`
(1.2.0 present locally); no pandoc on this machine.

| Document | Contents |
|---|---|
| `docs/architecture.md` | Pipeline stages, concurrency model, failure model, security model, upsides and downsides, references |
| `docs/api.md` | Python and Rust surface, parameter semantics, returned stats |
| `docs/operations.md` | Running it on Databricks, sizing, VACUUM, failure recovery |

Every external claim carries a link: Postgres docs, docs.rs for pinned crate versions,
the Delta protocol. See the References section of `docs/architecture.md`.

## Layout

```
src/lib.rs        crate docs + pyo3 module shell
src/error.rs      hand-rolled error enum
src/dump.rs       compression detection + newline-aligned chunking
src/scan.rs       streaming dump scanner (DDL + COPY detection, TableName)
src/copy.rs       COPY TEXT row decoder (hot path)
src/types.rs      pg type text -> internal type model
src/values.rs     field bytes -> typed values
src/builders.rs   Arrow column + batch builders
src/sink.rs       Delta write path (open_table, TableWriter, commit_table)
src/chan.rs       bounded MPMC channel feeding the decode pool
src/pipeline.rs   orchestration (reader thread, decode/encode pool, two-phase commit)
src/python.rs     pyo3 surface
python/pgdelta/__init__.py
python/pgdelta/__init__.pyi
python/pgdelta/py.typed
pyproject.toml    maturin config (abi3-py310, mixed layout)
README.md
LICENSE
docs/architecture.md
docs/api.md
docs/operations.md
tools/md2docx.py   generates docs/*.docx from docs/*.md
```

Build with maturin (`pip install maturin && maturin build --release`).

## Open items

Resolved and shipped:

- Crate versions pinned; see Dependency budget.
- `maturin` installed; the wheel builds as `pgdelta-0.1.0-cp310-abi3`.
- The 16-vs-17 question was an artifact of the staging-database architecture. No
  `pg_dump` binary and no PostgreSQL server are needed at all.
- Compression: gzip is detected by magic bytes and decoded via `flate2`. zstd is
  recognised and rejected with a precise error rather than misparsed.
- Commit cadence is one commit per table, all deferred to phase 2. No periodic commits,
  because a short table that looks complete is the worst failure mode here.
- Schema drift is all-or-nothing ("either fail or load"), via two-phase.
- `tables=` filtering lives in the scanner. An unwanted COPY block is scanned for its
  terminator and decoded not at all.
- `excluded_schemas=` goes further: the scanner recognises an excluded table's `CREATE
  TABLE` only well enough to find its end, never parsing a column. Unlike `tables`, this
  means a DDL construct this library cannot parse, in a schema nobody wanted, cannot fail
  the load. Exclusion wins over a name also listed in `tables`.
- Re-run story: `overwrite` is idempotent and a failed run commits nothing, so the
  recovery procedure is to run it again. Documented in `docs/operations.md`, Chapter V.
- `VACUUM` guidance and a per-table loop are in `docs/operations.md`, Chapter IV.

Still open, and each needs a decision rather than a default:

- Whether a `VACUUM` schedule actually exists in the job. The library cannot run it and
  the documentation can only say so.
- Behaviour when a table disappears from the dump. Today the previous Delta table is
  left untouched and silently goes stale; nothing detects it.
- Whether a `numeric` outside Arrow's decimal range should be reported in the stats. It
  currently degrades to text without appearing in `text_fallback_columns`, which is
  reserved for types that were not recognised at all.
- Whether the Python surface should accept a file object as well as a path.

## Environment notes

- `grep` in this repo's Git Bash crashes on `-P` and sometimes aborts outright.
  Use the Grep tool rather than shelling out to grep.
- Git Bash heredocs collapse backslashes. For a patch script containing them, write the
  script to a file and run it rather than piping it in.
- The repo is not kept `cargo fmt` clean under default settings; match the surrounding
  hand style rather than reformatting whole files.
- Rust 1.94.0, Python 3.10.11, maturin 1.15.0.
