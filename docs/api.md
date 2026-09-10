# pgdelta: API Reference

**Document type** Interface specification
**Status** Complete. Describes the surface as built.
**Audience** Anyone calling this library from Python or from Rust.
**Companion documents** `architecture.md` for why the design is shaped this way, `operations.md` for running it.
**Version** 1.0
**Date** 2026-09-10

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. Which surface to use
- II. Python Surface
  - 1. Installation and import
  - 2. `stream_dump_to_delta`
  - 3. Parameters
  - 4. Return value
  - 5. Exceptions
  - 6. The progress callback
- III. Returned Statistics
  - 1. `LoadReport`
  - 2. `TableStats`
  - 3. Reading the statistics
  - 4. A drifting table set
- IV. Rust Surface
  - 1. Entry points
  - 2. `LoadConfig`
  - 3. Module map
  - 4. Error type
- V. Semantics That Callers Must Know
  - 1. Write modes
  - 2. Table selection
  - 3. Naming and output paths
  - 4. Timestamps and time zones
  - 5. Type fidelity
  - 6. Schema drift
- VI. Worked Examples
  - 1. A daily load
  - 2. A subset load with progress
  - 3. Calling from Rust
- References
- Appendix A. Parameter quick reference

### List of Tables

- `<Table 2-1>` Parameters of `stream_dump_to_delta`
- `<Table 2-2>` Exceptions raised
- `<Table 3-1>` Fields of `LoadReport`
- `<Table 3-2>` Fields of `TableStats`
- `<Table 4-1>` Public Rust modules
- `<Table 5-1>` Write modes
- `<Table A-1>` Parameter quick reference

### List of Figures

- `[Figure 2-1]` Shape of a call
- `[Figure 6-1]` Minimal daily load

---

## I. Introduction

### 1. Purpose

This document specifies the callable surface of `pgdelta`: every parameter, every returned
value, and the semantics a caller must understand in order to use the results correctly.
It does not explain the internal design, which is the subject of `architecture.md`.

### 2. Which surface to use

The library is a Rust crate with Python bindings. The Python surface is the intended
consumer path and is what Databricks jobs call. The Rust surface exists because the Python
one is a thin wrapper over it, and is documented here for anyone embedding the crate
directly or modifying it.

The two are not equivalent in one respect. The Rust surface accepts any
`std::io::Read`, whereas the Python surface accepts a path. That is deliberate: the feed
this library was built for is delivered as files, and a Python file object would have to
be read back through the GIL on every chunk, which would serialise the very stage the
design works hardest to parallelise.

---

## II. Python Surface

### 1. Installation and import

The package builds as an `abi3` wheel and loads on CPython 3.10 and later without
recompilation.

```
pip install maturin
maturin build --release
pip install target/wheels/pgdelta-0.1.0-cp310-abi3-*.whl
```

```python
import pgdelta

pgdelta.stream_dump_to_delta(...)
```

The package exports exactly four names: `stream_dump_to_delta`, `LoadReport`,
`TableStats` and `PartialCommitError`. Type stubs and a `py.typed` marker ship with the
wheel, so `mypy` and `pyright` resolve the surface without configuration.

### 2. `stream_dump_to_delta`

```python
report = pgdelta.stream_dump_to_delta(
    dump_path,
    output_uri,
    *,
    tables=None,
    mode="overwrite",
    batch_rows=100_000,
    batch_bytes=134_217_728,
    threads=None,
    storage_options=None,
    expect_pg_major=None,
    max_field_bytes=None,
    max_row_bytes=None,
    max_columns=None,
    progress=None,
)
```

[Figure 2-1] Shape of a call. Only the first two arguments are positional.

The call blocks until the load finishes. It releases the Global Interpreter Lock for the
duration, so other Python threads continue to run, and reacquires it briefly for each
progress callback.

The dump is read once, front to back. Every table's Parquet is written and staged during
that pass, and nothing becomes visible to a Delta reader until the pass completes without
error, at which point every table is committed.

Be precise about what that buys, because the guarantee is narrower than "atomic". Delta
has no cross-table transaction, and hundreds of tables mean hundreds of independent
commits, so a group of them cannot be made atomic. What holds is:

- Each table's own commit is atomic.
- A failure anywhere in the decode pass, which is the multi-hour part and where
  essentially every failure occurs, leaves **no visible change to any table**.
- The commit burst at the end is not atomic across tables. A failure partway through it
  leaves some tables on the new day and some on the previous one, and a reader querying
  during it can observe a mixture.

The design therefore shrinks the window in which a partial state is observable from the
whole decode to a metadata-only burst; it does not eliminate it. Recovery is to re-run,
which is idempotent in `overwrite` mode. See `architecture.md`, Chapter VI, Section 1, and
`operations.md`, Chapter V.

### 3. Parameters

`<Table 2-1>` Parameters of `stream_dump_to_delta`

| Parameter | Type | Default | Meaning |
|---|---|---|---|
| `dump_path` | `str` or `os.PathLike` | required | Path to the plain-text dump. gzip is detected and decoded transparently |
| `output_uri` | `str` | required | Prefix every table is written beneath |
| `tables` | `list[str]` or `None` | `None` | Qualified names to load. `None` loads every table in the dump |
| `mode` | `str` | `"overwrite"` | `"overwrite"`, `"append"` or `"error"`. See Chapter V, Section 1 |
| `batch_rows` | `int` | `100_000` | Row ceiling for one in-memory Arrow batch |
| `batch_bytes` | `int` | `128 << 20` | Byte ceiling for one in-memory Arrow batch, on decoded field bytes |
| `threads` | `int` or `None` | `None` | Decode and Parquet-encode workers. `None` uses the machine's parallelism |
| `storage_options` | `dict[str, str]` or `None` | `None` | Backend options passed through to `object_store` |
| `expect_pg_major` | `int` or `None` | `None` | When set, the load fails unless the dump's `pg_dump` major matches exactly |
| `expect_tables` | `list[str]` or `None` | `None` | Names this dump is expected to contain. Reports only; see Chapter III, Section 4 |
| `max_field_bytes` | `int` or `None` | 64 MiB | Largest single field permitted |
| `max_row_bytes` | `int` or `None` | 256 MiB | Largest single row permitted. Also bounds reader buffering |
| `max_columns` | `int` or `None` | 1600 | Largest field count permitted. The default is PostgreSQL's own limit |
| `progress` | callable or `None` | `None` | Called after each chunk and each table. See Section 6 |

Every parameter after `output_uri` is keyword-only, so a positional argument cannot drift
onto the wrong slot as the signature grows.

Two of these interact and are worth stating together: **`threads` and `batch_bytes`
multiply.** Peak memory is roughly `threads * batch_bytes` plus a Parquet write buffer per
worker. The defaults on a sixteen-core driver reserve about two gigabytes of builders. See
`operations.md`, Chapter III.

`expect_pg_major` deserves particular attention on an unattended feed. It is the tripwire
for the source system being upgraded without notice, and setting it converts a silent
misparse into a loud failure. It is `None` by default only because a default cannot know
which major you have qualified.

### 4. Return value

A `LoadReport`, described in Chapter III. It is returned only on success; every failure
raises.

### 5. Exceptions

`<Table 2-2>` Exceptions raised

| Exception | Cause |
|---|---|
| `ValueError` | The dump is wrong: a truncated `COPY` block, a row whose field count disagrees with its header, a bad escape, a value contradicting its column type, an unqualified or mismatched `pg_dump` major, or a table name that would escape the prefix |
| `PartialCommitError` | A commit failed part way through the final burst. **The only failure that can leave visible change behind.** Subclasses `RuntimeError` |
| `RuntimeError` | The Delta or Arrow write path failed, or an internal invariant of the library was violated |
| `ValueError`, managed storage | `output_uri` points at Unity Catalog managed storage or the legacy Hive warehouse. Refused before anything is opened |
| `OSError` | The dump could not be read |
| `KeyboardInterrupt` | The load was interrupted by a signal |

The division is deliberate. `ValueError` means the input is at fault and the sender should
be told; `RuntimeError` means this library or the storage layer is at fault and the
operator should be told.

If the progress callback itself raises, **that exception propagates unchanged**, rather
than being replaced by a generic interrupt. A callback that raises `ValueError` after two
hours of decoding reports its own message, not `KeyboardInterrupt`.

In every failing case except `PartialCommitError`, nothing has been committed.

`PartialCommitError` is the exception to that, and the reason it has its own type. It
carries three attributes so the caller need not parse the message:

| Attribute | Meaning |
|---|---|
| `table` | Qualified name of the table whose commit failed |
| `committed` | Tables committed before the failure. **Zero means nothing became visible** |
| `total` | Tables that were to be committed |

```python
try:
    report = pgdelta.stream_dump_to_delta(dump, output)
except pgdelta.PartialCommitError as exc:
    if exc.committed:
        alert(f"{exc.committed} of {exc.total} tables are on the new day; re-run")
    else:
        alert("commit failed before anything became visible; safe to re-run")
```

It subclasses `RuntimeError`, so a handler written without knowing about it still catches
it. See `operations.md`, Chapter V.

### 6. The progress callback

`progress` is called with a single dictionary argument:

```python
{"bytes_read": int, "rows": int, "tables_done": int, "table": str | None}
```

It fires once after each chunk of input, with `table` set to `None`, and once after each
`COPY` block closes, with `table` set to the name of the table that just finished.

`bytes_read` counts bytes **taken from the input, before decompression**, so it is
directly comparable with the size of the file on disk. This is what makes a percentage
meaningful on a gzipped dump.

The callback runs on the calling thread with the GIL held, so it should be inexpensive.
Decode workers never call it, so it is never invoked concurrently and needs no locking.

Returning a value has no effect. To stop a load, raise; the exception propagates as
described in Section 5, and nothing is committed.

---

## III. Returned Statistics

### 1. `LoadReport`

`<Table 3-1>` Fields of `LoadReport`

| Field | Type | Meaning |
|---|---|---|
| `dumped_by` | `int` | `pg_dump` major version that wrote the dump, from the preamble |
| `from_database` | `int` or `None` | Source server major version, when the preamble stated one |
| `compression` | `str` | Compression decoded off the input: `"none"` or `"gzip"` |
| `bytes_read` | `int` | Bytes taken from the input, counted before decompression |
| `total_rows` | `int` | Rows decoded across every loaded table |
| `tables` | `list[TableStats]` | One entry per loaded table, in the order their blocks closed |
| `missing_tables` | `list[str]` | Expected names the dump did not contain |
| `unexpected_tables` | `list[str]` | Names the dump contained that `expect_tables` did not list |

Instances are immutable.

`dumped_by` and `from_database` are the record of which PostgreSQL produced the data. They
are worth persisting alongside the load, because they are the only version signal a plain
dump carries, and after the fact they are the only way to answer why a load behaved
differently on one day.

### 2. `TableStats`

`<Table 3-2>` Fields of `TableStats`

| Field | Type | Meaning |
|---|---|---|
| `table` | `str` | Qualified name as written in the dump |
| `rows` | `int` | Rows decoded from this table's `COPY` block |
| `batches` | `int` | Arrow batches encoded to Parquet, summed across workers |
| `delta_version` | `int` | Delta version the commit produced |
| `null_substitutions` | `dict[str, int]` | Per column, values stored as NULL because the Arrow type could not represent them |
| `text_fallback_columns` | `dict[str, str]` | Per column, the declared PostgreSQL type of a column written as text because that type was not recognised |
| `schema_drift` | `dict[str, list[list[str]]]` | How this run's schema differed from the one the table already declared. See Chapter V, Section 6 |

### 3. Reading the statistics

`null_substitutions` and `text_fallback_columns` are the fidelity report, and both are
normally empty. Each carries a specific meaning.

A non-empty `null_substitutions` entry means the column held `infinity`, `-infinity` or
`NaN`, which Arrow's date, timestamp and decimal types cannot represent. Those values
became NULL. A high count is a signal that the column wants a different mapping, and the
honest fix is to declare it text at the source.

A non-empty `text_fallback_columns` entry means the column's declared type was not
recognised, so the literal dump text was preserved as a string. Enums, domains, composites
and ranges land here by design. Nothing is lost, and the text can be reinterpreted
downstream.

Note one gap: a `numeric` whose precision or scale Arrow cannot hold is also written as
text, but is **not** listed in `text_fallback_columns`, because its type was recognised.
It is visible in the resulting Delta schema. See `architecture.md`, Chapter VIII,
Section 2.

`batches` is a diagnostic rather than a fidelity signal. A table whose `batches` count is
far larger than `rows / batch_rows` was flushed by the byte bound rather than the row
bound, which usually means wide rows.

### 4. A drifting table set

The table set of a third-party feed drifts. `expect_tables` is how a run says what it
thought it was getting, and the report says how that differed.

```python
report = pgdelta.stream_dump_to_delta(
    dump, output, expect_tables=yesterdays_table_names
)
if report.missing_tables:
    alert(f"stopped arriving: {report.missing_tables}")
if report.unexpected_tables:
    alert(f"new since yesterday: {report.unexpected_tables}")
```

Neither fails the load, deliberately. A drifting set is the normal condition of this feed,
and failing over it would mean a human intervening most days.

`missing_tables` is the one that matters operationally. **A table absent from the dump is
not emptied and not deleted; it is left exactly as it was**, so it silently keeps serving
the previous run's data. Nothing else detects that, and a downstream consumer has no way
to tell yesterday's rows from today's.

`unexpected_tables` is computed only when `expect_tables` is given, since without an
expectation nothing can be unexpected.

If `expect_tables` is omitted but `tables` is given, the filter doubles as the expectation
for the missing check. A name in `tables` that never appears in the dump loads nothing at
all, and that used to be silent.

Note that a table excluded by the `tables` filter is still *seen*: its `COPY` block is
scanned past, so its name is known and it is not counted as missing.

---

## IV. Rust Surface

### 1. Entry points

```rust
pub fn run<R, F>(input: R, config: &LoadConfig, progress: F) -> Result<LoadReport>
where
    R: std::io::Read + 'static,
    F: FnMut(Progress) -> bool;

pub fn run_file<F>(path: &Path, config: &LoadConfig, progress: F) -> Result<LoadReport>
where
    F: FnMut(Progress) -> bool;
```

Both live in `pgdelta::pipeline`. Both **block**, and both build a private multi-threaded
Tokio runtime for the storage calls, so **neither may be called from inside an existing
Tokio runtime**. This is the single most important constraint on the Rust surface, and
violating it will deadlock rather than fail cleanly.

The progress closure returns `bool` here rather than raising: `false` aborts the load with
`Error::Interrupted`. Pass `|_| true` to ignore progress entirely. It is called only from
the calling thread.

`run` accepting any `Read` is what makes the crate testable without touching the file
system, and what would permit a future caller to stream from a network source.

### 2. `LoadConfig`

Every field is public, and `LoadConfig::default()` supplies the same defaults as the
Python surface except `output_uri`, which is empty and must be set.

```rust
let config = LoadConfig {
    output_uri: "/Volumes/main/raw/pg/".into(),
    mode: WriteMode::Overwrite,
    expect_pg_major: Some(17),
    threads: 8,
    ..LoadConfig::default()
};
```

`threads` is `usize` rather than an option: `0` means "use `available_parallelism`", which
is what the Python `None` maps to.

`limits` is a `copy::Limits` carrying `max_field_bytes`, `max_row_bytes` and
`max_columns`.

### 3. Module map

`<Table 4-1>` Public Rust modules

| Module | Contents |
|---|---|
| `pipeline` | `run`, `run_file`, `LoadConfig`, `LoadReport`, `TableStats`, `Progress` |
| `scan` | `Scanner`, `Event`, `TableDef`, `ColumnDef`, `TableName`, `parse_create_table`, `parse_copy_header` |
| `copy` | `rows`, `fields`, `decode_row`, `Field`, `Limits` |
| `types` | `resolve`, `PgType`, `ResolvedType` |
| `values` | `parse_i16` through `parse_bytea`, one per supported type |
| `builders` | `arrow_type`, `arrow_schema`, `ColumnBuilder`, `BatchBuilder` |
| `sink` | `relative_path`, `open_table`, `commit_table`, `TableWriter`, `TableSink`, `WriteMode` |
| `dump` | `detect`, `decompressed`, `ChunkReader`, `Compression` |
| `chan` | `bounded`, `Sender`, `Receiver` |
| `error` | `Error`, `Result` |

Everything is public because the crate is intended for unrestricted downstream use, and
because the stages are individually useful: a caller who wants COPY TEXT decoding without
Delta can depend on `copy` alone.

### 4. Error type

`Error` is a hand-rolled enum, `Clone` and `PartialEq`, so it moves cheaply between
threads and compares in tests. No variant carries field contents, because error text
reaches logs and Python tracebacks and the dump is untrusted. Variants carry positions,
table names, counts and limits only.

---

## V. Semantics That Callers Must Know

### 1. Write modes

`<Table 5-1>` Write modes

| Mode | Behaviour |
|---|---|
| `"overwrite"` | The previous run's files are tombstoned in the same commit that adds the new ones, so no reader ever observes an empty or half-replaced table |
| `"append"` | New files are added to whatever the table already holds |
| `"error"` | The load fails if a target table already holds data |

Overwrite is the default because the feed is a full daily dump. It is a logical
replacement, not a delete followed by a write: there is no window in which the table
appears empty.

Note that overwrite tombstones files rather than deleting them. Storage does not shrink
until `VACUUM` runs. See `operations.md`, Chapter IV.

### 2. Table selection

`tables` matches against the qualified name as the dump writes it, for example
`"public.users"`. A `COPY` block for any other table is scanned forward to its terminator
and otherwise skipped, which costs almost nothing: no decoding, no type resolution, and no
Delta table is created.

A name in `tables` that never appears in the dump is not an error and is not reported. If
you need to know that a table you expected was absent, compare the names in
`report.tables` against your list.

### 3. Naming and output paths

A table maps to `output_uri` plus its schema and name as path components, so
`public.users` beneath `/Volumes/main/raw/pg/` becomes
`/Volumes/main/raw/pg/public/users`.

Table names come from a third party, so this mapping is a security boundary. Each
component must consist of letters, digits, underscores, hyphens and spaces, with no
leading or trailing space. Letters are judged by Unicode, so `"Räksmörgås"` is accepted.
Anything else, including path separators, dots and control characters, is **rejected, not
sanitised**, and fails the load with `ValueError`.

The consequence worth knowing is that a table whose name contains a dot, which is legal in
PostgreSQL when quoted, cannot be loaded. Rejecting it is deliberate: silently mapping
`public."a.b"` to `public/a/b` would collide with table `b` in a schema named `public.a`.

### 4. Timestamps and time zones

A PostgreSQL `timestamp with time zone` is normalised to UTC and written as Delta
`timestamp`, which is microseconds UTC. That is exact.

A PostgreSQL `timestamp without time zone` has no zone to normalise, and is **assumed to
be UTC**. This is the one semantic assumption the library makes that a caller cannot
detect from the output. Delta's `timestamp_ntz`, which would represent a naive timestamp
honestly, requires reader version 3 and writer version 7 and would break the compatibility
floor described in `operations.md`, Chapter II.

If your source stores naive local timestamps, the values will be wrong by your offset, and
you must convert downstream.

### 5. Type fidelity

Type uncertainty degrades, it never fails. An unrecognised PostgreSQL type maps to `Utf8`
with the literal dump text preserved, and is reported in `text_fallback_columns`. Across
hundreds of third-party tables the type zoo is wide, and one unknown type must not kill a
scheduled load.

Structural disagreement does fail. Text in an integer column means the dump contradicts
its own DDL, and that is not a fidelity question. Likewise a text column carrying bytes
that are not valid UTF-8 fails, because Arrow strings are UTF-8 and substituting
replacement characters would corrupt values silently.

The full mapping is `architecture.md`, Chapter VIII.

### 6. Schema drift

The sender controls the schema and changes it without notice, so a change is a routine
event rather than an exception.

Under `mode="overwrite"` the table is rewritten wholesale, so an added, removed or retyped
column is **applied**: the Delta schema is replaced in the same commit that replaces the
data, and the declared columns therefore always agree with the files. The change is
reported per table:

```python
for stats in report.tables:
    drift = stats.schema_drift
    if any(drift.values()):
        print(stats.table, drift)
        # {'added': [['email']], 'removed': [], 'retyped': [['id', 'integer', 'string']]}
```

`added` and `removed` carry one column name per entry; `retyped` carries
`[column, was, now]` using Delta's type names. All three are empty on a first run and on
any run whose DDL is unchanged, so a non-empty value is exactly the signal that something
moved.

Under `mode="append"` a change is **refused** with `ValueError`, naming the table and the
columns. Appending rows shaped one way to a table declared another way cannot be made to
mean anything, so it fails rather than guessing.

Note what this does not do. There is no merge: `overwrite` declares the dump's schema as
the truth, and a column the dump stopped carrying is gone from the table. That is the
correct reading of a full daily snapshot, and it is why `schema_drift` is worth alerting
on: a downstream query referencing a dropped column will break, and this is how you learn
before the query does.

---

## VI. Worked Examples

### 1. A daily load

```python
import pgdelta

report = pgdelta.stream_dump_to_delta(
    "/Volumes/main/landing/pg/day.sql",
    "/Volumes/main/raw/pg/",
    mode="overwrite",
    expect_pg_major=17,
)

print(f"{len(report.tables)} tables, {report.total_rows:,} rows")
for stats in report.tables:
    if stats.text_fallback_columns:
        print(f"  {stats.table}: text fallback {stats.text_fallback_columns}")
```

[Figure 6-1] Minimal daily load. `expect_pg_major` is what makes an upstream upgrade loud.

### 2. A subset load with progress

```python
from pathlib import Path

size = Path(dump).stat().st_size

def report_progress(event):
    if event["table"]:
        print(f"  done {event['table']}: {event['rows']:,} rows so far")
    else:
        print(f"  {100 * event['bytes_read'] / size:.1f}%")

report = pgdelta.stream_dump_to_delta(
    dump,
    "abfss://raw@account.dfs.core.windows.net/pg/",
    tables=["public.users", "public.orders"],
    mode="overwrite",
    threads=8,
    batch_bytes=32 << 20,
    storage_options={"account_name": "account", "account_key": key},
    expect_pg_major=17,
    progress=report_progress,
)
```

`bytes_read` is pre-decompression, so dividing by the file size is correct even when the
dump is gzipped.

### 3. Calling from Rust

```rust
use pgdelta::pipeline::{run_file, LoadConfig};
use pgdelta::sink::WriteMode;

let config = LoadConfig {
    output_uri: "/Volumes/main/raw/pg/".into(),
    mode: WriteMode::Overwrite,
    expect_pg_major: Some(17),
    ..LoadConfig::default()
};

let report = run_file(std::path::Path::new("day.sql"), &config, |_| true)?;
println!("{} tables, {} rows", report.tables.len(), report.total_rows);
```

Remember that this blocks and builds its own Tokio runtime. Calling it from inside an
async context will deadlock.

---

## References

1. PostgreSQL Global Development Group. *COPY*, PostgreSQL 17 Documentation.
   https://www.postgresql.org/docs/17/sql-copy.html
2. PostgreSQL Global Development Group. *Data Types*, PostgreSQL 17 Documentation.
   https://www.postgresql.org/docs/17/datatype.html
3. Delta Lake Project. *Delta Transaction Log Protocol*.
   https://github.com/delta-io/delta/blob/master/PROTOCOL.md
4. Apache Arrow Project. *Arrow Columnar Format*.
   https://arrow.apache.org/docs/format/Columnar.html
5. PyO3 Project. *Parallelism*. https://pyo3.rs/latest/parallelism.html
6. maturin Project. *maturin User Guide*. https://www.maturin.rs/
7. Python Software Foundation. *PEP 561, Distributing and Packaging Type Information*.
   https://peps.python.org/pep-0561/
8. Apache Software Foundation. *object_store crate*.
   https://docs.rs/object_store/latest/object_store/

---

## Appendix A. Parameter quick reference

`<Table A-1>` Parameter quick reference

| Set this | When |
|---|---|
| `expect_pg_major` | Always, on an unattended feed. It is the upgrade tripwire |
| `mode="overwrite"` | The dump is a full daily snapshot, which is the intended case |
| `mode="error"` | A one-off backfill that must not clobber an existing table |
| `tables` | You need a subset. Skipping a block is nearly free |
| `threads` | The driver is shared, or memory is tight. Otherwise leave it |
| `batch_bytes` | Memory is tight. Lower this before lowering `threads` |
| `storage_options` | Output is `abfss://` or another authenticated backend |
| `max_field_bytes`, `max_row_bytes`, `max_columns` | The source is known to be hostile, or a legitimate wide row is being rejected |
| `progress` | The load is long enough that silence is indistinguishable from a hang |
