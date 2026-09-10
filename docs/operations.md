# pgdelta: Operations

**Document type** Operations manual
**Status** Complete. Describes the system as built.
**Audience** Whoever runs the daily load and is paged when it fails.
**Companion documents** `architecture.md` for the design, `api.md` for the callable surface.
**Version** 1.0
**Date** 2026-09-10

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. The workload
- II. Deployment
  - 1. Building the wheel
  - 2. Installing on Databricks
  - 3. Where output may be written
  - 4. The compatibility floor
- III. Sizing
  - 1. What scales and what does not
  - 2. Memory
  - 3. Cores and threads
  - 4. Expected runtime
  - 5. Choosing a driver
- IV. Storage Maintenance
  - 1. Why storage grows
  - 2. Scheduling VACUUM
  - 3. Choosing a retention window
- V. Failure and Recovery
  - 1. What a failure leaves behind
  - 2. Re-running a failed load
  - 3. Diagnosing by exception
  - 4. Failures that need the sender
- VI. Monitoring
  - 1. What to record every run
  - 2. What should page someone
- VII. Change Management
  - 1. A new PostgreSQL major
  - 2. Schema drift
  - 3. A table that disappears
- References
- Appendix A. Runbook
- Appendix B. Known gaps
- Appendix D. Reading one run back as a consistent set

### List of Tables

- `<Table 2-1>` Output location options
- `<Table 3-1>` Measured stage throughput
- `<Table 3-2>` Indicative runtimes
- `<Table 3-3>` Commit burst against a local filesystem, 200 small tables
- `<Table 5-1>` Exception to first action
- `<Table 6-1>` Values worth recording per run
- `<Table B-1>` Known gaps

### List of Figures

- `[Figure 3-1]` Peak memory
- `[Figure 5-1]` State after a failed run

---

## I. Introduction

### 1. Purpose

This document covers running `pgdelta` in production: how to deploy it, how to size the
machine, what maintenance it requires, and what to do when it fails. It assumes the
architecture is not of interest until something goes wrong.

### 2. The workload

The system this was built for receives one plain-text `pg_dump` per day from a third
party. As measured in September 2026:

- Roughly 48 GB, uncompressed, and growing.
- Around 450 tables. The count varies between dumps, which is itself evidence that the
  table set drifts.
- The largest table is about 60 million rows, the second about 13 million. The remaining
  hundreds are comparatively small.

Two consequences follow, and both shape everything below. First, per-table commit overhead
rather than decoding dominates the tail of the run. Second, there is no network access to
the source database, so the dump preamble is the only version signal available.

---

## II. Deployment

### 1. Building the wheel

The package builds as an `abi3` wheel: one artefact loads on CPython 3.10 and later
without recompilation.

```
pip install maturin
maturin build --release
```

The wheel lands in `target/wheels/`. Build it on the same platform family as the cluster;
an `abi3` wheel is portable across Python versions, not across operating systems or
architectures.

The release profile enables thin LTO and a single codegen unit. A debug build decodes
roughly an order of magnitude slower and is not representative of anything.

Two optional features exist. `fast-gzip` selects the zlib-ng backend, which is materially
faster but requires a C toolchain and cmake at build time; it is worth enabling only if
the feed is gzipped. `azure` adds the object-store backend needed for `abfss://` output. A
`/Volumes` FUSE path needs neither.

### 2. Installing on Databricks

Install the wheel on the cluster, or as a job-scoped library. The load runs entirely on
the **driver**: it is a single-node design and executors are not used. Sizing the cluster
therefore means sizing the driver, and a large worker pool is wasted money.

### 3. Where output may be written

`<Table 2-1>` Output location options

| Target | Supported | Notes |
|---|---|---|
| External location, `abfss://` | Yes, and preferred for the tables | Requires the `azure` feature and `storage_options` |
| `/Volumes/...` FUSE path | Yes | Intended for landing the dump. Usable for output, but see below |
| Unity Catalog **managed** storage | **Refused** | The load fails immediately with `ValueError` |

The intended layout separates the file from the tables:

```
Input:    /Volumes/<catalog>/<schema>/<landing-volume>/dump.sql.gz
Output:   abfss://<container>@<account>/bronze/<source>/public/<table>
Register: <catalog>.<schema>.<table>, as Unity Catalog external tables
```

A Volume is the right place to land the dump, because a Volume holds files. The Delta
tables belong in an external location, which is what a Unity Catalog external table can be
registered against. Writing the tables into a Volume works, but it is not the arrangement
Databricks expects and it makes registration awkward.

**Managed storage is refused rather than discouraged.** A prefix containing
`__unitystorage` or `/user/hive/warehouse` fails the load before anything is opened, so the
failure costs a second rather than a decoded dump. Managed tables assume the catalog is
their only writer, and an outside writer can leave them inconsistent in ways that do not
surface as a clean error. That is precisely the class of failure this library exists to
avoid, so it is not offered as an option.

### 4. The compatibility floor

Tables are created at Delta **reader version 1 and writer version 2**, with no deletion
vectors and no column mapping, so any Databricks Runtime can read the output. This is a
deliberate constraint and a test asserts it, because a delta-rs upgrade could otherwise
raise the default silently.

The visible cost is that naive PostgreSQL timestamps are assumed to be UTC rather than
written as `timestamp_ntz`, which would require reader version 3 and writer version 7. See
`api.md`, Chapter V, Section 4.

---

## III. Sizing

### 1. What scales and what does not

Peak memory is **O(1) in dump size**. A 480 GB dump costs the same footprint as a 48 GB
one. Only wall-clock time scales, and that is what sizing is about.

### 2. Memory

```
threads x (batch_bytes + one Parquet write buffer)      the open block
  + threads x queue depth x chunk size                  the decode queue
```

[Figure 3-1] Peak memory

Only one `COPY` block is open at a time, so the first term does not multiply by the table
count. With the defaults on a sixteen-core driver, `threads` is 16 and `batch_bytes` is
128 MB, which reserves about two gigabytes of builders before Parquet buffers are counted.
The decode queue adds roughly `16 x 3 x 16 MB`, another three quarters of a gigabyte.

**`threads` and `batch_bytes` multiply.** When memory is tight, lower `batch_bytes`
first. Batches flush on whichever bound is reached first, so a smaller byte bound costs
only more frequent flushes, whereas fewer threads costs throughput directly.

### 3. Cores and threads

`threads` defaults to the machine's parallelism. Lower it when the driver is shared with
other work, or when memory is constrained and lowering `batch_bytes` was not enough.

Raising it above the core count achieves nothing: the workers are CPU-bound on decoding
and Parquet encoding, not waiting on anything.

`commit_concurrency` is the opposite case and defaults to 16. A commit is a small metadata
write waiting on a storage round trip, so it is bounded by latency rather than by cores,
and a number well above the core count is correct. This is the knob for the long tail: the
feed has around 450 tables, most of them small, and committing them one at a time turns a
metadata burst into a serial queue of round trips.

`<Table 3-3>` Commit burst against a local filesystem, 200 small tables

| `commit_concurrency` | Total run |
|---|---|
| 1 | 3.89 s |
| 4 | 2.56 s |
| 16 | 2.24 s |
| 64 | 2.47 s |

Read that as a floor, not as the expected benefit. A local filesystem has almost none of
the round-trip latency the concurrency exists to hide, and it still returns 1.7x between
serial and the default. Against `abfss://`, where a round trip is tens of milliseconds
rather than microseconds, the gap is wider.

The measurement also shows why the default is 16 rather than something larger: 64 is
slower than 16 here, because past a point the requests queue anyway and the coordination
is pure cost.

Raise it if the commit burst is a visible fraction of the run against `abfss://`. There is
little reason to lower it below the default except to reduce request pressure on a shared
storage account.

### 4. Expected runtime

`<Table 3-1>` Measured stage throughput

Measured on synthetic COPY TEXT data carrying realistic entropy, at a 3.0x gzip ratio, via
`cargo run --release --example throughput`.

| Stage | Rate | Scales with cores |
|---|---|---|
| gunzip | 349 MiB/s | No, inherently serial |
| Row decode | 1068 MiB/s per core | Yes |
| Scan | 8681 MiB/s | No, but negligible |

`<Table 3-2>` Indicative runtimes

| Dump | Plain input | gzip input |
|---|---|---|
| 48 GB | Bounded by Parquet encode and upload | 2.3 min of gunzip alone, serial |
| 480 GB | As above, ten times longer | 23.5 min of gunzip alone, serial |

Read those figures carefully. **With gzip input the decompressor governs the run**: one
decode thread already outpaces it three to one, so the decode pool exists to remove a
single-core ceiling and to absorb a future format change, not because decoding is scarce.
With plain input, which is what the current feed delivers, the decompression stage
disappears entirely and Parquet encoding becomes the constraint, which is exactly the
stage the worker pool parallelises.

### 5. Choosing a driver

Prefer cores and memory on the driver over any worker allocation. A memory-optimised
single node is the right shape. Start with the defaults, record peak memory from the first
few runs, and lower `batch_bytes` if the headroom is uncomfortable.

---

## IV. Storage Maintenance

### 1. Why storage grows

Two mechanisms add files that are invisible to readers but still billed.

- **Overwrites tombstone.** A daily full overwrite marks the previous run's files removed
  in the transaction log. The files persist until `VACUUM`. Across roughly 450 tables,
  every day, this accumulates quickly.
- **Failed runs orphan.** A load that fails in Phase 1 leaves every Parquet file it had
  written up to that point, referenced by nothing. At 480 GB a late failure orphans a
  large number of dead bytes.

Neither is visible to a query. Both are visible on the invoice.

### 2. Scheduling VACUUM

`VACUUM` is **not optional** and nothing in this library runs it. Schedule it explicitly
across every table beneath the output prefix.

```sql
VACUUM delta.`/Volumes/main/raw/pg/public/users` RETAIN 168 HOURS;
```

Iterate over the tables the load reports rather than over a hard-coded list, since the
table set drifts:

```python
for stats in report.tables:
    path = f"{output_uri.rstrip('/')}/{stats.table.replace('.', '/')}"
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
```

Run it after a successful load, not before, and not concurrently with one.

### 3. Choosing a retention window

The default retention is seven days, and lowering it below that requires overriding a
safety check that exists to protect concurrent readers. Seven days is a reasonable
starting point here: it is longer than the daily cadence, so a run can always be compared
against yesterday, and short enough that storage does not compound.

Shorten it only if storage cost demands it and you are certain no long-running reader or
time-travel query depends on the window.

---

## V. Failure and Recovery

### 1. What a failure leaves behind

```
Phase 1 fails  ->  orphaned Parquet, no commits, every table unchanged
Phase 2 fails  ->  some tables at the new version, some at the old
```

[Figure 5-1] State after a failed run

This is the property that makes recovery simple. The load is two-phase: the entire dump is
decoded and staged with **nothing committed**, and only once the stream has been consumed
cleanly is every table committed. A failure during the long decode phase therefore leaves
no visible change at all.

A failure in that burst raises `PartialCommitError`, which carries `committed` and
`total`. Those two numbers are the whole diagnosis: `committed == 0` means nothing became
visible and the situation is identical to a Phase 1 failure, while any larger value means
that many tables are on the new day and the rest are not.

Phase 2 is a metadata-only burst at the very end, and it is the only window in which a
partial result is observable. It is short, and it is the least failure-prone part of the
pipeline, but it is not atomic across tables. Delta has no cross-table transaction. If
strict cross-table atomicity is ever required, the pattern is a manifest table, discussed
in `architecture.md`, Chapter VI, Section 1.

### 2. Re-running a failed load

**Simply run it again.** In `overwrite` mode the load is idempotent: a re-run rewrites
every table from the dump, replacing whatever the previous attempt left, whether that was
nothing at all or a partially committed set.

There is no cleanup step and no state to reset. The orphaned files from the failed attempt
are collected by the next `VACUUM`.

The one case needing thought is a failure partway through Phase 2, which leaves some
tables new and some old. Re-running fixes it, because every table is rewritten from the
same dump. Do not attempt to re-run only the tables that failed unless you still have that
day's dump and are certain of which ones they were.

### 3. Diagnosing by exception

`<Table 5-1>` Exception to first action

| Exception | Likely cause | First action |
|---|---|---|
| `ValueError`, unterminated `COPY` | The transfer was truncated | Check the delivered file size against what the sender reports. Re-fetch |
| `ValueError`, unqualified `pg_dump` major | The sender upgraded PostgreSQL | See Chapter VII, Section 1. Do not simply raise `expect_pg_major` |
| `ValueError`, field count mismatch | The dump is malformed or the DDL was misparsed | Capture the table name and raise it with the sender. This is not tuneable |
| `ValueError`, unsafe table name | A table name contains a character the path mapping rejects | See `api.md`, Chapter V, Section 3 |
| `ValueError`, schema differs | The sender changed a column and the run used `append` | Only `overwrite` may change a schema. Confirm the change is intended, then use `overwrite` |
| `ValueError`, field or row too large | A legitimately wide row, or a hostile dump | Raise `max_field_bytes` or `max_row_bytes` only after confirming the row is genuine |
| `PartialCommitError`, `committed` is 0 | The commit burst failed on its first table | Nothing became visible. Fix the cause and re-run |
| `PartialCommitError`, `committed` above 0 | The commit burst failed part way | `committed` tables are on the new day, the rest on the old. Re-run as soon as the cause is fixed |
| `RuntimeError`, delta error | Storage, permissions, or a concurrent writer | Check credentials and that nothing else writes to the prefix |
| `RuntimeError`, internal invariant | A defect in this library | File it with the table name. Do not retry blindly |
| `OSError` | The dump is missing or unreadable | Check the landing path and permissions |
| `KeyboardInterrupt` | Cancelled, or the job timed out | Nothing was committed. Re-run |

### 4. Failures that need the sender

Three failures are not fixable on this side, and recognising them quickly saves hours:

- A truncated `COPY` block means the file is incomplete. Plain dumps carry no row counts,
  so this structural check is the only truncation detector there is.
- A field count mismatch means the dump contradicts its own DDL.
- A text column carrying bytes that are not valid UTF-8 means the source database has an
  encoding problem. Arrow strings are UTF-8, and substituting replacement characters would
  corrupt the values silently, so the load fails instead.

---

## VI. Monitoring

### 1. What to record every run

`<Table 6-1>` Values worth recording per run

| Value | Why |
|---|---|
| Whether `PartialCommitError` was raised, and its `committed` count | The only signal that a failed run left visible change |
| `report.dumped_by`, `report.from_database` | The only version signal a plain dump carries. Answers "what changed" after the fact |
| `report.bytes_read` | Compare against the delivered file size. A mismatch means truncation |
| `report.total_rows` | Compare against yesterday. A large drop is a signal even when the load succeeded |
| `len(report.tables)` | The table set drifts. A change is worth knowing about |
| Per-table `rows` | Where a drop actually happened |
| Per-table `text_fallback_columns` | A new entry means the sender introduced a type this build does not recognise |
| Per-table `schema_drift` | Non-empty means the sender added, removed or retyped a column. This is how DDL change announces itself |
| `missing_tables` | A table stopped arriving. It is still serving the previous run's data |
| `unexpected_tables` | A table appeared that nobody declared. Decide whether anything downstream should consume it |
| Per-table `null_substitutions` | A rising count means a column wants a different mapping |
| Wall-clock duration | The trend matters more than any single run |
| `report.load_id` | How this run is found again in the history, and how it is read back as a set |

### 2. What should page someone

Page on a failed load, since the day's data is missing and the window to re-fetch from the
sender may be limited.

Alert, without paging, on a row count that moves by more than an expected margin against
the previous run, on a change in the table count, on a new `text_fallback_columns` entry,
and on any non-empty `schema_drift`. None of these is an error, and all four are how the
sender's changes announce themselves. A non-empty `schema_drift` in particular means the
Delta schema has just changed underneath every downstream consumer of that table.

Do not alert on `null_substitutions` alone unless the count is rising run over run.

---

## VII. Change Management

### 1. A new PostgreSQL major

Set `expect_pg_major` on every scheduled run. When the sender upgrades, the load fails
immediately and loudly rather than parsing an unfamiliar dialect speculatively and
committing something subtly wrong.

When that failure arrives, the correct response is **not** to raise the number and re-run.
It is to qualify the new major: check the new `pg_dump` output for DDL constructs the
scanner does not handle, add the major to `SUPPORTED_MAJORS` in `scan.rs`, extend the
tests, and release a new wheel. COPY TEXT itself is stable across majors, so the risk is
concentrated in DDL, which is exactly what the tripwire protects.

### 2. Schema drift

The sender controls the schema and will change it without notice. Three cases behave
differently.

- **A new, removed or retyped column** is picked up from the `CREATE TABLE` and the
  `COPY` header. Under `overwrite` the Delta schema is replaced in the same commit that
  replaces the data, so the declared columns and the files always agree, and the change is
  reported per table in `schema_drift`. Under `append` the load fails with `ValueError`
  naming the table and the columns, because appending rows shaped one way to a table
  declared another way cannot be made to mean anything.
- **A new type** this build does not recognise degrades to text and shows up in
  `text_fallback_columns`. The load succeeds. Decide later whether the type deserves a real
  mapping.

A removed column is worth calling out separately: under `overwrite` the new schema no
longer has it, so a downstream query referencing it breaks. That is the correct outcome,
and `schema_drift` is how you find out before the query does rather than after.

### 3. A table that disappears

A table absent from today's dump is not loaded, and its Delta table is **left exactly as
it was**. It is not emptied and not deleted, so it silently becomes stale.

Pass the previous run's table set as `expect_tables` and the report will say so:

```python
report = pgdelta.stream_dump_to_delta(
    DUMP, OUTPUT, mode="overwrite", expect_pg_major=17,
    expect_tables=previous_run_table_names,
)
if report.missing_tables:
    alert(f"stopped arriving, now stale: {report.missing_tables}")
```

That reports it; it does not resolve it. Deciding what a stale table should do, be marked,
be dropped, or be left alone, is a policy question the library cannot answer, and the
answer constrains every downstream consumer. What it no longer is, is invisible.

---

## References

1. Databricks. *VACUUM*.
   https://docs.databricks.com/aws/en/sql/language-manual/delta-vacuum
2. Databricks. *External locations*.
   https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations
3. Databricks. *Unity Catalog managed tables*.
   https://docs.databricks.com/aws/en/tables/managed
4. Databricks. *Volumes*. https://docs.databricks.com/aws/en/volumes/
5. Delta Lake Project. *Delta Transaction Log Protocol*.
   https://github.com/delta-io/delta/blob/master/PROTOCOL.md
6. PostgreSQL Global Development Group. *pg_dump*, PostgreSQL 17 Documentation.
   https://www.postgresql.org/docs/17/app-pgdump.html
7. maturin Project. *maturin User Guide*. https://www.maturin.rs/
8. zlib-ng Project. *zlib-ng*. https://github.com/zlib-ng/zlib-ng

---

## Appendix D. Reading one run back as a consistent set

The commit burst is not atomic, so a reader during it can see a mixture of two runs. The
load history is how that is recovered after the fact rather than prevented.

Every run appends a row per table to `<output_uri>/_pgdelta_loads` recording the Delta
version that table reached. Reading every table at its recorded version reconstructs
exactly what one run produced:

```sql
SELECT table, delta_version FROM delta.`<output>/_pgdelta_loads`
WHERE load_id = '<load_id from the report>'
```

```python
for row in spark.sql(query).collect():
    path = f"{OUTPUT.rstrip('/')}/{row['table'].replace('.', '/')}"
    spark.read.format("delta").option("versionAsOf", row["delta_version"]).load(path)
```

This is what to reach for when a downstream consumer needs a set that is internally
consistent, or when reproducing what a report saw on a particular day.

Two limits. Time travel works only while the files survive, so retention (Chapter IV)
governs how far back a load stays readable: a seven-day `VACUUM` window means seven days
of recoverable snapshots. And the history is a record, not a lock. Nothing stops a later
run from overwriting a table whose version it still names.

Registering the tables in Unity Catalog is a separate step, since delta-rs cannot call a
catalog:

```python
from pgdelta.catalog import register_external_tables
register_external_tables(spark, report, OUTPUT, "main", "bronze_pg")
```

It is idempotent, so running it after every load is the cheapest way to make a
newly-arrived table queryable the same day.

---

## Appendix C. Producing a compatibility fixture

A parser bug that only reproduces on the real feed cannot be investigated if the feed
cannot leave your environment. `tools/anonymise_dump.py` exists for that case.

```
python tools/anonymise_dump.py /Volumes/main/landing/pg/day.sql fixture.sql     --max-rows 200 --audit
```

It runs where the dump already is. It opens no network connection and imports nothing
outside the standard library, so it can be read in full before being trusted.

It preserves what breaks parsers: every DDL construct verbatim, every `COPY` header and
column order, and per field the NULL-ness, the length and the character classes, so
escapes, multi-byte characters and long values still occur where they did. It replaces
every data byte with a value derived from the field's position under a per-run key, never
from its content, so the output cannot be correlated back even by someone holding both
files.

Dates, timestamps, numbers and booleans are regenerated as valid values of their kind
rather than having their characters replaced. This matters: scrambling the digits of
`2026-01-31` yields `2099-45-99`, which is not a date, and a fixture that will not load
tests nothing.

`--audit` re-reads both files afterwards and fails if any token of six or more characters
from a data field survived. That catches the failure that matters, a value passed through
untouched, but it cannot prove the result is safe. **Read the output before sharing it.**

Add `--rename-identifiers` if table and column names are themselves sensitive. It is off
by default because a parser bug is often tied to the exact characters in a name, and
renaming would hide the very thing being reported.

---

## Appendix A. Runbook

The daily job, reduced to its essentials.

```python
import pgdelta

DUMP = "/Volumes/main/landing/pg/day.sql"
OUTPUT = "/Volumes/main/raw/pg/"

report = pgdelta.stream_dump_to_delta(
    DUMP,
    OUTPUT,
    mode="overwrite",
    expect_pg_major=17,
)

# Record these. They are the whole audit trail.
print(f"pg_dump {report.dumped_by}, server {report.from_database}")
print(f"{report.bytes_read:,} bytes, {report.total_rows:,} rows, {len(report.tables)} tables")

for stats in report.tables:
    if stats.text_fallback_columns:
        print(f"NEW UNRECOGNISED TYPE {stats.table}: {stats.text_fallback_columns}")

# Only after success.
for stats in report.tables:
    path = f"{OUTPUT.rstrip('/')}/{stats.table.replace('.', '/')}"
    spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")
```

On failure: read the exception, consult Chapter V, Section 3, and in almost every case
re-run once the underlying cause is addressed. Nothing needs cleaning up first.

---

## Appendix B. Known gaps

Stated plainly, so that nobody discovers them during an incident.

`<Table B-1>` Known gaps

| Gap | Consequence | Mitigation |
|---|---|---|
| A table absent from the dump is left untouched | It silently serves stale data | Pass `expect_tables` and alert on `missing_tables`. The library reports it but will not act on it |
| Phase 2 is not atomic across tables | A reader during the commit burst may see a mix of two days | Accept, or adopt the manifest-table pattern |
| `VACUUM` is not run by the library | Storage grows quietly | Schedule it. See Chapter IV |
| A `numeric` outside Arrow's decimal range becomes text without being reported | Visible only in the resulting schema | Check the schema when a numeric column reads as a string |
| `overwrite` treats the dump as the truth, so a dropped column is dropped | A downstream query referencing it breaks | Alert on `schema_drift`; there is no merge mode |
| Row order within a table is not preserved | Any consumer relying on insertion order breaks | Sort downstream. Delta tables are unordered sets |
| Registering in Unity Catalog needs a Spark session | delta-rs cannot call a catalog | `pgdelta.catalog.register_external_tables`. See Appendix D |
| The load history grows without bound | One row per table per run, so a few hundred a day | Small, but prune it if a year of history is not wanted |
| zstd input is rejected rather than decoded | A format change by the sender fails the load | Add the crate and a match arm before agreeing to any such change |
| Single-node only | Ceiling in the high hundreds of gigabytes | See `architecture.md`, Chapter IV, Section 8 |
