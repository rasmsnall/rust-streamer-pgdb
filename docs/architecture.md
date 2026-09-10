# pgdelta: Architecture

**Document type** Technical architecture specification
**Status** Complete and implemented. The pipeline runs end to end, decoding in parallel, behind both the Rust and the Python surface.
**Audience** Anyone integrating, operating, or modifying this library. No prior context assumed.
**Companion documents** `api.md` for the callable surface, `operations.md` for running it.
**Version** 1.1
**Date** 2026-09-10

---

## Contents

- I. Introduction
  - 1. Purpose
  - 2. Rationale
  - 3. Scope and non-goals
- II. Input Format
  - 1. Structure of a plain dump
  - 2. Properties the design depends on
- III. Pipeline Architecture
  - 1. Stage overview
  - 2. Read
  - 3. Scan
  - 4. Decode
  - 5. Type mapping and array building
  - 6. Write
- IV. Concurrency Model
  - 1. Execution model by component
  - 2. Rationale for the division
  - 3. Parallel decoding
  - 4. Invariants
  - 5. The Global Interpreter Lock
  - 6. Disadvantages
  - 7. Advantages
  - 8. Known ceiling
- V. Resource Model
- VI. Failure Model
  - 1. All-or-nothing loading
  - 2. Classification of faults
  - 3. Truncation detection
- VII. Version Handling
- VIII. Type Mapping
  - 1. Mapping table
  - 2. Edge cases
  - 3. Schema changes between runs
  - 4. Timestamps
- IX. Security Model
- X. Deployment Constraints
- XI. Assessment
  - 1. Advantages
  - 2. Disadvantages
  - 3. Conditions under which this design is inappropriate
- XII. Dependencies
- References
- Appendix A. Glossary

### List of Tables

- `<Table 3-1>` Pipeline stages and responsibilities
- `<Table 4-1>` Execution model by component
- `<Table 8-1>` PostgreSQL to Arrow type mapping
- `<Table 12-1>` Direct dependencies
- `<Table A-1>` Glossary of terms

### List of Figures

- `[Figure 1-1]` Pipeline superseded by this library
- `[Figure 1-2]` Pipeline implemented by this library
- `[Figure 2-1]` Structure of a COPY block
- `[Figure 5-1]` Determinants of peak memory

---

## I. Introduction

### 1. Purpose

`pgdelta` streams a PostgreSQL `pg_dump` plain-text dump directly into Delta Lake
tables. It is a Rust core with Python bindings, intended for use from Databricks.

It exists to remove an intermediate database from the ingestion path.

```
dump file -> restore into a PostgreSQL server -> read back out -> Delta
```

[Figure 1-1] Pipeline superseded by this library

That intermediate server performs one useful function, parsing dump text into rows, and
charges for it in write amplification, storage proportional to the dataset, index builds
whose output is discarded, and an unbounded operational surface. This library performs
the parsing directly.

```
dump file -> decode -> Arrow -> Delta
```

[Figure 1-2] Pipeline implemented by this library

### 2. Rationale

The change is not merely an optimisation. Restoring a dump produced by `pg_dump` 17 into
a PostgreSQL 16 server is **not supported upstream**, and it fails in the worst available
manner. `psql` treats errors as non-fatal by default, so a statement the older server
cannot parse prints a message, is skipped, and the load continues. The result is a table
that is short or missing columns but reports success.

Removing the intermediate server eliminates that entire class of failure, because
nothing ever re-parses the dump as SQL.

### 3. Scope and non-goals

In scope:

- Plain-format (`--format=plain`) dumps, read from a file path. gzip is decoded on the way in.
- `CREATE TABLE` DDL sufficient to recover column names and types.
- `COPY ... FROM stdin` data blocks in TEXT format.
- Writing one Delta table per source table.

Not in scope:

- Custom-format and directory-format archives. These are compressed containers requiring
  `pg_restore` to decode, and they do not stream.
- Restoring into any PostgreSQL server. This library never speaks the wire protocol and
  never executes SQL.
- Views, functions, triggers, indexes, grants, and sequences. Delta cannot use them.
- Incremental or change-data-capture loading. Each run is a full load.
- Binary-format COPY blocks.
- Reading from an arbitrary Python file object or file descriptor. The Rust surface accepts
  any `std::io::Read`, but the Python surface takes a path, because the feed is delivered
  as files.

---

## II. Input Format

Understanding the concurrency model requires understanding the input format, so it is
presented first.

### 1. Structure of a plain dump

A plain dump is a single text stream: a preamble, then for each table a `CREATE TABLE`
statement, and later a `COPY` block containing the rows. PostgreSQL emits its entire
*pre-data* section before its *data* section, which guarantees that every `CREATE TABLE`
is seen before the `COPY` that needs it. That ordering is what makes a single forward
pass possible with no seeking and no buffering of the dump.

```sql
COPY public.users (id, email, created_at) FROM stdin;
1	alice@example.com	2024-01-01 00:00:00
2	\N	2024-01-02 00:00:00
\.
```

[Figure 2-1] Structure of a COPY block. Field separators are single tab characters.

### 2. Properties the design depends on

1. Fields are tab-delimited and rows are newline-delimited.
2. `\N` denotes NULL, and is distinct from an empty string.
3. Literal newlines and tabs inside field data are escaped. They are emitted as the
   two-character sequences `\n` and `\t`, never as a raw `0x0A` or `0x09` byte.

Property 3 is load-bearing. It means that **a raw newline byte in the stream is always a
row terminator**, unconditionally and with no parsing context required. That single fact
is what permits the decoder to be parallelised, as described in Chapter IV, Section 3.
Without it the stream would have to be decoded strictly sequentially.

Each block terminates with a line containing exactly `\.`.

---

## III. Pipeline Architecture

### 1. Stage overview

Each stage is a module. Data flows in one direction and no stage reads back.

`<Table 3-1>` Pipeline stages and responsibilities

| Stage | Module | Responsibility |
|---|---|---|
| Read | `dump.rs` | Open the dump, decompress it, and hand out newline-aligned byte chunks |
| Scan | `scan.rs` | Track DDL and COPY block boundaries, and recover table and column names |
| Decode | `copy.rs` | Split COPY TEXT bytes into rows and fields, resolving escapes |
| Type map | `types.rs` | Map PostgreSQL type names to the internal type model |
| Parse | `values.rs` | Convert unescaped field bytes into typed values |
| Build | `builders.rs` | Append typed values into Arrow arrays |
| Write | `sink.rs` | Encode Arrow batches to Parquet, then commit to Delta |
| Transport | `chan.rs` | Bounded multi-producer, multi-consumer channel feeding the decode pool |
| Orchestrate | `pipeline.rs` | Wire the stages together and own the threads |
| Bind | `python.rs` | Translate the Python surface onto `pipeline.rs` |

`types.rs` and `values.rs` are deliberately free of any Arrow dependency. They resolve the
*text* of a type and the *text* of a value respectively, which is the intricate part, so
both can be tested without compiling the Delta write path.

### 2. Read

Opens a file or file descriptor and produces byte chunks. Deliberately synchronous
(`std::io::Read`), because the input is a local file or FUSE-mounted path read
sequentially at line rate. Asynchrony would add complexity and provide no benefit.

Because the source is a delivered file rather than a spawned process, this module
contains no subprocess handling, no credentials, and no connection strings.

Compression is detected from the stream's leading magic bytes rather than from
configuration or a file extension, so a change of format by the sender is handled rather
than misread. Three outcomes are possible:

- `1f 8b`, gzip, is decoded. `MultiGzDecoder` is used rather than `GzDecoder`, because a
  concatenated archive would otherwise stop silently at the end of its first member,
  which is a truncated load reported as success.
- `28 b5 2f fd`, zstd, is **recognised but not decoded**, and fails the load with a
  precise error. Support is not compiled in, and recognising the format is what turns a
  corrupt parse into a clear message.
- Anything else is treated as plain text, which is what the current feed delivers.

Note the asymmetry: an *unrecognised* format is assumed to be plain text and will fail
later in the scanner, whereas a *recognised but unsupported* one fails immediately. Adding
zstd is a small change (one crate and one match arm), and should be made before asking the
sender to switch, not after.

**Decompression is the pipeline's only unavoidable serial stage.** A compressed stream
must be decoded in order, so unlike row decoding it does not scale with cores, and it
sits in front of every other stage. It therefore sets the floor on total runtime. Measured
gunzip throughput is 349 MiB/s against a row decoder at 1068 MiB/s per core, so with gzip
input the decompressor governs the run and is roughly three times the cost of the decode
it feeds. Where gzip is fixed, a faster backend such as zlib-ng is worth the build
complexity, because no amount of parallelism elsewhere will compensate; it is available
behind the `fast-gzip` feature. A plain-text feed avoids the stage altogether and is the
case the current deployment is sized for.

### 3. Scan

A two-state machine over the stream.

- **`Sql`** accumulates statements. It extracts column names and type names from
  `CREATE TABLE`, and recognises `COPY <table> (<cols>) FROM stdin;` as the transition
  into data.
- **`Copy`** consumes row data until a line that is exactly `\.`.

DDL splitting is paren-depth and quote aware, because a naive split on commas fails on
`numeric(10,2)`, `character varying(255)`, `timestamp(3) without time zone`, table-level
constraints, `GENERATED ALWAYS AS (...) STORED`, and quoted identifiers containing `""`.

This stage is **strictly sequential** and cannot be parallelised, because DDL must be
observed in order. It is not a bottleneck: DDL is a negligible fraction of total bytes in
a dump whose size is dominated by rows.

The scanner additionally parses the preamble comments `-- Dumped from database version`
and `-- Dumped by pg_dump version`. These are the only version signal available when
reading a file, and are used to reject a major version that has not been qualified.

### 4. Decode

The hot path. It splits a chunk into rows on `0x0A`, splits rows into fields on `0x09`,
and resolves backslash escapes, using `memchr` for SIMD-accelerated scanning.

The implementation must remain zero-copy. Fields are `&[u8]` slices into the chunk
buffer, with no `String` per field and no allocation per row. At tens of gigabytes per
run, a single per-field allocation is the difference between minutes and hours.

### 5. Type mapping and array building

Type names are mapped to Arrow types as specified in Chapter VIII. Decoded bytes are then
appended into Arrow column builders, producing `RecordBatch` values bounded by
`batch_rows` and `batch_bytes`.

### 6. Write

Batches are encoded to Parquet and written through delta-rs.

Note that `batch_bytes` bounds the *in-memory Arrow batch*, not the output file. Several
batches accumulate into each Parquet file, targeting roughly 256 MB to 1 GB written.
Emitting one small file per batch across hundreds of tables produces small-file sprawl
that degrades every downstream query.

---

## IV. Concurrency Model

This is the aspect most likely to be misunderstood when modifying the library, so it is
specified precisely.

### 1. Execution model by component

`<Table 4-1>` Execution model by component

| Component | Model | Executes on |
|---|---|---|
| Dump reading | Synchronous | Reader thread |
| Scanning (DDL, block boundaries) | Synchronous | Reader thread |
| COPY decoding | Synchronous, CPU-bound | Dedicated OS thread pool |
| Arrow array building | Synchronous, CPU-bound | Same decode threads |
| Parquet encoding | Synchronous, CPU-bound | Same decode threads |
| Object-store I/O | Asynchronous | Tokio runtime |
| Delta commits | Asynchronous | Tokio runtime |
| Python entry point | Synchronous, blocking | Calling Python thread, GIL released |

Stated in one sentence: **the core is synchronous, and only the storage edge is
asynchronous.**

### 2. Rationale for the division

Asynchrony and threading solve different problems, and conflating them is the most common
source of pathological performance in Rust data pipelines.

- **Asynchrony serves I/O concurrency.** It allows one thread to hold many network
  requests in flight, because each is mostly waiting. Object-store latency is high and
  highly parallelisable, so this is appropriate for writes.
- **Threading serves CPU parallelism.** Decoding waits on nothing; it saturates a core.
  Asynchrony offers it no benefit.

The library is asynchronous only where it must be. `deltalake` exposes an async-only API
and requires a Tokio runtime, so a runtime exists, but it is confined to storage
operations.

### 3. Parallel decoding

Because a raw newline always terminates a row, as established in Chapter II, the stream
can be cut at newline boundaries and the resulting pieces decoded independently.

- **A.** The reader thread accumulates approximately 16 MB, then moves the cut back to the
  last newline in the buffer using a reverse `memchr`, carrying the remainder into the
  next chunk.
- **B.** The scanner classifies each chunk. Only the row interiors of an open `COPY` block
  leave the reader; DDL is handled in place.
- **C.** Row bytes are handed round-robin, at chunk granularity, to one of `threads`
  workers over a per-worker bounded channel.
- **D.** Each worker decodes whole rows, builds Arrow batches, and **encodes Parquet into
  its own writer**. By construction no row spans a chunk, so workers never coordinate over
  data. Putting the Parquet encoder on the worker is what keeps the expected bottleneck off
  a single thread.
- **E.** At the end of a block the reader broadcasts a close and collects one
  acknowledgement per worker, each carrying that worker's staged `Add` actions and its row
  and substitution counts. Those are pooled and committed together in Phase 2.

This requires a single pass, no seeking, and no second read of the file. Throughput
scales with available cores.

Round-robin at chunk granularity has a useful secondary property: a table smaller than one
chunk is seen by exactly one worker and therefore produces exactly one file. Only tables
large enough to span chunks fan out, which is precisely where several files are wanted.
Were work distributed per row instead, every one of several hundred small tables would
emit one small file per worker.

Measured on synthetic COPY TEXT data carrying real entropy, at a 3.0x gzip ratio
(`cargo run --release --example throughput`):

| Stage | Rate | 48 GB | 480 GB |
|---|---|---|---|
| gunzip (serial) | 349 MiB/s | 2.3 min | 23.5 min |
| Row decode (per core) | 1068 MiB/s | 0.8 min | 7.7 min |
| Scan (serial) | 8681 MiB/s | 6 s | 56 s |

The fixture matters here. An earlier version repeated a handful of strings per row,
compressed 22x, and made gunzip appear to run at 3500 MiB/s, because the decoder was
copying long matches rather than working. Only a fixture with realistic entropy gives a
usable decompression figure.

This revises the emphasis of the parallel design rather than its substance. Decoding is
not the bottleneck and would not become one at ten times the current volume. Note in
particular that with gzip input **a single decode thread outpaces the decompressor by
three to one**, so the decode pool can be small: it exists to keep the design from having
a hard single-core ceiling, and to absorb a future switch to a faster compression format,
not because decoding is scarce today. The chunking scheme is retained because it costs
almost nothing and is the same property a future multi-node split would rely on.
The practical bottleneck order is decompression first, since it is serial and cannot be
parallelised at all, then Parquet encoding, then object-store upload, with row decoding a
distant last. See Chapter III, Section 2.

Phase two is concurrent as well, at [`LoadConfig::commit_concurrency`] commits at once.
That is a separate dimension from the decode pool: a commit is a metadata write waiting on
a storage round trip, not CPU work, so it is sized by latency rather than by cores. With
hundreds of small tables, committing serially turns the burst into a queue of round trips,
which is the tail described in Chapter I.

Concurrency does not weaken the failure model, and it changes one detail of the report.
The commit that fails need not be the last one attempted, so the count carried by
`CommitFailed` is how many succeeded rather than how far a loop had progressed. That is
the more useful number in either case, since it is what says whether anything became
visible.

Row order is not preserved across chunks. Delta tables are unordered sets, so this is
correct. A chunk index may be carried if determinism is ever required.

### 4. Invariants

Violating any of the following produces stalls or deadlocks that are difficult to
diagnose.

- **A.** Never perform decoding on a Tokio worker thread. CPU work on an async worker
  blocks that worker and starves every task scheduled on it. Use the decode pool or
  `spawn_blocking`.
- **B.** Never call `block_on` from inside an async context. It deadlocks.
- **C.** Cross the synchronous and asynchronous boundary only at defined points, namely
  the sink.
- **D.** Bound every channel. Backpressure is the memory bound, as described in Chapter V.
- **E.** Never panic across a thread boundary. Errors travel as values.

### 5. The Global Interpreter Lock

The Python entry point is an ordinary blocking call. It wraps the entire run in
`Python::detach`, releasing the GIL so that other Python threads continue to run. The
method was named `Python::allow_threads` before PyO3 0.29 and is still described that way
in much of the surrounding literature.

The GIL is reacquired on every progress callback, which happens once per chunk and once
per table, and each such reacquisition checks for signals so that `KeyboardInterrupt`
works. Callbacks should therefore be inexpensive. A signal, or an exception raised by the
caller's own callback, is restored as the pending Python exception and re-raised once the
load has unwound, so the caller sees the exception they raised rather than a generic
interrupt. Decode workers never touch Python.

### 6. Disadvantages

Stated plainly, because they are real.

- **Two concurrency systems coexist** in one process. Contributors must know which
  context they are in. This document exists partly to make that possible.
- **A Tokio runtime is created even for local `/Volumes` writes**, where asynchrony
  provides little benefit. The cost is small but not zero.
- **Backpressure must be explicit.** The synchronous and asynchronous halves share no
  scheduler, so bounded channels are the only mechanism preventing unbounded memory
  growth.
- **Errors must be marshalled** across thread boundaries rather than propagating
  naturally with `?`.

### 7. Advantages

- CPU parallelism scales independently of I/O concurrency, and neither starves the other.
- High object-store latency is hidden behind many concurrent uploads.
- The decoder is pure synchronous code: trivially unit-testable, deterministic, and
  requiring no async test harness.
- The Python caller sees a simple blocking function.

### 8. Known ceiling

This is a **single-node** design, scaling to the cores and network bandwidth of one
Databricks driver. That should comfortably cover the high hundreds of gigabytes.

Beyond that point, the next step is distributing COPY-block byte ranges across Spark
executors, which is a genuinely different architecture requiring seekable input and a
boundary-index pass. It is not built, but the chunking scheme above is deliberately
compatible with it, since both rest on the same newline-splittability property.

---

## V. Resource Model

Peak memory is **O(1) in dump size**. A 480 GB dump costs the same footprint as a 48 GB
dump; only wall-clock time scales.

```
threads x (batch_bytes + one Parquet write buffer)      the open block
  + threads x queue depth x chunk size                  the decode queue
```

[Figure 5-1] Determinants of peak memory

Only one `COPY` block is open at a time, because the scanner is sequential, so the first
term does not multiply by the table count. Neither term involves the size of the dump.

The practical consequence is that `threads` and `batch_bytes` multiply. The default
`batch_bytes` of 128 MB across sixteen workers is two gigabytes of builders before any
Parquet buffer is counted, which is the figure to check first on a small driver. Lower
`batch_bytes`, not `threads`, when memory is tight: batches are flushed by whichever bound
is reached first, so a smaller byte bound costs nothing but more frequent flushes.

To this are added hard caps on maximum field bytes, maximum row bytes, and maximum column
count. A malformed or hostile dump must not exhaust the driver. The row cap additionally
bounds how far the reader will grow a chunk around one very long line.

---

## VI. Failure Model

The governing principle is that **a load which reports success must be complete.** A
silently truncated table committed as success is data corruption, and is the worst
outcome this library can produce.

### 1. All-or-nothing loading

The requirement is that either the whole dump loads or it fails. No partial run is
acceptable.

Delta provides **no cross-table transaction**. Atomicity is per-table, and a dump with
hundreds of tables means hundreds of independent commits. The requirement is satisfied by
a two-phase load.

- **Phase 1, decode and stage.** Consume the entire dump, writing every Parquet file for
  every table, and committing nothing. Data files not referenced by a transaction log are
  invisible to Delta readers. They do not exist as far as any query is concerned.
- **Phase 2, commit.** Only once the stream has been consumed cleanly, commit all tables.

Any failure in Phase 1 leaves orphaned files and **zero visible change**. The non-atomic
window shrinks from the full multi-hour decode to a metadata-only commit burst at the end.

This is not strictly atomic. A reader active during Phase 2 can observe a mixture of the
new load and the previous one. Should a consumer ever require strict atomicity, the
established pattern is a manifest table: write each run under a new load identifier, then
flip the entire set with a single commit to a pointer table that views resolve. This is
deferred, because it constrains how every downstream query must be written.

Orphaned files from a failed run are invisible but not free. `VACUUM` must be scheduled.

### 2. Classification of faults

**Structural faults fail the load**, because the data is wrong:

- A COPY block that does not close with `\.`
- EOF encountered inside a COPY block, which is the classic truncated-transfer signature
- A row whose field count disagrees with its column count
- A `Dumped by` major version that has not been qualified
- A table name that escapes the output prefix, as described in Chapter IX
- Any resource limit from Chapter V being exceeded

**Type uncertainty degrades and never fails.** An unrecognised PostgreSQL type maps to
`Utf8`, preserving the literal text, and is reported in the returned statistics. Across
hundreds of third-party tables the type zoo is wide. One unknown type must not kill a
scheduled load, and nothing is lost, because text can be reinterpreted later.

### 3. Truncation detection

Plain dumps carry no row counts, so there is nothing to reconcile against. The integrity
check is therefore structural: every COPY block must close with `\.`, and EOF inside a
block is a hard error. On a large scheduled transfer a truncated file is a realistic
failure mode, and this is the only mechanism that detects it.

---

## VII. Version Handling

`pg_dump` output is stable in the respects this library depends upon. The COPY TEXT
format is unchanged across supported major versions, so the decoder is version-agnostic.
Version differences reside in DDL and the preamble, which are the scanner's concern.

The preamble's `-- Dumped by pg_dump version` line is parsed and checked against the set
of qualified major versions. An unqualified major version **fails the load** rather than
being parsed speculatively. This is the tripwire for a source system being upgraded
without notice, which on an unattended feed from a third party is a matter of when rather
than if.

---

## VIII. Type Mapping

### 1. Mapping table

`<Table 8-1>` PostgreSQL to Arrow type mapping

| PostgreSQL | Arrow |
|---|---|
| `smallint`, `int2` | `Int16` |
| `integer`, `int4` | `Int32` |
| `bigint`, `int8` | `Int64` |
| `real` | `Float32` |
| `double precision` | `Float64` |
| `numeric(p,s)` where 1 <= p <= 38 and 0 <= s <= p | `Decimal128(p,s)` |
| `numeric` unconstrained, or outside that range | `Utf8` |
| `boolean` | `Boolean` |
| `date` | `Date32` |
| `timestamp` | `Timestamp(Micros, None)` |
| `timestamptz` | `Timestamp(Micros, UTC)` |
| `time` | `Time64(Micros)` |
| `bytea` | `Binary` |
| `text`, `varchar`, `char`, `uuid`, `json`, `jsonb`, `inet`, enums, arrays, `interval` | `Utf8` |
| Anything unrecognised | `Utf8`, reported in statistics |

Arrays and `interval` are retained as their unescaped PostgreSQL literal. This is honest:
it avoids guessing at a structure the caller may not want, and the text remains
convertible downstream.

### 2. Edge cases

`bytea` is accepted in both output formats, the modern hexadecimal `\x` form and the
older escape form, distinguished by their leading bytes so that a dump written under
either `bytea_output` setting is read correctly. ` BC` suffixed dates are converted to
proleptic Gregorian years, PostgreSQL counting BC years from 1 where the proleptic
calendar counts through zero.

Two values have no Arrow encoding at all, and the substitution chosen for them is a
judgement rather than a fact:

- **`infinity` and `-infinity`** in a date or timestamp column become NULL. The
  alternative, saturating to the extreme representable day, would place a year in the
  millions into the column and quietly corrupt every downstream aggregate. NULL is the
  closer equivalent, since an infinite date usually carries the sense of "no bound".
- **`NaN`** in a `numeric` column becomes NULL, for the same reason. Note that `NaN` and
  the infinities in `real` and `double precision` columns are preserved exactly, because
  IEEE 754 represents them.

A third degradation is structural rather than per value. PostgreSQL 15 and later accept a
`numeric` whose scale is negative or exceeds its precision, such as `numeric(5,-2)` or
`numeric(2,5)`. Arrow's `Decimal128` requires `0 <= scale <= precision <= 38`, so such a
column is written as text in its entirety, and its values are preserved as the literal the
dump carried. This is decided once, when the type is resolved, so the schema and the array
builder cannot disagree; deciding it per value would produce a batch whose arrays did not
match their own schema.

Both per-value substitutions are **counted per column and reported in the run statistics**,
so they are visible rather than silent. A column with a high `infinity` count is a signal that the
source table wants a different mapping, and the honest fix is to declare it text.

Text columns carrying bytes that are not valid UTF-8 **fail the load**. Arrow strings are
UTF-8, and substituting replacement characters would corrupt values silently. Failing lets
the operator address the source encoding, which is the real problem.

A value that contradicts its declared type, such as text in an integer column, also fails
the load. That is structural: the dump disagrees with its own DDL.

### 3. Schema changes between runs

A third-party feed changes its DDL, so the schema a dump declares need not match the one
the Delta table already carries. The two must be reconciled in the same commit, or the
table declares one shape while its files hold another.

Under [`WriteMode::Overwrite`] the dump is treated as the truth. Three things happen in one
commit: the previous run's files are tombstoned, the new files are added, and a `Metadata`
action carrying the dump's schema is emitted. The decode workers are given that schema
explicitly rather than reading it from the table's metadata, because the metadata still
describes yesterday until the commit lands.

The comparison is made between kernel types on both sides. Comparing an Arrow type against
a Delta one as rendered text does not work, since the kernel writes `Primitive(Integer)`
where Arrow writes `Int32`, and every column would appear to have changed. Converting first
also means the drift report and the decision to emit `Metadata` come from the same
comparison, so they cannot disagree.

Under [`WriteMode::Append`] a difference is refused. Appending rows shaped one way to a
table declared another way has no defensible meaning, and guessing at one is exactly the
class of silent corruption this library exists to avoid.

The difference is reported per table in the run statistics, whether or not it was applied.

### 4. Timestamps

Delta's `timestamp` is microseconds UTC. `timestamp_ntz` requires reader v3 and writer
v7, which breaks the compatibility floor established in Chapter X. Naive PostgreSQL
timestamps are therefore **assumed to be UTC**, and that assumption is documented at the
Python surface. An opt-in `naive_timestamps="ntz"` may be added later for callers who can
accept the higher protocol requirement.

---

## IX. Security Model

The dump is **untrusted input from an external party**, processed unattended. A third
party controls every table name, column name, and field byte the library will ever see.
Security was the starting point of the design, not a review pass.

- **A.** `#![forbid(unsafe_code)]`.
- **B.** **Path-traversal guard on the table-to-path mapping.** `../` is a legal quoted
  PostgreSQL identifier. An attacker-controlled table name must not escape the output
  prefix. Validate and reject; never sanitise silently.
- **B2.** **Schema and table are never joined into one string.** A dot is legal inside a
  quoted identifier, so `public."a.b"` and schema `public.a` table `b` are
  indistinguishable once joined, and would map to the same output path. The parts are
  carried separately from the scanner through to the path mapping, and each is validated
  on its own. A dot appearing inside an identifier is rejected rather than split.
- **C.** **Bounded limits** on maximum field bytes, maximum row bytes, and maximum column
  count. These are the only barrier between a malformed dump and an out-of-memory driver.
- **D.** **Never log row data.** Error messages identify positions and tables, never
  contents.
- **E.** **Checked integer parsing throughout.** No silent wrapping.
- **F.** Errors carry no input-derived payloads that could leak data into a traceback.

Because dumps arrive as files, the library spawns no subprocess. There is no command
line, no `PGPASSWORD`, no connection string, and no TLS configuration. An entire class of
credential-handling risk is absent by construction rather than by mitigation.

---

## X. Deployment Constraints

- Write to an **external location or a `/Volumes/...` FUSE path**, never to a Unity
  Catalog *managed* table. Third-party writers can corrupt UC-managed tables.
- Keep tables at **reader v1 and writer v2**, with no deletion vectors and no column
  mapping, so that any DBR version can read the output.
- Schedule `VACUUM`. Full overwrites tombstone the previous run's files across every
  table, and storage grows quietly without it.

---

## XI. Assessment

### 1. Advantages

- No intermediate database, and therefore no storage ceiling, no standing cost, no
  cross-run state, no second copy of the data at rest, and no second set of credentials.
- Constant memory regardless of dump size.
- Throughput scales with cores.
- One pass over the data, with no write amplification.
- Small, widely used dependency surface.
- The unsupported cross-version restore path is removed entirely.

### 2. Disadvantages

- **The project owns a PostgreSQL DDL parser.** PostgreSQL maintains its own; this
  library maintains a second one. That is the central cost of the design: not writing it,
  but owning it. It is made tractable by needing only column names and types, and by
  routing the entire long tail of types to `Utf8`.
- **Type coverage is deliberately shallow.** Arrays, composites, ranges, and `interval`
  arrive as text.
- **Single-node ceiling**, as described in Chapter IV, Section 8.
- **Not strictly atomic across tables**, as described in Chapter VI, Section 1.
- **Plain format only.** Custom-format archives are unsupported.
- **Full load on every run.** There is no incremental or CDC path.
- **Two concurrency models** in one codebase, as described in Chapter IV, Section 6.
- **A `numeric` that Arrow cannot hold is silently written as text.** It is visible in the
  resulting schema, but unlike an unrecognised type it is not called out in the run
  statistics. See Chapter VIII, Section 2.
- **Row order within a table is not preserved.** Each worker stages its own files. Delta
  tables are unordered sets so this is correct, but a caller porting from a system that
  happened to preserve insertion order should know.

### 3. Conditions under which this design is inappropriate

If dumps are small, infrequent, and a PostgreSQL instance already exists for other
reasons, restoring into it and reading it back out is simpler and entirely defensible.
The cost of that approach scales with data size while its benefit does not, which is
precisely why it ceases to be the right answer as volume grows.

---

## XII. Dependencies

The dependency surface is kept deliberately minimal, admitting only widely used and
actively maintained crates.

`<Table 12-1>` Direct dependencies

| Crate | Version | Rationale | Documentation |
|---|---|---|---|
| `pyo3` | 0.29.2 | Python bindings | [docs.rs](https://docs.rs/pyo3/0.29.2/pyo3/), [crates.io](https://crates.io/crates/pyo3) |
| `deltalake` | 0.32.4 | Delta write path; transitively supplies arrow, parquet, object_store, tokio, chrono | [docs.rs](https://docs.rs/deltalake/0.32.4/deltalake/), [crates.io](https://crates.io/crates/deltalake) |
| `memchr` | 2.8.3 | SIMD scanning for newlines and tabs in the hot loop | [docs.rs](https://docs.rs/memchr/2.8.3/memchr/), [crates.io](https://crates.io/crates/memchr) |
| `flate2` | 1.1.10 | gzip decoding. The default backend is pure Rust, so a wheel builds with no C toolchain | [docs.rs](https://docs.rs/flate2/1.1.10/flate2/), [crates.io](https://crates.io/crates/flate2) |
| `tokio` | 1.53.1 | Runtime for the storage edge. Already in the `deltalake` tree; declared so `sink.rs` and `pipeline.rs` may name it | [docs.rs](https://docs.rs/tokio/1.53.1/tokio/), [crates.io](https://crates.io/crates/tokio) |
| `futures` | 0.3.34 | Stream combinators over the Delta file listing. Already in the `deltalake` tree | [docs.rs](https://docs.rs/futures/0.3.34/futures/), [crates.io](https://crates.io/crates/futures) |

Use the `deltalake::arrow` re-exports rather than depending on `arrow` directly, so as to
avoid version skew against the arrow release that delta-rs pins.

`/Volumes` FUSE paths require no object-store feature. `abfss://` requires the `azure`
feature of `deltalake`.

`tokio` and `futures` are pinned to the versions `deltalake` already resolves, so
declaring them builds no duplicate copy.

Three optional features are defined. `extension-module` links the Python extension against
the interpreter that loads it and is enabled only for the wheel build, never for
`cargo test`, which needs to link libpython. `fast-gzip` selects the zlib-ng backend and
requires a C toolchain and cmake. `azure` adds the object-store backend for `abfss://`
output, which a `/Volumes` FUSE path does not need.

Deliberately excluded:

- **`thiserror`.** A hand-rolled error enum is roughly forty lines, and removes a
  dependency from a library whose merit is a small surface.
- **`sqlparser`.** `pg_dump` emits constructs it rejects, and dump output is
  machine-generated and regular enough to scan directly.

The project is built with maturin.

---

## References

1. PostgreSQL Global Development Group. *COPY*, PostgreSQL 17 Documentation.
   https://www.postgresql.org/docs/17/sql-copy.html
2. PostgreSQL Global Development Group. *pg_dump*, PostgreSQL 17 Documentation.
   https://www.postgresql.org/docs/17/app-pgdump.html
3. PostgreSQL Global Development Group. *Upgrading a PostgreSQL Cluster*, PostgreSQL 17
   Documentation. https://www.postgresql.org/docs/17/upgrading.html
4. Delta Lake Project. *Delta Transaction Log Protocol*.
   https://github.com/delta-io/delta/blob/master/PROTOCOL.md
5. Delta Lake Project. *delta-rs*. https://github.com/delta-io/delta-rs
6. Databricks. *External locations*.
   https://docs.databricks.com/aws/en/connect/unity-catalog/external-locations
7. Apache Arrow Project. *Arrow Columnar Format*.
   https://arrow.apache.org/docs/format/Columnar.html
8. Apache Arrow Project. *arrow-rs*. https://docs.rs/arrow/latest/arrow/
9. Tokio Project. *Tokio Documentation*. https://docs.rs/tokio/latest/tokio/
10. Tokio Project. *`spawn_blocking`*.
    https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html
11. Rust Project. *Asynchronous Programming in Rust*.
    https://rust-lang.github.io/async-book/
12. PyO3 Project. *Parallelism*. https://pyo3.rs/latest/parallelism.html
13. maturin Project. *maturin User Guide*. https://www.maturin.rs/

---

## Appendix A. Glossary

`<Table A-1>` Glossary of terms

| Term | Definition |
|---|---|
| COPY TEXT | PostgreSQL's default tab-delimited bulk data format, using backslash escapes |
| Pre-data and data sections | `pg_dump` output phases, in which all DDL precedes all rows |
| RecordBatch | Arrow's unit of columnar data: a bounded set of rows across all columns |
| Tombstone | A Delta log entry marking a data file as removed; the file persists until `VACUUM` |
| Reader and writer version | Delta protocol levels a client must support to read or write a table |
| Backpressure | A bounded queue forcing a fast producer to wait for a slow consumer |
| Zero-copy | Referencing bytes in an existing buffer rather than copying them out |
