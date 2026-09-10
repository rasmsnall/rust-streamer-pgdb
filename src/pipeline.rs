//! Orchestration: the dump goes in, Delta tables come out.
//!
//! This module wires the other stages together. It pulls the dump through
//! [`crate::dump::ChunkReader`], feeds each chunk to [`crate::scan::Scanner`], fans the
//! rows of every `COPY` block to a pool of decode workers, and each worker decodes with
//! [`crate::copy`], batches with [`crate::builders`], and encodes Parquet with a
//! [`crate::sink::TableWriter`].
//!
//! # Inputs and outputs
//!
//! Input is any [`std::io::Read`], normally a file. Output is one Delta table per source
//! table beneath [`LoadConfig::output_uri`]. The return value is a [`LoadReport`]:
//! per-table row counts, the type substitutions that were applied, and the `pg_dump`
//! versions recovered from the preamble.
//!
//! # Two-phase load
//!
//! Delta has no cross-table transaction, so a dump with hundreds of tables commits
//! hundreds of times. To keep the load all-or-nothing, [`run`] decodes the **entire**
//! dump and stages every table's Parquet without committing anything (phase one), then
//! commits every table once the stream has been consumed cleanly (phase two). A failure
//! in phase one leaves orphaned files and no visible change; see [`crate::sink`] for why
//! that is safe and why `VACUUM` must be scheduled.
//!
//! # Threading
//!
//! [`run`] is **blocking and synchronous**. It builds a private multi-threaded Tokio
//! runtime for the storage calls, so it must not be called from inside an existing Tokio
//! runtime.
//!
//! The **reader** stays on the calling thread: the scanner is strictly sequential because
//! `CREATE TABLE` DDL must be read in order and `COPY` block boundaries must be observed.
//! Only row bytes leave it. Each `COPY` payload is copied once and handed, round-robin at
//! chunk granularity, to one of [`LoadConfig::threads`] **decode workers**. A worker
//! decodes whole rows (no row spans a chunk, because a raw newline always terminates a
//! row in COPY TEXT) and encodes Parquet into its own [`crate::sink::TableWriter`], so
//! Parquet encoding, the expected bottleneck, runs on every core at once. Round-robin at
//! chunk granularity keeps a small table, which fits in one chunk, on a single worker and
//! therefore in a single file.
//!
//! Row order within a table is not preserved: each worker stages its own files. Delta
//! tables are unordered, so this does not matter.
//!
//! Peak memory is `threads * (batch_bytes + one Parquet write buffer)` for the single
//! open block, plus the bounded job queue, plus the batches in flight. It does not grow
//! with the dump size or the table count. With many threads and a large `batch_bytes`
//! this is the number to watch.

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use deltalake::DeltaTable;
use deltalake::kernel::Action;
use tokio::runtime::Handle;

use crate::builders::{self, BatchBuilder};
use crate::chan::{self, bounded};
use crate::copy::{self, Limits};
use crate::dump::{ChunkReader, decompressed};
use crate::error::{Error, Result};
use crate::scan::{Event, Scanner, TableDef, TableName};
use crate::sink::{TableWriter, WriteMode, commit_table, open_table};
use crate::types::{self, ResolvedType};

/// Per-worker job queue depth. Bounds how many undecoded chunks can be in flight per
/// worker, and with the chunk size sets the decode queue's memory.
const QUEUE_DEPTH: usize = 3;

/// Settings for one load.
///
/// [`LoadConfig::default`] supplies the batch bounds documented on the Python surface and
/// leaves `output_uri` empty, which [`run`] will reject.
#[derive(Debug, Clone)]
pub struct LoadConfig {
    /// Prefix every table is written beneath, for example `/Volumes/main/raw/pg/` or an
    /// `abfss://` URL. A table's path within it is derived from its qualified name and is
    /// validated against traversal by [`crate::sink::relative_path`].
    pub output_uri: String,
    /// Qualified names of the tables to load. `None` loads every table in the dump; a
    /// `COPY` block for any other table is scanned for its terminator and otherwise
    /// skipped, which costs almost nothing.
    pub tables: Option<Vec<String>>,
    /// How an existing Delta table is treated. See [`WriteMode`].
    pub mode: WriteMode,
    /// Row ceiling for one in-memory Arrow batch.
    pub batch_rows: usize,
    /// Byte ceiling for one in-memory Arrow batch, measured on the decoded field bytes.
    /// Bounds memory; it does not bound the Parquet file size.
    pub batch_bytes: usize,
    /// Decode workers to run. `0` picks [`std::thread::available_parallelism`].
    pub threads: usize,
    /// Backend-specific options passed to `object_store`, for example credentials for an
    /// `abfss://` target.
    pub storage_options: HashMap<String, String>,
    /// When set, the load fails unless the dump's `pg_dump` major matches exactly. This
    /// is the tripwire for the source system being upgraded without notice.
    pub expect_pg_major: Option<u32>,
    /// Bounds enforced on every field, row, and column while decoding. `max_row_bytes`
    /// also caps how far the reader will grow a chunk around one very long line.
    pub limits: Limits,
}

impl Default for LoadConfig {
    fn default() -> Self {
        Self {
            output_uri: String::new(),
            tables: None,
            mode: WriteMode::Overwrite,
            batch_rows: 100_000,
            batch_bytes: 128 << 20,
            threads: 0,
            storage_options: HashMap::new(),
            expect_pg_major: None,
            limits: Limits::default(),
        }
    }
}

/// Progress delivered to the callback passed to [`run`].
///
/// One is emitted after each chunk with `table` set to `None`, and one after each `COPY`
/// block closes with `table` set to that table. The callback returns `false` to abort the
/// load, which fails it with [`Error::Interrupted`] before anything is committed.
#[derive(Debug, Clone)]
pub struct Progress {
    /// Bytes taken from the input so far, counted before decompression so the figure is
    /// comparable with the size of the dump on disk.
    pub bytes_read: u64,
    /// Rows decoded across all tables so far.
    pub rows: u64,
    /// Tables whose `COPY` block has closed so far.
    pub tables_done: usize,
    /// The table that just finished, when this update marks a block boundary.
    pub table: Option<String>,
}

/// What one source table produced.
#[derive(Debug, Clone)]
pub struct TableStats {
    /// Qualified name as written in the dump.
    pub table: String,
    /// Rows decoded from the table's `COPY` block.
    pub rows: u64,
    /// Arrow batches encoded to Parquet for this table, summed across workers.
    pub batches: u64,
    /// The Delta version the phase-two commit produced.
    pub delta_version: u64,
    /// Columns where a valid but unrepresentable value (`infinity`, `NaN`) was stored as
    /// NULL, with the count per column. Empty when nothing was substituted.
    pub null_substitutions: Vec<(String, u64)>,
    /// Columns whose declared type was not recognised and so were written as text, paired
    /// with the declaration as the dump wrote it. Empty when every type was recognised.
    pub text_fallback_columns: Vec<(String, String)>,
}

/// The outcome of a completed load.
#[derive(Debug, Clone)]
pub struct LoadReport {
    /// `pg_dump` major version that wrote the dump, from `-- Dumped by pg_dump version`.
    pub dumped_by: u32,
    /// Source server major version, from `-- Dumped from database version`, when stated.
    pub from_database: Option<u32>,
    /// Compression that was decoded off the input, or `"none"`.
    pub compression: &'static str,
    /// Bytes taken from the input, counted before decompression.
    pub bytes_read: u64,
    /// Rows decoded across every loaded table.
    pub total_rows: u64,
    /// One entry per loaded table, in the order their blocks closed.
    pub tables: Vec<TableStats>,
}

/// Resolves each `COPY` column against the table's `CREATE TABLE` definition.
///
/// The `COPY` statement lists columns in transfer order, which need not match declaration
/// order, so each name is looked up rather than taken positionally.
///
/// # Errors
///
/// [`Error::MalformedCreateTable`] if the `COPY` statement names a column the parsed DDL
/// does not carry, which means the two disagree and the dump cannot be trusted.
fn resolve_copy_columns(def: &TableDef, columns: &[String]) -> Result<Vec<ResolvedType>> {
    columns
        .iter()
        .map(|name| {
            def.columns
                .iter()
                .find(|c| &c.name == name)
                .map(|c| types::resolve(&c.sql_type))
                .ok_or_else(|| Error::MalformedCreateTable {
                    table: def.name.qualified(),
                })
        })
        .collect()
}

/// Reader-side bookkeeping for the `COPY` block currently open.
struct OpenBlock {
    generation: u64,
    table: TableName,
    qualified: String,
    columns: Vec<(String, ResolvedType)>,
}

/// Immutable description of one open `COPY` block, shared with every worker.
struct BlockCtx {
    generation: u64,
    table: DeltaTable,
    columns: Vec<(String, ResolvedType)>,
    arity: usize,
    limits: Limits,
    batch_rows: usize,
    batch_bytes: usize,
}

/// A unit of work for a decode worker.
enum Job {
    /// A new block began. Every worker gets this before any `Rows` for the block.
    Open(Arc<BlockCtx>),
    /// Newline-terminated whole rows of the block identified by `generation`.
    Rows { generation: u64, bytes: Vec<u8> },
    /// The block ended. Every worker gets this and replies with a [`BlockClosed`].
    Close { generation: u64 },
}

/// A worker's contribution to one closed block.
struct BlockClosed {
    generation: u64,
    rows: u64,
    batches: u64,
    /// Per-column count of values this worker stored as NULL because the type could not
    /// hold them. Same length and order as [`BlockCtx::columns`].
    substitutions: Vec<u64>,
    /// `Add` actions for the Parquet this worker wrote, not yet committed.
    staged: Vec<Action>,
}

/// What a worker sends back to the reader.
enum WorkerEvent {
    Closed(BlockClosed),
    Failed(Error),
}

/// One worker's decode-and-encode state for a single block.
struct WorkerBlock {
    builder: BatchBuilder,
    writer: TableWriter,
    batches: u64,
}

/// Streams `input` into Delta tables and returns what was written.
///
/// The dump is consumed in a single forward pass on the calling thread. Row decoding and
/// Parquet encoding run on a pool of [`LoadConfig::threads`] workers. Every table's
/// Parquet is written and staged during the pass; nothing is committed until the pass
/// completes without error, at which point every table is committed. A failure at any
/// point leaves orphaned files and no visible change to any table.
///
/// `progress` is called after every chunk and after every `COPY` block; returning `false`
/// aborts the load with [`Error::Interrupted`]. Pass `|_| true` to ignore it. It is the
/// hook a binding uses to check for an interrupt signal, and it is only ever called from
/// the calling thread.
///
/// # Errors
///
/// - [`Error::UnsafeTableName`] if a table name would escape `config.output_uri`.
/// - [`Error::UnsupportedDumpVersion`] if the dump's `pg_dump` major is unqualified, or
///   does not match `config.expect_pg_major` when that is set.
/// - [`Error::MissingDumpVersion`] if the preamble carried no version comment.
/// - [`Error::UnterminatedCopy`] if the stream ended inside a `COPY` block.
/// - [`Error::FieldCountMismatch`], [`Error::TruncatedEscape`], [`Error::InvalidHexEscape`],
///   [`Error::UnparsableValue`], [`Error::NonUtf8Text`], [`Error::FieldTooLarge`],
///   [`Error::RowTooLarge`], [`Error::TooManyColumns`] for a malformed or hostile dump.
/// - [`Error::MalformedCopyHeader`], [`Error::MalformedCreateTable`] for DDL the scanner
///   cannot parse.
/// - [`Error::UnsupportedCompression`] if the input is compressed in a format this build
///   was not compiled with.
/// - [`Error::TableExists`] if `config.mode` is [`WriteMode::ErrorIfExists`] and a target
///   table already holds data.
/// - [`Error::Delta`], [`Error::Arrow`], [`Error::Io`] for a storage, encoding, read, or
///   internal pool failure.
/// - [`Error::Interrupted`] if `progress` returned `false`.
/// - [`Error::Internal`] if the scanner and this module disagree about block structure,
///   which would be a defect here rather than a bad dump.
///
/// # Panics
///
/// Does not panic. A decode worker that panics is reported as [`Error::Io`], and a broken
/// invariant is reported as [`Error::Internal`] rather than unwinding, because this runs
/// underneath the Python bindings.
///
/// # Blocking
///
/// Blocks until the load finishes. Builds a private multi-threaded Tokio runtime for the
/// Delta calls, so it must not be called from within a Tokio runtime.
///
/// # Examples
///
/// ```no_run
/// use pgdelta::pipeline::{run, LoadConfig};
/// use pgdelta::sink::WriteMode;
///
/// let config = LoadConfig {
///     output_uri: "/Volumes/main/raw/pg/".into(),
///     mode: WriteMode::Overwrite,
///     expect_pg_major: Some(17),
///     ..LoadConfig::default()
/// };
/// let file = std::fs::File::open("day.sql")?;
/// let report = run(std::io::BufReader::new(file), &config, |_| true)?;
/// println!("{} tables, {} rows", report.tables.len(), report.total_rows);
/// # Ok::<(), pgdelta::Error>(())
/// ```
pub fn run<R, F>(input: R, config: &LoadConfig, progress: F) -> Result<LoadReport>
where
    R: std::io::Read + 'static,
    F: FnMut(Progress) -> bool,
{
    let threads = match config.threads {
        0 => std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4),
        n => n,
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .map_err(|e| Error::Io {
            message: e.to_string(),
        })?;
    let handle = runtime.handle().clone();

    let (event_tx, event_rx) = bounded::<WorkerEvent>(threads * 2 + 4);
    let mut job_tx: Vec<chan::Sender<Job>> = Vec::with_capacity(threads);
    let mut joins = Vec::with_capacity(threads);
    for _ in 0..threads {
        let (tx, rx) = bounded::<Job>(QUEUE_DEPTH);
        job_tx.push(tx);
        let events = event_tx.clone();
        let worker_handle = handle.clone();
        joins.push(std::thread::spawn(move || {
            worker(rx, events, worker_handle);
        }));
    }
    drop(event_tx);

    let outcome = drive(input, config, progress, &handle, &job_tx, &event_rx, threads);

    // Shut the pool down and reap it, whatever the outcome. Dropping every job sender
    // ends each worker's `recv`; the event channel has room for a final event from each.
    drop(job_tx);
    for join in joins {
        let _ = join.join();
    }
    drop(runtime);

    outcome
}

/// Opens `path` and streams it through [`run`].
///
/// # Errors
///
/// [`Error::Io`] if `path` cannot be opened, plus every error [`run`] can return.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks until the load finishes; see [`run`].
///
/// # Examples
///
/// ```no_run
/// use pgdelta::pipeline::{run_file, LoadConfig};
///
/// let config = LoadConfig {
///     output_uri: "/Volumes/main/raw/pg/".into(),
///     ..LoadConfig::default()
/// };
/// let report = run_file(std::path::Path::new("day.sql"), &config, |_| true)?;
/// # let _ = report;
/// # Ok::<(), pgdelta::Error>(())
/// ```
pub fn run_file<F>(path: &std::path::Path, config: &LoadConfig, progress: F) -> Result<LoadReport>
where
    F: FnMut(Progress) -> bool,
{
    let file = std::fs::File::open(path)?;
    run(std::io::BufReader::new(file), config, progress)
}

/// Wraps the raw input to count bytes before they reach the decompressor.
///
/// The reported figure is therefore comparable with the size of the dump on disk, which
/// is what a caller driving a progress bar needs. Counting the decompressed stream would
/// run several times past the file's length on a gzipped dump.
struct CountingReader<R> {
    inner: R,
    count: Arc<AtomicU64>,
}

impl<R: std::io::Read> std::io::Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let got = self.inner.read(buf)?;
        self.count.fetch_add(got as u64, Ordering::Relaxed);
        Ok(got)
    }
}

/// Signals that the decode pool is gone when the reader expected it to be working.
fn pool_stopped() -> Error {
    Error::Io {
        message: "decode pool stopped unexpectedly".to_string(),
    }
}

/// Sends one job to a worker. A send only fails once that worker has exited, which it
/// does only after reporting why, so on failure the real cause is waiting on `event_rx`.
fn dispatch(
    tx: &chan::Sender<Job>,
    job: Job,
    event_rx: &chan::Receiver<WorkerEvent>,
) -> Result<()> {
    tx.send(job).map_err(|_| match event_rx.recv() {
        Some(WorkerEvent::Failed(err)) => err,
        _ => pool_stopped(),
    })
}

/// The reader: scans the dump on the calling thread, drives the pool, then commits.
#[allow(clippy::too_many_arguments)]
fn drive<R, F>(
    input: R,
    config: &LoadConfig,
    mut progress: F,
    handle: &Handle,
    job_tx: &[chan::Sender<Job>],
    event_rx: &chan::Receiver<WorkerEvent>,
    threads: usize,
) -> Result<LoadReport>
where
    R: std::io::Read + 'static,
    F: FnMut(Progress) -> bool,
{
    // Count on the way in, before decompression, so the figure reported to the caller can
    // be compared against the size of the file on disk.
    let consumed = Arc::new(AtomicU64::new(0));
    let counted = CountingReader {
        inner: input,
        count: Arc::clone(&consumed),
    };
    let (compression, reader) = decompressed(counted)?;

    // A chunk is only allowed to outgrow the target when a single line does, so the row
    // ceiling is what should bound it. Without this the reader would buffer up to the
    // 256 MiB default however tightly the caller set `max_row_bytes`.
    let max_chunk = config
        .limits
        .max_row_bytes
        .max(crate::dump::DEFAULT_CHUNK_BYTES);
    let mut chunks =
        ChunkReader::with_limits(reader, crate::dump::DEFAULT_CHUNK_BYTES, max_chunk);
    let mut scanner = Scanner::new();

    let mut table_defs: HashMap<String, TableDef> = HashMap::new();
    let mut dumped_by: Option<u32> = None;
    let mut from_database: Option<u32> = None;
    let mut total_rows: u64 = 0;
    let mut tables: Vec<TableStats> = Vec::new();

    // Loaded Delta tables and their pooled, uncommitted actions, held until phase two.
    let mut open_tables: Vec<(String, DeltaTable)> = Vec::new();
    let mut staged: HashMap<String, Vec<Action>> = HashMap::new();

    let mut generation: u64 = 0;
    let mut current: Option<OpenBlock> = None;
    let mut round_robin: usize = 0;
    let mut skipping = false;

    let wanted = |name: &str| {
        config
            .tables
            .as_deref()
            .is_none_or(|t| t.iter().any(|x| x == name))
    };

    while let Some(chunk) = chunks.next_chunk()? {
        let bytes_read = consumed.load(Ordering::Relaxed);

        for event in scanner.feed(chunk)? {
            match event {
                Event::DumpVersion {
                    dumped_by: by,
                    from_database: from,
                } => {
                    if let Some(expected) = config.expect_pg_major
                        && by != expected
                    {
                        return Err(Error::UnsupportedDumpVersion { found: by });
                    }
                    dumped_by = Some(by);
                    from_database = from;
                }

                Event::Table(def) => {
                    table_defs.insert(def.name.qualified(), def);
                }

                Event::CopyStart { table, columns } => {
                    if current.is_some() {
                        return Err(Error::Internal {
                            detail: "COPY block started while another was still open",
                        });
                    }
                    let qualified = table.qualified();
                    if !wanted(&qualified) {
                        skipping = true;
                        continue;
                    }
                    let def = table_defs.get(&qualified).ok_or_else(|| {
                        Error::MalformedCreateTable {
                            table: qualified.clone(),
                        }
                    })?;
                    let resolved = resolve_copy_columns(def, &columns)?;
                    let pairs: Vec<(String, ResolvedType)> =
                        columns.iter().cloned().zip(resolved).collect();
                    let schema = builders::arrow_schema(&pairs);
                    let delta_table = handle.block_on(open_table(
                        &config.output_uri,
                        &table,
                        &schema,
                        config.mode,
                        &config.storage_options,
                    ))?;
                    open_tables.push((qualified.clone(), delta_table.clone()));

                    generation += 1;
                    let ctx = Arc::new(BlockCtx {
                        generation,
                        table: delta_table,
                        columns: pairs.clone(),
                        arity: columns.len(),
                        limits: config.limits,
                        batch_rows: config.batch_rows,
                        batch_bytes: config.batch_bytes,
                    });
                    for tx in job_tx {
                        dispatch(tx, Job::Open(Arc::clone(&ctx)), event_rx)?;
                    }
                    current = Some(OpenBlock {
                        generation,
                        table,
                        qualified,
                        columns: pairs,
                    });
                    round_robin = 0;
                }

                Event::CopyRows(payload) => {
                    if skipping {
                        continue;
                    }
                    let Some(block) = current.as_ref() else {
                        // Dropping rows here would commit a short table, which is the
                        // worst outcome this library has. Fail instead.
                        return Err(Error::Internal {
                            detail: "COPY rows arrived with no open block",
                        });
                    };
                    let worker = round_robin % job_tx.len();
                    round_robin += 1;
                    dispatch(
                        &job_tx[worker],
                        Job::Rows {
                            generation: block.generation,
                            bytes: payload.into_owned(),
                        },
                        event_rx,
                    )?;
                }

                Event::CopyEnd { table } => {
                    if skipping {
                        skipping = false;
                        continue;
                    }
                    let Some(OpenBlock {
                        generation: block_gen,
                        table: name,
                        qualified,
                        columns,
                    }) = current.take()
                    else {
                        return Err(Error::Internal {
                            detail: "COPY block ended with none open",
                        });
                    };
                    if name != table {
                        return Err(Error::Internal {
                            detail: "COPY block ended under a different name than it began",
                        });
                    }

                    for tx in job_tx {
                        dispatch(
                            tx,
                            Job::Close {
                                generation: block_gen,
                            },
                            event_rx,
                        )?;
                    }

                    let mut rows = 0u64;
                    let mut batches = 0u64;
                    let mut subs = vec![0u64; columns.len()];
                    let mut block_actions: Vec<Action> = Vec::new();
                    let mut acked = 0usize;
                    while acked < threads {
                        match event_rx.recv() {
                            None => return Err(pool_stopped()),
                            Some(WorkerEvent::Failed(e)) => return Err(e),
                            Some(WorkerEvent::Closed(c)) => {
                                debug_assert_eq!(c.generation, block_gen);
                                rows += c.rows;
                                batches += c.batches;
                                for (slot, add) in subs.iter_mut().zip(&c.substitutions) {
                                    *slot += add;
                                }
                                block_actions.extend(c.staged);
                                acked += 1;
                            }
                        }
                    }

                    total_rows += rows;
                    staged
                        .entry(qualified.clone())
                        .or_default()
                        .extend(block_actions);

                    let null_substitutions = columns
                        .iter()
                        .zip(&subs)
                        .filter(|&(_, &count)| count > 0)
                        .map(|((n, _), &count)| (n.clone(), count))
                        .collect();
                    let text_fallback_columns = columns
                        .iter()
                        .filter(|(_, rt)| !rt.recognised)
                        .map(|(n, rt)| (n.clone(), rt.source.clone()))
                        .collect();
                    tables.push(TableStats {
                        table: qualified,
                        rows,
                        batches,
                        delta_version: 0,
                        null_substitutions,
                        text_fallback_columns,
                    });

                    if !progress(Progress {
                        bytes_read,
                        rows: total_rows,
                        tables_done: tables.len(),
                        table: Some(table.qualified()),
                    }) {
                        return Err(Error::Interrupted);
                    }
                }
            }
        }

        if !progress(Progress {
            bytes_read,
            rows: total_rows,
            tables_done: tables.len(),
            table: None,
        }) {
            return Err(Error::Interrupted);
        }
    }

    // Structural checks: an unterminated block or a missing version comment fails here.
    scanner.finish()?;
    let dumped_by = dumped_by.ok_or(Error::MissingDumpVersion)?;

    // Phase two. Nothing above committed anything; every table becomes visible now.
    for (name, delta_table) in &mut open_tables {
        let actions = staged.remove(name).unwrap_or_default();
        let version = handle.block_on(commit_table(delta_table, actions, config.mode))?;
        if let Some(stat) = tables.iter_mut().find(|t| &t.table == name) {
            stat.delta_version = version;
        }
    }

    Ok(LoadReport {
        dumped_by,
        from_database,
        compression: compression.name(),
        bytes_read: consumed.load(Ordering::Relaxed),
        total_rows,
        tables,
    })
}

/// A decode worker. Owns its own [`TableWriter`] per block, so Parquet encoding runs here
/// rather than on a single writer thread.
fn worker(rx: chan::Receiver<Job>, events: chan::Sender<WorkerEvent>, handle: Handle) {
    let report = events.clone();
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| worker_loop(&rx, &events, &handle)));
    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            let _ = report.send(WorkerEvent::Failed(err));
        }
        Err(_) => {
            let _ = report.send(WorkerEvent::Failed(Error::Io {
                message: "decode worker panicked".to_string(),
            }));
        }
    }
}

/// The worker's job loop. Returns `Err` on the first decode or storage failure; the
/// caller turns that, and any panic, into a [`WorkerEvent::Failed`].
fn worker_loop(
    rx: &chan::Receiver<Job>,
    events: &chan::Sender<WorkerEvent>,
    handle: &Handle,
) -> Result<()> {
    let mut ctxs: HashMap<u64, Arc<BlockCtx>> = HashMap::new();
    let mut blocks: HashMap<u64, WorkerBlock> = HashMap::new();
    let mut scratch: Vec<u8> = Vec::new();
    let mut ranges: Vec<Option<std::ops::Range<usize>>> = Vec::new();

    while let Some(job) = rx.recv() {
        match job {
            Job::Open(ctx) => {
                ctxs.insert(ctx.generation, ctx);
            }

            Job::Rows { generation, bytes } => {
                let ctx = ctxs.get(&generation).ok_or_else(pool_stopped)?;
                let block = match blocks.entry(generation) {
                    std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                    std::collections::hash_map::Entry::Vacant(e) => e.insert(WorkerBlock {
                        builder: BatchBuilder::new(&ctx.columns, ctx.batch_rows, ctx.batch_bytes),
                        writer: TableWriter::new(&ctx.table)?,
                        batches: 0,
                    }),
                };
                for row in copy::rows(&bytes) {
                    copy::decode_row(row, ctx.arity, ctx.limits, &mut scratch, &mut ranges)?;
                    block.builder.append_row_ranges(&scratch, &ranges)?;
                    if block.builder.is_full()
                        && let Some(batch) = block.builder.finish()?
                    {
                        handle.block_on(block.writer.write(batch))?;
                        block.batches += 1;
                    }
                }
            }

            Job::Close { generation } => {
                ctxs.remove(&generation);
                let closed = match blocks.remove(&generation) {
                    Some(mut block) => {
                        if let Some(batch) = block.builder.finish()? {
                            handle.block_on(block.writer.write(batch))?;
                            block.batches += 1;
                        }
                        let staged = handle.block_on(block.writer.stage())?;
                        BlockClosed {
                            generation,
                            rows: block.writer.rows(),
                            batches: block.batches,
                            substitutions: block.builder.substitutions().to_vec(),
                            staged,
                        }
                    }
                    None => BlockClosed {
                        generation,
                        rows: 0,
                        batches: 0,
                        substitutions: Vec::new(),
                        staged: Vec::new(),
                    },
                };
                if events.send(WorkerEvent::Closed(closed)).is_err() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pgdelta-pipe-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn prefix(dir: &std::path::Path) -> String {
        format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
    }

    fn dump(body: &str) -> Vec<u8> {
        let mut d = String::from("-- Dumped from database version 17.4\n");
        d.push_str("-- Dumped by pg_dump version 17.4\n");
        d.push_str(body);
        d.into_bytes()
    }

    const TWO_TABLES: &str = "\
CREATE TABLE public.users (
    id integer,
    name text
);
CREATE TABLE public.events (
    id bigint,
    kind mystery_enum
);
COPY public.users (id, name) FROM stdin;
1\talice
2\tbob
3\t\\N
\\.
COPY public.events (id, kind) FROM stdin;
10\tclick
11\tview
\\.
";

    #[test]
    fn loads_every_table_and_reports_versions() {
        let dir = tmpdir("all");
        let config = LoadConfig {
            output_uri: prefix(&dir),
            mode: WriteMode::Overwrite,
            expect_pg_major: Some(17),
            threads: 3,
            ..LoadConfig::default()
        };

        let report = run(Cursor::new(dump(TWO_TABLES)), &config, |_| true).unwrap();

        assert_eq!(report.dumped_by, 17);
        assert_eq!(report.from_database, Some(17));
        assert_eq!(report.compression, "none");
        assert_eq!(report.total_rows, 5);
        assert_eq!(report.tables.len(), 2);

        let users = report.tables.iter().find(|t| t.table == "public.users").unwrap();
        assert_eq!(users.rows, 3);
        assert!(users.delta_version >= 1, "table must be committed");
        assert!(users.text_fallback_columns.is_empty());

        let events = report.tables.iter().find(|t| t.table == "public.events").unwrap();
        assert_eq!(events.rows, 2);
        assert_eq!(
            events.text_fallback_columns,
            vec![("kind".to_string(), "mystery_enum".to_string())],
            "an unrecognised type must degrade to text and be reported"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_large_table_fans_out_across_workers() {
        let dir = tmpdir("fanout");
        // Rows enough to force several 16 MiB chunks, so more than one worker sees the
        // block and the per-worker staged actions must be pooled.
        let mut body = String::from("CREATE TABLE public.wide (\n    id integer,\n    blob text\n);\n");
        body.push_str("COPY public.wide (id, blob) FROM stdin;\n");
        let filler = "x".repeat(200);
        for i in 0..300_000 {
            body.push_str(&format!("{i}\t{filler}\n"));
        }
        body.push_str("\\.\n");

        let config = LoadConfig {
            output_uri: prefix(&dir),
            threads: 4,
            batch_rows: 20_000,
            ..LoadConfig::default()
        };
        let report = run(Cursor::new(dump(&body)), &config, |_| true).unwrap();

        assert_eq!(report.total_rows, 300_000);
        let wide = &report.tables[0];
        assert_eq!(wide.rows, 300_000);
        assert!(wide.batches >= 4, "expected many batches, got {}", wide.batches);
        assert!(wide.delta_version >= 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A caller sizes a progress bar against the file on disk, so the count has to be of
    /// what was taken from the input, not of what came out of the decompressor.
    #[test]
    fn bytes_read_counts_the_input_not_the_inflated_stream() {
        let dir = tmpdir("gzbytes");
        let plain = dump(TWO_TABLES);

        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &plain).unwrap();
        let gz = encoder.finish().unwrap();
        assert!(gz.len() < plain.len(), "fixture must actually compress");

        let config = LoadConfig {
            output_uri: prefix(&dir),
            threads: 2,
            ..LoadConfig::default()
        };
        let report = run(Cursor::new(gz.clone()), &config, |_| true).unwrap();

        assert_eq!(report.compression, "gzip");
        assert_eq!(report.total_rows, 5);
        assert_eq!(
            report.bytes_read,
            gz.len() as u64,
            "bytes_read must match the compressed input, not the {} inflated bytes",
            plain.len()
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn table_filter_skips_unwanted_blocks() {
        let dir = tmpdir("filter");
        let config = LoadConfig {
            output_uri: prefix(&dir),
            tables: Some(vec!["public.events".to_string()]),
            threads: 2,
            ..LoadConfig::default()
        };

        let report = run(Cursor::new(dump(TWO_TABLES)), &config, |_| true).unwrap();

        assert_eq!(report.tables.len(), 1);
        assert_eq!(report.tables[0].table, "public.events");
        assert_eq!(report.total_rows, 2);
        assert!(
            !dir.join("public/users").exists(),
            "skipped table must not be written"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unterminated_copy_block_fails_the_load() {
        let dir = tmpdir("trunc");
        let config = LoadConfig {
            output_uri: prefix(&dir),
            threads: 2,
            ..LoadConfig::default()
        };
        let truncated = "\
CREATE TABLE public.t (
    id integer
);
COPY public.t (id) FROM stdin;
1
2
";
        let err = run(Cursor::new(dump(truncated)), &config, |_| true).unwrap_err();
        assert_eq!(
            err,
            Error::UnterminatedCopy {
                table: "public.t".to_string()
            }
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_mismatched_major_is_rejected() {
        let dir = tmpdir("major");
        let config = LoadConfig {
            output_uri: prefix(&dir),
            expect_pg_major: Some(16),
            ..LoadConfig::default()
        };
        let err = run(Cursor::new(dump(TWO_TABLES)), &config, |_| true).unwrap_err();
        assert_eq!(err, Error::UnsupportedDumpVersion { found: 17 });

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_callback_returning_false_interrupts_before_commit() {
        let dir = tmpdir("interrupt");
        let config = LoadConfig {
            output_uri: prefix(&dir),
            threads: 2,
            ..LoadConfig::default()
        };
        let err = run(Cursor::new(dump(TWO_TABLES)), &config, |_| false).unwrap_err();
        assert_eq!(err, Error::Interrupted);
        assert!(
            !dir.join("public/events").exists(),
            "the block after the interrupt must never be opened"
        );

        let log = dir.join("public/users/_delta_log");
        assert!(
            log.join("00000000000000000000.json").exists(),
            "the table is created at open"
        );
        assert!(
            !log.join("00000000000000000001.json").exists(),
            "an interrupted load commits nothing"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_field_count_mismatch_fails_the_load() {
        let dir = tmpdir("arity");
        let config = LoadConfig {
            output_uri: prefix(&dir),
            threads: 2,
            ..LoadConfig::default()
        };
        let bad = "\
CREATE TABLE public.t (
    id integer,
    name text
);
COPY public.t (id, name) FROM stdin;
1\talice\textra
\\.
";
        let err = run(Cursor::new(dump(bad)), &config, |_| true).unwrap_err();
        assert_eq!(
            err,
            Error::FieldCountMismatch {
                found: 3,
                expected: 2
            }
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
