//! The Delta write path, and the two-phase commit that keeps a failed load invisible.
//!
//! **This is the only asynchronous module in the crate.** delta-rs exposes an async-only
//! API, so a Tokio runtime exists, but it is confined to storage operations. Everything
//! upstream of here is synchronous and CPU-bound, and must stay off the runtime's worker
//! threads. See Chapter IV of the architecture document.
//!
//! # Two-phase commit
//!
//! Delta has no cross-table transaction. Atomicity is per-table, and a dump with hundreds
//! of tables means hundreds of independent commits, so a naive loop that commits each
//! table as it finishes leaves earlier tables visible when a later one fails.
//!
//! Instead, [`TableSink::stage`] writes every Parquet file and returns the resulting
//! `Add` actions **without committing them**. Files not referenced by a transaction log
//! are invisible to Delta readers: they do not exist as far as any query is concerned.
//! Only once the entire dump has been consumed cleanly does [`TableSink::commit`] run,
//! turning the whole set visible in a burst of metadata-only operations.
//!
//! A failure during staging therefore leaves orphaned files and **zero visible change**.
//! Those files are not free, so `VACUUM` must be scheduled.
//!
//! # Protocol floor
//!
//! Tables are created at reader version 1 and writer version 2, which delta-rs uses by
//! default when no advanced feature is requested. No deletion vectors and no column
//! mapping, so any DBR version can read the output. A test asserts this rather than
//! trusting it, because a delta-rs upgrade could raise the default silently.

use std::collections::HashMap;
use std::sync::Arc;

use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::{Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use deltalake::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake::kernel::transaction::{CommitBuilder, TableReference};
use deltalake::kernel::{Action, MetadataExt, StructType};
use deltalake::logstore::LogStore;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::table::builder::ensure_table_uri;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::{DeltaTable, DeltaTableBuilder, ObjectStore};
use futures::TryStreamExt;

use crate::error::{Error, Result};
use crate::scan::TableName;

/// How an existing table is treated when a load begins.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteMode {
    /// Replace the table's contents. The previous run's files are tombstoned in the same
    /// commit that adds the new ones, so readers never observe an empty table.
    Overwrite,
    /// Add to the table's existing contents.
    Append,
    /// Fail if the table already holds data.
    ErrorIfExists,
}

impl From<WriteMode> for SaveMode {
    fn from(m: WriteMode) -> Self {
        match m {
            WriteMode::Overwrite => SaveMode::Overwrite,
            WriteMode::Append => SaveMode::Append,
            WriteMode::ErrorIfExists => SaveMode::ErrorIfExists,
        }
    }
}

/// Translates a delta-rs error, which is not `Clone`, into this crate's error type.
fn delta(e: impl std::fmt::Display) -> Error {
    Error::Delta {
        message: e.to_string(),
    }
}

/// Path fragments that mark storage a catalog manages for itself.
///
/// Deliberately short and specific. A guess that is too broad would refuse a legitimate
/// external location, and the failure mode of that is a load that cannot run at all.
const MANAGED_MARKERS: &[&str] = &[
    // Unity Catalog's managed table storage.
    "__unitystorage",
    // The legacy Hive metastore warehouse, managed by the same argument.
    "/user/hive/warehouse",
];

/// Refuses an output prefix that points at catalog-managed storage.
///
/// Managed tables assume the catalog is their only writer. Writing Delta files underneath
/// one from outside can leave it inconsistent in ways that do not surface as a clean
/// error, which is precisely the class of failure this library exists to avoid. The
/// supported layout is an external location, with the resulting tables registered.
///
/// Note that a `/Volumes/...` path is **not** refused. A Volume is managed, but it is
/// intended to hold files, and landing the dump there is the documented arrangement.
///
/// # Errors
///
/// [`Error::ManagedTableTarget`] if the prefix carries a marker of managed storage.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use pgdelta::sink::reject_managed_storage;
///
/// assert!(reject_managed_storage("abfss://c@a.dfs.core.windows.net/bronze/pg/").is_ok());
/// assert!(reject_managed_storage("/Volumes/main/raw/pg/").is_ok());
/// assert!(reject_managed_storage("dbfs:/user/hive/warehouse/db.db/t").is_err());
/// ```
pub fn reject_managed_storage(uri: &str) -> Result<()> {
    let haystack = uri.to_ascii_lowercase();
    for marker in MANAGED_MARKERS {
        if haystack.contains(marker) {
            return Err(Error::ManagedTableTarget {
                uri: uri.to_string(),
                marker,
            });
        }
    }
    Ok(())
}

/// Maps a table name to a path relative to the output prefix.
///
/// Table names originate with a third party and `../` is a legal quoted PostgreSQL
/// identifier, so this is a security boundary rather than a formatting convenience.
/// Names are **validated and rejected**, never sanitised: silently rewriting a name
/// would map two different source tables onto one output path.
///
/// The schema and the table are validated as separate components and are never recovered
/// by splitting a joined string, because a dot is legal inside a quoted identifier:
/// `public."a.b"` and schema `public.a` table `b` would otherwise yield the same path.
/// See [`TableName`].
///
/// A component may contain letters, digits, underscores, hyphens and spaces. Anything
/// else, including path separators, dots, control characters, and a leading or trailing
/// space, is refused. Letters are judged by Unicode, so `"Räksmörgås"` is accepted.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] for any name that fails validation.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use pgdelta::scan::TableName;
/// use pgdelta::sink::relative_path;
///
/// fn name(schema: Option<&str>, table: &str) -> TableName {
///     TableName { schema: schema.map(str::to_string), table: table.to_string() }
/// }
///
/// assert_eq!(relative_path(&name(Some("public"), "users")).unwrap(), "public/users");
/// assert_eq!(relative_path(&name(None, "users")).unwrap(), "users");
///
/// // A dot inside an identifier is refused, not split into extra path components.
/// assert!(relative_path(&name(Some("public"), "a.b")).is_err());
/// assert!(relative_path(&name(None, "..")).is_err());
/// assert!(relative_path(&name(Some("public"), "../../etc/passwd")).is_err());
/// ```
pub fn relative_path(name: &TableName) -> Result<String> {
    let reject = || Error::UnsafeTableName {
        name: name.qualified(),
    };

    let mut parts: Vec<&str> = Vec::with_capacity(2);
    if let Some(schema) = &name.schema {
        parts.push(schema);
    }
    parts.push(&name.table);

    let mut total = 0usize;
    for component in &parts {
        if component.is_empty() || component.len() > 255 {
            return Err(reject());
        }
        if component.starts_with(' ') || component.ends_with(' ') {
            return Err(reject());
        }
        // Excluding the dot here is what makes `.` and `..` unreachable as whole
        // components without a special case for them.
        let safe = component
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '-' || c == ' ')
            && !component.chars().any(char::is_control);
        if !safe {
            return Err(reject());
        }
        total += component.len();
    }
    if total > 512 {
        return Err(reject());
    }
    Ok(parts.join("/"))
}

/// Joins the output prefix and a table's relative path.
fn table_uri(prefix: &str, table: &TableName) -> Result<String> {
    let rel = relative_path(table)?;
    let trimmed = prefix.trim_end_matches('/');
    Ok(format!("{trimmed}/{rel}"))
}

/// Returns `Remove` actions tombstoning every file currently visible in `table`.
///
/// Also serves as an emptiness test: an empty result means the table holds no data.
async fn current_files(table: &DeltaTable) -> Result<Vec<Action>> {
    let log_store = table.log_store();
    let Ok(state) = table.snapshot() else {
        return Ok(Vec::new());
    };
    state
        .snapshot()
        .file_views(&log_store, None)
        .map_ok(|f| Action::Remove(f.remove_action(true)))
        .try_collect()
        .await
        .map_err(delta)
}

/// How the dump's schema for a table differs from the one Delta currently declares.
///
/// Empty on a first run, and empty on any later run whose DDL is unchanged. A third party
/// controls the schema and changes it without notice, so this is reported per table in the
/// run statistics rather than discovered by a downstream query.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaDrift {
    /// Columns the dump carries that the Delta table does not.
    pub added: Vec<String>,
    /// Columns the Delta table has that the dump no longer carries.
    pub removed: Vec<String>,
    /// Columns present in both whose type changed, as `(column, was, now)`.
    pub retyped: Vec<(String, String, String)>,
}

impl SchemaDrift {
    /// True when the dump and the table agree.
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.retyped.is_empty()
    }

    /// One-line description naming the columns involved.
    ///
    /// Carries column names and type names only, never a value, because this reaches
    /// error text and the dump is untrusted.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();
        if !self.added.is_empty() {
            parts.push(format!("added {}", self.added.join(", ")));
        }
        if !self.removed.is_empty() {
            parts.push(format!("removed {}", self.removed.join(", ")));
        }
        for (column, was, now) in &self.retyped {
            parts.push(format!("{column} changed from {was} to {now}"));
        }
        if parts.is_empty() {
            "no change".to_string()
        } else {
            parts.join("; ")
        }
    }
}

/// Compares the dump's schema against the one the table currently declares.
///
/// Both sides are kernel types. Comparing an Arrow type against a Delta one as rendered
/// text cannot work, because the two spell the same type differently: the kernel writes
/// `Primitive(Integer)` where Arrow writes `Int32`, so every column would look retyped.
/// Converting first also means this agrees with [`commit_table`] by construction, since
/// that decides whether to emit a `Metadata` action from the same comparison.
fn diff_schema(current: &StructType, incoming: &StructType) -> SchemaDrift {
    let mut drift = SchemaDrift::default();

    for field in incoming.fields() {
        match current.fields().find(|f| f.name() == field.name()) {
            None => drift.added.push(field.name().to_string()),
            Some(existing) => {
                if existing.data_type() != field.data_type() {
                    drift.retyped.push((
                        field.name().to_string(),
                        existing.data_type().to_string(),
                        field.data_type().to_string(),
                    ));
                }
            }
        }
    }
    for field in current.fields() {
        if !incoming.fields().any(|f| f.name() == field.name()) {
            drift.removed.push(field.name().to_string());
        }
    }
    drift
}

/// A Delta table opened for one source table, with whatever its schema drifted by.
#[derive(Debug)]
pub struct OpenedTable {
    /// The loaded table, ready to be written to and later committed.
    pub table: DeltaTable,
    /// Fully qualified URI, which a [`TableWriter`] needs in order to write the dump's
    /// schema rather than the table's current one.
    pub uri: String,
    /// How the dump's schema differs from what the table declared on entry.
    pub drift: SchemaDrift,
}

/// A table whose existence and, if it already existed, current schema have been checked,
/// but which has not yet been created or diffed against an incoming schema.
///
/// The network-bound part of opening a table (reading its Delta log, if any) needs no
/// schema at all, only the table's name. Separating it out lets a caller start it as soon
/// as a `CREATE TABLE` is parsed, well before the `COPY` block that carries the exact,
/// possibly reordered column list [`finish_open`] needs. See [`preload_table`].
#[derive(Debug)]
pub struct PreloadedTable {
    table: DeltaTable,
    uri: String,
    /// The table's current schema, or `None` for a table with no Delta log yet, meaning
    /// [`finish_open`] must create it.
    existing_schema: Option<StructType>,
}

/// The object store backing every table beneath one output prefix, resolved once.
///
/// Resolving a URL into a store normally builds a fresh backend client: for a cloud
/// target, a new HTTP client and, under identity-based auth (managed identity, workload
/// identity), a fresh token fetch. `object_store` splits a location into a store rooted at
/// the whole storage account/container and a path prefix within it precisely so many
/// objects under one account can share the root store; a dump's few hundred tables all
/// sit under the same `output_uri` account/container, so they qualify. [`preload_table`]
/// and [`open_table`] decorate this one root store with each table's own prefix instead of
/// re-resolving a client per table.
///
/// A local path or a `/Volumes/...` FUSE mount has no such client to share, so this makes
/// no difference there; it matters for `abfss://`, `s3://` and `gs://` targets.
#[derive(Clone)]
pub struct SharedStore {
    store: Arc<dyn ObjectStore>,
}

impl SharedStore {
    /// Resolves the object store for `prefix`.
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] for a storage or protocol failure while resolving the backend.
    ///
    /// # Panics
    ///
    /// Does not panic.
    ///
    /// # Blocking
    ///
    /// Synchronous: resolving a backend client is local work, not a storage round trip.
    pub fn resolve(prefix: &str, storage_options: &HashMap<String, String>) -> Result<Self> {
        let url = ensure_table_uri(prefix).map_err(delta)?;
        let logstore = DeltaTableBuilder::from_url(url)
            .map_err(delta)?
            .with_storage_options(storage_options.clone())
            .build_storage()
            .map_err(delta)?;
        Ok(Self {
            store: logstore.root_object_store(None),
        })
    }
}

/// Checks whether the Delta table for `table_name` beneath `prefix` exists, and loads its
/// current schema if so, without touching the incoming schema at all.
///
/// This is the network-bound half of opening a table: reading its `_delta_log`, if one
/// exists. It carries no dependency on the dump's `COPY` column order, so it can run as
/// soon as a table's `CREATE TABLE` has been parsed, overlapping its storage round trip
/// with whatever table is currently being decoded rather than serialising it on the
/// reader thread right as the `COPY` block starts. Pair with [`finish_open`], which needs
/// the schema and therefore cannot run until the `COPY` header has been read.
///
/// `shared_store` is decorated with this table's own path rather than rebuilt; see
/// [`SharedStore`].
///
/// For a local path or a `/Volumes/...` FUSE mount, this creates the table's directory
/// (delta-rs's `ensure_table_uri` does that eagerly, to canonicalise it) even though no
/// Delta table exists there yet: the directory carries no `_delta_log` and is not a
/// readable Delta table until [`finish_open`] runs. Calling this well ahead of a table's
/// `COPY` block, which is the point of it, means that empty directory can now appear for
/// every wanted table before the load has gotten anywhere near it, including one whose
/// `COPY` block is never reached because the load fails or is interrupted first. Object
/// storage backends have no such directory to pre-create and are unaffected.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] if the name would escape the prefix, and [`Error::Delta`]
/// for a storage or protocol failure.
///
/// # Panics
///
/// Does not panic.
pub async fn preload_table(
    prefix: &str,
    table_name: &TableName,
    shared_store: &SharedStore,
    storage_options: &HashMap<String, String>,
) -> Result<PreloadedTable> {
    let uri = table_uri(prefix, table_name)?;
    let url = ensure_table_uri(&uri).map_err(delta)?;
    let mut table = DeltaTableBuilder::from_url(url.clone())
        .map_err(delta)?
        .with_storage_options(storage_options.clone())
        .with_storage_backend(shared_store.store.clone(), url)
        .build()
        .map_err(delta)?;

    // `load` fails when no log exists yet, which is how a first run is detected.
    let existing_schema = if table.load().await.is_ok() {
        let snapshot = table.snapshot().map_err(delta)?;
        Some(snapshot.schema().as_ref().clone())
    } else {
        None
    };

    Ok(PreloadedTable {
        table,
        uri,
        existing_schema,
    })
}

/// Finishes opening a [`PreloadedTable`] once the dump's exact, `COPY`-ordered schema for
/// it is known: creates the table on a first run, or diffs the incoming schema against
/// what [`preload_table`] already read.
///
/// The incoming `schema` is compared against the table's current one and the difference is
/// returned. Under [`WriteMode::Overwrite`] a difference is legal and is applied by
/// [`commit_table`]; under [`WriteMode::Append`] it is refused, because appending rows
/// shaped one way to a table declared another way cannot be made to mean anything.
///
/// # Errors
///
/// [`Error::TableExists`] if `mode` is [`WriteMode::ErrorIfExists`] and the table already
/// holds data, [`Error::SchemaChanged`] if `mode` is [`WriteMode::Append`] and the schema
/// differs, and [`Error::Delta`] for a storage or protocol failure.
///
/// # Panics
///
/// Does not panic.
pub async fn finish_open(
    preloaded: PreloadedTable,
    table_name: &TableName,
    schema: &ArrowSchema,
    mode: WriteMode,
) -> Result<OpenedTable> {
    let PreloadedTable {
        mut table,
        uri,
        existing_schema,
    } = preloaded;

    let Some(current_schema) = existing_schema else {
        let kernel: StructType = schema.try_into_kernel().map_err(delta)?;
        let columns = kernel.fields().cloned().collect::<Vec<_>>();
        table = table
            .create()
            .with_columns(columns)
            .with_save_mode(SaveMode::ErrorIfExists)
            .await
            .map_err(delta)?;
        return Ok(OpenedTable {
            table,
            uri,
            drift: SchemaDrift::default(),
        });
    };

    if mode == WriteMode::ErrorIfExists && !current_files(&table).await?.is_empty() {
        return Err(Error::TableExists {
            table: table_name.qualified(),
        });
    }

    let drift = {
        let incoming: StructType = schema.try_into_kernel().map_err(delta)?;
        diff_schema(&current_schema, &incoming)
    };
    if !drift.is_empty() && mode == WriteMode::Append {
        return Err(Error::SchemaChanged {
            table: table_name.qualified(),
            detail: drift.summary(),
        });
    }

    Ok(OpenedTable { table, uri, drift })
}

/// Opens the Delta table for `table_name` beneath `prefix`, creating it on a first run.
///
/// The returned table is loaded and ready for a [`TableWriter`].
///
/// A convenience composition of [`preload_table`] and [`finish_open`] for a caller with no
/// reason to start the two apart, such as [`TableSink::open`]. The parallel pipeline calls
/// them separately so a table's network-bound existence check can run well ahead of the
/// `COPY` block that supplies the schema [`finish_open`] needs; see their docs.
///
/// # Errors
///
/// [`Error::UnsafeTableName`] if the name would escape the prefix, [`Error::TableExists`]
/// if `mode` is [`WriteMode::ErrorIfExists`] and the table already holds data,
/// [`Error::SchemaChanged`] if `mode` is [`WriteMode::Append`] and the schema differs, and
/// [`Error::Delta`] for a storage or protocol failure.
///
/// # Panics
///
/// Does not panic.
pub async fn open_table(
    prefix: &str,
    table_name: &TableName,
    schema: &ArrowSchema,
    mode: WriteMode,
    storage_options: &HashMap<String, String>,
) -> Result<OpenedTable> {
    // A one-off resolve: this convenience wrapper is for a caller opening a single table
    // (the manifest table, or a test), where there is nothing else to share the store with.
    let shared_store = SharedStore::resolve(prefix, storage_options)?;
    let preloaded = preload_table(prefix, table_name, &shared_store, storage_options).await?;
    finish_open(preloaded, table_name, schema, mode).await
}

/// Commits `staged` against `table`, making its new contents visible in one Delta version.
///
/// For [`WriteMode::Overwrite`] the table's current files are tombstoned in the same
/// commit, so no reader observes an empty or half-replaced table. `table` is reloaded to
/// the new version before returning.
///
/// When `new_schema` is supplied and differs from what the table declares, a `Metadata`
/// action carrying it is committed alongside the data, so the declared schema and the
/// files agree. Without that the table would keep yesterday's schema while holding
/// today's files. Only [`WriteMode::Overwrite`] may change a schema; see [`open_table`].
///
/// # Errors
///
/// [`Error::Delta`] on a commit conflict, a storage failure, or a schema that cannot be
/// converted to the Delta kernel's representation.
///
/// # Panics
///
/// Does not panic.
pub async fn commit_table(
    table: &mut DeltaTable,
    staged: Vec<Action>,
    mode: WriteMode,
    new_schema: Option<&ArrowSchema>,
) -> Result<u64> {
    let mut actions = staged;

    if mode == WriteMode::Overwrite {
        for file in current_files(table).await? {
            actions.push(file);
        }

        if let Some(schema) = new_schema {
            let kernel: StructType = schema.try_into_kernel().map_err(delta)?;
            let snapshot = table.snapshot().map_err(delta)?;
            if kernel != *snapshot.schema().as_ref() {
                let updated = snapshot
                    .metadata()
                    .clone()
                    .with_schema(&kernel)
                    .map_err(delta)?;
                actions.push(Action::Metadata(updated));
            }
        }
    }

    let operation = DeltaOperation::Write {
        mode: mode.into(),
        partition_by: None,
        predicate: None,
    };
    let version = {
        let snapshot = table.snapshot().ok().map(|s| s as &dyn TableReference);
        CommitBuilder::default()
            .with_actions(actions)
            .build(snapshot, table.log_store(), operation)
            .await
            .map_err(delta)?
            .version()
    };
    table.load().await.map_err(delta)?;
    Ok(version)
}

/// Encodes one table's batches into Parquet and stages the resulting files.
///
/// One of these exists per decode worker per open `COPY` block, so Parquet encoding, the
/// pipeline's expected bottleneck, runs on every worker at once rather than on a single
/// writer thread. It stages files but never commits: the reader collects every worker's
/// staged actions and commits them together in phase two.
///
/// Every method is async and must run on the Tokio runtime.
#[derive(Debug)]
pub struct TableWriter {
    writer: RecordBatchWriter,
    rows: u64,
}

impl TableWriter {
    /// Creates a writer for the table at `uri` that writes `schema`.
    ///
    /// The schema is passed explicitly rather than taken from the table's metadata,
    /// because under [`WriteMode::Overwrite`] the dump may carry a different one and the
    /// files must be written in the shape that [`commit_table`] is about to declare.
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] if a writer cannot be built for that location.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn new(
        uri: &str,
        schema: ArrowSchemaRef,
        storage_options: &HashMap<String, String>,
    ) -> Result<Self> {
        Ok(Self {
            writer: RecordBatchWriter::try_new(uri, schema, None, Some(storage_options.clone()))
                .map_err(delta)?,
            rows: 0,
        })
    }

    /// Rows handed to this writer so far.
    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Buffers `batch`. Parquet may be written on this call or deferred to [`TableWriter::stage`].
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] if the batch does not match the table's schema, or on a storage
    /// failure.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.rows += batch.num_rows() as u64;
        self.writer.write(batch).await.map_err(delta)
    }

    /// Flushes every remaining Parquet file and returns `Add` actions for all files
    /// written, **without committing them**. The files exist in storage but no reader can
    /// see them until they are committed.
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] on a storage failure.
    pub async fn stage(&mut self) -> Result<Vec<Action>> {
        let adds = self.writer.flush().await.map_err(delta)?;
        Ok(adds.into_iter().map(Action::Add).collect())
    }
}

/// Writes one table's batches, holding the commit until the whole load succeeds.
///
/// This is the single-threaded convenience wrapper over [`open_table`], [`TableWriter`]
/// and [`commit_table`]. The parallel pipeline uses those directly. Every method is async
/// and must run on the Tokio runtime.
#[derive(Debug)]
pub struct TableSink {
    name: String,
    table: DeltaTable,
    writer: TableWriter,
    mode: WriteMode,
    staged: Vec<Action>,
    schema: ArrowSchemaRef,
    drift: SchemaDrift,
}

impl TableSink {
    /// Opens or creates the Delta table for `table_name` beneath `prefix`.
    ///
    /// # Errors
    ///
    /// [`Error::UnsafeTableName`] if the table name would escape the prefix,
    /// [`Error::TableExists`] under [`WriteMode::ErrorIfExists`],
    /// [`Error::SchemaChanged`] under [`WriteMode::Append`] if the schema moved, and
    /// [`Error::Delta`] for a storage or protocol failure.
    pub async fn open(
        prefix: &str,
        table_name: &TableName,
        schema: &ArrowSchema,
        mode: WriteMode,
        storage_options: &HashMap<String, String>,
    ) -> Result<Self> {
        let schema: ArrowSchemaRef = Arc::new(schema.clone());
        let opened = open_table(prefix, table_name, &schema, mode, storage_options).await?;
        let writer = TableWriter::new(&opened.uri, Arc::clone(&schema), storage_options)?;
        Ok(Self {
            name: table_name.qualified(),
            table: opened.table,
            writer,
            mode,
            staged: Vec::new(),
            schema,
            drift: opened.drift,
        })
    }

    /// How the dump's schema differed from the table's on open.
    pub fn drift(&self) -> &SchemaDrift {
        &self.drift
    }

    /// Qualified name of the table this sink writes.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Rows written so far.
    pub fn rows(&self) -> u64 {
        self.writer.rows()
    }

    /// Buffers a batch. Parquet is written when the buffer fills or [`TableSink::stage`]
    /// runs, not necessarily on this call.
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] if the batch does not match the table's schema, or on a storage
    /// failure.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        self.writer.write(batch).await
    }

    /// Ends phase one: flushes every remaining Parquet file and retains the resulting
    /// actions **without committing them**.
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] on a storage failure.
    pub async fn stage(&mut self) -> Result<()> {
        let adds = self.writer.stage().await?;
        self.staged.extend(adds);
        Ok(())
    }

    /// Number of actions waiting to be committed.
    pub fn staged_actions(&self) -> usize {
        self.staged.len()
    }

    /// Ends phase two: commits the staged actions, making the table's new contents
    /// visible in one atomic version.
    ///
    /// For [`WriteMode::Overwrite`] the previous run's files are tombstoned in the same
    /// commit, so no reader ever observes an empty or half-replaced table.
    ///
    /// # Errors
    ///
    /// [`Error::Delta`] on a commit conflict or storage failure.
    pub async fn commit(&mut self) -> Result<u64> {
        let actions = std::mem::take(&mut self.staged);
        let schema = (!self.drift.is_empty()).then_some(self.schema.as_ref());
        commit_table(&mut self.table, actions, self.mode, schema).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int32Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    fn schema() -> ArrowSchema {
        ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
        ])
    }

    fn batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema()),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    fn prefix(dir: &std::path::Path) -> String {
        // delta-rs wants a URI; a bare Windows path is not one.
        format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
    }

    /// A schema-qualified name, as the scanner would produce it.
    fn qualified(schema: &str, table: &str) -> TableName {
        TableName {
            schema: Some(schema.to_string()),
            table: table.to_string(),
        }
    }

    /// A bare name, as the scanner would produce it for an unqualified `COPY`.
    fn bare(table: &str) -> TableName {
        TableName {
            schema: None,
            table: table.to_string(),
        }
    }

    #[test]
    fn safe_table_names_map_to_nested_paths() {
        assert_eq!(
            relative_path(&qualified("public", "users")).unwrap(),
            "public/users"
        );
        assert_eq!(relative_path(&bare("users")).unwrap(), "users");
        assert_eq!(
            relative_path(&qualified("my_schema", "order-items")).unwrap(),
            "my_schema/order-items"
        );
        assert_eq!(
            relative_path(&qualified("s", "Räksmörgås")).unwrap(),
            "s/Räksmörgås",
            "a non-ASCII identifier is legal and must survive"
        );
    }

    #[test]
    fn traversal_attempts_are_rejected_not_sanitised() {
        for name in [
            bare(".."),
            bare("."),
            bare("../../etc/passwd"),
            qualified("public", "/etc/passwd"),
            qualified("public", "users/../../x"),
            qualified("public", "us\\ers"),
            bare(""),
            qualified("public", ""),
            qualified("", "users"),
            qualified("public", "us:ers"),
        ] {
            assert!(relative_path(&name).is_err(), "{name} must be rejected");
        }
    }

    /// A dot inside a quoted identifier must not become a path separator, because that
    /// would collide with a genuinely nested name. Rejected, never rewritten.
    #[test]
    fn a_dot_inside_an_identifier_is_rejected_rather_than_split() {
        assert!(relative_path(&qualified("public", "a.b")).is_err());
        assert!(relative_path(&qualified("public.a", "b")).is_err());
        assert!(relative_path(&bare("a.b.c.d")).is_err());

        // The pair that would otherwise share a path.
        let quoted_dot = qualified("public", "a.b");
        let nested = qualified("public.a", "b");
        assert_eq!(quoted_dot.qualified(), nested.qualified());
        assert!(relative_path(&quoted_dot).is_err() && relative_path(&nested).is_err());
    }

    #[test]
    fn control_characters_and_edge_spaces_are_rejected() {
        assert!(relative_path(&qualified("public", "us\0ers")).is_err());
        assert!(relative_path(&qualified("public", "us\ners")).is_err());
        assert!(relative_path(&qualified("public", " users")).is_err());
        assert!(relative_path(&qualified("public", "users ")).is_err());
    }

    #[test]
    fn staged_files_are_invisible_until_commit() {
        let dir = std::env::temp_dir().join(format!("pgdelta-stage-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        rt().block_on(async {
            let mut sink = TableSink::open(
                &prefix(&dir),
                &qualified("public", "users"),
                &schema(),
                WriteMode::Append,
                &HashMap::new(),
            )
            .await
            .unwrap();

            sink.write(batch(&[1, 2], &["a", "b"])).await.unwrap();
            sink.stage().await.unwrap();
            assert!(sink.staged_actions() > 0, "staging must produce actions");

            // Phase one is done and Parquet exists, but a fresh reader sees nothing.
            let reader_url = ensure_table_uri(format!("{}/public/users", prefix(&dir))).unwrap();
            let mut reader = DeltaTableBuilder::from_url(reader_url)
                .unwrap()
                .build()
                .unwrap();
            reader.load().await.unwrap();
            assert_eq!(reader.version(), Some(0), "no data version before commit");

            sink.commit().await.unwrap();
            reader.load().await.unwrap();
            assert_eq!(reader.version(), Some(1), "commit must publish one version");
        });

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn new_tables_sit_at_the_compatibility_floor() {
        let dir = std::env::temp_dir().join(format!("pgdelta-proto-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        rt().block_on(async {
            let mut sink = TableSink::open(
                &prefix(&dir),
                &qualified("public", "t"),
                &schema(),
                WriteMode::Append,
                &HashMap::new(),
            )
            .await
            .unwrap();
            sink.write(batch(&[1], &["a"])).await.unwrap();
            sink.stage().await.unwrap();
            sink.commit().await.unwrap();

            let protocol = sink.table.snapshot().unwrap().protocol();
            assert_eq!(
                protocol.min_reader_version(),
                1,
                "reader floor must stay at 1"
            );
            assert_eq!(
                protocol.min_writer_version(),
                2,
                "writer floor must stay at 2"
            );
        });

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn overwrite_replaces_rather_than_appends() {
        let dir = std::env::temp_dir().join(format!("pgdelta-ovw-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        rt().block_on(async {
            let p = prefix(&dir);
            let mut first = TableSink::open(
                &p,
                &bare("t"),
                &schema(),
                WriteMode::Append,
                &HashMap::new(),
            )
            .await
            .unwrap();
            first.write(batch(&[1, 2], &["a", "b"])).await.unwrap();
            first.stage().await.unwrap();
            first.commit().await.unwrap();

            let mut second = TableSink::open(
                &p,
                &bare("t"),
                &schema(),
                WriteMode::Overwrite,
                &HashMap::new(),
            )
            .await
            .unwrap();
            second.write(batch(&[9], &["z"])).await.unwrap();
            second.stage().await.unwrap();
            second.commit().await.unwrap();

            let files = second
                .table
                .snapshot()
                .unwrap()
                .snapshot()
                .file_views(&second.table.log_store(), None)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(files.len(), 1, "overwrite must tombstone the prior files");
            let _ = &files;
        });

        let _ = std::fs::remove_dir_all(&dir);
    }
}
