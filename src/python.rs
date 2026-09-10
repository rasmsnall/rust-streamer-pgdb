//! Python bindings.
//!
//! A thin translation layer over [`crate::pipeline`]: it converts keyword arguments into
//! a [`LoadConfig`], releases the GIL for the streaming work, checks for `KeyboardInterrupt`
//! between chunks, and returns the [`LoadReport`] as a Python object.
//!
//! Executes on the thread that called into it. The streaming work runs with the GIL
//! released; the progress callback re-acquires it.

use std::collections::HashMap;

use pyo3::exceptions::{PyIOError, PyKeyboardInterrupt, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::create_exception;
use pyo3::types::PyDict;

use crate::copy::Limits;
use crate::error::Error;
use crate::pipeline::{self, LoadConfig, LoadReport, Progress, TableStats};
use crate::sink::WriteMode;

create_exception!(
    _pgdelta,
    PartialCommitError,
    PyRuntimeError,
    "A commit failed after some tables had already been committed. \
     Delta has no cross-table transaction, so the target now holds a mixture of this run \
     and the previous one. Carries table, committed and total attributes saying how far \
     the burst got. Re-run to restore consistency: overwrite rewrites every table from \
     the same dump. Subclasses RuntimeError, so existing handlers still catch it."
);

/// Registers everything the extension module exposes.
pub(crate) fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(stream_dump_to_delta, module)?)?;
    module.add_class::<PyLoadReport>()?;
    module.add_class::<PyTableStats>()?;
    module.add(
        "PartialCommitError",
        module.py().get_type::<PartialCommitError>(),
    )?;
    module.add(
        "__all__",
        (
            "stream_dump_to_delta",
            "LoadReport",
            "TableStats",
            "PartialCommitError",
        ),
    )?;
    Ok(())
}

/// Maps a crate error onto the closest Python exception.
///
/// `ValueError` is the default because most variants describe a dump that is wrong.
/// Faults that are ours rather than the dump's raise `RuntimeError` instead.
///
/// [`Error::CommitFailed`] gets its own type, because it is the only failure that can
/// leave visible change behind, and a caller may want to handle it differently from every
/// other kind. Its counts are attached as attributes so they need not be parsed out of the
/// message.
fn to_pyerr(py: Python<'_>, err: Error) -> PyErr {
    let message = err.to_string();
    match err {
        Error::Interrupted => PyKeyboardInterrupt::new_err(message),
        Error::Io { .. } => PyIOError::new_err(message),
        Error::CommitFailed {
            table,
            committed,
            total,
            ..
        } => {
            let raised = PartialCommitError::new_err(message);
            let value = raised.value(py);
            // Best effort: a failure to attach these must not mask the error itself.
            let _ = value.setattr("table", table);
            let _ = value.setattr("committed", committed);
            let _ = value.setattr("total", total);
            raised
        }
        Error::Delta { .. } | Error::Arrow { .. } | Error::Internal { .. } => {
            PyRuntimeError::new_err(message)
        }
        _ => PyValueError::new_err(message),
    }
}

/// Per-table result, mirroring [`TableStats`].
#[pyclass(name = "TableStats", frozen, get_all)]
pub struct PyTableStats {
    /// Qualified table name as written in the dump.
    table: String,
    /// Rows decoded from the table's `COPY` block.
    rows: u64,
    /// Arrow batches written for this table.
    batches: u64,
    /// Delta version produced by the commit.
    delta_version: u64,
    /// `{column: count}` for values stored as NULL because the type could not hold them.
    null_substitutions: HashMap<String, u64>,
    /// `{column: declared_type}` for columns written as text because the type was not
    /// recognised.
    text_fallback_columns: HashMap<String, String>,
}

#[pymethods]
impl PyTableStats {
    fn __repr__(&self) -> String {
        format!(
            "TableStats(table={:?}, rows={}, batches={}, delta_version={})",
            self.table, self.rows, self.batches, self.delta_version
        )
    }
}

impl From<TableStats> for PyTableStats {
    fn from(s: TableStats) -> Self {
        Self {
            table: s.table,
            rows: s.rows,
            batches: s.batches,
            delta_version: s.delta_version,
            null_substitutions: s.null_substitutions.into_iter().collect(),
            text_fallback_columns: s.text_fallback_columns.into_iter().collect(),
        }
    }
}

/// Whole-load result, mirroring [`LoadReport`].
#[pyclass(name = "LoadReport", frozen, get_all)]
pub struct PyLoadReport {
    /// `pg_dump` major version that wrote the dump.
    dumped_by: u32,
    /// Source server major version, when the preamble stated one.
    from_database: Option<u32>,
    /// Compression decoded off the input, or `"none"`.
    compression: String,
    /// Bytes taken from the input, counted before decompression.
    bytes_read: u64,
    /// Rows decoded across every loaded table.
    total_rows: u64,
    /// One [`PyTableStats`] per loaded table, in the order their blocks closed.
    tables: Vec<Py<PyTableStats>>,
}

#[pymethods]
impl PyLoadReport {
    fn __repr__(&self) -> String {
        format!(
            "LoadReport(dumped_by={}, tables={}, total_rows={})",
            self.dumped_by,
            self.tables.len(),
            self.total_rows
        )
    }
}

/// Reads `mode` into a [`WriteMode`].
fn parse_mode(mode: &str) -> PyResult<WriteMode> {
    match mode {
        "overwrite" => Ok(WriteMode::Overwrite),
        "append" => Ok(WriteMode::Append),
        "error" => Ok(WriteMode::ErrorIfExists),
        other => Err(PyValueError::new_err(format!(
            "mode must be 'overwrite', 'append' or 'error', got {other:?}"
        ))),
    }
}

/// Stream a ``pg_dump`` plain-text file into one Delta table per source table.
///
/// The dump is read once, front to back. Every table's Parquet is written and staged
/// during the pass; nothing becomes visible to a Delta reader until the pass completes
/// without error, at which point every table is committed. A failure during that pass,
/// which is where essentially every failure occurs, leaves orphaned files and no visible
/// change to any table.
///
/// That is not cross-table atomicity, which Delta cannot provide: the commit at the end is
/// one independent commit per table, so a failure partway through it leaves some tables on
/// the new day and some on the old, and a reader during it can see a mixture. Re-run to
/// recover; ``overwrite`` is idempotent.
///
/// Parameters
/// ----------
/// dump_path : str | os.PathLike
///     Path to the plain-text dump. gzip is decompressed transparently.
/// output_uri : str
///     Prefix the tables are written beneath, for example ``/Volumes/main/raw/pg/`` or an
///     ``abfss://`` URL. Each table's sub-path comes from its qualified name and is
///     rejected, not sanitised, if it would escape the prefix.
/// tables : list[str] | None
///     Qualified names to load. ``None`` loads every table in the dump.
/// mode : str
///     ``"overwrite"``, ``"append"`` or ``"error"``. Overwrite tombstones the previous
///     run's files in the same commit that adds the new ones.
/// batch_rows, batch_bytes : int
///     Bounds on one in-memory Arrow batch. They cap memory; they do not set the Parquet
///     file size. Peak memory is roughly ``threads * batch_bytes``.
/// threads : int | None
///     Decode and Parquet-encode workers. ``None`` uses the machine's parallelism.
/// storage_options : dict[str, str] | None
///     Backend options passed through to ``object_store``.
/// expect_pg_major : int | None
///     When set, the load fails unless the dump's ``pg_dump`` major matches exactly.
/// max_field_bytes, max_row_bytes, max_columns : int | None
///     Override the decode limits that protect the driver from a hostile dump.
/// progress : callable | None
///     Called with a dict ``{bytes_read, rows, tables_done, table}`` after each chunk and
///     each table. ``bytes_read`` counts the input before decompression, so it is
///     comparable with the file size. Raising from it, or a Ctrl-C, aborts the load before
///     any commit; the exception raised is the one that propagates.
///
/// Returns
/// -------
/// LoadReport
///     Per-table row counts and Delta versions, the substitutions that were applied, and
///     the ``pg_dump`` and source-server versions from the dump preamble.
///
/// Raises
/// ------
/// ValueError
///     A malformed dump: a truncated ``COPY`` block, a row whose field count disagrees
///     with its ``COPY`` header, a bad escape, a value that contradicts its column type,
///     an unqualified or mismatched ``pg_dump`` major, or a table name that escapes the
///     prefix.
/// PartialCommitError
///     A commit failed part way through the final burst, so some tables hold this run's
///     contents and the rest hold the previous run's. Carries ``table``, ``committed`` and
///     ``total``. This is the only failure that can leave visible change behind.
///     Subclasses ``RuntimeError``.
/// RuntimeError
///     The Delta or Arrow write path failed, or an internal invariant was violated.
/// OSError
///     The dump could not be read.
/// KeyboardInterrupt
///     The load was interrupted. Nothing was committed.
///
/// Notes
/// -----
/// A PostgreSQL ``timestamp without time zone`` is written as Delta ``timestamp``, which
/// is microseconds UTC: naive timestamps are therefore assumed to be UTC. Delta
/// ``timestamp_ntz`` would raise the table's protocol version above the compatibility
/// floor this library targets, so it is not used.
#[pyfunction]
#[pyo3(signature = (
    dump_path,
    output_uri,
    *,
    tables = None,
    mode = "overwrite",
    batch_rows = 100_000,
    batch_bytes = 128 << 20,
    threads = None,
    storage_options = None,
    expect_pg_major = None,
    max_field_bytes = None,
    max_row_bytes = None,
    max_columns = None,
    progress = None,
))]
#[allow(clippy::too_many_arguments)]
fn stream_dump_to_delta(
    py: Python<'_>,
    dump_path: std::path::PathBuf,
    output_uri: String,
    tables: Option<Vec<String>>,
    mode: &str,
    batch_rows: usize,
    batch_bytes: usize,
    threads: Option<usize>,
    storage_options: Option<HashMap<String, String>>,
    expect_pg_major: Option<u32>,
    max_field_bytes: Option<usize>,
    max_row_bytes: Option<usize>,
    max_columns: Option<usize>,
    progress: Option<Py<PyAny>>,
) -> PyResult<PyLoadReport> {
    let defaults = Limits::default();
    let config = LoadConfig {
        output_uri,
        tables,
        mode: parse_mode(mode)?,
        batch_rows,
        batch_bytes,
        threads: threads.unwrap_or(0),
        storage_options: storage_options.unwrap_or_default(),
        expect_pg_major,
        limits: Limits {
            max_field_bytes: max_field_bytes.unwrap_or(defaults.max_field_bytes),
            max_row_bytes: max_row_bytes.unwrap_or(defaults.max_row_bytes),
            max_columns: max_columns.unwrap_or(defaults.max_columns),
        },
    };

    // The callback runs on the calling thread with the GIL held. It reports progress and,
    // by re-checking signals, lets Ctrl-C stop a load that would otherwise run for hours.
    // Returning false aborts with Error::Interrupted.
    //
    // A signal, or an exception raised by the caller's callback, is restored as the
    // pending Python exception rather than discarded, and picked back up below. Reporting
    // every abort as a bare KeyboardInterrupt would throw away the real diagnostic after
    // what may have been hours of work.
    let on_progress = |p: Progress| -> bool {
        Python::attach(|py| {
            if let Err(err) = py.check_signals() {
                err.restore(py);
                return false;
            }
            let Some(cb) = progress.as_ref() else {
                return true;
            };
            let payload = PyDict::new(py);
            let called = payload
                .set_item("bytes_read", p.bytes_read)
                .and_then(|()| payload.set_item("rows", p.rows))
                .and_then(|()| payload.set_item("tables_done", p.tables_done))
                .and_then(|()| payload.set_item("table", p.table))
                .and_then(|()| cb.call1(py, (payload,)).map(|_| ()));
            match called {
                Ok(()) => true,
                Err(err) => {
                    err.restore(py);
                    false
                }
            }
        })
    };

    let outcome = py.detach(|| pipeline::run_file(&dump_path, &config, on_progress));

    // Whatever the callback restored wins: it is the cause, and Error::Interrupted is
    // only the mechanism by which the load stopped.
    if let Some(err) = PyErr::take(py) {
        return Err(err);
    }
    let report: LoadReport = outcome.map_err(|err| to_pyerr(py, err))?;

    let tables = report
        .tables
        .into_iter()
        .map(|t| Py::new(py, PyTableStats::from(t)))
        .collect::<PyResult<Vec<_>>>()?;

    Ok(PyLoadReport {
        dumped_by: report.dumped_by,
        from_database: report.from_database,
        compression: report.compression.to_string(),
        bytes_read: report.bytes_read,
        total_rows: report.total_rows,
        tables,
    })
}
