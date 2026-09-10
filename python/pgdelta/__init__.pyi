"""Type stubs for the ``pgdelta`` extension module.

The naive-timestamp assumption is documented on :func:`stream_dump_to_delta`: a
PostgreSQL ``timestamp without time zone`` is written as Delta ``timestamp`` (microseconds
UTC), so naive timestamps are assumed to be UTC.
"""

from collections.abc import Callable, Mapping, Sequence
from os import PathLike
from typing import Any, Literal, TypedDict

__version__: str
__all__: list[str]

class PartialCommitError(RuntimeError):
    """A commit failed after some tables had already been committed.

    Delta has no cross-table transaction, so the target now holds a mixture of this run
    and the previous one. Re-run to restore consistency: ``overwrite`` rewrites every
    table from the same dump.

    Subclasses :class:`RuntimeError`, so a handler written before this type existed still
    catches it. Catch it specifically when the difference between "nothing changed" and
    "some tables changed" should drive different recovery.
    """

    table: str
    """Qualified name of the table whose commit failed."""

    committed: int
    """Tables committed successfully before the failure. Zero means nothing became
    visible and the target is untouched."""

    total: int
    """Tables that were to be committed in this phase."""

class ProgressEvent(TypedDict):
    """Payload passed to the ``progress`` callback."""

    bytes_read: int
    rows: int
    tables_done: int
    table: str | None

class TableStats:
    """What one source table produced. Instances are immutable."""

    @property
    def table(self) -> str:
        """Qualified table name as written in the dump."""

    @property
    def rows(self) -> int:
        """Rows decoded from the table's ``COPY`` block."""

    @property
    def batches(self) -> int:
        """Arrow batches written for this table."""

    @property
    def delta_version(self) -> int:
        """Delta version produced by the phase-two commit."""

    @property
    def null_substitutions(self) -> dict[str, int]:
        """``{column: count}`` for values stored as NULL because the Arrow type could not
        represent them, such as ``infinity`` or ``NaN``."""

    @property
    def text_fallback_columns(self) -> dict[str, str]:
        """``{column: declared_type}`` for columns written as text because their declared
        PostgreSQL type was not recognised."""

class LoadReport:
    """The outcome of a completed load. Instances are immutable."""

    @property
    def dumped_by(self) -> int:
        """``pg_dump`` major version that wrote the dump."""

    @property
    def from_database(self) -> int | None:
        """Source server major version, when the preamble stated one."""

    @property
    def compression(self) -> str:
        """Compression decoded off the input, or ``"none"``."""

    @property
    def bytes_read(self) -> int:
        """Bytes taken from the input, counted before decompression."""

    @property
    def total_rows(self) -> int:
        """Rows decoded across every loaded table."""

    @property
    def tables(self) -> list[TableStats]:
        """One entry per loaded table, in the order their blocks closed."""

def stream_dump_to_delta(
    dump_path: str | PathLike[str],
    output_uri: str,
    *,
    tables: Sequence[str] | None = ...,
    mode: Literal["overwrite", "append", "error"] = ...,
    batch_rows: int = ...,
    batch_bytes: int = ...,
    threads: int | None = ...,
    storage_options: Mapping[str, str] | None = ...,
    expect_pg_major: int | None = ...,
    max_field_bytes: int | None = ...,
    max_row_bytes: int | None = ...,
    max_columns: int | None = ...,
    progress: Callable[[ProgressEvent], Any] | None = ...,
) -> LoadReport:
    """Stream a ``pg_dump`` plain-text file into one Delta table per source table.

    The dump is read once, front to back. Every table's Parquet is written and staged
    during the pass; nothing becomes visible to a Delta reader until the pass completes
    without error, at which point every table is committed. A failure during that pass,
    which is where essentially every failure occurs, leaves orphaned files and no visible
    change to any table.

    That is not cross-table atomicity, which Delta cannot provide: the commit at the end is
    one independent commit per table, so a failure partway through it leaves some tables on
    the new day and some on the old, and a reader during it can see a mixture. Re-run to
    recover; ``overwrite`` is idempotent.

    Parameters
    ----------
    dump_path:
        Path to the plain-text dump. gzip is decompressed transparently.
    output_uri:
        Prefix the tables are written beneath, for example ``/Volumes/main/raw/pg/`` or an
        ``abfss://`` URL. Each table's sub-path comes from its qualified name and is
        rejected, not sanitised, if it would escape the prefix.
    tables:
        Qualified names to load. ``None`` loads every table in the dump; other ``COPY``
        blocks are scanned for their terminator and skipped.
    mode:
        ``"overwrite"`` tombstones the previous run's files in the same commit that adds
        the new ones. ``"append"`` adds to what is there. ``"error"`` fails if the target
        already holds data.
    batch_rows, batch_bytes:
        Bounds on one in-memory Arrow batch. They cap memory; they do not set the Parquet
        file size. Peak memory is roughly ``threads * batch_bytes``.
    threads:
        Decode and Parquet-encode workers. ``None`` uses the machine's parallelism.
    storage_options:
        Backend options passed through to ``object_store``.
    expect_pg_major:
        When set, the load fails unless the dump's ``pg_dump`` major matches exactly. This
        is the tripwire for the source system being upgraded without notice.
    max_field_bytes, max_row_bytes, max_columns:
        Override the decode limits that protect the driver from a hostile dump.
    progress:
        Called with a :class:`ProgressEvent` after each chunk and each table. Raising from
        it, or a Ctrl-C, aborts the load before any commit; the exception you raised is
        the one that propagates, not a generic ``KeyboardInterrupt``.

    Returns
    -------
    LoadReport

    Raises
    ------
    ValueError
        A malformed dump: a truncated ``COPY`` block, a row whose field count disagrees
        with its ``COPY`` header, a bad escape, a value that contradicts its column type,
        an unqualified or mismatched ``pg_dump`` major, or a table name that escapes the
        prefix.
    PartialCommitError
        A commit failed part way through the final burst, so some tables hold this run's
        contents and the rest hold the previous run's. This is the only failure that can
        leave visible change behind. Subclasses ``RuntimeError``.
    RuntimeError
        The Delta or Arrow write path failed, or an internal invariant was violated.
    OSError
        The dump could not be read.
    KeyboardInterrupt
        The load was interrupted. Nothing was committed.

    Notes
    -----
    A PostgreSQL ``timestamp without time zone`` is written as Delta ``timestamp``, which
    is microseconds UTC: naive timestamps are therefore assumed to be UTC. Delta
    ``timestamp_ntz`` would raise the table's protocol version above the compatibility
    floor this library targets, so it is not used.
    """
