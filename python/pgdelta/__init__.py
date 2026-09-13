"""Stream ``pg_dump`` plain-text output directly into Delta Lake tables.

The public surface is :func:`stream_dump_to_delta`, which reads a dump file once and
writes one Delta table per source table beneath a target prefix, committing nothing until
the whole dump has been consumed cleanly. :func:`validate_dump` runs the same structural,
type-fidelity and path-traversal checks without writing anything, for a cheap pre-flight
check before a real load.
"""

from ._pgdelta import (
    LoadReport,
    PartialCommitError,
    TableStats,
    TableValidation,
    ValidationReport,
    stream_dump_to_delta,
    validate_dump,
)

__all__ = [
    "stream_dump_to_delta",
    "validate_dump",
    "LoadReport",
    "TableStats",
    "ValidationReport",
    "TableValidation",
    "PartialCommitError",
]
__version__ = "0.1.0"
