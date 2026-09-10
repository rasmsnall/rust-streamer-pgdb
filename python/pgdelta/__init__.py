"""Stream ``pg_dump`` plain-text output directly into Delta Lake tables.

The public surface is :func:`stream_dump_to_delta`, which reads a dump file once and
writes one Delta table per source table beneath a target prefix, committing nothing until
the whole dump has been consumed cleanly.
"""

from ._pgdelta import LoadReport, TableStats, stream_dump_to_delta

__all__ = ["stream_dump_to_delta", "LoadReport", "TableStats"]
__version__ = "0.1.0"
