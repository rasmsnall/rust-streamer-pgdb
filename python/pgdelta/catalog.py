"""Registering the loaded tables as Unity Catalog external tables.

The Rust core writes path-based Delta tables and stops there. It cannot register them:
that needs a call to the catalog, which delta-rs does not make and which has no meaning
outside Databricks. So this module generates the SQL and, when handed a Spark session,
runs it.

Registration is idempotent and cheap. Running it after every load costs almost nothing and
means a table that appeared for the first time today is queryable today.

Two things it deliberately does not do:

- It does not create managed tables. The Rust side refuses to write into catalog-managed
  storage at all, and registering an external location as managed would defeat that.
- It does not drop anything. A table that stopped arriving keeps both its data and its
  registration, because deciding what a stale table should do is a policy question this
  library will not answer on your behalf. See ``missing_tables`` on the report.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable

if TYPE_CHECKING:  # pragma: no cover - typing only
    from . import LoadReport

__all__ = ["external_table_sql", "register_external_tables", "snapshot_sql"]

def _safe(identifier: str) -> bool:
    """Mirrors the rule the Rust side applies to an output path.

    Deliberately the same rule rather than a stricter one. A name the loader accepts must
    be registerable, or a table loads and then cannot be queried by name, and letters are
    judged by Unicode there: ``Räksmörgås`` is a legal table name.
    """
    # Only the literal space is allowed among whitespace: a tab or newline is neither
    # alphanumeric nor in the set, so it is rejected without a special case.
    return bool(identifier) and all(
        character.isalnum() or character in "_- " for character in identifier
    )


def _quote(identifier: str) -> str:
    """Backquotes one identifier, refusing anything that could break out of the quoting."""
    if not _safe(identifier):
        raise ValueError(
            f"refusing to interpolate {identifier!r} into SQL: "
            "expected letters, digits, underscores, hyphens or spaces"
        )
    return f"`{identifier}`"


def _split(qualified: str) -> tuple[str, str]:
    """Splits ``schema.table`` as the dump wrote it, tolerating an unqualified name."""
    if "." in qualified:
        schema, _, table = qualified.partition(".")
        return schema, table
    return "", qualified


def external_table_sql(
    report: "LoadReport",
    output_uri: str,
    catalog: str,
    schema: str,
    *,
    tables: Iterable[str] | None = None,
) -> list[str]:
    """Returns one ``CREATE TABLE IF NOT EXISTS ... LOCATION`` per loaded table.

    Args:
        report: the result of a load. Only tables it actually loaded are registered.
        output_uri: the same prefix the load was given.
        catalog: Unity Catalog catalog name.
        schema: schema, sometimes called a database, to register the tables into.
        tables: restrict to these qualified names. Defaults to everything loaded.

    Returns:
        The statements, in a stable order. Executing them is left to the caller so the
        SQL can be reviewed, logged, or run somewhere other than a Spark session.

    Raises:
        ValueError: if a name cannot be safely interpolated into SQL.

    The source schema becomes part of the table name rather than a nested schema, because
    Unity Catalog is three levels deep and the source is already using two of them. A
    source table ``public.users`` registers as ``<catalog>.<schema>.public_users``.
    """
    wanted = set(tables) if tables is not None else None
    prefix = output_uri.rstrip("/")
    statements = []
    for stats in report.tables:
        if wanted is not None and stats.table not in wanted:
            continue
        source_schema, source_table = _split(stats.table)
        # Checked before joining. Otherwise "public." becomes the identifier "public_",
        # which is well formed and wrong.
        for part in filter(None, [source_schema, source_table]) if source_schema else [source_table]:
            if not _safe(part):
                raise ValueError(f"refusing to interpolate {stats.table!r} into SQL")
        if not source_table:
            raise ValueError(f"refusing to register {stats.table!r}: no table name")
        relative = f"{source_schema}/{source_table}" if source_schema else source_table
        flat = f"{source_schema}_{source_table}" if source_schema else source_table
        target = f"{_quote(catalog)}.{_quote(schema)}.{_quote(flat)}"
        statements.append(
            f"CREATE TABLE IF NOT EXISTS {target} "
            f"USING DELTA LOCATION '{prefix}/{relative}'"
        )
    return statements


def register_external_tables(
    spark: Any,
    report: "LoadReport",
    output_uri: str,
    catalog: str,
    schema: str,
    *,
    tables: Iterable[str] | None = None,
) -> list[str]:
    """Executes :func:`external_table_sql` against a Spark session.

    Args:
        spark: a session with ``sql()``, normally the one Databricks injects.
        report: the result of a load.
        output_uri: the same prefix the load was given.
        catalog: Unity Catalog catalog name.
        schema: schema to register into. Created if absent.
        tables: restrict to these qualified names.

    Returns:
        The statements that were executed.

    Raises:
        ValueError: if a name cannot be safely interpolated into SQL.

    The statements use ``IF NOT EXISTS``, so this is safe to run after every load. It does
    not update an existing registration: a table whose *location* changed must be dropped
    and re-registered, which this will not do silently.
    """
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_quote(catalog)}.{_quote(schema)}")
    statements = external_table_sql(
        report, output_uri, catalog, schema, tables=tables
    )
    for statement in statements:
        spark.sql(statement)
    return statements


def snapshot_sql(output_uri: str, load_id: str) -> str:
    """Returns SQL listing the Delta version each table reached in one load.

    Args:
        output_uri: the same prefix the load was given.
        load_id: the identifier returned on the report.

    Returns:
        A query against the load history.

    Reading every table at the version this returns reconstructs exactly the set that load
    produced, whatever has happened since. That is what the history is for: Delta has no
    cross-table transaction, so consistency across tables is recovered after the fact
    rather than enforced at write time.

    Note that time travel only works while the files survive. Once ``VACUUM`` has passed
    its retention window, an older load stops being readable.
    """
    prefix = output_uri.rstrip("/")
    return (
        f"SELECT table, delta_version, rows, status\n"
        f"FROM delta.`{prefix}/_pgdelta_loads`\n"
        f"WHERE load_id = '{load_id}'\n"
        f"ORDER BY table"
    )
