"""Validate a pgdelta wheel on a Databricks cluster.

Run this as a notebook cell or a job task on each Databricks Runtime you intend to
support, currently 16.4 LTS and 17.3 LTS. It cannot be run from CI: it needs a real
workspace, a real external location, and a real Unity Catalog.

    %pip install /Volumes/<catalog>/<schema>/<vol>/pgdelta-0.1.0-cp310-abi3-manylinux*.whl
    dbutils.library.restartPython()

then, in a later cell::

    OUTPUT = "abfss://<container>@<account>.dfs.core.windows.net/bronze/pgdelta-validation/"
    CATALOG, SCHEMA = "main", "pgdelta_validation"
    exec(open("/Workspace/.../databricks_validate.py").read())

What it checks, in order, stopping at the first failure:

1. The wheel imports and reports its version and the interpreter it loaded on.
2. A synthetic dump loads end to end against the real external location.
3. The values survive the round trip, read back through Spark rather than through the
   library that wrote them.
4. A second dump with changed DDL is applied and reported.
5. The load history records the versions, and reading at them reconstructs the first run.
6. The tables register as Unity Catalog external tables and are queryable by name.
7. Catalog-managed storage is refused.

Every check prints PASS or raises. Nothing is cleaned up automatically: the tables are
left in place so a failure can be investigated. Drop the schema and delete the prefix when
finished.
"""

from __future__ import annotations

import gzip
import sys
import textwrap

import pgdelta
from pgdelta.catalog import register_external_tables, snapshot_sql

# Set these before running. OUTPUT must be an external location, not a managed one.
OUTPUT = globals().get("OUTPUT", "")
CATALOG = globals().get("CATALOG", "main")
SCHEMA = globals().get("SCHEMA", "pgdelta_validation")
LANDING = globals().get("LANDING", "/tmp/pgdelta-validation")

DAY1 = textwrap.dedent(
    """\
    -- Dumped from database version 17.4
    -- Dumped by pg_dump version 17.4
    CREATE TABLE public.customers (
        id integer,
        "belopp_öre" bigint,
        ratio numeric(10,2),
        seen_at timestamp without time zone,
        seen_tz timestamp with time zone,
        born date,
        raw bytea,
        active boolean
    )
    WITH (fillfactor='70');
    CREATE UNLOGGED TABLE public.staging (
        id integer,
        note text
    );
    COPY public.customers (id, "belopp_öre", ratio, seen_at, seen_tz, born, raw, active) FROM stdin;
    1\t250\t12.50\t2026-01-31 12:00:00\t2026-01-31 12:00:00+02\t2026-01-31\t\\\\x48690a\tt
    2\t\\N\t\\N\t\\N\t\\N\tinfinity\t\\N\tf
    \\.
    COPY public.staging (id, note) FROM stdin;
    7\tc\\tarol
    \\.
    """
)

DAY2 = textwrap.dedent(
    """\
    -- Dumped from database version 17.4
    -- Dumped by pg_dump version 17.4
    CREATE TABLE public.staging (
        id integer,
        note text,
        added_col integer
    );
    COPY public.staging (id, note, added_col) FROM stdin;
    7\tc\\tarol\t1
    8\tsecond\t2
    \\.
    """
)

_checks = 0


def check(condition: bool, message: str) -> None:
    global _checks
    if not condition:
        raise AssertionError(f"FAIL: {message}")
    _checks += 1
    print(f"  PASS  {message}")


def section(title: str) -> None:
    print(f"\n=== {title} ===")


def main() -> int:
    if not OUTPUT:
        raise SystemExit(
            "set OUTPUT to an external location before running, for example\n"
            '  OUTPUT = "abfss://c@a.dfs.core.windows.net/bronze/pgdelta-validation/"'
        )

    section("1. Interpreter and wheel")
    print(f"  python  {sys.version}")
    print(f"  pgdelta {pgdelta.__version__}")
    try:
        runtime = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")  # noqa: F821
        print(f"  runtime {runtime}")
    except Exception:  # pragma: no cover - not every context exposes it
        print("  runtime unknown")
    check(callable(pgdelta.stream_dump_to_delta), "the extension module loaded")
    check(
        issubclass(pgdelta.PartialCommitError, RuntimeError),
        "PartialCommitError subclasses RuntimeError",
    )

    section("2. Load against the real external location")
    dbutils.fs.mkdirs(LANDING)  # noqa: F821
    day1 = f"{LANDING}/day1.sql.gz"
    day2 = f"{LANDING}/day2.sql"
    _write(day1, gzip.compress(DAY1.encode("utf-8")))
    _write(day2, DAY2.encode("utf-8"))

    first = pgdelta.stream_dump_to_delta(
        _local(day1), OUTPUT, mode="overwrite", expect_pg_major=17
    )
    print(f"  {first}")
    check(first.total_rows == 3, f"3 rows loaded, got {first.total_rows}")
    check(first.compression == "gzip", "gzip was decoded")
    check(first.load_id is not None, "a load id was recorded")

    section("3. Values survive the round trip, read through Spark")
    rows = {
        r["id"]: r
        for r in spark.read.format("delta")  # noqa: F821
        .load(f"{OUTPUT.rstrip('/')}/public/customers")
        .collect()
    }
    check(rows[1]["belopp_öre"] == 250, "a quoted non-ASCII column name survived")
    check(str(rows[1]["ratio"]) == "12.50", f"numeric(10,2) is {rows[1]['ratio']}")
    check(rows[1]["raw"] == bytearray(b"Hi\n"), f"bytea is {rows[1]['raw']!r}")
    check(rows[2]["born"] is None, "an infinity date became NULL")
    check(rows[1]["active"] and not rows[2]["active"], "booleans round tripped")
    check(
        first.tables[0].null_substitutions.get("born") == 1
        or first.tables[1].null_substitutions.get("born") == 1,
        "the infinity substitution was counted",
    )

    section("4. A changed schema is applied and reported")
    second = pgdelta.stream_dump_to_delta(
        _local(day2), OUTPUT, mode="overwrite", expect_pg_major=17
    )
    drift = {t.table: t.schema_drift for t in second.tables}["public.staging"]
    print(f"  drift {drift}")
    check(drift["added"] == [["added_col"]], f"added_col reported, got {drift}")
    columns = spark.read.format("delta").load(  # noqa: F821
        f"{OUTPUT.rstrip('/')}/public/staging"
    ).columns
    check(columns == ["id", "note", "added_col"], f"schema is now {columns}")

    section("5. The history makes the first run readable again")
    query = snapshot_sql(OUTPUT, first.load_id)
    print(textwrap.indent(query, "  "))
    recorded = {r["table"]: r["delta_version"] for r in spark.sql(query).collect()}  # noqa: F821
    check(len(recorded) >= 2, f"the history recorded {len(recorded)} tables")
    staged_version = recorded["public.staging"]
    old = (
        spark.read.format("delta")  # noqa: F821
        .option("versionAsOf", staged_version)
        .load(f"{OUTPUT.rstrip('/')}/public/staging")
    )
    check(
        old.columns == ["id", "note"],
        f"at version {staged_version} the schema is the first run's, got {old.columns}",
    )
    check(old.count() == 1, "the first run's row count is recoverable")

    section("6. Unity Catalog external tables")
    statements = register_external_tables(spark, second, OUTPUT, CATALOG, SCHEMA)  # noqa: F821
    for statement in statements:
        print(f"  {statement}")
    count = spark.sql(  # noqa: F821
        f"SELECT count(*) AS n FROM `{CATALOG}`.`{SCHEMA}`.`public_staging`"
    ).collect()[0]["n"]
    check(count == 2, f"the registered table is queryable, got {count} rows")

    section("7. Managed storage is refused")
    try:
        pgdelta.stream_dump_to_delta(
            _local(day2), "abfss://c@a.dfs.core.windows.net/__unitystorage/x/y"
        )
    except ValueError as exc:
        check("managed" in str(exc).lower(), f"refused with: {exc}")
    else:
        raise AssertionError("FAIL: managed storage was not refused")

    print(f"\nAll {_checks} checks passed.")
    print(f"Left in place for inspection: {OUTPUT} and {CATALOG}.{SCHEMA}")
    return 0


def _write(path: str, payload: bytes) -> None:
    """Writes through the driver filesystem, which /Volumes and /tmp both expose."""
    with open(_local(path), "wb") as handle:
        handle.write(payload)


def _local(path: str) -> str:
    """Maps a dbfs:/ path to its FUSE equivalent, leaving other paths alone."""
    if path.startswith("dbfs:/"):
        return "/dbfs/" + path[len("dbfs:/") :].lstrip("/")
    return path


if __name__ == "__main__":
    raise SystemExit(main())
else:
    main()
