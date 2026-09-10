"""End-to-end round trip through the built wheel.

Builds a small dump exercising the constructs that have broken before, loads it, and reads
the result back with ``deltalake`` to check the values actually survived. Run it against an
installed wheel::

    python tools/smoke.py

Exits non-zero on the first failure, so it is usable as a CI gate. ``deltalake`` and
``pyarrow`` are optional: without them the load is still exercised and only the read-back
is skipped.
"""

from __future__ import annotations

import gzip
import sys
import tempfile
import textwrap
from pathlib import Path

import pgdelta

# Every construct here has broken this library at least once, or is the reason a guard
# exists. Keep additions in that spirit rather than adding volume.
DUMP = textwrap.dedent(
    """\
    -- Dumped from database version 17.4
    -- Dumped by pg_dump version 17.4
    CREATE TABLE public."Räksmörgås" (
        id integer,
        "belopp_öre" bigint,
        odd numeric(2,5),
        ratio numeric(10,2),
        when_ts timestamp without time zone,
        when_tz timestamp with time zone,
        born date,
        raw bytea,
        flag boolean
    )
    WITH (fillfactor='70');
    CREATE UNLOGGED TABLE public.staging (
        id integer,
        note text
    );
    CREATE TABLE public.parted (
        id integer,
        created date
    )
    PARTITION BY RANGE (created);
    CREATE TABLE public.oddities (
        id integer,
        kind mystery_enum,
        tags text[],
        span interval
    );
    COPY public."Räksmörgås" (id, "belopp_öre", odd, ratio, when_ts, when_tz, born, raw, flag) FROM stdin;
    1\t250\t1.5\t12.50\t2026-01-31 12:00:00\t2026-01-31 12:00:00+02\t2026-01-31\t\\\\x48690a\tt
    2\t\\N\t\\N\t\\N\t\\N\t\\N\tinfinity\t\\N\tf
    \\.
    COPY public.staging (id, note) FROM stdin;
    7\tc\\tarol
    8\tline\\nbreak
    \\.
    COPY public.parted (id, created) FROM stdin;
    9\t2026-01-31
    \\.
    COPY public.oddities (id, kind, tags, span) FROM stdin;
    1\tclick\t{a,b}\t1 day 02:03:04
    \\.
    """
)

EXPECTED_TABLES = {
    "public.Räksmörgås": 2,
    "public.staging": 2,
    "public.parted": 1,
    "public.oddities": 1,
}


def check(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"FAIL: {message}")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="pgdelta-smoke-"))
    # gzipped, so the decompression path and the pre-decompression byte count are both
    # exercised rather than assumed.
    dump_path = tmp / "day.sql.gz"
    dump_path.write_bytes(gzip.compress(DUMP.encode("utf-8")))
    out = tmp / "delta"
    out.mkdir()
    uri = f"file://{out.as_posix()}"

    events: list[dict] = []
    report = pgdelta.stream_dump_to_delta(
        dump_path,
        uri,
        mode="overwrite",
        expect_pg_major=17,
        threads=4,
        batch_rows=1000,
        progress=events.append,
    )

    print("report:", report)
    for stats in report.tables:
        print(f"   {stats}  text_fallback={stats.text_fallback_columns}")

    by_name = {t.table: t for t in report.tables}
    check(set(by_name) == set(EXPECTED_TABLES), f"tables were {sorted(by_name)}")
    for name, rows in EXPECTED_TABLES.items():
        check(by_name[name].rows == rows, f"{name} had {by_name[name].rows} rows, want {rows}")
        check(by_name[name].delta_version >= 1, f"{name} was not committed")

    check(report.compression == "gzip", f"compression was {report.compression}")
    check(report.dumped_by == 17, "pg_dump major not recovered")
    check(
        report.bytes_read == dump_path.stat().st_size,
        f"bytes_read {report.bytes_read} != gzip size {dump_path.stat().st_size}",
    )
    check(len(events) > 0, "progress callback never fired")

    # An unrecognised type must degrade to text and say so; a recognised one must not.
    check(
        by_name["public.oddities"].text_fallback_columns.get("kind") == "mystery_enum",
        "an unrecognised type was not reported",
    )
    check(
        "ratio" not in by_name["public.Räksmörgås"].text_fallback_columns,
        "a representable numeric was wrongly reported as a fallback",
    )
    # infinity has no Arrow encoding and must be counted, not silently dropped.
    check(
        by_name["public.Räksmörgås"].null_substitutions.get("born") == 1,
        "an infinity date was not counted as a substitution",
    )

    # A quoted non-ASCII name must survive as a path component.
    check((out / "public" / "Räksmörgås").is_dir(), "non-ASCII table path missing")

    try:
        from deltalake import DeltaTable
    except ImportError:
        print("deltalake not installed; skipping read-back")
        print("OK (load only)")
        return 0

    rows = DeltaTable(str(out / "public" / "Räksmörgås")).to_pyarrow_table().to_pylist()
    rows.sort(key=lambda r: r["id"])
    print("read-back:", rows)
    check(rows[0]["belopp_öre"] == 250, "quoted UTF-8 column name did not survive")
    check(rows[1]["belopp_öre"] is None, "NULL was not preserved")
    check(str(rows[0]["ratio"]) == "12.50", f"numeric became {rows[0]['ratio']!r}")
    check(rows[0]["odd"] == "1.5", "numeric(2,5) should degrade to text")
    check(rows[0]["raw"] == b"Hi\n", f"bytea hex decode gave {rows[0]['raw']!r}")
    check(rows[0]["flag"] is True and rows[1]["flag"] is False, "boolean round trip")
    check(rows[1]["born"] is None, "infinity date should be NULL")

    staging = DeltaTable(str(out / "public" / "staging")).to_pyarrow_table().to_pylist()
    staging.sort(key=lambda r: r["id"])
    check(staging[0]["note"] == "c\tarol", "escaped tab did not decode")
    check(staging[1]["note"] == "line\nbreak", "escaped newline did not decode")

    oddities = DeltaTable(str(out / "public" / "oddities")).to_pyarrow_table().to_pylist()
    check(oddities[0]["tags"] == "{a,b}", "array should stay a PostgreSQL literal")
    check(oddities[0]["span"] == "1 day 02:03:04", "interval should stay a literal")

    print("OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
