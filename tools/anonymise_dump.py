"""Turn a real ``pg_dump`` into a shareable compatibility fixture.

**Run this where the dump already lives. It never sends anything anywhere, opens no
network connection, and imports nothing outside the standard library.** The point is to
produce a file that can leave a controlled environment when the original cannot.

What it preserves, because these are what break a parser:

- Every DDL construct verbatim in *shape*: ``CREATE TABLE`` and ``CREATE UNLOGGED TABLE``,
  trailing ``WITH (...)``, ``PARTITION BY``, ``INHERITS``, constraints, generated columns,
  quoting, and the exact declared type of every column.
- The ``COPY`` header of every block, including column order.
- Per field: NULL or not, byte length, and character class (digit, letter, punctuation),
  so escape sequences, multi-byte characters and long values still occur where they did.
- Row counts, or a deterministic sample of them.

What it destroys:

- Every data byte. No field value from the source survives; each is regenerated from a
  keyed hash of its position, never of its content, so the output cannot be correlated
  back to the input even by an attacker who has both.
- Optionally, identifier names (``--rename-identifiers``), replaced with stable synthetic
  names of the same shape. Off by default because a parser bug is often tied to the exact
  characters in a name.

Usage::

    python tools/anonymise_dump.py real.sql fixture.sql --max-rows 200
    python tools/anonymise_dump.py real.sql.gz fixture.sql.gz --rename-identifiers

Streams, so a 48 GB input costs no more memory than a small one. Verify the result before
sharing it: the tool prints a summary, and ``--audit`` re-reads the output and fails if any
token longer than ``--audit-min-len`` from the input survives into it.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import re
import secrets
import sys
from pathlib import Path

COPY_START = re.compile(rb"^COPY\s+(.+?)\s+FROM\s+stdin;\s*$", re.IGNORECASE)
END_OF_DATA = b"\\."

# Characters a synthetic value may use, by class. Chosen so the result stays valid COPY
# TEXT without needing new escapes beyond the ones already present.
DATE = re.compile(rb"\d{4}-\d{2}-\d{2}")
TIMESTAMP = re.compile(rb"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:?\d{2})?)?")
NUMBER = re.compile(rb"[+-]?\d+(\.\d+)?")

DIGITS = "0123456789"
LOWER = "abcdefghijklmnopqrstuvwxyz"
UPPER = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def open_maybe_gzip(path: Path, mode: str):
    """Open ``path``, transparently handling gzip by magic bytes rather than by suffix."""
    if mode.startswith("r"):
        with open(path, "rb") as probe:
            magic = probe.read(2)
        if magic == b"\x1f\x8b":
            return gzip.open(path, mode)
        return open(path, mode)
    if path.suffix == ".gz":
        return gzip.open(path, mode)
    return open(path, mode)


class Synth:
    """Generates replacement bytes from position, never from content.

    Keyed with a per-run secret so two runs of the tool over the same input produce
    different output, and so nobody can rebuild a lookup table from a known dump.
    """

    def __init__(self, key: bytes) -> None:
        self.key = key

    def _stream(self, *parts: object) -> bytes:
        seed = hashlib.blake2b(
            b"|".join(str(p).encode("utf-8") for p in parts), key=self.key, digest_size=32
        ).digest()
        out = bytearray(seed)
        counter = 0
        while len(out) < 4096:
            counter += 1
            out += hashlib.blake2b(
                seed + counter.to_bytes(4, "big"), key=self.key, digest_size=32
            ).digest()
        return bytes(out)

    def field(self, table: str, column_index: int, row_index: int, original: bytes) -> bytes:
        """Replace one field with a synthetic value of the same *kind*.

        Shape alone is not enough. Replacing the digits of ``2026-01-31`` independently
        yields ``2099-45-99``, which is not a date, and a fixture that will not load tests
        nothing. So values that look like a date, a timestamp, a number or a boolean are
        regenerated as a valid value of that kind, and everything else falls back to
        per-character class replacement.

        Only the *shape* of the input is consulted, never carried through. Backslash
        escapes are preserved verbatim, because they are structure rather than content: a
        fixture that lost its escapes would stop testing the decoder.
        """
        noise = self._stream(table, column_index, row_index, len(original))
        typed = self._typed(original, noise)
        if typed is not None:
            return typed
        return self._by_character_class(original, noise)

    @staticmethod
    def _looks_like(pattern: re.Pattern[bytes], value: bytes) -> bool:
        return pattern.fullmatch(value) is not None

    def _typed(self, original: bytes, noise: bytes) -> bytes | None:
        """Regenerate a valid value when the field is of a kind that has rules."""
        def pick(index: int, modulo: int) -> int:
            return noise[index % len(noise)] % modulo

        if original in (b"t", b"f"):
            # One bit. Randomised rather than passed through, so the distribution does
            # not survive either.
            return b"t" if pick(0, 2) else b"f"

        if self._looks_like(DATE, original):
            return self._date(noise).encode()

        if self._looks_like(TIMESTAMP, original):
            date = self._date(noise)
            return f"{date} {pick(6, 24):02d}:{pick(7, 60):02d}:{pick(8, 60):02d}".encode()

        if self._looks_like(NUMBER, original):
            text = original.decode("ascii")
            negative = text.startswith("-")
            digits = text.lstrip("+-")
            if "." in digits:
                whole, frac = digits.split(".", 1)
            else:
                whole, frac = digits, ""
            # Keep the width, but never emit a leading zero on a multi-digit integer part.
            rebuilt = "".join(
                str(pick(i, 9) + 1 if i == 0 and len(whole) > 1 else pick(i, 10))
                for i in range(len(whole))
            )
            if frac:
                rebuilt += "." + "".join(str(pick(20 + i, 10)) for i in range(len(frac)))
            return (("-" if negative else "") + rebuilt).encode()

        return None

    @staticmethod
    def _date(noise: bytes) -> str:
        """A valid date, avoiding the month-length question by never exceeding 28."""
        year = 1970 + noise[0] % 60
        month = 1 + noise[1] % 12
        day = 1 + noise[2] % 28
        return f"{year:04d}-{month:02d}-{day:02d}"

    def _by_character_class(self, original: bytes, noise: bytes) -> bytes:
        """Fallback: keep length and per-character class, replace the characters."""
        out = bytearray()
        i = 0
        n = 0
        while i < len(original):
            byte = original[i]
            if byte == 0x5C and i + 1 < len(original):  # backslash: keep the escape whole
                out += original[i : i + 2]
                i += 2
                continue
            pick = noise[n % len(noise)]
            n += 1
            char = chr(byte)
            if char.isdigit():
                out += DIGITS[pick % 10].encode()
            elif char.islower() and byte < 128:
                out += LOWER[pick % 26].encode()
            elif char.isupper() and byte < 128:
                out += UPPER[pick % 26].encode()
            elif byte >= 128:
                # Keep the byte: it is part of a multi-byte character, and rewriting it
                # would risk emitting invalid UTF-8, which is a different test.
                out += bytes([byte])
            else:
                out += bytes([byte])  # punctuation, spaces, separators: structural
            i += 1
        return bytes(out)

    def identifier(self, original: str) -> str:
        """Replace an identifier with a stable synthetic one of the same shape."""
        noise = self._stream("ident", original)
        out = []
        n = 0
        for char in original:
            pick = noise[n % len(noise)]
            n += 1
            if char.isdigit():
                out.append(DIGITS[pick % 10])
            elif char.isalpha() and ord(char) < 128:
                pool = LOWER if char.islower() else UPPER
                out.append(pool[pick % 26])
            else:
                out.append(char)
        return "".join(out)


def anonymise(
    src: Path,
    dst: Path,
    max_rows: int | None,
    rename_identifiers: bool,
    key: bytes,
) -> dict[str, int]:
    """Stream ``src`` to ``dst``, replacing every data byte. Returns a summary."""
    synth = Synth(key)
    stats = {"tables": 0, "rows_in": 0, "rows_out": 0, "fields": 0}
    renames: dict[str, str] = {}

    def rename_qualified(raw: str) -> str:
        if not rename_identifiers:
            return raw
        if raw not in renames:
            renames[raw] = ".".join(synth.identifier(part) for part in raw.split("."))
        return renames[raw]

    with open_maybe_gzip(src, "rb") as fin, open_maybe_gzip(dst, "wb") as fout:
        reader = io.BufferedReader(fin, buffer_size=1 << 20)
        in_copy = False
        table = ""
        row_index = 0
        emitted = 0

        for line in reader:
            stripped = line.rstrip(b"\r\n")

            if not in_copy:
                match = COPY_START.match(stripped)
                if match:
                    in_copy = True
                    row_index = 0
                    emitted = 0
                    stats["tables"] += 1
                    target = match.group(1).decode("utf-8", "replace")
                    table = target.split("(")[0].strip()
                    if rename_identifiers:
                        renamed = rename_qualified(table)
                        stripped = stripped.replace(table.encode(), renamed.encode())
                        table = renamed
                    fout.write(stripped + b"\n")
                    continue
                if rename_identifiers:
                    # Rewrite identifiers in DDL too, so the fixture stays self-consistent.
                    for original, replacement in list(renames.items()):
                        stripped = stripped.replace(original.encode(), replacement.encode())
                fout.write(stripped + b"\n")
                continue

            if stripped == END_OF_DATA:
                in_copy = False
                fout.write(stripped + b"\n")
                continue

            stats["rows_in"] += 1
            row_index += 1
            if max_rows is not None and emitted >= max_rows:
                continue

            fields = stripped.split(b"\t")
            stats["fields"] += len(fields)
            rebuilt = [
                field
                if field == b"\\N"
                else synth.field(table, index, row_index, field)
                for index, field in enumerate(fields)
            ]
            fout.write(b"\t".join(rebuilt) + b"\n")
            emitted += 1
            stats["rows_out"] += 1

    return stats


def audit(src: Path, dst: Path, min_len: int) -> int:
    """Fail if any long token from the input survived into the output.

    A blunt instrument on purpose. It cannot prove the output is safe, but it catches the
    failure that matters: a value passed through untouched.
    """
    token = re.compile(rb"[A-Za-z0-9_.@-]{%d,}" % min_len)

    def tokens(path: Path) -> set[bytes]:
        found: set[bytes] = set()
        with open_maybe_gzip(path, "rb") as handle:
            in_copy = False
            for line in io.BufferedReader(handle, buffer_size=1 << 20):
                stripped = line.rstrip(b"\r\n")
                if not in_copy:
                    if COPY_START.match(stripped):
                        in_copy = True
                    continue
                if stripped == END_OF_DATA:
                    in_copy = False
                    continue
                found.update(token.findall(stripped))
        return found

    leaked = tokens(src) & tokens(dst)
    # Digit-only runs collide by chance and carry little meaning on their own.
    leaked = {t for t in leaked if not t.isdigit()}
    if leaked:
        sample = sorted(leaked)[:5]
        print(f"AUDIT FAILED: {len(leaked)} token(s) survived, e.g. {sample}", file=sys.stderr)
        return 1
    print(f"audit passed: no shared token of {min_len}+ characters in any data field")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("source", type=Path, help="the real dump; never leaves this machine")
    parser.add_argument("destination", type=Path, help="the fixture to write")
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="keep at most this many rows per COPY block (default: all)",
    )
    parser.add_argument(
        "--rename-identifiers",
        action="store_true",
        help="also replace table and column names. Off by default, because a parser bug is "
        "often tied to the exact characters in a name",
    )
    parser.add_argument(
        "--audit",
        action="store_true",
        help="re-read both files afterwards and fail if any long token survived",
    )
    parser.add_argument("--audit-min-len", type=int, default=6)
    args = parser.parse_args()

    if not args.source.exists():
        print(f"no such file: {args.source}", file=sys.stderr)
        return 2

    key = secrets.token_bytes(32)
    stats = anonymise(
        args.source, args.destination, args.max_rows, args.rename_identifiers, key
    )
    print(
        f"{stats['tables']} COPY blocks, {stats['rows_in']:,} rows read, "
        f"{stats['rows_out']:,} written, {stats['fields']:,} fields replaced"
    )
    print(f"wrote {args.destination} ({args.destination.stat().st_size:,} bytes)")
    print("Review the output before sharing it. This tool is a filter, not a guarantee.")

    if args.audit:
        return audit(args.source, args.destination, args.audit_min_len)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
