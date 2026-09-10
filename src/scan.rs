//! Streaming scanner over a plain-text dump.
//!
//! Consumes the dump as a sequence of byte chunks and emits [`Event`] values: the
//! `pg_dump` version from the preamble, a [`TableDef`] for each `CREATE TABLE`, and the
//! boundaries and row payloads of each `COPY` block.
//!
//! Executes on the reader thread and is **strictly sequential**. DDL must be observed in
//! order, so this stage cannot be parallelised. It is not a bottleneck: DDL is a
//! negligible fraction of the bytes in a dump whose size is dominated by rows, and row
//! payloads are passed through as borrowed slices for the decode pool to handle.
//!
//! Nothing here is async.
//!
//! # Ordering guarantee
//!
//! PostgreSQL emits its entire pre-data section before its data section, so every
//! `CREATE TABLE` is seen before the `COPY` that needs it. That is what makes a single
//! forward pass possible with no seeking and no buffering of the dump.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use crate::copy::is_end_of_data;
use crate::error::{Error, Result};

/// `pg_dump` major versions this build has been qualified against.
///
/// A dump from any other major fails the load rather than being parsed speculatively,
/// which is the tripwire for a source system being upgraded without notice.
pub const SUPPORTED_MAJORS: &[u32] = &[16, 17];

/// A table's name, with the schema kept separate from the table identifier.
///
/// The two are never joined into one string internally, because a dot is legal inside a
/// quoted PostgreSQL identifier. Joined, `public."a.b"` and schema `public.a` table `b`
/// are indistinguishable and would map to the same output path. Table names come from a
/// third party, so that collision is a security boundary rather than a nicety; see
/// [`crate::sink::relative_path`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName {
    /// Schema, when the dump qualified the name.
    pub schema: Option<String>,
    /// Table identifier, with any quoting removed and `""` unescaped.
    pub table: String,
}

impl TableName {
    /// Renders the name the way the dump writes it, for statistics, filters and messages.
    ///
    /// Display only. It is ambiguous when an identifier contains a dot, which is exactly
    /// why the parts are stored apart; never split the result to recover them.
    ///
    /// # Panics
    ///
    /// Does not panic.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgdelta::scan::TableName;
    ///
    /// let name = TableName { schema: Some("public".into()), table: "users".into() };
    /// assert_eq!(name.qualified(), "public.users");
    /// ```
    pub fn qualified(&self) -> String {
        self.to_string()
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.schema {
            Some(schema) => write!(f, "{schema}.{}", self.table),
            None => f.write_str(&self.table),
        }
    }
}

/// One column recovered from a `CREATE TABLE` statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name, with any identifier quoting removed.
    pub name: String,
    /// The SQL type text, verbatim and with parameters intact, for example
    /// `numeric(10,2)` or `timestamp(3) without time zone`.
    pub sql_type: String,
}

/// A table's shape, recovered from `CREATE TABLE`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableDef {
    /// Name as written in the dump, with schema and table kept apart.
    pub name: TableName,
    /// Columns in declaration order. Table-level constraints are not represented.
    pub columns: Vec<ColumnDef>,
}

/// Something the scanner recognised.
///
/// [`Event::CopyRows`] borrows from the chunk passed to [`Scanner::feed`] in the normal
/// case. It owns its bytes only for a row that straddles a chunk boundary, which cannot
/// occur when the caller supplies newline-aligned chunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event<'a> {
    /// Versions recovered from the dump preamble.
    DumpVersion {
        /// Major version of the `pg_dump` that wrote this dump.
        dumped_by: u32,
        /// Major version of the server it was read from, when stated.
        from_database: Option<u32>,
    },
    /// A table definition.
    Table(TableDef),
    /// A `COPY` block began.
    CopyStart {
        /// Table name, schema kept separate.
        table: TableName,
        /// Column names from the `COPY` statement, which need not match declaration
        /// order in `CREATE TABLE`.
        columns: Vec<String>,
    },
    /// A run of whole, newline-separated rows belonging to the open `COPY` block.
    CopyRows(Cow<'a, [u8]>),
    /// A `COPY` block closed with `\.`.
    CopyEnd {
        /// Table name, schema kept separate.
        table: TableName,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Sql,
    Copy,
}

/// Streaming dump scanner. See the module documentation.
#[derive(Debug)]
pub struct Scanner {
    state: State,
    /// Bytes after the last complete line, carried into the next chunk.
    pending: Vec<u8>,
    /// Accumulated text of a `CREATE TABLE` currently being read.
    ddl: Vec<u8>,
    in_create: bool,
    open_table: Option<TableName>,
    dumped_by: Option<u32>,
    from_database: Option<u32>,
    tables: HashMap<String, TableDef>,
}

impl Default for Scanner {
    fn default() -> Self {
        Self::new()
    }
}

impl Scanner {
    /// Creates a scanner positioned at the start of a dump.
    pub fn new() -> Self {
        Self {
            state: State::Sql,
            pending: Vec::new(),
            ddl: Vec::new(),
            in_create: false,
            open_table: None,
            dumped_by: None,
            from_database: None,
            tables: HashMap::new(),
        }
    }

    /// Major version of the `pg_dump` that produced this dump, once the preamble is read.
    pub fn dumped_by(&self) -> Option<u32> {
        self.dumped_by
    }

    /// Major version of the source server, when the preamble stated one.
    pub fn from_database(&self) -> Option<u32> {
        self.from_database
    }

    /// Table definitions recovered so far, keyed by [`TableName::qualified`].
    pub fn tables(&self) -> &HashMap<String, TableDef> {
        &self.tables
    }

    /// Consumes one chunk and returns everything recognised within it.
    ///
    /// Chunks should be newline-aligned, as produced by the reader thread. A trailing
    /// partial line is retained and completed by the next call, so alignment is an
    /// efficiency property rather than a correctness requirement.
    ///
    /// # Errors
    ///
    /// [`Error::UnsupportedDumpVersion`] for an unqualified `pg_dump` major, and
    /// [`Error::MalformedCopyHeader`] or [`Error::MalformedCreateTable`] for DDL this
    /// scanner cannot parse.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn feed<'a>(&mut self, chunk: &'a [u8]) -> Result<Vec<Event<'a>>> {
        let mut events = Vec::new();
        let mut input = chunk;

        // Complete a line left partial by the previous chunk. Its bytes are owned, so a
        // row completed this way is emitted as an owned event.
        if !self.pending.is_empty() {
            let Some(nl) = memchr::memchr(b'\n', input) else {
                self.pending.extend_from_slice(input);
                return Ok(events);
            };
            self.pending.extend_from_slice(&input[..nl]);
            let line = std::mem::take(&mut self.pending);
            let line = strip_cr(&line);
            match self.state {
                State::Sql => self.sql_line(line, &mut events)?,
                State::Copy => {
                    if is_end_of_data(line) {
                        self.close_copy(&mut events);
                    } else {
                        let mut owned = line.to_vec();
                        owned.push(b'\n');
                        events.push(Event::CopyRows(Cow::Owned(owned)));
                    }
                }
            }
            input = &input[nl + 1..];
        }

        loop {
            match self.state {
                State::Sql => {
                    let Some(nl) = memchr::memchr(b'\n', input) else { break };
                    let line = strip_cr(&input[..nl]);
                    self.sql_line(line, &mut events)?;
                    input = &input[nl + 1..];
                }
                State::Copy => {
                    let (consumed, terminator) = find_terminator(input);
                    match terminator {
                        Some(at) => {
                            if at > 0 {
                                events.push(Event::CopyRows(Cow::Borrowed(&input[..at])));
                            }
                            self.close_copy(&mut events);
                            input = &input[consumed..];
                        }
                        None => {
                            if consumed > 0 {
                                events.push(Event::CopyRows(Cow::Borrowed(&input[..consumed])));
                            }
                            input = &input[consumed..];
                            break;
                        }
                    }
                }
            }
        }

        self.pending.extend_from_slice(input);
        Ok(events)
    }

    /// Asserts that the dump ended in a valid state.
    ///
    /// # Errors
    ///
    /// [`Error::UnterminatedCopy`] if input ended inside a `COPY` block, which is the
    /// signature of a truncated transfer, and [`Error::MissingDumpVersion`] if no
    /// `pg_dump` version comment was ever seen.
    pub fn finish(&mut self) -> Result<()> {
        if self.state == State::Copy {
            return Err(Error::UnterminatedCopy {
                table: self
                    .open_table
                    .as_ref()
                    .map(TableName::qualified)
                    .unwrap_or_default(),
            });
        }
        if self.dumped_by.is_none() {
            return Err(Error::MissingDumpVersion);
        }
        Ok(())
    }

    fn close_copy(&mut self, events: &mut Vec<Event<'_>>) {
        self.state = State::Sql;
        if let Some(table) = self.open_table.take() {
            events.push(Event::CopyEnd { table });
        }
    }

    fn sql_line(&mut self, line: &[u8], events: &mut Vec<Event<'_>>) -> Result<()> {
        if self.in_create {
            self.ddl.push(b'\n');
            self.ddl.extend_from_slice(line);
            if line.trim_ascii_end().ends_with(b");") {
                self.in_create = false;
                let ddl = std::mem::take(&mut self.ddl);
                let def = parse_create_table(&ddl)?;
                self.tables.insert(def.name.qualified(), def.clone());
                events.push(Event::Table(def));
            }
            return Ok(());
        }

        if let Some(rest) = strip_prefix_ci(line, b"-- Dumped by pg_dump version ") {
            let major = leading_major(rest);
            if let Some(major) = major {
                if !SUPPORTED_MAJORS.contains(&major) {
                    return Err(Error::UnsupportedDumpVersion { found: major });
                }
                self.dumped_by = Some(major);
                events.push(Event::DumpVersion {
                    dumped_by: major,
                    from_database: self.from_database,
                });
            }
            return Ok(());
        }

        if let Some(rest) = strip_prefix_ci(line, b"-- Dumped from database version ") {
            self.from_database = leading_major(rest);
            return Ok(());
        }

        if line.starts_with(b"--") {
            return Ok(());
        }

        if strip_create_table(line).is_some() {
            // pg_dump writes the opening paren on the first line and one column per
            // line thereafter, but a single-line form is also accepted.
            self.ddl.clear();
            self.ddl.extend_from_slice(line);
            if line.trim_ascii_end().ends_with(b");") {
                let ddl = std::mem::take(&mut self.ddl);
                let def = parse_create_table(&ddl)?;
                self.tables.insert(def.name.qualified(), def.clone());
                events.push(Event::Table(def));
            } else {
                self.in_create = true;
            }
            return Ok(());
        }

        if strip_prefix_ci(line, b"COPY ").is_some() {
            let trimmed = line.trim_ascii_end();
            if ends_with_ci(trimmed, b"FROM stdin;") {
                let (table, columns) = parse_copy_header(trimmed)?;
                self.open_table = Some(table.clone());
                self.state = State::Copy;
                events.push(Event::CopyStart { table, columns });
            }
            return Ok(());
        }

        Ok(())
    }
}

/// Scans `input` for the `\.` terminator line.
///
/// Returns the number of bytes consumed and, when the terminator was found, the offset
/// at which its line begins. When it was not found, the consumed count covers every
/// complete line so the remainder is a partial line.
fn find_terminator(input: &[u8]) -> (usize, Option<usize>) {
    let mut pos = 0;
    while let Some(nl) = memchr::memchr(b'\n', &input[pos..]) {
        let start = pos;
        let end = pos + nl;
        if is_end_of_data(strip_cr(&input[start..end])) {
            return (end + 1, Some(start));
        }
        pos = end + 1;
    }
    (pos, None)
}

fn strip_cr(line: &[u8]) -> &[u8] {
    if line.last() == Some(&b'\r') {
        &line[..line.len() - 1]
    } else {
        line
    }
}

fn strip_prefix_ci<'a>(line: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    if line.len() >= prefix.len() && line[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&line[prefix.len()..])
    } else {
        None
    }
}

fn ends_with_ci(line: &[u8], suffix: &[u8]) -> bool {
    line.len() >= suffix.len() && line[line.len() - suffix.len()..].eq_ignore_ascii_case(suffix)
}

/// Extracts the leading integer of a version string, so that `17.2` and `17beta1` both
/// yield 17.
fn leading_major(s: &[u8]) -> Option<u32> {
    let digits: Vec<u8> = s
        .iter()
        .copied()
        .skip_while(|b| b.is_ascii_whitespace())
        .take_while(u8::is_ascii_digit)
        .collect();
    if digits.is_empty() {
        return None;
    }
    // Checked throughout: a malformed preamble must not wrap into a plausible version.
    std::str::from_utf8(&digits).ok()?.parse::<u32>().ok()
}

/// Splits on `sep` at paren depth zero, ignoring separators inside double-quoted
/// identifiers. `""` inside a quoted identifier is an escaped quote.
fn split_top_level(s: &[u8], sep: u8) -> Vec<&[u8]> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quoted = false;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < s.len() {
        let c = s[i];
        if quoted {
            if c == b'"' {
                if s.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                quoted = false;
            }
        } else {
            match c {
                b'"' => quoted = true,
                b'(' => depth += 1,
                b')' => depth -= 1,
                _ if c == sep && depth == 0 => {
                    out.push(&s[start..i]);
                    start = i + 1;
                }
                _ => {}
            }
        }
        i += 1;
    }
    out.push(&s[start..]);
    out
}

/// Reads one identifier, which may be double-quoted, returning it and the rest.
fn take_identifier(s: &[u8]) -> Option<(String, &[u8])> {
    let s = s.trim_ascii_start();
    if s.first() == Some(&b'"') {
        // Collect bytes and decode once. Pushing each byte `as char` would decode the
        // name as Latin-1, and pg_dump quotes every identifier that is not lowercase
        // ASCII, so that would mangle any name carrying a non-ASCII character.
        let mut name: Vec<u8> = Vec::new();
        let mut i = 1;
        while i < s.len() {
            if s[i] == b'"' {
                if s.get(i + 1) == Some(&b'"') {
                    name.push(b'"');
                    i += 2;
                    continue;
                }
                return Some((String::from_utf8_lossy(&name).into_owned(), &s[i + 1..]));
            }
            name.push(s[i]);
            i += 1;
        }
        None
    } else {
        let end = s
            .iter()
            .position(|c| c.is_ascii_whitespace() || *c == b'(' || *c == b',' || *c == b'.')
            .unwrap_or(s.len());
        if end == 0 {
            return None;
        }
        Some((String::from_utf8_lossy(&s[..end]).into_owned(), &s[end..]))
    }
}

/// Reads a possibly schema-qualified identifier such as `public."odd.name"`.
///
/// The parts are kept apart rather than joined, so that a dot inside a quoted identifier
/// stays distinguishable from the schema separator. See [`TableName`].
fn take_qualified(s: &[u8]) -> Option<(TableName, &[u8])> {
    let (first, rest) = take_identifier(s)?;
    if rest.first() == Some(&b'.') {
        let (second, rest) = take_identifier(&rest[1..])?;
        Some((
            TableName {
                schema: Some(first),
                table: second,
            },
            rest,
        ))
    } else {
        Some((
            TableName {
                schema: None,
                table: first,
            },
            rest,
        ))
    }
}

/// Strips the `CREATE ... TABLE ` keyword, returning what follows.
///
/// pg_dump writes `CREATE UNLOGGED TABLE` for an unlogged table and dumps its rows
/// normally, so recognising only the plain form would leave the table undefined and fail
/// the load when its `COPY` block arrived.
fn strip_create_table(line: &[u8]) -> Option<&[u8]> {
    strip_prefix_ci(line, b"CREATE TABLE ")
        .or_else(|| strip_prefix_ci(line, b"CREATE UNLOGGED TABLE "))
}

/// Returns the index of the `)` matching the `(` at `open`, ignoring parens inside
/// double-quoted identifiers.
///
/// Taking the last `)` in the statement instead would swallow whatever pg_dump writes
/// after the column list: `PARTITION BY RANGE (...)`, `INHERITS (...)` and
/// `WITH (fillfactor=...)` all appear there.
fn matching_paren(s: &[u8], open: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut quoted = false;
    let mut i = open;
    while i < s.len() {
        let c = s[i];
        if quoted {
            if c == b'"' {
                if s.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                quoted = false;
            }
        } else {
            match c {
                b'"' => quoted = true,
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
                _ => {}
            }
        }
        i += 1;
    }
    None
}

/// Keywords that end a column's type text. Matched at paren depth zero only, so that
/// `numeric(10,2)` and `timestamp(3) without time zone` survive intact.
const TYPE_TERMINATORS: &[&[u8]] = &[
    b"DEFAULT",
    b"NOT",
    b"NULL",
    b"GENERATED",
    b"COLLATE",
    b"CONSTRAINT",
    b"CHECK",
    b"REFERENCES",
    b"PRIMARY",
    b"UNIQUE",
];

/// Keywords that mark a table-level constraint rather than a column definition.
const CONSTRAINT_LEADERS: &[&[u8]] = &[
    b"CONSTRAINT",
    b"PRIMARY",
    b"UNIQUE",
    b"FOREIGN",
    b"CHECK",
    b"EXCLUDE",
    b"LIKE",
];

/// Trims a column definition's tail down to just its type text.
fn take_type(s: &[u8]) -> String {
    let s = s.trim_ascii();
    let mut depth = 0i32;
    let mut i = 0usize;
    let mut cut = s.len();
    while i < s.len() {
        match s[i] {
            b'(' => depth += 1,
            b')' => depth -= 1,
            _ => {}
        }
        if depth == 0 && (i == 0 || s[i - 1].is_ascii_whitespace()) {
            let word_end = s[i..]
                .iter()
                .position(u8::is_ascii_whitespace)
                .map(|p| i + p)
                .unwrap_or(s.len());
            let word = &s[i..word_end];
            if TYPE_TERMINATORS
                .iter()
                .any(|k| word.eq_ignore_ascii_case(k))
            {
                cut = i;
                break;
            }
        }
        i += 1;
    }
    String::from_utf8_lossy(s[..cut].trim_ascii()).into_owned()
}

/// Parses a complete `CREATE TABLE` statement into a [`TableDef`].
///
/// Table-level constraints are recognised and skipped. Only column names and their type
/// text are recovered, which is all the Delta write path needs.
///
/// # Errors
///
/// [`Error::MalformedCreateTable`] if the statement has no parenthesised body or its
/// name cannot be read.
pub fn parse_create_table(ddl: &[u8]) -> Result<TableDef> {
    let after_kw =
        strip_create_table(ddl.trim_ascii_start()).ok_or_else(|| Error::MalformedCreateTable {
            table: String::new(),
        })?;
    let (name, rest) = take_qualified(after_kw).ok_or_else(|| Error::MalformedCreateTable {
        table: String::new(),
    })?;

    let open = rest
        .iter()
        .position(|c| *c == b'(')
        .ok_or_else(|| Error::MalformedCreateTable {
            table: name.qualified(),
        })?;
    let close = matching_paren(rest, open).ok_or_else(|| Error::MalformedCreateTable {
        table: name.qualified(),
    })?;

    let mut columns = Vec::new();
    for item in split_top_level(&rest[open + 1..close], b',') {
        let item = item.trim_ascii();
        if item.is_empty() {
            continue;
        }
        let first_word_end = item
            .iter()
            .position(u8::is_ascii_whitespace)
            .unwrap_or(item.len());
        if CONSTRAINT_LEADERS
            .iter()
            .any(|k| item[..first_word_end].eq_ignore_ascii_case(k))
        {
            continue;
        }
        let Some((col, tail)) = take_identifier(item) else {
            continue;
        };
        let sql_type = take_type(tail);
        if sql_type.is_empty() {
            continue;
        }
        columns.push(ColumnDef {
            name: col,
            sql_type,
        });
    }

    Ok(TableDef { name, columns })
}

/// Parses `COPY <table> (<cols>) FROM stdin;`, returning the table and its column list.
///
/// The column list is optional; an absent one yields an empty vector, meaning the table's
/// declaration order applies.
///
/// # Errors
///
/// [`Error::MalformedCopyHeader`] if the table name cannot be read. The reported offset
/// is zero, because this function sees one statement rather than the whole dump.
pub fn parse_copy_header(line: &[u8]) -> Result<(TableName, Vec<String>)> {
    let after_kw =
        strip_prefix_ci(line.trim_ascii_start(), b"COPY ").ok_or(Error::MalformedCopyHeader {
            offset: 0,
        })?;
    let (table, rest) = take_qualified(after_kw).ok_or(Error::MalformedCopyHeader { offset: 0 })?;

    let mut columns = Vec::new();
    let open = rest.iter().position(|c| *c == b'(');
    let close = open.and_then(|o| matching_paren(rest, o));
    if let (Some(open), Some(close)) = (open, close)
        && close > open
    {
        for item in split_top_level(&rest[open + 1..close], b',') {
            if let Some((name, _)) = take_identifier(item) {
                columns.push(name);
            }
        }
    }
    Ok((table, columns))
}

#[cfg(test)]
mod tests {
    use super::*;

    const PREAMBLE: &[u8] = b"-- Dumped from database version 17.2\n-- Dumped by pg_dump version 17.2\n";

    fn events_of<'a>(s: &mut Scanner, chunk: &'a [u8]) -> Vec<Event<'a>> {
        s.feed(chunk).unwrap()
    }

    /// A schema-qualified name, for comparing against what the scanner produced.
    fn qualified(schema: &str, table: &str) -> TableName {
        TableName {
            schema: Some(schema.to_string()),
            table: table.to_string(),
        }
    }

    #[test]
    fn reads_preamble_versions() {
        let mut s = Scanner::new();
        let ev = events_of(&mut s, PREAMBLE);
        assert_eq!(
            ev,
            vec![Event::DumpVersion {
                dumped_by: 17,
                from_database: Some(17)
            }]
        );
        assert_eq!(s.dumped_by(), Some(17));
    }

    #[test]
    fn unqualified_major_is_fatal() {
        let mut s = Scanner::new();
        let err = s
            .feed(b"-- Dumped by pg_dump version 18.0\n")
            .unwrap_err();
        assert_eq!(err, Error::UnsupportedDumpVersion { found: 18 });
    }

    #[test]
    fn parses_multiline_create_table() {
        let ddl = b"CREATE TABLE public.users (\n    id integer NOT NULL,\n    email character varying(255),\n    price numeric(10,2) DEFAULT 0,\n    made timestamp(3) without time zone,\n    total integer GENERATED ALWAYS AS (id * 2) STORED,\n    CONSTRAINT users_pkey PRIMARY KEY (id)\n);";
        let def = parse_create_table(ddl).unwrap();
        assert_eq!(def.name, qualified("public", "users"));
        let got: Vec<_> = def
            .columns
            .iter()
            .map(|c| (c.name.as_str(), c.sql_type.as_str()))
            .collect();
        assert_eq!(
            got,
            vec![
                ("id", "integer"),
                ("email", "character varying(255)"),
                ("price", "numeric(10,2)"),
                ("made", "timestamp(3) without time zone"),
                ("total", "integer"),
            ]
        );
    }

    #[test]
    fn quoted_identifiers_survive() {
        let ddl = br#"CREATE TABLE public."odd ""name" ("a,b" integer, "sel ect" text);"#;
        let def = parse_create_table(ddl).unwrap();
        assert_eq!(def.name, qualified("public", r#"odd "name"#));
        assert_eq!(def.columns[0].name, "a,b");
        assert_eq!(def.columns[1].name, "sel ect");
    }

    /// pg_dump quotes every identifier that is not plain lowercase ASCII, so a quoted
    /// name carrying UTF-8 is the normal case for any non-English schema. Decoding it
    /// byte-by-byte as Latin-1 would mangle it into mojibake, and for a table name that
    /// mojibake then fails the path guard and kills the whole load.
    #[test]
    fn quoted_identifiers_are_utf8_not_latin1() {
        let ddl = "CREATE TABLE public.\"Räksmörgås\" (\"belopp_öre\" bigint);".as_bytes();
        let def = parse_create_table(ddl).unwrap();
        assert_eq!(def.name, qualified("public", "Räksmörgås"));
        assert_eq!(def.columns[0].name, "belopp_öre");

        let (table, columns) =
            parse_copy_header("COPY public.\"Räksmörgås\" (\"belopp_öre\") FROM stdin;".as_bytes())
                .unwrap();
        assert_eq!(table, qualified("public", "Räksmörgås"));
        assert_eq!(columns, vec!["belopp_öre"]);
    }

    /// A dot is legal inside a quoted identifier, and must stay attached to the part it
    /// came from rather than being taken for the schema separator.
    #[test]
    fn a_dot_inside_a_quoted_identifier_stays_in_that_part() {
        let (table, _) = parse_copy_header(br#"COPY public."a.b" (x) FROM stdin;"#).unwrap();
        assert_eq!(table, qualified("public", "a.b"));
        assert_eq!(table.schema.as_deref(), Some("public"));
        assert_eq!(table.table, "a.b");
    }

    /// The column list ends at the paren matching the one that opened it. Taking the last
    /// paren in the statement instead swallows whatever pg_dump writes afterwards.
    #[test]
    fn trailing_clauses_do_not_swallow_the_column_list() {
        let cases: &[&[u8]] = &[
            b"CREATE TABLE public.t (
    a integer,
    b integer
)
WITH (fillfactor='70');",
            b"CREATE TABLE public.t (
    a integer,
    b integer
)
PARTITION BY RANGE (b);",
            b"CREATE TABLE public.t (
    a integer,
    b integer
)
INHERITS (public.parent);",
        ];
        for ddl in cases {
            let def = parse_create_table(ddl).unwrap();
            let got: Vec<_> = def
                .columns
                .iter()
                .map(|c| (c.name.as_str(), c.sql_type.as_str()))
                .collect();
            assert_eq!(
                got,
                vec![("a", "integer"), ("b", "integer")],
                "trailing clause leaked into the columns of {}",
                String::from_utf8_lossy(ddl)
            );
        }
    }

    /// pg_dump writes `CREATE UNLOGGED TABLE` for an unlogged table and dumps its rows
    /// normally, so missing the keyword leaves the table undefined when its COPY arrives.
    #[test]
    fn unlogged_tables_are_recognised() {
        let def =
            parse_create_table(b"CREATE UNLOGGED TABLE public.staging (id integer);").unwrap();
        assert_eq!(def.name, qualified("public", "staging"));
        assert_eq!(def.columns[0].name, "id");

        let mut s = Scanner::new();
        let mut dump = PREAMBLE.to_vec();
        dump.extend_from_slice(b"CREATE UNLOGGED TABLE public.staging (id integer);\n");
        dump.extend_from_slice(b"COPY public.staging (id) FROM stdin;\n1\n");
        dump.extend_from_slice(br"\.");
        dump.push(b'\n');
        let ev = s.feed(&dump).unwrap();
        s.finish().unwrap();
        assert!(
            ev.iter().any(|e| matches!(e, Event::Table(d) if d.name.table == "staging")),
            "no table definition was emitted for an unlogged table"
        );
    }

    #[test]
    fn parses_copy_header() {
        let (t, c) = parse_copy_header(b"COPY public.users (id, email) FROM stdin;").unwrap();
        assert_eq!(t, qualified("public", "users"));
        assert_eq!(c, vec!["id", "email"]);

        let (t, c) = parse_copy_header(b"COPY public.users FROM stdin;").unwrap();
        assert_eq!(t, qualified("public", "users"));
        assert!(c.is_empty());
    }

    #[test]
    fn full_block_in_one_chunk() {
        let mut s = Scanner::new();
        let mut dump = PREAMBLE.to_vec();
        dump.extend_from_slice(
            b"CREATE TABLE public.t (id integer);\nCOPY public.t (id) FROM stdin;\n1\n2\n\\.\n",
        );
        let ev = s.feed(&dump).unwrap();
        s.finish().unwrap();

        assert!(matches!(ev[1], Event::Table(_)));
        assert!(matches!(ev[2], Event::CopyStart { .. }));
        assert_eq!(ev[3], Event::CopyRows(Cow::Borrowed(b"1\n2\n")));
        assert_eq!(
            ev[4],
            Event::CopyEnd {
                table: qualified("public", "t")
            }
        );
    }

    #[test]
    fn empty_copy_block_emits_no_rows() {
        let mut s = Scanner::new();
        let mut dump = PREAMBLE.to_vec();
        dump.extend_from_slice(b"COPY public.t (id) FROM stdin;\n\\.\n");
        let ev = s.feed(&dump).unwrap();
        assert!(!ev.iter().any(|e| matches!(e, Event::CopyRows(_))));
        assert!(matches!(ev.last(), Some(Event::CopyEnd { .. })));
    }

    #[test]
    fn block_split_across_chunks() {
        let mut s = Scanner::new();
        s.feed(PREAMBLE).unwrap();
        s.feed(b"COPY public.t (id) FROM stdin;\n").unwrap();

        let ev = s.feed(b"1\n2\n").unwrap();
        assert_eq!(ev, vec![Event::CopyRows(Cow::Borrowed(b"1\n2\n"))]);

        let ev = s.feed(b"3\n\\.\n").unwrap();
        assert_eq!(ev[0], Event::CopyRows(Cow::Borrowed(b"3\n")));
        assert!(matches!(ev[1], Event::CopyEnd { .. }));
        s.finish().unwrap();
    }

    #[test]
    fn row_split_mid_line_is_rejoined() {
        let mut s = Scanner::new();
        s.feed(PREAMBLE).unwrap();
        s.feed(b"COPY public.t (a, b) FROM stdin;\n").unwrap();

        assert!(s.feed(b"left").unwrap().is_empty());
        let ev = s.feed(b"over\tright\n\\.\n").unwrap();
        assert_eq!(ev[0], Event::CopyRows(Cow::Owned(b"leftover\tright\n".to_vec())));
        assert!(matches!(ev[1], Event::CopyEnd { .. }));
    }

    #[test]
    fn truncated_dump_is_detected() {
        let mut s = Scanner::new();
        s.feed(PREAMBLE).unwrap();
        s.feed(b"COPY public.t (id) FROM stdin;\n1\n2\n").unwrap();
        assert_eq!(
            s.finish().unwrap_err(),
            Error::UnterminatedCopy {
                table: "public.t".into()
            }
        );
    }

    #[test]
    fn missing_version_is_detected() {
        let mut s = Scanner::new();
        s.feed(b"CREATE TABLE public.t (id integer);\n").unwrap();
        assert_eq!(s.finish().unwrap_err(), Error::MissingDumpVersion);
    }

    #[test]
    fn data_resembling_the_terminator_does_not_close_the_block() {
        let mut s = Scanner::new();
        s.feed(PREAMBLE).unwrap();
        s.feed(b"COPY public.t (a) FROM stdin;\n").unwrap();
        // A field whose text begins with a backslash-period, which PostgreSQL escapes.
        let ev = s.feed(b"\\\\.\n\\.\n").unwrap();
        assert_eq!(ev[0], Event::CopyRows(Cow::Borrowed(b"\\\\.\n")));
        assert!(matches!(ev[1], Event::CopyEnd { .. }));
    }

    #[test]
    fn scanner_returns_to_sql_after_a_block() {
        let mut s = Scanner::new();
        let mut dump = PREAMBLE.to_vec();
        dump.extend_from_slice(b"COPY public.a (id) FROM stdin;\n1\n\\.\n");
        dump.extend_from_slice(b"CREATE TABLE public.b (x text);\n");
        dump.extend_from_slice(b"COPY public.b (x) FROM stdin;\n\\.\n");
        let ev = s.feed(&dump).unwrap();
        s.finish().unwrap();
        let starts = ev
            .iter()
            .filter(|e| matches!(e, Event::CopyStart { .. }))
            .count();
        let ends = ev
            .iter()
            .filter(|e| matches!(e, Event::CopyEnd { .. }))
            .count();
        assert_eq!((starts, ends), (2, 2));
        assert_eq!(s.tables().len(), 1);
    }
}
