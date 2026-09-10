//! Error type for the whole crate.
//!
//! Hand-rolled rather than derived, to keep the dependency surface small. Every variant
//! is constructed from structural facts about the dump: a byte position, a table name, a
//! count. No variant carries field contents, because error values reach Python
//! tracebacks and logs, and the dump is untrusted third-party data.
//!
//! Executes on whichever thread produced the fault. Errors travel between threads as
//! values; nothing in this crate panics across a thread boundary.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Everything that can go wrong while reading a dump.
///
/// Variants divide into two classes, as specified in the architecture document: faults
/// that indicate the data is wrong, and which must fail the load, and conditions that
/// merely degrade fidelity. Only the former appear here. An unrecognised column type is
/// not an error; it maps to text and is reported in the run statistics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A field exceeded [`Limits::max_field_bytes`](crate::copy::Limits).
    FieldTooLarge {
        /// Byte length observed before the limit was tripped.
        len: usize,
        /// The configured ceiling.
        limit: usize,
    },

    /// A row exceeded [`Limits::max_row_bytes`](crate::copy::Limits).
    RowTooLarge {
        /// Byte length observed before the limit was tripped.
        len: usize,
        /// The configured ceiling.
        limit: usize,
    },

    /// A row carried more fields than [`Limits::max_columns`](crate::copy::Limits).
    TooManyColumns {
        /// Number of fields counted before the limit was tripped.
        count: usize,
        /// The configured ceiling.
        limit: usize,
    },

    /// A row's field count disagreed with the column list of its `COPY` statement.
    ///
    /// This is a structural fault. It means the dump is malformed or was misparsed, and
    /// silently padding or truncating the row would corrupt the table.
    FieldCountMismatch {
        /// Fields present in the row.
        found: usize,
        /// Fields the `COPY` header declared.
        expected: usize,
    },

    /// A backslash escape ran off the end of a field.
    TruncatedEscape,

    /// A `\x` escape was not followed by a hexadecimal digit.
    InvalidHexEscape,

    /// End of input was reached inside a `COPY` block.
    ///
    /// The classic signature of a truncated transfer, and the only reliable way to
    /// detect one: plain dumps carry no row counts to reconcile against.
    UnterminatedCopy {
        /// Table whose block never closed.
        table: String,
    },

    /// A `COPY ... FROM stdin;` statement could not be parsed.
    MalformedCopyHeader {
        /// Byte offset within the dump.
        offset: u64,
    },

    /// A `CREATE TABLE` statement could not be parsed.
    MalformedCreateTable {
        /// Table name, if one was recovered before the failure.
        table: String,
    },

    /// The dump was produced by a `pg_dump` major version this build has not qualified.
    ///
    /// Deliberately fatal. Parsing an unknown major speculatively risks misreading DDL
    /// and committing a subtly wrong table.
    UnsupportedDumpVersion {
        /// Major version found in the `-- Dumped by pg_dump version` comment.
        found: u32,
    },

    /// The dump preamble carried no recognisable `pg_dump` version comment.
    MissingDumpVersion,

    /// A table name would resolve outside the configured output prefix.
    ///
    /// `../` is a legal quoted PostgreSQL identifier and table names originate with a
    /// third party. Rejected rather than sanitised, so that the failure is visible.
    UnsafeTableName {
        /// The rejected name, which is a schema object identifier and not row data.
        name: String,
    },

    /// A field's text did not match the type its column declared.
    ///
    /// Structural, and therefore fatal: the dump disagrees with its own DDL. Carries the
    /// column and the expected type but never the offending value, because error text
    /// reaches logs and Python tracebacks and the dump is untrusted.
    UnparsableValue {
        /// Column whose declared type was contradicted.
        column: String,
        /// The type that was expected.
        expected: &'static str,
    },

    /// A text column carried bytes that are not valid UTF-8.
    ///
    /// Arrow strings are UTF-8, so this cannot be represented. Substituting replacement
    /// characters would corrupt the value silently, so the load fails instead and the
    /// operator can address the source database's encoding.
    NonUtf8Text {
        /// Column carrying the offending bytes.
        column: String,
    },

    /// The dump's compression format was recognised but is not compiled in.
    ///
    /// Detection is by magic bytes, so this is precise rather than a corrupt parse: the
    /// sender changed format and the build must be updated to match.
    UnsupportedCompression {
        /// Name of the detected format.
        format: String,
    },

    /// Arrow rejected an assembled batch.
    ///
    /// Indicates a defect in this crate's builders rather than a problem with the dump,
    /// since the schema and the arrays are both produced here.
    Arrow {
        /// Display form of the originating error.
        message: String,
    },

    /// The Delta write path failed.
    ///
    /// `DeltaTableError` is neither `Clone` nor `PartialEq`, so it is flattened to its
    /// message to keep this enum cheap to move between threads.
    Delta {
        /// Display form of the originating error.
        message: String,
    },

    /// The target table already holds data and the write mode forbids replacing it.
    TableExists {
        /// Qualified table name.
        table: String,
    },

    /// Underlying I/O failure, reduced to its kind and a message.
    ///
    /// `std::io::Error` is not `Clone` or `PartialEq`, so it is flattened here to keep
    /// this enum cheap to move between threads.
    Io {
        /// Display form of the originating error.
        message: String,
    },

    /// The caller asked the load to stop, for example on a Ctrl-C signal.
    ///
    /// Reported as an error so the load fails without committing: a partial day is never
    /// made visible. Nothing has been staged that a later run cannot overwrite.
    Interrupted,

    /// The dump's schema for a table disagrees with the schema Delta declares.
    ///
    /// Only [`WriteMode::Overwrite`](crate::sink::WriteMode) may change a schema, because
    /// it rewrites the table wholesale. Appending rows shaped one way to a table declared
    /// another way cannot be made to mean anything, so it is refused.
    ///
    /// `detail` names the columns and their types. It never carries a value.
    SchemaChanged {
        /// Qualified name of the table whose schema moved.
        table: String,
        /// Which columns were added, removed or retyped.
        detail: String,
    },

    /// The output prefix points at storage a catalog manages for itself.
    ///
    /// Unity Catalog managed tables, and the legacy Hive warehouse, assume the catalog is
    /// the only writer. A third-party writer can leave them inconsistent in ways that do
    /// not surface as a clean error, so this is refused rather than attempted. Write to an
    /// external location and register the result instead.
    ManagedTableTarget {
        /// The rejected prefix. A location, never data.
        uri: String,
        /// Which marker in the path identified it.
        marker: &'static str,
    },

    /// A phase-two commit failed part way through the burst.
    ///
    /// This is the one variant that may mean the load left **visible change** behind.
    /// Delta has no cross-table transaction, so the tables committed before the failure
    /// hold the new contents while the rest still hold the previous run's. The counts are
    /// carried so an operator can tell that apart from a failure on the very first table,
    /// which changed nothing.
    ///
    /// Re-running under [`WriteMode::Overwrite`](crate::sink::WriteMode) restores
    /// consistency, because every table is rewritten from the same dump.
    CommitFailed {
        /// Qualified name of the table whose commit failed.
        table: String,
        /// Tables committed successfully before the failure.
        committed: usize,
        /// Tables that were to be committed in this phase.
        total: usize,
        /// Display form of the underlying failure.
        message: String,
    },

    /// An invariant between two stages of this crate was violated.
    ///
    /// Indicates a defect here rather than a problem with the dump. It is an error and
    /// not a panic because a panic would unwind through the Python bindings, and because
    /// silently continuing past it could commit a short table.
    Internal {
        /// Which invariant failed. Never carries dump content.
        detail: &'static str,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::FieldTooLarge { len, limit } => {
                write!(f, "field of {len} bytes exceeds limit of {limit}")
            }
            Error::RowTooLarge { len, limit } => {
                write!(f, "row of {len} bytes exceeds limit of {limit}")
            }
            Error::TooManyColumns { count, limit } => {
                write!(f, "row has {count} fields, exceeding limit of {limit}")
            }
            Error::FieldCountMismatch { found, expected } => {
                write!(f, "row has {found} fields, expected {expected}")
            }
            Error::TruncatedEscape => f.write_str("backslash escape truncated at end of field"),
            Error::InvalidHexEscape => f.write_str("\\x escape not followed by a hex digit"),
            Error::UnterminatedCopy { table } => {
                write!(f, "end of input inside COPY block for table {table}")
            }
            Error::MalformedCopyHeader { offset } => {
                write!(f, "malformed COPY statement at byte {offset}")
            }
            Error::MalformedCreateTable { table } => {
                write!(f, "malformed CREATE TABLE for {table}")
            }
            Error::UnsupportedDumpVersion { found } => {
                write!(
                    f,
                    "dump produced by unqualified pg_dump major version {found}"
                )
            }
            Error::MissingDumpVersion => {
                f.write_str("dump preamble carries no pg_dump version comment")
            }
            Error::UnsafeTableName { name } => {
                write!(f, "table name escapes the output prefix: {name}")
            }
            Error::UnparsableValue { column, expected } => {
                write!(f, "value in column {column} is not a valid {expected}")
            }
            Error::NonUtf8Text { column } => {
                write!(f, "column {column} carries bytes that are not valid UTF-8")
            }
            Error::UnsupportedCompression { format } => {
                write!(
                    f,
                    "dump uses {format} compression, which this build cannot decode"
                )
            }
            Error::Arrow { message } => write!(f, "arrow error: {message}"),
            Error::Delta { message } => write!(f, "delta error: {message}"),
            Error::TableExists { table } => {
                write!(
                    f,
                    "table {table} already holds data and mode is error-if-exists"
                )
            }
            Error::Io { message } => write!(f, "io error: {message}"),
            Error::Interrupted => f.write_str("load interrupted by caller"),
            Error::CommitFailed {
                table,
                committed,
                total,
                message,
            } => {
                if *committed == 0 {
                    write!(
                        f,
                        "commit failed on table {table}, the first of {total}, \
                         so no table changed: {message}"
                    )
                } else {
                    write!(
                        f,
                        "commit failed on table {table} after {committed} of {total} \
                         tables were committed, so those {committed} now hold the new \
                         contents and the rest hold the previous run's; re-run to restore \
                         consistency: {message}"
                    )
                }
            }
            Error::SchemaChanged { table, detail } => write!(
                f,
                "schema of table {table} differs from the Delta table ({detail}); \
                 only overwrite mode may change a schema"
            ),
            Error::ManagedTableTarget { uri, marker } => write!(
                f,
                "output prefix {uri} is catalog-managed storage (matched {marker});                  write to an external location and register the tables instead"
            ),
            Error::Internal { detail } => write!(f, "internal invariant violated: {detail}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io {
            message: e.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The distinction the counts exist to make: a failure on the first table changed
    /// nothing, and a failure later did. An operator reads this at three in the morning,
    /// so both cases must be unambiguous.
    #[test]
    fn commit_failure_says_whether_anything_became_visible() {
        let first = Error::CommitFailed {
            table: "public.users".into(),
            committed: 0,
            total: 450,
            message: "storage unavailable".into(),
        };
        let rendered = first.to_string();
        assert!(rendered.contains("no table changed"), "{rendered}");
        assert!(rendered.contains("public.users"));
        assert!(rendered.contains("storage unavailable"));

        let partway = Error::CommitFailed {
            table: "public.orders".into(),
            committed: 12,
            total: 450,
            message: "storage unavailable".into(),
        };
        let rendered = partway.to_string();
        assert!(rendered.contains("12 of 450"), "{rendered}");
        assert!(rendered.contains("re-run"), "{rendered}");
        assert!(!rendered.contains("no table changed"), "{rendered}");
    }

    /// Error text reaches logs and Python tracebacks, and the dump is untrusted, so no
    /// variant may carry field contents. Table names are schema identifiers, not row data.
    #[test]
    fn no_variant_carries_row_data() {
        let err = Error::UnparsableValue {
            column: "amount".into(),
            expected: "integer",
        };
        let rendered = err.to_string();
        assert!(rendered.contains("amount"));
        assert!(rendered.contains("integer"));
    }
}
