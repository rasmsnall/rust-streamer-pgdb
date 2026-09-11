//! Binding resolved PostgreSQL types to Arrow arrays.
//!
//! Executes on the decode thread pool, immediately after [`crate::copy`] has produced a
//! row's fields. Nothing here is async.
//!
//! This is the only module that names Arrow. It uses the `deltalake::arrow` re-export
//! rather than a direct `arrow` dependency, so the arrow version can never skew from the
//! one delta-rs pins.
//!
//! [`crate::types`] decides *what* a column is and [`crate::values`] decides how to read
//! one value; this module owns the Arrow builders and the batch bounds.
//!
//! # Nullability
//!
//! Every field is declared nullable. `NOT NULL` is recoverable from the DDL but is not
//! relied upon: a column declared `NOT NULL` in PostgreSQL that nonetheless produced a
//! `\N` would otherwise fail at write time, turning a source-data oddity into a failed
//! load. Delta gains nothing from the tighter declaration here.

use std::sync::Arc;

use deltalake::arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, Float32Builder,
    Float64Builder, Int16Builder, Int32Builder, Int64Builder, ListArray, RecordBatch,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use deltalake::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use deltalake::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

use crate::error::{Error, Result};
use crate::types::{self, PgType, ResolvedType};
use crate::values;

/// The timezone stamped on every `timestamp` column, with or without a source time zone.
///
/// Delta's `timestamp` is microseconds UTC. Arrow's own convention is that a `Timestamp`
/// field with **no** timezone is *naive*, and delta-rs maps that straight to Delta's
/// `timestamp_ntz`, which needs reader v3 / writer v7 and breaks the compatibility floor
/// this library targets (see `docs/architecture.md`, Chapter X). A naive PostgreSQL
/// `timestamp without time zone` is assumed to already be UTC (documented on the Python
/// surface), so stamping it with this timezone, exactly like `timestamptz`, is what makes
/// it land as Delta's plain `timestamp` instead. See [`crate::values::parse_timestamp`]:
/// the value on the wire is already computed as UTC micros either way, so this is a
/// schema label, not a value transform.
const UTC: &str = "UTC";

/// Returns the Arrow type a resolved column maps to.
///
/// Columns that [`ResolvedType::is_textual`] identifies, which covers arrays (unless a
/// native mapping is found under `native_arrays`) and everything unrecognised, map to
/// `Utf8` regardless of their nominal type. A too-wide `numeric`
/// (see [`types::numeric_too_wide`]) also identifies as textual, but maps instead to
/// `decimal(38,18)` when `wide_numeric_as_decimal` opts into it.
pub fn arrow_type(
    rt: &ResolvedType,
    wide_numeric_as_decimal: bool,
    native_arrays: bool,
) -> DataType {
    if rt.is_array {
        if native_arrays && rt.array_dimensions == 1 {
            let element_rt = as_element(rt);
            if let Some(element_type) = array_element_type(&element_rt, wide_numeric_as_decimal) {
                return DataType::List(Arc::new(Field::new("item", element_type, true)));
            }
        }
        return DataType::Utf8;
    }
    if let PgType::Numeric { precision, .. } = rt.pg
        && wide_numeric_as_decimal
        && types::numeric_too_wide(precision)
    {
        return DataType::Decimal128(types::WIDE_NUMERIC_PRECISION, types::WIDE_NUMERIC_SCALE);
    }
    if rt.is_textual() {
        return DataType::Utf8;
    }
    match rt.pg {
        PgType::SmallInt => DataType::Int16,
        PgType::Integer => DataType::Int32,
        PgType::BigInt => DataType::Int64,
        PgType::Real => DataType::Float32,
        PgType::DoublePrecision => DataType::Float64,
        PgType::Numeric { precision, scale } => {
            // is_textual has already rejected anything that does not fit.
            DataType::Decimal128(precision.unwrap_or(38), scale.unwrap_or(0))
        }
        PgType::Boolean => DataType::Boolean,
        PgType::Date => DataType::Date32,
        // Both variants carry the same Arrow type: a naive `timestamp` is assumed UTC
        // (see the UTC constant's doc), and a bare Arrow `Timestamp` with no timezone at
        // all would map to Delta's timestamp_ntz instead of timestamp.
        PgType::Timestamp { .. } => DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
        PgType::Time { .. } => DataType::Time64(TimeUnit::Microsecond),
        PgType::Bytea => DataType::Binary,
        PgType::Text => DataType::Utf8,
    }
}

/// `rt`, treated as its own element type: `is_array` cleared and `array_dimensions` zeroed.
///
/// Used to resolve a one-dimensional array's element type by recursing into the same
/// scalar logic ([`arrow_type`], [`ColumnBuilder::new`]) a plain column of that type
/// would use, rather than duplicating it.
fn as_element(rt: &ResolvedType) -> ResolvedType {
    ResolvedType {
        is_array: false,
        array_dimensions: 0,
        ..rt.clone()
    }
}

/// Returns the Arrow type a one-dimensional array's element maps to, or `None` if the
/// element type is not one `native_arrays` supports, in which case the whole array column
/// falls back to text (see [`arrow_type`]).
///
/// Unsupported means: `rt.pg` is `Text` and [`ResolvedType::recognised`] is false (an
/// enum, domain, composite, or anything else this library could not identify), or `rt.pg`
/// is `Numeric` with a precision or scale that does not fit natively and either
/// `wide_numeric_as_decimal` is off or the shape is the narrower "scale Arrow cannot
/// represent" problem `wide_numeric_as_decimal` does not cover (see
/// [`types::numeric_too_wide`]). Every other `PgType` always has a concrete Arrow mapping.
///
/// `rt` must already be de-arrayed, as [`as_element`] produces.
fn array_element_type(rt: &ResolvedType, wide_numeric_as_decimal: bool) -> Option<DataType> {
    debug_assert!(!rt.is_array);
    let unsupported = match rt.pg {
        PgType::Text => !rt.recognised,
        PgType::Numeric { precision, .. } => {
            rt.is_textual() && !(wide_numeric_as_decimal && types::numeric_too_wide(precision))
        }
        _ => false,
    };
    if unsupported {
        None
    } else {
        Some(arrow_type(rt, wide_numeric_as_decimal, false))
    }
}

/// Builds the Arrow schema for a table's columns.
pub fn arrow_schema(
    columns: &[(String, ResolvedType)],
    wide_numeric_as_decimal: bool,
    native_arrays: bool,
) -> Schema {
    Schema::new(
        columns
            .iter()
            .map(|(name, rt)| {
                Field::new(
                    name,
                    arrow_type(rt, wide_numeric_as_decimal, native_arrays),
                    true,
                )
            })
            .collect::<Vec<_>>(),
    )
}

/// A typed Arrow array builder for one column.
///
/// Constructed from a [`ResolvedType`] and fed unescaped field bytes. Reused across
/// batches: Arrow's `finish` resets the builder rather than consuming it.
#[derive(Debug)]
pub enum ColumnBuilder {
    /// `smallint`.
    Int16(Int16Builder),
    /// `integer`.
    Int32(Int32Builder),
    /// `bigint`.
    Int64(Int64Builder),
    /// `real`.
    Float32(Float32Builder),
    /// `double precision`.
    Float64(Float64Builder),
    /// Fixed-precision `numeric`, carrying the precision and scale used to align and
    /// validate incoming values.
    Decimal(Decimal128Builder, u8, i8),
    /// `boolean`.
    Boolean(BooleanBuilder),
    /// `date`.
    Date(Date32Builder),
    /// `timestamp`, with a flag for whether the source carries an offset.
    Timestamp(TimestampMicrosecondBuilder, bool),
    /// `time`.
    Time(Time64MicrosecondBuilder),
    /// `bytea`.
    Binary(BinaryBuilder),
    /// A one-dimensional array, native to Arrow under `native_arrays`. `element` is any
    /// non-array variant, reused to parse, validate and store each array's elements
    /// exactly as it would that same type's own scalar column. `offsets` and `validity`
    /// are this list's own bookkeeping: one more `offsets` entry than rows, and one
    /// `validity` entry per row.
    List {
        /// Builder for the flattened element values, of any non-array variant.
        element: Box<ColumnBuilder>,
        /// Cumulative element count after each row; always starts as `[0]`.
        offsets: Vec<i32>,
        /// Whether each row's array itself (not its elements) is non-null.
        validity: Vec<bool>,
    },
    /// Text, and everything that degrades to it.
    Utf8(StringBuilder),
}

impl ColumnBuilder {
    /// Creates a builder matching `rt`.
    ///
    /// The builder must agree with [`arrow_type`], because the schema is built from the
    /// latter and [`BatchBuilder::finish`] would otherwise fail: both check a
    /// one-dimensional array against `native_arrays` and a too-wide `numeric` against
    /// `wide_numeric_as_decimal` first, then fall back to [`ResolvedType::is_textual`],
    /// which routes anything else Arrow cannot represent (a multi-dimensional array,
    /// anything unrecognised) to text.
    ///
    /// # Panics
    ///
    /// Does not panic. The decimal fallbacks below are unreachable defence: if either
    /// ever did fire, the mismatch with [`arrow_type`] would surface as [`Error::Arrow`]
    /// on the next flush rather than as a panic here.
    pub fn new(rt: &ResolvedType, wide_numeric_as_decimal: bool, native_arrays: bool) -> Self {
        if rt.is_array {
            if native_arrays && rt.array_dimensions == 1 {
                let element_rt = as_element(rt);
                if array_element_type(&element_rt, wide_numeric_as_decimal).is_some() {
                    let element = Box::new(ColumnBuilder::new(
                        &element_rt,
                        wide_numeric_as_decimal,
                        native_arrays,
                    ));
                    return ColumnBuilder::List {
                        element,
                        offsets: vec![0],
                        validity: Vec::new(),
                    };
                }
            }
            return ColumnBuilder::Utf8(StringBuilder::new());
        }
        if let PgType::Numeric { precision, .. } = rt.pg
            && wide_numeric_as_decimal
            && types::numeric_too_wide(precision)
        {
            let p = types::WIDE_NUMERIC_PRECISION;
            let s = types::WIDE_NUMERIC_SCALE;
            return match Decimal128Builder::new().with_precision_and_scale(p, s) {
                Ok(b) => ColumnBuilder::Decimal(b, p, s),
                Err(_) => ColumnBuilder::Utf8(StringBuilder::new()),
            };
        }
        if rt.is_textual() {
            return ColumnBuilder::Utf8(StringBuilder::new());
        }
        match rt.pg {
            PgType::SmallInt => ColumnBuilder::Int16(Int16Builder::new()),
            PgType::Integer => ColumnBuilder::Int32(Int32Builder::new()),
            PgType::BigInt => ColumnBuilder::Int64(Int64Builder::new()),
            PgType::Real => ColumnBuilder::Float32(Float32Builder::new()),
            PgType::DoublePrecision => ColumnBuilder::Float64(Float64Builder::new()),
            PgType::Numeric { precision, scale } => {
                let p = precision.unwrap_or(38);
                let s = scale.unwrap_or(0);
                match Decimal128Builder::new().with_precision_and_scale(p, s) {
                    Ok(b) => ColumnBuilder::Decimal(b, p, s),
                    Err(_) => ColumnBuilder::Utf8(StringBuilder::new()),
                }
            }
            PgType::Boolean => ColumnBuilder::Boolean(BooleanBuilder::new()),
            PgType::Date => ColumnBuilder::Date(Date32Builder::new()),
            PgType::Timestamp { tz } => {
                ColumnBuilder::Timestamp(TimestampMicrosecondBuilder::new(), tz)
            }
            PgType::Time { .. } => ColumnBuilder::Time(Time64MicrosecondBuilder::new()),
            PgType::Bytea => ColumnBuilder::Binary(BinaryBuilder::new()),
            PgType::Text => ColumnBuilder::Utf8(StringBuilder::new()),
        }
    }

    /// Appends one value, or NULL when `value` is `None`.
    ///
    /// Returns `true` if a non-null input was stored as NULL because its type could not
    /// represent it, which happens for `infinity` timestamps and `NaN` numerics. The
    /// caller counts these for the run statistics so the substitution is visible.
    ///
    /// # Errors
    ///
    /// [`Error::UnparsableValue`] if the text contradicts the column's declared type,
    /// and [`Error::NonUtf8Text`] for a text column carrying non-UTF-8 bytes.
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn append(&mut self, value: Option<&[u8]>, column: &str) -> Result<bool> {
        let Some(v) = value else {
            self.append_null();
            return Ok(false);
        };
        let mut substituted = false;
        match self {
            ColumnBuilder::Int16(b) => b.append_option(values::parse_i16(v, column)?),
            ColumnBuilder::Int32(b) => b.append_option(values::parse_i32(v, column)?),
            ColumnBuilder::Int64(b) => b.append_option(values::parse_i64(v, column)?),
            ColumnBuilder::Float32(b) => b.append_option(values::parse_f32(v, column)?),
            ColumnBuilder::Float64(b) => b.append_option(values::parse_f64(v, column)?),
            ColumnBuilder::Decimal(b, precision, scale) => {
                let parsed = values::parse_decimal(v, *precision, *scale, column)?;
                substituted = parsed.is_none();
                b.append_option(parsed);
            }
            ColumnBuilder::Boolean(b) => b.append_option(values::parse_bool(v, column)?),
            ColumnBuilder::Date(b) => {
                let parsed = values::parse_date(v, column)?;
                substituted = parsed.is_none();
                b.append_option(parsed);
            }
            ColumnBuilder::Timestamp(b, tz) => {
                let parsed = values::parse_timestamp(v, *tz, column)?;
                substituted = parsed.is_none();
                b.append_option(parsed);
            }
            ColumnBuilder::Time(b) => b.append_option(values::parse_time(v, column)?),
            ColumnBuilder::Binary(b) => {
                // Decoded into a scratch buffer so the builder receives one contiguous
                // value rather than being appended to byte by byte.
                let mut buf = Vec::with_capacity(v.len() / 2);
                values::parse_bytea(v, column, &mut buf)?;
                b.append_value(&buf);
            }
            ColumnBuilder::List {
                element,
                offsets,
                validity,
            } => {
                // Every element goes through the same parser and validation its own
                // scalar column would use, including the unquoted `NULL` token landing
                // as a genuine null element rather than a substitution: see
                // values::parse_array_elements.
                let elements = values::parse_array_elements(v, column)?;
                for elem in &elements {
                    substituted |= element.append(elem.as_deref(), column)?;
                }
                let overflow = || Error::UnparsableValue {
                    column: column.to_string(),
                    expected: "array",
                };
                let count = i32::try_from(elements.len()).map_err(|_| overflow())?;
                // `offsets` always holds at least one entry (see `ColumnBuilder::new`).
                let last = *offsets.last().expect("offsets is never empty");
                offsets.push(last.checked_add(count).ok_or_else(overflow)?);
                validity.push(true);
            }
            ColumnBuilder::Utf8(b) => {
                let s = std::str::from_utf8(v).map_err(|_| Error::NonUtf8Text {
                    column: column.to_string(),
                })?;
                b.append_value(s);
            }
        }
        Ok(substituted)
    }

    /// Appends a NULL.
    pub fn append_null(&mut self) {
        match self {
            ColumnBuilder::Int16(b) => b.append_null(),
            ColumnBuilder::Int32(b) => b.append_null(),
            ColumnBuilder::Int64(b) => b.append_null(),
            ColumnBuilder::Float32(b) => b.append_null(),
            ColumnBuilder::Float64(b) => b.append_null(),
            ColumnBuilder::Decimal(b, ..) => b.append_null(),
            ColumnBuilder::Boolean(b) => b.append_null(),
            ColumnBuilder::Date(b) => b.append_null(),
            ColumnBuilder::Timestamp(b, _) => b.append_null(),
            ColumnBuilder::Time(b) => b.append_null(),
            ColumnBuilder::Binary(b) => b.append_null(),
            ColumnBuilder::List {
                offsets, validity, ..
            } => {
                // A null array contributes no elements: the offset simply repeats.
                let last = *offsets.last().expect("offsets is never empty");
                offsets.push(last);
                validity.push(false);
            }
            ColumnBuilder::Utf8(b) => b.append_null(),
        }
    }

    /// Finishes the current batch and resets the builder for reuse.
    ///
    /// # Panics
    ///
    /// Does not panic, except for an unreachable internal-invariant violation in the
    /// `List` case: see the comment at its `ListArray::try_new` call.
    pub fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Int16(b) => Arc::new(b.finish()),
            ColumnBuilder::Int32(b) => Arc::new(b.finish()),
            ColumnBuilder::Int64(b) => Arc::new(b.finish()),
            ColumnBuilder::Float32(b) => Arc::new(b.finish()),
            ColumnBuilder::Float64(b) => Arc::new(b.finish()),
            ColumnBuilder::Decimal(b, ..) => Arc::new(b.finish()),
            ColumnBuilder::Boolean(b) => Arc::new(b.finish()),
            ColumnBuilder::Date(b) => Arc::new(b.finish()),
            ColumnBuilder::Timestamp(b, _) => {
                // Stamped regardless of the source's own tz-ness: the schema declares
                // this timezone for both (see the UTC constant's doc), and the array's
                // type must agree with the schema's or batch construction fails.
                Arc::new(b.finish().with_timezone(UTC))
            }
            ColumnBuilder::Time(b) => Arc::new(b.finish()),
            ColumnBuilder::Binary(b) => Arc::new(b.finish()),
            ColumnBuilder::List {
                element,
                offsets,
                validity,
            } => {
                let values = element.finish();
                // The field's type is taken from the values array actually produced,
                // rather than recomputed, so the two cannot disagree.
                let field = Arc::new(Field::new("item", values.data_type().clone(), true));
                let offsets =
                    OffsetBuffer::new(ScalarBuffer::from(std::mem::replace(offsets, vec![0])));
                let nulls = NullBuffer::from(std::mem::take(validity));
                // `offsets`, `values` and `nulls` are this builder's own bookkeeping,
                // kept in lockstep by `append`/`append_null` above, so the only way
                // `try_new` rejects them is a bug in that bookkeeping. A bug there must
                // not silently commit a mismatched or short array, which is why this
                // panics rather than falling back to something plausible-looking.
                Arc::new(
                    ListArray::try_new(field, offsets, values, Some(nulls))
                        .expect("list builder offsets/values/nulls must already agree"),
                )
            }
            ColumnBuilder::Utf8(b) => Arc::new(b.finish()),
        }
    }
}

/// Accumulates rows into an Arrow [`RecordBatch`], bounded by row count and byte size.
///
/// `max_bytes` bounds the in-memory batch, not the output file. Several batches
/// accumulate into each Parquet file downstream, because one small file per batch across
/// hundreds of tables produces small-file sprawl that degrades every query.
#[derive(Debug)]
pub struct BatchBuilder {
    schema: Arc<Schema>,
    names: Vec<String>,
    columns: Vec<ColumnBuilder>,
    rows: usize,
    bytes: usize,
    max_rows: usize,
    max_bytes: usize,
    /// Per-column count of values stored as NULL because the type could not hold them.
    substitutions: Vec<u64>,
}

impl BatchBuilder {
    /// Creates a builder for `columns`, bounded by `max_rows` and `max_bytes`.
    pub fn new(
        columns: &[(String, ResolvedType)],
        max_rows: usize,
        max_bytes: usize,
        wide_numeric_as_decimal: bool,
        native_arrays: bool,
    ) -> Self {
        Self {
            schema: Arc::new(arrow_schema(
                columns,
                wide_numeric_as_decimal,
                native_arrays,
            )),
            names: columns.iter().map(|(n, _)| n.clone()).collect(),
            columns: columns
                .iter()
                .map(|(_, rt)| ColumnBuilder::new(rt, wide_numeric_as_decimal, native_arrays))
                .collect(),
            rows: 0,
            bytes: 0,
            max_rows,
            max_bytes,
            substitutions: vec![0; columns.len()],
        }
    }

    /// The Arrow schema of the batches this builder produces.
    pub fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Rows accumulated since the last [`BatchBuilder::finish`].
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// True when either bound has been reached and the batch should be flushed.
    pub fn is_full(&self) -> bool {
        self.rows >= self.max_rows || self.bytes >= self.max_bytes
    }

    /// Per-column count of values stored as NULL because the type could not hold them.
    ///
    /// Accumulates across batches for the lifetime of the builder, and is reported in the
    /// run statistics so that `infinity` and `NaN` substitutions are visible.
    pub fn substitutions(&self) -> &[u64] {
        &self.substitutions
    }

    /// Appends one row.
    ///
    /// # Errors
    ///
    /// [`Error::FieldCountMismatch`] if the row's arity disagrees with the schema, plus
    /// any error from [`ColumnBuilder::append`].
    ///
    /// # Panics
    ///
    /// Does not panic.
    pub fn append_row(&mut self, values: &[Option<&[u8]>]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(Error::FieldCountMismatch {
                found: values.len(),
                expected: self.columns.len(),
            });
        }
        for (i, value) in values.iter().enumerate() {
            if self.columns[i].append(*value, &self.names[i])? {
                self.substitutions[i] += 1;
            }
            self.bytes += value.map_or(0, |v| v.len());
        }
        self.rows += 1;
        Ok(())
    }

    /// Appends one row whose fields are ranges into a shared `scratch` buffer.
    ///
    /// This is the form the decode hot loop uses: [`crate::copy::decode_row`] writes a
    /// row's unescaped bytes into a reused buffer and reports each field as `None` for
    /// NULL or a `Range<usize>` into that buffer, and this method binds them to the Arrow
    /// columns without the intermediate `Vec<Option<&[u8]>>` that [`BatchBuilder::append_row`]
    /// would require per row.
    ///
    /// # Errors
    ///
    /// [`Error::FieldCountMismatch`] if `ranges.len()` disagrees with the schema, plus any
    /// error from [`ColumnBuilder::append`].
    ///
    /// # Panics
    ///
    /// Panics if a range falls outside `scratch`, which cannot happen for ranges produced
    /// by [`crate::copy::decode_row`] against the same buffer.
    pub fn append_row_ranges(
        &mut self,
        scratch: &[u8],
        ranges: &[Option<std::ops::Range<usize>>],
    ) -> Result<()> {
        if ranges.len() != self.columns.len() {
            return Err(Error::FieldCountMismatch {
                found: ranges.len(),
                expected: self.columns.len(),
            });
        }
        for (i, range) in ranges.iter().enumerate() {
            let value = range.clone().map(|r| &scratch[r]);
            if self.columns[i].append(value, &self.names[i])? {
                self.substitutions[i] += 1;
            }
            self.bytes += range.as_ref().map_or(0, |r| r.len());
        }
        self.rows += 1;
        Ok(())
    }

    /// Produces a batch from the accumulated rows and resets for reuse.
    ///
    /// Returns `None` when no rows are pending, so a flush at end of block is harmless.
    ///
    /// # Errors
    ///
    /// [`Error::Arrow`] if the assembled arrays do not satisfy the schema, which would
    /// indicate a defect in this module rather than in the dump.
    pub fn finish(&mut self) -> Result<Option<RecordBatch>> {
        if self.rows == 0 {
            return Ok(None);
        }
        let arrays: Vec<ArrayRef> = self.columns.iter_mut().map(|c| c.finish()).collect();
        self.rows = 0;
        self.bytes = 0;
        RecordBatch::try_new(Arc::clone(&self.schema), arrays)
            .map(Some)
            .map_err(|e| Error::Arrow {
                message: e.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::resolve;
    use deltalake::arrow::array::{
        Array, BinaryArray, Date32Array, Decimal128Array, Int32Array, StringArray,
        TimestampMicrosecondArray,
    };

    fn cols(specs: &[(&str, &str)]) -> Vec<(String, ResolvedType)> {
        specs
            .iter()
            .map(|(n, t)| (n.to_string(), resolve(t)))
            .collect()
    }

    #[test]
    fn types_map_as_documented() {
        assert_eq!(
            arrow_type(&resolve("smallint"), false, false),
            DataType::Int16
        );
        assert_eq!(
            arrow_type(&resolve("integer"), false, false),
            DataType::Int32
        );
        assert_eq!(
            arrow_type(&resolve("bigint"), false, false),
            DataType::Int64
        );
        assert_eq!(
            arrow_type(&resolve("real"), false, false),
            DataType::Float32
        );
        assert_eq!(
            arrow_type(&resolve("double precision"), false, false),
            DataType::Float64
        );
        assert_eq!(
            arrow_type(&resolve("boolean"), false, false),
            DataType::Boolean
        );
        assert_eq!(arrow_type(&resolve("date"), false, false), DataType::Date32);
        assert_eq!(
            arrow_type(&resolve("bytea"), false, false),
            DataType::Binary
        );
        assert_eq!(
            arrow_type(&resolve("numeric(10,2)"), false, false),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            arrow_type(&resolve("timestamp without time zone"), false, false),
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into())),
            "a naive timestamp must carry a timezone in its Arrow type too, or delta-rs \
             maps it to timestamp_ntz instead of timestamp"
        );
        assert_eq!(
            arrow_type(&resolve("timestamp with time zone"), false, false),
            DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        );
        assert_eq!(
            arrow_type(&resolve("time without time zone"), false, false),
            DataType::Time64(TimeUnit::Microsecond)
        );
    }

    #[test]
    fn everything_uncertain_becomes_utf8() {
        for t in [
            "numeric",
            "numeric(39,2)",
            "integer[]",
            "public.my_enum",
            "interval",
            "jsonb",
        ] {
            assert_eq!(arrow_type(&resolve(t), false, false), DataType::Utf8, "{t}");
        }
    }

    /// `wide_numeric_as_decimal` maps a too-wide `numeric` to `decimal(38,18)`, but leaves
    /// arrays, a `numeric(p,s)` that already fits, and a `numeric(p,s)` whose scale Arrow
    /// cannot represent (a narrower, different problem) exactly as they were.
    #[test]
    fn wide_numeric_as_decimal_only_affects_too_wide_numeric() {
        assert_eq!(
            arrow_type(&resolve("numeric"), true, false),
            DataType::Decimal128(38, 18),
            "unconstrained numeric"
        );
        assert_eq!(
            arrow_type(&resolve("numeric(39,2)"), true, false),
            DataType::Decimal128(38, 18),
            "precision over 38"
        );
        assert_eq!(
            arrow_type(&resolve("numeric(10,2)"), true, false),
            DataType::Decimal128(10, 2),
            "already fits natively; must not be widened"
        );
        assert_eq!(
            arrow_type(&resolve("numeric(2,5)"), true, false),
            DataType::Utf8,
            "scale exceeding precision is a different problem, unaffected by this flag"
        );
        assert_eq!(
            arrow_type(&resolve("numeric(5,-2)"), true, false),
            DataType::Utf8,
            "negative scale is a different problem, unaffected by this flag"
        );
        assert_eq!(
            arrow_type(&resolve("numeric[]"), true, false),
            DataType::Utf8,
            "arrays stay text regardless of this flag"
        );
    }

    /// `native_arrays` maps a one-dimensional array to `List<element>` only when the
    /// element type itself has a concrete mapping; everything else (multi-dimensional,
    /// an unsupported element) is unaffected and keeps falling back to text.
    #[test]
    fn native_arrays_only_affects_one_dimensional_arrays_of_a_supported_element() {
        assert_eq!(
            arrow_type(&resolve("integer[]"), false, true),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );
        assert_eq!(
            arrow_type(&resolve("text[]"), false, true),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            "a recognised textual element must still become a native list of Utf8"
        );
        assert_eq!(
            arrow_type(&resolve("integer[][]"), false, true),
            DataType::Utf8,
            "multi-dimensional arrays are unsupported regardless of this flag"
        );
        assert_eq!(
            arrow_type(&resolve("public.my_enum[]"), false, true),
            DataType::Utf8,
            "an unrecognised element type falls the whole column back to text"
        );
        assert_eq!(
            arrow_type(&resolve("numeric[]"), false, true),
            DataType::Utf8,
            "an unconstrained numeric element without wide_numeric_as_decimal is unsupported"
        );
        assert_eq!(
            arrow_type(&resolve("numeric[]"), true, true),
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(38, 18),
                true
            ))),
            "wide_numeric_as_decimal applies to an array's element exactly as it would a \
             plain column of that type"
        );
        assert_eq!(
            arrow_type(&resolve("integer[]"), false, false),
            DataType::Utf8,
            "off by default"
        );
    }

    #[test]
    fn native_array_values_round_trip_including_nulls() {
        let columns = cols(&[("tags", "integer[]")]);
        let mut b = BatchBuilder::new(&columns, 10, 1 << 20, false, true);

        b.append_row(&[Some(b"{1,2,3}".as_slice())]).unwrap();
        b.append_row(&[Some(b"{4,NULL,6}".as_slice())]).unwrap();
        b.append_row(&[None]).unwrap(); // The array itself is null.
        b.append_row(&[Some(b"{}".as_slice())]).unwrap(); // An empty array is not null.

        let batch = b.finish().unwrap().unwrap();
        let list = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();

        assert_eq!(list.len(), 4);
        assert!(!list.is_null(0));
        assert!(!list.is_null(1));
        assert!(list.is_null(2));
        assert!(!list.is_null(3));

        let row0 = list.value(0);
        let row0 = row0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            row0.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );

        let row1 = list.value(1);
        let row1 = row1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(
            row1.iter().collect::<Vec<_>>(),
            vec![Some(4), None, Some(6)]
        );

        let row3 = list.value(3);
        assert_eq!(row3.len(), 0, "an empty array must have no elements");
    }

    #[test]
    fn builds_a_batch_with_mixed_types() {
        let columns = cols(&[
            ("id", "integer"),
            ("name", "text"),
            ("price", "numeric(10,2)"),
            ("made", "timestamp without time zone"),
            ("born", "date"),
            ("blob", "bytea"),
        ]);
        let mut b = BatchBuilder::new(&columns, 1000, 1 << 20, false, false);

        b.append_row(&[
            Some(b"1"),
            Some("alice".as_bytes()),
            Some(b"123.45"),
            Some(b"1970-01-01 00:00:01"),
            Some(b"1970-01-02"),
            Some(br"\x4869"),
        ])
        .unwrap();
        b.append_row(&[Some(b"2"), None, None, None, None, None])
            .unwrap();

        assert_eq!(b.rows(), 2);
        let batch = b.finish().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "alice");
        assert!(names.is_null(1));

        let price = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(price.value(0), 12345);

        let made = batch
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(made.value(0), 1_000_000);

        let born = batch
            .column(4)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        assert_eq!(born.value(0), 1);

        let blob = batch
            .column(5)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(blob.value(0), b"Hi");
    }

    #[test]
    fn finish_resets_for_reuse() {
        let columns = cols(&[("id", "integer")]);
        let mut b = BatchBuilder::new(&columns, 1000, 1 << 20, false, false);
        b.append_row(&[Some(b"1")]).unwrap();
        assert_eq!(b.finish().unwrap().unwrap().num_rows(), 1);
        assert_eq!(b.rows(), 0);
        assert!(
            b.finish().unwrap().is_none(),
            "empty flush must be harmless"
        );
        b.append_row(&[Some(b"2")]).unwrap();
        assert_eq!(b.finish().unwrap().unwrap().num_rows(), 1);
    }

    #[test]
    fn unrepresentable_values_become_null_and_are_counted() {
        let columns = cols(&[("t", "timestamp without time zone"), ("n", "numeric(5,2)")]);
        let mut b = BatchBuilder::new(&columns, 1000, 1 << 20, false, false);
        b.append_row(&[Some(b"infinity"), Some(b"NaN")]).unwrap();
        b.append_row(&[Some(b"1970-01-01 00:00:00"), Some(b"1.00")])
            .unwrap();

        assert_eq!(b.substitutions(), &[1, 1]);
        let batch = b.finish().unwrap().unwrap();
        assert!(batch.column(0).is_null(0));
        assert!(!batch.column(0).is_null(1));
    }

    #[test]
    fn timestamptz_carries_utc_in_its_type() {
        let columns = cols(&[("t", "timestamp with time zone")]);
        let mut b = BatchBuilder::new(&columns, 10, 1 << 20, false, false);
        b.append_row(&[Some(b"2024-01-01 13:00:00+01")]).unwrap();
        let batch = b.finish().unwrap().unwrap();
        assert_eq!(
            batch.schema().field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
        );
        assert_eq!(
            batch.column(0).data_type(),
            batch.schema().field(0).data_type()
        );
    }

    #[test]
    fn arity_mismatch_is_rejected() {
        let columns = cols(&[("a", "integer"), ("b", "integer")]);
        let mut b = BatchBuilder::new(&columns, 10, 1 << 20, false, false);
        assert_eq!(
            b.append_row(&[Some(b"1")]).unwrap_err(),
            Error::FieldCountMismatch {
                found: 1,
                expected: 2
            }
        );
    }

    #[test]
    fn malformed_value_fails_the_load() {
        let columns = cols(&[("a", "integer")]);
        let mut b = BatchBuilder::new(&columns, 10, 1 << 20, false, false);
        assert!(matches!(
            b.append_row(&[Some(b"not-a-number")]),
            Err(Error::UnparsableValue { .. })
        ));
    }

    #[test]
    fn non_utf8_text_fails_rather_than_corrupting() {
        let columns = cols(&[("a", "text")]);
        let mut b = BatchBuilder::new(&columns, 10, 1 << 20, false, false);
        assert!(matches!(
            b.append_row(&[Some(&[0xFF, 0xFE])]),
            Err(Error::NonUtf8Text { .. })
        ));
    }

    #[test]
    fn bounds_signal_when_to_flush() {
        let columns = cols(&[("a", "integer")]);
        let mut b = BatchBuilder::new(&columns, 2, 1 << 20, false, false);
        assert!(!b.is_full());
        b.append_row(&[Some(b"1")]).unwrap();
        assert!(!b.is_full());
        b.append_row(&[Some(b"2")]).unwrap();
        assert!(b.is_full());

        let mut b = BatchBuilder::new(&columns, 1_000_000, 4, false, false);
        b.append_row(&[Some(b"12345")]).unwrap();
        assert!(b.is_full(), "byte bound must also trigger a flush");
    }

    #[test]
    fn array_columns_keep_their_postgres_literal() {
        let columns = cols(&[("tags", "text[]")]);
        let mut b = BatchBuilder::new(&columns, 10, 1 << 20, false, false);
        b.append_row(&[Some(b"{a,b,c}")]).unwrap();
        let batch = b.finish().unwrap().unwrap();
        let got = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(got.value(0), "{a,b,c}");
    }
}
