//! Target-schema coercion.
//!
//! Reads the target's schema, casts the transformed batch to it, and **fails loudly** on
//! anything it cannot do exactly. There is no dead-letter queue by design (plan §2.7):
//! input is typed Parquet, not arbitrary JSON, so a cast failure is a pipeline failure and
//! should stop the world rather than silently null a column.

use std::sync::Arc;

use deltalake::arrow::array::{new_null_array, RecordBatch};
use deltalake::arrow::compute::{cast_with_options, CastOptions};
use deltalake::arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::error::{Error, Result};

/// Casts batches to a fixed target schema.
#[derive(Clone, Debug)]
pub struct SchemaCoercer {
    target: SchemaRef,
}

impl SchemaCoercer {
    pub fn new(target: SchemaRef) -> Self {
        Self { target }
    }

    pub fn target(&self) -> SchemaRef {
        self.target.clone()
    }

    /// Cast `batch` to the target schema.
    ///
    /// - A target column missing from the batch is an error, unless it is nullable, in
    ///   which case it is filled with nulls (the column genuinely has no value).
    /// - A batch column absent from the target is dropped, because the target schema is
    ///   the contract and v1 does not evolve it.
    /// - A cast that cannot be performed exactly is an error, never a silent null.
    pub fn coerce(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(self.target.fields().len());

        for field in self.target.fields() {
            match batch.schema().index_of(field.name()) {
                Ok(idx) => {
                    let col = batch.column(idx);
                    let resolved = if col.data_type() == field.data_type() {
                        col.clone()
                    } else {
                        // safe: false => a value that does not fit becomes an error, not NULL.
                        let opts = CastOptions {
                            safe: false,
                            ..Default::default()
                        };
                        cast_with_options(col, field.data_type(), &opts).map_err(|e| {
                            Error::Schema(format!(
                                "column {:?}: cannot cast {} -> {} without loss: {e}. Fix the \
                                 transform_sql to produce the target type explicitly, or change \
                                 the target column's type.",
                                field.name(),
                                col.data_type(),
                                field.data_type()
                            ))
                        })?
                    };
                    // Checked for passthrough as well as cast columns: a nullable source
                    // feeding a NOT NULL target is a mismatch however the types line up.
                    if !field.is_nullable() && resolved.null_count() > 0 {
                        return Err(Error::Schema(format!(
                            "column {:?} is NOT NULL in the target but the batch contains \
                             {} null(s)",
                            field.name(),
                            resolved.null_count()
                        )));
                    }
                    columns.push(resolved);
                }
                Err(_) if field.is_nullable() => {
                    columns.push(new_null_array(field.data_type(), batch.num_rows()));
                }
                Err(_) => {
                    return Err(Error::Schema(format!(
                        "target column {:?} ({}) is NOT NULL but the transform produced no \
                         such column. Available: [{}]",
                        field.name(),
                        field.data_type(),
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
            }
        }

        RecordBatch::try_new(self.target.clone(), columns)
            .map_err(|e| Error::Schema(format!("could not assemble batch in target schema: {e}")))
    }
}

/// Drop metadata so two schemas compare on structure alone.
pub fn bare_schema(s: &Schema) -> Schema {
    Schema::new(
        s.fields()
            .iter()
            .map(|f| Arc::new(Field::new(f.name(), f.data_type().clone(), f.is_nullable())))
            .collect::<Vec<_>>(),
    )
}

/// True when `from` can be cast to `to` without losing information.
///
/// Used for pre-flight diagnostics; the actual guarantee comes from `safe: false` casting.
pub fn is_widening(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    matches!(
        (from, to),
        (Int8, Int16 | Int32 | Int64)
            | (Int16, Int32 | Int64)
            | (Int32, Int64)
            | (UInt8, UInt16 | UInt32 | UInt64 | Int16 | Int32 | Int64)
            | (UInt16, UInt32 | UInt64 | Int32 | Int64)
            | (UInt32, UInt64 | Int64)
            | (Float32, Float64)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn batch(s: SchemaRef, cols: Vec<deltalake::arrow::array::ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(s, cols).unwrap()
    }

    #[test]
    fn identical_schema_passes_through() {
        let s = schema(vec![Field::new("a", DataType::Int32, false)]);
        let b = batch(s.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]);
        let out = SchemaCoercer::new(s).coerce(&b).unwrap();
        assert_eq!(out.num_rows(), 3);
    }

    #[test]
    fn widening_cast_is_performed() {
        let src = schema(vec![Field::new("a", DataType::Int32, false)]);
        let tgt = schema(vec![Field::new("a", DataType::Int64, false)]);
        let b = batch(src, vec![Arc::new(Int32Array::from(vec![1, 2]))]);
        let out = SchemaCoercer::new(tgt).coerce(&b).unwrap();
        assert_eq!(out.column(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn lossy_cast_errors_instead_of_nulling() {
        // The headline guarantee: a value that does not fit must not become NULL.
        let src = schema(vec![Field::new("a", DataType::Utf8, false)]);
        let tgt = schema(vec![Field::new("a", DataType::Int32, false)]);
        let b = batch(
            src,
            vec![Arc::new(StringArray::from(vec!["1", "not-a-number"]))],
        );
        let err = SchemaCoercer::new(tgt).coerce(&b).unwrap_err();
        assert!(err.to_string().contains("cannot cast"), "got: {err}");
    }

    #[test]
    fn missing_nullable_column_is_filled_with_nulls() {
        let src = schema(vec![Field::new("a", DataType::Int32, false)]);
        let tgt = schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let b = batch(src, vec![Arc::new(Int32Array::from(vec![1, 2]))]);
        let out = SchemaCoercer::new(tgt).coerce(&b).unwrap();
        assert_eq!(out.num_columns(), 2);
        assert_eq!(out.column(1).null_count(), 2);
    }

    #[test]
    fn missing_non_nullable_column_is_an_error_that_lists_what_was_available() {
        let src = schema(vec![Field::new("a", DataType::Int32, false)]);
        let tgt = schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("required", DataType::Utf8, false),
        ]);
        let b = batch(src, vec![Arc::new(Int32Array::from(vec![1]))]);
        let err = SchemaCoercer::new(tgt).coerce(&b).unwrap_err();
        assert!(err.to_string().contains("required"), "got: {err}");
        assert!(err.to_string().contains("Available"), "got: {err}");
    }

    #[test]
    fn extra_batch_column_is_dropped_not_evolved() {
        // v1 does not evolve the target schema; the target is the contract.
        let src = schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("extra", DataType::Utf8, true),
        ]);
        let tgt = schema(vec![Field::new("a", DataType::Int32, false)]);
        let b = batch(
            src,
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["x"])),
            ],
        );
        let out = SchemaCoercer::new(tgt).coerce(&b).unwrap();
        assert_eq!(out.num_columns(), 1);
    }

    #[test]
    fn column_order_follows_the_target_not_the_batch() {
        let src = schema(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int32, false),
        ]);
        let tgt = schema(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let b = batch(
            src,
            vec![
                Arc::new(StringArray::from(vec!["x"])),
                Arc::new(Int32Array::from(vec![7])),
            ],
        );
        let out = SchemaCoercer::new(tgt).coerce(&b).unwrap();
        assert_eq!(out.schema().field(0).name(), "a");
        assert_eq!(
            out.column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(0),
            7
        );
    }

    #[test]
    fn float_to_decimal_precision_note() {
        // Plan §3: doubles at bronze have already lost precision. This only checks the
        // cast mechanism; the real fix is decimal(18,4) upstream.
        let src = schema(vec![Field::new("p", DataType::Float64, false)]);
        let tgt = schema(vec![Field::new("p", DataType::Decimal128(18, 4), false)]);
        let b = batch(src, vec![Arc::new(Float64Array::from(vec![1.25, 2.5]))]);
        let out = SchemaCoercer::new(tgt).coerce(&b).unwrap();
        assert_eq!(out.column(0).data_type(), &DataType::Decimal128(18, 4));
    }

    #[test]
    fn is_widening_recognises_int_promotion() {
        assert!(is_widening(&DataType::Int32, &DataType::Int64));
        assert!(!is_widening(&DataType::Int64, &DataType::Int32));
    }

    #[test]
    fn not_null_target_rejects_nulls_introduced_by_casting() {
        let src = schema(vec![Field::new("a", DataType::Int64, true)]);
        let tgt = schema(vec![Field::new("a", DataType::Int64, false)]);
        let b = batch(src, vec![Arc::new(Int64Array::from(vec![Some(1), None]))]);
        let err = SchemaCoercer::new(tgt).coerce(&b).unwrap_err();
        assert!(err.to_string().contains("NOT NULL"), "got: {err}");
    }
}
