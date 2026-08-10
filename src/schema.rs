//! Target-schema coercion.
//!
//! Reads the target's schema, casts the transformed batch to it, and never lets a value it
//! cannot convert exactly reach the target as a silent NULL.
//!
//! There are two ways to honour that, and a pipeline picks one by whether it has somewhere
//! to put rejects:
//!
//! - [`SchemaCoercer::coerce`] fails the whole batch on the first bad value. Simple, and
//!   right when the input is a typed contract you would rather stop than violate.
//! - [`SchemaCoercer::coerce_quarantining`] separates the offending **rows** from the rest,
//!   so the good ones commit and the bad ones go to a dead-letter table. Nothing is nulled
//!   and nothing is dropped — the guarantee moves from the batch to the row.
//!
//! Both refuse a *structural* mismatch outright, and deliberately so. A target column the
//! transform does not produce at all is not bad data: it is the same on every batch, it
//! cannot be attributed to any row, and quarantining it would leave a target that silently
//! never grows.

use std::sync::Arc;

use deltalake::arrow::array::{new_null_array, Array, ArrayRef, BooleanArray, RecordBatch};
use deltalake::arrow::compute::{cast_with_options, filter, filter_record_batch, CastOptions};
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

    /// The target columns `batch` actually carries, in target order.
    ///
    /// The complement is what [`Self::coerce`] fills with nulls, and an upsert must not
    /// write those. The null means *"the transform said nothing about this column"*, not
    /// *"this column is now empty"* — appending a row with one is honest, because the row
    /// is new and nobody has filled it in yet, but **updating** an existing row with one
    /// erases whatever was there. On a target where an enrichment job or a dbt post-hook
    /// owns a column this tool does not select, `UPDATE SET *` would blank it on every
    /// re-delivery, and unlike an append there is no new row left behind for the other
    /// writer to notice.
    pub fn columns_present_in(&self, batch: &RecordBatch) -> Vec<String> {
        self.target
            .fields()
            .iter()
            .filter(|f| batch.schema().index_of(f.name()).is_ok())
            .map(|f| f.name().clone())
            .collect()
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

    /// Cast `batch` to the target schema, setting aside the rows that will not go.
    ///
    /// Same guarantee as [`Self::coerce`], enforced one row lower down: a value that cannot
    /// be converted exactly never reaches the target, because the row carrying it does not
    /// either. Nothing is nulled to make it fit and nothing is silently dropped — the
    /// rejects come back in [`Coerced::bad`] for the caller to write somewhere durable.
    ///
    /// # How a bad row is spotted
    ///
    /// The cast runs with `safe: true`, which turns a value that will not fit into a NULL
    /// instead of an error. On its own that is precisely the silent corruption this tool
    /// exists to avoid — so the NULL is not the result, it is the *signal*. A row is
    /// rejected when the cast produced a NULL where the incoming value was not null, which
    /// is exactly the set of values `safe: false` would have failed the batch for.
    ///
    /// Structural problems are still errors: see the module header.
    pub fn coerce_quarantining(&self, batch: &RecordBatch) -> Result<Coerced> {
        let rows = batch.num_rows();
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.target.fields().len());
        // Per row, the first column that rejected it and why. `None` means the row is fine.
        let mut rejected: Vec<Option<(String, String)>> = vec![None; rows];

        for field in self.target.fields() {
            let resolved = match batch.schema().index_of(field.name()) {
                Ok(idx) => {
                    let col = batch.column(idx);
                    // A nested column cannot take the lenient path. Arrow pushes the cast
                    // options down into the *children* of a list, struct or map and rebuilds
                    // the parent with its original null buffer — so a value that will not
                    // convert becomes a NULL element inside a row that still looks perfectly
                    // valid from the outside. The row-level check below would not see it,
                    // and the target would quietly receive `[1, NULL]` where the strict path
                    // stops the batch. That is the exact corruption this whole design
                    // refuses, and it would appear only once a data-quality table was
                    // configured — turning a visible stall into an invisible one.
                    //
                    // So nested columns keep the strict cast. A bad value in one fails the
                    // batch, which now means this pipeline backs off and retries rather than
                    // taking the fleet with it.
                    let nested = field.data_type().is_nested() || col.data_type().is_nested();
                    let resolved = if col.data_type() == field.data_type() {
                        col.clone()
                    } else {
                        let opts = CastOptions {
                            safe: !nested,
                            ..Default::default()
                        };
                        cast_with_options(col, field.data_type(), &opts).map_err(|e| {
                            Error::Schema(format!(
                                "column {:?}: cannot cast {} -> {} without loss: {e}. Fix the \
                                 transform_sql to produce the target type explicitly, or \
                                 change the target column's type.",
                                field.name(),
                                col.data_type(),
                                field.data_type()
                            ))
                        })?
                    };
                    for (i, slot) in rejected.iter_mut().enumerate() {
                        if slot.is_some() || resolved.is_valid(i) {
                            continue;
                        }
                        if col.is_valid(i) {
                            *slot = Some((
                                field.name().clone(),
                                format!(
                                    "value does not fit {:?} ({} -> {})",
                                    field.name(),
                                    col.data_type(),
                                    field.data_type()
                                ),
                            ));
                        } else if !field.is_nullable() {
                            *slot = Some((
                                field.name().clone(),
                                format!("column {:?} is NOT NULL in the target", field.name()),
                            ));
                        }
                    }
                    resolved
                }
                Err(_) if field.is_nullable() => new_null_array(field.data_type(), rows),
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
            };
            columns.push(resolved);
        }

        if rejected.iter().all(Option::is_none) {
            return Ok(Coerced {
                good: RecordBatch::try_new(self.target.clone(), columns).map_err(|e| {
                    Error::Schema(format!("could not assemble batch in target schema: {e}"))
                })?,
                bad: None,
            });
        }

        let keep: BooleanArray = rejected.iter().map(|r| Some(r.is_none())).collect();
        let drop: BooleanArray = rejected.iter().map(|r| Some(r.is_some())).collect();

        // Filter before assembling: a rejected row may hold a NULL in a NOT NULL target
        // column, which `RecordBatch::try_new` would refuse.
        let kept: Vec<ArrayRef> = columns
            .iter()
            .map(|c| filter(c, &keep))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| Error::Schema(format!("could not separate the good rows: {e}")))?;

        let (reasons, offending): (Vec<String>, Vec<String>) = rejected
            .into_iter()
            .flatten()
            .map(|(column, reason)| (reason, column))
            .unzip();

        Ok(Coerced {
            good: RecordBatch::try_new(self.target.clone(), kept).map_err(|e| {
                Error::Schema(format!("could not assemble batch in target schema: {e}"))
            })?,
            bad: Some(Rejected {
                // The rows as they arrived, not as they were coerced: the point of keeping
                // them is to show what could not be converted.
                rows: filter_record_batch(batch, &drop)
                    .map_err(|e| Error::Schema(format!("could not separate the bad rows: {e}")))?,
                reasons,
                columns: offending,
            }),
        })
    }
}

/// One batch, split into what the target will take and what it will not.
#[derive(Debug, Clone)]
pub struct Coerced {
    /// Rows in the target schema, ready to commit. May have no rows at all.
    pub good: RecordBatch,
    /// Rows that could not be converted. `None` when every row went through, which is the
    /// case that must stay cheap.
    pub bad: Option<Rejected>,
}

/// Rows the target would not take, and why.
#[derive(Debug, Clone)]
pub struct Rejected {
    /// The rows as they arrived, in the transform's own schema.
    pub rows: RecordBatch,
    /// Why each row was rejected, one per row of `rows`.
    pub reasons: Vec<String>,
    /// Which column did it, one per row of `rows`.
    pub columns: Vec<String>,
}

impl Rejected {
    pub fn len(&self) -> usize {
        self.rows.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
mod quarantine_tests {
    use super::*;
    use deltalake::arrow::array::{Int64Array, ListArray, StringArray};

    fn target() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    fn incoming(ids: Vec<Option<i64>>, amounts: Vec<Option<&str>>) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("amount", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(amounts)) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn ids_of(b: &RecordBatch) -> Vec<i64> {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    #[test]
    fn the_row_that_will_not_cast_is_the_only_one_set_aside() {
        let c = SchemaCoercer::new(target());
        let out = c
            .coerce_quarantining(&incoming(
                vec![Some(1), Some(2), Some(3)],
                vec![Some("10"), Some("n/a"), Some("30")],
            ))
            .unwrap();

        assert_eq!(ids_of(&out.good), vec![1, 3]);
        let bad = out.bad.expect("row 2 must be set aside");
        assert_eq!(bad.len(), 1);
        assert_eq!(bad.columns, vec!["amount"]);
    }

    #[test]
    fn a_value_that_was_already_null_is_not_a_reject() {
        // Only a cast that *destroyed* a value counts. A column that arrived null and is
        // allowed to be null is simply null.
        let c = SchemaCoercer::new(target());
        let out = c
            .coerce_quarantining(&incoming(vec![Some(1)], vec![None]))
            .unwrap();
        assert_eq!(ids_of(&out.good), vec![1]);
        assert!(out.bad.is_none());
    }

    #[test]
    fn a_null_where_the_target_says_not_null_is_a_reject() {
        let c = SchemaCoercer::new(target());
        let out = c
            .coerce_quarantining(&incoming(vec![Some(1), None], vec![Some("10"), Some("20")]))
            .unwrap();
        assert_eq!(ids_of(&out.good), vec![1]);
        let bad = out.bad.expect("a null id cannot be stored");
        assert_eq!(bad.columns, vec!["id"]);
        assert!(bad.reasons[0].contains("NOT NULL"), "{:?}", bad.reasons);
    }

    #[test]
    fn a_clean_batch_costs_nothing_and_reports_nothing() {
        let c = SchemaCoercer::new(target());
        let out = c
            .coerce_quarantining(&incoming(
                vec![Some(1), Some(2)],
                vec![Some("10"), Some("20")],
            ))
            .unwrap();
        assert_eq!(out.good.num_rows(), 2);
        assert!(out.bad.is_none());
    }

    #[test]
    fn a_missing_not_null_column_is_structural_and_still_errors() {
        let c = SchemaCoercer::new(target());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "amount",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![Some("10")])) as ArrayRef],
        )
        .unwrap();
        let e = c.coerce_quarantining(&batch).unwrap_err().to_string();
        assert!(e.contains("id"), "should name the column: {e}");
        assert!(
            e.contains("NOT NULL"),
            "and say why it cannot be invented: {e}"
        );
    }

    #[test]
    fn a_bad_value_inside_a_list_is_never_nulled_into_the_target() {
        // The trap this whole split has to avoid. Arrow pushes cast options down into a
        // list's *child* values and keeps the parent's null buffer, so a lenient cast turns
        // an unconvertible element into a NULL inside a row that still looks valid. Checking
        // nullness at the top level would not notice, and the target would receive
        // `[1, NULL]` — silently, and only for pipelines that configured a data-quality
        // table. Nested columns therefore keep the strict cast.
        let target = Arc::new(Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )]));
        // One row holding two elements: one that converts, one that does not.
        let values = StringArray::from(vec![Some("1"), Some("oops")]);
        let offsets = deltalake::arrow::buffer::OffsetBuffer::new(vec![0, 2].into());
        let list = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            offsets,
            Arc::new(values),
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            )])),
            vec![Arc::new(list) as ArrayRef],
        )
        .unwrap();

        let c = SchemaCoercer::new(target);
        let e = c
            .coerce_quarantining(&batch)
            .expect_err("a nested bad value must not be quarantined into a NULL");
        let e = e.to_string();
        assert!(e.contains("items"), "names the column: {e}");
        assert!(
            e.contains("without loss"),
            "and uses the strict wording, because that is what happened: {e}"
        );
    }
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
