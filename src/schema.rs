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
//!
//! A value with a fraction going into an integer column does not convert exactly either, though
//! Arrow's cast truncates it rather than failing, so it is looked for separately
//! ([`Fractions`]). A Kafka pipeline keeps a float's truncation, as kafka-delta-ingest wrote it.

use std::ops::Range;
use std::sync::Arc;

use deltalake::arrow::array::{
    new_null_array, Array, ArrayRef, AsArray, BooleanArray, RecordBatch,
};
use deltalake::arrow::compute::{
    can_cast_types, cast_with_options, filter, filter_record_batch, CastOptions,
};
use deltalake::arrow::datatypes::{
    ArrowNativeType, ArrowNativeTypeOp, DataType, Decimal128Type, Decimal256Type, Decimal32Type,
    Decimal64Type, DecimalType, Field, FieldRef, Fields, Float16Type, Float32Type, Float64Type,
    Int16Type, Int32Type, Int64Type, RunEndIndexType, Schema, SchemaRef, UnionFields,
};

use crate::error::{Error, Result};
use crate::transform::decimal;

/// Casts batches to a fixed target schema.
#[derive(Clone, Debug)]
pub struct SchemaCoercer {
    target: SchemaRef,
    fractions: Fractions,
}

/// What becomes of a value with a fraction that goes into an integer column: a float, or a
/// decimal with a scale, that is not a whole number, at any depth. A TIMESTAMP or DURATION column
/// counts as one, since the cast reaches it through a BIGINT count of its unit.
///
/// Arrow's cast truncates it toward zero, even with `safe: false`: 1.5 lands as 1 with no error
/// and, because the result is not NULL, nothing for [`SchemaCoercer::coerce_quarantining`] to
/// notice either. So it is looked for before the value is written ([`fractions_dropped`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fractions {
    /// Refused like any other value that does not convert exactly: [`SchemaCoercer::coerce`]
    /// fails the batch, [`SchemaCoercer::coerce_quarantining`] sets the row aside.
    Refused,
    /// A float's truncated toward zero, as the cast does, and a decimal's refused. A Kafka
    /// pipeline's: kafka-delta-ingest's arrow-json read a number with a fraction into an integer
    /// column through `f64` and `NumCast`, which truncates (docs/kafka-design.md §5.6, "cast"
    /// into a typed column). It never wrote a decimal into one: kdi's JSON held an Avro decimal
    /// as an array of bytes, which arrow-json refused there, so there is nothing to match.
    FloatsTruncated,
}

impl SchemaCoercer {
    /// A coercer that refuses a fraction going into an integer column.
    pub fn new(target: SchemaRef) -> Self {
        Self {
            target,
            fractions: Fractions::Refused,
        }
    }

    /// A coercer that truncates a float's fraction going into an integer column, as a Kafka
    /// pipeline's does, and refuses a decimal's ([`Fractions::FloatsTruncated`]).
    pub fn truncating_float_fractions(target: SchemaRef) -> Self {
        Self {
            target,
            fractions: Fractions::FloatsTruncated,
        }
    }

    /// Per row of `col`, whether going into `to` drops a fraction this coercer refuses.
    fn refused_fractions(&self, col: &dyn Array, to: &DataType) -> Option<Vec<bool>> {
        match self.fractions {
            Fractions::Refused => fractions_dropped(col, to),
            Fractions::FloatsTruncated => decimal_fractions_dropped(col, to),
        }
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
    /// - A cast that cannot be performed exactly is an error, never a silent null, and a
    ///   fraction going into an integer column is not silently truncated, a float's under
    ///   [`Fractions::FloatsTruncated`] aside.
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
                        // A DECIMAL landing in a DOUBLE or REAL column becomes the value a
                        // CAST in a model makes of it — see `crate::transform::decimal`.
                        let opts = CastOptions {
                            safe: false,
                            ..Default::default()
                        };
                        let cast = decimal::cast_with_options(col, field.data_type(), &opts)
                            .map_err(|e| {
                                Error::Schema(format!(
                                    "column {:?}: cannot cast {} -> {} without loss: {e}. Fix \
                                     the transform_sql to produce the target type explicitly, or \
                                     change the target column's type.",
                                    field.name(),
                                    col.data_type(),
                                    field.data_type()
                                ))
                            })?;
                        let dropped = self.refused_fractions(col.as_ref(), field.data_type());
                        if let Some(row) = dropped.and_then(|d| d.iter().position(|d| *d)) {
                            return Err(Error::Schema(format!(
                                "column {:?}: cannot cast {} -> {} without loss: row {row} holds \
                                 a value with a fraction, which {} would drop. Fix the \
                                 transform_sql to produce the target type explicitly (rounding \
                                 the value as you mean it), or change the target column's type.",
                                field.name(),
                                col.data_type(),
                                field.data_type(),
                                field.data_type()
                            )));
                        }
                        cast
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
    /// A fraction going into an integer column leaves no NULL to see, so those rows are found
    /// before the cast instead ([`Fractions`]), nested columns included: the row is set aside
    /// however deep the value is, because the check reads the source values themselves.
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
                        decimal::cast_with_options(col, field.data_type(), &opts).map_err(|e| {
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
                    let dropped = self.refused_fractions(col.as_ref(), field.data_type());
                    for (i, slot) in rejected.iter_mut().enumerate() {
                        if slot.is_some() {
                            continue;
                        }
                        if resolved.is_valid(i) {
                            if dropped.as_ref().is_some_and(|d| d[i]) {
                                *slot = Some((
                                    field.name().clone(),
                                    format!(
                                        "value of {:?} has a fraction, which {} -> {} would drop",
                                        field.name(),
                                        col.data_type(),
                                        field.data_type()
                                    ),
                                ));
                            }
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
                columns: offending.into_iter().map(Some).collect(),
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

/// Rows the target would not take, or the transform could not evaluate, and why.
#[derive(Debug, Clone)]
pub struct Rejected {
    /// The rows as they arrived where they were refused: the transform's output for a row
    /// that would not coerce, the source row for one the transform could not evaluate —
    /// there is no output row to show for that one.
    pub rows: RecordBatch,
    /// Why each row was rejected, one per row of `rows`.
    pub reasons: Vec<String>,
    /// Which column did it, one per row of `rows`. `None` when the transform could not
    /// evaluate the row, because nothing says which of its expressions failed.
    pub columns: Vec<Option<String>>,
}

impl Rejected {
    pub fn len(&self) -> usize {
        self.rows.num_rows()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

// ------------------------------------------------------- fractions going into integers

/// Per row of `col`, whether casting it to `to` drops a fraction: a finite float that is not a
/// whole number, or a decimal with a positive scale that is not, landing in an integer, at any
/// depth. A timestamp or a duration counts as one, since the cast reaches either through
/// `Int64`. `None` when the two types cannot do that, which is every column whose type already
/// matches and every cast with no float or decimal going into one of those.
///
/// Values are paired as Arrow's cast pairs them (arrow-cast cast/mod.rs), so that each is judged
/// where it lands: a run-end encoded array through its runs; a union through the one child the
/// cast extracts (`resolve_child_array`), its other rows becoming NULL; a dictionary through its
/// values; a list's elements in place; a one-element fixed-size list unwrapped, its own nulls
/// dropped as the cast drops them; a plain value wrapped into a one-element list; a map's keys
/// and values by position; a struct's fields in order when every name lines up, by name when
/// every target field is in the source, and by position otherwise. A value under a null list,
/// map or struct is not written, so it is not judged.
///
/// A NaN, an infinity or a value past the integer's range is not looked for: the cast refuses
/// those itself, with an error, or under `safe: true` a NULL.
pub fn fractions_dropped(col: &dyn Array, to: &DataType) -> Option<Vec<bool>> {
    dropped_fractions(col, to, Sources::FloatsAndDecimals)
}

/// [`fractions_dropped`] for decimals alone: what a Kafka pipeline refuses, whose floats are
/// truncated as kafka-delta-ingest truncated them ([`Fractions::FloatsTruncated`]).
pub fn decimal_fractions_dropped(col: &dyn Array, to: &DataType) -> Option<Vec<bool>> {
    dropped_fractions(col, to, Sources::Decimals)
}

/// Which values a dropped fraction is looked for in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sources {
    FloatsAndDecimals,
    Decimals,
}

fn dropped_fractions(col: &dyn Array, to: &DataType, sources: Sources) -> Option<Vec<bool>> {
    use DataType::*;

    if !drops_fraction(col.data_type(), to, sources) {
        return None;
    }
    let rows = col.len();
    Some(match (col.data_type(), to) {
        (RunEndEncoded(ends, _), _) => {
            // Into another run-end encoding the runs stay, and only their values are cast.
            let to = match to {
                RunEndEncoded(_, value) => value.data_type(),
                _ => to,
            };
            match ends.data_type() {
                Int16 => runs::<Int16Type>(col, to, sources)?,
                Int32 => runs::<Int32Type>(col, to, sources)?,
                Int64 => runs::<Int64Type>(col, to, sources)?,
                _ => return None,
            }
        }
        (_, RunEndEncoded(_, value)) => return dropped_fractions(col, value.data_type(), sources),
        (Union(fields, _), _) => {
            let union = col.as_union();
            let (id, _) = union_child(fields, to)?;
            let dropped = dropped_fractions(union.child(id).as_ref(), to, sources)?;
            (0..rows)
                .map(|i| {
                    let at = union.offsets().map_or(i, |o| o[i].as_usize());
                    union.type_id(i) == id && dropped[at]
                })
                .collect()
        }
        (Dictionary(_, _), _) => {
            let dict = col.as_any_dictionary();
            let values = dropped_fractions(dict.values().as_ref(), to, sources)?;
            // Also the way out for a dictionary with no values at all, all of whose keys are
            // null, on which `normalized_keys` would panic.
            if !values.contains(&true) {
                return Some(vec![false; rows]);
            }
            let keys = dict.normalized_keys();
            (0..rows)
                .map(|i| col.is_valid(i) && values[keys[i]])
                .collect()
        }
        (_, Dictionary(_, value)) => return dropped_fractions(col, value, sources),
        (from, _) if list_item(from).is_some() && list_item(to).is_some() => {
            let (elements, ranges) = elements(col);
            let dropped =
                dropped_fractions(elements.as_ref(), list_item(to)?.data_type(), sources)?;
            per_row(col, &ranges, &dropped)
        }
        (FixedSizeList(_, 1), _) => {
            return dropped_fractions(col.as_fixed_size_list().values().as_ref(), to, sources)
        }
        (_, List(item) | LargeList(item) | ListView(item) | LargeListView(item)) => {
            return dropped_fractions(col, item.data_type(), sources)
        }
        (_, FixedSizeList(item, 1)) => return dropped_fractions(col, item.data_type(), sources),
        (Map(_, _), Map(entries, _)) => {
            let map = col.as_map();
            let (key, value) = key_value(entries)?;
            let keys = dropped_fractions(map.keys().as_ref(), key.data_type(), sources);
            let values = dropped_fractions(map.values().as_ref(), value.data_type(), sources);
            let dropped: Vec<bool> = (0..map.entries().len())
                .map(|j| {
                    map.entries().is_valid(j)
                        && (keys.as_ref().is_some_and(|k| k[j])
                            || values.as_ref().is_some_and(|v| v[j]))
                })
                .collect();
            let ranges = map
                .offsets()
                .windows(2)
                .map(|w| w[0].as_usize()..w[1].as_usize())
                .collect::<Vec<_>>();
            per_row(col, &ranges, &dropped)
        }
        (Struct(from), Struct(fields)) => {
            let fields_of = col.as_struct();
            let mut dropped = vec![false; rows];
            for (index, field) in struct_pairs(from, fields) {
                if let Some(d) =
                    dropped_fractions(fields_of.column(index).as_ref(), field.data_type(), sources)
                {
                    dropped.iter_mut().zip(d).for_each(|(o, d)| *o |= d);
                }
            }
            (0..rows).map(|i| col.is_valid(i) && dropped[i]).collect()
        }
        (Float16, _) => float_fractions(col.as_primitive::<Float16Type>().iter(), |v| v.to_f64()),
        (Float32, _) => float_fractions(col.as_primitive::<Float32Type>().iter(), f64::from),
        (Float64, _) => float_fractions(col.as_primitive::<Float64Type>().iter(), |v| v),
        (Decimal32(_, scale), _) => decimal_fractions::<Decimal32Type>(col, *scale),
        (Decimal64(_, scale), _) => decimal_fractions::<Decimal64Type>(col, *scale),
        (Decimal128(_, scale), _) => decimal_fractions::<Decimal128Type>(col, *scale),
        (Decimal256(_, scale), _) => decimal_fractions::<Decimal256Type>(col, *scale),
        _ => return None,
    })
}

/// Whether casting a `from` to a `to` can put a fraction of `sources` into an integer:
/// [`fractions_dropped`] on types alone, with the same pairing, so that a column it has nothing
/// to say about costs nothing.
fn drops_fraction(from: &DataType, to: &DataType, sources: Sources) -> bool {
    use DataType::*;

    if from == to {
        return false;
    }
    match (from, to) {
        (RunEndEncoded(_, from), RunEndEncoded(_, to)) => {
            drops_fraction(from.data_type(), to.data_type(), sources)
        }
        (RunEndEncoded(_, value), _) => drops_fraction(value.data_type(), to, sources),
        (_, RunEndEncoded(_, value)) => drops_fraction(from, value.data_type(), sources),
        (Union(fields, _), _) => union_child(fields, to)
            .is_some_and(|(_, child)| drops_fraction(child.data_type(), to, sources)),
        (Dictionary(_, value), _) => drops_fraction(value, to, sources),
        (_, Dictionary(_, value)) => drops_fraction(from, value, sources),
        (_, _) if list_item(from).is_some() && list_item(to).is_some() => drops_fraction(
            list_item(from).unwrap().data_type(),
            list_item(to).unwrap().data_type(),
            sources,
        ),
        (FixedSizeList(item, 1), _) => drops_fraction(item.data_type(), to, sources),
        (_, List(item) | LargeList(item) | ListView(item) | LargeListView(item)) => {
            drops_fraction(from, item.data_type(), sources)
        }
        (_, FixedSizeList(item, 1)) => drops_fraction(from, item.data_type(), sources),
        (Map(from, _), Map(to, _)) => match (key_value(from), key_value(to)) {
            (Some((fk, fv)), Some((tk, tv))) => {
                drops_fraction(fk.data_type(), tk.data_type(), sources)
                    || drops_fraction(fv.data_type(), tv.data_type(), sources)
            }
            _ => false,
        },
        (Struct(from), Struct(to)) => struct_pairs(from, to).into_iter().any(|(index, field)| {
            drops_fraction(from[index].data_type(), field.data_type(), sources)
        }),
        (_, _) if to.is_integer() || matches!(to, Timestamp(_, _) | Duration(_)) => match from {
            Float16 | Float32 | Float64 => sources == Sources::FloatsAndDecimals,
            Decimal32(_, scale)
            | Decimal64(_, scale)
            | Decimal128(_, scale)
            | Decimal256(_, scale) => *scale > 0,
            _ => false,
        },
        _ => false,
    }
}

/// Per row of the run-end encoded `col`, whether its run's value going into `to` drops a
/// fraction: the cast expands every row to its run's value, or keeps the runs and casts the
/// values (arrow-cast cast/run_array.rs), which comes to the same.
fn runs<R: RunEndIndexType>(col: &dyn Array, to: &DataType, sources: Sources) -> Option<Vec<bool>> {
    let run = col.as_run::<R>();
    let values = dropped_fractions(run.values().as_ref(), to, sources)?;
    Some(
        (0..run.len())
            .map(|i| values[run.get_physical_index(i)])
            .collect(),
    )
}

/// The child of a union the cast extracts into `to`, as `resolve_child_array` picks it
/// (arrow-cast cast/union.rs): the first of the same type, else the first of the same family,
/// else, into a type that is not nested, the first that casts to it.
fn union_child<'a>(fields: &'a UnionFields, to: &DataType) -> Option<(i8, &'a FieldRef)> {
    use DataType::*;
    let same_family = |a: &DataType| {
        matches!(
            (a, to),
            (Utf8 | LargeUtf8 | Utf8View, Utf8 | LargeUtf8 | Utf8View)
                | (
                    Binary | LargeBinary | BinaryView,
                    Binary | LargeBinary | BinaryView
                )
                | (Int8 | Int16 | Int32 | Int64, Int8 | Int16 | Int32 | Int64)
                | (
                    UInt8 | UInt16 | UInt32 | UInt64,
                    UInt8 | UInt16 | UInt32 | UInt64
                )
                | (Float16 | Float32 | Float64, Float16 | Float32 | Float64)
        )
    };
    fields
        .iter()
        .find(|(_, f)| f.data_type() == to)
        .or_else(|| fields.iter().find(|(_, f)| same_family(f.data_type())))
        .or_else(|| {
            if to.is_nested() {
                return None;
            }
            fields
                .iter()
                .find(|(_, f)| can_cast_types(f.data_type(), to))
        })
}

/// The item field of a list of any kind (list, large list, list view, fixed-size list).
fn list_item(t: &DataType) -> Option<&FieldRef> {
    use DataType::*;
    match t {
        List(item) | LargeList(item) | ListView(item) | LargeListView(item) => Some(item),
        FixedSizeList(item, _) => Some(item),
        _ => None,
    }
}

/// A list's elements, and per row the range of them it holds.
fn elements(col: &dyn Array) -> (ArrayRef, Vec<Range<usize>>) {
    use DataType::*;
    match col.data_type() {
        List(_) => {
            let list = col.as_list::<i32>();
            let ranges = list.offsets().windows(2);
            (
                list.values().clone(),
                ranges.map(|w| w[0].as_usize()..w[1].as_usize()).collect(),
            )
        }
        LargeList(_) => {
            let list = col.as_list::<i64>();
            let ranges = list.offsets().windows(2);
            (
                list.values().clone(),
                ranges.map(|w| w[0].as_usize()..w[1].as_usize()).collect(),
            )
        }
        ListView(_) => {
            let list = col.as_list_view::<i32>();
            let ranges = list.offsets().iter().zip(list.sizes().iter());
            (
                list.values().clone(),
                ranges
                    .map(|(o, s)| o.as_usize()..(o + s).as_usize())
                    .collect(),
            )
        }
        LargeListView(_) => {
            let list = col.as_list_view::<i64>();
            let ranges = list.offsets().iter().zip(list.sizes().iter());
            (
                list.values().clone(),
                ranges
                    .map(|(o, s)| o.as_usize()..(o + s).as_usize())
                    .collect(),
            )
        }
        FixedSizeList(_, _) => {
            let list = col.as_fixed_size_list();
            let size = list.value_length().as_usize();
            let ranges = (0..list.len()).map(|i| {
                let start = list.value_offset(i).as_usize();
                start..start + size
            });
            (list.values().clone(), ranges.collect())
        }
        other => unreachable!("elements of a {other}, which is no list"),
    }
}

/// Per row of a list or map, whether it is valid and one of its elements in `ranges` drops a
/// fraction.
fn per_row(col: &dyn Array, ranges: &[Range<usize>], dropped: &[bool]) -> Vec<bool> {
    ranges
        .iter()
        .enumerate()
        .map(|(i, range)| col.is_valid(i) && dropped[range.clone()].iter().any(|d| *d))
        .collect()
}

/// A map's key and value fields: the first two of its entries, by position, as the cast reads
/// them (arrow-cast cast/map.rs).
fn key_value(entries: &FieldRef) -> Option<(&FieldRef, &FieldRef)> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() >= 2 => Some((&fields[0], &fields[1])),
        _ => None,
    }
}

/// Which source field each target field of a struct cast reads, as `(source index, target
/// field)`: in order when the names line up one for one, by name when every target field is in
/// the source, and otherwise by position (arrow-cast cast/mod.rs, `cast_struct_to_struct`).
fn struct_pairs<'a>(from: &Fields, to: &'a Fields) -> Vec<(usize, &'a FieldRef)> {
    let in_order = from.len() == to.len()
        && from
            .iter()
            .zip(to.iter())
            .all(|(f, t)| f.name() == t.name());
    let by_name = to
        .iter()
        .map(|t| from.iter().position(|f| f.name() == t.name()))
        .collect::<Option<Vec<usize>>>();
    match by_name {
        Some(indices) if !in_order => indices.into_iter().zip(to.iter()).collect(),
        _ => to.iter().enumerate().take(from.len()).collect(),
    }
}

/// Per value, whether it is a finite float that is not a whole number.
fn float_fractions<T>(
    values: impl Iterator<Item = Option<T>>,
    widen: impl Fn(T) -> f64,
) -> Vec<bool> {
    values
        .map(|v| {
            v.map(&widen)
                .is_some_and(|v| v.is_finite() && v.fract() != 0.0)
        })
        .collect()
}

/// Per value of a decimal column with a positive `scale`, whether it is not a whole number.
fn decimal_fractions<T: DecimalType>(col: &dyn Array, scale: i8) -> Vec<bool> {
    // A unit too large for the type holds every value of it, so only 0 is whole then.
    let unit = T::Native::usize_as(10)
        .pow_checked(scale.unsigned_abs().into())
        .ok();
    col.as_primitive::<T>()
        .iter()
        .map(|v| {
            v.is_some_and(|v| match unit {
                Some(unit) => !v.mod_wrapping(unit).is_zero(),
                None => !v.is_zero(),
            })
        })
        .collect()
}

// ------------------------------------------------------- reading somebody else's files

/// Read a batch as the table says it should be read.
///
/// The Delta protocol defines `timestamp` as microseconds, but not every engine writes it
/// that way: Trino writes milliseconds, into both its checkpoints and the files its
/// `OPTIMIZE` rewrites. Those files are legal parquet, they are already in the lakehouse in
/// volume, and the information needed to read them correctly is unambiguous — the table's
/// own schema says what the column is, and a millisecond value widens to microseconds
/// without loss. So a physical column that differs from the declared one **in precision
/// alone** is coerced here rather than refused.
///
/// This is not a licence to accept anything, and it is deliberately not a new gate either.
/// Only a **widening** between two timestamps is touched. Every other difference — a
/// narrowing, a different logical type, anything at all — is left exactly as it arrived and
/// handed to the code that handled it before this function existed, so it succeeds or fails
/// there in the words it always did: a `string` where the table says `timestamp` still
/// fails, and says so the same way.
///
/// # Timezones
///
/// Trino's column is `timestamp[ms]` with **no** timezone where delta-rs writes
/// `timestamp[us, tz=UTC]`. Delta's `timestamp` is UTC-adjusted by definition, so an absent
/// timezone on a physical column is read as UTC, never as local — Arrow's cast agrees,
/// treating both as the same count from the epoch and changing only the unit.
/// `timestamp_ntz` is untouched by this and continues to mean what it means.
///
/// Returns the batch unchanged, without copying a single buffer, when the file already
/// agrees with the table — which is the common case and has to stay free.
pub fn read_as_declared(batch: RecordBatch, declared: &Schema) -> Result<RecordBatch> {
    let file = batch.schema();
    let Some(target) = aligned_schema(declared, &file)? else {
        return Ok(batch);
    };

    // safe: false => this must be exact. It is a widening by construction, so nothing can
    // fail to fit; the flag is here so that if that ever stops being true, it stops loudly.
    let opts = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = batch
        .columns()
        .iter()
        .zip(target.fields())
        .map(|(col, field)| {
            if col.data_type() == field.data_type() {
                Ok(col.clone())
            } else {
                cast_with_options(col, field.data_type(), &opts).map_err(|e| {
                    Error::Schema(format!(
                        "column {:?}: the file holds {} where the table's schema declares \
                         {}, and reading it as declared failed: {e}",
                        field.name(),
                        col.data_type(),
                        field.data_type()
                    ))
                })
            }
        })
        .collect::<Result<Vec<ArrayRef>>>()?;

    RecordBatch::try_new(target, columns).map_err(|e| {
        Error::Schema(format!(
            "could not read the file at the table's declared precision: {e}"
        ))
    })
}

/// The schema `file` must be read as, or `None` when it already agrees with `declared`.
///
/// Named columns only: a column the table does not declare keeps whatever the file gave it,
/// and a column the file does not carry is not invented here.
fn aligned_schema(declared: &Schema, file: &Schema) -> Result<Option<SchemaRef>> {
    let mut fields: Vec<Arc<Field>> = Vec::with_capacity(file.fields().len());
    let mut changed = false;

    for f in file.fields() {
        let Ok(want) = declared.field_with_name(f.name()) else {
            fields.push(f.clone());
            continue;
        };
        match widened(want.data_type(), f.data_type())
            .map_err(|why| Error::Schema(format!("column {:?}: {why}", f.name())))?
        {
            Some(dt) => {
                changed = true;
                fields.push(Arc::new(f.as_ref().clone().with_data_type(dt)));
            }
            None => fields.push(f.clone()),
        }
    }

    if !changed {
        return Ok(None);
    }
    Ok(Some(Arc::new(Schema::new_with_metadata(
        fields,
        file.metadata().clone(),
    ))))
}

/// The type `have` should be read as to become `want`, or `None` to leave it alone.
///
/// `Err` is reserved for the pairs where reading as declared would change what the value
/// *means* rather than merely how precisely it is written — a timezone that would be
/// applied or dropped rather than relabelled.
fn widened(want: &DataType, have: &DataType) -> std::result::Result<Option<DataType>, String> {
    use DataType::*;

    if want == have {
        return Ok(None);
    }
    match (want, have) {
        (Timestamp(wu, wtz), Timestamp(hu, htz)) => {
            // A file finer than the table is not this function's to touch, and refusing it
            // would be a regression rather than a safeguard. Spark writes Delta timestamps
            // as INT96, which the parquet reader decodes as `Timestamp(ns)` whatever the
            // table declares — so on a Spark-written lakehouse this is not the exception,
            // it is every file. Those values are microsecond-resolution by construction,
            // they have always been read by casting down to the target, and they still are:
            // leaving them alone hands them to exactly the code that handled them before.
            //
            // Widening is the whole of the argument for coercing here ("a millisecond value
            // widens to microseconds without loss"), and it does not extend to this.
            if rank(hu) > rank(wu) {
                return Ok(None);
            }
            match (wtz.as_deref(), htz.as_deref()) {
                // Declared UTC-adjusted and the file is too, whatever it calls its zone: an
                // Arrow timezone is a display label on a count from the epoch, so this
                // changes the unit and nothing else.
                (Some(_), Some(_)) => Ok(Some(want.clone())),
                // Declared UTC-adjusted, file naive. This is the Trino case, and reading
                // the absent zone as UTC is not a guess: Delta's `timestamp` is
                // UTC-adjusted by definition, so UTC is the only thing it can mean.
                //
                // Guarded on the declared zone actually being UTC, because Arrow's cast
                // from a naive timestamp to a zoned one preserves the *wall clock* rather
                // than the instant — it subtracts that zone's offset. Against UTC that is
                // arithmetically nothing; against anything else it would move every value
                // by hours, silently. Delta never declares another zone, so this arm is
                // unreachable in practice and exists to keep it that way.
                (Some(tz), None) if is_utc(tz) => Ok(Some(want.clone())),
                (Some(tz), None) => Err(format!(
                    "the file holds {have} with no timezone and the table's schema declares \
                     {want}. Reading it at the declared type would shift every value by \
                     {tz:?}'s offset to keep the wall clock, which is not what Delta's \
                     UTC-adjusted `timestamp` means, so it is refused rather than guessed."
                )),
                // Declared `timestamp_ntz`, and the file agrees. Widen the unit only.
                (None, None) => Ok(Some(want.clone())),
                // Declared `timestamp_ntz`, file zoned. Dropping a UTC label leaves the same
                // number and the same meaning. Dropping any other one turns an instant into
                // a wall clock in a zone nobody named, which is a change of meaning even
                // though no digit moves — so it stops here.
                (None, Some(tz)) if is_utc(tz) => Ok(Some(want.clone())),
                (None, Some(tz)) => Err(format!(
                    "the file holds {have} but the table's schema declares {want}, which is \
                     a wall clock. Reading a {tz:?} instant as one would change what the \
                     value means, so it is refused rather than guessed."
                )),
            }
        }
        (Struct(want_fields), Struct(have_fields)) => {
            let mut fields: Vec<Arc<Field>> = Vec::with_capacity(have_fields.len());
            let mut changed = false;
            for h in have_fields {
                let w = want_fields.iter().find(|w| w.name() == h.name());
                match w
                    .map(|w| widened(w.data_type(), h.data_type()))
                    .transpose()?
                {
                    Some(Some(dt)) => {
                        changed = true;
                        fields.push(Arc::new(h.as_ref().clone().with_data_type(dt)));
                    }
                    _ => fields.push(h.clone()),
                }
            }
            Ok(changed.then(|| Struct(fields.into())))
        }
        (List(w), List(h)) => Ok(widened_item(w, h)?.map(List)),
        (LargeList(w), LargeList(h)) => Ok(widened_item(w, h)?.map(LargeList)),
        (ListView(w), ListView(h)) => Ok(widened_item(w, h)?.map(ListView)),
        (LargeListView(w), LargeListView(h)) => Ok(widened_item(w, h)?.map(LargeListView)),
        (FixedSizeList(w, _), FixedSizeList(h, n)) => {
            Ok(widened_item(w, h)?.map(|f| FixedSizeList(f, *n)))
        }
        (Map(w, _), Map(h, sorted)) => Ok(widened_item(w, h)?.map(|f| Map(f, *sorted))),
        // Everything else is left as it arrived, so a genuine mismatch is reported by
        // whoever would have reported it before this function existed, in the same words.
        _ => Ok(None),
    }
}

fn widened_item(
    want: &Arc<Field>,
    have: &Arc<Field>,
) -> std::result::Result<Option<Arc<Field>>, String> {
    Ok(widened(want.data_type(), have.data_type())?
        .map(|dt| Arc::new(have.as_ref().clone().with_data_type(dt))))
}

/// How much a unit resolves, so "coarser" and "finer" can be compared.
fn rank(u: &deltalake::arrow::datatypes::TimeUnit) -> u8 {
    use deltalake::arrow::datatypes::TimeUnit::*;
    match u {
        Second => 0,
        Millisecond => 1,
        Microsecond => 2,
        Nanosecond => 3,
    }
}

/// The spellings of UTC that Arrow and the engines writing these files actually use.
fn is_utc(tz: &str) -> bool {
    tz.eq_ignore_ascii_case("utc")
        || tz.eq_ignore_ascii_case("z")
        || tz == "+00:00"
        || tz == "-00:00"
        || tz == "00:00"
        || tz.eq_ignore_ascii_case("etc/utc")
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
        assert_eq!(bad.columns, vec![Some("amount".to_string())]);
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
        assert_eq!(bad.columns, vec![Some("id".to_string())]);
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
mod read_as_declared_tests {
    use super::*;
    use deltalake::arrow::datatypes::TimeUnit;

    fn ts(unit: TimeUnit, tz: Option<&str>) -> DataType {
        DataType::Timestamp(unit, tz.map(Into::into))
    }

    /// Delta's `timestamp`, as the kernel spells it in Arrow.
    fn declared() -> DataType {
        ts(TimeUnit::Microsecond, Some("UTC"))
    }

    #[test]
    fn a_coarser_unit_is_widened_to_the_declared_one() {
        use TimeUnit::*;
        // What Trino writes: milliseconds, and no timezone at all.
        for have in [
            ts(Millisecond, None),
            ts(Second, None),
            ts(Millisecond, Some("UTC")),
            // An Arrow timezone labels a count from the epoch, so relabelling it moves
            // nothing — only the unit changes.
            ts(Millisecond, Some("Europe/Berlin")),
        ] {
            assert_eq!(
                widened(&declared(), &have),
                Ok(Some(declared())),
                "{have} should widen"
            );
        }
    }

    #[test]
    fn an_agreeing_column_asks_for_no_work_at_all() {
        assert_eq!(widened(&declared(), &declared()), Ok(None));
        assert_eq!(widened(&DataType::Int64, &DataType::Int64), Ok(None));
    }

    #[test]
    fn a_finer_unit_is_left_alone_rather_than_narrowed_here() {
        // Not a refusal — a hand-off. Spark writes Delta timestamps as INT96, which the
        // parquet reader decodes as `Timestamp(ns)` however the table is declared, so on a
        // Spark-written lakehouse this is every file rather than an oddity. Widening is the
        // only thing this function claims; a narrowing goes to the coercer that has always
        // done it, and refusing it here would stall those pipelines outright.
        assert_eq!(
            widened(&declared(), &ts(TimeUnit::Nanosecond, None)),
            Ok(None)
        );
        assert_eq!(
            widened(
                &ts(TimeUnit::Millisecond, None),
                &ts(TimeUnit::Microsecond, None)
            ),
            Ok(None)
        );
    }

    #[test]
    fn a_declared_zone_that_is_not_utc_never_shifts_a_naive_value() {
        // Arrow's cast from naive to zoned keeps the *wall clock*, subtracting the zone's
        // offset — so allowing this would move every value by hours without a word. Delta
        // never declares such a column; this is the guard that keeps it that way.
        let e = widened(
            &ts(TimeUnit::Microsecond, Some("Europe/Berlin")),
            &ts(TimeUnit::Millisecond, None),
        )
        .unwrap_err();
        assert!(e.contains("shift"), "got: {e}");
    }

    #[test]
    fn timestamp_ntz_keeps_meaning_what_it_means() {
        let ntz = ts(TimeUnit::Microsecond, None);
        // A naive file widens into a naive column.
        assert_eq!(
            widened(&ntz, &ts(TimeUnit::Millisecond, None)),
            Ok(Some(ntz.clone()))
        );
        // A UTC instant read as a wall clock is the same number and the same meaning.
        assert_eq!(
            widened(&ntz, &ts(TimeUnit::Millisecond, Some("UTC"))),
            Ok(Some(ntz.clone()))
        );
        // Any other zone is a change of meaning, digits or no digits.
        let e = widened(&ntz, &ts(TimeUnit::Millisecond, Some("Asia/Tokyo"))).unwrap_err();
        assert!(e.contains("what the value means"), "got: {e}");
    }

    #[test]
    fn anything_that_is_not_two_timestamps_is_left_exactly_as_it_arrived() {
        // Not this function's to refuse: leaving it alone is what makes the failure land
        // downstream, in the words it has always been reported in.
        assert_eq!(widened(&declared(), &DataType::Utf8), Ok(None));
        assert_eq!(widened(&DataType::Int64, &DataType::Utf8), Ok(None));
        assert_eq!(widened(&DataType::Int64, &DataType::Int32), Ok(None));
    }

    #[test]
    fn a_timestamp_nested_in_a_struct_is_widened_too() {
        // An OPTIMIZE rewrites every column of the file, including the ones inside a struct.
        let inner = |unit| {
            DataType::Struct(
                vec![
                    Field::new("at", ts(unit, None), true),
                    Field::new("who", DataType::Utf8, true),
                ]
                .into(),
            )
        };
        let want = DataType::Struct(
            vec![
                Field::new("at", declared(), true),
                Field::new("who", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(
            widened(&want, &inner(TimeUnit::Millisecond)),
            Ok(Some(DataType::Struct(
                vec![
                    Field::new("at", declared(), true),
                    Field::new("who", DataType::Utf8, true),
                ]
                .into()
            )))
        );
    }

    #[test]
    fn a_timestamp_inside_any_of_the_collection_types_is_widened_too() {
        // One arm each, because an `OPTIMIZE` rewrites whatever the column happens to be
        // and a missing arm would silently leave that shape at the wrong precision.
        let item = |unit, tz| Arc::new(Field::new("element", ts(unit, tz), true));
        let want = item(TimeUnit::Microsecond, Some("UTC"));
        let have = item(TimeUnit::Millisecond, None);

        let shapes: Vec<(DataType, DataType)> = vec![
            (DataType::List(want.clone()), DataType::List(have.clone())),
            (
                DataType::LargeList(want.clone()),
                DataType::LargeList(have.clone()),
            ),
            (
                DataType::ListView(want.clone()),
                DataType::ListView(have.clone()),
            ),
            (
                DataType::LargeListView(want.clone()),
                DataType::LargeListView(have.clone()),
            ),
            (
                DataType::FixedSizeList(want.clone(), 2),
                DataType::FixedSizeList(have.clone(), 2),
            ),
        ];
        for (want, have) in shapes {
            assert_eq!(
                widened(&want, &have),
                Ok(Some(want.clone())),
                "{have} should widen to {want}"
            );
        }

        // A map's timestamp lives in the value half of its entries struct.
        let entries = |unit, tz| {
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", ts(unit, tz), true),
                    ]
                    .into(),
                ),
                false,
            ))
        };
        assert_eq!(
            widened(
                &DataType::Map(entries(TimeUnit::Microsecond, Some("UTC")), false),
                &DataType::Map(entries(TimeUnit::Millisecond, None), false),
            ),
            Ok(Some(DataType::Map(
                entries(TimeUnit::Microsecond, Some("UTC")),
                false
            )))
        );
    }

    #[test]
    fn a_column_the_table_does_not_declare_is_not_touched() {
        // A projection hands back a subset, and a file may carry a column the schema has
        // since dropped. Neither is this function's business.
        let file = Schema::new(vec![
            Field::new("kept", ts(TimeUnit::Millisecond, None), true),
            Field::new("stranger", ts(TimeUnit::Millisecond, None), true),
        ]);
        let table = Schema::new(vec![Field::new("kept", declared(), true)]);
        let got = aligned_schema(&table, &file)
            .unwrap()
            .expect("kept changes");
        assert_eq!(got.field(0).data_type(), &declared());
        assert_eq!(
            got.field(1).data_type(),
            &ts(TimeUnit::Millisecond, None),
            "a column nobody declared keeps whatever the file gave it"
        );
    }

    #[test]
    fn a_nested_timestamp_widens_its_values_too_not_just_its_type() {
        // Computing the right nested type is only half of it: Arrow has to be willing to
        // push the cast down into a struct's children and come back with the values moved.
        // If it ever stops doing that, the schema tests above would still pass and the data
        // would be wrong, so this asserts the numbers.
        use deltalake::arrow::array::{AsArray, StructArray, TimestampMillisecondArray};
        use deltalake::arrow::datatypes::{Fields, TimestampMicrosecondType};

        let ms = 1_770_000_000_000i64;
        let child: Fields = vec![Field::new("at", ts(TimeUnit::Millisecond, None), true)].into();
        let event = StructArray::new(
            child.clone(),
            vec![Arc::new(TimestampMillisecondArray::from(vec![ms])) as ArrayRef],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "event",
                DataType::Struct(child),
                true,
            )])),
            vec![Arc::new(event) as ArrayRef],
        )
        .unwrap();

        let table = Schema::new(vec![Field::new(
            "event",
            DataType::Struct(vec![Field::new("at", declared(), true)].into()),
            true,
        )]);
        let out = read_as_declared(batch, &table).unwrap();
        let at = out.column(0).as_struct().column(0);
        assert_eq!(at.data_type(), &declared());
        assert_eq!(
            at.as_primitive::<TimestampMicrosecondType>().value(0),
            ms * 1000
        );
    }

    #[test]
    fn the_values_widen_by_exactly_a_thousand_and_do_not_move() {
        use deltalake::arrow::array::{AsArray, TimestampMillisecondArray};
        use deltalake::arrow::datatypes::TimestampMicrosecondType;

        // 2026-02-02T02:40:00Z, far enough from the epoch that a factor-of-1000 slip lands
        // in 1970 rather than merely a little early.
        let ms = 1_770_000_000_000i64;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "at",
                ts(TimeUnit::Millisecond, None),
                true,
            )])),
            vec![Arc::new(TimestampMillisecondArray::from(vec![ms])) as ArrayRef],
        )
        .unwrap();

        let table = Schema::new(vec![Field::new("at", declared(), true)]);
        let out = read_as_declared(batch, &table).unwrap();
        assert_eq!(out.column(0).data_type(), &declared());
        assert_eq!(
            out.column(0)
                .as_primitive::<TimestampMicrosecondType>()
                .value(0),
            ms * 1000,
            "the absent timezone means UTC, so the value widens and stays put"
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
    fn a_decimal_lands_in_a_double_column_as_the_nearest_double() {
        // A DECIMAL(38,17) column into a DOUBLE or REAL target: the value Trino would store,
        // not Arrow's double-rounded division. Through both paths, and inside a list.
        use deltalake::arrow::array::{AsArray, Decimal128Array, ListArray};
        use deltalake::arrow::buffer::OffsetBuffer;
        use deltalake::arrow::datatypes::{Float32Type, Float64Type};

        let texts = [
            "0.49979999999999997",
            "0.9017000000000001",
            "0.45909999999999995",
            "0.40380000000000005",
        ];
        let decimals = || {
            Decimal128Array::from(vec![
                49979999999999997i128,
                90170000000000010,
                45909999999999995,
                40380000000000005,
            ])
            .with_precision_and_scale(38, 17)
            .unwrap()
        };
        let src = schema(vec![Field::new("v", DataType::Decimal128(38, 17), false)]);
        let b = batch(src, vec![Arc::new(decimals())]);

        let doubles = schema(vec![Field::new("v", DataType::Float64, false)]);
        let strict = SchemaCoercer::new(doubles.clone()).coerce(&b).unwrap();
        let lenient = SchemaCoercer::new(doubles).coerce_quarantining(&b).unwrap();
        assert!(
            lenient.bad.is_none(),
            "nothing is lost, so nothing is set aside"
        );
        for out in [&strict, &lenient.good] {
            let got = out.column(0).as_primitive::<Float64Type>();
            for (i, text) in texts.iter().enumerate() {
                assert_eq!(
                    got.value(i).to_bits(),
                    text.parse::<f64>().unwrap().to_bits(),
                    "{text} became {:?}",
                    got.value(i)
                );
            }
        }

        let reals = schema(vec![Field::new("v", DataType::Float32, false)]);
        let out = SchemaCoercer::new(reals).coerce(&b).unwrap();
        let got = out.column(0).as_primitive::<Float32Type>();
        for (i, text) in texts.iter().enumerate() {
            assert_eq!(
                got.value(i).to_bits(),
                text.parse::<f32>().unwrap().to_bits(),
                "{text} became {:?}",
                got.value(i)
            );
        }

        let item = |t: DataType| Arc::new(Field::new("item", t, true));
        let list = ListArray::new(
            item(DataType::Decimal128(38, 17)),
            OffsetBuffer::new(vec![0, 4].into()),
            Arc::new(decimals()),
            None,
        );
        let src = schema(vec![Field::new(
            "v",
            DataType::List(item(DataType::Decimal128(38, 17))),
            false,
        )]);
        let tgt = schema(vec![Field::new(
            "v",
            DataType::List(item(DataType::Float64)),
            false,
        )]);
        let out = SchemaCoercer::new(tgt)
            .coerce(&batch(src, vec![Arc::new(list)]))
            .unwrap();
        let got = out.column(0).as_list::<i32>().value(0);
        let got = got.as_primitive::<Float64Type>();
        for (i, text) in texts.iter().enumerate() {
            assert_eq!(
                got.value(i).to_bits(),
                text.parse::<f64>().unwrap().to_bits()
            );
        }
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

    /// The ids of `out`'s column `a`, which these tests make BIGINT.
    fn ids(out: &RecordBatch) -> Vec<Option<i64>> {
        out.column_by_name("a")
            .unwrap()
            .as_primitive::<deltalake::arrow::datatypes::Int64Type>()
            .iter()
            .collect()
    }

    #[test]
    fn a_fraction_going_into_an_integer_is_refused_not_truncated() {
        // Arrow's cast writes 1.5 as 1 and -0.5 as 0, with no error and no NULL to notice.
        let src = schema(vec![Field::new("a", DataType::Float64, true)]);
        let tgt = schema(vec![Field::new("a", DataType::Int64, true)]);
        let b = batch(
            src,
            vec![Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(1.5),
                Some(-2.0),
                Some(-0.5),
                None,
            ]))],
        );

        let err = SchemaCoercer::new(tgt.clone()).coerce(&b).unwrap_err();
        assert!(err.to_string().contains("row 1 holds"), "got: {err}");

        let split = SchemaCoercer::new(tgt.clone())
            .coerce_quarantining(&b)
            .unwrap();
        assert_eq!(ids(&split.good), [Some(1), Some(-2), None]);
        let bad = split.bad.unwrap();
        assert_eq!(bad.len(), 2);
        assert_eq!(bad.columns, [Some("a".to_string()), Some("a".to_string())]);
        assert!(
            bad.reasons[0].contains("has a fraction"),
            "{:?}",
            bad.reasons
        );
        // The rows as they arrived, with the fraction they had.
        let kept = bad.rows.column(0).as_primitive::<Float64Type>();
        assert_eq!(kept.values().to_vec(), [1.5, -0.5]);

        // A Kafka pipeline's coercer truncates, as kafka-delta-ingest's arrow-json did.
        let kafka = SchemaCoercer::truncating_float_fractions(tgt);
        assert_eq!(
            ids(&kafka.coerce(&b).unwrap()),
            [Some(1), Some(1), Some(-2), Some(0), None]
        );
        let split = kafka.coerce_quarantining(&b).unwrap();
        assert!(split.bad.is_none());
        assert_eq!(split.good.num_rows(), 5);
    }

    #[test]
    fn a_decimal_with_a_fraction_going_into_an_integer_is_refused() {
        use deltalake::arrow::array::Decimal128Array;
        let src = schema(vec![Field::new("a", DataType::Decimal128(10, 2), true)]);
        let tgt = schema(vec![Field::new("a", DataType::Int64, true)]);
        // 1.00, 1.50, -2.50, NULL, 120.00
        let decimals =
            Decimal128Array::from(vec![Some(100), Some(150), Some(-250), None, Some(12000)])
                .with_precision_and_scale(10, 2)
                .unwrap();
        let b = batch(src, vec![Arc::new(decimals)]);
        // A Kafka pipeline's coercer too: kafka-delta-ingest never wrote a decimal into an
        // integer column, so there is no truncation of one to keep.
        for coercer in [
            SchemaCoercer::new(tgt.clone()),
            SchemaCoercer::truncating_float_fractions(tgt.clone()),
        ] {
            let err = coercer.coerce(&b).unwrap_err();
            assert!(err.to_string().contains("row 1 holds"), "got: {err}");
            let split = coercer.coerce_quarantining(&b).unwrap();
            assert_eq!(ids(&split.good), [Some(1), None, Some(120)]);
            assert_eq!(split.bad.unwrap().len(), 2);
        }
    }

    #[test]
    fn whole_floats_and_decimals_still_go_into_an_integer() {
        use deltalake::arrow::array::{Decimal128Array, Float32Array};
        let src = schema(vec![
            Field::new("a", DataType::Float32, false),
            Field::new("d", DataType::Decimal128(38, 38), false),
        ]);
        let tgt = schema(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("d", DataType::Int64, false),
        ]);
        let b = batch(
            src,
            vec![
                Arc::new(Float32Array::from(vec![3.0, -0.0, 1e10])),
                // At scale 38 only 0 is whole, and 10^38 itself still fits an i128.
                Arc::new(
                    Decimal128Array::from(vec![0, 0, 0])
                        .with_precision_and_scale(38, 38)
                        .unwrap(),
                ),
            ],
        );
        let out = SchemaCoercer::new(tgt.clone()).coerce(&b).unwrap();
        assert_eq!(ids(&out), [Some(3), Some(0), Some(10_000_000_000)]);
        assert!(SchemaCoercer::new(tgt)
            .coerce_quarantining(&b)
            .unwrap()
            .bad
            .is_none());
    }

    #[test]
    fn a_fraction_is_found_at_any_depth_where_the_cast_puts_it() {
        use deltalake::arrow::array::{
            DictionaryArray, FixedSizeListArray, ListArray, MapArray, StructArray,
        };
        use deltalake::arrow::buffer::{NullBuffer, OffsetBuffer};
        use deltalake::arrow::datatypes::Int32Type;

        let floats = |v: Vec<f64>| -> ArrayRef { Arc::new(Float64Array::from(v)) };
        let list_of = |t: DataType| DataType::List(Arc::new(Field::new_list_field(t, true)));
        // Per row of `col`, whether going into `to` drops a fraction.
        let check = |col: ArrayRef, to: DataType| fractions_dropped(col.as_ref(), &to).unwrap();

        // A list's elements in place; the null row's 1.5 is never written, so it is not judged.
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::from_lengths([2, 2, 2]),
            floats(vec![1.0, 2.0, 1.0, 2.5, 1.5, 3.0]),
            Some(NullBuffer::from(vec![true, true, false])),
        );
        assert_eq!(
            check(Arc::new(list), list_of(DataType::Int64)),
            [false, true, false]
        );

        // A struct's fields by name when every target field is in the source...
        let fields = Fields::from(vec![
            Field::new("a", DataType::Float64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let record = StructArray::new(
            fields,
            vec![
                floats(vec![1.5, 2.0]),
                Arc::new(StringArray::from(vec!["x", "y"])),
            ],
            None,
        );
        let swapped = DataType::Struct(Fields::from(vec![
            Field::new("b", DataType::Utf8, true),
            Field::new("a", DataType::Int64, true),
        ]));
        assert_eq!(check(Arc::new(record.clone()), swapped), [true, false]);
        // ...and by position when one is not, so `a`'s values go into `n`.
        let renamed = DataType::Struct(Fields::from(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        assert_eq!(check(Arc::new(record), renamed), [true, false]);

        // A map's values by position.
        let entries = Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Float64, true),
        ]);
        let map = MapArray::new(
            Arc::new(Field::new("entries", DataType::Struct(entries), false)),
            OffsetBuffer::from_lengths([1, 1]),
            StructArray::new(
                Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Float64, true),
                ]),
                vec![
                    Arc::new(StringArray::from(vec!["k", "k"])),
                    floats(vec![2.0, 0.25]),
                ],
                None,
            ),
            None,
            false,
        );
        let into = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        );
        assert_eq!(check(Arc::new(map), into), [false, true]);

        // A dictionary through its values.
        let dict = DictionaryArray::<Int32Type>::new(vec![1, 0, 1].into(), floats(vec![4.0, 4.5]));
        assert_eq!(check(Arc::new(dict), DataType::Int64), [true, false, true]);

        // A plain value wrapped into a one-element list, and one unwrapped from one, whose
        // null row the cast writes as the value it holds.
        assert_eq!(
            check(floats(vec![7.0, 7.5]), list_of(DataType::Int64)),
            [false, true]
        );
        let single = FixedSizeListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            1,
            floats(vec![0.5, 8.0]),
            Some(NullBuffer::from(vec![false, true])),
        );
        assert_eq!(check(Arc::new(single), DataType::Int64), [true, false]);

        // Nothing to look for: same types, or no float or decimal going into an integer.
        assert!(fractions_dropped(&Float64Array::from(vec![1.5]), &DataType::Float64).is_none());
        assert!(fractions_dropped(&Float64Array::from(vec![1.5]), &DataType::Utf8).is_none());
    }

    #[test]
    fn a_fraction_is_found_through_runs_unions_and_into_timestamps() {
        use deltalake::arrow::array::{Decimal128Array, ListArray, RunArray, UnionArray};
        use deltalake::arrow::buffer::OffsetBuffer;
        use deltalake::arrow::datatypes::TimeUnit;

        let floats = |v: Vec<f64>| -> ArrayRef { Arc::new(Float64Array::from(v)) };
        let check = |col: ArrayRef, to: DataType| fractions_dropped(col.as_ref(), &to).unwrap();

        // Into a timestamp or a duration the cast goes through Int64, and truncates on the way.
        let utc = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
        assert_eq!(check(floats(vec![1.5, 2.0]), utc), [true, false]);
        let cents: ArrayRef = Arc::new(
            Decimal128Array::from(vec![150, 200])
                .with_precision_and_scale(10, 2)
                .unwrap(),
        );
        assert_eq!(
            check(cents, DataType::Duration(TimeUnit::Second)),
            [true, false]
        );

        // Runs of 2, 1 and 2 rows, sliced to start inside the first.
        let runs = RunArray::<Int32Type>::try_new(
            &vec![2, 3, 5].into(),
            &Float64Array::from(vec![1.0, 1.5, 2.0]),
        )
        .unwrap();
        assert_eq!(
            check(Arc::new(runs.clone()), DataType::Int64),
            [false, false, true, false, false]
        );
        assert_eq!(
            check(Arc::new(runs.slice(1, 3)), DataType::Int64),
            [false, true, false]
        );

        // A union: the float child is the first that casts to an integer, so it is extracted,
        // and a row of the text child becomes NULL.
        let fields = UnionFields::try_new(
            [0, 1],
            [
                Field::new("f", DataType::Float64, true),
                Field::new("s", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let text: ArrayRef = Arc::new(StringArray::from(vec!["7.5", "7.5", "7.5"]));
        let sparse = UnionArray::try_new(
            fields.clone(),
            vec![0, 1, 0].into(),
            None,
            vec![floats(vec![0.5, 9.5, 3.0]), text],
        )
        .unwrap();
        assert_eq!(
            check(Arc::new(sparse), DataType::Int64),
            [true, false, false]
        );
        let dense = UnionArray::try_new(
            fields,
            vec![1, 0, 0].into(),
            Some(vec![0, 1, 0].into()),
            vec![
                floats(vec![3.0, 0.25]),
                Arc::new(StringArray::from(vec!["7.5"])),
            ],
        )
        .unwrap();
        assert_eq!(
            check(Arc::new(dense), DataType::Int64),
            [false, true, false]
        );

        // A dictionary with no values, which is how an all-null one is made, at the top level
        // and inside a list: nothing to refuse, and no panic.
        let dictionary =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Float64));
        let nulls = new_null_array(&dictionary, 3);
        assert_eq!(check(nulls.clone(), DataType::Int64), [false; 3]);
        let list = ListArray::new(
            Arc::new(Field::new_list_field(dictionary, true)),
            OffsetBuffer::from_lengths([3]),
            nulls,
            None,
        );
        let into = DataType::List(Arc::new(Field::new_list_field(DataType::Int64, true)));
        assert_eq!(check(Arc::new(list), into), [false]);
    }

    #[test]
    fn a_nested_fraction_sets_its_row_aside() {
        use deltalake::arrow::array::ListArray;
        use deltalake::arrow::buffer::OffsetBuffer;

        let item = |t: DataType| Arc::new(Field::new_list_field(t, true));
        let src = schema(vec![Field::new(
            "a",
            DataType::List(item(DataType::Float64)),
            true,
        )]);
        let tgt = schema(vec![Field::new(
            "a",
            DataType::List(item(DataType::Int64)),
            true,
        )]);
        let list = ListArray::new(
            item(DataType::Float64),
            OffsetBuffer::from_lengths([2, 1]),
            Arc::new(Float64Array::from(vec![1.0, 2.0, 2.5])),
            None,
        );
        let b = batch(src, vec![Arc::new(list)]);
        let err = SchemaCoercer::new(tgt.clone()).coerce(&b).unwrap_err();
        assert!(err.to_string().contains("row 1 holds"), "got: {err}");
        let split = SchemaCoercer::new(tgt).coerce_quarantining(&b).unwrap();
        assert_eq!(split.good.num_rows(), 1);
        assert_eq!(split.bad.unwrap().len(), 1);
    }
}
