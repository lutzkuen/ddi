//! Fitting decoded rows to what reads them (docs/kafka-design.md §5.5, §5.6).
//!
//! # Two places rows are fitted
//!
//! - **Into the union `U`, before the SQL** ([`UnionSchema`], §5.5). A decoded batch holds the
//!   rows of one writer schema, and `transform_sql` runs over one table with one schema, so
//!   every batch is aligned to `U` first: the fields of the newest writer schema, then those
//!   only older ones have, newest first, each with the type of the newest writer schema that
//!   has it, and the Kafka metadata columns after them. An older row is cast to that type, and
//!   one that does not convert goes to the data-quality table; a field its writer schema lacks
//!   is null. Without SQL nothing is aligned: batches of different writer schemas go through
//!   `Identity` as they are, and are conformed to the target one by one.
//! - **Into the target, after the transform and before the coercer** ([`Conformer`], §5.6). A
//!   column goes to the target column of exactly its name, case included, and so on at every
//!   depth: a struct's children by name, a list's elements and a map's keys and values in
//!   place, the containers rebuilt with the target's own field names (`element`, `key_value`,
//!   `key`, `value`). A general union takes the branch each row holds, a float goes into a
//!   decimal through its shortest text, and a row whose value does not fit goes to the
//!   data-quality table, in the same write as the batch's other rejects.
//!
//! # Why by name
//!
//! Arrow's cast of a struct into a struct pairs the fields by name only when every target field
//! is in the source, and otherwise by position (arrow-cast cast/mod.rs:2223-2252, reached from
//! the coercer through cast/mod.rs:1183): a record that gained or lost a field would have its
//! values written into its neighbours' columns, silently. So nothing nested reaches that cast.
//! A conformed column already has the target's type wherever it is nested, and the coercer
//! passes it through as it is.
//!
//! # What is left to the coercer
//!
//! A top-level column of a plain type: the coercer casts it to the target's type, and sets
//! aside the rows that do not convert, as it does for a Delta source (schema.rs:151-275). And a
//! target column nothing produces: nullable, it is null there (warned about here first);
//! NOT NULL, the coercer refuses the batch, as it would for any source.
//!
//! Inside a nested column it is the other way round. The coercer casts a nested column strictly,
//! failing the batch on the first bad value rather than setting the row aside (schema.rs:161-173:
//! Arrow pushes a lenient cast into the children and keeps the parent valid). So here a nested
//! value that does not convert, and a null where the target's nested field is NOT NULL, rejects
//! its row, and the coercer never sees either.
//!
//! # kafka-delta-ingest's text
//!
//! `kdi_compat` and `kdi_null_text` (§5.6 steps 5 and 6) write values going into a STRING
//! column the way kafka-delta-ingest wrote them: a value that is not text as its JSON, by its
//! Arrow type at every depth, and a null as the text `"null"` exactly where kdi's JSON had the
//! key, which the row's own writer schema says ([`crate::kafka::kdi_text`]). So a conformer that
//! writes either is told every writer schema its rows can come from ([`Conformer::observe`]).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use deltalake::arrow::array::{
    new_empty_array, new_null_array, Array, ArrayRef, AsArray, BooleanArray, Decimal128Array,
    Decimal256Array, ListArray, MapArray, NullArray, RecordBatch, StringArray, StringBuilder,
    StructArray, UnionArray,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::compute::kernels::cast_utils::parse_decimal;
use deltalake::arrow::compute::kernels::zip::zip;
use deltalake::arrow::compute::{
    can_cast_types, concat, filter_record_batch, interleave, is_not_null, CastOptions,
};
use deltalake::arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Field, FieldRef, Fields, Float16Type, Float32Type,
    Float64Type, Int32Type, Schema, SchemaRef, UnionFields, UnionMode,
};
use deltalake::arrow::error::ArrowError;
use tracing::warn;

use crate::error::{Error, Result};
use crate::kafka::decode::{decoded_schema, metadata_fields};
use crate::kafka::kdi_text::{self, At, Shape};
use crate::kafka::registry::RegisteredSchema;
use crate::schema::Rejected;
use crate::transform::SqlTransform;

// ================================================================== fitting one column

/// What fitting an array to a type made of it.
struct Fitted {
    /// The array, in exactly the type it was fitted to.
    array: ArrayRef,
    /// Why each value was rejected, where one was: one entry per value of `array`, or none at
    /// all when no value was. A rejected value's slot in `array` holds a null or a placeholder;
    /// its row goes to the data-quality table and is never written.
    bad: Vec<Option<String>>,
}

impl Fitted {
    fn clean(array: ArrayRef) -> Self {
        Self {
            array,
            bad: Vec::new(),
        }
    }

    fn reason(&self, i: usize) -> Option<&String> {
        self.bad.get(i).and_then(Option::as_ref)
    }
}

/// Per-value reasons, allocated on the first one.
#[derive(Default)]
struct Reasons(Vec<Option<String>>);

impl Reasons {
    fn set(&mut self, n: usize, i: usize, reason: impl FnOnce() -> String) {
        if self.0.is_empty() {
            self.0 = vec![None; n];
        }
        if self.0[i].is_none() {
            self.0[i] = Some(reason());
        }
    }
}

/// What a value is fitted into, which is what a rejection is described against.
#[derive(Clone, Copy, Debug)]
enum Dest {
    /// The target table (§5.6).
    Target,
    /// The union `U` (§5.5): the writer schema the rows were written with, when they say, and
    /// the writer schema the field's type in `U` comes from.
    Union { writer: Option<i32>, newest: u32 },
}

/// Where each value of an array stood in kafka-delta-ingest's JSON (§5.6 step 6), and whether
/// kdi's coercion reached that place: given when the conformer writes kdi's text.
#[derive(Clone, Copy)]
struct Kdi<'a, 's> {
    /// One per value of the array being fitted.
    at: &'a [At<'s>],
    /// kdi's coercion reached here: not inside a map, nor an array of arrays or maps.
    reach: bool,
}

/// Fits arrays to types, by name at every depth.
struct Fitter {
    into: Dest,
    /// kafka-delta-ingest's text, for values going into a STRING (§5.6 steps 5 and 6). Off
    /// when fitting to `U`: only the target's STRING columns take it.
    text: KdiText,
}

impl Fitter {
    /// `array` in type `to`, its values matched to `to`'s by name at every depth. `path` names
    /// the value in a rejection (`customer.address.zip`). `top` is set for a top-level column of
    /// the target, whose plain values are left as they are, for the coercer to cast. `kdi` is
    /// where each value stood in kdi's JSON, given when kdi's text is written.
    fn fit(
        &self,
        array: &ArrayRef,
        to: &DataType,
        path: &str,
        top: bool,
        kdi: Option<Kdi<'_, '_>>,
    ) -> Result<Fitted> {
        let from = array.data_type();
        // Equal types need nothing, unless a null in them may become "null", or a map's
        // entries be put in kdi's order.
        let kdi_work = kdi.is_some_and(|k| {
            (self.text.null_text && kdi_text::takes_null_text(to, k.reach))
                || (self.text.compat && kdi_text::has_map(to))
        });
        if from == to && !kdi_work {
            return Ok(Fitted::clean(array.clone()));
        }
        match (from, to) {
            (DataType::Union(..), DataType::Union(..)) => self.union_to_union(array, to, path),
            (DataType::Union(..), _) => self.union_to(array, to, path, kdi),
            (_, DataType::Union(..)) => self.wrap_in_union(array, to, path),
            // §5.6 steps 5 and 6: kafka-delta-ingest's text, by the value's Arrow type.
            (_, DataType::Utf8) if kdi.is_some() => {
                self.kdi_text(array, path, top, kdi.expect("matched as some"))
            }
            (DataType::Null, _) => Ok(Fitted::clean(new_null_array(to, array.len()))),
            (DataType::Struct(_), DataType::Struct(to_fields)) => {
                self.structs(array, to_fields, path, kdi)
            }
            (DataType::List(_), DataType::List(item)) => self.list(array, item, path, kdi),
            (DataType::Map(..), DataType::Map(entries, sorted)) => {
                self.map(array, entries, *sorted, path, kdi)
            }
            (
                DataType::Float16 | DataType::Float32 | DataType::Float64,
                DataType::Decimal128(..) | DataType::Decimal256(..),
            ) => self.float_to_decimal(array, to, path),
            _ if top => Ok(Fitted::clean(array.clone())),
            _ => self.cast(array, to, path),
        }
    }

    /// A value going into a STRING where kafka-delta-ingest's text is written (§5.6 steps 5 and
    /// 6). Text stays as it is. Under `kdi_compat` anything else is its kdi text, by its Arrow
    /// type ([`kdi_text::text`]), and a value that has none (NaN, the infinities) rejects its
    /// row; without it, Arrow's cast as anywhere else. Under `kdi_null_text` a null becomes
    /// `"null"` where kdi's JSON had the key and its coercion reached.
    fn kdi_text(
        &self,
        array: &ArrayRef,
        path: &str,
        top: bool,
        kdi: Kdi<'_, '_>,
    ) -> Result<Fitted> {
        let n = array.len();
        let null_word = |i: usize| self.text.null_text && kdi.reach && kdi.at[i].key;
        let textual =
            |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
        let from = array.data_type();
        let is_text = textual(from) || matches!(from, DataType::Dictionary(_, v) if textual(v));
        if is_text || !self.text.compat {
            let fitted = if from == &DataType::Utf8 {
                Fitted::clean(array.clone())
            } else if top && !(0..n).any(|i| null_word(i) && kdi_text::is_null(array.as_ref(), i)) {
                // Nothing to write "null" into: the coercer casts it, as without the options.
                return Ok(Fitted::clean(array.clone()));
            } else {
                self.cast(array, &DataType::Utf8, path)?
            };
            return Ok(with_null_word(fitted, null_word));
        }
        let mut out = StringBuilder::with_capacity(n, n * 8);
        let mut bad = Reasons::default();
        for i in 0..n {
            if kdi_text::is_null(array.as_ref(), i) {
                if null_word(i) {
                    out.append_value("null");
                } else {
                    out.append_null();
                }
                continue;
            }
            match kdi_text::text(array.as_ref(), i, kdi.at[i].shape) {
                Ok(text) => out.append_value(text),
                Err(why) => {
                    out.append_null();
                    bad.set(n, i, || format!("{path}: {why}"));
                }
            }
        }
        Ok(Fitted {
            array: Arc::new(out.finish()),
            bad: bad.0,
        })
    }

    /// A plain value cast to `to`, leniently: a value the cast turns into a null is rejected.
    /// One with no cast at all is rejected wherever it is not null.
    fn cast(&self, array: &ArrayRef, to: &DataType, path: &str) -> Result<Fitted> {
        let from = array.data_type();
        let options = CastOptions {
            safe: true,
            ..Default::default()
        };
        let cast = can_cast_types(from, to)
            .then(|| crate::transform::decimal::cast_with_options(array, to, &options).ok())
            .flatten();
        let out = match cast {
            Some(c) => c,
            None => new_null_array(to, array.len()),
        };
        let before = array.logical_nulls();
        let after = out.logical_nulls();
        let mut bad = Reasons::default();
        for i in 0..array.len() {
            let was = before.as_ref().is_none_or(|b| b.is_valid(i));
            let is = after.as_ref().is_none_or(|a| a.is_valid(i));
            if was && !is {
                bad.set(array.len(), i, || self.not_converted(path, from, to));
            }
        }
        Ok(Fitted {
            array: out,
            bad: bad.0,
        })
    }

    /// A struct into a struct, child by child of the target, each found by its exact name. A
    /// child the source lacks is null, and one only the source has is dropped.
    fn structs(
        &self,
        array: &ArrayRef,
        to_fields: &Fields,
        path: &str,
        kdi: Option<Kdi<'_, '_>>,
    ) -> Result<Fitted> {
        let s = array.as_struct();
        let n = s.len();
        let parent = s.logical_nulls();
        let parent_valid = |i: usize| parent.as_ref().is_none_or(|p| p.is_valid(i));
        let mut bad = Reasons::default();
        let mut columns = Vec::with_capacity(to_fields.len());
        for field in to_fields {
            let child_path = format!("{path}.{}", field.name());
            let fitted = match s.column_by_name(field.name()) {
                Some(child) => {
                    let at: Option<Vec<At>> = kdi.map(|k| {
                        (0..n)
                            .map(|i| match parent_valid(i) {
                                true => k.at[i].child(field.name()),
                                false => At::ABSENT,
                            })
                            .collect()
                    });
                    let kdi = at
                        .as_deref()
                        .zip(kdi)
                        .map(|(at, k)| Kdi { at, reach: k.reach });
                    self.fit(child, field.data_type(), &child_path, false, kdi)?
                }
                None => {
                    if !field.is_nullable() {
                        for i in (0..n).filter(|i| parent_valid(*i)) {
                            bad.set(n, i, || self.missing_not_null(&child_path));
                        }
                    }
                    Fitted::clean(new_null_array(field.data_type(), n))
                }
            };
            if !fitted.bad.is_empty() {
                for i in (0..n).filter(|i| parent_valid(*i)) {
                    if let Some(reason) = fitted.reason(i) {
                        bad.set(n, i, || reason.clone());
                    }
                }
            }
            let child = fitted.array;
            let child = if !field.is_nullable() && child.logical_null_count() > 0 {
                let nulls = child.logical_nulls();
                for i in (0..n).filter(|i| parent_valid(*i)) {
                    if nulls.as_ref().is_some_and(|v| v.is_null(i)) {
                        bad.set(n, i, || self.null_into_not_null(&child_path));
                    }
                }
                force_valid(&child)?
            } else {
                child
            };
            columns.push(child);
        }
        let out = if to_fields.is_empty() {
            StructArray::new_empty_fields(n, s.nulls().cloned())
        } else {
            StructArray::try_new(to_fields.clone(), columns, s.nulls().cloned())
                .map_err(|e| assembly(path, e))?
        };
        Ok(Fitted {
            array: Arc::new(out),
            bad: bad.0,
        })
    }

    /// A list into a list: its elements fitted in place, under the target's element field.
    fn list(
        &self,
        array: &ArrayRef,
        item: &FieldRef,
        path: &str,
        kdi: Option<Kdi<'_, '_>>,
    ) -> Result<Fitted> {
        let l = array.as_list::<i32>();
        let item_path = format!("{path}[]");
        let at = kdi.map(|k| per_element(l, l.offsets(), l.values().len(), k, At::element));
        let kdi = at.as_deref().zip(kdi).map(|(at, k)| Kdi {
            at,
            reach: k.reach && kdi_text::reaches_elements(item.data_type()),
        });
        let fitted = self.fit(l.values(), item.data_type(), &item_path, false, kdi)?;
        let (values, bad) = self.elements(
            l.len(),
            l.offsets(),
            l.logical_nulls(),
            fitted,
            item,
            &item_path,
        )?;
        let out = ListArray::try_new(
            item.clone(),
            l.offsets().clone(),
            values,
            l.nulls().cloned(),
        )
        .map_err(|e| assembly(path, e))?;
        Ok(Fitted {
            array: Arc::new(out),
            bad,
        })
    }

    /// A map into a map: keys and values fitted in place, under the target's entry fields.
    fn map(
        &self,
        array: &ArrayRef,
        entries: &FieldRef,
        sorted: bool,
        path: &str,
        kdi: Option<Kdi<'_, '_>>,
    ) -> Result<Fitted> {
        let m = array.as_map();
        let DataType::Struct(entry_fields) = entries.data_type() else {
            return Err(Error::Schema(format!(
                "{path}: the target's map has entries of type {}, not a struct",
                entries.data_type()
            )));
        };
        let [key_field, value_field] = [&entry_fields[0], &entry_fields[1]];
        let n = m.len();
        let key_path = format!("{path}{{key}}");
        let value_path = format!("{path}{{}}");
        // kdi's coercion never reached inside a map (coercions.rs:64): no "null" there, and
        // its values are written by type only under `kdi_compat`.
        let value_at = kdi.map(|k| per_element(m, m.offsets(), m.values().len(), k, At::value));
        let key_at = kdi.map(|_| vec![At::ABSENT; m.keys().len()]);
        let keys = self.fit(
            m.keys(),
            key_field.data_type(),
            &key_path,
            false,
            key_at.as_deref().map(|at| Kdi { at, reach: false }),
        )?;
        let values = self.fit(
            m.values(),
            value_field.data_type(),
            &value_path,
            false,
            value_at.as_deref().map(|at| Kdi { at, reach: false }),
        )?;
        let (keys, key_bad) = self.elements(
            n,
            m.offsets(),
            m.logical_nulls(),
            keys,
            key_field,
            &key_path,
        )?;
        let (values, value_bad) = self.elements(
            n,
            m.offsets(),
            m.logical_nulls(),
            values,
            value_field,
            &value_path,
        )?;
        let bad = match (key_bad.is_empty(), value_bad.is_empty()) {
            (true, _) => value_bad,
            (false, true) => key_bad,
            (false, false) => key_bad
                .into_iter()
                .zip(value_bad)
                .map(|(k, v)| k.or(v))
                .collect(),
        };
        let mut offsets = m.offsets().clone();
        let (keys, values) = match kdi.is_some() && self.text.compat {
            true => match kdi_order(&keys, &offsets) {
                Some((indices, ordered)) => {
                    offsets = ordered;
                    let take = |a: &ArrayRef| {
                        deltalake::arrow::compute::take(a.as_ref(), &indices, None)
                            .map_err(|e| assembly(path, e))
                    };
                    (take(&keys)?, take(&values)?)
                }
                None => (keys, values),
            },
            false => (keys, values),
        };
        let entries_array = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)
            .map_err(|e| assembly(path, e))?;
        let out = MapArray::try_new(
            entries.clone(),
            offsets,
            entries_array,
            m.nulls().cloned(),
            sorted,
        )
        .map_err(|e| assembly(path, e))?;
        Ok(Fitted {
            array: Arc::new(out),
            bad,
        })
    }

    /// The elements of `n` lists (or maps), fitted to `field`: each list's row takes the first
    /// reason of its elements, and a null element where `field` is NOT NULL is one. Only the
    /// elements of a valid list count; those under a null one are never read.
    fn elements(
        &self,
        n: usize,
        offsets: &OffsetBuffer<i32>,
        valid: Option<deltalake::arrow::buffer::NullBuffer>,
        fitted: Fitted,
        field: &FieldRef,
        path: &str,
    ) -> Result<(ArrayRef, Vec<Option<String>>)> {
        let values = fitted.array.clone();
        let not_null = !field.is_nullable() && values.logical_null_count() > 0;
        let mut bad = Reasons::default();
        if !fitted.bad.is_empty() || not_null {
            let nulls = values.logical_nulls();
            for row in 0..n {
                if valid.as_ref().is_some_and(|v| v.is_null(row)) {
                    continue;
                }
                let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
                for j in start..end {
                    if let Some(reason) = fitted.reason(j) {
                        bad.set(n, row, || reason.clone());
                        break;
                    }
                    if not_null && nulls.as_ref().is_some_and(|v| v.is_null(j)) {
                        bad.set(n, row, || self.null_into_not_null(path));
                        break;
                    }
                }
            }
        }
        let values = if not_null {
            force_valid(&values)?
        } else {
            values
        };
        Ok((values, bad.0))
    }

    /// A general union into a plain type: each row's value is its own branch's, fitted to `to`.
    /// A branch that does not convert rejects the rows that hold it.
    fn union_to(
        &self,
        array: &ArrayRef,
        to: &DataType,
        path: &str,
        kdi: Option<Kdi<'_, '_>>,
    ) -> Result<Fitted> {
        let u = as_union(array, path)?;
        let DataType::Union(fields, _) = array.data_type() else {
            unreachable!("matched as a union")
        };
        let n = u.len();
        if n == 0 {
            return Ok(Fitted::clean(new_empty_array(to)));
        }
        let mut branches: HashMap<i8, usize> = HashMap::new();
        let mut fitted = Vec::with_capacity(fields.len());
        for (type_id, field) in fields.iter() {
            let branch_path = format!("{path} (union branch {})", field.name());
            branches.insert(type_id, fitted.len());
            // Each value of the branch stood where its row's union did.
            let at: Option<Vec<At>> = kdi.map(|k| {
                let mut at = vec![At::ABSENT; u.child(type_id).len()];
                for i in (0..n).filter(|i| u.type_id(*i) == type_id) {
                    at[u.value_offset(i)] = k.at[i].branch(field.name());
                }
                at
            });
            let kdi = at
                .as_deref()
                .zip(kdi)
                .map(|(at, k)| Kdi { at, reach: k.reach });
            fitted.push(self.fit(u.child(type_id), to, &branch_path, false, kdi)?);
        }
        let indices: Vec<(usize, usize)> = (0..n)
            .map(|i| (branches[&u.type_id(i)], u.value_offset(i)))
            .collect();
        let arrays: Vec<&dyn Array> = fitted.iter().map(|f| f.array.as_ref()).collect();
        let out = interleave(&arrays, &indices).map_err(|e| assembly(path, e))?;
        let mut bad = Reasons::default();
        for (i, (branch, offset)) in indices.iter().enumerate() {
            if let Some(reason) = fitted[*branch].reason(*offset) {
                bad.set(n, i, || reason.clone());
            }
        }
        Ok(Fitted {
            array: out,
            bad: bad.0,
        })
    }

    /// A plain value into a general union: every value goes into one branch (see
    /// [`Self::branch_for`]), a null into the union's null branch where it has one. This is
    /// how an older writer schema's plain field is read once a newer one widened it into a
    /// union (§5.5).
    fn wrap_in_union(&self, array: &ArrayRef, to: &DataType, path: &str) -> Result<Fitted> {
        let fields = dense_union_fields(to, path)?;
        let n = array.len();
        let (type_id, fitted) = self.branch_for(array, fields, path)?;
        let null_branch = fields
            .iter()
            .find(|(_, f)| f.data_type() == &DataType::Null)
            .map(|(t, _)| t);
        let valid = array.logical_nulls();
        let mut type_ids = Vec::with_capacity(n);
        let mut offsets = Vec::with_capacity(n);
        let mut nulls = 0i32;
        for i in 0..n {
            let is_null = valid.as_ref().is_some_and(|v| v.is_null(i));
            match null_branch {
                Some(nb) if is_null && nb != type_id => {
                    type_ids.push(nb);
                    offsets.push(nulls);
                    nulls += 1;
                }
                _ => {
                    type_ids.push(type_id);
                    offsets.push(i as i32);
                }
            }
        }
        let children = fields
            .iter()
            .map(|(t, f)| {
                if t == type_id {
                    fitted.array.clone()
                } else if Some(t) == null_branch {
                    Arc::new(NullArray::new(nulls as usize)) as ArrayRef
                } else {
                    new_empty_array(f.data_type())
                }
            })
            .collect();
        let out = UnionArray::try_new(
            fields.clone(),
            type_ids.into(),
            Some(offsets.into()),
            children,
        )
        .map_err(|e| assembly(path, e))?;
        Ok(Fitted {
            array: Arc::new(out),
            bad: fitted.bad,
        })
    }

    /// A general union into another: each branch's values into the branch of the other that
    /// takes them (see [`Self::branch_for`]).
    fn union_to_union(&self, array: &ArrayRef, to: &DataType, path: &str) -> Result<Fitted> {
        let u = as_union(array, path)?;
        let DataType::Union(from_fields, _) = array.data_type() else {
            unreachable!("matched as a union")
        };
        let to_fields = dense_union_fields(to, path)?;
        let n = u.len();
        // Per source branch: the target branch, its values there, and where they start among
        // the values of every source branch that went into that target branch.
        let mut mapped: HashMap<i8, (i8, Fitted, i32)> = HashMap::new();
        let mut parts: BTreeMap<i8, Vec<ArrayRef>> = BTreeMap::new();
        for (type_id, field) in from_fields.iter() {
            let branch_path = format!("{path} (union branch {})", field.name());
            let (target, fitted) = self.branch_for(u.child(type_id), to_fields, &branch_path)?;
            let earlier = parts.entry(target).or_default();
            let shift: usize = earlier.iter().map(|a| a.len()).sum();
            earlier.push(fitted.array.clone());
            mapped.insert(type_id, (target, fitted, shift as i32));
        }
        let mut type_ids = Vec::with_capacity(n);
        let mut offsets = Vec::with_capacity(n);
        let mut bad = Reasons::default();
        for i in 0..n {
            let (target, fitted, shift) = &mapped[&u.type_id(i)];
            let offset = u.value_offset(i);
            type_ids.push(*target);
            offsets.push(shift + offset as i32);
            if let Some(reason) = fitted.reason(offset) {
                bad.set(n, i, || reason.clone());
            }
        }
        let mut children = Vec::with_capacity(to_fields.len());
        for (type_id, field) in to_fields.iter() {
            children.push(match parts.get(&type_id) {
                None => new_empty_array(field.data_type()),
                Some(arrays) if arrays.len() == 1 => arrays[0].clone(),
                Some(arrays) => {
                    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
                    concat(&refs).map_err(|e| assembly(path, e))?
                }
            });
        }
        let out = UnionArray::try_new(
            to_fields.clone(),
            type_ids.into(),
            Some(offsets.into()),
            children,
        )
        .map_err(|e| assembly(path, e))?;
        Ok(Fitted {
            array: Arc::new(out),
            bad: bad.0,
        })
    }

    /// The branch of a union that values of `array`'s type go into, and the values fitted to it:
    /// the branch of the same type; else the first of the same kind (a record, an array, a
    /// map), fitted by name; else the first branch every value converts to, or failing that
    /// the first any value converts to, its other values rejected; else none, and every value
    /// is rejected.
    fn branch_for(
        &self,
        array: &ArrayRef,
        fields: &UnionFields,
        path: &str,
    ) -> Result<(i8, Fitted)> {
        let from = array.data_type();
        if let Some((type_id, _)) = fields.iter().find(|(_, f)| f.data_type() == from) {
            return Ok((type_id, Fitted::clean(array.clone())));
        }
        let same_kind = |t: &DataType| {
            matches!(
                (from, t),
                (DataType::Struct(_), DataType::Struct(_))
                    | (DataType::List(_), DataType::List(_))
                    | (DataType::Map(..), DataType::Map(..))
            )
        };
        if let Some((type_id, field)) = fields.iter().find(|(_, f)| same_kind(f.data_type())) {
            return Ok((
                type_id,
                self.fit(array, field.data_type(), path, false, None)?,
            ));
        }
        let mut partly = None;
        for (type_id, field) in fields.iter() {
            let to = field.data_type();
            if to == &DataType::Null || to.is_nested() || !can_cast_types(from, to) {
                continue;
            }
            let fitted = self.cast(array, to, path)?;
            if fitted.bad.is_empty() {
                return Ok((type_id, fitted));
            }
            partly.get_or_insert((type_id, fitted));
        }
        if let Some(found) = partly {
            return Ok(found);
        }
        let (type_id, field) = fields
            .iter()
            .find(|(_, f)| f.data_type() == &DataType::Null)
            .or_else(|| fields.iter().next())
            .ok_or_else(|| Error::Schema(format!("{path}: a union without branches")))?;
        let to = DataType::Union(fields.clone(), UnionMode::Dense);
        let mut bad = Reasons::default();
        let valid = array.logical_nulls();
        for i in 0..array.len() {
            if valid.as_ref().is_none_or(|v| v.is_valid(i)) {
                bad.set(array.len(), i, || self.not_converted(path, from, &to));
            }
        }
        Ok((
            type_id,
            Fitted {
                array: new_null_array(field.data_type(), array.len()),
                bad: bad.0,
            },
        ))
    }

    /// A float into a decimal as kafka-delta-ingest read one: the float's shortest round-trip
    /// text (Rust's `Display`, what serde_json's number became in arrow-json 55's decimal
    /// reader, decimal_array.rs:80-90) parsed by arrow's `parse_decimal`, which keeps the
    /// digits the scale has room for and drops the rest. A multiply by the scale, Arrow's cast,
    /// would round instead, and can be off in the last digit. NaN, the infinities and a value
    /// with more integer digits than the decimal holds are rejected.
    fn float_to_decimal(&self, array: &ArrayRef, to: &DataType, path: &str) -> Result<Fitted> {
        let floats: Vec<Option<f64>> = match array.data_type() {
            DataType::Float16 => array
                .as_primitive::<Float16Type>()
                .iter()
                .map(|v| v.map(f64::from))
                .collect(),
            DataType::Float32 => array
                .as_primitive::<Float32Type>()
                .iter()
                .map(|v| v.map(f64::from))
                .collect(),
            _ => array.as_primitive::<Float64Type>().iter().collect(),
        };
        let n = floats.len();
        let mut bad = Reasons::default();
        let from = array.data_type();
        let out: ArrayRef = match *to {
            DataType::Decimal128(precision, scale) => {
                let values: Vec<Option<i128>> = floats
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let v = (*v)?;
                        let parsed = decimal_text(v, precision, scale).and_then(|t| {
                            parse_decimal::<Decimal128Type>(&t, precision, scale).ok()
                        });
                        if parsed.is_none() {
                            bad.set(n, i, || self.not_converted(path, from, to));
                        }
                        parsed
                    })
                    .collect();
                Arc::new(
                    Decimal128Array::from(values)
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| assembly(path, e))?,
                )
            }
            DataType::Decimal256(precision, scale) => {
                let values: Vec<Option<_>> = floats
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        let v = (*v)?;
                        let parsed = decimal_text(v, precision, scale).and_then(|t| {
                            parse_decimal::<Decimal256Type>(&t, precision, scale).ok()
                        });
                        if parsed.is_none() {
                            bad.set(n, i, || self.not_converted(path, from, to));
                        }
                        parsed
                    })
                    .collect();
                Arc::new(
                    Decimal256Array::from(values)
                        .with_precision_and_scale(precision, scale)
                        .map_err(|e| assembly(path, e))?,
                )
            }
            _ => unreachable!("matched as a decimal"),
        };
        Ok(Fitted {
            array: out,
            bad: bad.0,
        })
    }

    fn not_converted(&self, path: &str, from: &DataType, to: &DataType) -> String {
        match self.into {
            Dest::Target => format!("{path}: the value does not convert from {from} to {to}"),
            Dest::Union { writer, newest } => format!(
                "writer schema {} field {path}: {from} does not convert to {to} (its type in \
                 writer schema {newest}, the newest that has it)",
                writer.map_or("?".to_string(), |w| w.to_string())
            ),
        }
    }

    fn null_into_not_null(&self, path: &str) -> String {
        format!("{path} is NOT NULL in the target, and this value is null")
    }

    fn missing_not_null(&self, path: &str) -> String {
        format!(
            "{path} is NOT NULL in the target, and nothing produces it (the row's writer schema \
             has no such field)"
        )
    }
}

/// The text of `v` that `parse_decimal` reads into a decimal of `precision` and `scale`, or
/// `None` where it cannot hold `v`. Only a float that cannot fit is turned away before parsing:
/// `parse_decimal` counts digits in a `u8`, and Rust's text of 1e300 has 301 of them.
fn decimal_text(v: f64, precision: u8, scale: i8) -> Option<String> {
    if !v.is_finite() {
        return None;
    }
    let integer_digits = (precision as i32 - scale as i32).max(0);
    // One digit of slack: whether a value at the edge fits is `parse_decimal`'s to say.
    if v.abs() >= 10f64.powi(integer_digits + 1) {
        return None;
    }
    Some(v.to_string())
}

/// Where each of `len` elements of `n` lists (or maps) stood in kafka-delta-ingest's JSON:
/// `step` of its list's, for the elements of a list that is not null; nowhere for the others.
fn per_element<'s>(
    lists: &dyn Array,
    offsets: &OffsetBuffer<i32>,
    len: usize,
    kdi: Kdi<'_, 's>,
    step: impl Fn(At<'s>) -> At<'s>,
) -> Vec<At<'s>> {
    let mut at = vec![At::ABSENT; len];
    for row in 0..lists.len() {
        if lists.is_valid(row) {
            let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
            for slot in &mut at[start..end] {
                *slot = step(kdi.at[row]);
            }
        }
    }
    at
}

/// Each map's entries as kafka-delta-ingest wrote them, where they differ from `keys`' order:
/// one per key, the last one read (apache-avro held a map in a `HashMap`), in the keys' byte
/// order (serde_json's `BTreeMap`, which arrow-json read the object from). The entries to take,
/// and the maps' new offsets; `None` when every map already is so, or a key is not text.
fn kdi_order(
    keys: &ArrayRef,
    offsets: &OffsetBuffer<i32>,
) -> Option<(deltalake::arrow::array::UInt32Array, OffsetBuffer<i32>)> {
    let keys = keys.as_string_opt::<i32>()?;
    let mut indices: Vec<u32> = Vec::with_capacity(keys.len());
    let mut lengths = Vec::with_capacity(offsets.len().saturating_sub(1));
    let mut changed = false;
    for w in offsets.windows(2) {
        let (start, end) = (w[0] as usize, w[1] as usize);
        let mut entries: BTreeMap<&str, usize> = BTreeMap::new();
        for j in start..end {
            entries.insert(keys.value(j), j);
        }
        changed |= entries.len() != end - start || entries.values().copied().ne(start..end);
        lengths.push(entries.len());
        indices.extend(entries.into_values().map(|j| j as u32));
    }
    changed.then(|| (indices.into(), OffsetBuffer::from_lengths(lengths)))
}

/// `fitted`, a STRING array, with the text `"null"` in place of each null `null_word` asks for
/// (§5.6 step 6); a value whose row is rejected stays null.
fn with_null_word(fitted: Fitted, null_word: impl Fn(usize) -> bool) -> Fitted {
    let strings = fitted.array.as_string::<i32>();
    let n = strings.len();
    if !(0..n).any(|i| strings.is_null(i) && null_word(i) && fitted.reason(i).is_none()) {
        return fitted;
    }
    let array: StringArray = (0..n)
        .map(|i| match strings.is_null(i) {
            false => Some(strings.value(i)),
            true if null_word(i) && fitted.reason(i).is_none() => Some("null"),
            true => None,
        })
        .collect();
    Fitted {
        array: Arc::new(array),
        bad: fitted.bad,
    }
}

/// `array` with every null replaced by a placeholder value, so that it can stand in a field
/// that is NOT NULL. Only for values whose rows are rejected: the placeholder is never
/// written.
fn force_valid(array: &ArrayRef) -> Result<ArrayRef> {
    if matches!(array.data_type(), DataType::Null | DataType::Union(..)) {
        return Ok(array.clone());
    }
    let mask = is_not_null(array.as_ref()).map_err(|e| assembly("a NOT NULL field", e))?;
    let filler = placeholders(array.data_type(), array.len())?;
    zip(&mask, array, &filler).map_err(|e| assembly("a NOT NULL field", e))
}

/// `n` valid values of type `t`, of no meaning: zeros, empty strings, empty lists.
fn placeholders(t: &DataType, n: usize) -> Result<ArrayRef> {
    let built: std::result::Result<ArrayRef, ArrowError> = match t {
        DataType::Struct(fields) => fields
            .iter()
            .map(|f| placeholders(f.data_type(), n))
            .collect::<Result<Vec<_>>>()
            .and_then(|columns| {
                StructArray::try_new(fields.clone(), columns, None)
                    .map(|s| Arc::new(s) as ArrayRef)
                    .map_err(|e| assembly("a NOT NULL field", e))
            })
            .map_err(|e| ArrowError::ComputeError(e.to_string())),
        DataType::List(item) => ListArray::try_new(
            item.clone(),
            OffsetBuffer::new_zeroed(n),
            new_empty_array(item.data_type()),
            None,
        )
        .map(|l| Arc::new(l) as ArrayRef),
        DataType::Map(entries, sorted) => {
            if !matches!(entries.data_type(), DataType::Struct(_)) {
                return Err(Error::Schema(format!("a map with entries of type {t}")));
            }
            let entries_array = new_empty_array(entries.data_type()).as_struct().clone();
            MapArray::try_new(
                entries.clone(),
                OffsetBuffer::new_zeroed(n),
                entries_array,
                None,
                *sorted,
            )
            .map(|m| Arc::new(m) as ArrayRef)
        }
        // Zeroed buffers without a validity mask: zero, the empty string, the empty bytes.
        _ => new_null_array(t, n)
            .to_data()
            .into_builder()
            .nulls(None)
            .build()
            .map(deltalake::arrow::array::make_array),
    };
    built.map_err(|e| assembly("a NOT NULL field", e))
}

fn as_union<'a>(array: &'a ArrayRef, path: &str) -> Result<&'a UnionArray> {
    array
        .as_any()
        .downcast_ref::<UnionArray>()
        .ok_or_else(|| Error::Schema(format!("{path}: a union that is not a UnionArray")))
}

/// The fields of a dense union: the only kind arrow-avro decodes into (codec.rs:1493), and so
/// the only kind `U` has.
fn dense_union_fields<'a>(t: &'a DataType, path: &str) -> Result<&'a UnionFields> {
    match t {
        DataType::Union(fields, UnionMode::Dense) => Ok(fields),
        _ => Err(Error::Schema(format!(
            "{path}: cannot fit a value into {t}, which is not a dense union"
        ))),
    }
}

/// An array this module built could not be put together: a bug, never the data's fault.
fn assembly(path: &str, e: impl std::fmt::Display) -> Error {
    Error::Schema(format!("internal: could not assemble {path}: {e}"))
}

/// Rows of `batch` whose `bad` entry is set, as they arrived, with their reasons and columns;
/// and the others.
fn split(
    batch: &RecordBatch,
    out: RecordBatch,
    bad: Vec<Option<(String, String)>>,
) -> Result<(RecordBatch, Option<Rejected>)> {
    if bad.iter().all(Option::is_none) {
        return Ok((out, None));
    }
    let keep: BooleanArray = bad.iter().map(|b| Some(b.is_none())).collect();
    let drop: BooleanArray = bad.iter().map(|b| Some(b.is_some())).collect();
    let good = filter_record_batch(&out, &keep).map_err(|e| assembly("the good rows", e))?;
    let rows = filter_record_batch(batch, &drop).map_err(|e| assembly("the rejected rows", e))?;
    let (columns, reasons): (Vec<Option<String>>, Vec<String>) = bad
        .into_iter()
        .flatten()
        .map(|(column, reason)| (Some(column), reason))
        .unzip();
    Ok((
        good,
        Some(Rejected {
            rows,
            reasons,
            columns,
        }),
    ))
}

// ======================================================================= the target (§5.6)

/// How a value going into a STRING column is written where kafka-delta-ingest wrote text of
/// its own (§5.6 steps 5 and 6): `kdi_compat` writes a value that is not text by its Arrow type,
/// as kdi's JSON had it, and `kdi_null_text` writes `"null"` where kdi's JSON had the key.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KdiText {
    pub compat: bool,
    pub null_text: bool,
}

impl KdiText {
    fn any(self) -> bool {
        self.compat || self.null_text
    }
}

/// Fits a Kafka pipeline's rows to its target, by name at every depth, before the coercer
/// casts them (§5.6). See the module header.
#[derive(Debug)]
pub struct Conformer {
    target: SchemaRef,
    prefix: String,
    text: KdiText,
    /// The writer schema ids a missing target column has been warned about, `None` for rows
    /// that do not say which (SQL that dropped `<prefix>value_schema_id`).
    warned: BTreeSet<Option<i32>>,
    /// The nullable target columns the last batch did not produce.
    unmatched: Vec<String>,
    /// For kdi's text: each writer schema's shape, by id ([`Conformer::observe`]).
    shapes: BTreeMap<u32, Shape>,
    /// Every observed writer schema's shape merged, newest first: what a row that does not say
    /// its writer schema is read with (§5.6 step 6, "approximate").
    merged: Option<Shape>,
    /// A batch whose rows did not say their writer schema was written with `merged`
    /// (`ddi_kafka_null_text_approximate`); warned about once.
    approximate: bool,
}

impl Conformer {
    /// A conformer into `target`, the Kafka metadata columns named with `prefix`. With either
    /// of kafka-delta-ingest's texts (`text`), it must be told every writer schema its rows can
    /// come from ([`Conformer::observe`]) before it conforms them.
    pub fn new(target: SchemaRef, prefix: impl Into<String>, text: KdiText) -> Result<Self> {
        Ok(Self {
            target,
            prefix: prefix.into(),
            text,
            warned: BTreeSet::new(),
            unmatched: Vec::new(),
            shapes: BTreeMap::new(),
            merged: None,
            approximate: false,
        })
    }

    /// Takes note of writer schemas the rows can come from: kafka-delta-ingest's text reads where
    /// its JSON had a key, and the bytes a decimal was written with, off the row's own writer
    /// schema (§5.6 steps 5 and 6). The members of `U` on the SQL path, and every writer schema
    /// the decoder met on either path. Nothing to do without kdi's text.
    ///
    /// A writer schema the decoder cannot use either is left out: none of its messages becomes
    /// a row.
    pub fn observe(&mut self, schemas: &[Arc<RegisteredSchema>]) {
        if !self.text.any() {
            return;
        }
        let mut grew = false;
        for schema in schemas {
            if self.shapes.contains_key(&schema.id) {
                continue;
            }
            match Shape::of(schema) {
                Ok(shape) => {
                    self.shapes.insert(schema.id, shape);
                    grew = true;
                }
                Err(reason) => warn!(
                    writer_schema = schema.id,
                    "no kafka-delta-ingest text for this writer schema's rows, which it has \
                     none of: {reason}"
                ),
            }
        }
        if grew {
            let mut merged: Option<Shape> = None;
            for shape in self.shapes.values().rev() {
                match &mut merged {
                    Some(m) => m.merge(shape),
                    None => merged = Some(shape.clone()),
                }
            }
            self.merged = merged;
        }
    }

    /// Whether a batch's rows did not say their writer schema, so that where kdi's JSON had a
    /// key was read off every writer schema observed instead (`ddi_kafka_null_text_approximate`,
    /// §5.6 step 6). The only rows written otherwise than kdi wrote them are those whose writer
    /// schema lacks a field another has.
    pub fn null_text_approximate(&self) -> bool {
        self.approximate
    }

    /// The nullable target columns the last batch did not produce: they were written null
    /// (`ddi_kafka_target_columns_unmatched`, §5.6 step 1).
    pub fn unmatched_target_columns(&self) -> &[String] {
        &self.unmatched
    }

    /// Conforms every batch, adding the rows that do not fit to `rejects` (§5.6). Without a
    /// data-quality table to put them in (`quarantine` false), the first such row fails the
    /// batch, as the coercer's strict cast does.
    pub fn conform_all(
        &mut self,
        batches: Vec<RecordBatch>,
        rejects: &mut Vec<Rejected>,
        quarantine: bool,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            let (good, bad) = self.conform(&batch)?;
            if let Some(bad) = bad {
                if !quarantine {
                    return Err(Error::Schema(format!(
                        "{} ({} row(s) of this batch). Creating the data-quality table sets such \
                         rows aside instead of stopping this pipeline.",
                        bad.reasons[0],
                        bad.len()
                    )));
                }
                rejects.push(bad);
            }
            if good.num_rows() > 0 {
                out.push(good);
            }
        }
        Ok(out)
    }

    /// One batch, conformed: every target column the batch has (by exact name) fitted to the
    /// target's type at every depth, its plain top-level values left for the coercer; the
    /// batch's other columns as they are, for the coercer to drop. The rows that do not fit
    /// come back apart, as they arrived.
    pub fn conform(&mut self, batch: &RecordBatch) -> Result<(RecordBatch, Option<Rejected>)> {
        let schema = batch.schema();
        let n = batch.num_rows();
        let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
        let mut columns = batch.columns().to_vec();
        let mut bad: Vec<Option<(String, String)>> = vec![None; n];
        let mut unmatched = Vec::new();
        let fitter = Fitter {
            into: Dest::Target,
            text: self.text,
        };
        let ids = (self.text.any() && n > 0).then(|| self.writer_ids(batch));
        let writers = ids.map(|ids| self.writers(ids)).transpose()?;
        // kdi had no key or headers: those take Arrow's cast, as without the options (§5.6 step 5).
        let plain = [
            format!("{}key", self.prefix),
            format!("{}headers", self.prefix),
        ];
        for field in self.target.fields() {
            let Ok(index) = schema.index_of(field.name()) else {
                // NOT NULL: the coercer refuses the batch, naming what there is instead.
                if field.is_nullable() {
                    unmatched.push(field.name().clone());
                }
                continue;
            };
            let at: Option<Vec<At>> = writers
                .as_ref()
                .filter(|_| !plain.contains(field.name()))
                .map(|w| w.iter().map(|s| At::column(s, field.name())).collect());
            let kdi = at.as_deref().map(|at| Kdi { at, reach: true });
            let fitted = fitter.fit(&columns[index], field.data_type(), field.name(), true, kdi)?;
            for (i, reason) in fitted.bad.iter().enumerate() {
                if let (Some(reason), None) = (reason, &bad[i]) {
                    bad[i] = Some((field.name().clone(), reason.clone()));
                }
            }
            // Nullable here, whatever the source said: a union's value can be null although
            // the union itself never is. Whether the target takes a null is the coercer's to
            // check, as for every column.
            fields[index] = Arc::new(Field::new(
                field.name(),
                fitted.array.data_type().clone(),
                true,
            ));
            columns[index] = fitted.array;
        }
        self.note_unmatched(batch, unmatched);
        let out = RecordBatch::try_new(
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone())),
            columns,
        )
        .map_err(|e| assembly("the conformed batch", e))?;
        split(batch, out, bad)
    }

    /// Each row's writer schema id, from `<prefix>value_schema_id`, as kafka-delta-ingest's
    /// text reads where its JSON had keys (§5.6 step 6). Rows that do not say theirs (SQL that
    /// dropped the column) are read with every observed writer schema merged: a warning once,
    /// and the gauge set.
    fn writer_ids(&mut self, batch: &RecordBatch) -> Vec<Option<i32>> {
        let column = format!("{}value_schema_id", self.prefix);
        let ids: Vec<Option<i32>> = match batch.column_by_name(&column) {
            Some(c) if c.data_type() == &DataType::Int32 => {
                c.as_primitive::<Int32Type>().iter().collect()
            }
            _ => vec![None; batch.num_rows()],
        };
        if ids.iter().any(Option::is_none) && !self.approximate {
            warn!(
                column = %column,
                "rows that do not say their writer schema: where kafka-delta-ingest wrote \
                 \"null\" is read off every writer schema seen instead of the row's own, so a \
                 row whose writer schema lacks a field another has gets \"null\" there where kdi \
                 had nothing. Keep the column in the SQL's output (SELECT *, ...) for kdi's exact \
                 text"
            );
            self.approximate = true;
        }
        ids
    }

    /// The shape of each row's writer schema, by id; for a row without one, every observed
    /// writer schema's merged.
    fn writers(&self, ids: Vec<Option<i32>>) -> Result<Vec<&Shape>> {
        let missing = |what: String| {
            Error::Other(format!(
                "internal: kafka-delta-ingest's text needs {what}, which the conformer was not \
                 told of (Conformer::observe)"
            ))
        };
        ids.into_iter()
            .map(|id| match id {
                Some(id) => self
                    .shapes
                    .get(&(id as u32))
                    .ok_or_else(|| missing(format!("writer schema {id}"))),
                None => self
                    .merged
                    .as_ref()
                    .ok_or_else(|| missing("a writer schema".to_string())),
            })
            .collect()
    }

    /// Warns once per writer schema id about target columns its rows do not have, which are
    /// written null: usually a field named in another case (`customerId` against
    /// `customerid`), since names match exactly.
    fn note_unmatched(&mut self, batch: &RecordBatch, unmatched: Vec<String>) {
        if !unmatched.is_empty() {
            let ids = schema_ids(batch, &self.prefix);
            let new: Vec<Option<i32>> = ids
                .into_iter()
                .filter(|id| !self.warned.contains(id))
                .collect();
            if !new.is_empty() {
                let near = case_only(&batch.schema(), &unmatched);
                warn!(
                    writer_schema_ids = ?new,
                    columns = ?unmatched,
                    differ_in_case_only = ?near,
                    "target columns these rows do not have are written as null; names are \
                     matched exactly, case included"
                );
                self.warned.extend(new);
            }
        }
        self.unmatched = unmatched;
    }

    /// The writer schema ids warned about so far.
    #[cfg(test)]
    fn warned(&self) -> &BTreeSet<Option<i32>> {
        &self.warned
    }
}

/// The columns of `unmatched` that `schema` has in another case only, each with the name the
/// rows have: `customerid (the rows have customerId)`.
fn case_only(schema: &Schema, unmatched: &[String]) -> Vec<String> {
    unmatched
        .iter()
        .filter_map(|name| {
            schema
                .fields()
                .iter()
                .find(|f| f.name() != name && f.name().eq_ignore_ascii_case(name))
                .map(|f| format!("{name} (the rows have {})", f.name()))
        })
        .collect()
}

/// The distinct writer schema ids of a batch's rows, from `<prefix>value_schema_id`; `{None}`
/// when the batch does not have that column.
fn schema_ids(batch: &RecordBatch, prefix: &str) -> BTreeSet<Option<i32>> {
    let column = format!("{prefix}value_schema_id");
    match batch.column_by_name(&column) {
        Some(c) if c.data_type() == &DataType::Int32 => {
            c.as_primitive::<Int32Type>().iter().collect()
        }
        _ => BTreeSet::from([None]),
    }
}

// ===================================================================== the union U (§5.5)

/// A field whose type differs between writer schemas of the union.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeChange {
    /// The field, `a.b` when nested.
    pub field: String,
    /// The writer schema whose type `U` takes, and that type.
    pub newer: (u32, DataType),
    /// An older writer schema with another type.
    pub older: (u32, DataType),
}

impl std::fmt::Display for TypeChange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({} in writer schema {}, {} in writer schema {})",
            self.field, self.newer.1, self.newer.0, self.older.1, self.older.0
        )
    }
}

/// The union `U` the SQL path aligns its batches to (§5.5): the writer schemas' fields by name,
/// each with the type of the newest writer schema that has it, nested records merged, and the
/// Kafka metadata columns after them. Every field of it is nullable, at every depth: a row
/// whose writer schema lacks a field has a null there.
#[derive(Clone, Debug)]
pub struct UnionSchema {
    schema: SchemaRef,
    /// The writer schema ids in it, newest first.
    members: Vec<u32>,
    /// For each top-level field, the writer schema its type comes from.
    origin: HashMap<String, u32>,
    changes: Vec<TypeChange>,
    prefix: String,
}

impl UnionSchema {
    /// `U` of `members`: a subject's latest version and every writer schema seen
    /// ([`crate::kafka::registry::SubjectSchemas::members`]). The highest id is the newest
    /// (Confluent ids only grow).
    ///
    /// A member the decoder could not use either, or one with a field named like a metadata
    /// column, is left out with a warning: its messages are undecodable, or hold the pipeline,
    /// when they come, and the rest of `U` is still worth planning against. Nothing about
    /// history refuses to build: versions never seen are not members at all (PO-M7).
    pub fn build(members: &[Arc<RegisteredSchema>], prefix: &str) -> Self {
        let mut ordered: Vec<&Arc<RegisteredSchema>> = members.iter().collect();
        ordered.sort_by_key(|m| std::cmp::Reverse(m.id));
        ordered.dedup_by_key(|m| m.id);
        let mut fields: Vec<Field> = Vec::new();
        let mut origin = HashMap::new();
        let mut ids = Vec::new();
        let mut changes = Vec::new();
        for member in ordered {
            let schema = match decoded_schema(member) {
                Ok(s) => s,
                Err(reason) => {
                    warn!(
                        writer_schema = member.id,
                        "left out of the union the SQL is planned over: {reason}"
                    );
                    continue;
                }
            };
            if let Some(f) = schema
                .fields()
                .iter()
                .find(|f| f.name().starts_with(prefix))
            {
                warn!(
                    writer_schema = member.id,
                    field = %f.name(),
                    metadata_prefix = %prefix,
                    "left out of the union the SQL is planned over: a field is named like a \
                     metadata column, and its messages hold the pipeline when they come"
                );
                continue;
            }
            ids.push(member.id);
            for field in schema.fields() {
                match fields.iter_mut().find(|f| f.name() == field.name()) {
                    Some(existing) => {
                        let newer = origin[field.name()];
                        let merged = merge(
                            existing.data_type(),
                            field.data_type(),
                            field.name(),
                            (newer, member.id),
                            &mut changes,
                        );
                        *existing = Field::new(field.name(), merged, true);
                    }
                    None => {
                        origin.insert(field.name().clone(), member.id);
                        fields.push(Field::new(field.name(), relax(field.data_type()), true));
                    }
                }
            }
        }
        fields.extend(
            metadata_fields(prefix)
                .into_iter()
                .map(|f| Field::new(f.name(), relax(f.data_type()), true)),
        );
        Self {
            schema: Arc::new(Schema::new(fields)),
            members: ids,
            origin,
            changes,
            prefix: prefix.to_string(),
        }
    }

    /// The schema every aligned batch has.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// The writer schema ids `U` was built from, newest first. Empty when none was known (a
    /// subject the registry does not have, and no message seen yet): `U` then has only the
    /// metadata columns.
    pub fn members(&self) -> &[u32] {
        &self.members
    }

    /// The fields whose type differs between members, and in which.
    pub fn changes(&self) -> &[TypeChange] {
        &self.changes
    }

    /// Every batch aligned to `U` (§5.5), the rows that do not convert added to `rejects`.
    pub fn align_all(
        &self,
        batches: Vec<RecordBatch>,
        rejects: &mut Vec<Rejected>,
    ) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::with_capacity(batches.len());
        for batch in batches {
            let (good, bad) = self.align(&batch)?;
            rejects.extend(bad);
            if good.num_rows() > 0 {
                out.push(good);
            }
        }
        Ok(out)
    }

    /// A decoded batch aligned to `U`: each of `U`'s fields from the batch's field of that name,
    /// cast to `U`'s type (by name, at every depth), or null where the batch has none. A row
    /// whose value does not convert comes back apart, as it arrived, with the reason.
    ///
    /// A batch with a field `U` lacks is refused: its writer schema was not observed into `U`
    /// before aligning, and the field would be lost.
    pub fn align(&self, batch: &RecordBatch) -> Result<(RecordBatch, Option<Rejected>)> {
        let writer = schema_ids(batch, &self.prefix).into_iter().next().flatten();
        if let Some(lost) = batch
            .schema()
            .fields()
            .iter()
            .find(|f| self.schema.field_with_name(f.name()).is_err())
        {
            return Err(Error::Other(format!(
                "internal: writer schema {} has the field {:?}, which the union the SQL runs \
                 over does not: it was aligned before its writer schema joined the union",
                writer.map_or("?".to_string(), |w| w.to_string()),
                lost.name()
            )));
        }
        let n = batch.num_rows();
        let mut columns = Vec::with_capacity(self.schema.fields().len());
        let mut bad: Vec<Option<(String, String)>> = vec![None; n];
        for field in self.schema.fields() {
            let Some(column) = batch.column_by_name(field.name()) else {
                columns.push(new_null_array(field.data_type(), n));
                continue;
            };
            let newest = self
                .origin
                .get(field.name())
                .copied()
                .or_else(|| self.members.first().copied())
                .unwrap_or_default();
            let fitter = Fitter {
                into: Dest::Union { writer, newest },
                text: KdiText::default(),
            };
            let fitted = fitter.fit(column, field.data_type(), field.name(), false, None)?;
            for (i, reason) in fitted.bad.iter().enumerate() {
                if let (Some(reason), None) = (reason, &bad[i]) {
                    bad[i] = Some((field.name().clone(), reason.clone()));
                }
            }
            columns.push(fitted.array);
        }
        let out = RecordBatch::try_new(self.schema.clone(), columns)
            .map_err(|e| assembly("the batch aligned to the union", e))?;
        split(batch, out, bad)
    }

    /// The schema `sql` outputs over `U`. Where it does not plan, and the SQL names a field
    /// whose type changed between members, that is [`Error::KafkaSchemaConflict`], naming those
    /// fields with both writer schemas and both types; any other planning failure is the SQL's
    /// own, and comes back as it is.
    ///
    /// Whether the SQL names a field is read off its text, as an identifier, case ignored
    /// (DataFusion folds unquoted names to lower case): a change it never names cannot be why
    /// it fails.
    pub async fn plan(&self, sql: &SqlTransform) -> Result<SchemaRef> {
        let e = match sql.output_schema(self.schema()).await {
            Ok(schema) => return Ok(schema),
            Err(e) => e,
        };
        let text = sql.sql().to_ascii_lowercase();
        let named: Vec<String> = self
            .changes
            .iter()
            .filter(|c| {
                let top = c.field.split(['.', '[', '{']).next().unwrap_or_default();
                mentions(&text, &top.to_ascii_lowercase())
            })
            .map(TypeChange::to_string)
            .collect();
        if named.is_empty() {
            return Err(e);
        }
        Err(Error::KafkaSchemaConflict {
            fields: named,
            detail: e.to_string(),
        })
    }
}

/// Whether `word` occurs in `text` as an identifier, not inside a longer one.
fn mentions(text: &str, word: &str) -> bool {
    let ident = |c: char| c.is_ascii_alphanumeric() || c == '_';
    text.match_indices(word).any(|(i, _)| {
        let before = text[..i].chars().next_back().is_none_or(|c| !ident(c));
        let after = text[i + word.len()..]
            .chars()
            .next()
            .is_none_or(|c| !ident(c));
        before && after
    })
}

/// `t` with every field nullable, at every depth but a map's entries and keys, which Arrow
/// requires to be valid.
fn relax(t: &DataType) -> DataType {
    match t {
        DataType::Struct(fields) => DataType::Struct(relax_fields(fields)),
        DataType::List(item) => DataType::List(Arc::new(Field::new(
            item.name(),
            relax(item.data_type()),
            true,
        ))),
        DataType::Map(entries, sorted) => match entries.data_type() {
            DataType::Struct(kv) if kv.len() == 2 => DataType::Map(
                Arc::new(Field::new(
                    entries.name(),
                    DataType::Struct(Fields::from(vec![
                        Field::new(kv[0].name(), kv[0].data_type().clone(), false),
                        Field::new(kv[1].name(), relax(kv[1].data_type()), true),
                    ])),
                    false,
                )),
                *sorted,
            ),
            _ => t.clone(),
        },
        DataType::Union(fields, mode) => DataType::Union(
            UnionFields::try_new(
                fields.iter().map(|(t, _)| t),
                fields
                    .iter()
                    .map(|(_, f)| Field::new(f.name(), relax(f.data_type()), true)),
            )
            .unwrap_or_else(|_| fields.clone()),
            *mode,
        ),
        _ => t.clone(),
    }
}

fn relax_fields(fields: &Fields) -> Fields {
    fields
        .iter()
        .map(|f| Field::new(f.name(), relax(f.data_type()), true))
        .collect()
}

/// The type of a field in `U`, from `newer` (already in `U`) and an older member's `older`:
/// records merge child by child (the newer's children first, then those only the older has),
/// lists and maps through their values, and any other difference keeps the newer type,
/// recorded as a [`TypeChange`].
fn merge(
    newer: &DataType,
    older: &DataType,
    path: &str,
    ids: (u32, u32),
    changes: &mut Vec<TypeChange>,
) -> DataType {
    let older_relaxed = relax(older);
    if newer == &older_relaxed {
        return newer.clone();
    }
    match (newer, older) {
        (DataType::Struct(a), DataType::Struct(b)) => {
            let mut children: Vec<Field> = a.iter().map(|f| f.as_ref().clone()).collect();
            for child in b {
                match children.iter_mut().find(|c| c.name() == child.name()) {
                    Some(existing) => {
                        let merged = merge(
                            existing.data_type(),
                            child.data_type(),
                            &format!("{path}.{}", child.name()),
                            ids,
                            changes,
                        );
                        *existing = Field::new(child.name(), merged, true);
                    }
                    None => children.push(Field::new(child.name(), relax(child.data_type()), true)),
                }
            }
            DataType::Struct(children.into())
        }
        (DataType::List(a), DataType::List(b)) => DataType::List(Arc::new(Field::new(
            a.name(),
            merge(
                a.data_type(),
                b.data_type(),
                &format!("{path}[]"),
                ids,
                changes,
            ),
            true,
        ))),
        (DataType::Map(ea, sorted), DataType::Map(eb, _)) => {
            match (ea.data_type(), eb.data_type()) {
                (DataType::Struct(ka), DataType::Struct(kb)) if ka.len() == 2 && kb.len() == 2 => {
                    let value = merge(
                        ka[1].data_type(),
                        kb[1].data_type(),
                        &format!("{path}{{}}"),
                        ids,
                        changes,
                    );
                    DataType::Map(
                        Arc::new(Field::new(
                            ea.name(),
                            DataType::Struct(Fields::from(vec![
                                ka[0].as_ref().clone(),
                                Field::new(ka[1].name(), value, true),
                            ])),
                            false,
                        )),
                        *sorted,
                    )
                }
                _ => newer.clone(),
            }
        }
        _ => {
            changes.push(TypeChange {
                field: path.to_string(),
                newer: (ids.0, newer.clone()),
                older: (ids.1, older_relaxed),
            });
            newer.clone()
        }
    }
}

// ================================================================= partition columns (§5.6)

/// What the partition-column check of §5.6 step 0 could say.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionCheck {
    /// Every partition column is produced, or the target has none.
    Passed,
    /// No writer schema was known (a subject the registry does not have, and no message seen
    /// yet), so nothing could be checked: check again on the first batch.
    Deferred,
}

/// §5.6 step 0: refuses with [`Error::KafkaPartitionColumnUnmapped`] when a partition column of
/// the target is not produced: by `sql` planned over `union`, or without SQL by the writer
/// schemas and the metadata columns as they are. Names match exactly, case included, as the
/// coercer matches them.
pub async fn check_partition_columns(
    target_uri: &str,
    partition_columns: &[String],
    sql: Option<&SqlTransform>,
    union: &UnionSchema,
) -> Result<PartitionCheck> {
    if partition_columns.is_empty() {
        return Ok(PartitionCheck::Passed);
    }
    if union.members().is_empty() {
        return Ok(PartitionCheck::Deferred);
    }
    let produced: Vec<String> = match sql {
        Some(sql) => union.plan(sql).await?,
        None => union.schema(),
    }
    .fields()
    .iter()
    .map(|f| f.name().clone())
    .collect();
    let columns: Vec<String> = partition_columns
        .iter()
        .filter(|c| !produced.contains(c))
        .cloned()
        .collect();
    if columns.is_empty() {
        return Ok(PartitionCheck::Passed);
    }
    Err(Error::KafkaPartitionColumnUnmapped {
        target_uri: target_uri.to_string(),
        columns,
        produced,
        sql: sql.is_some(),
    })
}

#[cfg(test)]
mod tests;
