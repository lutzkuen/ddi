//! Upserting into the target, reading back only the part of it that could hold these keys.
//!
//! # Why the target has to be read at all
//!
//! Appending never asks a question about the target. Upserting does: "is this key already
//! there, and is the row I am holding newer than the one that is?" Answering it naively
//! means scanning the whole table on every batch, which grows without bound and is exactly
//! the cost this tool exists to avoid.
//!
//! # The window
//!
//! The MERGE is given a predicate of the shape
//!
//! ```text
//! target.<key> = source.<key> AND target.<sequence> >= <lo>
//! ```
//!
//! The second conjunct is the bound. Delta records `minValues`/`maxValues` per file, so
//! `>= lo` lets the engine skip whole files without opening them, and `<lo>` decides how
//! much of the target is in play.
//!
//! # Choosing `lo`
//!
//! Two things set it, and they pull in opposite directions.
//!
//! *Completeness* says: no row that could hold one of these keys may be skipped. The
//! target's own log answers that without reading any data — walk the live files, keep the
//! ones whose recorded key range touches the keys in hand, and take the lowest sequence
//! value **any** of them holds. Call it `L`. A window starting at `L` cannot miss a match.
//!
//! The minimum has to be taken over *every* candidate file, and this is the single most
//! important line in the module. `target.<sequence> >= lo` is not a file filter: delta-rs
//! makes the predicate the `ON` clause of a full outer join, and file skipping is only an
//! optimisation layered on top of it, so a row below `lo` is unmatched *even when its file
//! is read*. An unmatched row means `WHEN NOT MATCHED` fires and the key is inserted a
//! second time. Taking the minimum over all candidates puts `lo` at or below the first row
//! of every file still in play, so no row inside one can fall out. Lowering it only for
//! files lying wholly below `lo` would leave every file that *straddles* `lo` half-visible
//! — and ddi's own merges manufacture straddling files, because delta-rs rewrites a matched
//! file whole, copying its untouched old rows in beside the new ones.
//!
//! *Cost* says: `L` can be the beginning of the table. If the key is a UUID, every file's
//! key range overlaps every other, and "the files that could hold these keys" is all of
//! them. `upsert_lookback` is the operator's answer to that — a floor, below which the
//! window will not open however far back the statistics reach.
//!
//! So `lo = max(L, min(batch sequence) - lookback)`, and the two cases are reported
//! differently:
//!
//! - `L` wins: the window is as wide as completeness requires and no wider. Nothing to say.
//! - the floor wins: a key in this batch may have an older row somewhere below the floor,
//!   and that row will not be updated — it will be inserted alongside. That is the bargain
//!   `upsert_lookback` buys, so it is logged at `warn` and counted, never silent.
//!
//! With no `upsert_lookback` there is no floor: correctness always wins, and a target whose
//! statistics cannot rule anything out is simply read in full. Being slow is a cost; being
//! wrong is not an option.
//!
//! # Where the statistics run out
//!
//! Any file whose key or sequence statistics are missing — a column past
//! `delta.dataSkippingNumIndexedCols`, or one excluded by `delta.dataSkippingStatsColumns`
//! — makes the whole question unanswerable, and the window opens to the entire target.
//! Truncated string statistics are handled rather than trusted; see
//! [`crate::stats::ranges_can_overlap`].

use std::collections::HashMap;

use deltalake::arrow::array::{Array, ArrayRef, RecordBatch};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::DataType;
use deltalake::datafusion::common::ScalarValue;
use deltalake::datafusion::prelude::{col, lit, Expr};
use deltalake::DeltaTable;

use crate::error::{Error, Result};
use crate::stats::{column_stats, range_touches_any, Bound};

/// The alias the MERGE gives the incoming batch.
pub const SOURCE_ALIAS: &str = "s";
/// The alias the MERGE gives the target table.
pub const TARGET_ALIAS: &str = "t";

// ------------------------------------------------------------------------ lookback

/// The furthest back the merge window is allowed to reach.
///
/// Parsed at config load rather than on the first batch, because a pipeline that cannot be
/// correct must fail where the operator is still looking at it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Lookback {
    /// A span of time, in microseconds, for a clock-valued sequence column.
    Duration(i64),
    /// A plain offset, for a numeric sequence column.
    Offset(f64),
}

impl Lookback {
    /// `"48h"`, `"90m"`, `"500ms"` — or a bare number for a numeric sequence.
    pub fn parse(s: &str) -> Result<Self> {
        // Longest first: "ms" must not be read as "m".
        const UNITS: &[(&str, i64)] = &[
            ("us", 1),
            ("ms", 1_000),
            ("s", 1_000_000),
            ("m", 60 * 1_000_000),
            ("h", 3_600 * 1_000_000),
            ("d", 86_400 * 1_000_000),
            ("w", 7 * 86_400 * 1_000_000),
        ];
        let t = s.trim();
        let bad = |why: &str| {
            Error::Config(format!(
                "upsert_lookback {s:?}: {why}. Give a duration for a timestamp sequence \
                 column (\"48h\", \"90m\", \"500ms\"), or a bare number for a numeric one \
                 (\"10000\"). Omit it entirely to let the target's statistics decide, which \
                 is always correct and sometimes slow."
            ))
        };

        for (suffix, micros) in UNITS {
            let Some(head) = t.strip_suffix(suffix) else {
                continue;
            };
            // A bare number ends in a digit, so an empty head means the whole value was a
            // unit — and `"1e5"` must not be read as 1 exasecond.
            if head.is_empty() || !head.ends_with(|c: char| c.is_ascii_digit()) {
                continue;
            }
            let n: f64 = head.trim().parse().map_err(|_| bad("not a number"))?;
            if n < 0.0 {
                return Err(bad("a lookback cannot be negative"));
            }
            let scaled = n * (*micros as f64);
            if scaled > i64::MAX as f64 {
                return Err(bad("too large to express in microseconds"));
            }
            return Ok(Lookback::Duration(scaled as i64));
        }

        let n: f64 = t.parse().map_err(|_| bad("not a number or a duration"))?;
        if n < 0.0 {
            return Err(bad("a lookback cannot be negative"));
        }
        Ok(Lookback::Offset(n))
    }

    /// This lookback subtracted from `from`, in `from`'s own units.
    ///
    /// `None` when the two do not belong together — a duration against a text sequence, or
    /// a bare offset against a clock. The caller reports that as a configuration error
    /// rather than guessing a floor.
    fn floor_below(&self, from: &Bound, sequence_is_temporal: bool) -> Option<Bound> {
        match (self, from) {
            (Lookback::Duration(us), Bound::Int(v)) if sequence_is_temporal => {
                Some(Bound::Int(v.saturating_sub(*us)))
            }
            (Lookback::Offset(n), Bound::Int(v)) if !sequence_is_temporal => {
                Some(Bound::Int(v.saturating_sub(*n as i64)))
            }
            (Lookback::Offset(n), Bound::Float(v)) => Some(Bound::Float(v - n)),
            _ => None,
        }
    }
}

/// True when a lookback for this column has to be spelled as a duration.
pub fn is_temporal(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64
    )
}

// ------------------------------------------------------------------------ the window

/// The slice of the target this MERGE is allowed to look at.
#[derive(Debug, Clone, Default)]
pub struct Window {
    /// Lower bound on the sequence column. `None` means the whole target.
    lower: Option<Bound>,
    /// The lookback floor stopped the window opening as far as the statistics said it
    /// should. Some key in this batch may have an older row that will not be updated.
    pub clamped: bool,
    /// Live files whose statistics could not rule them out.
    pub candidate_files: usize,
    /// Live files looked at. Fewer than the table holds when the walk stopped early.
    pub examined_files: usize,
    /// Which statistic was missing, when the window had to open to the whole target.
    pub unbounded_because: Option<&'static str>,
}

impl Window {
    /// The whole target, because its statistics cannot answer the question.
    fn unbounded(why: &'static str, examined: usize) -> Self {
        Self {
            lower: None,
            clamped: false,
            candidate_files: 0,
            examined_files: examined,
            unbounded_because: Some(why),
        }
    }

    pub fn is_bounded(&self) -> bool {
        self.lower.is_some()
    }

    /// Decide how far back the merge has to reach.
    ///
    /// Reads the target's log only — no data files are opened.
    pub fn plan(
        target: &DeltaTable,
        key_column: &str,
        sequence_column: &str,
        bounds: &BatchBounds,
        lookback: Option<Lookback>,
        sequence_is_temporal: bool,
    ) -> Result<Self> {
        let floor = match lookback {
            Some(l) => match l.floor_below(&bounds.sequence_min, sequence_is_temporal) {
                Some(f) => Some(f),
                None => {
                    return Err(Error::Config(format!(
                        "upsert_lookback does not fit the sequence column {sequence_column:?}: \
                         a timestamp column needs a duration (\"48h\"), a numeric one needs a \
                         bare number (\"10000\"), and a text one cannot be offset at all."
                    )))
                }
            },
            None => None,
        };

        let snapshot = target.snapshot().map_err(Error::Delta)?;

        let mut lower: Option<Bound> = None;
        let mut candidates = 0usize;
        let mut examined = 0usize;

        for file in snapshot.log_data().iter() {
            examined += 1;

            let Some(raw) = file.stats() else {
                return Ok(Self::unbounded("the file records no statistics", examined));
            };
            let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) else {
                return Ok(Self::unbounded(
                    "the file's statistics are unreadable",
                    examined,
                ));
            };

            let keys = column_stats(&parsed, key_column, &bounds.keys[0]);
            let (Some(kmin), Some(kmax)) = (keys.min, keys.max) else {
                return Ok(Self::unbounded(
                    "no statistics for the key column",
                    examined,
                ));
            };
            if !range_touches_any(&kmin, &kmax, &bounds.keys) {
                continue; // provably cannot hold any of these keys
            }
            candidates += 1;

            let seq = column_stats(&parsed, sequence_column, &bounds.sequence_min);
            let Some(smin) = seq.min else {
                return Ok(Self::unbounded(
                    "no statistics for the sequence column",
                    examined,
                ));
            };

            // The minimum over *every* candidate, never just the ones lying below the
            // running bound — see the module header. `lo` must sit at or under the first
            // row of every file the merge will still read.
            let lowered = match &lower {
                Some(cur) if cur <= &smin => false,
                _ => {
                    lower = Some(smin);
                    true
                }
            };

            // Once the running bound has sunk to the floor, nothing further can lower the
            // answer, so there is no reason to keep parsing statistics.
            if lowered {
                if let (Some(f), Some(l)) = (&floor, &lower) {
                    if l <= f {
                        break;
                    }
                }
            }
        }

        // No candidate file can hold one of these keys, so nothing will match however the
        // window is drawn. Draw it tight.
        let lower = lower.unwrap_or_else(|| bounds.sequence_min.clone());

        let (lower, clamped) = match floor {
            Some(f) if lower < f => (f, true),
            _ => (lower, false),
        };

        Ok(Self {
            lower: Some(lower),
            clamped,
            candidate_files: candidates,
            examined_files: examined,
            unbounded_because: None,
        })
    }

    /// The MERGE's `ON` clause.
    ///
    /// `target.<key> = source.<key>` is what delta-rs turns into a static key range over the
    /// batch, and `target.<sequence> >= lo` is the bound. Both prune files from the log
    /// before any parquet is opened.
    pub fn predicate(
        &self,
        key_column: &str,
        sequence_column: &str,
        sequence_type: &DataType,
    ) -> Result<Expr> {
        let key = col(format!("{TARGET_ALIAS}.{}", quote(key_column)))
            .eq(col(format!("{SOURCE_ALIAS}.{}", quote(key_column))));

        let Some(lo) = &self.lower else {
            return Ok(key);
        };
        let scalar = scalar_of_bound(lo, sequence_type).ok_or_else(|| {
            Error::Schema(format!(
                "cannot express the merge window's lower bound as a {sequence_type} to match \
                 the sequence column {sequence_column:?}"
            ))
        })?;
        Ok(key.and(col(format!("{TARGET_ALIAS}.{}", quote(sequence_column))).gt_eq(lit(scalar))))
    }

    /// The bound, rendered for a log line.
    pub fn lower_bound_display(&self) -> String {
        match &self.lower {
            None => "unbounded".to_string(),
            Some(Bound::Int(v)) => v.to_string(),
            Some(Bound::Float(v)) => v.to_string(),
            Some(Bound::Text(v)) => v.clone(),
        }
    }
}

// ------------------------------------------------------------------------ the plan

/// Everything one merge needs, resolved against this batch and this target.
#[derive(Debug, Clone)]
pub struct MergePlan {
    /// The `ON` clause: key equality, bounded by the window.
    pub predicate: Expr,
    /// The rule that makes a re-delivery of older data a no-op rather than a rollback:
    /// `source` is newer than what is stored, compared over the sequence and then each
    /// tie-breaker. See [`newer_than_stored`].
    pub newer_than_stored: Expr,
    /// Columns a matched row may have overwritten — only those the transform produced. The
    /// rest are the coercer's nulls, and writing them would erase a co-writer's work; see
    /// [`crate::schema::SchemaCoercer::columns_present_in`].
    pub update_columns: Vec<String>,
    /// Columns an inserted row gets. The whole target schema: a new row has no history to
    /// preserve, so the coercer's nulls are the honest value.
    pub insert_columns: Vec<String>,
    /// How the window was arrived at, for logging.
    pub window: Window,
}

/// "Newer than the row already stored", over the sequence and then each tie-breaker.
///
/// With no tie-breaker this is the original `source.<seq> > target.<seq>`. With them it is
/// the lexicographic form, which has to be spelled out rather than expressed as a row
/// comparison because the two sides are different tables:
///
/// ```sql
/// s.seq > t.seq
///   OR (s.seq = t.seq AND s.a > t.a)
///   OR (s.seq = t.seq AND s.a = t.a AND s.b > t.b)
/// ```
///
/// The equality prefixes are what make it an ordering rather than three independent tests:
/// without them a row with an older `seq` but a larger `b` would count as newer and roll the
/// target back, which is the exact failure this predicate exists to prevent.
///
/// This is the half of the tie-breaker that `collapse` cannot do. Collapsing settles ties
/// *within* one batch; a tie between an arriving row and one already committed is only
/// visible here, and it is the one that regrouping commits creates.
fn newer_than_stored(sequence_column: &str, tiebreak: &[String]) -> Expr {
    let s = |c: &str| col(format!("{SOURCE_ALIAS}.{}", quote(c)));
    let t = |c: &str| col(format!("{TARGET_ALIAS}.{}", quote(c)));

    let columns: Vec<&str> = std::iter::once(sequence_column)
        .chain(tiebreak.iter().map(String::as_str))
        .collect();

    let mut out: Option<Expr> = None;
    for (i, c) in columns.iter().enumerate() {
        // Everything before this column decided nothing, so it must be equal for this
        // column to be the one that decides.
        let term = columns[..i].iter().fold(s(c).gt(t(c)), |acc, earlier| {
            s(earlier).eq(t(earlier)).and(acc)
        });
        out = Some(match out {
            Some(prev) => prev.or(term),
            None => term,
        });
    }
    out.expect("there is always at least the sequence column")
}

impl MergePlan {
    /// Resolve the plan for one collapsed batch.
    pub fn resolve(
        target: &DeltaTable,
        batch: &RecordBatch,
        key_column: &str,
        sequence_column: &str,
        lookback: Option<Lookback>,
        update_columns: Vec<String>,
        tiebreak: &[String],
    ) -> Result<Self> {
        let sequence_type = batch
            .schema()
            .field_with_name(sequence_column)
            .map_err(|_| {
                Error::Schema(format!(
                    "upsert sequence column {sequence_column:?} is not in the batch"
                ))
            })?
            .data_type()
            .clone();

        // A batch whose key or sequence column has a type Delta statistics cannot express
        // gets no window at all — correct, and as slow as the target is large.
        let window = match BatchBounds::of(batch, key_column, sequence_column) {
            Some(bounds) => Window::plan(
                target,
                key_column,
                sequence_column,
                &bounds,
                lookback,
                is_temporal(&sequence_type),
            )?,
            None => Window::unbounded("the key or sequence column has an uncomparable type", 0),
        };

        Ok(Self {
            predicate: window.predicate(key_column, sequence_column, &sequence_type)?,
            newer_than_stored: newer_than_stored(sequence_column, tiebreak),
            update_columns,
            insert_columns: batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect(),
            window,
        })
    }
}

// ------------------------------------------------------------------------ batch bounds

/// What one batch holds, in the two columns the window is drawn from.
#[derive(Debug, Clone)]
pub struct BatchBounds {
    /// Every distinct key in the batch, sorted. Not a span: see
    /// [`crate::stats::range_touches_any`].
    pub keys: Vec<Bound>,
    /// The oldest sequence value in the batch — where the window would start if the target
    /// asked nothing more of it.
    pub sequence_min: Bound,
}

impl BatchBounds {
    /// `None` when either column has a type that cannot be lined up against Delta
    /// statistics — the caller then reads the whole target rather than guessing.
    pub fn of(batch: &RecordBatch, key_column: &str, sequence_column: &str) -> Option<Self> {
        if batch.num_rows() == 0 {
            return None;
        }
        let mut keys = bounds_of(&column(batch, key_column).ok()?)?;
        keys.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        keys.dedup();

        let sequence = bounds_of(&column(batch, sequence_column).ok()?)?;
        let sequence_min = sequence
            .into_iter()
            .reduce(|a, b| if b < a { b } else { a })?;

        Some(Self { keys, sequence_min })
    }
}

/// Every non-null value in a column, as comparable bounds.
///
/// `None` for a type that cannot be lined up against Delta statistics — a struct, a list, a
/// boolean. The caller then reads the whole target rather than guessing.
fn bounds_of(column: &ArrayRef) -> Option<Vec<Bound>> {
    use deltalake::arrow::array::AsArray;
    use deltalake::arrow::datatypes::{Float64Type, Int64Type, TimeUnit, TimestampMicrosecondType};

    let live = |a: &dyn Array| -> Vec<usize> { (0..a.len()).filter(|i| !a.is_null(*i)).collect() };

    match column.data_type() {
        // Dates go through microseconds, not days, so they line up with the `"%Y-%m-%d"`
        // a writer records for the same column. See `crate::stats::bound_of_scalar`.
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
            let cast = cast(column, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
            let a = cast.as_primitive_opt::<TimestampMicrosecondType>()?;
            Some(
                live(a)
                    .into_iter()
                    .map(|i| Bound::Int(a.value(i)))
                    .collect(),
            )
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let cast = cast(column, &DataType::Int64).ok()?;
            let a = cast.as_primitive_opt::<Int64Type>()?;
            Some(
                live(a)
                    .into_iter()
                    .map(|i| Bound::Int(a.value(i)))
                    .collect(),
            )
        }
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            let cast = cast(column, &DataType::Float64).ok()?;
            let a = cast.as_primitive_opt::<Float64Type>()?;
            Some(
                live(a)
                    .into_iter()
                    .map(|i| Bound::Float(a.value(i)))
                    .collect(),
            )
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let cast = cast(column, &DataType::Utf8).ok()?;
            let a = cast.as_string_opt::<i32>()?;
            Some(
                live(a)
                    .into_iter()
                    .map(|i| Bound::Text(a.value(i).to_string()))
                    .collect(),
            )
        }
        _ => None,
    }
}

/// Turn a bound back into a literal of the target column's exact type.
fn scalar_of_bound(bound: &Bound, dtype: &DataType) -> Option<ScalarValue> {
    use deltalake::arrow::array::{
        Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use std::sync::Arc;

    let natural: ArrayRef = match (bound, dtype) {
        (Bound::Int(v), DataType::Timestamp(_, _)) => {
            Arc::new(TimestampMicrosecondArray::from(vec![*v]))
        }
        (Bound::Int(v), _) => Arc::new(Int64Array::from(vec![*v])),
        (Bound::Float(v), _) => Arc::new(Float64Array::from(vec![*v])),
        (Bound::Text(v), _) => Arc::new(StringArray::from(vec![v.clone()])),
    };
    let exact = cast(&natural, dtype).ok()?;
    ScalarValue::try_from_array(&exact, 0).ok()
}

// ------------------------------------------------------------------------ collapsing

/// Reduce a batch to one row per key, keeping the newest.
///
/// Not an optimisation. delta-rs rejects a MERGE whose source matches the same target row
/// twice — *"MERGE matched a target row with multiple source rows that satisfy duplicate
/// relevant WHEN MATCHED clauses"* — so two versions of one key inside a single batch would
/// abort the commit rather than apply in order. Collapsing first is what makes a batch that
/// contains a correction *and* the row it corrects behave the same as the two arriving in
/// separate batches.
///
/// Ties on the sequence value are broken by `tiebreak`, compared left to right. With no
/// tie-breaker configured they fall back to position: later in the batch is later in the
/// source, so it wins. That fallback is correct only while batch boundaries are stable, and
/// they are not — a retry regroups commits, and `staged_upsert` regroups them by design.
/// See [`crate::config::PipelineConfig::upsert_tiebreak`].
pub fn collapse(
    batches: &[RecordBatch],
    key_column: &str,
    sequence_column: &str,
    tiebreak: &[String],
) -> Result<RecordBatch> {
    use deltalake::arrow::array::{StringArray, UInt64Array};
    use deltalake::arrow::compute::{concat_batches, take_record_batch};
    use deltalake::arrow::row::{RowConverter, SortField};

    let Some(first) = batches.first() else {
        return Err(Error::Other("upsert: nothing to collapse".into()));
    };
    let all = concat_batches(&first.schema(), batches)
        .map_err(|e| Error::Other(format!("upsert: cannot concatenate the batch: {e}")))?;
    if all.num_rows() == 0 {
        return Ok(all);
    }

    let keys = column(&all, key_column)?;
    if keys.null_count() > 0 {
        return Err(Error::Schema(format!(
            "upsert key {key_column:?} contains {} null(s). A row with no key can be neither \
             matched against the target nor safely inserted — every redelivery would append \
             another copy, because SQL does not consider two nulls equal.",
            keys.null_count()
        )));
    }
    let sequence = column(&all, sequence_column)?;
    if sequence.null_count() > 0 {
        return Err(Error::Schema(format!(
            "upsert sequence column {sequence_column:?} contains {} null(s). It is what \
             decides whether an arriving row is newer than the one already stored, so a row \
             without one can be neither applied nor skipped safely.",
            sequence.null_count()
        )));
    }

    let text = cast(&keys, &DataType::Utf8)
        .map_err(|e| Error::Schema(format!("upsert key {key_column:?} is not comparable: {e}")))?;
    let text = text.as_any().downcast_ref::<StringArray>().expect("utf8");

    // The sequence first, then each tie-breaker in the order it was declared. A row
    // encoding compares lexicographically, so this *is* the ordering rule — no per-type
    // match arm, and no separate code path for the tie.
    let mut order = vec![sequence.clone()];
    for c in tiebreak {
        let v = column(&all, c)?;
        if v.null_count() > 0 {
            return Err(Error::Schema(format!(
                "upsert tie-breaker {c:?} contains {} null(s). It decides which of two rows \
                 sharing a {sequence_column:?} is the later one, and a null cannot be \
                 compared — the rows would fall back to whichever the batch happened to \
                 carry last, which is the instability the tie-breaker exists to remove.",
                v.null_count()
            )));
        }
        order.push(v);
    }

    // A byte-comparable encoding, so any Delta type orders correctly without a match arm
    // per type.
    let converter = RowConverter::new(
        order
            .iter()
            .map(|c| SortField::new(c.data_type().clone()))
            .collect(),
    )
    .map_err(|e| {
        Error::Schema(format!(
            "upsert sequence column {sequence_column:?} cannot be ordered: {e}"
        ))
    })?;
    let rows = converter
        .convert_columns(&order)
        .map_err(|e| Error::Other(format!("upsert: cannot order the batch: {e}")))?;

    let mut newest: HashMap<&str, usize> = HashMap::with_capacity(all.num_rows());
    for i in 0..all.num_rows() {
        let k = text.value(i);
        match newest.get(k) {
            // `>=`: a tie survives every configured tie-breaker, so the two rows are
            // indistinguishable by any persisted value and the later one in the batch is
            // taken as the later one in the source.
            Some(&best) if rows.row(i) < rows.row(best) => {}
            _ => {
                newest.insert(k, i);
            }
        }
    }
    if newest.len() == all.num_rows() {
        return Ok(all); // every key distinct — the common case
    }

    let mut keep: Vec<u64> = newest.values().map(|i| *i as u64).collect();
    keep.sort_unstable();
    take_record_batch(&all, &UInt64Array::from(keep))
        .map_err(|e| Error::Other(format!("upsert: cannot collapse duplicate keys: {e}")))
}

// ------------------------------------------------------------------------ preflight

/// Everything that can make an upsert wrong, checked once while the operator is still
/// watching.
///
/// A pipeline that cannot be correct must fail at startup, not on its first batch — the
/// same rule `Config::resolve` follows, applied to the things that need the table open.
pub async fn preflight(
    target: &DeltaTable,
    target_schema: &deltalake::arrow::datatypes::SchemaRef,
    key_column: &str,
    sequence_column: &str,
    lookback: Option<Lookback>,
    tiebreak: &[String],
) -> Result<u32> {
    let named = |c: &str| -> Result<&DataType> {
        target_schema
            .field_with_name(c)
            .map(|f| f.data_type())
            .map_err(|_| {
                Error::Config(format!(
                    "upsert column {c:?} is not in the target table. Columns: [{}]",
                    target_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })
    };
    named(key_column)?;
    let sequence_type = named(sequence_column)?.clone();

    // The tie-breaker is compared against the *stored* row, not only between rows in hand,
    // so a column that is not in the target cannot settle anything. Caught here rather than
    // in the merge, where it would surface as a planner error on the first contested key —
    // which may be days after the pipeline started.
    for c in tiebreak {
        named(c)?;
    }

    // A duration against a counter, or a bare number against a clock, has no defined
    // meaning. Guessing one would silently draw the window at the wrong scale — a
    // thousandfold-too-short window looks like it is working right up until a correction is
    // duplicated instead of applied.
    if let Some(l) = lookback {
        let probe = if is_temporal(&sequence_type) {
            Bound::Int(0)
        } else {
            match &sequence_type {
                t if t.is_integer() => Bound::Int(0),
                DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
                    Bound::Float(0.0)
                }
                other => {
                    return Err(Error::Config(format!(
                        "upsert_lookback cannot be applied to the sequence column \
                         {sequence_column:?}, which is {other}. Only a clock or a number can \
                         be offset backwards. Drop upsert_lookback and let the target's \
                         statistics decide how far the window has to reach."
                    )))
                }
            }
        };
        if l.floor_below(&probe, is_temporal(&sequence_type)).is_none() {
            return Err(Error::Config(format!(
                "upsert_lookback does not fit the sequence column {sequence_column:?}, which \
                 is {sequence_type}. A timestamp or date column needs a duration (\"48h\"); a \
                 numeric one needs a bare number (\"10000\")."
            )));
        }
    }

    // delta-rs rejects a MERGE on a table with column mapping, and Delta rejects any commit
    // carrying a Remove on an appendOnly table. Both surface as an opaque failure on the
    // first batch otherwise.
    use deltalake::table::config::TablePropertiesExt;
    let snapshot = target.snapshot().map_err(Error::Delta)?;
    if snapshot.snapshot().table_properties().append_only() {
        return Err(Error::Config(
            "the target has delta.appendOnly = true, which forbids the Remove actions a \
             merge writes. Clear the property, or use write_mode = \"append\"."
                .into(),
        ));
    }

    assert_one_row_per_key(target, key_column).await
}

/// Refuse to start on a target that already holds a key twice.
///
/// A MERGE keys its duplicate detection on the *target* row, so one source row matching two
/// stored rows updates **both** and neither complains. A table that accumulated duplicates
/// while running append-only — which is exactly what append-only does with a restated key —
/// would therefore keep them forever, in lockstep, and every count and sum over it would
/// stay wrong. Switching a pipeline to upsert has to start from a target that already holds
/// the grain it claims.
///
/// # Cost
///
/// One pass over the key column alone — the projection is pushed into the Delta scan, so no
/// other column is decoded — repeated once per congruence class of the key space, where the
/// number of classes is eight bytes per target row divided by
/// [`crate::config::Defaults::max_grain_check_memory`]. For almost every target that is one
/// pass. It holds a fixed number of bytes and writes **nothing** to any temporary directory,
/// which is a property of its shape rather than of a limit it is held to; see [`crate::grain`]
/// for why, and for why the obvious `GROUP BY ... HAVING ... LIMIT 3` could not be bounded and
/// did not stop early.
///
/// It stops at the first few duplicates it finds, so the expensive answer is always "the
/// target is fine" and never "the target is broken". It is still proportional to the target,
/// which is why it happens once at startup rather than per batch, and why it runs behind
/// `max_concurrent_upsert_preflights`. See [`crate::dedup::Dedup::read`] for the same
/// reasoning applied to the watermark — this function used to be the exception to it, and the
/// exception is what evicted a pod.
async fn assert_one_row_per_key(target: &DeltaTable, key_column: &str) -> Result<u32> {
    /// Enough to show the operator the shape of the problem without gathering all of it.
    const EXAMPLES: usize = 3;

    match crate::grain::check(target, key_column, crate::grain::ceiling(), EXAMPLES).await? {
        crate::grain::Grain::Unique { rows, passes } => {
            tracing::debug!(
                key_column,
                ?rows,
                passes,
                "the target holds one row per key"
            );
            Ok(passes)
        }
        crate::grain::Grain::Duplicated { examples, passes } => {
            tracing::debug!(
                key_column,
                passes,
                "the target does not hold one row per key"
            );
            let mut examples: Vec<String> = examples
                .into_iter()
                .map(|d| format!("{:?} ({} rows)", d.key, d.rows))
                .collect();
            examples.sort();
            Err(Error::Config(format!(
                "the target already holds {key_column:?} more than once — for example {}. A \
                 merge matches on the stored row, so it would update every copy rather than \
                 collapse them, and the duplicates would stay forever. This is what an \
                 append-only target looks like after a key was restated. Collapse the target \
                 to one row per key first (a one-off `CREATE OR REPLACE TABLE ... AS SELECT \
                 ... QUALIFY row_number() OVER (PARTITION BY {key_column} ORDER BY <sequence> \
                 DESC) = 1`), then start this pipeline.",
                examples.join(", ")
            )))
        }
    }
}

fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        Error::Schema(format!(
            "upsert column {name:?} is not in the transformed batch. Columns: [{}]",
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ))
    })?;
    Ok(batch.column(idx).clone())
}

/// Quote an identifier so a column called `order` or `_timestamp` survives the planner.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pre-tie-breaker signature. Most of these tests are about collapsing itself and
    /// say nothing about ties, so they keep reading as they did; the tie-breaker gets its
    /// own tests below. An explicit item shadows the glob import above.
    fn collapse(
        batches: &[RecordBatch],
        key_column: &str,
        sequence_column: &str,
    ) -> Result<RecordBatch> {
        super::collapse(batches, key_column, sequence_column, &[])
    }
    use deltalake::arrow::array::{Int64Array, StringArray, TimestampMicrosecondArray};
    use deltalake::arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("status", DataType::Utf8, true),
        ]))
    }

    fn batch(rows: &[(&str, i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn rows_of(b: &RecordBatch) -> Vec<(String, i64, String)> {
        let k = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let t = b
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let s = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        (0..b.num_rows())
            .map(|i| (k.value(i).to_string(), t.value(i), s.value(i).to_string()))
            .collect()
    }

    #[test]
    fn durations_and_bare_numbers_both_parse() {
        assert_eq!(
            Lookback::parse("48h").unwrap(),
            Lookback::Duration(48 * 3_600_000_000)
        );
        assert_eq!(
            Lookback::parse("90m").unwrap(),
            Lookback::Duration(90 * 60_000_000)
        );
        assert_eq!(
            Lookback::parse("500ms").unwrap(),
            Lookback::Duration(500_000)
        );
        assert_eq!(
            Lookback::parse("7d").unwrap(),
            Lookback::Duration(7 * 86_400_000_000)
        );
        assert_eq!(
            Lookback::parse("10000").unwrap(),
            Lookback::Offset(10_000.0)
        );
    }

    #[test]
    fn ms_is_not_read_as_minutes() {
        // The classic units bug: "500ms" stripped of "s" leaves "500m", which would then
        // read as 500 minutes if the suffixes were tried shortest-first.
        assert_eq!(
            Lookback::parse("500ms").unwrap(),
            Lookback::Duration(500_000)
        );
        assert_eq!(
            Lookback::parse("500m").unwrap(),
            Lookback::Duration(500 * 60_000_000)
        );
    }

    #[test]
    fn a_nonsense_lookback_names_the_alternatives() {
        let e = Lookback::parse("soon").unwrap_err().to_string();
        assert!(e.contains("48h"), "should show the spelling: {e}");
        assert!(e.contains("10000"), "and the numeric one: {e}");
        assert!(Lookback::parse("-1h").is_err(), "negative is meaningless");
    }

    /// `order_id, _timestamp, status, kafka_offset` — the shape a tie-breaker needs, which
    /// the three-column fixture above deliberately does not have.
    fn batch_with_offset(rows: &[(&str, i64, &str, i64)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("status", DataType::Utf8, false),
            Field::new("kafka_offset", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(StringArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(deltalake::arrow::array::Int64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn statuses(b: &RecordBatch) -> Vec<String> {
        let s = b.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        (0..b.num_rows()).map(|i| s.value(i).to_string()).collect()
    }

    #[test]
    fn a_tie_is_settled_by_the_tie_breaker_rather_than_by_batch_position() {
        let tiebreak = vec!["kafka_offset".to_string()];

        // Same instant, and the winner is written *first* — so position would pick the
        // wrong one. Only the offset can tell them apart.
        let out = super::collapse(
            &[batch_with_offset(&[
                ("a", 10, "shipped", 99),
                ("a", 10, "placed", 7),
            ])],
            "order_id",
            "_timestamp",
            &tiebreak,
        )
        .unwrap();
        assert_eq!(statuses(&out), vec!["shipped".to_string()]);

        // Reverse the rows: the answer must not move.
        let out = super::collapse(
            &[batch_with_offset(&[
                ("a", 10, "placed", 7),
                ("a", 10, "shipped", 99),
            ])],
            "order_id",
            "_timestamp",
            &tiebreak,
        )
        .unwrap();
        assert_eq!(
            statuses(&out),
            vec!["shipped".to_string()],
            "the winner is a property of the rows, not of the order they arrived in"
        );
    }

    #[test]
    fn without_a_tie_breaker_position_still_decides() {
        // The pre-existing rule, kept: an unconfigured pipeline behaves exactly as before.
        let out = super::collapse(
            &[batch_with_offset(&[
                ("a", 10, "shipped", 99),
                ("a", 10, "placed", 7),
            ])],
            "order_id",
            "_timestamp",
            &[],
        )
        .unwrap();
        assert_eq!(statuses(&out), vec!["placed".to_string()], "last in wins");
    }

    #[test]
    fn regrouping_the_same_rows_cannot_change_the_winner() {
        // The property `staged_upsert` needs and batch position cannot give: an apply worker
        // accumulates a different number of commits each time it runs, so a winner that
        // depends on where the batch boundary fell is a winner that changes on replay.
        let tiebreak = vec!["kafka_offset".to_string()];
        let rows = [
            ("a", 10, "one", 5),
            ("a", 10, "two", 9),
            ("a", 10, "three", 3),
        ];

        let whole = super::collapse(
            &[batch_with_offset(&rows)],
            "order_id",
            "_timestamp",
            &tiebreak,
        )
        .unwrap();

        // The same rows, delivered as three separate commits the apply worker grouped.
        let split = super::collapse(
            &[
                batch_with_offset(&rows[0..1]),
                batch_with_offset(&rows[1..2]),
                batch_with_offset(&rows[2..3]),
            ],
            "order_id",
            "_timestamp",
            &tiebreak,
        )
        .unwrap();

        assert_eq!(statuses(&whole), vec!["two".to_string()]);
        assert_eq!(
            statuses(&split),
            statuses(&whole),
            "grouping is not evidence"
        );
    }

    #[test]
    fn a_null_tie_breaker_is_an_error_rather_than_a_silent_fallback() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Utf8, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("kafka_offset", DataType::Int64, true),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![10, 10])) as ArrayRef,
                Arc::new(deltalake::arrow::array::Int64Array::from(vec![
                    Some(1),
                    None,
                ])) as ArrayRef,
            ],
        )
        .unwrap();

        let e = super::collapse(
            &[b],
            "order_id",
            "_timestamp",
            &["kafka_offset".to_string()],
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("kafka_offset"), "names the column: {e}");
        assert!(e.contains("null"), "and what was wrong with it: {e}");
    }

    #[test]
    fn the_merge_predicate_compares_lexicographically() {
        // Collapsing settles ties inside one batch. This is the other half: a tie between an
        // arriving row and one already committed, which is the case regrouping creates and
        // which no amount of collapsing can see.
        let plain = newer_than_stored("_timestamp", &[]).to_string();
        assert!(
            !plain.contains("OR"),
            "with no tie-breaker it stays the original single comparison: {plain}"
        );

        let lex = newer_than_stored(
            "_timestamp",
            &["kafka_partition".to_string(), "kafka_offset".to_string()],
        )
        .to_string();
        assert!(lex.contains("kafka_partition"), "{lex}");
        assert!(lex.contains("kafka_offset"), "{lex}");
        // The equality prefixes are what make it an ordering rather than three independent
        // tests: without them an older row with a larger offset would count as newer.
        assert!(
            lex.matches("= t.").count() >= 3,
            "each term must require every earlier column to be equal: {lex}"
        );
    }

    #[test]
    fn collapsing_keeps_only_the_newest_row_per_key() {
        // The batch holds an order and its correction. Appending both would put two rows in
        // a table that is meant to hold one; sending both to the MERGE would abort it.
        let out = collapse(
            &[batch(&[
                ("a", 10, "placed"),
                ("b", 11, "placed"),
                ("a", 20, "shipped"),
            ])],
            "order_id",
            "_timestamp",
        )
        .unwrap();
        assert_eq!(
            rows_of(&out),
            vec![
                ("b".into(), 11, "placed".into()),
                ("a".into(), 20, "shipped".into()),
            ],
            "one row per key, and it is the corrected one"
        );
    }

    #[test]
    fn an_out_of_order_correction_still_loses_to_the_newer_row() {
        let out = collapse(
            &[batch(&[("a", 20, "shipped"), ("a", 10, "placed")])],
            "order_id",
            "_timestamp",
        )
        .unwrap();
        assert_eq!(rows_of(&out), vec![("a".into(), 20, "shipped".into())]);
    }

    #[test]
    fn a_tie_is_broken_by_position_in_the_batch() {
        let out = collapse(
            &[batch(&[("a", 10, "first"), ("a", 10, "second")])],
            "order_id",
            "_timestamp",
        )
        .unwrap();
        assert_eq!(
            rows_of(&out),
            vec![("a".into(), 10, "second".into())],
            "same instant, so later in the source wins"
        );
    }

    #[test]
    fn distinct_keys_pass_through_untouched() {
        let b = batch(&[("a", 10, "x"), ("b", 11, "y")]);
        let out = collapse(std::slice::from_ref(&b), "order_id", "_timestamp").unwrap();
        assert_eq!(out.num_rows(), 2);
        assert_eq!(rows_of(&out), rows_of(&b));
    }

    #[test]
    fn a_null_key_is_an_error_rather_than_an_endless_stream_of_duplicates() {
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Utf8, true),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new("status", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![None as Option<&str>])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![1])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ],
        )
        .unwrap();
        let e = collapse(&[b], "order_id", "_timestamp")
            .unwrap_err()
            .to_string();
        assert!(e.contains("order_id"), "got: {e}");
        assert!(
            e.contains("nulls equal") || e.contains("null"),
            "should say why: {e}"
        );
    }

    #[test]
    fn a_null_sequence_value_is_an_error() {
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Utf8, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
                Field::new("status", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![None as Option<i64>])) as ArrayRef,
                Arc::new(StringArray::from(vec!["x"])) as ArrayRef,
            ],
        )
        .unwrap();
        assert!(collapse(&[b], "order_id", "_timestamp").is_err());
    }

    #[test]
    fn batch_bounds_list_the_keys_sorted_and_start_at_the_oldest_row() {
        let b = batch(&[("c", 30, "x"), ("a", 10, "y"), ("b", 20, "z")]);
        let got = BatchBounds::of(&b, "order_id", "_timestamp").unwrap();
        assert_eq!(
            got.keys,
            vec![
                Bound::Text("a".into()),
                Bound::Text("b".into()),
                Bound::Text("c".into())
            ],
            "sorted, because the candidate test binary-searches them"
        );
        assert_eq!(
            got.sequence_min,
            Bound::Int(10),
            "the window has to reach back to the oldest row in hand"
        );
    }

    #[test]
    fn repeated_keys_are_listed_once() {
        let b = batch(&[("a", 10, "x"), ("a", 20, "y"), ("b", 30, "z")]);
        let got = BatchBounds::of(&b, "order_id", "_timestamp").unwrap();
        assert_eq!(
            got.keys,
            vec![Bound::Text("a".into()), Bound::Text("b".into())]
        );
    }

    #[test]
    fn a_numeric_key_yields_numeric_bounds() {
        let s = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("seq", DataType::Int64, false),
        ]));
        let b = RecordBatch::try_new(
            s,
            vec![
                Arc::new(Int64Array::from(vec![5, 1, 9])) as ArrayRef,
                Arc::new(Int64Array::from(vec![100, 200, 300])) as ArrayRef,
            ],
        )
        .unwrap();
        let got = BatchBounds::of(&b, "id", "seq").unwrap();
        assert_eq!(got.keys, vec![Bound::Int(1), Bound::Int(5), Bound::Int(9)]);
        assert_eq!(got.sequence_min, Bound::Int(100));
    }

    #[test]
    fn a_floor_needs_the_right_kind_of_lookback() {
        let ts = Bound::Int(1_000_000_000);
        assert_eq!(
            Lookback::Duration(1_000).floor_below(&ts, true),
            Some(Bound::Int(999_999_000))
        );
        assert!(
            Lookback::Duration(1_000).floor_below(&ts, false).is_none(),
            "a duration against a plain counter is a configuration mistake, not a guess"
        );
        assert!(
            Lookback::Offset(5.0)
                .floor_below(&Bound::Text("x".into()), false)
                .is_none(),
            "text cannot be offset"
        );
    }

    #[test]
    fn an_unbounded_window_matches_on_the_key_alone() {
        let w = Window::unbounded("no statistics for the key column", 3);
        assert!(!w.is_bounded());
        let e = w
            .predicate("order_id", "_timestamp", &DataType::Int64)
            .unwrap();
        let rendered = format!("{e}");
        assert!(rendered.contains("order_id"), "got: {rendered}");
        assert!(
            !rendered.contains("_timestamp"),
            "no bound means no bound: {rendered}"
        );
    }

    #[test]
    fn a_bounded_window_constrains_the_target_side() {
        let w = Window {
            lower: Some(Bound::Int(500)),
            ..Default::default()
        };
        let e = w.predicate("order_id", "seq", &DataType::Int64).unwrap();
        let rendered = format!("{e}");
        assert!(rendered.contains("order_id"), "got: {rendered}");
        assert!(rendered.contains("seq"), "got: {rendered}");
        assert!(rendered.contains("500"), "got: {rendered}");
    }
}
