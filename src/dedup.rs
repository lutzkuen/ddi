//! Skipping rows a rebuild already wrote, without the rebuilder's cooperation.
//!
//! # Why a timestamp
//!
//! A batch rebuild reads the source at some snapshot and commits its output later. Rows
//! that arrive in between are in neither: not in the rebuild's snapshot, and — once the
//! rebuild overwrites the target — no longer in the target either, even though `ddi` may
//! have streamed them. That window is the whole problem.
//!
//! A timestamp that increases with arrival order closes it, because the question "did the
//! rebuild already cover this row?" becomes a property of the row rather than of the
//! schedule:
//!
//! ```text
//! 00:00  batch reads source@100, whose newest row is _timestamp = T
//! 00:03  rows T+1, T+2 arrive; ddi streams them
//! 00:05  batch OVERWRITES the target with everything up to T
//! 00:06  ddi reads max(_timestamp) = T from the target, and emits T+1, T+2
//! ```
//!
//! No handshake, no hooks, no shared state. The answer was already in the data.
//!
//! # Why a key as well
//!
//! `> T` alone is wrong at the boundary. Rows sharing exactly `T` may be split between
//! "the rebuild saw it" and "it arrived a moment later" — `>` drops the latter, `>=`
//! duplicates the former. Neither is acceptable, so rows *at* `T` are resolved
//! individually against the keys the target already holds at `T`. That set is small by
//! construction: only the rows sharing the maximum timestamp.
//!
//! With no key configured, ties fall back to `>`, which can drop a row that arrived in
//! the same instant as the rebuild's newest. Fine for a strictly increasing sequence,
//! not for a second-granularity clock under load.

use std::collections::HashSet;

use deltalake::arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, StringArray};
use deltalake::arrow::compute::kernels::boolean::{and, or};
use deltalake::arrow::compute::kernels::cmp::{eq, gt};
use deltalake::arrow::compute::{cast, filter_record_batch};
use deltalake::arrow::datatypes::DataType;
use deltalake::DeltaTable;
use futures::TryStreamExt;

use crate::error::{Error, Result};
use crate::source::Version;

/// The default timestamp column, matching the convention this tool assumes tables follow.
pub const DEFAULT_TIMESTAMP_COLUMN: &str = "_timestamp";

/// What the target already contains, expressed as a cut-off.
#[derive(Debug, Clone, Default)]
pub struct Dedup {
    timestamp_column: String,
    key_column: Option<String>,
    /// `max(timestamp)` in the target, as a one-element array. `None` when the target is
    /// empty, which means nothing has been covered and everything passes.
    watermark: Option<ArrayRef>,
    /// Keys already present *at* the watermark instant, stringified for comparability.
    boundary_keys: HashSet<String>,
}

impl Dedup {
    /// Read the cut-off out of the target table.
    ///
    /// One pass over the target per pipeline start — not per batch.
    pub async fn read(
        target: &DeltaTable,
        timestamp_column: &str,
        key_column: Option<&str>,
    ) -> Result<Self> {
        use deltalake::datafusion::datasource::MemTable;
        use deltalake::datafusion::prelude::SessionContext;

        let mut out = Self {
            timestamp_column: timestamp_column.to_string(),
            key_column: key_column.map(str::to_string),
            ..Default::default()
        };

        let (_t, stream) = target.clone().scan_table().await.map_err(Error::Delta)?;
        let batches: Vec<RecordBatch> = stream.try_collect().await.map_err(|e| {
            Error::Other(format!(
                "dedup: cannot scan the target to read its watermark: {e}"
            ))
        })?;
        let Some(schema) = batches.first().map(|b| b.schema()) else {
            return Ok(out); // empty target: nothing covered yet
        };
        if schema.index_of(timestamp_column).is_err() {
            return Err(Error::Config(format!(
                "dedup timestamp column {timestamp_column:?} is not in the target table. \
                 Columns: [{}]",
                schema
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        let ctx = SessionContext::new();
        let provider = MemTable::try_new(schema, vec![batches])
            .map_err(|e| Error::Other(format!("dedup: {e}")))?;
        ctx.register_table("target", std::sync::Arc::new(provider))
            .map_err(|e| Error::Other(format!("dedup: cannot register target: {e}")))?;

        let ts = quote(timestamp_column);
        let rows = ctx
            .sql(&format!("SELECT max({ts}) AS m FROM target"))
            .await
            .map_err(|e| Error::Other(format!("dedup: max({timestamp_column}) failed: {e}")))?
            .collect()
            .await
            .map_err(|e| Error::Other(format!("dedup: max({timestamp_column}) failed: {e}")))?;

        for b in rows {
            if b.num_rows() == 1 && !b.column(0).is_null(0) {
                out.watermark = Some(b.column(0).slice(0, 1));
            }
        }
        let Some(_) = out.watermark else {
            return Ok(out);
        };

        // Keys sharing the maximum timestamp. Only these need individual resolution.
        if let Some(key) = key_column {
            let k = quote(key);
            let rows = ctx
                .sql(&format!(
                    "SELECT DISTINCT CAST({k} AS VARCHAR) AS k FROM target \
                     WHERE {ts} = (SELECT max({ts}) FROM target)"
                ))
                .await
                .map_err(|e| Error::Config(format!("dedup: key column {key:?}: {e}")))?
                .collect()
                .await
                .map_err(|e| Error::Config(format!("dedup: key column {key:?}: {e}")))?;
            for b in rows {
                let col = cast(b.column(0), &DataType::Utf8)
                    .map_err(|e| Error::Other(format!("dedup: key to text: {e}")))?;
                let col = col.as_any().downcast_ref::<StringArray>().expect("utf8");
                for i in 0..col.len() {
                    if !col.is_null(i) {
                        out.boundary_keys.insert(col.value(i).to_string());
                    }
                }
            }
        }
        Ok(out)
    }

    /// True when there is nothing to suppress, so `apply` can be skipped entirely.
    pub fn is_inert(&self) -> bool {
        self.watermark.is_none()
    }

    pub fn watermark_is_known(&self) -> bool {
        self.watermark.is_some()
    }

    /// The cut-off itself, for bounding how far back a rescan must reach.
    pub fn watermark(&self) -> Option<&ArrayRef> {
        self.watermark.as_ref()
    }

    pub fn boundary_key_count(&self) -> usize {
        self.boundary_keys.len()
    }

    /// Drop the rows the target already holds.
    pub fn apply(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let Some(mark) = &self.watermark else {
            return Ok(batch);
        };

        let ts = column(&batch, &self.timestamp_column)?;
        if ts.null_count() > 0 {
            return Err(Error::Schema(format!(
                "dedup timestamp {:?} contains {} null(s); a row with no timestamp can be \
                 neither kept nor skipped safely",
                self.timestamp_column,
                ts.null_count()
            )));
        }
        let scalar = deltalake::arrow::array::Scalar::new(mark.clone());

        let after = gt(&ts, &scalar).map_err(|e| {
            Error::Schema(format!(
                "dedup timestamp {:?}: cannot compare against the target's newest value; \
                 source and target must agree on its type ({e})",
                self.timestamp_column
            ))
        })?;

        let keep = match &self.key_column {
            // Rows exactly at the watermark are decided one by one.
            Some(key) => {
                let at = eq(&ts, &scalar).map_err(|e| Error::Schema(format!("dedup: {e}")))?;
                let unseen = self.keys_not_yet_present(&batch, key)?;
                or(
                    &after,
                    &and(&at, &unseen).map_err(|e| Error::Other(e.to_string()))?,
                )
                .map_err(|e| Error::Other(e.to_string()))?
            }
            None => after,
        };

        filter_record_batch(&batch, &keep)
            .map_err(|e| Error::Other(format!("dedup: filter failed: {e}")))
    }

    fn keys_not_yet_present(&self, batch: &RecordBatch, key: &str) -> Result<BooleanArray> {
        let col = column(batch, key)?;
        let text = cast(&col, &DataType::Utf8)
            .map_err(|e| Error::Schema(format!("dedup key {key:?} is not comparable: {e}")))?;
        let text = text.as_any().downcast_ref::<StringArray>().expect("utf8");
        Ok((0..text.len())
            .map(|i| Some(text.is_null(i) || !self.boundary_keys.contains(text.value(i))))
            .collect())
    }
}

fn column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let idx = batch.schema().index_of(name).map_err(|_| {
        Error::Schema(format!(
            "dedup column {name:?} is not in the transformed batch. Columns: [{}]",
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
    use deltalake::arrow::array::{Int64Array, TimestampMicrosecondArray};
    use deltalake::arrow::datatypes::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
        ]))
    }

    fn batch(rows: &[(i64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn mark(ts: i64) -> ArrayRef {
        Arc::new(TimestampMicrosecondArray::from(vec![ts])) as ArrayRef
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        let a = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        (0..a.len()).map(|i| a.value(i)).collect()
    }

    #[test]
    fn an_unknown_watermark_passes_everything() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            ..Default::default()
        };
        assert!(d.is_inert());
        assert_eq!(
            ids(&d.apply(batch(&[(1, 10), (2, 20)])).unwrap()),
            vec![1, 2]
        );
    }

    #[test]
    fn rows_older_than_the_watermark_are_dropped() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let out = d
            .apply(batch(&[(1, 10), (2, 20), (3, 30), (4, 40)]))
            .unwrap();
        assert_eq!(
            ids(&out),
            vec![3, 4],
            "20 is at the watermark, so it is covered"
        );
    }

    #[test]
    fn a_row_at_the_watermark_with_an_unseen_key_is_kept() {
        // The boundary case a bare `>` would silently drop: it arrived in the same
        // instant as the rebuild's newest row, but the rebuild never saw it.
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            key_column: Some("order_id".into()),
            watermark: Some(mark(20)),
            boundary_keys: ["2".to_string()].into_iter().collect(),
        };
        let out = d
            .apply(batch(&[(1, 10), (2, 20), (9, 20), (3, 30)]))
            .unwrap();
        assert_eq!(
            ids(&out),
            vec![9, 3],
            "2 is already in the target at t=20; 9 shares the instant but is new"
        );
    }

    #[test]
    fn without_a_key_ties_fall_back_to_strictly_greater() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let out = d.apply(batch(&[(2, 20), (9, 20), (3, 30)])).unwrap();
        assert_eq!(
            ids(&out),
            vec![3],
            "both rows at the boundary are assumed covered"
        );
    }

    #[test]
    fn a_null_timestamp_is_an_error_rather_than_a_guess() {
        let d = Dedup {
            timestamp_column: "_timestamp".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let b = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("order_id", DataType::Int64, false),
                Field::new(
                    "_timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(vec![None as Option<i64>])) as ArrayRef,
            ],
        )
        .unwrap();
        assert!(d.apply(b).is_err());
    }

    #[test]
    fn a_missing_timestamp_column_names_what_is_available() {
        let d = Dedup {
            timestamp_column: "nope".into(),
            watermark: Some(mark(20)),
            ..Default::default()
        };
        let e = d.apply(batch(&[(1, 10)])).unwrap_err().to_string();
        assert!(e.contains("nope"), "got: {e}");
        assert!(e.contains("order_id"), "should list the real columns: {e}");
    }
}

// ---------------------------------------------------------------- bounded rescan

/// A value from either side, reduced to something comparable.
///
/// Delta writes per-file statistics as JSON, so the target's watermark (an Arrow scalar)
/// and the source's `maxValues` (a JSON value) have to meet somewhere.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
enum Bound {
    Int(i64),
    Float(f64),
    Text(String),
}

/// Reduce the watermark to a comparable bound. `None` when its type is not one we can
/// line up against Delta statistics — the caller then falls back to a full rescan.
fn watermark_bound(watermark: &ArrayRef) -> Option<Bound> {
    use deltalake::arrow::array::{AsArray, Int64Array};
    use deltalake::arrow::datatypes::{Int64Type, TimeUnit};

    match watermark.data_type() {
        DataType::Timestamp(_, _) => {
            let us = cast(watermark, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
            let us =
                us.as_primitive_opt::<deltalake::arrow::datatypes::TimestampMicrosecondType>()?;
            (!us.is_null(0)).then(|| Bound::Int(us.value(0)))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let v = cast(watermark, &DataType::Int64).ok()?;
            let v = v.as_any().downcast_ref::<Int64Array>()?;
            (!v.is_null(0)).then(|| Bound::Int(v.value(0)))
        }
        DataType::Date32 | DataType::Date64 => {
            let v = cast(watermark, &DataType::Int64).ok()?;
            let v = v.as_primitive_opt::<Int64Type>()?;
            (!v.is_null(0)).then(|| Bound::Int(v.value(0)))
        }
        // A numeric sequence works as well as a clock, and Delta writes those stats as
        // JSON numbers.
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            let v = cast(watermark, &DataType::Float64).ok()?;
            let v = v.as_primitive_opt::<deltalake::arrow::datatypes::Float64Type>()?;
            (!v.is_null(0)).then(|| Bound::Float(v.value(0)))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let v = cast(watermark, &DataType::Utf8).ok()?;
            let v = v.as_string_opt::<i32>()?;
            (!v.is_null(0)).then(|| Bound::Text(v.value(0).to_string()))
        }
        _ => None,
    }
}

/// Interpret a Delta statistic in the same shape as the watermark.
fn stat_bound(stat: &serde_json::Value, like: &Bound) -> Option<Bound> {
    match like {
        Bound::Int(_) => match stat {
            serde_json::Value::Number(n) => n.as_i64().map(Bound::Int),
            // Timestamps are written as text, in more than one shape depending on writer.
            serde_json::Value::String(s) => parse_timestamp_micros(s).map(Bound::Int),
            _ => None,
        },
        Bound::Float(_) => stat.as_f64().map(Bound::Float),
        Bound::Text(_) => stat.as_str().map(|s| Bound::Text(s.to_string())),
    }
}

/// Microseconds since epoch, from the spellings Delta writers actually emit.
fn parse_timestamp_micros(s: &str) -> Option<i64> {
    use chrono::NaiveDateTime;

    const FORMATS: &[&str] = &[
        "%Y-%m-%dT%H:%M:%S%.fZ",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ];
    for f in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, f) {
            return dt.and_utc().timestamp_micros().into();
        }
    }
    None
}

/// The earliest source version that can still hold rows beyond `watermark`.
///
/// After a rebuild, `ddi` has to re-read the source far enough back to recover whatever
/// the rebuild wiped — but it does not know how far back that is. Reading from
/// `starting_version` is always correct and always wasteful: on a large bronze table it
/// re-reads history every night.
///
/// Delta records per-file `maxValues`, so the log can answer it directly. Walking
/// backwards from the head, the first commit whose newest row is already covered by the
/// watermark is the boundary: everything before it is covered too, because the timestamp
/// increases with arrival — which is the same assumption the filter already rests on.
///
/// Anything unexpected — statistics missing, a type that will not line up, a log that
/// runs out — returns `fallback`. Being slow is a cost; being wrong is not an option.
pub async fn bounded_rescan_start(
    source: &DeltaTable,
    timestamp_column: &str,
    watermark: &ArrayRef,
    fallback: Version,
    max_scan: u64,
) -> Result<Version> {
    use deltalake::kernel::Action;
    use deltalake::logstore::get_actions;

    let Some(mark) = watermark_bound(watermark) else {
        return Ok(fallback);
    };
    let Some(head) = source.version() else {
        return Ok(fallback);
    };
    let log = source.log_store();

    let mut v = head;
    let mut scanned = 0u64;
    loop {
        if scanned >= max_scan || v < fallback {
            return Ok(fallback);
        }
        let Some(raw) = log.read_commit_entry(v).await? else {
            return Ok(fallback); // log truncated under us
        };
        let actions = get_actions(v, &raw)?;

        // Highest value this commit added. A commit that added nothing (compaction, a
        // txn marker) says nothing about coverage, so keep walking.
        let mut commit_max: Option<Bound> = None;
        let mut saw_add = false;
        for a in &actions {
            let Action::Add(add) = a else { continue };
            if !add.data_change {
                continue;
            }
            saw_add = true;
            let Some(stats) = add.stats.as_deref() else {
                return Ok(fallback); // no statistics: cannot reason, so re-read it all
            };
            let parsed: serde_json::Value = match serde_json::from_str(stats) {
                Ok(p) => p,
                Err(_) => return Ok(fallback),
            };
            let Some(stat) = parsed
                .get("maxValues")
                .and_then(|m| m.get(timestamp_column))
            else {
                return Ok(fallback);
            };
            let Some(b) = stat_bound(stat, &mark) else {
                return Ok(fallback);
            };
            commit_max = Some(match commit_max {
                Some(cur) if cur > b => cur,
                _ => b,
            });
        }

        if saw_add {
            if let Some(cmax) = commit_max {
                // Everything this commit added is already in the target, so everything
                // before it is too.
                if cmax <= mark {
                    return Ok(v.saturating_add(1));
                }
            }
        }

        if v == 0 {
            return Ok(fallback);
        }
        v -= 1;
        scanned += 1;
    }
}
