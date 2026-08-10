//! Reading Delta's per-file statistics, and comparing them against Arrow values.
//!
//! Delta records `minValues`/`maxValues` per file as JSON, in the log. Two parts of this
//! tool reason from them rather than from the data:
//!
//! - [`crate::dedup::bounded_rescan_start`] asks the **source** log how far back a rescan
//!   has to reach.
//! - [`crate::upsert`] asks the **target** log which of its files could hold the keys in
//!   hand, so a MERGE can be told to leave the rest alone.
//!
//! Both need the same thing: a JSON statistic and an Arrow value reduced to something
//! comparable. That is [`Bound`].
//!
//! # Truncation
//!
//! String statistics are not required to be exact. A writer may truncate them — Spark
//! truncates at 32 characters — so a recorded `maxValues` of `"order-00000000000000000000"`
//! stands for *any* string beginning with it. Ruling a file out by comparing against the
//! truncated value directly would rule out files that really do hold the value. Every
//! comparison that can exclude data therefore goes through [`Bound::provably_below`],
//! which answers only when the answer is provable. See [`ranges_can_overlap`].

use deltalake::arrow::array::{Array, ArrayRef};
use deltalake::arrow::compute::cast;
use deltalake::arrow::datatypes::DataType;

/// A value from either side, reduced to something comparable.
///
/// Delta writes per-file statistics as JSON, so an Arrow scalar and a `maxValues` entry
/// have to meet somewhere.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum Bound {
    Int(i64),
    Float(f64),
    Text(String),
}

impl Bound {
    /// True when `self` is provably less than `other`, **allowing for a truncated string**.
    ///
    /// For numbers this is just `<`. For text it is the part that matters: a truncated
    /// statistic `a` stands for the true value `a·s` for some unknown suffix `s`. If `a` is
    /// a prefix of `other`, the suffix could carry the true value past `other` (`"ord"` is
    /// below `"order"`, but `"ordz"` is not), so nothing is provable. Otherwise `a < other`
    /// at some character before either string runs out, and every extension of `a` differs
    /// there in the same direction — so the whole family is below `other`.
    pub fn provably_below(&self, other: &Bound) -> bool {
        match (self, other) {
            (Bound::Text(a), Bound::Text(b)) => a < b && !b.starts_with(a.as_str()),
            _ => self < other,
        }
    }
}

/// Could a file whose statistics report `[file_min, file_max]` hold a value inside
/// `[want_min, want_max]`?
///
/// Answers `false` only when disjointness is provable; anything unproven is `true`, because
/// the caller uses this to *exclude* data and a wrong exclusion is a lost update.
///
/// The two sides are deliberately asymmetric. Truncating a minimum downwards keeps it a
/// valid lower bound, so `file_min > want_max` needs no special care. Truncating a maximum
/// keeps a *prefix*, not a bound, so the left-hand test goes through
/// [`Bound::provably_below`].
pub fn ranges_can_overlap(
    file_min: &Bound,
    file_max: &Bound,
    want_min: &Bound,
    want_max: &Bound,
) -> bool {
    let below = file_max.provably_below(want_min);
    let above = want_max.provably_below(file_min);
    !(below || above)
}

/// Could a file reporting `[file_min, file_max]` hold **any** of `wanted`?
///
/// `wanted` must be sorted ascending and hold no duplicates.
///
/// Testing against the set rather than against its min and max is what keeps one outlier
/// key from opening the window onto the whole table. A batch of recent orders plus a single
/// re-delivered ancient one spans nearly the entire key space as a range, but as a set it
/// still touches only two regions of it.
///
/// # Why one comparison is enough
///
/// `i` is the first key not below the file's minimum. Everything before it is genuinely
/// below — truncating a minimum only ever lowers it, so a key under the recorded minimum is
/// under the real one too. Everything from `i` on is `>= file_min`, so the only question
/// left is whether the smallest of them is under the file's maximum; if `wanted[i]` is
/// provably above `file_max`, so is every key after it. That last step needs the ordering
/// argument behind [`Bound::provably_below`]: a longer key cannot slip back under a
/// truncated maximum that a shorter one cleared.
pub fn range_touches_any(file_min: &Bound, file_max: &Bound, wanted: &[Bound]) -> bool {
    use std::cmp::Ordering;

    let i = wanted.partition_point(|k| matches!(k.partial_cmp(file_min), Some(Ordering::Less)));
    match wanted.get(i) {
        Some(k) => !file_max.provably_below(k),
        None => false, // every key sits below this file
    }
}

/// Reduce a one-element Arrow array to a comparable bound.
///
/// `None` when its type is not one that can be lined up against Delta statistics — the
/// caller then falls back to reading everything.
pub fn bound_of_scalar(value: &ArrayRef) -> Option<Bound> {
    use deltalake::arrow::array::{AsArray, Int64Array};
    use deltalake::arrow::datatypes::{Float64Type, TimeUnit, TimestampMicrosecondType};

    match value.data_type() {
        DataType::Timestamp(_, _) => {
            let us = cast(value, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
            let us = us.as_primitive_opt::<TimestampMicrosecondType>()?;
            (!us.is_null(0)).then(|| Bound::Int(us.value(0)))
        }
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let v = cast(value, &DataType::Int64).ok()?;
            let v = v.as_any().downcast_ref::<Int64Array>()?;
            (!v.is_null(0)).then(|| Bound::Int(v.value(0)))
        }
        // Normalised to microseconds, not left as days or milliseconds, so it lines up
        // with the `"2026-08-10"` a Delta writer records for the same column. Comparing
        // days against parsed midnight-microseconds would be off by a factor of 86.4
        // billion and silently rule out every file.
        DataType::Date32 | DataType::Date64 => {
            let us = cast(value, &DataType::Timestamp(TimeUnit::Microsecond, None)).ok()?;
            let us = us.as_primitive_opt::<TimestampMicrosecondType>()?;
            (!us.is_null(0)).then(|| Bound::Int(us.value(0)))
        }
        // A numeric sequence works as well as a clock, and Delta writes those stats as
        // JSON numbers.
        DataType::Float32 | DataType::Float64 | DataType::Decimal128(_, _) => {
            let v = cast(value, &DataType::Float64).ok()?;
            let v = v.as_primitive_opt::<Float64Type>()?;
            (!v.is_null(0)).then(|| Bound::Float(v.value(0)))
        }
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
            let v = cast(value, &DataType::Utf8).ok()?;
            let v = v.as_string_opt::<i32>()?;
            (!v.is_null(0)).then(|| Bound::Text(v.value(0).to_string()))
        }
        _ => None,
    }
}

/// Interpret a Delta statistic in the same shape as `like`.
pub fn bound_of_stat(stat: &serde_json::Value, like: &Bound) -> Option<Bound> {
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
pub fn parse_timestamp_micros(s: &str) -> Option<i64> {
    use chrono::{NaiveDate, NaiveDateTime};

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
    // A `date` column: delta-rs writes its statistics as a bare `"%Y-%m-%d"`, with no time
    // part, so none of the formats above match it. Read as midnight, which is what
    // [`bound_of_scalar`] produces for the Arrow side of the same column.
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .map(|d| d.and_time(Default::default()).and_utc().timestamp_micros())
}

/// The `minValues`/`maxValues` recorded for one column of one file.
///
/// `None` for either side means the writer did not record it — for a column past
/// `delta.dataSkippingNumIndexedCols` (32 by default), or one excluded by
/// `delta.dataSkippingStatsColumns`. Callers must treat that as "unknown", never as "empty".
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min: Option<Bound>,
    pub max: Option<Bound>,
}

/// Pull one column's statistics out of a file's `stats` JSON, shaped like `like`.
pub fn column_stats(stats: &serde_json::Value, column: &str, like: &Bound) -> ColumnStats {
    let side = |which: &str| {
        stats
            .get(which)
            .and_then(|m| m.get(column))
            .and_then(|s| bound_of_stat(s, like))
    };
    ColumnStats {
        min: side("minValues"),
        max: side("maxValues"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> Bound {
        Bound::Text(s.into())
    }

    #[test]
    fn numbers_compare_directly() {
        assert!(Bound::Int(1).provably_below(&Bound::Int(2)));
        assert!(!Bound::Int(2).provably_below(&Bound::Int(2)));
        assert!(!Bound::Int(3).provably_below(&Bound::Int(2)));
    }

    #[test]
    fn a_truncated_maximum_that_prefixes_the_target_proves_nothing() {
        // The whole reason this function exists. Spark records max = "ord" for a file whose
        // real maximum is "ordz". Concluding "ord" < "order", therefore the file cannot hold
        // "order", would skip the file that holds the row we are about to update — and we
        // would insert a duplicate instead.
        assert!(
            !t("ord").provably_below(&t("order")),
            "a prefix could extend past the target"
        );
        assert!(
            t("orc").provably_below(&t("order")),
            "differs before either runs out, so every extension differs the same way"
        );
    }

    #[test]
    fn overlap_is_only_denied_when_provable() {
        // [a, c] vs [x, z] — disjoint and provably so.
        assert!(!ranges_can_overlap(&t("a"), &t("c"), &t("x"), &t("z")));
        // [a, c] vs [b, d] — genuinely overlapping.
        assert!(ranges_can_overlap(&t("a"), &t("c"), &t("b"), &t("d")));
        // File max "ord" is a prefix of the wanted min "order": unprovable, so keep it.
        assert!(
            ranges_can_overlap(&t("a"), &t("ord"), &t("order"), &t("orderz")),
            "a truncated maximum must never exclude a file"
        );
    }

    #[test]
    fn one_outlier_key_does_not_drag_every_file_in() {
        // The reason the set is tested rather than its span. Keys 100..103 plus a single
        // re-delivered key 1: as a range that is [1, 103] and touches everything, but the
        // file holding 40..60 genuinely holds none of them.
        let wanted: Vec<Bound> = [1, 100, 101, 102, 103]
            .into_iter()
            .map(Bound::Int)
            .collect();
        assert!(
            !range_touches_any(&Bound::Int(40), &Bound::Int(60), &wanted),
            "no wanted key lies in [40, 60]"
        );
        assert!(
            ranges_can_overlap(&Bound::Int(40), &Bound::Int(60), &wanted[0], &wanted[4]),
            "the span-based test cannot tell, which is exactly the weakness"
        );
        assert!(range_touches_any(&Bound::Int(0), &Bound::Int(5), &wanted));
        assert!(range_touches_any(
            &Bound::Int(99),
            &Bound::Int(200),
            &wanted
        ));
    }

    #[test]
    fn the_set_test_keeps_a_file_a_truncated_maximum_cannot_rule_out() {
        let wanted = vec![t("order")];
        assert!(
            range_touches_any(&t("a"), &t("ord"), &wanted),
            "\"ord\" may be a truncation of something above \"order\""
        );
        assert!(
            !range_touches_any(&t("a"), &t("orc"), &wanted),
            "\"orc\" is provably below, prefix or not"
        );
    }

    #[test]
    fn a_file_below_every_wanted_key_is_excluded() {
        let wanted: Vec<Bound> = [10, 20].into_iter().map(Bound::Int).collect();
        assert!(!range_touches_any(&Bound::Int(1), &Bound::Int(5), &wanted));
    }

    #[test]
    fn a_file_entirely_above_the_wanted_range_is_excluded() {
        assert!(!ranges_can_overlap(
            &Bound::Int(100),
            &Bound::Int(200),
            &Bound::Int(1),
            &Bound::Int(50)
        ));
    }

    #[test]
    fn missing_statistics_are_unknown_not_empty() {
        let s: serde_json::Value = serde_json::json!({"maxValues": {"id": 9}});
        let got = column_stats(&s, "id", &Bound::Int(0));
        assert_eq!(got.max, Some(Bound::Int(9)));
        assert!(got.min.is_none(), "absent must not read as a bound");
    }

    #[test]
    fn timestamps_are_read_from_the_spellings_writers_emit() {
        let want = chrono::NaiveDate::from_ymd_opt(2026, 8, 10)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_micros();
        for spelling in [
            "2026-08-10T12:00:00Z",
            "2026-08-10T12:00:00",
            "2026-08-10 12:00:00",
            "2026-08-10T12:00:00.000Z",
        ] {
            assert_eq!(parse_timestamp_micros(spelling), Some(want), "{spelling}");
        }
        assert!(parse_timestamp_micros("not a time").is_none());
    }

    #[test]
    fn a_date_statistic_reads_as_midnight_on_that_day() {
        // delta-rs writes a `date` column's statistics as a bare "2026-08-10". Failing to
        // parse it does not error — it silently drops the bound and reads the whole table,
        // which is why this is worth pinning.
        use deltalake::arrow::array::Date32Array;
        use std::sync::Arc;

        let want = parse_timestamp_micros("2026-08-10").expect("a bare date must parse");
        let days = (chrono::NaiveDate::from_ymd_opt(2026, 8, 10).unwrap()
            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
        let arrow: ArrayRef = Arc::new(Date32Array::from(vec![days]));
        assert_eq!(
            bound_of_scalar(&arrow),
            Some(Bound::Int(want)),
            "the Arrow side and the statistic side of a date column must agree"
        );
    }
}
