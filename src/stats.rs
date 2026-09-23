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
//!
//! # Decimals
//!
//! A DECIMAL reaches a [`Bound`] only as a nearby double, and on both sides of the
//! comparison. This side is Arrow's cast, which divides the unscaled integer by `10^scale`
//! in floating point. The statistic's side is whatever its writer recorded: delta-rs writes
//! that same division, and steps a fixed-length decimal one ULP toward zero when rounding
//! gains it an integer digit; Spark writes the exact decimal text, which reads as the
//! nearest double; a checkpoint's `stats_parsed`, re-serialised when the raw string is
//! gone, has been truncated to the scale on the way in. The two sides do not agree bit for
//! bit, and two different decimals can even share one double — DECIMAL(38,18)
//! `0.403800000000000050` and `0.403800000000000051` do.
//!
//! So nothing is excluded, and no window drawn, on the assumption that they agree. Every
//! decision taken from a DECIMAL bound goes through [`Slack`]: one unit of the column's
//! scale, which covers a reader's truncation, plus [`Slack::ULPS`] representable doubles,
//! which cover the few ULPs each conversion can be off by. That widens a range by one unit
//! and about 4e-15 of its value: a cost in what is read, never in what is found.

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

/// How far a DECIMAL bound may lie from the value it stands for. See the module's
/// "Decimals" section.
///
/// Only DECIMAL columns get one. Integers, clocks and text compare exactly, text's truncation
/// aside, which [`Bound::provably_below`] already handles; a DOUBLE is written and read back
/// as exactly the double it is.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Slack {
    /// One unit of the column's scale: `0.01` for DECIMAL(10,2).
    unit: f64,
}

impl Slack {
    /// Representable doubles added beyond the unit. The conversions on either side are off
    /// by two or three ULPs up to scale 22; past it `10f64.powi(scale)` is itself rounded
    /// and adds a few more, and delta-rs's fixed-length step and a reader without a
    /// correctly rounded float parser add one each. Sixteen covers all of it with room.
    pub const ULPS: u32 = 16;

    /// `Some` for a DECIMAL column, `None` for every type whose bounds are exact.
    pub fn of(dtype: &DataType) -> Option<Self> {
        match dtype {
            DataType::Decimal32(_, s)
            | DataType::Decimal64(_, s)
            | DataType::Decimal128(_, s)
            | DataType::Decimal256(_, s) => Some(Self {
                unit: 10f64.powi(-i32::from(*s)),
            }),
            _ => None,
        }
    }

    /// The lowest value `b` could stand for.
    pub fn below(&self, b: &Bound) -> Bound {
        match b {
            Bound::Float(v) => {
                Bound::Float((0..Self::ULPS).fold(v - self.unit, |x, _| next_toward(x, false)))
            }
            other => other.clone(),
        }
    }

    /// The highest value `b` could stand for.
    pub fn above(&self, b: &Bound) -> Bound {
        match b {
            Bound::Float(v) => {
                Bound::Float((0..Self::ULPS).fold(v + self.unit, |x, _| next_toward(x, true)))
            }
            other => other.clone(),
        }
    }
}

/// The next representable double above (`up`) or below `v`.
///
/// `f64::next_up` and `f64::next_down` would do, but they arrived in Rust 1.86 and this crate
/// declares 1.85. NaN and the infinities come back unchanged.
fn next_toward(v: f64, up: bool) -> f64 {
    if v.is_nan() || v.is_infinite() {
        return v;
    }
    if v == 0.0 {
        let least = f64::from_bits(1);
        return if up { least } else { -least };
    }
    // Stepping away from zero is one more in the bit pattern, whichever the sign.
    let bits = v.to_bits();
    f64::from_bits(if (v > 0.0) == up { bits + 1 } else { bits - 1 })
}

/// Reduce a one-element Arrow array to a comparable bound.
///
/// `None` when its type is not one that can be lined up against Delta statistics — the
/// caller then falls back to reading everything.
///
/// A DECIMAL becomes Arrow's own division of its unscaled integer by `10^scale`, which is
/// not the nearest double but is what delta-rs records for the same value, bit for bit, so
/// ddi's own files line up without help. Nothing rests on that match: other writers record
/// other doubles, so every decision drawn from a DECIMAL bound allows for the difference
/// through [`Slack`]. See the module's "Decimals" section.
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
///
/// A DOUBLE statistic reads back as exactly the double its writer recorded, which the upsert
/// window depends on: a sequence minimum read one ULP high puts `>= lo` above the row it came
/// from. That holds only because serde_json is built with `float_roundtrip`; its default
/// parser is one ULP off for the 17-digit spellings such doubles are written with (issue
/// #12). A DECIMAL statistic is only near its value however it is read; see [`Slack`].
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

impl ColumnStats {
    /// Widened by `slack`, when there is one: the minimum to the lowest value it could stand
    /// for, the maximum to the highest.
    pub fn loosened(self, slack: Option<Slack>) -> Self {
        let Some(s) = slack else {
            return self;
        };
        Self {
            min: self.min.map(|b| s.below(&b)),
            max: self.max.map(|b| s.above(&b)),
        }
    }
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

    /// Doubles that need all 17 significant digits to round-trip, as a serialiser writes
    /// them. The four from issue #12.
    const SEVENTEEN_DIGITS: [&str; 4] = [
        "0.49979999999999997",
        "0.9017000000000001",
        "0.45909999999999995",
        "0.40380000000000005",
    ];

    fn float(b: Option<Bound>) -> f64 {
        match b {
            Some(Bound::Float(v)) => v,
            other => panic!("expected a float bound, got {other:?}"),
        }
    }

    #[test]
    fn a_float_statistic_reads_back_as_the_double_that_was_written() {
        // A DOUBLE sequence's minimum read one ULP high draws the window's `>= lo` above the
        // row it came from, and the key is inserted a second time. serde_json's default
        // parser does exactly that for these spellings; `float_roundtrip` does not.
        for text in SEVENTEEN_DIGITS {
            let stat: serde_json::Value = serde_json::from_str(text).unwrap();
            let got = float(bound_of_stat(&stat, &Bound::Float(0.0)));
            let want: f64 = text.parse().unwrap();
            assert_eq!(
                got.to_bits(),
                want.to_bits(),
                "{text} read as {got:?}, not as the double that was written"
            );
        }

        let stats: serde_json::Value = serde_json::from_str(
            r#"{"minValues":{"seq":0.40380000000000005},"maxValues":{"seq":0.9017000000000001}}"#,
        )
        .unwrap();
        let got = column_stats(&stats, "seq", &Bound::Float(0.0));
        assert_eq!(
            float(got.min).to_bits(),
            "0.40380000000000005".parse::<f64>().unwrap().to_bits()
        );
        assert_eq!(
            float(got.max).to_bits(),
            "0.9017000000000001".parse::<f64>().unwrap().to_bits()
        );
    }

    #[test]
    fn a_decimal_statistic_from_any_writer_still_touches_its_key() {
        // One DECIMAL(38,17) row per file, min = max, spelled as each writer spells it. The
        // key is what ddi makes of the same value. Every spelling must still reach it.
        use deltalake::arrow::array::Decimal128Array;
        use std::sync::Arc;

        let dtype = DataType::Decimal128(38, 17);
        let slack = Slack::of(&dtype);
        // The fraction written out to the scale, as a checkpoint's `stats_parsed` holds it
        // after reading `text` as a decimal: extra digits truncated, missing ones zero.
        let to_scale = |text: &str| {
            let (int, frac) = text.split_once('.').unwrap();
            let frac: String = frac
                .chars()
                .chain(std::iter::repeat('0'))
                .take(17)
                .collect();
            format!("{int}.{frac}")
        };

        for (unscaled, exact) in [
            (49979999999999997i128, "0.49979999999999997"),
            (92030920993190389, "0.92030920993190389"),
        ] {
            let value: ArrayRef = Arc::new(
                Decimal128Array::from(vec![unscaled])
                    .with_precision_and_scale(38, 17)
                    .unwrap(),
            );
            let key = bound_of_scalar(&value).unwrap();
            let delta_rs = serde_json::to_string(&(unscaled as f64 / 1e17)).unwrap();
            let checkpoint = to_scale(&delta_rs);
            for (writer, text) in [
                ("delta-rs", delta_rs.as_str()),
                ("Spark", exact),
                ("a checkpoint", checkpoint.as_str()),
            ] {
                let stat = serde_json::from_str::<serde_json::Value>(text).unwrap();
                let stats =
                    serde_json::json!({"minValues": {"k": stat.clone()}, "maxValues": {"k": stat}});
                let file = column_stats(&stats, "k", &key).loosened(slack);
                assert!(
                    range_touches_any(
                        file.min.as_ref().unwrap(),
                        file.max.as_ref().unwrap(),
                        std::slice::from_ref(&key)
                    ),
                    "{writer}'s {text} for {exact} no longer touches the key {key:?}"
                );
            }
        }

        // Why the slack is needed at all: Spark's exact text reads as the nearest double,
        // one ULP below the one Arrow's division gives the key, and without the slack the
        // file holding the key would be ruled out.
        let value: ArrayRef = Arc::new(
            Decimal128Array::from(vec![49979999999999997i128])
                .with_precision_and_scale(38, 17)
                .unwrap(),
        );
        let key = bound_of_scalar(&value).unwrap();
        let spark = Bound::Float("0.49979999999999997".parse().unwrap());
        assert!(
            !range_touches_any(&spark, &spark, std::slice::from_ref(&key)),
            "the two sides agree after all, and this test no longer shows why Slack exists"
        );
    }

    #[test]
    fn a_slack_is_only_for_decimals_and_moves_at_least_a_unit() {
        use deltalake::arrow::datatypes::TimeUnit;

        for exact in [
            DataType::Int64,
            DataType::Float64,
            DataType::Float32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Utf8,
        ] {
            assert_eq!(Slack::of(&exact), None, "{exact} compares exactly");
        }
        assert!(Slack::of(&DataType::Decimal128(38, 17)).is_some());
        assert!(Slack::of(&DataType::Decimal256(76, 10)).is_some());

        let cents = Slack::of(&DataType::Decimal128(10, 2)).unwrap();
        assert!(float(Some(cents.below(&Bound::Float(12.34)))) <= 12.33);
        assert!(float(Some(cents.above(&Bound::Float(12.34)))) >= 12.35);

        // Past the unit, which at this scale is less than one ULP of 0.4.
        let fine = Slack::of(&DataType::Decimal128(38, 17)).unwrap();
        let lowered = float(Some(fine.below(&Bound::Float(0.4))));
        assert!(lowered.to_bits() + u64::from(Slack::ULPS) <= 0.4f64.to_bits());
        let raised = float(Some(fine.above(&Bound::Float(0.4))));
        assert!(raised.to_bits() >= 0.4f64.to_bits() + u64::from(Slack::ULPS));

        // Only floats move.
        assert_eq!(cents.below(&Bound::Int(7)), Bound::Int(7));
        assert_eq!(cents.above(&t("order")), t("order"));

        // Across zero and below it.
        let whole = Slack::of(&DataType::Decimal128(10, 0)).unwrap();
        assert!(float(Some(whole.below(&Bound::Float(0.0)))) < -1.0);
        assert!(float(Some(whole.above(&Bound::Float(-1.0)))) > 0.0);
        assert!(float(Some(whole.below(&Bound::Float(-7.0)))) < -8.0);
        assert_eq!(next_toward(0.0, true), f64::from_bits(1));
        assert_eq!(next_toward(0.0, false), -f64::from_bits(1));
        assert_eq!(next_toward(f64::from_bits(1), false), 0.0);
        assert_eq!(next_toward(-f64::from_bits(1), true), 0.0);
        assert!(next_toward(-1.0, false) < -1.0 && next_toward(-1.0, true) > -1.0);
        assert!(next_toward(1.0, false) < 1.0 && next_toward(1.0, true) > 1.0);
        assert_eq!(next_toward(f64::INFINITY, true), f64::INFINITY);
        assert!(next_toward(f64::NAN, true).is_nan());
    }
}
