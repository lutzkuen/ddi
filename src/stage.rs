//! The table between the two halves of a staged upsert, and the naming that keeps them apart.
//!
//! # Why a staged upsert exists
//!
//! A direct upsert pays for the target on every batch. That is the right trade when a batch
//! touches a few files, and the wrong one for a high-cardinality current-state stream: 5,000
//! rows carrying random keys touch every file in the target, so the merge rewrites the whole
//! state to apply a rounding error's worth of change. Nothing about the batch makes that
//! cheaper — not better statistics, not sorting, not `upsert_lookback`, not a smaller batch,
//! which only multiplies a fixed cost by more batches.
//!
//! The cost is per *merge*, so the fix is fewer merges. Splitting the pipeline in two is what
//! buys that:
//!
//! ```text
//! source ──▶ ingest ──▶ <target>__ddi_stage ──▶ apply ──▶ target
//!            append,                            merge,
//!            per commit                         per accumulation
//! ```
//!
//! The ingest half keeps the latency and the cheapness: transform, lookups, coercion,
//! data-quality handling, append. The apply half accumulates many staged commits and pays
//! the target's price once for all of them.
//!
//! # Why it is two pipelines rather than one pipeline with two modes
//!
//! Because everything the two halves need already exists, and none of it agrees on what a
//! pipeline is if a pipeline can be halfway through itself. Exactly-once, cursor resume,
//! backoff, the metrics, the merge window, the data-quality table: all of it is written
//! against one source, one target, one `txn` offset. Expanding a staged pipeline into two
//! ordinary ones at config time means every one of those keeps working unchanged, and the
//! two offsets the design requires are simply the two pipelines' own.
//!
//! It also means the failure modes are the ones already understood. The stage is append-only,
//! so the apply half reads a table whose commits never rewrite anything; a compaction of the
//! stage is `dataChange: false` and skipped in the reader that already skips them, while a
//! genuine rewrite is a change commit that stops the apply half loudly under the default
//! `change_policy`.
//!
//! # What this costs
//!
//! The target is eventually consistent, by exactly the apply half's accumulation window.
//! That is the point rather than a regrettable side effect, and it is the number an operator
//! has to be told: see `apply_max_latency`.

/// Appended to a target's URI to find the stage that feeds it.
///
/// The same shape as [`crate::dq::SUFFIX`], so a lake's `__ddi_` tables read as one family.
pub const SUFFIX: &str = "__ddi_stage";

/// Where a staged pipeline's rows wait, unless it says otherwise.
pub fn uri_for(target_uri: &str) -> String {
    format!("{}{SUFFIX}", target_uri.trim_end_matches('/'))
}

/// The name of the half that reads the real source and appends to the stage.
pub fn ingest_name(name: &str) -> String {
    format!("{name}__ingest")
}

/// The name of the half that reads the stage and merges into the real target.
pub fn apply_name(name: &str) -> String {
    format!("{name}__apply")
}

/// The `txn` app id the raw→stage offset is stored under.
///
/// Distinct from the apply half's, and that distinction is the exactly-once story: the two
/// halves commit to different tables at different times, and an offset that both advanced
/// would let one of them resume from a version the other had reached.
pub fn ingest_app_id(app_id: &str) -> String {
    format!("{app_id}.ingest")
}

/// The `txn` app id the stage→target offset is stored under.
pub fn apply_app_id(app_id: &str) -> String {
    format!("{app_id}.apply")
}

/// True when `uri` is a stage table, by the only rule anything else can apply.
pub fn is_stage_uri(uri: &str) -> bool {
    uri.trim_end_matches('/').ends_with(SUFFIX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_stage_sits_beside_its_target() {
        assert_eq!(
            uri_for("abfss://lake/silver/style"),
            "abfss://lake/silver/style__ddi_stage"
        );
        // A trailing slash is a path, not a different table.
        assert_eq!(
            uri_for("abfss://lake/silver/style/"),
            "abfss://lake/silver/style__ddi_stage"
        );
    }

    #[test]
    fn a_stage_is_recognisable_from_its_uri_alone() {
        // The feedback-loop guard has nothing else to go on: it runs over configuration,
        // before any table has been opened.
        assert!(is_stage_uri(&uri_for("s3://lake/orders")));
        assert!(is_stage_uri("s3://lake/orders__ddi_stage/"));
        assert!(!is_stage_uri("s3://lake/orders"));
        assert!(!is_stage_uri("s3://lake/orders__ddi_dq"));
    }

    #[test]
    fn the_two_halves_never_share_an_offset() {
        // One `txn` app id advanced by both halves would let either resume from a version
        // the other had reached. Nothing downstream can repair that, so it is a naming rule.
        assert_ne!(ingest_app_id("style"), apply_app_id("style"));
        assert_ne!(ingest_name("style"), apply_name("style"));
        // And neither collides with the undivided pipeline's own id.
        assert_ne!(ingest_app_id("style"), "style");
        assert_ne!(apply_app_id("style"), "style");
    }
}
