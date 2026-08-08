//! The resumable stream position.

use std::cmp::Ordering;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Delta commit version. Matches `deltalake::kernel::Version` (`u64`).
///
/// Note: the design sketch used `i64`. delta-rs 0.32 types log versions as `u64`, and a
/// negative version is meaningless, so this follows the library. The Delta `txn` action's
/// `version` field *is* `i64` — conversion happens only at that boundary, in [`crate::offset`].
pub type Version = u64;

/// A resumable position in a table's commit history.
///
/// Read it as *the next thing to consume*: `(version, index)` means "version `version` is
/// the next commit to read, and within it, `index` is the next unconsumed `dataChange=true`
/// `Add` action". `index == 0` means the commit has not been started.
///
/// Ordering is total and lexicographic on `(version, index)`, which is what makes a
/// persisted cursor comparable across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamCursor {
    pub version: Version,
    pub index: usize,
}

impl StreamCursor {
    /// A cursor at the start of `version`.
    pub const fn at_version(version: Version) -> Self {
        Self { version, index: 0 }
    }

    pub const fn new(version: Version, index: usize) -> Self {
        Self { version, index }
    }

    /// The cursor positioned at the start of the following commit.
    ///
    /// Used when a commit has been consumed in full; it is always preferable to
    /// `(version, n_files)` because it does not require re-reading `version` to discover
    /// that nothing is left in it.
    pub const fn next_version(&self) -> Self {
        Self {
            version: self.version + 1,
            index: 0,
        }
    }

    /// Advance within the current commit.
    pub const fn advanced_by(&self, n: usize) -> Self {
        Self {
            version: self.version,
            index: self.index + n,
        }
    }

    /// True when this cursor sits on a commit boundary rather than part-way through one.
    ///
    /// The v1 daemon requires this: a boundary cursor is fully described by a bare version
    /// number, which is what the Delta `txn` action can store. See plan §2.3.
    pub const fn is_commit_boundary(&self) -> bool {
        self.index == 0
    }

    /// The last version fully consumed by a stream that has reached this cursor.
    ///
    /// `None` when the cursor is mid-commit (nothing is *fully* consumed yet at
    /// `self.version`) or when the cursor sits at version 0 having consumed nothing.
    pub const fn last_fully_consumed_version(&self) -> Option<Version> {
        if !self.is_commit_boundary() {
            return None;
        }
        match self.version {
            0 => None,
            v => Some(v - 1),
        }
    }
}

impl Ord for StreamCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.version
            .cmp(&other.version)
            .then(self.index.cmp(&other.index))
    }
}

impl PartialOrd for StreamCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl fmt::Display for StreamCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}+{}", self.version, self.index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordering_is_lexicographic_and_total() {
        let a = StreamCursor::new(1, 0);
        let b = StreamCursor::new(1, 5);
        let c = StreamCursor::new(2, 0);
        assert!(a < b, "same version, higher index sorts later");
        assert!(b < c, "version dominates index");
        assert!(a < c);
        assert_eq!(a.cmp(&a), Ordering::Equal);
    }

    #[test]
    fn index_never_makes_a_lower_version_sort_higher() {
        // The trap a naive `version + index` comparison falls into.
        let big_index_low_version = StreamCursor::new(1, 100_000);
        let start_of_next = StreamCursor::new(2, 0);
        assert!(big_index_low_version < start_of_next);
    }

    #[test]
    fn next_version_resets_the_index() {
        assert_eq!(
            StreamCursor::new(7, 42).next_version(),
            StreamCursor::new(8, 0)
        );
    }

    #[test]
    fn advanced_by_stays_within_the_commit() {
        assert_eq!(
            StreamCursor::new(3, 2).advanced_by(4),
            StreamCursor::new(3, 6)
        );
    }

    #[test]
    fn commit_boundary_detection() {
        assert!(StreamCursor::at_version(9).is_commit_boundary());
        assert!(!StreamCursor::new(9, 1).is_commit_boundary());
    }

    #[test]
    fn last_fully_consumed_version_semantics() {
        // Sitting at the start of v5 means v4 was the last one finished.
        assert_eq!(
            StreamCursor::at_version(5).last_fully_consumed_version(),
            Some(4)
        );
        // Nothing consumed yet.
        assert_eq!(
            StreamCursor::at_version(0).last_fully_consumed_version(),
            None
        );
        // Mid-commit: v5 is only partly done, so nothing at v5 is complete.
        assert_eq!(StreamCursor::new(5, 3).last_fully_consumed_version(), None);
    }

    #[test]
    fn round_trips_through_json() {
        let c = StreamCursor::new(12, 34);
        let s = serde_json::to_string(&c).unwrap();
        assert_eq!(serde_json::from_str::<StreamCursor>(&s).unwrap(), c);
    }

    #[test]
    fn display_is_stable_for_logs() {
        assert_eq!(StreamCursor::new(12, 34).to_string(), "v12+34");
    }
}
