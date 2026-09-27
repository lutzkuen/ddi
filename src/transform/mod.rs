//! Stateless, row-local transforms.
//!
//! Everything here is validated at config load ([`validate`]) so that a transform which
//! could not be correct never gets the chance to run. The contract is narrow on purpose:
//! rows in, rows out, no memory of previous batches.
//!
//! Row-locality is also what lets one bad value cost one row. When a batch fails on a value,
//! [`Transform::apply_isolating`] evaluates it in parts until the rows that fail alone are
//! found, and the rest is the batch's answer only because every output row comes from one
//! input row. A transform that cannot promise that says so through [`Transform::cross_row`],
//! and is never evaluated in parts.

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;

use crate::error::Result;
use crate::lookup::LookupSnapshot;
use crate::schema::Rejected;

pub mod decimal;
pub mod dialect;
pub mod json;
pub mod json_build;
pub mod jsonval;
pub mod lambda;
pub mod sql;
pub mod udf;
pub mod unnest;
pub mod validate;

pub use sql::SqlTransform;
pub use validate::validate_sql;

/// The escape hatch for users who need real Rust without forking.
///
/// Implementations MUST be stateless across calls: `apply` may be invoked on batches in
/// any order after a restart, and anything remembered between calls silently breaks the
/// exactly-once guarantee.
#[async_trait]
pub trait Transform: Send + Sync {
    async fn apply(&self, input: Vec<RecordBatch>) -> Result<Vec<RecordBatch>>;

    /// Apply the transform with the source batch's pinned lookup snapshots.
    ///
    /// Most transforms are still source-only. SQL transforms override this to register lookup
    /// tables in their fresh DataFusion session; a custom Rust transform has to opt in rather
    /// than accidentally reading a second relation without documenting its semantics.
    async fn apply_with_lookups(
        &self,
        input: Vec<RecordBatch>,
        lookups: &[LookupSnapshot],
    ) -> Result<Vec<RecordBatch>> {
        if !lookups.is_empty() {
            return Err(crate::Error::Transform(
                "this transform does not support pinned lookup snapshots".into(),
            ));
        }
        self.apply(input).await
    }

    /// Why this transform cannot be evaluated in parts, or `None` when it can.
    ///
    /// `None` is a promise that the output for a batch is exactly the output for each of its
    /// rows, put together — which is what makes setting one row aside and keeping the answer
    /// for the others correct. A custom transform is not assumed to keep it.
    fn cross_row(&self) -> Option<String> {
        Some("a custom Rust transform does not declare that it is row-local".into())
    }

    /// Apply the transform, setting aside up to `max_rejects` rows it cannot evaluate.
    ///
    /// The default isolates nothing: it is [`Self::apply_with_lookups`], and a row that
    /// fails fails the batch. [`SqlTransform`] overrides it.
    async fn apply_isolating(
        &self,
        input: Vec<RecordBatch>,
        lookups: &[LookupSnapshot],
        max_rejects: usize,
    ) -> Result<Isolated> {
        let _ = max_rejects;
        Ok(Isolated {
            output: self.apply_with_lookups(input, lookups).await?,
            unevaluable: None,
            reevaluations: 0,
        })
    }

    fn describe(&self) -> String {
        "transform".into()
    }
}

/// What [`Transform::apply_isolating`] made of one batch.
#[derive(Debug)]
pub struct Isolated {
    /// The output of every row that evaluated, in input order.
    pub output: Vec<RecordBatch>,
    /// The source rows the transform could not evaluate, each with its error. `None` when
    /// every row evaluated.
    pub unevaluable: Option<Rejected>,
    /// Runs of the transform after the first. Zero for a batch that evaluated as a whole,
    /// which is the case that must stay free.
    pub reevaluations: usize,
}

/// Passes batches through untouched — a straight table copy.
pub struct Identity;

#[async_trait]
impl Transform for Identity {
    async fn apply(&self, input: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        Ok(input)
    }

    fn cross_row(&self) -> Option<String> {
        None
    }

    fn describe(&self) -> String {
        "identity (straight copy)".into()
    }
}
