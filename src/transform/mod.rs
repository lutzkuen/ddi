//! Stateless, row-local transforms.
//!
//! Everything here is validated at config load ([`validate`]) so that a transform which
//! could not be correct never gets the chance to run. The contract is narrow on purpose:
//! rows in, rows out, no memory of previous batches.

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;

use crate::error::Result;

pub mod json;
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

    fn describe(&self) -> String {
        "transform".into()
    }
}

/// Passes batches through untouched — a straight table copy.
pub struct Identity;

#[async_trait]
impl Transform for Identity {
    async fn apply(&self, input: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        Ok(input)
    }

    fn describe(&self) -> String {
        "identity (straight copy)".into()
    }
}
