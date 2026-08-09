//! Exactly-once, append-only, stateless Delta-to-Delta streaming.
//!
//! No checkpoint directory, no changelog reconciliation, no cluster. Restart is a
//! version number read from the target's own transaction log.

pub mod config;
pub mod dbt;
pub mod dedup;
pub mod error;
pub mod locate;
pub mod metrics;
pub mod offset;
pub mod pipeline;
pub mod schema;
pub mod sink;
pub mod source;
pub mod storage;
pub mod transform;
pub mod trino;

pub use error::{Error, Result};
