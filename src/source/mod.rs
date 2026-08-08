//! The streaming Delta source.
//!
//! Plan Phase 1, vendored per §1.9: the API mirrors what an upstream delta-rs
//! contribution would expose, so swapping to it later is a dependency change.

pub mod cursor;
pub mod log_stream;

pub use cursor::{StreamCursor, Version};
pub use log_stream::{classify, ChangePolicy, CommitClass, LogBatch, LogStreamBuilder};
