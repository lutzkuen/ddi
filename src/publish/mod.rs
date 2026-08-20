//! Pushing a committed batch somewhere a dashboard can see it.
//!
//! The whole module is downstream of a Delta commit that has already succeeded, and nothing
//! in it may change that fact. Publication is at-most-once and says so: there is no retry
//! beyond the request in flight, no queue, and no outbox — an outbox would be state, in a
//! daemon whose premise is that it has none. A client detects what it missed from the
//! cursor in each message and reloads a baseline; that is the design, not a shortfall of it.

pub mod jwt;
pub mod webpubsub;
