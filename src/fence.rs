//! Partitioned offsets in a Delta table: one `txn` action per partition.
//!
//! A Delta source's offset is one number, held in one `txn` action named by the app_id. A
//! source with partitions, as a Kafka topic has, keeps one per partition, each under its own
//! appId, so that every partition advances and is fenced on its own. Nothing here knows
//! about Kafka; it is about how such offsets are named, read and defended in a Delta table
//! (docs/kafka-design.md §4.3).

/// The `txn` appId that holds partition `partition`'s offset for `app_id`.
///
/// kafka-delta-ingest's naming, kept exactly, so a table it wrote resumes under ddi where
/// it stopped.
pub fn txn_app_id(app_id: &str, partition: i32) -> String {
    format!("{app_id}-{partition}")
}

#[cfg(test)]
mod tests {
    use super::txn_app_id;

    #[test]
    fn a_partition_offset_is_named_as_kafka_delta_ingest_names_it() {
        assert_eq!(txn_app_id("orders", 0), "orders-0");
        assert_eq!(txn_app_id("orders", 12), "orders-12");
    }
}
