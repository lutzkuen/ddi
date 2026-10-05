//! Operator views of a Kafka pipeline that never disturb its group (docs/kafka-design.md
//! §4.8): a client without a `group.id`, which joins nothing.
//!
//! M3 has `ddi status`; `plan`, `clone-table` and `set-offsets` come in M4 and M5.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::Consumer as _;

use crate::config::ResolvedPipeline;
use crate::error::{Error, Result};
use crate::fence::{self, LookupCounters};

/// One partition as `ddi status` prints it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionStatus {
    pub partition: i32,
    /// The last offset the target holds, `None` without a txn.
    pub stored: Option<i64>,
    pub low: i64,
    pub high: i64,
}

impl PartitionStatus {
    /// Messages between the next offset to read and the end of the topic.
    pub fn lag(&self) -> i64 {
        let next = self.stored.map_or(self.low.max(0), |s| s + 1);
        (self.high - next).max(0)
    }

    /// What a resume would make of it (§4.4), in a word.
    pub fn verdict(&self) -> &'static str {
        match self.stored {
            Some(b) if b + 1 < self.low => "RETENTION_GAP",
            Some(b) if b >= self.high => "REGRESSED",
            Some(_) => "OK",
            None if self.low == 0 => "NO_TXN->offset 0",
            None => "NO_TXN->blocked",
        }
    }
}

/// Every partition of a Kafka pipeline's topic, with its offset in the target and its
/// watermarks, and the era of its app_id's offsets (§4.3).
pub async fn status(cfg: &ResolvedPipeline) -> Result<(fence::Era, Vec<PartitionStatus>)> {
    let k = cfg
        .kafka
        .as_ref()
        .ok_or_else(|| Error::Other("internal: status of a Kafka pipeline without one".into()))?;
    let settings = super::client::settings(&cfg.name, k)?;
    let consumer = Arc::new(super::client::group_less(&cfg.name, &settings)?);
    let topic = k.topic.clone();
    let metadata = {
        let consumer = consumer.clone();
        let topic = topic.clone();
        tokio::task::spawn_blocking(move || {
            consumer.fetch_metadata(Some(&topic), Duration::from_secs(10))
        })
        .await
        .map_err(|e| Error::Other(format!("internal: {e}")))?
        .map_err(|e| super::client::classify(&e, "reading the topic's metadata"))?
    };
    let partitions: Vec<i32> = metadata
        .topics()
        .iter()
        .filter(|t| t.name() == topic && t.error().is_none())
        .flat_map(|t| t.partitions().iter().map(|p| p.id()))
        .collect();
    let target = cfg.storage.open(&cfg.target_uri).await?;
    let mut ids: Vec<String> = partitions
        .iter()
        .map(|p| fence::txn_app_id(&cfg.app_id, *p))
        .collect();
    ids.push(fence::takeover_marker_app_id(&cfg.app_id));
    let txns = fence::read_txns(&target, &ids, &LookupCounters::default()).await?;
    let era = fence::era(&cfg.app_id, &txns);
    let mut out = Vec::new();
    let mut watermarks = BTreeMap::new();
    for p in &partitions {
        let consumer = consumer.clone();
        let topic = topic.clone();
        let p = *p;
        let w = tokio::task::spawn_blocking(move || {
            consumer.fetch_watermarks(&topic, p, Duration::from_secs(10))
        })
        .await
        .map_err(|e| Error::Other(format!("internal: {e}")))?
        .map_err(|e| super::client::classify(&e, "reading watermarks"))?;
        watermarks.insert(p, w);
    }
    for p in partitions {
        let (low, high) = watermarks[&p];
        let stored = txns
            .get(&fence::txn_app_id(&cfg.app_id, p))
            .and_then(|t| t.version);
        out.push(PartitionStatus {
            partition: p,
            stored,
            low,
            high,
        });
    }
    Ok((era, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partitions_status_reads_as_its_resume_would() {
        let s = |stored, low, high| PartitionStatus {
            partition: 0,
            stored,
            low,
            high,
        };
        assert_eq!(s(Some(9), 0, 20).verdict(), "OK");
        assert_eq!(s(Some(9), 0, 20).lag(), 10);
        assert_eq!(s(Some(9), 12, 20).verdict(), "RETENTION_GAP");
        assert_eq!(s(Some(20), 0, 20).verdict(), "REGRESSED");
        assert_eq!(s(None, 0, 5).verdict(), "NO_TXN->offset 0");
        assert_eq!(s(None, 0, 5).lag(), 5);
        assert_eq!(s(None, 3, 5).verdict(), "NO_TXN->blocked");
    }
}
