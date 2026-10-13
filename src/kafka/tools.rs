//! Operator views of a Kafka pipeline that never disturb its group (docs/kafka-design.md
//! §4.8): a client without a `group.id`, which joins nothing.
//!
//! `ddi status` (M3), `ddi kafka plan` and `clone-table` (M4, [`super::plan`], [`super::clone`])
//! and `set-offsets` (M5, [`super::setoffsets`]).

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

/// What the operator tools read in one go: the target, who wrote the offsets in it, and each
/// partition of the topic with its offset and watermarks.
pub struct View {
    pub target: deltalake::DeltaTable,
    pub era: fence::Era,
    pub txns: BTreeMap<String, fence::TxnState>,
    pub partitions: Vec<PartitionStatus>,
}

/// A client without a `group.id` for `cfg`'s pipeline, and its settings.
pub(super) fn group_less_client(
    cfg: &ResolvedPipeline,
) -> Result<Arc<rdkafka::consumer::BaseConsumer>> {
    let k = cfg.kafka.as_ref().ok_or_else(|| {
        Error::Other("internal: a tool run on a pipeline that is not Kafka".into())
    })?;
    let settings = super::client::settings(&cfg.name, k)?;
    Ok(Arc::new(super::client::group_less(&cfg.name, &settings)?))
}

/// Read a Kafka pipeline's target and topic without joining a group.
pub async fn view(cfg: &ResolvedPipeline) -> Result<View> {
    let k = cfg.kafka.as_ref().ok_or_else(|| {
        Error::Other("internal: a tool run on a pipeline that is not Kafka".into())
    })?;
    let consumer = group_less_client(cfg)?;
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
    let mut out = Vec::new();
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
    Ok(View {
        target,
        era,
        txns,
        partitions: out,
    })
}

/// Every partition of a Kafka pipeline's topic, with its offset in the target and its
/// watermarks, and the era of its app_id's offsets (§4.3).
pub async fn status(cfg: &ResolvedPipeline) -> Result<(fence::Era, Vec<PartitionStatus>)> {
    let v = view(cfg).await?;
    Ok((v.era, v.partitions))
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
