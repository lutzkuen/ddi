//! Assigning and revoking partitions, deferred (docs/kafka-design.md §3.4), and the librdkafka
//! calls around them.
//!
//! The rebalance callback only copies the partition list (`context`). Here the main loop acts
//! on it, while librdkafka keeps the group waiting with heartbeats running:
//!
//! - **Assign**: refresh the target, read every new partition's offset in one walk of its log
//!   (`Fence::reset`), fetch the topic's watermarks, resolve each start (§4.4), and only then
//!   `assign()` with those offsets. No offset ever comes from `auto.offset.reset`, and nothing
//!   is sought after assigning (EO-m2).
//! - **Revoke**: commit the buffer within `revoke_flush_timeout_secs`, then `unassign()`, so a
//!   partition's rows are committed or dropped before another member can read it (I6).
//!
//! Every call that blocks in librdkafka (metadata, watermarks, assign, seek) runs in
//! `spawn_blocking` on the shared consumer, never on the runtime's threads (EO-m7).

use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{StreamExt, TryStreamExt};
use rdkafka::consumer::Consumer as _;
use rdkafka::{Offset, TopicPartitionList};
use tracing::{debug, info, warn};

use crate::error::{Error, KafkaClass, Result};
use crate::kafka::client::{self, Consumer};
use crate::kafka::partitions::{Block, Buffer, State};
use crate::kafka::start::{self, Start};
use crate::pipeline::{Pipeline, StepOutcome};

/// How long one librdkafka request may take before it counts as failed.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
/// How many watermark requests are in flight at once.
const WATERMARKS_IN_FLIGHT: usize = 8;

/// Run a blocking librdkafka call on the blocking pool.
async fn blocking<T: Send + 'static>(
    consumer: &Arc<Consumer>,
    what: &'static str,
    call: impl FnOnce(&Consumer) -> rdkafka::error::KafkaResult<T> + Send + 'static,
) -> Result<T> {
    let consumer = consumer.clone();
    tokio::task::spawn_blocking(move || call(&consumer))
        .await
        .map_err(|e| Error::Other(format!("internal: {what} panicked: {e}")))?
        .map_err(|e| client::classify(&e, what))
}

/// The partitions of `topic`, from the brokers' metadata (§4.4 step 1).
pub(crate) async fn topic_partitions(consumer: &Arc<Consumer>, topic: &str) -> Result<Vec<i32>> {
    let name = topic.to_string();
    let metadata = blocking(consumer, "reading the topic's metadata", move |c| {
        c.fetch_metadata(Some(&name), REQUEST_TIMEOUT)
    })
    .await?;
    let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) else {
        return Err(unknown_topic(
            topic,
            "the brokers' metadata does not have it",
        ));
    };
    if let Some(e) = t.error() {
        return Err(unknown_topic(topic, &format!("{e:?}")));
    }
    let mut partitions: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
    partitions.sort_unstable();
    if partitions.is_empty() {
        return Err(unknown_topic(topic, "it has no partitions"));
    }
    Ok(partitions)
}

fn unknown_topic(topic: &str, why: &str) -> Error {
    Error::KafkaClient {
        failure: crate::error::KafkaClientFailure::Config,
        detail: format!(
            "topic {topic:?} cannot be read ({why}). Check the topic name and the brokers; ddi \
             never creates a topic."
        ),
    }
}

/// Each partition's low and high watermarks (§3.4 step 3). Under `read_committed` "high" is
/// the last stable offset (§0.3.15).
pub(crate) async fn watermarks(
    consumer: &Arc<Consumer>,
    topic: &str,
    partitions: &[i32],
) -> Result<BTreeMap<i32, (i64, i64)>> {
    futures::stream::iter(partitions.iter().copied())
        .map(|p| {
            let topic = topic.to_string();
            async move {
                let w = blocking(consumer, "reading a partition's watermarks", move |c| {
                    c.fetch_watermarks(&topic, p, REQUEST_TIMEOUT)
                })
                .await?;
                Ok::<_, Error>((p, w))
            }
        })
        .buffer_unordered(WATERMARKS_IN_FLIGHT)
        .try_collect()
        .await
}

impl Pipeline {
    /// Take `partitions`, which the group gave this member (or once-mode takes itself): read
    /// their starts from the target, then assign them at those offsets, the blocked ones
    /// paused (§3.4). When the target cannot be read, they are assigned paused and resolved by
    /// the recheck, and the member stays in its group.
    pub(crate) async fn assign_partitions(
        &mut self,
        partitions: Vec<i32>,
        once: bool,
    ) -> Result<()> {
        let now = Instant::now();
        let recheck = Duration::from_secs(self.kafka().kafka.blocked_recheck_secs.max(1));
        self.kafka_mut().parts.clear();
        let resolved = self.read_starts(&partitions).await;
        let topic = self.kafka().kafka.topic.clone();
        let (starts, ends) = match resolved {
            Ok(r) => r,
            Err(e) if e.kafka_class() == KafkaClass::ConsumerFatal || once => return Err(e),
            Err(e) => {
                warn!(
                    pipeline = %self.cfg.name,
                    "the partitions are assigned paused until their offsets can be read: {e}"
                );
                self.kafka().metrics.observe_error(&e);
                let src = self.kafka_mut();
                src.blocked = Some(e.to_string());
                src.next_recheck = Some(
                    now + match e.kafka_class() {
                        KafkaClass::Transient => super::BACKOFF_MIN * 5,
                        _ => recheck,
                    },
                );
                let starts = partitions
                    .iter()
                    .map(|p| (*p, Start::Blocked(Block::Unresolved)))
                    .collect();
                (starts, BTreeMap::new())
            }
        };
        let mut tpl = TopicPartitionList::new();
        let mut paused = Vec::new();
        {
            let src = self.kafka_mut();
            for (p, s) in &starts {
                let offset = match s {
                    Start::Ready { offset, reread } => {
                        src.parts.ready_at(*p, *offset, *reread, now);
                        Offset::Offset(*offset)
                    }
                    Start::Blocked(block) => {
                        if *block != Block::Unresolved {
                            let e = block.error(&topic, *p);
                            warn!(partition = p, "{e}");
                            src.metrics.observe_error(&e);
                            src.next_recheck.get_or_insert(now + recheck);
                        }
                        src.parts.block(*p, block.clone(), now);
                        paused.push(*p);
                        Offset::End
                    }
                };
                tpl.add_partition_offset(&topic, *p, offset)
                    .map_err(|e| Error::Other(format!("internal: {e}")))?;
            }
        }
        let consumer = self.kafka().consumer().clone();
        blocking(&consumer, "assigning partitions", move |c| c.assign(&tpl)).await?;
        self.kafka_mut().paused.clear();
        self.pause(&paused);
        if once {
            self.kafka_mut().once_end =
                Some(ends.iter().map(|(p, (_, high))| (*p, *high)).collect());
        }
        let src = self.kafka();
        info!(
            pipeline = %self.cfg.name,
            partitions = ?partitions,
            blocked = paused.len(),
            once,
            "partitions assigned at their offsets in the target"
        );
        src.metrics
            .assigned_partitions
            .store(partitions.len() as i64, Ordering::Relaxed);
        Ok(())
    }

    /// §3.4 steps 1-4: refresh, one walk for every partition's offset and mark, watermarks,
    /// and each start resolved. Also returns the watermarks, once-mode's ends.
    async fn read_starts(
        &mut self,
        partitions: &[i32],
    ) -> Result<(BTreeMap<i32, Start>, BTreeMap<i32, (i64, i64)>)> {
        self.refresh_target().await?;
        let (src, target) = self.kafka_and_target();
        src.fence.reset(target, partitions).await?;
        src.refreshed_at = Instant::now();
        let consumer = src.consumer().clone();
        let topic = src.kafka.topic.clone();
        let w = watermarks(&consumer, &topic, partitions).await?;
        let src = self.kafka_mut();
        let explicit = src
            .kafka
            .starting_offsets
            .partitions()
            .cloned()
            .unwrap_or_default();
        let mut starts = BTreeMap::new();
        src.unbased_start.clear();
        for p in partitions {
            let (low, high) = w.get(p).copied().unwrap_or((0, 0));
            let (base, mark) = src.fence.base(*p).unwrap_or((None, None));
            let start = start::resolve(base, mark, low, high, explicit.get(p).copied());
            if let (None, Start::Ready { offset, .. }) = (base, &start) {
                src.unbased_start.insert(*p, *offset);
            }
            debug!(partition = p, ?base, low, high, ?start, "start resolved");
            starts.insert(*p, start);
        }
        Ok((starts, w))
    }

    /// A revoke (§3.4): commit the buffer within the time box, then give the partitions up.
    pub(crate) async fn flush_then_unassign(&mut self) -> Result<Option<StepOutcome>> {
        let outcome = if self.kafka().parts.is_empty() {
            None
        } else {
            self.flush("revoke").await?
        };
        self.unassign().await?;
        Ok(outcome)
    }

    /// A lost revoke or a rebalance error: nothing is committed, and the partitions go.
    pub(crate) async fn drop_and_unassign(&mut self) -> Result<()> {
        let src = self.kafka();
        if src.held.is_some() || !src.buf.is_empty() {
            src.metrics
                .revoke_flushes_dropped
                .fetch_add(1, Ordering::Relaxed);
        }
        self.unassign().await
    }

    /// Give every partition up: the buffer and anything held dropped, every offset forgotten,
    /// so no commit of this member asserts one of them again (I6).
    async fn unassign(&mut self) -> Result<()> {
        let src = self.kafka_mut();
        src.decoder_mut().discard();
        src.buf = Buffer::default();
        src.held = None;
        src.held_for = None;
        src.stalled = None;
        src.blocked = None;
        src.next_recheck = None;
        for p in src.parts.owned() {
            src.fence.forget(p);
        }
        src.parts.clear();
        src.paused.clear();
        src.unbased_start.clear();
        src.metrics.assigned_partitions.store(0, Ordering::Relaxed);
        let consumer = src.consumer().clone();
        blocking(&consumer, "unassigning partitions", |c| c.unassign()).await?;
        info!(pipeline = %self.cfg.name, "partitions given up");
        Ok(())
    }

    /// Pause `partitions` in librdkafka: nothing more is fetched for them, and a message that
    /// was already on its way is dropped (`take`). Their position is set again before they
    /// resume (`release`).
    pub(crate) fn pause(&mut self, partitions: &[i32]) {
        let src = self.kafka_mut();
        let topic = src.kafka.topic.clone();
        let mut tpl = TopicPartitionList::new();
        for p in partitions {
            if src.paused.insert(*p) {
                tpl.add_partition(&topic, *p);
            }
        }
        if tpl.count() > 0 {
            if let Err(e) = src.consumer().pause(&tpl) {
                warn!("cannot pause partitions {partitions:?}: {e}");
            }
        }
    }

    /// Pause every owned partition: a batch is held, or a message waits for the registry.
    pub(crate) fn pause_all(&mut self) {
        let owned = self.kafka().parts.owned();
        self.pause(&owned);
    }

    /// Resume every paused partition that is Ready, at the offset it expects next: a seek
    /// first, since messages read and dropped while it was paused moved librdkafka's position.
    pub(crate) async fn release(&mut self) -> Result<()> {
        let src = self.kafka();
        let ready: Vec<(i32, i64)> = src
            .paused
            .iter()
            .filter_map(|p| match src.parts.get(*p) {
                Some(State::Ready { next_expected, .. }) => Some((*p, *next_expected)),
                _ => None,
            })
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        let topic = src.kafka.topic.clone();
        let mut seek = TopicPartitionList::new();
        let mut resume = TopicPartitionList::new();
        for (p, next) in &ready {
            seek.add_partition_offset(&topic, *p, Offset::Offset(*next))
                .map_err(|e| Error::Other(format!("internal: {e}")))?;
            resume.add_partition(&topic, *p);
        }
        let consumer = src.consumer().clone();
        let sought = blocking(&consumer, "seeking partitions", move |c| {
            c.seek_partitions(seek, REQUEST_TIMEOUT)
        })
        .await?;
        for e in sought.elements() {
            if let Err(err) = e.error() {
                // Not resumed: it stays paused, and the next pass tries again.
                warn!(partition = e.partition(), "cannot seek: {err}");
                resume = TopicPartitionList::from_topic_map(
                    &resume
                        .to_topic_map()
                        .into_iter()
                        .filter(|((_, p), _)| *p != e.partition())
                        .collect(),
                )
                .map_err(|e| Error::Other(format!("internal: {e}")))?;
            }
        }
        consumer
            .resume(&resume)
            .map_err(|e| client::classify(&e, "resuming partitions"))?;
        let src = self.kafka_mut();
        let now = Instant::now();
        for e in resume.elements() {
            src.paused.remove(&e.partition());
            // Paused is not idle: once-mode's idle rule counts from the resume (§3.8).
            src.parts.polled_again(Some(e.partition()), now);
        }
        debug!(partitions = ?ready, "partitions resumed at their next offsets");
        Ok(())
    }
}
