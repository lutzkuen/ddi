//! What librdkafka calls back into: rebalances, statistics, logs and errors
//! (docs/kafka-design.md §3.2).
//!
//! # Rebalances are deferred
//!
//! The rebalance callback runs inside our own `recv()` (rdkafka base_consumer.rs:149-153,
//! 196-209), so it could assign there and then, but what to assign each partition at is in the
//! target's Delta log, which takes requests to read, and what to do with the buffer before a
//! revoke is a commit. So the callback assigns and revokes nothing: it copies the partition
//! list into an [`Event`] and returns. librdkafka keeps the group waiting meanwhile, with the
//! old assignment paused and heartbeats running (rdkafka_cgrp.c:1349-1373, 6054-6066), until
//! the main loop has read the offsets and calls `assign()`, or has committed the buffer and
//! calls `unassign()` (§3.4).
//!
//! Only while the consumer is being closed does the callback answer at once: nothing reads
//! the events any more, and librdkafka's close waits for the revoke to be answered.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use rdkafka::client::ClientContext;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, RebalanceProtocol};
use rdkafka::error::KafkaError;
use rdkafka::types::{RDKafkaErrorCode, RDKafkaRespErr};
use rdkafka::TopicPartitionList;
use serde::Deserialize;
use tracing::{debug, error, info, warn};

use crate::metrics::KafkaMetrics;

/// What the callbacks hand the main loop, in the order they happened.
#[derive(Debug)]
pub(crate) enum Event {
    /// The group gave this member these partitions: assign them once their offsets are read.
    Assign(TopicPartitionList),
    /// The group takes every partition back (eager rebalancing). `lost` when the member was
    /// already out of the group (a session timeout, `max.poll.interval.ms` overrun): someone
    /// else may own them by now, and nothing of theirs is worth committing.
    Revoke { lost: bool },
    /// librdkafka reported a rebalance error rather than an assignment or a revoke: treated as
    /// lost, and answered with `assign(NULL)` (rdkafka.h:1989-1993).
    RebalanceError(String),
    /// The consumer cannot go on: librdkafka's fatal error, or a cooperative rebalance.
    Fatal(String),
}

/// One partition's watermarks, and where librdkafka's fetcher is, as the last statistics
/// reported them (§3.5).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Watermarks {
    /// The oldest offset the topic still holds.
    pub lo: i64,
    /// The next offset the topic will write.
    pub hi: i64,
    /// The last stable offset: `hi` but for open transactions, under `read_committed`.
    pub ls: i64,
    /// The next offset librdkafka fetches (`next_offset`): past every control record it has
    /// stepped over, which it never hands on. Negative while it has none.
    pub fetch_next: i64,
    /// One past the last offset librdkafka handed on (`app_offset`); negative before any.
    pub app_next: i64,
    /// Messages fetched and not yet handed on (`fetchq_cnt`).
    pub queued: i64,
}

/// The last statistics, as far as ddi reads them.
#[derive(Clone, Debug, Default)]
pub(crate) struct Stats {
    /// When they arrived; `None` before the first.
    pub at: Option<Instant>,
    pub partitions: BTreeMap<i32, Watermarks>,
}

/// The consumer's context: the queue of [`Event`]s, the last statistics, and the pipeline's
/// metrics, which the error callback counts into.
pub(crate) struct DdiContext {
    pipeline: String,
    topic: String,
    events: Mutex<VecDeque<Event>>,
    closing: AtomicBool,
    metrics: Arc<KafkaMetrics>,
    stats: Mutex<Stats>,
    /// Raised by every statistics report, so the loop recomputes lag only when there is news.
    stats_generation: AtomicU64,
}

impl DdiContext {
    pub(crate) fn new(pipeline: &str, topic: &str, metrics: Arc<KafkaMetrics>) -> Self {
        Self {
            pipeline: pipeline.to_string(),
            topic: topic.to_string(),
            events: Mutex::new(VecDeque::new()),
            closing: AtomicBool::new(false),
            metrics,
            stats: Mutex::new(Stats::default()),
            stats_generation: AtomicU64::new(0),
        }
    }

    fn push(&self, event: Event) {
        debug!(pipeline = %self.pipeline, ?event, "kafka event");
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push_back(event);
    }

    /// Every event since the last call, oldest first.
    pub(crate) fn drain(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain(..)
            .collect()
    }

    /// From now on, answer rebalances in the callback: the consumer is being closed, and
    /// nothing reads the events any more.
    pub(crate) fn closing(&self) {
        self.closing.store(true, Ordering::SeqCst);
    }

    /// The last statistics, and how many reports there have been.
    pub(crate) fn stats(&self) -> (Stats, u64) {
        let stats = self.stats.lock().unwrap_or_else(|e| e.into_inner()).clone();
        (stats, self.stats_generation.load(Ordering::Relaxed))
    }
}

impl ClientContext for DdiContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, message: &str) {
        let pipeline = self.pipeline.as_str();
        match level {
            RDKafkaLogLevel::Emerg
            | RDKafkaLogLevel::Alert
            | RDKafkaLogLevel::Critical
            | RDKafkaLogLevel::Error => error!(pipeline, facility = fac, "librdkafka: {message}"),
            RDKafkaLogLevel::Warning => warn!(pipeline, facility = fac, "librdkafka: {message}"),
            RDKafkaLogLevel::Notice | RDKafkaLogLevel::Info => {
                info!(pipeline, facility = fac, "librdkafka: {message}")
            }
            RDKafkaLogLevel::Debug => debug!(pipeline, facility = fac, "librdkafka: {message}"),
        }
    }

    /// Statistics, parsed for the little ddi reads of them: each partition's watermarks, and
    /// whether any broker is up. rdkafka's own `Statistics` would decode every field of a large
    /// document every `statistics.interval.ms`, and fail on one it does not know.
    fn stats_raw(&self, statistics: &[u8]) {
        #[derive(Deserialize)]
        struct Doc {
            #[serde(default)]
            brokers: BTreeMap<String, Broker>,
            #[serde(default)]
            topics: BTreeMap<String, Topic>,
        }
        #[derive(Deserialize)]
        struct Broker {
            #[serde(default)]
            state: String,
            #[serde(default)]
            nodeid: i32,
        }
        #[derive(Deserialize)]
        struct Topic {
            #[serde(default)]
            partitions: BTreeMap<String, Partition>,
        }
        #[derive(Deserialize)]
        struct Partition {
            partition: i32,
            #[serde(default)]
            lo_offset: i64,
            #[serde(default)]
            hi_offset: i64,
            #[serde(default)]
            ls_offset: i64,
            #[serde(default = "unknown")]
            next_offset: i64,
            #[serde(default = "unknown")]
            app_offset: i64,
            #[serde(default)]
            fetchq_cnt: i64,
        }
        // librdkafka's RD_KAFKA_OFFSET_INVALID, for a field a report leaves out.
        fn unknown() -> i64 {
            -1001
        }
        let doc: Doc = match serde_json::from_slice(statistics) {
            Ok(d) => d,
            Err(e) => {
                debug!(pipeline = %self.pipeline, "unreadable librdkafka statistics: {e}");
                return;
            }
        };
        // The bootstrap entries (node id -1) are never "UP" once the real brokers are known.
        let up = doc
            .brokers
            .values()
            .any(|b| b.nodeid >= 0 && b.state == "UP");
        self.metrics
            .brokers_up
            .store(i64::from(up), Ordering::Relaxed);
        let partitions = doc
            .topics
            .get(&self.topic)
            .map(|t| {
                t.partitions
                    .values()
                    // -1 is librdkafka's internal unassigned partition; a negative offset is
                    // one it does not know yet.
                    .filter(|p| p.partition >= 0 && p.hi_offset >= 0)
                    .map(|p| {
                        let w = Watermarks {
                            lo: p.lo_offset.max(0),
                            hi: p.hi_offset,
                            ls: if p.ls_offset >= 0 {
                                p.ls_offset
                            } else {
                                p.hi_offset
                            },
                            fetch_next: p.next_offset,
                            app_next: p.app_offset,
                            queued: p.fetchq_cnt,
                        };
                        (p.partition, w)
                    })
                    .collect()
            })
            .unwrap_or_default();
        *self.stats.lock().unwrap_or_else(|e| e.into_inner()) = Stats {
            at: Some(Instant::now()),
            partitions,
        };
        self.stats_generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Counted by code. All brokers down only lowers `ddi_kafka_brokers_up`: librdkafka keeps
    /// reconnecting, and the next statistics or message raises it again. A fatal error (a
    /// static member fenced by another with its `group.instance.id` among them, §3.4) ends this
    /// consumer.
    fn error(&self, error: KafkaError, reason: &str) {
        let code = error.rdkafka_error_code();
        let name = code.map_or_else(|| "Unknown".to_string(), |c| format!("{c:?}"));
        KafkaMetrics::count(&self.metrics.client_errors, &name);
        match code {
            Some(RDKafkaErrorCode::AllBrokersDown) => {
                self.metrics.brokers_up.store(0, Ordering::Relaxed);
                warn!(pipeline = %self.pipeline, "librdkafka reaches no broker: {reason}");
            }
            Some(RDKafkaErrorCode::Fatal) => {
                error!(pipeline = %self.pipeline, "librdkafka fatal error: {reason}");
                self.push(Event::Fatal(format!("librdkafka fatal error: {reason}")));
            }
            Some(RDKafkaErrorCode::PollExceeded) => warn!(
                pipeline = %self.pipeline,
                "this member went longer than max.poll.interval.ms without polling and left \
                 its group; its partitions come back revoked as lost: {reason}"
            ),
            _ => debug!(pipeline = %self.pipeline, code = %name, "librdkafka: {reason}"),
        }
    }
}

impl ConsumerContext for DdiContext {
    fn rebalance(
        &self,
        consumer: &BaseConsumer<Self>,
        err: RDKafkaRespErr,
        tpl: &mut TopicPartitionList,
    ) {
        let assign = err == RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS;
        if self.closing.load(Ordering::SeqCst) {
            // librdkafka's close waits for this revoke to be answered.
            let answered = match consumer.rebalance_protocol() {
                RebalanceProtocol::Cooperative if assign => consumer.incremental_assign(tpl),
                RebalanceProtocol::Cooperative => consumer.incremental_unassign(tpl),
                _ if assign => consumer.assign(tpl),
                _ => consumer.unassign(),
            };
            if let Err(e) = answered {
                debug!(pipeline = %self.pipeline, "rebalance while closing: {e}");
            }
            return;
        }
        // A backstop: config refuses every strategy but `range` and `roundrobin` (§2.5 rule 8).
        if matches!(
            consumer.rebalance_protocol(),
            RebalanceProtocol::Cooperative
        ) {
            let _ = if assign {
                consumer.incremental_assign(tpl)
            } else {
                consumer.incremental_unassign(tpl)
            };
            self.push(Event::Fatal(
                "the group rebalances cooperatively, and ddi assigns eagerly only; set \
                 partition.assignment.strategy to range or roundrobin"
                    .into(),
            ));
            return;
        }
        // The list belongs to librdkafka's event and dies with it: copy it.
        self.push(match err {
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS => Event::Assign(tpl.clone()),
            RDKafkaRespErr::RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS => Event::Revoke {
                lost: consumer.assignment_lost(),
            },
            other => Event::RebalanceError(format!("{other:?}")),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn context() -> DdiContext {
        DdiContext::new("orders", "orders", Arc::new(KafkaMetrics::default()))
    }

    /// The watermarks of the pipeline's own topic, and nothing of librdkafka's internal
    /// partition -1; a broker is up only if a real one says so.
    #[test]
    fn statistics_give_each_partitions_watermarks() {
        let ctx = context();
        let doc = br#"{
            "name": "c", "brokers": {
                "localhost:9092/bootstrap": {"state": "INIT", "nodeid": -1},
                "localhost:9092/1": {"state": "UP", "nodeid": 1}
            },
            "topics": {
                "orders": {"partitions": {
                    "-1": {"partition": -1, "lo_offset": -1001, "hi_offset": -1001, "ls_offset": -1001},
                    "0": {"partition": 0, "lo_offset": 5, "hi_offset": 40, "ls_offset": 38,
                          "next_offset": 39, "app_offset": 37, "fetchq_cnt": 2},
                    "1": {"partition": 1, "lo_offset": -1001, "hi_offset": 7, "ls_offset": -1001}
                }},
                "other": {"partitions": {"0": {"partition": 0, "lo_offset": 0, "hi_offset": 9, "ls_offset": 9}}}
            }
        }"#;
        ctx.stats_raw(doc);
        let (stats, generation) = ctx.stats();
        assert_eq!(generation, 1);
        assert!(stats.at.is_some());
        assert_eq!(
            stats.partitions,
            BTreeMap::from([
                (
                    0,
                    Watermarks {
                        lo: 5,
                        hi: 40,
                        ls: 38,
                        fetch_next: 39,
                        app_next: 37,
                        queued: 2,
                    }
                ),
                (
                    1,
                    Watermarks {
                        lo: 0,
                        hi: 7,
                        ls: 7,
                        fetch_next: -1001,
                        app_next: -1001,
                        queued: 0,
                    }
                ),
            ])
        );
        assert_eq!(ctx.metrics.brokers_up.load(Ordering::Relaxed), 1);

        ctx.stats_raw(br#"{"brokers": {"localhost:9092/1": {"state": "DOWN", "nodeid": 1}}}"#);
        assert_eq!(ctx.metrics.brokers_up.load(Ordering::Relaxed), 0);
    }

    /// librdkafka's fatal error ends the consumer; anything else is counted by its code.
    #[test]
    fn a_fatal_error_is_an_event_and_every_error_is_counted() {
        let ctx = context();
        ctx.error(
            KafkaError::Global(RDKafkaErrorCode::AllBrokersDown),
            "3/3 brokers are down",
        );
        assert!(ctx.drain().is_empty());
        assert_eq!(ctx.metrics.brokers_up.load(Ordering::Relaxed), 0);
        ctx.error(
            KafkaError::Global(RDKafkaErrorCode::Fatal),
            "fenced by another member",
        );
        assert!(matches!(ctx.drain().as_slice(), [Event::Fatal(_)]));
        let counted = ctx.metrics.client_errors.lock().unwrap().clone();
        assert_eq!(counted.get("AllBrokersDown"), Some(&1));
        assert_eq!(counted.get("Fatal"), Some(&1));
    }
}
