//! The Kafka source, compiled only with the `kafka` feature (docs/kafka-design.md).
//!
//! - [`registry`]: the Schema Registry client (§5.2);
//! - [`decode`]: Confluent-framed Avro into Arrow, one decoder per writer schema, with every
//!   message checked to be exactly one row and a malformed one isolated (§5.1, §5.4, §6.1);
//! - [`conform`]: the union `U` the SQL path aligns its batches to (§5.5), and the conform
//!   step that fits a batch to the target by name before the coercer (§5.6);
//! - `kdi_text`: kafka-delta-ingest's text for values going into STRING columns, which the
//!   conform step writes under `kdi_compat` and `kdi_null_text` (§5.6 steps 5 and 6);
//! - `client` and `context`: the librdkafka consumer, and what it calls back into (§3.1, §3.2);
//! - `partitions` and `start`: what a member knows of each partition it owns, and where each
//!   starts reading (§3.3, §4.4);
//! - `step` and `rebalance`: the poll loop, the commit of a batch with the error classes of
//!   §3.6, and the deferred assign and revoke (§3.3, §3.4), as `impl Pipeline`;
//! - [`KafkaSource`]: everything of the above one pipeline holds between steps;
//! - `tools`: `ddi status` for Kafka pipelines, with a client that joins no group.
//!
//! Configuration is not here: it is always compiled, in `config::kafka`, so a build without
//! this feature can still read and validate it, and hold its pipelines back by name.

pub(crate) mod client;
pub mod clone;
pub mod conform;
pub(crate) mod context;
pub mod decode;
pub(crate) mod fault;
pub(crate) mod kdi_text;
pub(crate) mod partitions;
pub mod plan;
pub(crate) mod rebalance;
pub mod registry;
pub mod setoffsets;
#[cfg(test)]
pub(crate) mod sr_stub;
pub(crate) mod start;
pub(crate) mod step;
pub mod tools;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use deltalake::arrow::array::RecordBatch;
use rdkafka::consumer::Consumer as _;
use tracing::{info, warn};

use crate::config::kafka::ResolvedKafka;
use crate::error::Result;
use crate::fence::Fence;
use crate::kafka::client::Consumer;
use crate::kafka::conform::{Conformer, UnionSchema};
use crate::kafka::decode::{Decoded, KafkaDecoder, KafkaMessage, NotFed};
use crate::kafka::partitions::{Buffer, Limits, Partitions};
use crate::kafka::registry::{RegisteredSchema, SubjectSchemas};
use crate::metrics::KafkaMetrics;
use crate::schema::Rejected;
use crate::sink::MessageMark;
use crate::transform::SqlTransform;

/// How long one step polls before it hands control back to the supervisor (§3.3).
pub(crate) const TICK: Duration = Duration::from_secs(1);
/// How long once-mode waits for a partition that has stopped short of its end (§3.8).
pub(crate) const ONCE_IDLE: Duration = Duration::from_secs(10);
/// The first and the longest wait before a held batch is tried again (§3.3, Transient).
pub(crate) const BACKOFF_MIN: Duration = Duration::from_secs(1);
pub(crate) const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// The wait after a capacity failure: the supervisor's `RETRY_MAX` (§3.6, Capacity).
pub(crate) const BACKOFF_CAPACITY: Duration = Duration::from_secs(300);
/// How long a shutdown waits for the consumer to close, and leave its group, before the
/// process exits all the same (§3.9).
pub(crate) const CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A Kafka pipeline's source, as `Pipeline` holds it between steps (docs/kafka-design.md
/// §1.2, §3.3). Everything a step can return in the middle of — the buffer, a held batch, a
/// message waiting for the registry — lives here, so returning to the supervisor loses
/// nothing.
pub(crate) struct KafkaSource {
    /// Fits each batch to the target before the coercer (§5.6).
    pub(crate) conformer: Conformer,
    pub(crate) kafka: Arc<ResolvedKafka>,
    /// `None` only while it is being dropped.
    pub(crate) consumer: Option<Arc<Consumer>>,
    pub(crate) fence: Fence,
    pub(crate) parts: Partitions,
    pub(crate) buf: Buffer,
    /// Behind a mutex only so that `Pipeline` stays `Sync` (the Delta path holds `&Pipeline`
    /// across awaits) while the decoder is only `Send`; every use is through `&mut`, which
    /// takes no lock ([`Self::decoder_mut`]).
    pub(crate) decoder: std::sync::Mutex<Decoder>,
    /// The SQL path's union `U` (§5.5): `None` on the identity path and for raw values.
    pub(crate) union: Option<SqlUnion>,
    /// The target's partition columns could not be checked at open (no writer schema was
    /// known): checked on the first batch instead (§5.6 step 0).
    pub(crate) partition_check_deferred: bool,
    pub(crate) partition_columns: Vec<String>,
    /// A batch waiting to be committed again.
    pub(crate) held: Option<Held>,
    /// A message the decoder handed back, waiting for the registry (§5.4).
    pub(crate) stalled: Option<Stalled>,
    /// Why the whole pipeline is paused, while it is: the `reason` of
    /// `ddi_kafka_pipeline_blocked` ([`crate::Error::kafka_block_reason`]).
    pub(crate) blocked: Option<&'static str>,
    /// When blocked partitions, or a blocked pipeline, are looked at again.
    pub(crate) next_recheck: Option<Instant>,
    /// Partitions paused in librdkafka.
    pub(crate) paused: BTreeSet<i32>,
    /// Where each owned partition without an offset in the target was assigned: where a reset
    /// puts it back.
    pub(crate) unbased_start: BTreeMap<i32, i64>,
    pub(crate) metrics: Arc<KafkaMetrics>,
    pub(crate) limits: Limits,
    /// The target's table id at open (I18).
    pub(crate) table_id: Option<String>,
    /// When the target handle last moved through our own commit or a reset (EO-m8).
    pub(crate) refreshed_at: Instant,
    pub(crate) last_commit: Option<Instant>,
    /// The step that is running reset the source: `Waiting` is not progress then (§3.9).
    pub(crate) reset_this_step: bool,
    /// Once-mode: each partition's end offset, read when it was assigned (§3.8).
    pub(crate) once_end: Option<BTreeMap<i32, i64>>,
    /// librdkafka's statistics interval; `None` when they are off.
    pub(crate) stats_interval: Option<Duration>,
    pub(crate) stats_seen: u64,
    /// Every partition's lag is 0 in statistics no older than two intervals.
    pub(crate) caught_up: bool,
    /// The SQL's output columns that copy a column of `U` under another name (§5.6), kept to
    /// rebuild the conformer when the target's schema changes.
    pub(crate) copies: BTreeMap<String, String>,
    pub(crate) kdi_text: conform::KdiText,
    /// What the held batch waits for.
    pub(crate) held_for: Option<step::HeldFor>,
    /// The backoff of the batch just taken out of `held`, which a further failure doubles.
    pub(crate) last_backoff: Option<Duration>,
    /// What a revoke flush committed in the middle of polling, for the step to return.
    pub(crate) pending_outcome: Option<crate::pipeline::StepOutcome>,
}

/// A batch whose commit failed in a way worth trying again.
pub(crate) struct Held {
    pub batch: KafkaBatch,
    pub due: Instant,
    pub backoff: Duration,
    /// Held after a transient failure or a capacity one (`ddi_kafka_retrying`,
    /// `ddi_capacity_exhausted`), rather than for an operator.
    pub retrying: bool,
}

/// A message waiting for its writer schema.
pub(crate) struct Stalled {
    pub message: KafkaMessage,
    pub due: Instant,
    pub backoff: Duration,
    /// Why the pipeline is held, when what the registry answered needs an operator.
    pub blocking: Option<&'static str>,
}

/// The SQL path's union of writer schemas (§5.5).
pub(crate) struct SqlUnion {
    pub subject: SubjectSchemas,
    pub sql: SqlTransform,
    /// `U` as last built, and the ids it was built from.
    pub union: UnionSchema,
}

/// One batch, read and closed, not yet committed: cheap to clone (its arrays are shared), so
/// it can be held and tried again.
#[derive(Clone)]
pub(crate) struct KafkaBatch {
    /// The decoded rows, one batch per flushed chunk, in their writer schemas' shapes: aligned
    /// to `U` when committed, to the `U` of that moment (§5.5).
    pub decoded: Vec<RecordBatch>,
    /// The messages that could not be decoded, as rows for the data-quality table (§5.7).
    pub rejects: Option<Rejected>,
    pub undecodable: usize,
    pub first_undecodable: Option<(i32, i64, String)>,
    pub tombstones: u64,
    /// Per partition, the first and the last offset read.
    pub ranges: BTreeMap<i32, (i64, i64)>,
    pub marks: BTreeMap<i32, MessageMark>,
    /// Per partition, its offset in the target when the batch was read: a reset that finds
    /// another one there invalidates the batch.
    pub bases: BTreeMap<i32, Option<i64>>,
    pub messages: u64,
    pub raw_bytes: u64,
    pub writer_schemas: Vec<Arc<RegisteredSchema>>,
}

/// What one poll of the source came to (§3.3).
pub(crate) enum Work {
    /// Nothing to commit this tick.
    Waiting,
    /// Once-mode: every partition is done and the buffer committed.
    Done,
    /// A batch closed.
    Batch(KafkaBatch),
    /// A held batch whose wait is over.
    Retry(KafkaBatch),
    /// A batch was committed on the way (a revoke flush, or before blocking a partition the
    /// topic trimmed under it), and this is what came of it.
    Committed(crate::pipeline::StepOutcome),
}

/// What decodes a pipeline's messages: Confluent Avro, or the raw bytes.
pub(crate) enum Decoder {
    Avro(KafkaDecoder),
    Raw(RawDecoder),
}

impl Decoder {
    pub(crate) async fn feed(
        &mut self,
        message: KafkaMessage,
    ) -> std::result::Result<(), Box<NotFed>> {
        match self {
            Self::Avro(d) => d.feed(message).await,
            Self::Raw(d) => {
                d.feed(message);
                Ok(())
            }
        }
    }

    pub(crate) fn decoded_bytes(&self) -> u64 {
        match self {
            Self::Avro(d) => d.decoded_bytes(),
            Self::Raw(d) => d.bytes,
        }
    }

    pub(crate) fn finish(&mut self) -> Result<Decoded> {
        match self {
            Self::Avro(d) => d.finish(),
            Self::Raw(d) => d.finish(),
        }
    }

    pub(crate) fn discard(&mut self) {
        match self {
            Self::Avro(d) => d.discard(),
            Self::Raw(d) => *d = RawDecoder::new(&d.topic, &d.prefix),
        }
    }

    pub(crate) fn writer_schemas(&self) -> Vec<Arc<RegisteredSchema>> {
        match self {
            Self::Avro(d) => d.writer_schemas(),
            Self::Raw(_) => Vec::new(),
        }
    }
}

/// `value_format = "raw"`: each value's bytes as they are, in `<prefix>value`, with the
/// metadata columns. For tests and diagnostics; nothing is ever undecodable.
pub(crate) struct RawDecoder {
    topic: String,
    prefix: String,
    messages: Vec<KafkaMessage>,
    tombstones: u64,
    bytes: u64,
}

impl RawDecoder {
    pub(crate) fn new(topic: &str, prefix: &str) -> Self {
        Self {
            topic: topic.to_string(),
            prefix: prefix.to_string(),
            messages: Vec::new(),
            tombstones: 0,
            bytes: 0,
        }
    }

    fn feed(&mut self, message: KafkaMessage) {
        if message.value.is_none() {
            self.tombstones += 1;
            return;
        }
        self.bytes += decode::raw_len(&message);
        self.messages.push(message);
    }

    fn finish(&mut self) -> Result<Decoded> {
        let messages = std::mem::take(&mut self.messages);
        let tombstones = std::mem::take(&mut self.tombstones);
        let bytes = std::mem::take(&mut self.bytes);
        let batches = if messages.is_empty() {
            Vec::new()
        } else {
            vec![decode::raw_batch(&self.topic, &self.prefix, &messages)?]
        };
        Ok(Decoded {
            batches,
            undecodable: Vec::new(),
            rejects: None,
            tombstones,
            decoded_bytes: bytes,
        })
    }
}

/// What a Kafka pipeline tells the supervisor about its progress (§3.9).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KafkaProgress {
    /// Partitions this member owns.
    pub assigned: usize,
    /// Every owned partition has lag 0 in statistics no older than two intervals.
    pub caught_up: bool,
    /// When this member last committed a batch.
    pub last_commit: Option<Instant>,
    /// A batch is held, or a message waits for the registry.
    pub retrying: bool,
    /// The pipeline, or one of its partitions, waits for an operator.
    pub blocked: bool,
    /// The last step reset the source.
    pub reset: bool,
    /// A batch is held after running out of capacity (`ddi_capacity_exhausted`).
    pub capacity: bool,
}

impl KafkaProgress {
    /// Whether a `Waiting` step counts as progress for `ddi_pipeline_seconds_since_progress`
    /// (§3.9, PO-M5): never while retrying, blocked or after a reset; otherwise when nothing
    /// is assigned (idle by design: the group's own alert covers it), when caught up, or when
    /// the last commit is recent.
    pub fn counts(&self, allowed_latency: Duration) -> bool {
        if self.retrying || self.blocked || self.reset {
            return false;
        }
        self.assigned == 0
            || self.caught_up
            || self
                .last_commit
                .is_some_and(|t| t.elapsed() <= allowed_latency + Duration::from_secs(60))
    }
}

impl KafkaSource {
    /// The consumer, which is there for as long as the source is.
    pub(crate) fn consumer(&self) -> &Arc<Consumer> {
        self.consumer
            .as_ref()
            .expect("the consumer is taken only when the source is dropped")
    }

    pub(crate) fn decoder_mut(&mut self) -> &mut Decoder {
        self.decoder.get_mut().unwrap_or_else(|e| e.into_inner())
    }

    fn decoder(&self) -> std::sync::MutexGuard<'_, Decoder> {
        self.decoder.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The current batch's decoded size (§3.3).
    pub(crate) fn decoded_bytes(&self) -> u64 {
        self.decoder().decoded_bytes()
    }

    /// The writer schemas the decoder has met.
    pub(crate) fn writer_schemas(&self) -> Vec<Arc<RegisteredSchema>> {
        self.decoder().writer_schemas()
    }

    pub(crate) fn progress(&self) -> KafkaProgress {
        KafkaProgress {
            assigned: self.parts.len(),
            caught_up: self.caught_up,
            last_commit: self.last_commit,
            retrying: self.held.as_ref().is_some_and(|h| h.retrying) || self.stalled.is_some(),
            blocked: self.blocked.is_some()
                || self.parts.blocked().next().is_some()
                || self.held.as_ref().is_some_and(|h| !h.retrying),
            reset: self.reset_this_step,
            capacity: self.held.is_some() && self.held_for == Some(step::HeldFor::Capacity),
        }
    }
}

#[cfg(test)]
impl KafkaSource {
    /// A source with no consumer behind it, for tests of what `Pipeline::process` does with a
    /// Kafka batch: `value_format = "raw"`, nothing assigned.
    pub(crate) fn for_tests(conformer: Conformer) -> Self {
        let toml = "[kafka]\nbrokers = \"127.0.0.1:9\"\n\n[[kafka.pipeline]]\nname = \"t\"\n\
                    app_id = \"t\"\ntopic = \"t\"\ntarget_uri = \"/tmp/t\"\n\
                    value_format = \"raw\"\n";
        let resolved = crate::config::Config::from_toml_str(toml)
            .and_then(|c| c.resolve_all())
            .expect("a minimal Kafka config");
        let kafka = resolved.pipelines[0]
            .kafka
            .clone()
            .expect("a Kafka pipeline");
        let metrics = Arc::new(KafkaMetrics::default());
        Self {
            conformer,
            consumer: None,
            fence: Fence::with_lookups("t", metrics.lookups.clone()),
            parts: Partitions::default(),
            buf: Buffer::default(),
            decoder: std::sync::Mutex::new(Decoder::Raw(RawDecoder::new(
                &kafka.topic,
                &kafka.metadata_prefix,
            ))),
            union: None,
            partition_check_deferred: false,
            partition_columns: Vec::new(),
            held: None,
            stalled: None,
            blocked: None,
            next_recheck: None,
            paused: BTreeSet::new(),
            unbased_start: BTreeMap::new(),
            metrics,
            limits: Limits {
                latency: Duration::from_secs(1),
                max_messages: None,
                max_bytes: u64::MAX,
                max_decoded: None,
            },
            table_id: None,
            refreshed_at: Instant::now(),
            last_commit: None,
            reset_this_step: false,
            once_end: None,
            stats_interval: None,
            stats_seen: 0,
            caught_up: false,
            copies: BTreeMap::new(),
            kdi_text: conform::KdiText::default(),
            held_for: None,
            last_backoff: None,
            pending_outcome: None,
            kafka,
        }
    }
}

impl KafkaSource {
    /// Close the consumer, which the source no longer has afterwards. librdkafka's close
    /// blocks until the group has taken the partitions back (base_consumer.rs:762-780), which
    /// can take as long as a rebalance, so it runs on a thread of its own, which needs no
    /// runtime (EO-m7); the context answers the revoke the close brings by itself from now on.
    /// The receiver hears when the close is done; `None` when there was no consumer, or when
    /// no thread could be started and it was closed here.
    pub(crate) fn close_consumer(&mut self) -> Option<tokio::sync::oneshot::Receiver<()>> {
        let consumer = self.consumer.take()?;
        consumer.context().closing();
        let topic = self.kafka.topic.clone();
        let (done, closed) = tokio::sync::oneshot::channel();
        let spawned = std::thread::Builder::new()
            .name("ddi-kafka-close".into())
            .spawn(move || {
                let started = Instant::now();
                drop(consumer);
                let took = started.elapsed();
                if took > CLOSE_TIMEOUT {
                    warn!(
                        topic = %topic,
                        secs = took.as_secs(),
                        "closing the Kafka consumer took longer than 30 s"
                    );
                } else {
                    info!(topic = %topic, ms = took.as_millis() as u64, "Kafka consumer closed");
                }
                let _ = done.send(());
            });
        match spawned {
            Ok(_) => Some(closed),
            Err(e) => {
                warn!("cannot start a thread to close the Kafka consumer, closing here: {e}");
                None
            }
        }
    }
}

impl Drop for KafkaSource {
    /// A source dropped without a shutdown (the supervisor reopening it) closes its consumer
    /// without waiting: the close never delays what comes next. A shutdown has closed it
    /// already, and waited (`Pipeline::kafka_shutdown`).
    fn drop(&mut self) {
        let _ = self.close_consumer();
    }
}

/// The librdkafka this binary was linked against, as librdkafka spells it (`"2.12.1"`).
pub fn librdkafka_version() -> String {
    rdkafka::util::get_rdkafka_version().1
}

/// What this librdkafka was built with: its `builtin.features`, e.g. `ssl`, `sasl_scram`,
/// `zstd`.
pub fn builtin_features() -> crate::Result<Vec<String>> {
    let native = rdkafka::config::ClientConfig::new()
        .create_native_config()
        .map_err(|e| crate::Error::Other(format!("librdkafka: {e}")))?;
    let features = native
        .get("builtin.features")
        .map_err(|e| crate::Error::Other(format!("librdkafka: builtin.features: {e}")))?;
    Ok(features.split(',').map(|f| f.trim().to_string()).collect())
}

/// What a ddi deployment needs from librdkafka: TLS, SASL/SCRAM over it (kdi's
/// `KAFKA_DELTA_INGEST_SCRAM_JSON`), and zstd-compressed topics (§3.1).
pub const REQUIRED_FEATURES: [&str; 3] = ["ssl", "sasl_scram", "zstd"];

#[cfg(test)]
mod tests {
    use super::*;

    /// The version every librdkafka reference in docs/kafka-design.md was checked against.
    /// A bump of rdkafka-sys must revisit those, so it fails here first.
    #[test]
    fn librdkafka_is_the_version_the_design_was_checked_against() {
        assert_eq!(librdkafka_version(), "2.12.1");
    }

    /// A regression check on the build, not an experiment (§8): the C build is configured
    /// by cargo features, and losing `ssl` or `zstd` would otherwise surface only when a
    /// consumer met a SASL_SSL cluster or a zstd batch.
    #[test]
    fn librdkafka_has_ssl_sasl_scram_and_zstd() {
        let features = builtin_features().unwrap();
        for f in REQUIRED_FEATURES {
            assert!(
                features.iter().any(|x| x == f),
                "{f} missing from {features:?}"
            );
        }
        // No libsasl2 in the build, so no GSSAPI; config refuses it (§2.5 rule 9).
        assert!(!features.iter().any(|x| x == "sasl_gssapi"), "{features:?}");
    }

    /// arrow-avro must speak deltalake's arrow, not a second copy: this is a type error, not
    /// a test failure, the day the two drift apart.
    #[test]
    fn arrow_avro_shares_deltalakes_arrow() {
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let avro = arrow_avro::schema::AvroSchema::try_from(&schema).unwrap();
        assert!(avro.json_string.contains("\"id\""), "{}", avro.json_string);
    }

    /// `Waiting` is progress when the member is idle by design, caught up, or committed
    /// recently, and never while it retries, is blocked, or has just reset (§3.9, I22).
    #[test]
    fn waiting_is_progress_only_when_nothing_is_wrong() {
        let latency = Duration::from_secs(30);
        let base = KafkaProgress {
            assigned: 3,
            caught_up: false,
            last_commit: None,
            retrying: false,
            blocked: false,
            reset: false,
            capacity: false,
        };
        assert!(!base.counts(latency));
        assert!(KafkaProgress {
            assigned: 0,
            ..base
        }
        .counts(latency));
        assert!(KafkaProgress {
            caught_up: true,
            ..base
        }
        .counts(latency));
        assert!(KafkaProgress {
            last_commit: Some(Instant::now()),
            ..base
        }
        .counts(latency));
        for not in [
            KafkaProgress {
                caught_up: true,
                retrying: true,
                ..base
            },
            KafkaProgress {
                caught_up: true,
                blocked: true,
                ..base
            },
            KafkaProgress {
                assigned: 0,
                reset: true,
                ..base
            },
        ] {
            assert!(!not.counts(latency), "{not:?}");
        }
    }

    #[test]
    fn raw_values_are_their_bytes_with_the_metadata() {
        let mut raw = RawDecoder::new("events", "kafka_");
        let message = |offset, value: Option<&[u8]>| KafkaMessage {
            partition: 2,
            offset,
            timestamp: decode::KafkaTimestamp::CreateTime(1_700_000_000_000),
            key: Some(b"k".to_vec()),
            value: value.map(<[u8]>::to_vec),
            headers: Vec::new(),
        };
        raw.feed(message(5, Some(b"{\"a\":1}")));
        raw.feed(message(6, None));
        raw.feed(message(7, Some(b"\x00\x01")));
        let decoded = raw.finish().unwrap();
        assert_eq!(decoded.tombstones, 1);
        let b = &decoded.batches[0];
        assert_eq!(b.num_rows(), 2);
        let schema = b.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "kafka_value",
                "kafka_topic",
                "kafka_partition",
                "kafka_offset",
                "kafka_timestamp",
                "kafka_timestamp_type",
                "kafka_key",
                "kafka_headers"
            ]
        );
        assert!(
            raw.finish().unwrap().batches.is_empty(),
            "the next batch starts empty"
        );
    }
}
