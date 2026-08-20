//! Pushing a committed batch somewhere a dashboard can see it.
//!
//! Everything here is downstream of a Delta commit that has **already succeeded**, and
//! nothing in it may change that fact. That is stated in the type system rather than in a
//! convention at the call site: [`Publisher::send`] returns [`PublishStats`] and not a
//! `Result`, so a caller sitting between the commit and its `StepOutcome` has no error to
//! propagate even if it wanted to. See [`Publisher::send`] for why swallowing is not merely
//! acceptable here but required.
//!
//! Publication is **at-most-once** and says so. There is no retry beyond the request in
//! flight, no queue, and no outbox. The batch cannot be replayed — the commit is durable and
//! the offset moved with it — so a retry loop would delay the *next* batch in order to resend
//! a message the client's own baseline reload already recovers. An outbox would be state, in
//! a daemon whose entire premise is that it has none.
//!
//! What makes that honest rather than lossy is the cursor in every message. A client that
//! sees a break in the publication chain reloads a baseline from the same dbt model and
//! carries on; see [`Envelope`].

pub mod jwt;
pub mod webpubsub;

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use deltalake::arrow::array::RecordBatch;
use deltalake::arrow::datatypes::SchemaRef;
use tracing::{debug, info, warn};

use crate::config::{PublishModel, PublisherConfig, PublisherKind, ResolvedPipeline};
use crate::error::Result;
use crate::source::Version;
use crate::transform::sql::SqlTransform;

/// The current envelope version. The one field that cannot be added later.
const ENVELOPE_VERSION: u32 = 1;

/// Something a committed batch can be pushed to.
///
/// Deliberately narrow: one call, one event, no lifecycle. A publisher is a leaf of the
/// pipeline, never a participant in it — which is why this returns `Result` (an
/// implementation must be able to say what went wrong) while [`Publisher::send`] above it
/// does not (a caller must not be able to act on it).
#[async_trait]
pub trait PostCommitPublisher: Send + Sync {
    /// Send one already-serialised envelope to one group.
    async fn publish(&self, group: &str, body: Vec<u8>) -> Result<()>;

    /// A short, safe description for logs. Never includes a credential.
    fn describe(&self) -> String;
}

#[async_trait]
impl PostCommitPublisher for webpubsub::WebPubSubPublisher {
    async fn publish(&self, group: &str, body: Vec<u8>) -> Result<()> {
        let status = self.send_json(group, body, now_unix()).await?;
        // Logged rather than asserted: 202 is the only documented success, and a change to
        // that should be visible without being an outage.
        debug!(status, group, "published");
        Ok(())
    }

    fn describe(&self) -> String {
        webpubsub::WebPubSubPublisher::describe(self)
    }
}

fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// One message: the rows a batch produced, and enough cursor to place them.
///
/// The cursor fields are the whole reason a best-effort transport is safe to build on. A
/// client applies `rows` on top of a baseline it read from the same dbt model, and uses
/// these to notice when it must read that baseline again:
///
/// * `prev_source_version` is the previous publication's `source_version` — a fact about
///   **this pipeline's publication sequence**, not about the source table. It is what the
///   client compares against, and it is deliberately not derived from the batch: source
///   versions are routinely skipped without producing a batch at all (a compaction, a
///   `dataChange: false` rewrite, a change commit under `skip_change_commits`), so
///   "consecutive source versions" is not an invariant of the reader and a client testing
///   for it would re-baseline every time anyone optimised a bronze table.
/// * `from_source_version` and `source_version` bound what this message covers, for display
///   and for discarding a replay.
/// * `target_version` is the Delta version the rows are already committed at, which lets a
///   client that reloads a baseline tell whether this message is already included in it.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Envelope {
    /// Envelope version.
    pub ddi: u32,
    /// The streamed pipeline, which is the dbt model being published for.
    pub pipeline: String,
    /// The offset key. Two deployments streaming the same tables under different app_ids
    /// are different cursors, and a client watching one must not apply the other's deltas.
    pub app_id: String,
    /// The dbt model whose SQL produced `rows`.
    pub model: String,
    /// The group this was sent to, echoed so a client on several routes without reading
    /// socket metadata.
    pub group: String,
    /// The previous publication's `source_version`, or `null` for the first since start.
    pub prev_source_version: Option<Version>,
    /// First source version this batch drew from. Display only — see the type docs.
    pub from_source_version: Version,
    /// Last source version this batch drew from. The dedup key.
    pub source_version: Version,
    /// Delta version of the target commit these rows are already in.
    pub target_version: Option<Version>,
    pub committed_at: String,
    /// `false` when the payload was too large to send: `rows` is then empty and the client
    /// should reload a baseline rather than assume nothing happened.
    pub complete: bool,
    /// How many rows the transform produced, which is meaningful even when `rows` is empty.
    pub row_count: usize,
    pub rows: serde_json::Value,
}

/// What one publish attempt did. Never an error: see [`Publisher::send`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PublishStats {
    /// The message reached the service and it accepted it. **Not** a delivery count: a
    /// group nobody has joined accepts and discards, so this counts what we sent.
    pub sent: bool,
    /// Building the payload failed, or the request did.
    pub failed: bool,
    /// Nothing was attempted because the breaker was open.
    pub skipped: bool,
    /// The payload was over the size cap and was sent as `complete: false`.
    pub truncated: bool,
    pub rows: usize,
    pub bytes: usize,
}

/// A payload built from the coerced batch, waiting for its commit to land.
#[derive(Debug, Clone)]
pub struct Rendered {
    rows: serde_json::Value,
    row_count: usize,
    complete: bool,
}

/// A sink that refuses to grow past a limit.
///
/// The size cap is enforced here, while the payload is being built, rather than by measuring
/// what was built. The difference matters: "an aggregate is small by construction" is a
/// convention, not a constraint — nothing stops a model grouping by a high-cardinality
/// column — and a cap applied afterwards has already paid for the allocation it exists to
/// prevent, on the durable path, before the commit.
struct Capped {
    buf: Vec<u8>,
    limit: usize,
}

impl Capped {
    fn new(limit: usize) -> Self {
        Self {
            // Not `with_capacity(limit)`: the limit is a ceiling on what is allowed, not a
            // prediction of what is usual, and an ordinary payload is a few hundred bytes.
            buf: Vec::new(),
            limit,
        }
    }
}

impl std::io::Write for Capped {
    fn write(&mut self, chunk: &[u8]) -> std::io::Result<usize> {
        if self.buf.len() + chunk.len() > self.limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("over the {} byte limit", self.limit),
            ));
        }
        self.buf.extend_from_slice(chunk);
        Ok(chunk.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Consecutive failures, and whether to stop paying for payloads nobody receives.
///
/// This is a circuit breaker rather than a retry policy — it does the opposite job. Its
/// value is not that it resends anything; it is that while the sink is down, [`Publisher`]
/// skips *rendering* as well as sending, which keeps a dead endpoint off the durable path
/// entirely instead of running an aggregation per batch for messages that go nowhere.
///
/// The clock is passed in rather than read, so the tests state the behaviour instead of
/// sleeping through it.
#[derive(Debug)]
struct Breaker {
    threshold: u32,
    cooldown: Duration,
    consecutive_failures: AtomicU32,
    /// Millis since `origin` at which the breaker may be tried again. 0 means closed.
    open_until_millis: AtomicU64,
}

impl Breaker {
    fn new(threshold: u32, cooldown: Duration) -> Self {
        Self {
            threshold,
            cooldown,
            consecutive_failures: AtomicU32::new(0),
            open_until_millis: AtomicU64::new(0),
        }
    }

    fn is_open(&self, elapsed: Duration) -> bool {
        let until = self.open_until_millis.load(Ordering::Relaxed);
        until != 0 && (elapsed.as_millis() as u64) < until
    }

    fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.open_until_millis.store(0, Ordering::Relaxed);
    }

    /// Returns true when this failure is the one that opens the breaker.
    fn record_failure(&self, elapsed: Duration) -> bool {
        let n = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if self.threshold > 0 && n >= self.threshold {
            let until = elapsed.as_millis() as u64 + self.cooldown.as_millis() as u64;
            self.open_until_millis.store(until, Ordering::Relaxed);
            return true;
        }
        false
    }
}

/// A pipeline's realtime publisher: what to build, and where to send it.
pub struct Publisher {
    pipeline: String,
    app_id: String,
    model: String,
    group: String,
    transform: SqlTransform,
    sink: std::sync::Arc<dyn PostCommitPublisher>,
    max_bytes: usize,
    breaker: Breaker,
    /// Origin for the breaker's clock, so it needs no wall time.
    origin: Instant,
}

impl Publisher {
    /// Build a publisher for this pipeline, or `None` if it does not publish.
    ///
    /// Never returns an error. Every way this can fail leaves the pipeline streaming to
    /// Delta exactly as it would have, having said what it found — the same contract the
    /// config resolver keeps, restated here because a caller must not be able to get this
    /// wrong by ignoring a `Result`.
    pub fn open(cfg: &ResolvedPipeline) -> Option<Self> {
        let (model, sink_cfg) = match (&cfg.publish, &cfg.publish_to) {
            (Some(m), Some(s)) => (m, s),
            _ => return None,
        };

        // Defensive, and deliberately not reachable: `publish_problem` refused this in the
        // resolver, and the dbt gate refused it before that. A merge does not say what the
        // dashboard delta was, so if this ever fires it is a bug rather than a setting.
        if cfg.write_mode.keeps_one_row_per_key() {
            warn!(
                pipeline = %cfg.name,
                "refusing to publish from a pipeline that merges; this should have been \
                 caught at config load"
            );
            return None;
        }

        let sink = Self::sink(sink_cfg, &cfg.name)?;

        Some(Self {
            pipeline: cfg.name.clone(),
            app_id: cfg.app_id.clone(),
            model: model.model.clone(),
            group: model.group.clone(),
            // Already normalised and validated by the resolver; this re-normalisation is
            // idempotent and protects a library caller that built the config by hand.
            transform: SqlTransform::new_per_batch(&model.publish_sql),
            sink,
            max_bytes: sink_cfg.max_message_bytes() as usize,
            breaker: Breaker::new(
                sink_cfg.failure_threshold,
                Duration::from_secs(sink_cfg.breaker_cooldown_secs),
            ),
            origin: Instant::now(),
        })
    }

    fn sink(
        cfg: &PublisherConfig,
        pipeline: &str,
    ) -> Option<std::sync::Arc<dyn PostCommitPublisher>> {
        match cfg.kind {
            PublisherKind::Webpubsub => {
                let conn = match cfg.connection_string() {
                    Ok(s) => match webpubsub::Connection::parse(&s) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(
                                pipeline,
                                "not publishing: {e}. The Delta stream is unaffected."
                            );
                            return None;
                        }
                    },
                    Err(e) => {
                        warn!(
                            pipeline,
                            "not publishing: {e}. The Delta stream is unaffected."
                        );
                        return None;
                    }
                };
                match webpubsub::WebPubSubPublisher::new(
                    conn,
                    &cfg.hub,
                    cfg.message_ttl_secs,
                    Duration::from_secs(cfg.timeout_secs),
                ) {
                    Ok(p) => Some(std::sync::Arc::new(p)),
                    Err(e) => {
                        warn!(
                            pipeline,
                            "not publishing: {e}. The Delta stream is unaffected."
                        );
                        None
                    }
                }
            }
        }
    }

    /// Build a publisher around an arbitrary sink. For tests, and for a library caller
    /// wiring in a backend of their own.
    pub fn with_sink(
        cfg: &ResolvedPipeline,
        model: &PublishModel,
        sink: std::sync::Arc<dyn PostCommitPublisher>,
        max_bytes: usize,
        failure_threshold: u32,
        cooldown: Duration,
    ) -> Self {
        Self {
            pipeline: cfg.name.clone(),
            app_id: cfg.app_id.clone(),
            model: model.model.clone(),
            group: model.group.clone(),
            transform: SqlTransform::new_per_batch(&model.publish_sql),
            sink,
            max_bytes,
            breaker: Breaker::new(failure_threshold, cooldown),
            origin: Instant::now(),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "{} group {:?} from {}",
            self.sink.describe(),
            self.group,
            self.model
        )
    }

    pub fn group(&self) -> &str {
        &self.group
    }

    /// True while the breaker is open, meaning the sink has been failing.
    ///
    /// Checked by the caller *before* the commit so a dead endpoint costs no aggregation.
    pub fn is_paused(&self) -> bool {
        self.breaker.is_open(self.origin.elapsed())
    }

    /// Run the publish SQL over the batch that is about to be committed.
    ///
    /// Called **before** the commit, for two reasons that point the same way. The coerced
    /// batch is moved into the writer, so it is not available afterwards without keeping it
    /// alive across the write — and keeping it alive would hold up to `max_bytes_per_batch`
    /// in memory during the one operation this tool is careful about, whereas the aggregate
    /// it produces is a handful of rows. Publication still happens strictly after the commit;
    /// what happens early is only the arithmetic.
    ///
    /// Returns `None` on any failure, having logged it. Nothing about a dashboard payload
    /// may fail a batch.
    pub async fn render(&self, schema: SchemaRef, batches: Vec<RecordBatch>) -> Option<Rendered> {
        let out = match self.transform.run(schema, batches).await {
            Ok(out) => out,
            Err(e) => {
                warn!(
                    pipeline = %self.pipeline,
                    model = %self.model,
                    "publish transform failed, so this batch publishes nothing: {e}"
                );
                return None;
            }
        };

        let row_count: usize = out.iter().map(|b| b.num_rows()).sum();
        match self.rows_to_json(&out) {
            Ok(rows) => Some(Rendered {
                rows,
                row_count,
                complete: true,
            }),
            Err(e) => {
                // Over the cap, or unserialisable. Either way the client is told to reload
                // rather than left believing nothing happened.
                debug!(pipeline = %self.pipeline, "publishing an incomplete payload: {e}");
                Some(Rendered {
                    rows: serde_json::Value::Array(Vec::new()),
                    row_count,
                    complete: false,
                })
            }
        }
    }

    /// Serialise the aggregate, refusing to build something too large to send.
    ///
    /// The size is checked against the serialised bytes rather than trusted to be small:
    /// "an aggregate is small by construction" is a convention, and nothing stops a model
    /// grouping by a high-cardinality column. The writer is fed batch by batch so the check
    /// bounds the allocation as it grows rather than after it has already happened.
    fn rows_to_json(&self, batches: &[RecordBatch]) -> Result<serde_json::Value> {
        use deltalake::arrow::json::writer::{ArrayWriter, WriterBuilder};

        let mut writer: ArrayWriter<Capped> = WriterBuilder::new()
            // A missing key and an explicit null mean different things to a client applying
            // a delta, so nulls are written rather than omitted.
            .with_explicit_nulls(true)
            .build(Capped::new(self.max_bytes));

        let too_big = |e: &dyn std::fmt::Display| {
            crate::error::Error::Transform(format!(
                "publish payload for {:?} does not fit in {} bytes ({e})",
                self.model, self.max_bytes
            ))
        };

        for batch in batches {
            writer.write(batch).map_err(|e| too_big(&e))?;
        }
        writer.finish().map_err(|e| too_big(&e))?;

        let buf = writer.into_inner().buf;
        if buf.is_empty() {
            return Ok(serde_json::Value::Array(Vec::new()));
        }
        serde_json::from_slice(&buf).map_err(|e| {
            crate::error::Error::Transform(format!("could not read back the publish payload: {e}"))
        })
    }

    /// Send what [`Publisher::render`] produced, now that the commit is durable.
    ///
    /// **Returns stats and never an error, deliberately.** Returning `Err` from here would
    /// still be *data-safe* — the `txn` action already moved, so a retry reopens and does not
    /// reprocess the batch — but it would be *observability-wrong*: the caller's
    /// `StepOutcome::Progressed` would never reach the supervisor, so a batch that committed
    /// would go uncounted, the pipeline's liveness gauge would drop to zero, and `--once`
    /// would exit non-zero for a run that succeeded. So: swallow, log, count.
    ///
    /// `prev_source_version` is the previous publication's last source version, held by the
    /// caller across steps. See [`Envelope`] for why it is not derived from the batch.
    pub async fn send(
        &self,
        pending: Option<Rendered>,
        prev_source_version: Option<Version>,
        from_source_version: Version,
        source_version: Version,
        target_version: Option<Version>,
    ) -> PublishStats {
        let elapsed = self.origin.elapsed();

        // `render` returns None only after logging why, and the caller skips rendering
        // entirely while the breaker is open — so these two cases are distinguished here
        // rather than conflated into one counter.
        let Some(rendered) = pending else {
            return if self.breaker.is_open(elapsed) {
                PublishStats {
                    skipped: true,
                    ..Default::default()
                }
            } else {
                PublishStats {
                    failed: true,
                    ..Default::default()
                }
            };
        };

        let envelope = Envelope {
            ddi: ENVELOPE_VERSION,
            pipeline: self.pipeline.clone(),
            app_id: self.app_id.clone(),
            model: self.model.clone(),
            group: self.group.clone(),
            prev_source_version,
            from_source_version,
            source_version,
            target_version,
            committed_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            complete: rendered.complete,
            row_count: rendered.row_count,
            rows: rendered.rows,
        };

        let body = match serde_json::to_vec(&envelope) {
            Ok(b) => b,
            Err(e) => {
                warn!(pipeline = %self.pipeline, "could not serialise the publish envelope: {e}");
                return PublishStats {
                    failed: true,
                    ..Default::default()
                };
            }
        };

        let bytes = body.len();
        let rows = envelope.row_count;
        match self.sink.publish(&self.group, body).await {
            Ok(()) => {
                self.breaker.record_success();
                PublishStats {
                    sent: true,
                    truncated: !envelope.complete,
                    rows,
                    bytes,
                    ..Default::default()
                }
            }
            Err(e) => {
                // One attempt, no retry: the batch cannot be replayed, and delaying the next
                // one to resend this would cost more than the client's own reload.
                warn!(
                    pipeline = %self.pipeline,
                    source_version,
                    "publishing failed; the Delta commit is unaffected: {e}"
                );
                if self.breaker.record_failure(elapsed) {
                    info!(
                        pipeline = %self.pipeline,
                        "pausing realtime publication after repeated failures; the Delta \
                         stream continues and publication resumes on its own"
                    );
                }
                PublishStats {
                    failed: true,
                    rows,
                    bytes,
                    ..Default::default()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::arrow::array::{Int64Array, StringArray};
    use deltalake::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::{Arc, Mutex};

    /// Records what it was asked to send, and fails on demand.
    struct Recorder {
        sent: Mutex<Vec<(String, Vec<u8>)>>,
        fail: bool,
    }

    impl Recorder {
        fn new(fail: bool) -> Arc<Self> {
            Arc::new(Self {
                sent: Mutex::new(Vec::new()),
                fail,
            })
        }
        fn envelopes(&self) -> Vec<Envelope> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|(_, b)| serde_json::from_slice(b).expect("we wrote it"))
                .collect()
        }
    }

    #[async_trait]
    impl PostCommitPublisher for Recorder {
        async fn publish(&self, group: &str, body: Vec<u8>) -> Result<()> {
            self.sent.lock().unwrap().push((group.into(), body));
            if self.fail {
                return Err(crate::error::Error::Config("the hub is down".into()));
            }
            Ok(())
        }
        fn describe(&self) -> String {
            "recorder".into()
        }
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("country", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]))
    }

    fn batch(countries: &[&str], amounts: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(countries.to_vec())),
                Arc::new(Int64Array::from(amounts.to_vec())),
            ],
        )
        .unwrap()
    }

    fn pipeline_cfg() -> ResolvedPipeline {
        let toml = "[[pipeline]]\nname = \"orders\"\napp_id = \"ddi.orders\"\n\
                    source_uri = \"/tmp/bronze/orders\"\ntarget_uri = \"/tmp/silver/orders\"\n";
        crate::config::Config::from_toml_str(toml)
            .unwrap()
            .resolve_all()
            .unwrap()
            .pipelines
            .pop()
            .unwrap()
    }

    fn model(sql: &str) -> PublishModel {
        PublishModel {
            model: "orders_live".into(),
            kind: PublisherKind::Webpubsub,
            group: "sales".into(),
            publish_sql: sql.into(),
        }
    }

    fn publisher(sink: Arc<dyn PostCommitPublisher>, max_bytes: usize) -> Publisher {
        Publisher::with_sink(
            &pipeline_cfg(),
            &model("SELECT country, sum(amount) AS sales_delta FROM source GROUP BY country"),
            sink,
            max_bytes,
            2,
            Duration::from_secs(30),
        )
    }

    #[tokio::test]
    async fn the_envelope_carries_the_aggregate_and_both_ends_of_the_range() {
        let sink = Recorder::new(false);
        let p = publisher(sink.clone(), 900_000);
        let rendered = p
            .render(schema(), vec![batch(&["NL", "NL", "DE"], &[10, 30, 20])])
            .await
            .expect("the aggregation is valid");
        let stats = p.send(Some(rendered), Some(120), 121, 123, Some(456)).await;

        assert!(stats.sent && !stats.failed, "{stats:?}");
        let e = &sink.envelopes()[0];
        assert_eq!(e.ddi, 1);
        assert_eq!(e.pipeline, "orders");
        assert_eq!(e.app_id, "ddi.orders");
        assert_eq!(e.model, "orders_live");
        assert_eq!(e.group, "sales");
        assert_eq!(e.prev_source_version, Some(120));
        assert_eq!(e.from_source_version, 121);
        assert_eq!(e.source_version, 123);
        assert_eq!(e.target_version, Some(456));
        assert!(e.complete);
        assert_eq!(e.row_count, 2, "one row per country: {:?}", e.rows);

        // The aggregate itself: NL is 10 + 30, and it is a delta for this batch alone.
        let rows = e.rows.as_array().expect("an array of objects");
        let nl = rows
            .iter()
            .find(|r| r["country"] == "NL")
            .expect("NL is present");
        assert_eq!(nl["sales_delta"], 40);
    }

    #[tokio::test]
    async fn a_zero_row_batch_still_publishes_so_the_chain_stays_contiguous() {
        // A fully-filtered batch still commits and still moves the offset. Publishing
        // nothing would make the *next* message look like one the client lost.
        let sink = Recorder::new(false);
        let p = publisher(sink.clone(), 900_000);
        let empty = RecordBatch::new_empty(schema());
        let rendered = p.render(schema(), vec![empty]).await.expect("valid");
        let stats = p.send(Some(rendered), Some(7), 8, 9, Some(3)).await;

        assert!(stats.sent);
        let e = &sink.envelopes()[0];
        assert_eq!(e.row_count, 0);
        assert_eq!(e.source_version, 9);
        assert!(e.complete);
    }

    #[tokio::test]
    async fn an_oversize_payload_is_marked_incomplete_rather_than_chunked_or_dropped() {
        // A client that applied chunk 1 and missed chunk 2 would be silently wrong; one
        // that is told the payload did not fit reloads and is right.
        let sink = Recorder::new(false);
        let p = publisher(sink.clone(), 64);
        let countries: Vec<String> = (0..500).map(|i| format!("country_{i}")).collect();
        let refs: Vec<&str> = countries.iter().map(|s| s.as_str()).collect();
        let amounts: Vec<i64> = (0..500).collect();
        let rendered = p
            .render(schema(), vec![batch(&refs, &amounts)])
            .await
            .expect("still renders, just not completely");
        let stats = p.send(Some(rendered), None, 1, 1, Some(1)).await;

        assert!(stats.sent && stats.truncated, "{stats:?}");
        let e = &sink.envelopes()[0];
        assert!(!e.complete, "the client is told to reload");
        assert_eq!(
            e.rows.as_array().unwrap().len(),
            0,
            "and gets no partial rows"
        );
        assert_eq!(e.row_count, 500, "but is told what it missed");
    }

    #[tokio::test]
    async fn a_failing_sink_reports_failure_and_never_an_error() {
        let sink = Recorder::new(true);
        let p = publisher(sink.clone(), 900_000);
        let rendered = p
            .render(schema(), vec![batch(&["NL"], &[1])])
            .await
            .unwrap();
        // The signature is the point: there is no `?` to write here even if a caller wanted
        // to, so a publisher cannot fail the commit that preceded it.
        let stats = p.send(Some(rendered), None, 1, 1, Some(1)).await;
        assert!(stats.failed && !stats.sent, "{stats:?}");
    }

    #[tokio::test]
    async fn the_breaker_opens_after_consecutive_failures_and_pauses_rendering() {
        let sink = Recorder::new(true);
        let p = publisher(sink.clone(), 900_000); // threshold 2
        assert!(!p.is_paused(), "starts closed");

        for _ in 0..2 {
            let r = p
                .render(schema(), vec![batch(&["NL"], &[1])])
                .await
                .unwrap();
            p.send(Some(r), None, 1, 1, Some(1)).await;
        }
        assert!(p.is_paused(), "two failures at threshold 2 opens it");

        // And while open, the caller skips rendering entirely — which is the point of it.
        let stats = p.send(None, None, 1, 1, Some(1)).await;
        assert!(stats.skipped && !stats.failed, "{stats:?}");
    }

    #[tokio::test]
    async fn a_success_closes_the_breaker_again() {
        let sink = Recorder::new(false);
        let p = publisher(sink.clone(), 900_000);
        p.breaker.record_failure(Duration::from_millis(1));
        let r = p
            .render(schema(), vec![batch(&["NL"], &[1])])
            .await
            .unwrap();
        p.send(Some(r), None, 1, 1, Some(1)).await;
        assert_eq!(p.breaker.consecutive_failures.load(Ordering::Relaxed), 0);
        assert!(!p.is_paused());
    }

    #[tokio::test]
    async fn a_render_failure_is_distinguished_from_a_paused_breaker() {
        // Both arrive at `send` as `None`; conflating them would report a dead endpoint as
        // a broken model.
        let sink = Recorder::new(false);
        let p = publisher(sink.clone(), 900_000);
        let stats = p.send(None, None, 1, 1, Some(1)).await;
        assert!(stats.failed && !stats.skipped, "{stats:?}");
    }

    #[tokio::test]
    async fn invalid_publish_sql_renders_nothing_and_raises_no_error() {
        let sink = Recorder::new(false);
        let p = Publisher::with_sink(
            &pipeline_cfg(),
            &model("SELECT no_such_column FROM source"),
            sink.clone(),
            900_000,
            5,
            Duration::from_secs(30),
        );
        assert!(p
            .render(schema(), vec![batch(&["NL"], &[1])])
            .await
            .is_none());
        assert!(sink.envelopes().is_empty());
    }

    #[test]
    fn open_returns_none_for_a_pipeline_that_does_not_publish() {
        assert!(Publisher::open(&pipeline_cfg()).is_none());
    }

    #[test]
    fn open_refuses_a_pipeline_that_merges() {
        // Defensive: the resolver and the dbt gate both refused this already.
        let mut cfg = pipeline_cfg();
        cfg.write_mode = crate::config::WriteMode::Upsert;
        cfg.publish = Some(model("SELECT count(*) AS c FROM source"));
        cfg.publish_to = Some(PublisherConfig {
            kind: PublisherKind::Webpubsub,
            connection_string: Some("Endpoint=https://x;AccessKey=k".into()),
            connection_string_env: None,
            hub: "ddi".into(),
            message_ttl_secs: 60,
            timeout_secs: 5,
            failure_threshold: 5,
            breaker_cooldown_secs: 30,
            max_message_bytes: "900KB".into(),
        });
        assert!(Publisher::open(&cfg).is_none());
    }
}
