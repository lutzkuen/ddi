//! Confluent-framed Avro messages into Arrow record batches (docs/kafka-design.md §5.1, §5.4,
//! §6.1).
//!
//! # One decoder per writer schema
//!
//! Each message names its writer schema in its header, and goes to that schema's own decoder,
//! so no decoder ever switches schema mid-batch and interleaved writer versions cost nothing.
//! A decoder holds at most [`CHUNK_ROWS`] rows before they are flushed into a batch; the raw
//! messages of those rows (the *chunk*) are kept beside it until then, and the Kafka metadata
//! columns are built from them at the flush, so a row's metadata can only ever be its own
//! message's.
//!
//! # One message, one row, or none
//!
//! arrow-avro's `Decoder` decodes a stream, not messages: given a truncated body it consumes
//! the header, waits for more bytes and reports success (reader/mod.rs:706-735), and a message
//! that fails half-way leaves the fields it did decode in the column builders. So every message
//! is checked to have become exactly one row from exactly its own bytes, and one that did not
//! is *isolated*: the decoder, which may hold part of its row, is replaced, the chunk's earlier
//! messages are decoded again into the new one and flushed, and the message alone goes to the
//! data-quality table. One malformed message cannot change or lose another's row (I10).
//!
//! # What comes out
//!
//! Batches in the writer schema's own shape, normalised the way the target's Delta schema reads
//! (UTC timestamps, the kernel's `element`/`key_value` container names, enums and uuids as
//! text, no Avro metadata on the fields), followed by the Kafka metadata columns (§6.1). And for
//! every message that could not be decoded, its coordinates, bytes and reason, as one
//! [`Rejected`] for the data-quality table (§5.7).
//!
//! Nothing here touches Kafka or Delta: messages come in as [`KafkaMessage`] values, which the
//! consumer copies out of librdkafka (§3.3).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use arrow_avro::codec::Tz;
use arrow_avro::reader::ReaderBuilder;
use arrow_avro::schema::{
    AvroSchema, Fingerprint, FingerprintAlgorithm, SchemaStore, AVRO_NAMESPACE_METADATA_KEY,
    AVRO_NAME_METADATA_KEY,
};
use base64::Engine as _;
use deltalake::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, FixedSizeBinaryArray, Int32Array, Int64Array, ListArray,
    MapArray, StringArray, StructArray, TimestampMicrosecondArray, UnionArray,
};
use deltalake::arrow::buffer::OffsetBuffer;
use deltalake::arrow::datatypes::{
    DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit, UnionFields,
};
use deltalake::arrow::error::ArrowError;
use deltalake::arrow::record_batch::RecordBatch;

use crate::error::{Error, Result};
use crate::kafka::registry::{Lookup, RegisteredSchema, WriterSchemas};
use crate::schema::Rejected;

/// Rows one writer schema's decoder holds before they are flushed into a batch. Also the most
/// raw messages kept per writer schema (§3.7).
pub const CHUNK_ROWS: usize = 8192;
/// The Confluent header: the magic byte 0 and a big-endian u32 schema id (§5.1).
pub const HEADER_LEN: usize = 5;
/// What a raw byte is taken to decode into before this batch has flushed anything to measure
/// (§3.3).
const INITIAL_DECODED_PER_RAW: f64 = 4.0;

// ------------------------------------------------------------------------------ messages

/// A Kafka message's timestamp, as librdkafka reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaTimestamp {
    NotAvailable,
    /// Milliseconds since the epoch, set by the producer.
    CreateTime(i64),
    /// Milliseconds since the epoch, set by the broker.
    LogAppendTime(i64),
}

impl KafkaTimestamp {
    pub fn millis(self) -> Option<i64> {
        match self {
            Self::NotAvailable => None,
            Self::CreateTime(ms) | Self::LogAppendTime(ms) => Some(ms),
        }
    }

    /// kdi's `kafka.timestamp` was milliseconds × 1000 (transforms.rs:468-475); so is ddi's,
    /// as a timestamp. `None` when there is none, or it is too far out to be one.
    fn micros(self) -> Option<i64> {
        self.millis().and_then(|ms| ms.checked_mul(1000))
    }

    /// kdi's `kafka.timestamp_type`: −1 not available, 0 create time, 1 log append time.
    pub fn type_code(self) -> i32 {
        match self {
            Self::NotAvailable => -1,
            Self::CreateTime(_) => 0,
            Self::LogAppendTime(_) => 1,
        }
    }
}

/// One header of a Kafka message; a key may repeat, and the order is the producer's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaHeader {
    pub key: String,
    pub value: Option<Vec<u8>>,
}

/// A Kafka message, copied out of librdkafka.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KafkaMessage {
    pub partition: i32,
    pub offset: i64,
    pub timestamp: KafkaTimestamp,
    pub key: Option<Vec<u8>>,
    /// `None` is a tombstone.
    pub value: Option<Vec<u8>>,
    pub headers: Vec<KafkaHeader>,
}

impl KafkaMessage {
    /// The bytes it arrived with, which is what the decoded size is measured against.
    fn raw_len(&self) -> u64 {
        let headers: usize = self
            .headers
            .iter()
            .map(|h| h.key.len() + h.value.as_ref().map_or(0, Vec::len))
            .sum();
        (self.key.as_ref().map_or(0, Vec::len) + self.value.as_ref().map_or(0, Vec::len) + headers)
            as u64
    }
}

/// The writer schema id of a Confluent-framed payload, or why it is not one (§5.1).
pub fn frame(payload: &[u8]) -> std::result::Result<u32, String> {
    match payload.first() {
        Some(0) => {}
        Some(b) => {
            let hint = match b {
                b'{' | b'[' | b'"' => "; is this topic JSON?",
                _ => "",
            };
            return Err(format!(
                "not Confluent-framed: the first byte is 0x{b:02x}, not 0x00{hint}"
            ));
        }
        None => return Err("not Confluent-framed: the value is empty".to_string()),
    }
    let Some(id) = payload.get(1..HEADER_LEN) else {
        return Err(format!(
            "not Confluent-framed: {} byte(s), fewer than the {HEADER_LEN}-byte header",
            payload.len()
        ));
    };
    Ok(u32::from_be_bytes(id.try_into().expect("four bytes")))
}

// ----------------------------------------------------------------- the decoder seam (§5.3)

/// The decoder of one writer schema. arrow-avro's is the one ddi uses; this is the seam an
/// apache-avro decoder would fill, should arrow-avro ever have to be replaced (§5.3).
pub trait Decode: Send {
    /// The Arrow schema of the rows, before normalisation.
    fn schema(&self) -> SchemaRef;
    /// Decodes one Confluent-framed message, header included, as exactly one row. When it was
    /// not exactly one row from exactly these bytes, says why; the decoder may then hold part
    /// of a row, and is not used again.
    fn decode_one(&mut self, framed: &[u8]) -> std::result::Result<(), String>;
    /// The rows decoded since the last flush, as one batch; `None` when there are none.
    fn flush(&mut self) -> std::result::Result<Option<RecordBatch>, String>;
}

/// Builds the decoder for one writer schema.
pub trait DecodeFactory: Send + Sync {
    fn build(&self, id: u32, writer_schema: &str) -> std::result::Result<Box<dyn Decode>, String>;
}

/// arrow-avro, pinned to deltalake's arrow (Cargo.toml): the production decoder.
#[derive(Clone, Copy, Debug, Default)]
pub struct ArrowAvro;

impl DecodeFactory for ArrowAvro {
    fn build(&self, id: u32, writer_schema: &str) -> std::result::Result<Box<dyn Decode>, String> {
        // A store holding exactly this id, so the decoder never switches schema
        // (schema.rs:861, 884).
        let mut store = SchemaStore::new_with_type(FingerprintAlgorithm::Id);
        store
            .set(
                Fingerprint::Id(id),
                AvroSchema::new(writer_schema.to_string()),
            )
            .map_err(|e| e.to_string())?;
        let build = || {
            ReaderBuilder::new()
                .with_writer_schema_store(store)
                .with_active_fingerprint(Fingerprint::Id(id))
                .with_batch_size(CHUNK_ROWS)
                .with_tz(Tz::Utc)
                .with_utf8_view(false)
                .build_decoder()
        };
        let inner = match catch_unwind(AssertUnwindSafe(build)) {
            Ok(Ok(d)) => d,
            Ok(Err(e)) => return Err(e.to_string()),
            Err(panic) => return Err(format!("arrow-avro panicked: {}", panic_text(&panic))),
        };
        Ok(Box::new(ArrowAvroDecoder { inner }))
    }
}

struct ArrowAvroDecoder {
    inner: arrow_avro::reader::Decoder,
}

impl Decode for ArrowAvroDecoder {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    /// The strict per-message check (§5.4): `decode` consumed every byte, and the decoder's
    /// capacity fell by exactly one row. arrow-avro alone would accept a truncated body (it
    /// waits for the rest), a second record after the first, or report an error after a row
    /// it already kept.
    fn decode_one(&mut self, framed: &[u8]) -> std::result::Result<(), String> {
        let before = self.inner.capacity();
        if before == 0 {
            return Err("internal: the decoder is full".to_string());
        }
        // A panic is arrow-avro's on input it did not expect, an overflow-checked negation of a
        // hostile block count among them: this message's failure, never the process's.
        let result = catch_unwind(AssertUnwindSafe(|| self.inner.decode(framed)));
        let rows = before.saturating_sub(self.inner.capacity());
        match result {
            Err(panic) => Err(format!(
                "the Avro decoder failed on this message: {}",
                panic_text(&panic)
            )),
            Ok(Err(e)) if rows == 0 => Err(format!("malformed Avro body: {e}")),
            Ok(Err(e)) => Err(format!("unexpected bytes after the record: {e}")),
            Ok(Ok(read)) => match rows {
                1 if read == framed.len() => Ok(()),
                1 => Err(format!(
                    "{} unexpected byte(s) after the record",
                    framed.len() - read
                )),
                0 if framed.len() <= HEADER_LEN => {
                    Err("truncated: no Avro body after the 5-byte header".to_string())
                }
                0 => Err(format!(
                    "truncated: the Avro body ends inside a value ({} byte(s) of body)",
                    framed.len() - HEADER_LEN
                )),
                n => Err(format!("the message holds {n} records, not one")),
            },
        }
    }

    fn flush(&mut self) -> std::result::Result<Option<RecordBatch>, String> {
        match catch_unwind(AssertUnwindSafe(|| self.inner.flush())) {
            Ok(r) => r.map_err(|e| e.to_string()),
            Err(panic) => Err(format!("arrow-avro panicked: {}", panic_text(&panic))),
        }
    }
}

fn panic_text(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic without a message".to_string())
}

// ------------------------------------------------------------------------------- results

/// A message that could not be decoded, and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Undecodable {
    pub partition: i32,
    pub offset: i64,
    /// The writer schema id its header named, when it had one.
    pub schema_id: Option<u32>,
    pub reason: String,
    /// Whether decoding it again could ever succeed: false only for a schema id the registry
    /// did not know, which may be registered later.
    pub deterministic: bool,
}

/// A message [`KafkaDecoder::feed`] did not take, because something other than the message
/// failed: the registry, or a writer schema that holds the pipeline. Fed again once the cause
/// clears, it is decoded as if for the first time.
#[derive(Debug)]
pub struct NotFed {
    pub error: Error,
    pub message: KafkaMessage,
}

/// What one batch of messages decoded into.
#[derive(Debug, Default)]
pub struct Decoded {
    /// Normalised rows followed by the metadata columns, one batch per flushed chunk; writer
    /// schemas may differ between batches.
    pub batches: Vec<RecordBatch>,
    /// Every message that could not be decoded, in the order they came.
    pub undecodable: Vec<Undecodable>,
    /// The same messages as rows for the data-quality table (§5.7): `None` when there are
    /// none.
    pub rejects: Option<Rejected>,
    /// Messages with no value, skipped and counted (kdi lib.rs:866-871).
    pub tombstones: u64,
    /// The batches' size, measured (§3.3).
    pub decoded_bytes: u64,
}

// ------------------------------------------------------------------------------- decoder

/// Decodes one pipeline's messages, batch by batch.
pub struct KafkaDecoder {
    topic: String,
    prefix: String,
    schemas: Arc<dyn WriterSchemas>,
    factory: Arc<dyn DecodeFactory>,
    groups: BTreeMap<u32, Group>,
    out: Out,
}

impl std::fmt::Debug for KafkaDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaDecoder")
            .field("topic", &self.topic)
            .field("prefix", &self.prefix)
            .field("writer_schemas", &self.groups.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

/// What one writer schema id turned out to be.
enum Group {
    Ready(Box<Ready>),
    /// Every message of this writer schema is undecodable, for this reason.
    Unusable(String),
    /// The writer schema has a top-level field named like a metadata column.
    Collision(String),
}

struct Ready {
    schema: Arc<RegisteredSchema>,
    decoder: Box<dyn Decode>,
    /// The messages whose rows are inside `decoder`, in order.
    chunk: Vec<KafkaMessage>,
    /// `(avro.name, avro.namespace)` of the schema's `fixed(16)` types with logicalType uuid.
    fixed_uuids: HashSet<(String, Option<String>)>,
}

/// What the current batch has produced so far.
#[derive(Default)]
struct Out {
    batches: Vec<RecordBatch>,
    undecodable: Vec<(KafkaMessage, Undecodable)>,
    tombstones: u64,
    /// Measured size of `batches`.
    flushed_bytes: u64,
    /// Raw size of the messages in `batches`.
    flushed_raw: u64,
    /// Raw size of the messages still inside decoders.
    pending_raw: u64,
}

impl KafkaDecoder {
    /// A decoder for `topic`'s messages, its metadata columns named with `prefix`, writer
    /// schemas from `schemas`.
    pub fn new(
        topic: impl Into<String>,
        prefix: impl Into<String>,
        schemas: Arc<dyn WriterSchemas>,
    ) -> Self {
        Self::with_factory(topic, prefix, schemas, Arc::new(ArrowAvro))
    }

    /// As [`Self::new`], decoding with `factory`'s decoders.
    pub fn with_factory(
        topic: impl Into<String>,
        prefix: impl Into<String>,
        schemas: Arc<dyn WriterSchemas>,
        factory: Arc<dyn DecodeFactory>,
    ) -> Self {
        Self {
            topic: topic.into(),
            prefix: prefix.into(),
            schemas,
            factory,
            groups: BTreeMap::new(),
            out: Out::default(),
        }
    }

    /// Takes one message into the current batch: as a row, as an undecodable message, or as a
    /// counted tombstone. Fetches its writer schema first when it is new.
    ///
    /// Gives the message back, untouched, when its writer schema could not be had for a reason
    /// that is not the message's (§5.2), or holds the pipeline (§6.1).
    pub async fn feed(&mut self, message: KafkaMessage) -> std::result::Result<(), NotFed> {
        let Some(value) = message.value.as_deref() else {
            self.out.tombstones += 1;
            return Ok(());
        };
        let id = match frame(value) {
            Ok(id) => id,
            Err(reason) => {
                self.out.reject(message, None, reason, true);
                return Ok(());
            }
        };
        if !self.groups.contains_key(&id) {
            match self.schemas.writer_schema(id).await {
                Err(error) => return Err(NotFed { error, message }),
                Ok(Lookup::Undecodable {
                    reason,
                    deterministic,
                }) => {
                    if deterministic {
                        self.groups.insert(id, Group::Unusable(reason.clone()));
                    }
                    self.out.reject(message, Some(id), reason, deterministic);
                    return Ok(());
                }
                Ok(Lookup::Avro(schema)) => {
                    let group = self.group_for(schema);
                    self.groups.insert(id, group);
                }
            }
        }
        match self.groups.get_mut(&id).expect("inserted above") {
            Group::Unusable(reason) => {
                let reason = reason.clone();
                self.out.reject(message, Some(id), reason, true);
            }
            Group::Collision(field) => {
                let error = Error::KafkaMetadataCollision {
                    schema_id: id,
                    field: field.clone(),
                    prefix: self.prefix.clone(),
                };
                return Err(NotFed { error, message });
            }
            Group::Ready(group) => {
                let ctx = Ctx {
                    topic: &self.topic,
                    prefix: &self.prefix,
                    factory: self.factory.as_ref(),
                    id,
                };
                ctx.decode(group, &mut self.out, message);
            }
        }
        Ok(())
    }

    /// The current batch's decoded size: what its flushed chunks measure, and the rows still
    /// inside the decoders at the ratio the flushed ones showed, 4× their raw size before any
    /// has (§3.3). Builder capacity is not counted.
    pub fn decoded_bytes(&self) -> u64 {
        let out = &self.out;
        let ratio = if out.flushed_raw > 0 {
            out.flushed_bytes as f64 / out.flushed_raw as f64
        } else {
            INITIAL_DECODED_PER_RAW
        };
        out.flushed_bytes + (out.pending_raw as f64 * ratio) as u64
    }

    /// Messages taken into the current batch so far, tombstones and undecodable ones included.
    pub fn is_empty(&self) -> bool {
        let out = &self.out;
        out.batches.is_empty()
            && out.undecodable.is_empty()
            && out.tombstones == 0
            && out.pending_raw == 0
            && self.groups.values().all(|g| match g {
                Group::Ready(r) => r.chunk.is_empty(),
                _ => true,
            })
    }

    /// Closes the current batch: flushes every writer schema's rows and hands over everything
    /// the batch produced. The next message starts a new batch.
    pub fn finish(&mut self) -> Result<Decoded> {
        for (id, group) in self.groups.iter_mut() {
            if let Group::Ready(group) = group {
                let ctx = Ctx {
                    topic: &self.topic,
                    prefix: &self.prefix,
                    factory: self.factory.as_ref(),
                    id: *id,
                };
                ctx.flush(group, &mut self.out);
            }
        }
        let out = std::mem::take(&mut self.out);
        let rejects = rejected_rows(&self.topic, &self.prefix, &out.undecodable)?;
        Ok(Decoded {
            batches: out.batches,
            undecodable: out.undecodable.into_iter().map(|(_, u)| u).collect(),
            rejects,
            tombstones: out.tombstones,
            decoded_bytes: out.flushed_bytes,
        })
    }

    /// Drops the current batch, rows still inside decoders included: for a revoke that does not
    /// commit it (§3.4). Writer schemas stay known.
    pub fn discard(&mut self) {
        let factory = self.factory.clone();
        self.groups.retain(|id, group| match group {
            Group::Ready(r) if !r.chunk.is_empty() => {
                r.chunk.clear();
                match factory.build(*id, &r.schema.schema) {
                    Ok(d) => {
                        r.decoder = d;
                        true
                    }
                    // It built before; should it not now, the id is looked up afresh.
                    Err(_) => false,
                }
            }
            _ => true,
        });
        self.out = Out::default();
    }

    /// The writer schemas this decoder has met and can decode, by id.
    pub fn writer_schemas(&self) -> Vec<Arc<RegisteredSchema>> {
        self.groups
            .values()
            .filter_map(|g| match g {
                Group::Ready(r) => Some(r.schema.clone()),
                _ => None,
            })
            .collect()
    }

    fn group_for(&self, schema: Arc<RegisteredSchema>) -> Group {
        let id = schema.id;
        if let Err(kind) = top_level_record(&schema.schema) {
            return Group::Unusable(format!(
                "writer schema {id} is not a record at the top level but {kind}; \
                 kafka-delta-ingest could not ingest such a value either"
            ));
        }
        let decoder = match self.factory.build(id, &schema.schema) {
            Ok(d) => d,
            Err(e) => {
                return Group::Unusable(format!("cannot decode writer schema {id}: {e}"));
            }
        };
        let arrow = decoder.schema();
        if let Some(f) = arrow
            .fields()
            .iter()
            .find(|f| f.name().starts_with(&self.prefix))
        {
            return Group::Collision(f.name().clone());
        }
        if zero_width(&DataType::Struct(arrow.fields().clone())) {
            // Its records encode to no bytes at all, and arrow-avro's `Decoder` decodes a body
            // only once it is handed at least one byte of it (reader/mod.rs:708): a header
            // alone leaves it waiting for the next message's bytes.
            return Group::Unusable(format!(
                "writer schema {id} encodes every record in zero bytes (it has no field that \
                 takes any), which arrow-avro cannot decode from a message alone"
            ));
        }
        if let Some(path) = zero_width_items(arrow.fields()) {
            // A block count is the producer's to write, and with nothing to read per item
            // nothing would bound the loop over it.
            return Group::Unusable(format!(
                "writer schema {id} has an array whose items take no bytes ({path}), which a \
                 hostile item count could keep the decoder busy on for good; ddi does not \
                 decode it"
            ));
        }
        Group::Ready(Box::new(Ready {
            fixed_uuids: fixed_uuids(&schema.schema),
            schema,
            decoder,
            chunk: Vec::new(),
        }))
    }
}

/// What a writer schema's group needs from the decoder to flush.
struct Ctx<'a> {
    topic: &'a str,
    prefix: &'a str,
    factory: &'a dyn DecodeFactory,
    id: u32,
}

impl Ctx<'_> {
    fn decode(&self, group: &mut Ready, out: &mut Out, message: KafkaMessage) {
        let value = message
            .value
            .as_deref()
            .expect("a tombstone is not decoded");
        match group.decoder.decode_one(value) {
            Ok(()) => {
                out.pending_raw += message.raw_len();
                group.chunk.push(message);
                if group.chunk.len() >= CHUNK_ROWS {
                    self.flush(group, out);
                }
            }
            Err(reason) => {
                self.isolate(group, out);
                out.reject(message, Some(self.id), reason, true);
            }
        }
    }

    /// The decoder may hold part of a row: a new one decodes the chunk's messages again (each
    /// passed before, and decoding is deterministic) and flushes them at once, so each good
    /// message is decoded again at most once however many bad ones follow it.
    fn isolate(&self, group: &mut Ready, out: &mut Out) {
        let mut good = std::mem::take(&mut group.chunk);
        'again: loop {
            match self.factory.build(self.id, &group.schema.schema) {
                Ok(d) => group.decoder = d,
                Err(e) => {
                    // It built before. Should it not now, nothing of the chunk is lost: each
                    // message goes to the data-quality table.
                    for m in good {
                        out.pending_raw -= m.raw_len();
                        let reason = format!("cannot decode writer schema {}: {e}", self.id);
                        out.reject(m, Some(self.id), reason, false);
                    }
                    return;
                }
            }
            for i in 0..good.len() {
                let value = good[i].value.as_deref().expect("decoded before");
                if let Err(reason) = group.decoder.decode_one(value) {
                    let m = good.remove(i);
                    out.pending_raw -= m.raw_len();
                    let reason = format!("{reason} (on decoding it again; it decoded before)");
                    out.reject(m, Some(self.id), reason, false);
                    continue 'again;
                }
            }
            break;
        }
        group.chunk = good;
        self.flush(group, out);
    }

    /// The chunk's rows as a batch, normalised and with their metadata columns.
    fn flush(&self, group: &mut Ready, out: &mut Out) {
        if group.chunk.is_empty() {
            return;
        }
        let chunk = std::mem::take(&mut group.chunk);
        let raw: u64 = chunk.iter().map(KafkaMessage::raw_len).sum();
        out.pending_raw -= raw;
        let rows = match group.decoder.flush() {
            Ok(Some(b)) if b.num_rows() == chunk.len() => normalise(b, &group.fixed_uuids).ok(),
            _ => None,
        };
        match rows {
            Some(rows) => self.emit(rows, chunk, raw, out),
            None => {
                // Each message alone, so that what failed is one message's.
                if let Ok(d) = self.factory.build(self.id, &group.schema.schema) {
                    group.decoder = d;
                }
                self.flush_one_by_one(group, chunk, out);
            }
        }
    }

    fn flush_one_by_one(&self, group: &Ready, chunk: Vec<KafkaMessage>, out: &mut Out) {
        let mut rows = Vec::new();
        let mut kept = Vec::new();
        for m in chunk {
            let one = self
                .factory
                .build(self.id, &group.schema.schema)
                .and_then(|mut d| {
                    d.decode_one(m.value.as_deref().unwrap_or_default())?;
                    match d.flush()? {
                        Some(b) if b.num_rows() == 1 => {
                            normalise(b, &group.fixed_uuids).map_err(|e| e.to_string())
                        }
                        _ => Err("decoded, but no row came out".to_string()),
                    }
                });
            match one {
                Ok(b) => {
                    rows.push(b);
                    kept.push(m);
                }
                Err(e) => {
                    let reason = format!("cannot turn the decoded record into Arrow: {e}");
                    out.reject(m, Some(self.id), reason, true)
                }
            }
        }
        if rows.is_empty() {
            return;
        }
        let raw = kept.iter().map(KafkaMessage::raw_len).sum();
        match deltalake::arrow::compute::concat_batches(&rows[0].schema(), &rows) {
            Ok(b) => self.emit(b, kept, raw, out),
            Err(e) => {
                for m in kept {
                    let reason = format!("internal: cannot assemble the batch: {e}");
                    out.reject(m, Some(self.id), reason, false);
                }
            }
        }
    }

    fn emit(&self, rows: RecordBatch, chunk: Vec<KafkaMessage>, raw: u64, out: &mut Out) {
        let mut fields: Vec<FieldRef> = rows.schema().fields().iter().cloned().collect();
        fields.extend(metadata_fields(self.prefix).into_iter().map(Arc::new));
        let mut columns = rows.columns().to_vec();
        columns.extend(metadata_columns(self.topic, &chunk, self.id));
        match RecordBatch::try_new(Arc::new(Schema::new(fields)), columns) {
            Ok(batch) => {
                out.flushed_bytes += measured_size(&batch);
                out.flushed_raw += raw;
                out.batches.push(batch);
            }
            Err(e) => {
                for m in chunk {
                    let reason = format!("internal: cannot assemble the batch: {e}");
                    out.reject(m, Some(self.id), reason, false);
                }
            }
        }
    }
}

impl Out {
    fn reject(
        &mut self,
        message: KafkaMessage,
        schema_id: Option<u32>,
        reason: String,
        deterministic: bool,
    ) {
        let u = Undecodable {
            partition: message.partition,
            offset: message.offset,
            schema_id,
            reason,
            deterministic,
        };
        self.undecodable.push((message, u));
    }
}

/// The used bytes of a batch's arrays (`get_slice_memory_size`), not their capacity: arrow-avro
/// sizes its builders for 1024 rows whatever arrives (§3.3, EO-M5).
fn measured_size(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|c| {
            c.to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| c.get_array_memory_size()) as u64
        })
        .sum()
}

// ---------------------------------------------------------------- metadata columns (§6.1)

/// The Kafka metadata columns, appended after every batch's Avro fields.
pub fn metadata_fields(prefix: &str) -> Vec<Field> {
    vec![
        Field::new(format!("{prefix}topic"), DataType::Utf8, false),
        Field::new(format!("{prefix}partition"), DataType::Int32, false),
        Field::new(format!("{prefix}offset"), DataType::Int64, false),
        Field::new(
            format!("{prefix}timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(format!("{prefix}timestamp_type"), DataType::Int32, false),
        Field::new(format!("{prefix}key"), DataType::Binary, true),
        Field::new(
            format!("{prefix}headers"),
            DataType::List(Arc::new(header_element())),
            false,
        ),
        Field::new(format!("{prefix}value_schema_id"), DataType::Int32, false),
    ]
}

fn header_element() -> Field {
    Field::new("element", DataType::Struct(header_fields()), false)
}

fn header_fields() -> Fields {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Binary, true),
    ])
}

fn metadata_columns(topic: &str, chunk: &[KafkaMessage], schema_id: u32) -> Vec<ArrayRef> {
    let n = chunk.len();
    let headers = {
        let headers = || chunk.iter().flat_map(|m| m.headers.iter());
        let keys = StringArray::from(headers().map(|h| h.key.as_str()).collect::<Vec<_>>());
        let values = BinaryArray::from(headers().map(|h| h.value.as_deref()).collect::<Vec<_>>());
        let entries = StructArray::new(
            header_fields(),
            vec![Arc::new(keys) as ArrayRef, Arc::new(values)],
            None,
        );
        ListArray::new(
            Arc::new(header_element()),
            OffsetBuffer::from_lengths(chunk.iter().map(|m| m.headers.len())),
            Arc::new(entries),
            None,
        )
    };
    vec![
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(topic, n))),
        Arc::new(Int32Array::from_iter_values(
            chunk.iter().map(|m| m.partition),
        )),
        Arc::new(Int64Array::from_iter_values(chunk.iter().map(|m| m.offset))),
        Arc::new(
            TimestampMicrosecondArray::from_iter(chunk.iter().map(|m| m.timestamp.micros()))
                .with_timezone("UTC"),
        ),
        Arc::new(Int32Array::from_iter_values(
            chunk.iter().map(|m| m.timestamp.type_code()),
        )),
        Arc::new(BinaryArray::from_iter(
            chunk.iter().map(|m| m.key.as_deref()),
        )),
        Arc::new(headers),
        // As the Java client reads the header: a signed int.
        Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(
            schema_id as i32,
            n,
        ))),
    ]
}

/// Undecodable messages as rows for the data-quality table (§5.7): `<prefix>topic`,
/// `<prefix>partition`, `<prefix>offset`, `<prefix>timestamp`, `<prefix>key_b64`,
/// `<prefix>value_b64`, `<prefix>value_schema_id`, each with the reason `avro: <reason>`.
fn rejected_rows(
    topic: &str,
    prefix: &str,
    undecodable: &[(KafkaMessage, Undecodable)],
) -> Result<Option<Rejected>> {
    if undecodable.is_empty() {
        return Ok(None);
    }
    let b64 = |b: &Option<Vec<u8>>| {
        b.as_ref()
            .map(|b| base64::engine::general_purpose::STANDARD.encode(b))
    };
    let messages = || undecodable.iter().map(|(m, _)| m);
    let schema = Schema::new(vec![
        Field::new(format!("{prefix}topic"), DataType::Utf8, false),
        Field::new(format!("{prefix}partition"), DataType::Int32, false),
        Field::new(format!("{prefix}offset"), DataType::Int64, false),
        Field::new(
            format!("{prefix}timestamp"),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(format!("{prefix}key_b64"), DataType::Utf8, true),
        Field::new(format!("{prefix}value_b64"), DataType::Utf8, true),
        Field::new(format!("{prefix}value_schema_id"), DataType::Int32, true),
    ]);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            topic,
            undecodable.len(),
        ))),
        Arc::new(Int32Array::from_iter_values(
            messages().map(|m| m.partition),
        )),
        Arc::new(Int64Array::from_iter_values(messages().map(|m| m.offset))),
        Arc::new(
            TimestampMicrosecondArray::from_iter(messages().map(|m| m.timestamp.micros()))
                .with_timezone("UTC"),
        ),
        Arc::new(StringArray::from_iter(messages().map(|m| b64(&m.key)))),
        Arc::new(StringArray::from_iter(messages().map(|m| b64(&m.value)))),
        Arc::new(Int32Array::from_iter(
            undecodable
                .iter()
                .map(|(_, u)| u.schema_id.map(|id| id as i32)),
        )),
    ];
    let rows = RecordBatch::try_new(Arc::new(schema), columns)
        .map_err(|e| Error::Other(format!("internal: cannot assemble undecodable rows: {e}")))?;
    Ok(Some(Rejected {
        rows,
        reasons: undecodable
            .iter()
            .map(|(_, u)| format!("avro: {}", u.reason))
            .collect(),
        columns: vec![None; undecodable.len()],
    }))
}

// ------------------------------------------------------------------- the decoded schema

/// The Arrow schema of the rows decoded from `schema`'s messages, before the metadata columns:
/// the decoder's own schema, normalised as every flushed batch is. The union `U` of the SQL
/// path is built from these (docs/kafka-design.md §5.5).
///
/// Fails, with the reason, where the decoder would refuse the writer schema: not a record at
/// the top level, or nothing arrow-avro can build a decoder for.
pub fn decoded_schema(schema: &RegisteredSchema) -> std::result::Result<SchemaRef, String> {
    top_level_record(&schema.schema).map_err(|kind| {
        format!(
            "writer schema {} is not a record at the top level but {kind}",
            schema.id
        )
    })?;
    let raw = ArrowAvro.build(schema.id, &schema.schema)?.schema();
    // The normalisation of a batch with no rows is the normalisation of its schema: the one
    // code path for both, so the two cannot drift apart.
    let empty = RecordBatch::new_empty(raw);
    normalise(empty, &fixed_uuids(&schema.schema))
        .map(|b| b.schema())
        .map_err(|e| e.to_string())
}

// ------------------------------------------------------------------------- normalisation

/// The decoder's batch in the form the target reads (§5.4), recursively through structs,
/// lists, maps and unions:
/// - an enum, `Dictionary(Int32, Utf8)`, becomes `Utf8`;
/// - a uuid, `FixedSizeBinary(16)` (from a `string` with logicalType uuid, codec.rs:1677-1683,
///   or a `fixed(16)` with it), becomes its lowercase hyphenated text, as apache-avro gives it
///   (types.rs:353);
/// - lists name their items `element`, maps their entries `key_value` (with `key` and `value`),
///   as the Delta kernel's Arrow schemas do (buoyant_kernel engine/arrow_conversion/mod.rs:37-40)
///   where arrow-avro says `item` and `entries`;
/// - fields keep their names, types and nullability, and lose the Avro metadata arrow-avro
///   attaches (names, namespaces, docs, defaults, enum symbols).
///
/// Timestamps are untouched: the decoder already says `UTC` (`Tz::Utc`) where arrow-avro would
/// say `+00:00`, and keeps local timestamps naive and millisecond ones in milliseconds, which
/// the conform step reads (§5.6).
fn normalise(
    batch: RecordBatch,
    fixed_uuids: &HashSet<(String, Option<String>)>,
) -> std::result::Result<RecordBatch, ArrowError> {
    let schema = batch.schema();
    let mut fields = Vec::with_capacity(schema.fields().len());
    let mut columns = Vec::with_capacity(schema.fields().len());
    for (field, column) in schema.fields().iter().zip(batch.columns()) {
        let (f, c) = normalise_column(field, column.clone(), fixed_uuids)?;
        fields.push(f);
        columns.push(c);
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
}

fn normalise_column(
    field: &Field,
    array: ArrayRef,
    fixed_uuids: &HashSet<(String, Option<String>)>,
) -> std::result::Result<(FieldRef, ArrayRef), ArrowError> {
    let array = normalise_array(array, field.metadata(), fixed_uuids)?;
    let field = Field::new(field.name(), array.data_type().clone(), field.is_nullable());
    Ok((Arc::new(field), array))
}

fn normalise_array(
    array: ArrayRef,
    metadata: &HashMap<String, String>,
    fixed_uuids: &HashSet<(String, Option<String>)>,
) -> std::result::Result<ArrayRef, ArrowError> {
    match array.data_type() {
        DataType::Dictionary(_, values) if values.as_ref() == &DataType::Utf8 => {
            deltalake::arrow::compute::cast(&array, &DataType::Utf8)
        }
        DataType::FixedSizeBinary(16) if is_uuid(metadata, fixed_uuids) => {
            let bytes = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("FixedSizeBinary");
            Ok(Arc::new(StringArray::from_iter(
                bytes.iter().map(|b| b.map(hyphenated)),
            )))
        }
        DataType::Struct(fields) => {
            let s = array.as_struct();
            let mut new_fields = Vec::with_capacity(fields.len());
            let mut columns = Vec::with_capacity(fields.len());
            for (f, c) in fields.iter().zip(s.columns()) {
                let (f, c) = normalise_column(f, c.clone(), fixed_uuids)?;
                new_fields.push(f);
                columns.push(c);
            }
            Ok(Arc::new(StructArray::try_new(
                Fields::from(new_fields),
                columns,
                s.nulls().cloned(),
            )?))
        }
        DataType::List(item) => {
            let l = array.as_list::<i32>();
            let values = normalise_array(l.values().clone(), item.metadata(), fixed_uuids)?;
            let item = Field::new("element", values.data_type().clone(), item.is_nullable());
            Ok(Arc::new(ListArray::try_new(
                Arc::new(item),
                l.offsets().clone(),
                values,
                l.nulls().cloned(),
            )?))
        }
        DataType::Map(_, sorted) => {
            let m = array.as_map();
            let (key_field, value_field) = m.entries_fields();
            let keys = normalise_array(m.keys().clone(), key_field.metadata(), fixed_uuids)?;
            let values = normalise_array(m.values().clone(), value_field.metadata(), fixed_uuids)?;
            let entry_fields = Fields::from(vec![
                Field::new("key", keys.data_type().clone(), false),
                Field::new(
                    "value",
                    values.data_type().clone(),
                    value_field.is_nullable(),
                ),
            ]);
            let entries = StructArray::try_new(
                entry_fields.clone(),
                vec![keys, values],
                m.entries().nulls().cloned(),
            )?;
            Ok(Arc::new(MapArray::try_new(
                Arc::new(Field::new(
                    "key_value",
                    DataType::Struct(entry_fields),
                    false,
                )),
                m.offsets().clone(),
                entries,
                m.nulls().cloned(),
                *sorted,
            )?))
        }
        DataType::Union(fields, _) => {
            let u = array.as_any().downcast_ref::<UnionArray>().expect("Union");
            let mut type_ids = Vec::with_capacity(fields.len());
            let mut new_fields = Vec::with_capacity(fields.len());
            let mut children = Vec::with_capacity(fields.len());
            for (type_id, f) in fields.iter() {
                let (f, c) = normalise_column(f, u.child(type_id).clone(), fixed_uuids)?;
                type_ids.push(type_id);
                new_fields.push(f);
                children.push(c);
            }
            Ok(Arc::new(UnionArray::try_new(
                UnionFields::try_new(type_ids, new_fields)?,
                u.type_ids().clone(),
                u.offsets().cloned(),
                children,
            )?))
        }
        _ => Ok(array),
    }
}

fn is_uuid(
    metadata: &HashMap<String, String>,
    fixed_uuids: &HashSet<(String, Option<String>)>,
) -> bool {
    if metadata.get("logicalType").is_some_and(|t| t == "uuid") {
        return true;
    }
    metadata.get(AVRO_NAME_METADATA_KEY).is_some_and(|name| {
        fixed_uuids.contains(&(
            name.clone(),
            metadata.get(AVRO_NAMESPACE_METADATA_KEY).cloned(),
        ))
    })
}

/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, lowercase.
fn hyphenated(b: &[u8]) -> String {
    let hex: String = b.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

// --------------------------------------------------------------- reading the writer schema

/// Whether the writer schema is a record at its top level; otherwise what it is.
fn top_level_record(schema: &str) -> std::result::Result<(), String> {
    let value: serde_json::Value =
        serde_json::from_str(schema).map_err(|e| format!("not JSON ({e})"))?;
    match &value {
        serde_json::Value::Object(o) => match o.get("type") {
            Some(serde_json::Value::String(t)) if t == "record" || t == "error" => Ok(()),
            Some(serde_json::Value::String(t)) => Err(format!("of type {t}")),
            _ => Err("of a nested type".to_string()),
        },
        serde_json::Value::Array(_) => Err("a union".to_string()),
        serde_json::Value::String(t) => Err(format!("of type {t}")),
        _ => Err("not a schema".to_string()),
    }
}

/// `(name, namespace)` of every `fixed` type of size 16 with logicalType uuid, the namespace
/// resolved as arrow-avro resolves it for the `avro.name` and `avro.namespace` metadata it puts
/// on the field (codec.rs:1530-1540): the type's own, else the enclosing named type's.
///
/// arrow-avro reads such a field as the 16 bytes it is (it knows the uuid logical type on
/// `string` only), so the field is told by that metadata.
fn fixed_uuids(schema: &str) -> HashSet<(String, Option<String>)> {
    fn walk(v: &serde_json::Value, ns: Option<&str>, out: &mut HashSet<(String, Option<String>)>) {
        use serde_json::Value;
        match v {
            Value::Array(branches) => branches.iter().for_each(|b| walk(b, ns, out)),
            Value::Object(o) => {
                let own_ns = o.get("namespace").and_then(Value::as_str).or(ns);
                match o.get("type") {
                    Some(Value::String(t)) if t == "record" || t == "error" => {
                        for f in o
                            .get("fields")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                        {
                            if let Some(t) = f.get("type") {
                                walk(t, own_ns, out);
                            }
                        }
                    }
                    Some(Value::String(t)) if t == "array" => {
                        if let Some(items) = o.get("items") {
                            walk(items, ns, out);
                        }
                    }
                    Some(Value::String(t)) if t == "map" => {
                        if let Some(values) = o.get("values") {
                            walk(values, ns, out);
                        }
                    }
                    Some(Value::String(t)) if t == "fixed" => {
                        let uuid = o.get("logicalType").and_then(Value::as_str) == Some("uuid")
                            && o.get("size").and_then(Value::as_u64) == Some(16);
                        if let (true, Some(name)) = (uuid, o.get("name").and_then(Value::as_str)) {
                            out.insert((name.to_string(), own_ns.map(str::to_string)));
                        }
                    }
                    Some(t @ (Value::Object(_) | Value::Array(_))) => walk(t, ns, out),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    let mut out = HashSet::new();
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(schema) {
        walk(&v, None, &mut out);
    }
    out
}

/// Whether a value of this type takes no bytes on the wire: null, `fixed(0)`, and records of
/// nothing else (a record without fields among them).
fn zero_width(t: &DataType) -> bool {
    match t {
        DataType::Null | DataType::FixedSizeBinary(0) => true,
        DataType::Struct(fs) => fs.iter().all(|f| zero_width(f.data_type())),
        _ => false,
    }
}

/// The first array whose items take no bytes on the wire, by its path.
fn zero_width_items(fields: &Fields) -> Option<String> {
    fn find(t: &DataType, path: String) -> Option<String> {
        match t {
            DataType::List(item) if zero_width(item.data_type()) => Some(path),
            DataType::List(item) => find(item.data_type(), format!("{path}[]")),
            DataType::Struct(fs) => fs
                .iter()
                .find_map(|f| find(f.data_type(), format!("{path}.{}", f.name()))),
            DataType::Map(entries, _) => find(entries.data_type(), path),
            DataType::Union(fs, _) => fs
                .iter()
                .find_map(|(_, f)| find(f.data_type(), path.clone())),
            _ => None,
        }
    }
    fields
        .iter()
        .find_map(|f| find(f.data_type(), f.name().clone()))
}

#[cfg(test)]
mod tests;
