//! `ddi kafka plan`: the read-only preflight before a takeover, a shadow run or a
//! `set-offsets` (docs/kafka-design.md §4.8).
//!
//! It reads the target and the topic with a client that joins no group, and prints what a
//! pipeline's open would make of them: who wrote the offsets, what the takeover check would
//! say, each partition's resume position, and a `starting_offsets` to paste. It writes
//! nothing, and exits non-zero on a finding that would stop the pipeline.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use futures::StreamExt as _;

use crate::config::ResolvedPipeline;
use crate::error::Result;
use crate::fence::{self, Era};

use super::tools::{self, PartitionStatus};

/// The table properties an operator wants to see, as Delta spells them.
const PROPERTIES: [&str; 6] = [
    "delta.checkpointInterval",
    "delta.logRetentionDuration",
    "delta.deletedFileRetentionDuration",
    "delta.setTransactionRetentionDuration",
    "delta.enableExpiredLogCleanup",
    "delta.appendOnly",
];

/// How many of the newest commits are read for the app_ids that have written lately.
const RECENT_COMMITS: u64 = 100;

/// What the target is, as far as writing Kafka offsets into it cares.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableFacts {
    pub uri: String,
    pub version: i64,
    pub reader_version: i32,
    pub writer_version: i32,
    pub partition_columns: Vec<String>,
    /// The properties of [`PROPERTIES`] the table sets, and `delta.columnMapping.mode`.
    pub properties: BTreeMap<String, String>,
    pub files: usize,
    /// `(name, type)` of every column, in the case the table has them.
    pub columns: Vec<(String, String)>,
    /// The newest commit's engine and operation, as its `commitInfo` says.
    pub last_commit: Option<String>,
}

/// One partition of a plan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlanPartition {
    pub status: PartitionStatus,
    /// What the takeover check asks of the operator for this partition (§4.4 step 3): an
    /// explicit `starting_offsets` entry. The partition has no offset, its topic holds
    /// messages, and no configured entry acknowledges it.
    pub ack_needed: bool,
    /// The configured acknowledgment, if there is one.
    pub configured: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    pub name: String,
    pub app_id: String,
    pub topic: String,
    pub table: TableFacts,
    pub era: Era,
    /// The takeover check's verdict: `None` when it passes or does not apply.
    pub takeover: Option<String>,
    pub partitions: Vec<PlanPartition>,
    /// Every app_id with a `txn` in the newest commits, which is how a mistyped one shows.
    pub other_app_ids: BTreeSet<String>,
    /// Advice and warnings, one line each.
    pub notes: Vec<String>,
    /// Findings that would stop the pipeline at open or block a partition.
    pub blocking: Vec<String>,
}

/// Read everything a plan prints.
pub async fn plan(cfg: &ResolvedPipeline) -> Result<Plan> {
    let k = cfg.kafka.as_ref().ok_or_else(|| {
        crate::error::Error::Other("internal: plan of a pipeline that is not Kafka".into())
    })?;
    let view = tools::view(cfg).await?;
    let configured: BTreeMap<i32, i64> =
        k.starting_offsets.partitions().cloned().unwrap_or_default();
    let table = facts(&view.target, &cfg.target_uri).await?;

    let by_partition: BTreeMap<i32, (Option<i64>, i64)> = view
        .partitions
        .iter()
        .map(|p| (p.partition, (p.stored, p.high)))
        .collect();
    // Whether the target may hold this topic's rows: the same question `open` asks, the
    // same way.
    let (data_files, holds_rows) =
        super::start::target_rows(&view.target, &k.metadata_prefix, &k.topic)?;
    let takeover = fence::takeover_check(
        &cfg.app_id,
        view.era,
        holds_rows,
        &by_partition,
        &BTreeSet::new(),
    )
    .err()
    .map(|e| e.to_string());
    // What the open decides from these facts (§4.4 steps 3, 6, 7, 8): the typo guard, the
    // takeover check, the `kdi_compat` refusal and the fresh baseline, by the open's own code.
    let app_ids = if view.era == Era::Fresh && data_files > 0 && !k.starting_offsets_explicit {
        fence::recent_txn_app_ids(&view.target, super::start::TYPO_GUARD_COMMITS).await?
    } else {
        BTreeSet::new()
    };
    let stored: BTreeMap<i32, Option<i64>> = view
        .partitions
        .iter()
        .map(|p| (p.partition, p.stored))
        .collect();
    let watermarks: BTreeMap<i32, (i64, i64)> = view
        .partitions
        .iter()
        .map(|p| (p.partition, (p.low, p.high)))
        .collect();
    let open = super::start::plan_open(&super::start::OpenFacts {
        app_id: &cfg.app_id,
        topic: &k.topic,
        target_uri: &cfg.target_uri,
        stored: &stored,
        watermarks: &watermarks,
        era: view.era,
        data_files,
        holds_rows,
        policy: &k.starting_offsets,
        explicit: k.starting_offsets_explicit,
        kdi_compat: k.kdi_compat,
        neighbours: &k.neighbours,
        app_ids: &app_ids,
    });

    let mut blocking = Vec::new();
    let mut notes = Vec::new();
    if let Err(e) = &open {
        blocking.push(format!("the open refuses: {e}"));
    }
    if let Err(e) = fence::refuse_txn_retention(&view.target) {
        blocking.push(e.to_string());
    }
    if let Err(e) = fence::refuse_unwritable_protocol(&view.target) {
        blocking.push(e.to_string());
    }

    let partitions: Vec<PlanPartition> = view
        .partitions
        .iter()
        .map(|p| {
            let ack_needed = takeover.is_some()
                && p.stored.is_none()
                && p.high > 0
                && matches!(view.era, Era::Kdi | Era::Fresh);
            let configured = configured.get(&p.partition).copied();
            if matches!(p.verdict(), "RETENTION_GAP" | "REGRESSED") {
                blocking.push(format!(
                    "partition {} is {}: stored offset {:?}, topic holds {}..{}",
                    p.partition,
                    p.verdict(),
                    p.stored,
                    p.low,
                    p.high
                ));
            } else if p.verdict() == "NO_TXN->blocked"
                && configured.is_none()
                && view.era == Era::Ddi
            {
                // In a fresh era the open's baseline starts every partition, and in
                // kafka-delta-ingest's the takeover check above speaks for it.
                blocking.push(format!(
                    "partition {} has no offset and the topic starts at {}: messages ddi never \
                     read were trimmed",
                    p.partition, p.low
                ));
            }
            PlanPartition {
                status: p.clone(),
                ack_needed,
                configured,
            }
        })
        .collect();
    if let Some(extra) = configured
        .keys()
        .find(|p| !view.partitions.iter().any(|x| x.partition == **p))
    {
        notes.push(format!(
            "starting_offsets names partition {extra}, which the topic does not have"
        ));
    }

    let other_app_ids = other_app_ids(&view.target, &cfg.app_id).await;
    if view.era == Era::Fresh && holds_rows && !k.starting_offsets_explicit {
        let seen: Vec<&String> = other_app_ids.iter().collect();
        notes.push(format!(
            "a fresh app_id on a target that already holds rows; app_ids seen in its newest \
             commits: {seen:?}. Is {:?} the one the existing writer uses?",
            cfg.app_id
        ));
    }

    if matches!(view.era, Era::Kdi | Era::Fresh) && k.kdi_compat.is_none() && holds_rows {
        let strings: Vec<&str> = table
            .columns
            .iter()
            .filter(|(_, t)| t.contains("string") || t.contains("String"))
            .map(|(n, _)| n.as_str())
            .collect();
        notes.push(format!(
            "kdi_compat is unset; a takeover refuses to open until it is true or false. Set it \
             true to keep kafka-delta-ingest's STRING rendering. STRING columns: {strings:?}. Count \
             the text 'null' per column with:  SELECT {} FROM <table>",
            strings
                .iter()
                .map(|c| format!("count_if(\"{c}\" = 'null') AS \"{c}\""))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    // A SELECT list replaces the source's columns, the value schema id included.
    if let (Some(true), Some(sql)) = (k.kdi_compat, cfg.transform_sql.as_deref()) {
        let compact = sql
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_ascii_lowercase();
        if !compact.contains("value_schema_id") && compact != "select * from source" {
            notes.push(format!(
                "transform_sql does not select {}value_schema_id: with kdi_compat the text written into \
                 STRING columns then cannot tell which writer schema a row had, and gives every row a key \
                 for every field any schema version has (ddi_kafka_null_text_approximate). Add it to the \
                 SELECT list; the target has no such column, so it is not written",
                k.metadata_prefix
            ));
        }
    }
    let prefix = &k.metadata_prefix;
    let missing: Vec<String> = ["topic", "partition", "offset", "timestamp"]
        .iter()
        .map(|c| format!("{prefix}{c}"))
        .filter(|c| !table.columns.iter().any(|(n, _)| n.eq_ignore_ascii_case(c)))
        .collect();
    if !missing.is_empty() {
        notes.push(format!(
            "the target has no column {missing:?}: the offset columns the shadow comparison and \
             the partition watermark use (§10.1)"
        ));
    }
    if let Some(v) = table
        .properties
        .get("delta.setTransactionRetentionDuration")
    {
        blocking.push(format!(
            "delta.setTransactionRetentionDuration = {v}: the pipeline refuses this target"
        ));
    }

    Ok(Plan {
        name: cfg.name.clone(),
        app_id: cfg.app_id.clone(),
        topic: k.topic.clone(),
        table,
        era: view.era,
        takeover,
        partitions,
        other_app_ids,
        notes,
        blocking,
    })
}

async fn facts(target: &deltalake::DeltaTable, uri: &str) -> Result<TableFacts> {
    use deltalake::delta_datafusion::DataFusionMixins as _;
    let snapshot = target.snapshot().map_err(crate::error::Error::Delta)?;
    let metadata = snapshot.metadata();
    let mut properties = BTreeMap::new();
    for (k, v) in metadata.configuration() {
        if PROPERTIES.iter().any(|p| p.eq_ignore_ascii_case(k))
            || k.eq_ignore_ascii_case("delta.columnMapping.mode")
        {
            properties.insert(k.clone(), v.clone());
        }
    }
    let columns = snapshot
        .snapshot()
        .read_schema()
        .fields()
        .iter()
        .map(|f| (f.name().to_string(), f.data_type().to_string()))
        .collect();
    let last_commit = match target.history(Some(1)).await {
        Ok(h) => {
            let mut stream = futures::stream::iter(h);
            stream.next().await.map(|c| {
                format!(
                    "{} {}",
                    c.engine_info.as_deref().unwrap_or("unknown engine"),
                    c.operation.as_deref().unwrap_or("unknown operation")
                )
            })
        }
        Err(_) => None,
    };
    Ok(TableFacts {
        uri: uri.to_string(),
        version: target.version().map_or(-1, |v| v as i64),
        reader_version: snapshot.protocol().min_reader_version(),
        writer_version: snapshot.protocol().min_writer_version(),
        partition_columns: metadata.partition_columns().to_vec(),
        properties,
        files: snapshot.log_data().num_files(),
        columns,
        last_commit,
    })
}

/// The app_ids with a `txn` in the newest commits, apart from this pipeline's own.
async fn other_app_ids(target: &deltalake::DeltaTable, app_id: &str) -> BTreeSet<String> {
    let own_prefix = format!("{app_id}-");
    let own_marker = fence::takeover_marker_app_id(app_id);
    fence::recent_txn_app_ids(target, RECENT_COMMITS)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|id| id != &own_marker && !id.starts_with(&own_prefix))
        .collect()
}

impl Plan {
    /// Whether anything stops the pipeline: the exit status of `ddi kafka plan`.
    pub fn is_blocked(&self) -> bool {
        !self.blocking.is_empty()
    }

    /// The `starting_offsets` a shadow run takes: each partition's next offset (stored + 1),
    /// for every partition with an offset.
    pub fn starting_offsets(&self) -> BTreeMap<i32, i64> {
        self.partitions
            .iter()
            .filter_map(|p| p.status.stored.map(|s| (p.status.partition, s + 1)))
            .collect()
    }

    pub fn render(&self) -> String {
        let mut o = String::new();
        let t = &self.table;
        let _ = writeln!(
            o,
            "pipeline {} ({}, app_id {:?})",
            self.name, self.topic, self.app_id
        );
        let _ = writeln!(o, "\ntable {}", t.uri);
        let _ = writeln!(
            o,
            "  version {}, {} data files, protocol reader {} writer {}",
            t.version, t.files, t.reader_version, t.writer_version
        );
        let _ = writeln!(o, "  partitioned by {:?}", t.partition_columns);
        for key in PROPERTIES.iter().chain(&["delta.columnMapping.mode"]) {
            let value = t
                .properties
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v.as_str());
            let _ = writeln!(o, "  {key} = {}", value.unwrap_or("(unset)"));
        }
        let _ = writeln!(
            o,
            "  last commit: {}",
            t.last_commit.as_deref().unwrap_or("unknown")
        );
        let _ = writeln!(o, "  columns: {}", t.columns.len());
        for (n, ty) in &t.columns {
            let _ = writeln!(o, "    {n}: {ty}");
        }
        let era = match self.era {
            Era::Fresh => "fresh: no partition has an offset and there is no takeover marker",
            Era::Kdi => {
                "kafka-delta-ingest's: some partition has an offset and there is no takeover marker"
            }
            Era::Ddi => "ddi's: the takeover marker is there",
        };
        let _ = writeln!(o, "\nera of the offsets: {era}");
        match &self.takeover {
            Some(e) => {
                let _ = writeln!(o, "takeover check without any acknowledgment: {e}");
            }
            None => {
                let _ = writeln!(o, "takeover check: passes, or does not apply");
            }
        }
        let _ = writeln!(o, "\npartitions");
        for p in &self.partitions {
            let s = &p.status;
            let verdict = if p.ack_needed {
                match p.configured {
                    Some(c) => format!("NO_TXN->acknowledged at {c}"),
                    None => "NO_TXN->ack needed".to_string(),
                }
            } else {
                s.verdict().to_string()
            };
            let _ = writeln!(
                o,
                "  {:>4}  stored={:<12} resume={:<12} low={:<12} high={:<12} lag={:<10} {verdict}",
                s.partition,
                s.stored.map_or("-".to_string(), |v| v.to_string()),
                s.stored.map_or("-".to_string(), |v| (v + 1).to_string()),
                s.low,
                s.high,
                s.lag(),
            );
        }
        let entries: Vec<String> = self
            .starting_offsets()
            .iter()
            .map(|(p, o)| format!("\"{p}\" = {o}"))
            .collect();
        let _ = writeln!(
            o,
            "\nstarting_offsets for a shadow run (stored + 1):\n  starting_offsets = {{ {} }}",
            entries.join(", ")
        );
        if !self.other_app_ids.is_empty() {
            let _ = writeln!(
                o,
                "\nother app_ids with a txn in the newest {RECENT_COMMITS} commits: {:?}",
                self.other_app_ids
            );
        }
        for n in &self.notes {
            let _ = writeln!(o, "\nnote: {n}");
        }
        if self.blocking.is_empty() {
            let _ = writeln!(o, "\nnothing blocks this pipeline");
        } else {
            let _ = writeln!(o, "\nBLOCKING");
            for b in &self.blocking {
                let _ = writeln!(o, "  - {b}");
            }
        }
        o
    }
}
