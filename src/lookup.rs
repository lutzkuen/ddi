//! Pinned Delta lookup snapshots for row-local enrichments.
//!
//! A lookup is deliberately not a second stream. A source batch is still the only thing
//! that advances an offset; the lookup is a small, immutable-for-the-batch Delta snapshot
//! registered beside it in DataFusion. Selecting that snapshot from the source commit's
//! timestamp makes a retry pick the same version even when the lookup has advanced in the
//! meantime.

use chrono::{DateTime, Utc};
use deltalake::logstore::commit_uri_from_version;
use deltalake::logstore::object_store::ObjectStoreExt;
use deltalake::DeltaTable;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};
use crate::source::Version;
use crate::storage::Storage;

/// How a lookup responds when the Delta table at its configured URI is replaced.
///
/// The default keeps lookup provenance reproducible: a source batch is enriched from the
/// snapshot that existed before that source commit, and a changed table id is an error. The
/// opt-in mode deliberately trades provenance across a replacement for availability: normal
/// same-id updates remain timestamp-pinned, but a snapshot from a different table lineage is
/// replaced with the current table head.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupTableIdChangePolicy {
    /// Reject a different Delta table id. This is the safe default.
    #[default]
    Strict,
    /// Use the lookup's current/head snapshot after a table replacement.
    UseCurrent,
}

/// A lookup as it appears in a hand-written or dbt-derived pipeline config.
///
/// `name` is both the safe SQL relation a model joins and the name registered in the
/// DataFusion session. `relation` is optional only for hand-written configs; dbt-derived
/// pipelines carry it so the deployment wrapper can ask Starburst to describe the model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LookupConfig {
    pub name: String,
    pub uri: String,
    #[serde(default)]
    pub relation: Option<String>,
    /// Explicit baseline for source commits that predate this lookup's first retained Delta
    /// log entry. This is opt-in because it intentionally applies a later, complete historical
    /// lookup snapshot to an initial source backfill.
    #[serde(default)]
    pub pre_history_version: Option<Version>,
    /// What to do when a table at this URI has a different Delta table id.
    ///
    /// `strict` is the default. `use_current` is an explicit availability-over-replay
    /// choice for a replacement; ordinary updates remain timestamp-pinned.
    #[serde(default)]
    pub table_id_change_policy: LookupTableIdChangePolicy,
}

/// A lookup with its location resolved and its name validated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedLookup {
    pub name: String,
    pub uri: String,
    pub relation: Option<String>,
    pub pre_history_version: Option<Version>,
    pub table_id_change_policy: LookupTableIdChangePolicy,
}

impl ResolvedLookup {
    /// Load the newest version strictly before the source Delta-log object's millisecond,
    /// together with the lookup's current head.
    ///
    /// Delta time travel is millisecond-granular. Excluding the whole source millisecond means a
    /// lookup commit that appears later in that same millisecond cannot change a failed batch's
    /// retry. We then advance through every earlier equal-timestamp lookup version, making the
    /// rule an explicit upper bound rather than accepting the binary search's arbitrary equality
    /// match. That is the deterministic mapping that makes a failed batch reproducible.
    pub async fn snapshots(
        &self,
        storage: &Storage,
        as_of: DateTime<Utc>,
    ) -> Result<LookupSnapshots> {
        let head_table = storage
            .open(&self.uri)
            .await
            .map_err(|e| Error::Config(format!("lookup {:?} at {:?}: {e}", self.name, self.uri)))?;
        let head = head_table.version().ok_or_else(|| {
            Error::Config(format!(
                "lookup {:?} at {:?} has no Delta version",
                self.name, self.uri
            ))
        })?;
        let source_millis = as_of.timestamp_millis();
        let cutoff = DateTime::from_timestamp_millis(source_millis.saturating_sub(1)).ok_or_else(|| {
            Error::Config(format!(
                "source timestamp {as_of} cannot be represented at millisecond precision for lookup {:?}",
                self.name
            ))
        })?;
        let selected = storage
            .open_at_datetime(&self.uri, cutoff)
            .await
            .map_err(|e| Error::Config(format!("lookup {:?} at {:?}: {e}", self.name, self.uri)))?;
        let mut version = selected.version().ok_or_else(|| {
            Error::Config(format!(
                "lookup {:?} at {:?} has no Delta version after loading its snapshot",
                self.name, self.uri
            ))
        })?;

        let selected_millis = lookup_log_timestamp(&head_table, version)
            .await?
            .timestamp_millis();
        let used_pre_history = if selected_millis >= source_millis {
            let baseline = self.pre_history_version.ok_or_else(|| {
                Error::Config(format!(
                    "lookup {:?} has no retained snapshot strictly before source commit time {as_of}. \
                     Materialize the lookup before the source history, start the pipeline after it, \
                     or configure an explicit pre_history_version whose historical contents are \
                     approved for this backfill.",
                    self.name
                ))
            })?;
            if baseline > head {
                return Err(Error::Config(format!(
                    "lookup {:?} configures pre_history_version {baseline}, but its current head is {head}",
                    self.name
                )));
            }
            version = baseline;
            true
        } else {
            // `load_with_datetime` stops as soon as its binary search sees equality. Delta log
            // objects are timestamped to milliseconds, so several earlier versions can share a
            // value. Select the greatest one still strictly before the source millisecond.
            while version < head {
                let next = version + 1;
                if lookup_log_timestamp(&head_table, next)
                    .await?
                    .timestamp_millis()
                    >= source_millis
                {
                    break;
                }
                version = next;
            }
            false
        };
        let table = storage
            .open_at_version(&self.uri, version)
            .await
            .map_err(|e| Error::Config(format!("lookup {:?} at {:?}: {e}", self.name, self.uri)))?;
        Ok(LookupSnapshots {
            selected: LookupSnapshot {
                name: self.name.clone(),
                version,
                table_id: table_id(&table),
                used_pre_history,
                used_current: false,
                table,
            },
            head: LookupSnapshot {
                name: self.name.clone(),
                version: head,
                table_id: table_id(&head_table),
                used_pre_history: false,
                used_current: false,
                table: head_table,
            },
        })
    }
}

/// Read a lookup commit's timestamp from the same object-store metadata Delta's own time travel
/// uses. A writer-provided `commitInfo.timestamp` is intentionally not part of lookup selection.
async fn lookup_log_timestamp(table: &DeltaTable, version: Version) -> Result<DateTime<Utc>> {
    table
        .log_store()
        .object_store(None)
        .head(&commit_uri_from_version(Some(version)))
        .await
        .map(|object| object.last_modified)
        .map_err(|e| {
            Error::Other(format!(
                "cannot read Delta-log timestamp for lookup commit {version}: {e}"
            ))
        })
}

/// One concrete Delta snapshot used to enrich one source batch.
///
/// It is intentionally owned rather than cached globally. The FX table is small, and opening a
/// new provider per source commit keeps the snapshot boundary explicit and prevents a later
/// lookup refresh leaking into an in-flight retry.
pub struct LookupSnapshot {
    pub name: String,
    pub version: Version,
    pub table_id: Option<String>,
    /// True only when an explicitly configured baseline serves source history that predates the
    /// lookup table. It is recorded in target commit metadata for auditability.
    pub used_pre_history: bool,
    /// True when the configured table-id policy deliberately used the lookup head rather
    /// than selecting a source-timestamp-pinned snapshot. It is recorded in target commit
    /// metadata so that the loss of replay determinism is visible after the fact.
    pub used_current: bool,
    pub table: DeltaTable,
}

/// The source-timestamp-pinned lookup snapshot and the current head opened from one lookup
/// URI. Keeping both makes a lineage change detectable without opening a potentially different
/// head a second time between comparison and use.
pub struct LookupSnapshots {
    pub selected: LookupSnapshot,
    pub head: LookupSnapshot,
}

/// Refuse names that would be ambiguous in a model or collide with the streaming input.
pub fn resolve(config: &LookupConfig) -> Result<ResolvedLookup> {
    let name = config.name.trim();
    if !valid_name(name) {
        return Err(Error::Config(format!(
            "lookup name {:?} must be a lowercase SQL identifier and must not be \"source\"",
            config.name
        )));
    }
    if config.uri.trim().is_empty() {
        return Err(Error::Config(format!("lookup {name:?} has an empty uri")));
    }
    Ok(ResolvedLookup {
        name: name.to_string(),
        uri: config.uri.clone(),
        relation: config.relation.clone(),
        pre_history_version: config.pre_history_version,
        table_id_change_policy: config.table_id_change_policy,
    })
}

/// Whether a dbt `meta.ddi_lookup` value can safely be used as a SQL relation alias.
pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name != "source"
        && name == name.to_ascii_lowercase()
        && name.chars().enumerate().all(|(i, c)| {
            (i == 0 && (c.is_ascii_alphabetic() || c == '_'))
                || (i > 0 && (c.is_ascii_alphanumeric() || c == '_'))
        })
}

/// A Delta table's protocol identity, used to make lookup provenance auditable.
pub fn table_id(table: &DeltaTable) -> Option<String> {
    let snapshot = table.snapshot().ok()?;
    serde_json::to_value(snapshot.metadata())
        .ok()?
        .get("id")?
        .as_str()
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_names_are_deliberately_narrow() {
        assert!(resolve(&LookupConfig {
            name: "fx_rates".into(),
            uri: "/tmp/fx".into(),
            relation: None,
            pre_history_version: None,
            table_id_change_policy: Default::default(),
        })
        .is_ok());
        for name in ["", "source", "FX", "fx-rates", "1fx"] {
            assert!(
                resolve(&LookupConfig {
                    name: name.into(),
                    uri: "/tmp/fx".into(),
                    relation: None,
                    pre_history_version: None,
                    table_id_change_policy: Default::default(),
                })
                .is_err(),
                "{name:?} must be rejected"
            );
        }
    }
}
