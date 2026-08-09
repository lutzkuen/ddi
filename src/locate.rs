//! Keeping track of where a table actually is, while it is allowed to move.
//!
//! A catalog entry points at a path, and that pointer can change without the name
//! changing. Resolving it once at startup means a relocation goes unnoticed until
//! something restarts — and the symptom is silence, not an error, because the old table
//! is usually still sitting there with nothing new in it.
//!
//! So the location is re-resolved on every poll. It is one small query next to a read of
//! the source's head version, which the loop already does.
//!
//! Moving is safe to act on because of work already in place: a different location means
//! a different table, its Delta id will not match the one recorded in the target's own
//! commit history, and the pipeline restarts from the beginning with the `dedup_timestamp`
//! filter suppressing everything the target already holds. The move case reduces to the
//! drop-and-recreate case.

use std::collections::HashMap;
use std::sync::Mutex;

use tracing::{info, warn};

use crate::config::ResolvedPipeline;
use crate::trino::TrinoClient;

/// Resolves table relations to storage locations, and remembers the last answer.
#[derive(Debug, Default)]
pub struct Locator {
    client: Option<TrinoClient>,
    /// relation -> last location the catalog gave us.
    last_known: Mutex<HashMap<String, String>>,
}

impl Locator {
    /// A locator with no catalog: every relation keeps the location dbt declared.
    pub fn none() -> Self {
        Self::default()
    }

    pub fn with_client(client: TrinoClient) -> Self {
        Self {
            client: Some(client),
            last_known: Mutex::new(HashMap::new()),
        }
    }

    pub fn is_active(&self) -> bool {
        self.client.is_some()
    }

    /// Where `relation` lives now.
    ///
    /// `declared` is what dbt said, used when there is no catalog to ask. When the
    /// catalog cannot be reached the last answer it gave is used instead, because a
    /// cluster being briefly unavailable is not a reason to stop streaming — and the
    /// location it last reported is far likelier to be right than to have changed during
    /// the outage. It says so loudly either way.
    pub async fn locate(&self, relation: Option<&str>, declared: &str) -> String {
        let (Some(client), Some(relation)) = (&self.client, relation) else {
            return declared.to_string();
        };

        match client.table_location(relation).await {
            Ok(found) => {
                let mut cache = self.last_known.lock().unwrap();
                match cache.insert(relation.to_string(), found.clone()) {
                    Some(prev) if prev != found => {
                        info!(relation, from = %prev, to = %found, "table has moved");
                    }
                    None if found != declared => {
                        info!(
                            relation,
                            declared, resolved = %found,
                            "catalog location differs from the one dbt declared; using the catalog"
                        );
                    }
                    _ => {}
                }
                found
            }
            Err(e) => {
                let cached = self.last_known.lock().unwrap().get(relation).cloned();
                match cached {
                    Some(prev) => {
                        warn!(
                            relation,
                            using = %prev,
                            error = %e,
                            "catalog unreachable; continuing with the last location it gave"
                        );
                        prev
                    }
                    None => {
                        warn!(
                            relation,
                            using = declared,
                            error = %e,
                            "catalog unreachable and never answered for this table; falling \
                             back to the location dbt declared"
                        );
                        declared.to_string()
                    }
                }
            }
        }
    }

    /// A copy of `cfg` with its locations refreshed from the catalog.
    pub async fn refresh(&self, cfg: &ResolvedPipeline) -> ResolvedPipeline {
        if !self.is_active() {
            return cfg.clone();
        }
        let mut out = cfg.clone();
        out.source_uri = self
            .locate(cfg.source_relation.as_deref(), &cfg.source_uri)
            .await;
        out.target_uri = self
            .locate(cfg.target_relation.as_deref(), &cfg.target_uri)
            .await;
        out
    }
}

/// True when a refresh moved either end of the pipeline.
pub fn moved(before: &ResolvedPipeline, after: &ResolvedPipeline) -> bool {
    before.source_uri != after.source_uri || before.target_uri != after.target_uri
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ResolvedPipeline {
        ResolvedPipeline {
            name: "orders_stg".into(),
            app_id: "ddi.orders_stg".into(),
            source_uri: "abfss://lake@a.dfs.core.windows.net/bronze/orders".into(),
            target_uri: "abfss://lake@a.dfs.core.windows.net/silver/orders".into(),
            source_relation: Some("hive.bronze.orders".into()),
            target_relation: Some("hive.silver.orders".into()),
            starting_version: 0,
            change_policy: Default::default(),
            transform_sql: None,
            allowed_latency_secs: 1,
            max_bytes_per_batch: 1,
            max_files_per_batch: 1,
            max_output_rows_per_batch: 1,
            target_file_size: 1,
            watermark_uri: None,
            dedup_timestamp: None,
            dedup_key: None,
            storage: Default::default(),
        }
    }

    #[tokio::test]
    async fn with_no_catalog_the_declared_location_stands() {
        let l = Locator::none();
        assert!(!l.is_active());
        let c = cfg();
        let after = l.refresh(&c).await;
        assert_eq!(after.source_uri, c.source_uri);
        assert!(!moved(&c, &after));
    }

    #[tokio::test]
    async fn a_relation_with_no_name_keeps_what_dbt_declared() {
        // Hand-written pipelines have no catalog relation to ask about.
        let l = Locator::none();
        assert_eq!(
            l.locate(None, "s3://declared/path").await,
            "s3://declared/path"
        );
    }

    #[test]
    fn moved_compares_both_ends() {
        let a = cfg();
        let mut b = a.clone();
        assert!(!moved(&a, &b));
        b.target_uri = "s3://elsewhere/silver/orders".into();
        assert!(
            moved(&a, &b),
            "a target move must count too — the offset lives there"
        );
    }
}
