//! `ddi kafka clone-table`: an empty copy of a bronze table for a shadow run (§4.8).
//!
//! A Trino `CREATE TABLE AS` lowercases column names and may turn column mapping on; this
//! writes a version-0 commit with the source's own column names and case, partition columns
//! and properties, and neither a column mapping nor the source's table id.

use deltalake::kernel::StructField;
use deltalake::protocol::SaveMode;
use deltalake::{ensure_table_uri, DeltaTable};

use crate::config::ResolvedPipeline;
use crate::error::{Error, Result};

/// Properties that belong to the source table and are not copied: its column mapping state.
/// The clone has none; a mapped source is refused before this (`refuse_unwritable_protocol`).
const NOT_COPIED: [&str; 2] = [
    "delta.columnMapping.maxColumnId",
    "delta.columnMapping.mode",
];

/// Create `to` as an empty table shaped like `cfg`'s target; its version.
pub async fn clone_table(cfg: &ResolvedPipeline, to: &str) -> Result<i64> {
    let source = cfg.storage.open(&cfg.target_uri).await?;
    crate::fence::refuse_unwritable_protocol(&source)?;
    let snapshot = source.snapshot().map_err(Error::Delta)?;
    let columns: Vec<StructField> = snapshot.schema().fields().cloned().collect();
    let partitions: Vec<String> = snapshot.metadata().partition_columns().to_vec();
    let configuration: Vec<(String, String)> = snapshot
        .metadata()
        .configuration()
        .iter()
        .filter(|(k, _)| !NOT_COPIED.iter().any(|n| k.eq_ignore_ascii_case(n)))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    if configuration
        .iter()
        .any(|(k, _)| k.eq_ignore_ascii_case("delta.setTransactionRetentionDuration"))
    {
        return Err(Error::Other(
            "the source sets delta.setTransactionRetentionDuration, which a Kafka pipeline refuses"
                .into(),
        ));
    }
    let url = ensure_table_uri(to).map_err(Error::Delta)?;
    let table = DeltaTable::try_from_url(url)
        .await
        .map_err(Error::Delta)?
        .create()
        .with_columns(columns)
        .with_partition_columns(partitions)
        .with_configuration(
            configuration
                .iter()
                .map(|(k, v)| (k.as_str(), Some(v.as_str()))),
        )
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .map_err(Error::Delta)?;
    Ok(table.version().unwrap_or(0) as i64)
}

/// Create the pipeline's data-quality table, `<target>__ddi_dq` unless `dq_uri` says otherwise,
/// with the columns every writer of it agrees on ([`crate::dq::columns`]). A pipeline without
/// one pauses on the first message it cannot decode, rather than discard it (§5.7). Refuses
/// where the table exists.
pub async fn create_dq(cfg: &ResolvedPipeline) -> Result<String> {
    let uri = cfg.dq_uri();
    let url = ensure_table_uri(&uri).map_err(Error::Delta)?;
    DeltaTable::try_from_url(url)
        .await
        .map_err(Error::Delta)?
        .create()
        .with_columns(crate::dq::columns())
        .with_save_mode(SaveMode::ErrorIfExists)
        .await
        .map_err(Error::Delta)?;
    Ok(uri)
}
