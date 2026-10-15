//! A source with Delta column mapping stores physical column names in its files. ddi reads the
//! files by logical name, so it would commit rows of NULLs and advance the offset: refused at
//! open instead.

mod common;

use common::{create_table, pipeline_cfg};
use delta_delta_ingest::pipeline::Pipeline;

#[tokio::test]
async fn a_column_mapped_source_is_refused_at_open() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let path = |name: &str| root.join(name).to_str().unwrap().to_string();
    let (source, target) = (path("source"), path("target"));
    create_table(&source).await;
    create_table(&target).await;

    let log = format!("{source}/_delta_log/{:020}.json", 0);
    let patched: Vec<String> = std::fs::read_to_string(&log)
        .unwrap()
        .lines()
        .map(|l| {
            let mut v: serde_json::Value = serde_json::from_str(l).unwrap();
            if let Some(m) = v.get_mut("metaData") {
                m["configuration"]["delta.columnMapping.mode"] = serde_json::json!("name");
            }
            v.to_string()
        })
        .collect();
    std::fs::write(&log, patched.join("\n") + "\n").unwrap();

    let err = match Pipeline::open(pipeline_cfg("mapped", &source, &target)).await {
        Ok(_) => panic!("a column-mapped source must be refused"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("column mapping"), "got: {err}");
}
