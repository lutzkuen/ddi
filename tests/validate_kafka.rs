//! What the `ddi` binary does with Kafka pipelines beside Delta pipelines
//! (docs/kafka-design.md, M1 and M3): `validate` judges them; a build without the `kafka`
//! feature holds them back by name (I16); with it they run, and one that cannot reach its
//! broker fails on its own while the Delta pipelines beside it run. Meaningful, and run, in both
//! feature configurations.

mod common;

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use common::*;

fn ddi() -> Command {
    Command::new(env!("CARGO_BIN_EXE_ddi"))
}

/// One Delta pipeline from `source` to `target`, and one Kafka pipeline beside it.
fn write_config(dir: &Path, source: &str, target: &str) -> PathBuf {
    let bronze = dir.join("bronze_events");
    let toml = format!(
        r#"
[runtime]
allowed_latency_secs = 1

[[pipeline]]
name = "orders"
app_id = "ddi.orders"
source_uri = "{source}"
target_uri = "{target}"

[kafka]
brokers = "127.0.0.1:9"

[kafka.schema_registry]
url = "http://127.0.0.1:9"

[[kafka.pipeline]]
name = "events"
app_id = "kafka_delta_ingest"
topic = "events"
target_uri = "{}"
"#,
        bronze.display()
    );
    let path = dir.join("pipelines.toml");
    std::fs::write(&path, toml).unwrap();
    path
}

/// stdout and stderr together: `tracing` writes to stdout, clap and panics to stderr.
fn text(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn version_names_librdkafka_only_with_the_feature() {
    let out = ddi().arg("--version").output().unwrap();
    assert!(out.status.success(), "{}", text(&out));
    let printed = String::from_utf8_lossy(&out.stdout);
    let plain = format!("ddi {}", env!("CARGO_PKG_VERSION"));
    if cfg!(feature = "kafka") {
        assert_eq!(printed.trim(), format!("{plain} (librdkafka 2.12.1)"));
    } else {
        assert_eq!(printed.trim(), plain, "unchanged without the feature");
    }
}

#[tokio::test]
async fn validate_judges_a_kafka_pipeline_and_reads_nothing() {
    let f = Fixture::new().await;
    let config = write_config(f.dir.path(), &f.source, &f.target);
    let out = ddi()
        .arg("validate")
        .arg("--config")
        .arg(&config)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    let printed = text(&out);
    assert!(printed.contains("orders"), "{printed}");

    if cfg!(feature = "kafka") {
        assert!(out.status.success(), "{printed}");
        assert!(
            printed.contains("kafka://events (group ddi-events) -> "),
            "{printed}"
        );
        assert!(printed.contains("app_id=kafka_delta_ingest"), "{printed}");
        assert!(printed.contains("2 pipeline(s) valid."), "{printed}");
    } else {
        // Held back, by name, and the exit code says so — a CI gate on `validate` gates.
        assert!(!out.status.success(), "{printed}");
        assert!(printed.contains("INVALID  events"), "{printed}");
        assert!(
            printed.contains("built without the `kafka` feature"),
            "{printed}"
        );
        assert!(printed.contains("1 pipeline(s) valid."), "{printed}");
    }
}

#[tokio::test]
async fn kafka_pipelines_are_held_back_or_fail_alone_and_the_delta_pipelines_run() {
    let f = Fixture::new().await;
    append(&f.source, &[1, 2, 3]).await;
    let config = write_config(f.dir.path(), &f.source, &f.target);

    let out = ddi()
        .arg("once")
        .arg("--config")
        .arg(&config)
        .env("RUST_LOG", "warn")
        .output()
        .unwrap();
    let printed = text(&out);
    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3], "{printed}");
    assert!(printed.contains("events"), "{printed}");
    if cfg!(feature = "kafka") {
        // It runs, and with no broker at 127.0.0.1:9 it fails on its own: the run's exit code
        // says so, and the Delta pipeline beside it was not stopped.
        assert!(!out.status.success(), "{printed}");
        assert!(printed.contains("pipeline failed"), "{printed}");
    } else {
        assert!(out.status.success(), "{printed}");
        assert!(printed.contains("held back"), "{printed}");
        assert!(
            printed.contains("built without the `kafka` feature"),
            "{printed}"
        );
    }
}
