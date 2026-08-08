//! Plan Milestone 4, the strict reading: kill the *process* mid-batch, repeatedly.
//!
//! The in-process restart tests in `exactly_once.rs` drop a `Pipeline` between steps,
//! which proves the offset logic. This proves the real thing: SIGKILL — no unwinding, no
//! destructors, no flush — at an arbitrary point, then restart and check the target.
//!
//! If this test does not hold, the project has no reason to exist.

mod common;

use std::process::Stdio;
use std::time::Duration;

use common::*;

fn ddi_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ddi")
}

fn write_config(path: &std::path::Path, source: &str, target: &str) {
    let toml = format!(
        r#"
[defaults]
allowed_latency_secs = 1

[[pipeline]]
name = "killtest"
app_id = "ddi.killtest"
source_uri = "{source}"
target_uri = "{target}"
max_files_per_batch = 1
"#
    );
    std::fs::write(path, toml).unwrap();
}

/// Run `ddi once`, SIGKILL it after `after`, and report whether it had already exited.
async fn run_and_kill(config: &std::path::Path, after: Duration) -> bool {
    let mut child = tokio::process::Command::new(ddi_bin())
        .arg("once")
        .arg("--config")
        .arg(config)
        .env("RUST_LOG", "warn")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn ddi");

    tokio::select! {
        status = child.wait() => {
            // Finished before we could kill it. Assert success: a run that dies instantly
            // on a usage error would otherwise masquerade as "completed too fast".
            let status = status.expect("wait");
            assert!(status.success(), "ddi exited with {status} instead of running");
            true
        }
        _ = tokio::time::sleep(after) => {
            // SIGKILL: the process gets no chance to clean up, flush, or commit.
            let _ = child.start_kill();
            let _ = child.wait().await;
            false
        }
    }
}

async fn run_to_completion(config: &std::path::Path) {
    let out = tokio::process::Command::new(ddi_bin())
        .arg("once")
        .arg("--config")
        .arg(config)
        .env("RUST_LOG", "warn")
        .output()
        .await
        .expect("spawn ddi");
    assert!(
        out.status.success(),
        "final run failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigkill_mid_batch_leaves_no_duplicates_and_no_gaps() {
    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();
    create_table(&source).await;
    create_table(&target).await;

    // 30 separate commits, one file each, and max_files_per_batch=1 — so the daemon
    // performs 30 distinct commit cycles and a kill is very likely to land inside one.
    let expected: Vec<i64> = (1..=30).collect();
    for i in &expected {
        append(&source, &[*i]).await;
    }

    let config = root.join("pipelines.toml");
    write_config(&config, &source, &target);

    // Kill at a spread of delays so the cut lands at different points in the cycle:
    // during a scan, during a parquet write, and just before/after a commit.
    let mut kills = 0;
    for ms in [40u64, 90, 160, 250, 380, 550, 700, 900] {
        let finished = run_and_kill(&config, Duration::from_millis(ms)).await;
        if !finished {
            kills += 1;
        }

        // The invariant must hold after EVERY kill, not just at the end: a partially
        // applied batch would show up as duplicates here.
        let ids = read_ids(&target).await;
        assert!(
            !has_duplicates(&ids),
            "duplicates after a kill at {ms}ms: {ids:?}"
        );
        assert!(
            ids.iter().all(|x| expected.contains(x)),
            "target grew rows the source never had: {ids:?}"
        );
        // Rows must only ever be a prefix of the source: no gaps in what was consumed.
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        assert_eq!(ids, sorted);
        assert_eq!(
            ids,
            expected[..ids.len()].to_vec(),
            "consumed rows must be a gap-free prefix of the source"
        );

        if ids.len() == expected.len() {
            break;
        }
    }

    assert!(
        kills > 0,
        "the process always finished first; the test proved nothing"
    );

    // Finally, let it finish cleanly.
    run_to_completion(&config).await;

    let ids = read_ids(&target).await;
    assert_eq!(
        ids, expected,
        "every row exactly once after repeated SIGKILLs"
    );
    assert!(!has_duplicates(&ids));

    // And one more clean run must be a no-op.
    let version_before = open(&target).await.version();
    run_to_completion(&config).await;
    assert_eq!(
        open(&target).await.version(),
        version_before,
        "a run after catch-up must not commit anything"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_killed_run_never_writes_rows_without_advancing_its_offset() {
    // The other direction of the same guarantee: data and offset move together, so the
    // number of rows in the target must always equal the number of source rows consumed
    // according to the txn action.
    use delta_delta_ingest::offset::OffsetStore;

    let dir = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(dir.path()).unwrap();
    let source = root.join("source").to_str().unwrap().to_string();
    let target = root.join("target").to_str().unwrap().to_string();
    create_table(&source).await;
    create_table(&target).await;

    for i in 1..=20i64 {
        append(&source, &[i]).await;
    }
    let config = root.join("pipelines.toml");
    write_config(&config, &source, &target);

    for ms in [60u64, 140, 300, 520] {
        run_and_kill(&config, Duration::from_millis(ms)).await;

        let ids = read_ids(&target).await;
        let t = open(&target).await;
        let stored = OffsetStore::new("ddi.killtest", 0)
            .last_committed_version(&t)
            .await
            .unwrap();

        match stored {
            // Source version V corresponds to rows 1..=V (version 0 is the CREATE).
            Some(v) => assert_eq!(
                ids.len() as u64,
                v,
                "offset says {v} source versions consumed but the target holds {} rows",
                ids.len()
            ),
            None => assert!(
                ids.is_empty(),
                "rows exist with no offset recorded: {ids:?}"
            ),
        }
    }
}
