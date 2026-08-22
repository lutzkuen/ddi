//! One spill directory and one budget, shared by every session in the process.
//!
//! Its own test binary on purpose, and for the same reason [`tests/memory_budget.rs`] is:
//! [`spill::install`] is process-wide and once, so a file that installs a budget cannot live
//! beside one that expects the default. Every test here installs the *same* budget through
//! [`installed`], and `install` is idempotent, so the order the harness runs them in does not
//! matter.
//!
//! What is on trial is the property the incident turned on: eleven pipelines in one pod each
//! honoured DataFusion's 100 GB spill cap, because each of them had its own. Nothing here
//! writes a hundred gigabytes — the assertions are structural, and they are the cheap,
//! CI-visible proof that there is one counter rather than N.

mod common;

use std::sync::{Arc, OnceLock};

use common::*;
use delta_delta_ingest::{budget, spill};

/// Small enough to be obviously not DataFusion's default, large enough to be a legal cap.
const BUDGET: u64 = 4 * 1024 * 1024;

static DIR: OnceLock<tempfile::TempDir> = OnceLock::new();

/// The process's budget, installed once however many tests ask for it.
fn installed() -> Arc<spill::Spill> {
    let dir = DIR.get_or_init(|| tempfile::tempdir().unwrap());
    let s = spill::Spill::resolve(dir.path().to_str(), Some(BUDGET)).expect("a tempdir is usable");
    // Ignored deliberately: whichever test ran first installed the identical value.
    let _ = spill::install(s);
    spill::current()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_session_this_process_builds_spills_into_the_one_configured_directory() {
    let f = Fixture::new().await;
    let installed = installed();
    let a = open(&f.source).await;
    let b = open(&f.target).await;

    // Four routes to a runtime, which between them cover every DataFusion consumer in this
    // tool: the transform's bare runtime, the scans and merges, and delta-rs's own write path.
    let runtimes = [
        budget::runtime().unwrap(),
        budget::session(&a).unwrap().runtime_env().clone(),
        budget::session(&b).unwrap().runtime_env().clone(),
        budget::delta_session(&b).unwrap().runtime_env().clone(),
    ];

    for (i, r) in runtimes.iter().enumerate() {
        assert!(
            Arc::ptr_eq(&r.disk_manager, installed.disk_manager()),
            "runtime {i} built its own disk manager, so it has its own {} budget",
            bytesize::ByteSize(r.disk_manager.max_temp_directory_size())
        );
        assert_eq!(r.disk_manager.max_temp_directory_size(), BUDGET);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_pipelines_draw_on_one_budget_rather_than_one_each() {
    // Acceptance criterion 5, in milliseconds and without writing a hundred gigabytes: what a
    // spill through one pipeline's runtime costs must be visible in another's, because there
    // is only one counter to charge it to.
    let f = Fixture::new().await;
    installed();
    let a = open(&f.source).await;
    let b = open(&f.target).await;

    let one = budget::session(&a).unwrap().runtime_env().clone();
    let two = budget::session(&b).unwrap().runtime_env().clone();

    let before = two.disk_manager.used_disk_space();
    let mut file = one
        .disk_manager
        .create_tmp_file("a pipeline spilling")
        .expect("the budget allows a spill file");
    // Written through the path rather than the handle: `RefCountedTempFile` hands out only a
    // shared reference, and what is on trial is the accounting, not the write.
    std::fs::write(file.path(), vec![0u8; 64 * 1024]).unwrap();
    file.update_disk_usage().unwrap();

    assert!(
        two.disk_manager.used_disk_space() > before,
        "one pipeline's spill must be charged against every other pipeline's budget"
    );
    assert_eq!(
        two.disk_manager.used_disk_space(),
        one.disk_manager.used_disk_space(),
        "there is one counter, not two that happen to agree"
    );

    // And released on drop, so a pipeline retrying a failed scan does not accumulate.
    drop(file);
    assert_eq!(two.disk_manager.used_disk_space(), before);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_append_path_spills_into_ddis_directory_and_not_delta_rss_own() {
    // delta-rs builds its own SessionState when it is not given one — an unbounded memory pool
    // and a DiskManager this process has never seen. `Sink::commit` now hands it ours. What is
    // asserted is that the write still works through delta-rs's own planner, which is the part
    // that made this more than a one-line change.
    let f = Fixture::new().await;
    let installed = installed();
    append(&f.source, &[1, 2, 3]).await;

    let sink = delta_delta_ingest::sink::Sink::new("ddi.test.appending", 128 * 1024 * 1024);
    let table = open(&f.target).await;
    sink.commit(table, vec![batch(&[1, 2, 3])], 0)
        .await
        .expect("an append through a bounded session still commits");

    assert_eq!(read_ids(&f.target).await, vec![1, 2, 3]);
    // A small append spills nothing, which is the point: the budget it did not spend is the
    // process's, not a private hundred gigabytes nobody can see.
    assert_eq!(installed.used_bytes(), 0);
    assert_eq!(installed.limit_bytes(), BUDGET);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_error_a_full_spill_directory_produces_names_the_key_that_raises_it() {
    installed();
    // DataFusion's own sentence names `max_temp_directory_size` as a "disk manager
    // configuration", which is not a thing this tool's operator has. Classifying it is what
    // turns it into a knob they can actually reach.
    let e = spill::classify(
        deltalake::datafusion::error::DataFusionError::ResourcesExhausted(
            "The used disk space during the spilling process has exceeded the allowable limit \
             of 4.0 MB."
                .into(),
        ),
        "upsert",
    );
    assert!(matches!(e, delta_delta_ingest::Error::Capacity(_)));
    let m = e.to_string();
    assert!(m.contains("[runtime] max_temp_directory_size"), "{m}");
    assert!(
        m.contains(&format!("({BUDGET} bytes)")),
        "the resolved size in bytes, so it cannot be confused with DataFusion's own \
         binary-unit rendering of the same number: {m}"
    );
    assert!(m.contains("no other pipeline was stopped"), "{m}");
}
