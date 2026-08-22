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
use deltalake::datafusion::execution::disk_manager::DiskManagerBuilder;

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
            Arc::ptr_eq(&r.disk_manager, &installed.disk_manager()),
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

#[tokio::test(flavor = "multi_thread")]
async fn the_spill_gauges_say_what_the_disk_manager_says() {
    let installed = installed();
    let m = delta_delta_ingest::metrics::Metrics::new();
    // A pipeline has to exist for the labelled series to render; the spill gauges are
    // process-wide and unlabelled, like the gate's.
    m.pipeline("anything");
    let rendered = m.render();

    assert!(
        rendered.contains(&format!(
            "ddi_spill_limit_bytes {}",
            installed.limit_bytes()
        )),
        "the budget an alert divides by has to be exported: {rendered}"
    );
    assert!(rendered.contains("ddi_spill_bytes "), "{rendered}");
    assert!(rendered.contains("ddi_spill_files "), "{rendered}");
    assert!(rendered.contains("ddi_capacity_exhausted{pipeline=\"anything\"} 0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn running_out_of_capacity_raises_a_gauge_a_storage_blip_does_not() {
    installed();
    let m = delta_delta_ingest::metrics::Metrics::new();
    let p = m.pipeline("thirsty");

    p.observe_error(&delta_delta_ingest::Error::Other("a storage blip".into()));
    assert!(m
        .render()
        .contains("ddi_capacity_exhausted{pipeline=\"thirsty\"} 0"));

    p.observe_error(&delta_delta_ingest::Error::Capacity(
        "the spill directory".into(),
    ));
    assert!(m
        .render()
        .contains("ddi_capacity_exhausted{pipeline=\"thirsty\"} 1"));

    // Raised, never lowered by another failure — only by a step that actually worked.
    p.observe_error(&delta_delta_ingest::Error::Other("something else".into()));
    assert!(m
        .render()
        .contains("ddi_capacity_exhausted{pipeline=\"thirsty\"} 1"));
    p.mark_progress();
    assert!(m
        .render()
        .contains("ddi_capacity_exhausted{pipeline=\"thirsty\"} 0"));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_real_spill_that_breaks_the_budget_reaches_a_pipeline_as_a_capacity_failure() {
    // The end-to-end shape of the incident, and the reason Error::Capacity exists at all.
    // Before this, every DataFusion failure that arrived through delta-rs became Error::Delta
    // and every one that arrived directly became Error::Transform — so a full spill directory
    // was indistinguishable from a wrong answer, the supervisor retried it a second later
    // forever, and ddi_capacity_exhausted never moved.
    //
    // Driving a merge into a genuine 4 MiB overrun would need a target far too large for CI,
    // so what is exercised here is the classification and the plumbing on top of a real
    // DiskManager that has really been pushed past its cap.
    let installed = installed();
    let dm = std::sync::Arc::new(
        DiskManagerBuilder::default()
            .with_max_temp_directory_size(64 * 1024)
            .build()
            .unwrap(),
    );
    let mut f = dm.create_tmp_file("a merge spilling").unwrap();
    std::fs::write(f.path(), vec![0u8; 256 * 1024]).unwrap();
    let raised = f
        .update_disk_usage()
        .expect_err("256 KiB does not fit a 64 KiB budget");

    // Exactly the sentence DataFusion produces, not one written by hand.
    let text = raised.to_string();
    assert!(
        text.contains("used disk space during the spilling process"),
        "{text}"
    );

    // Through delta-rs, as a merge's failure really arrives.
    let e = spill::classify_delta(
        deltalake::DeltaTableError::Generic(text.clone()),
        "upsert: merging into the target",
    );
    assert!(
        matches!(e, delta_delta_ingest::Error::Capacity(_)),
        "a full spill directory must not read as an ordinary Delta error: {e}"
    );
    assert!(e.to_string().contains("[runtime] max_temp_directory_size"));

    // And the gauge the operator alerts on moves for it.
    let m = delta_delta_ingest::metrics::Metrics::new();
    let p = m.pipeline("merging");
    p.observe_error(&e);
    assert!(m
        .render()
        .contains("ddi_capacity_exhausted{pipeline=\"merging\"} 1"));

    // The process budget is untouched by any of this: the throwaway manager above is not the
    // installed one, which is the property the whole module rests on.
    assert_eq!(installed.limit_bytes(), BUDGET);
}

#[tokio::test(flavor = "multi_thread")]
async fn hitting_the_cap_once_does_not_stop_the_process_spilling_for_good() {
    // The failure this module would otherwise have *created*. DataFusion 53.1 charges the write
    // that breaks the cap and returns before recording it, so `Drop` subtracts the older figure
    // and the difference stays charged. With one manager per operation that residue died with
    // the operation; with one manager for the process it accumulates, and once it reaches the
    // cap every subsequent spill anywhere in the process is refused — not the pipeline that
    // overran, all of them.
    //
    // Deliberately on its own `Spill` rather than the installed one. Every other test in this
    // binary shares the installed budget and asserts against its counter, and driving that
    // counter up to its cap here would fail them for reasons that have nothing to do with what
    // they are testing. What is on trial is `Spill`'s own behaviour, and a local one exercises
    // exactly the same code.
    let dir = tempfile::tempdir().unwrap();
    let spill = spill::Spill::resolve(dir.path().to_str(), Some(BUDGET)).unwrap();
    assert_eq!(spill.stranded_bytes(), 0);

    // Strand the whole cap, through this Spill's own shared manager.
    let dm = spill.disk_manager();
    let mut f = dm.create_tmp_file("a merge spilling").unwrap();
    std::fs::write(f.path(), vec![0u8; 1024]).unwrap();
    f.update_disk_usage().expect("1 KiB fits");
    std::fs::write(f.path(), vec![0u8; (BUDGET as usize) * 2]).unwrap();
    f.update_disk_usage()
        .expect_err("twice the budget does not");
    drop(f);

    // Nothing on disk, yet the counter reads over the cap: from here DataFusion refuses every
    // spill, however small.
    assert_eq!(dm.spilling_progress().active_files_count, 0);
    assert!(
        dm.used_disk_space() >= BUDGET,
        "the upstream leak this test exists for did not happen. DataFusion fixed it in 55.0.0 \
         (FileSpillWriter::write rolls its own fetch_add back), so if deltalake has bumped past \
         54, delete Spill::recover_if_wedged, wedged(), the stranded counter, \
         ddi_spill_stranded_bytes_total and this test together — and put the prototype back \
         behind a plain Arc"
    );
    let mut doomed = dm.create_tmp_file("a later, small spill").unwrap();
    std::fs::write(doomed.path(), vec![0u8; 4096]).unwrap();
    doomed
        .update_disk_usage()
        .expect_err("the old manager refuses everything, which is what makes replacing it safe");
    drop(doomed);

    // Asking for a runtime is what notices and repairs it.
    let fresh = spill.runtime_builder().build_arc().unwrap();
    assert!(
        !Arc::ptr_eq(&fresh.disk_manager, &dm),
        "a wedged manager has to be replaced, not reused"
    );
    assert_eq!(fresh.disk_manager.used_disk_space(), 0);
    assert_eq!(fresh.disk_manager.max_temp_directory_size(), BUDGET);
    assert!(
        spill.stranded_bytes() >= BUDGET,
        "what the leak cost is counted, not absorbed"
    );

    // A later spill genuinely works, which is the whole point.
    let mut g = fresh
        .disk_manager
        .create_tmp_file("a later, small spill")
        .unwrap();
    std::fs::write(g.path(), vec![0u8; 4096]).unwrap();
    g.update_disk_usage()
        .expect("a small spill after a capacity failure is not refused");
    drop(g);

    // Still one shared counter afterwards: recovery replaces the manager, it does not hand
    // every caller its own.
    let a = spill.runtime_builder().build_arc().unwrap();
    let b = spill.runtime_builder().build_arc().unwrap();
    assert!(Arc::ptr_eq(&a.disk_manager, &b.disk_manager));
    assert_eq!(a.disk_manager.max_temp_directory_size(), BUDGET);
}

#[tokio::test(flavor = "multi_thread")]
async fn residue_below_the_cap_is_lived_with_rather_than_replaced() {
    // The other half of the threshold, and the one that keeps the process bound honest. A
    // manager carrying residue below its cap still works, just with less room. Replacing it
    // there would leave the process holding two managers with a full cap each while any runtime
    // built before the swap is still alive — two times the cap, which is the arithmetic that
    // evicted the pod. The bounded loss is the right thing to accept.
    let dir = tempfile::tempdir().unwrap();
    let spill = spill::Spill::resolve(dir.path().to_str(), Some(BUDGET)).unwrap();
    let dm = spill.disk_manager();

    // Strand a little: grow past a recorded size, but stay under the cap.
    let mut f = dm.create_tmp_file("a spill").unwrap();
    std::fs::write(f.path(), vec![0u8; 1024]).unwrap();
    f.update_disk_usage().unwrap();
    std::fs::write(f.path(), vec![0u8; 64 * 1024]).unwrap();
    f.update_disk_usage().unwrap();
    drop(f);

    let residue = dm.used_disk_space();
    assert_eq!(dm.spilling_progress().active_files_count, 0);
    assert!(
        residue < BUDGET,
        "this test needs residue below the cap, got {residue}"
    );

    let next = spill.runtime_builder().build_arc().unwrap();
    assert!(
        Arc::ptr_eq(&next.disk_manager, &dm),
        "a manager that can still accept a spill must not be replaced — two live managers is \
         two caps, and two caps is the bug this module exists to remove"
    );
    assert_eq!(spill.stranded_bytes(), 0);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn a_spill_file_that_could_not_be_created_does_not_disarm_the_recovery() {
    // `create_tmp_file` increments active_files_count and *then* creates the file, returning
    // early if that fails — so one EMFILE, one read-only remount, one full volume leaks the
    // count with no RefCountedTempFile ever built to drop it. Using that counter as the
    // quiescence witness would therefore disarm recovery permanently, and stay disarmed long
    // after the directory was healthy again. The directory itself cannot lie that way, which is
    // why it is what `wedged` reads.
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let spill = spill::Spill::resolve(dir.path().to_str(), Some(BUDGET)).unwrap();
    let dm = spill.disk_manager();

    // One failed create, from a fault that lasts exactly one call.
    let inner = spill
        .temp_dir_paths()
        .into_iter()
        .next()
        .expect("a spill dir");
    let original = std::fs::metadata(&inner).unwrap().permissions();
    std::fs::set_permissions(&inner, std::fs::Permissions::from_mode(0o555)).unwrap();
    let refused = dm.create_tmp_file("a sort spilling").is_err();
    std::fs::set_permissions(&inner, original).unwrap();
    if !refused {
        return; // running as root, where the mode bits do not bite. Nothing to pin.
    }
    assert_eq!(
        dm.spilling_progress().active_files_count,
        1,
        "this test exists because the failed create leaks the count; if DataFusion has fixed \
         that, the counter is usable again and `wedged` can be simplified"
    );

    // Now strand the whole cap, exactly as a capacity failure does.
    let mut f = dm.create_tmp_file("a merge spilling").unwrap();
    std::fs::write(f.path(), vec![0u8; 1024]).unwrap();
    f.update_disk_usage().unwrap();
    std::fs::write(f.path(), vec![0u8; (BUDGET as usize) * 2]).unwrap();
    f.update_disk_usage()
        .expect_err("twice the budget does not fit");
    drop(f);

    // The leaked count still reads non-zero. The directory is empty, which is the truth.
    assert!(dm.spilling_progress().active_files_count > 0);
    assert!(spill
        .temp_dir_paths()
        .iter()
        .all(|d| std::fs::read_dir(d).unwrap().next().is_none()));

    let fresh = spill.runtime_builder().build_arc().unwrap();
    assert!(
        !Arc::ptr_eq(&fresh.disk_manager, &dm),
        "a leaked active-files count must not keep a wedged manager in service"
    );
    assert_eq!(fresh.disk_manager.used_disk_space(), 0);
    // And the replacement starts from a clean counter, so the leak is gone too.
    assert_eq!(fresh.disk_manager.spilling_progress().active_files_count, 0);
}
