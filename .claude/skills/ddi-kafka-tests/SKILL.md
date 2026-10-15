---
name: ddi-kafka-tests
description: Run ddi's Kafka integration tests - start the pinned Redpanda, set the env vars, run the whole ignored suite or one file, skip the soak, and run the real-kdi takeover test. Use before claiming any change to src/kafka, src/fence.rs, src/sink.rs or the operator tools works, and for M-level regression runs.
---

# Kafka integration tests

They are `#[ignore]`d without a broker. Read `ddi-build-hygiene` first (target dir on the external drive, one heavy job at a time). If cargo is blocked by the hook (after a reboot or replug the target image is not mounted): `udisksctl loop-setup -f /media/lutz/ExternalHD/cargo-target.img`, then `udisksctl mount -b /dev/loopN` (N from the first command's output); see `ddi-build-hygiene`.

## Broker
```
docker rm -f ddi-redpanda 2>/dev/null
docker run -d --rm --name ddi-redpanda --memory 1500m -p 127.0.0.1:9092:9092 -p 127.0.0.1:8081:8081 \
  redpandadata/redpanda:v26.2.3 redpanda start --mode dev-container --smp 1 --memory 1G \
  --kafka-addr 0.0.0.0:9092 --advertise-kafka-addr 127.0.0.1:9092 --schema-registry-addr 0.0.0.0:8081
until curl -sf http://127.0.0.1:8081/subjects >/dev/null; do sleep 2; done
export DDI_TEST_KAFKA=127.0.0.1:9092 DDI_TEST_SR=http://127.0.0.1:8081
```
The version is pinned on purpose (CI uses the same). Stop it when done: `docker rm -f ddi-redpanda`.

## Runs (all with `--features kafka,fault-injection`, `-j 2`, in the background)
- Whole suite, as CI: `cargo test -j 2 --features kafka,fault-injection --test 'kafka_*' -- --ignored --test-threads=1 --skip soak --skip ddi_takes_a_kdi` (~20 min, 57 tests; `--test-threads=1` because tests share one broker).
- One file: `--test kafka_tools -- --ignored` (also `kafka_bronze`, `kafka_basics`, `kafka_groups`, `kafka_offsets`, `kafka_schemas`, `kafka_targets`, `kafka_aging`).
- `kafka_soak.rs` is the multi-hour soak; never in a review loop.
- No broker needed: `kafka_translation`, `avro_production_parity`, the lib tests.

## Real kdi takeover (`tests/kafka_kdi.rs`)
Needs `DDI_TEST_KDI=<path to a kafka-delta-ingest binary>` (build: `ddi-build-hygiene`, "kdi"; keep a copy in `~/kdi-private/bin/`). ~100 s. See `ddi-kdi-parity` for what it proves and its traps.

## Reading failures
- Test output goes to a file; `grep -E "^test |panicked|FAILED"`, then read the assertion. A "left/right" mismatch on a row prints both rows in full.
- Fault hooks (`DDI_TEST_FAULT`) exist only with `fault-injection`; a test using one fails to compile without it.
- If many tests fail at once with connection errors, the broker died (memory): recreate the container.

## `kafka_kill_mid_batch` and its coverage guard (tests/kafka_basics.rs)
It SIGKILLs `ddi once` until >=30 kills and at least one left an orphan parquet (a kill between upload and commit). The panic `N of 200 runs were still running when killed, and 0 files left` is that coverage guard, **not** an exactly-once failure: the duplicate and exactly-once assertions come after the loop and never ran.
- Upload and commit are both inside one delta-rs `write.await` (`Sink::write`); there is no seam for a fault hook, and `sleep_before_commit` fires *before* the upload, so it does not widen the window. The deterministic cover of that window is `kill_between_upload_and_commit` (read-only `_delta_log`).
- The kills used to land 250-1650 ms after launch, which a slow runner outlasted (red twice on the v0.5.0 tag, green on the same SHA on main). They now spread over the measured length of one unkilled run (#18). If the guard fires again, compare the test's duration with green runs (76-215 s when an orphan shows up early; ~440 s when all 200 iterations run) and check the span, before touching the hooks.
- ddi's stderr is discarded in this test; to see why kills miss, send it to a file for a local run.
