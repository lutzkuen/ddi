# ddi

Exactly-once streaming into Delta tables (Delta → Delta, and Kafka → Delta behind the `kafka`
feature), in one Rust binary. Apache-2.0, public at github.com/lutzkuen/ddi, `main` is the
default branch. Instructions come from a dbt project; ddi has no functional config of its own
(only connections and "how eager" settings). Starburst/Trino spelling is the SQL contract.

## Build and test (this laptop)
- `cargo` is not on the non-interactive PATH: start commands with `export PATH="$HOME/.cargo/bin:$PATH"`.
- **Build output**: the target dir is `~/cargo-target` -> `/media/lutz/cargotarget`, an ext4 image on the external drive (active; watch `df -h /media/lutz/cargotarget`). If cargo is blocked by the hook (after a reboot or replug the target image is not mounted): `udisksctl loop-setup -f /media/lutz/ExternalHD/cargo-target.img`, then `udisksctl mount -b /dev/loopN` (N from the first command's output); see `ddi-build-hygiene`. Never build into the internal disk. One heavy cargo job at a time.
- Always `-j 2`. `~/.cargo/config.toml` runs rustc and test binaries in a capped `build.slice`
  (systemd-oomd otherwise kills the VS Code window). A clean build is ~8 min. Never plain `--release`
  (single huge LTO link); use `--profile release-lean` (~38 min).
- Wrap long runs in `timeout` and run them in the background; do not poll.
- `--features kafka` builds vendored librdkafka (~3.5 min); `BINDGEN_EXTRA_CLANG_ARGS` is already set in `~/.cargo/config.toml`.
- The gate CI enforces, in this order, before every push:
  ```
  cargo fmt --all -- --check
  cargo clippy --all-targets -j 2 -- -D warnings
  cargo clippy --all-targets --features kafka,fault-injection -j 2 -- -D warnings
  cargo test --all-targets -j 2
  cargo test --all-targets --features kafka -j 2
  cargo deny check licenses          # after any Cargo.toml / Cargo.lock change
  ```
  A Delta-only build (no features) must stay untouched by Kafka (design invariants I13, I16).
- Tests that need a service are `#[ignore]`d and take env vars (skills: `ddi-kafka-tests`, `ddi-kdi-parity`, `ddi-trino-check`):
  - Redpanda (Kafka + Schema Registry): the docker command is in `tests/kafka_common/mod.rs`, then
    `DDI_TEST_KAFKA=127.0.0.1:9092 DDI_TEST_SR=http://127.0.0.1:8081 cargo test --features kafka,fault-injection --test 'kafka_*' -- --ignored --test-threads=1 --skip soak --skip ddi_takes_a_kdi`
  - Trino 480: see the `ddi-trino-check` skill (`DDI_TEST_TRINO`).
  - Azurite: `--test azure_object_store -- --ignored` (CI job `object-store`).
  - dbt end-to-end: `examples/dbt/run_demo.sh` (needs dbt-core, dbt-duckdb, deltalake).
  - `kafka_soak.rs` is the multi-hour soak; never run it in a review loop.
- If a build OOMs, look for an unrelated runaway process first (`ps -eo pid,rss,args --sort=-rss | head`).

## Layout
`src/`: `pipeline.rs` (step loop), `sink.rs`, `fence.rs` (txn reader and takeover), `dedup.rs`,
`upsert.rs`, `dq.rs` (data-quality tables), `grain.rs`, `transform/` (DataFusion SQL, Trino
function spellings, lambdas, JSON), `dbt/` (manifest, `dbt check`/`convert`), `source/`,
`publish/`, `lookup.rs`, `trino.rs` (client), `kafka/` (consumer, decode, conform, start, step,
rebalance, tools; design in `docs/kafka-design.md`, milestones M-1..M6 in its §13).
`tests/` one file per topic; `tests/golden/` pins commit shapes; `tests/kafka_*.rs` are integration.
Dependency sources for lookups: `~/.cargo/registry/src/*/deltalake-core-1.1.1` etc. (grep, do not guess).

## Rules already settled
- Scope: a PR ships what its issues ask. Add-ons that keep failing review go on their own branch.
  One review round per milestone/PR unless a round finds a correctness bug; do not iterate on heuristics a round refuted - replace them with a mechanism that is exact by construction, or drop the optimisation.
- Never silently lose a message or row: prefer a visible duplicate/DQ row or a blocked pipeline over a skip.
- Kafka leg is append-only into bronze; keep kdi's txn convention (`"<app_id>-<partition>"`, version = last offset written).
- Behaviour claims about Trino/Starburst are verified against a live Trino (`ddi-trino-check`), not memory.
- Never print, log or commit credentials (SCRAM, SR, storage keys). Name the env var instead.
- Git identity for commits made from here: `git -c user.name="Lutz" -c user.email="lutz.kuenneke89@gmail.com"` (no global config is set). Commit messages: `type: sentence` (fix/feat/docs/test/chore), no trailing period needed.
- Releases and merging: read the `ddi-ship` skill first, also when the user dictates git commands. Do not push, merge or tag without being asked.
- Project skills: `ddi-ship` (PR, CI, merge, release), `ddi-review` (one bounded review round), `ddi-trino-check`, `ddi-kafka-tests`, `ddi-kdi-parity`, `ddi-inventory-intake`, `ddi-milestone`, `ddi-build-hygiene`.
- Private data (the production inventory, hosts, topics, customer rows) never goes into this repo: it lives in `~/kdi-private/`.

## Where state lives
`docs/kafka-design.md` (design + "as built" notes per milestone), `README.md`, `USING_DDI.md`
(standalone user guide, pinned image version), `pipelines.example.toml`. Session memory:
`~/.claude/projects/-home-lutz-ddi/memory/`. M-1 inventory of the production kdi deployments
is gathered by the user in their work repo (see `ddi-inventory-intake`) and kept in `~/kdi-private/`.
