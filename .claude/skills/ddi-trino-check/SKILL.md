---
name: ddi-trino-check
description: Verify ddi's SQL/function behaviour against a live Trino 480 container instead of memory or docs - parse checks for `ddi dbt convert` output and value comparisons of expressions (JSON, decimals, timestamps, from_unixtime, casts). Use before claiming ddi matches Trino/Starburst, when adding or changing a Trino function spelling, or when a transform result looks wrong.
---

# Check ddi against a real Trino

Trino's spelling is the contract (the user's models are written for Starburst). Docs and memory have been wrong before (e.g. `RETURNING JSON` is not valid, `json_object` key order, emoji escaping), so ask Trino.

## Start Trino (needs docker; check `docker ps` first)
```
docker run -d --name trino -p 127.0.0.1:8080:8080 trinodb/trino:480
until curl -sf http://127.0.0.1:8080/v1/info | grep -q '"starting":false'; do sleep 2; done   # ~30-60 s
```
Reuse a running `trino` container. Stop it when done (`docker rm -f trino`); it is not a service to leave up.

## Quick look at what Trino answers
`docker exec trino trino --execute "SELECT json_format(CAST(MAP(ARRAY['b','a'],ARRAY[1,2]) AS JSON))"`
Use this to learn the expected value, then pin it in a test below. Do not report a Trino behaviour you did not just see (or read from Trino source) as verified.

## Pin it in the suite: tests/trino_dialect.rs
Run: `export PATH="$HOME/.cargo/bin:$PATH"; DDI_TEST_TRINO=http://127.0.0.1:8080 cargo test --test trino_dialect -j 2 -- --ignored --nocapture [name]`
- `converted_transform_sql_parses_in_trino`: every model in the list is valid Trino and its `ddi dbt convert` text must parse there too (Trino's analyser refusing with "relation does not exist" is the pass signal). Add a model string when you add syntax.
- `differences(&trino, &source, &["expr", ...], per_batch)`: runs `SELECT <expr> AS v FROM <source>` in Trino and in ddi's `SqlTransform` over the same Arrow batch and lists mismatches, compared bit for bit for floats (`spelt_alike` normalises spelling). Add an expression to an existing test (`decimals(...)`, `epochs()` give typed sources) or a new `#[tokio::test] #[ignore]` test using `differences` and assert the vec is empty.
- A difference is a bug in ddi unless Trino's behaviour is documented as undefined; then say so in the test comment and in `docs`/`USING_DDI.md`.

## Report
State the Trino version (480), the expressions compared, and that CI's `trino-dialect` job runs the same file with `--ignored`. If docker is unavailable, say the claim is unverified rather than relying on docs.

**Cargo blocked by the hook?** The target-dir image is unmounted (reboot/replug). Re-attach: `udisksctl loop-setup -f /media/lutz/ExternalHD/cargo-target.img`, then `udisksctl mount -b /dev/loopN` (N from the first command's output); see `ddi-build-hygiene`.
