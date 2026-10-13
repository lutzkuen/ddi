---
name: ddi-kdi-parity
description: Check that ddi writes what kafka-delta-ingest (kdi) wrote - the real-kdi takeover test, the apache-avro version question, and the traps found building them. Use when changing decode/conform/kdi_text, the production transform_sql, takeover or rollback behaviour, or when asked "is ddi byte-identical to kdi".
---

# kdi parity

Never argue parity from reading code. Run kdi and compare.

## The harness: `tests/kafka_kdi.rs`
One corpus (240 messages, 3 partitions, two writer schema versions, nulls at several depths, a decimal, an out-of-order map, a tombstone, messages either side of midnight UTC), produced to one topic. Table A: kdi alone reads everything. Table B: kdi reads the first half, **ddi takes B over** (kdi's `app_id`, `kdi_compat = true`) and reads the rest, then **kdi reads B again** (rollback) and ddi runs once more (back in ddi's era). Compare A and B by `(kafka_partition, kafka_offset)`, every column as text, plus the `ingest_date` partition values. Needs `DDI_TEST_KDI` and Redpanda (`ddi-kafka-tests`).

## What it taught (keep these true)
- The production bronze `transform_sql` must keep **`kafka_value_schema_id`** in its SELECT list (the target has no such column, so it is never written). Without it the conform step cannot tell a row's writer schema and writes `"field":null` into `data` for fields the older writer never had. `ddi kafka plan` warns about it.
- `kdi_compat = true` alone is the production recipe; `kdi_null_text` follows it. A column the SQL computes (every lower-case alias) is NULL when null, never `"null"`.
- kdi's `-e` (ends at latest offsets) did not terminate here (it waits on the last message of each partition, and stopped after the first batch). The harness runs kdi until its table's offsets reach the topic's high watermarks, then kills it. A new consumer group also takes ~30 s to get its first assignment.
- A tombstone must not be the last message of a partition for kdi's sake; put it first.
- kdi `-t` targets are lower-cased by the production entrypoint (`messagetime`, not `messageTime`); copy that list, not the YAML's.

## Which avro does production use?
Read it off the facts, not the repo: `schema_registry_converter 3.1.0` depends on `apache-avro ^0.14` (crates.io API: `curl -sA x https://crates.io/api/v1/crates/<crate>/<version>/dependencies`), and kdi converts the decoded `Value` to JSON in the crate that owns that `Value`. `tests/avro_production_parity.rs` compares one datum of every Avro kind through 0.14.0 and 0.21.0 (renamed dev-dependency `apache-avro-014`): same JSON everywhere; 0.14.0 cannot parse a decimal on a `fixed` at all (so production topics have none). The older `kdi_compat` matrices in `src/kafka/conform/kdi_tests.rs` stay on 0.21.0 because 0.14.0 lacks `BigDecimal` and the local-timestamp variants.

## When you change something
Rerun `kafka_bronze`, `kafka_kdi`, `avro_production_parity` and the lib tests under `src/kafka/conform`. If kdi and ddi differ, decide per difference: a documented one (design §6.4) or a bug. Do not edit the test to match.
