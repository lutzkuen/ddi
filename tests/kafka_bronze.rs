//! The production shape of a bronze table: one envelope every topic shares, its payload kept as
//! kafka-delta-ingest's JSON text, and the same twelve columns the 22 raw tables have
//! (docs/kafka-design.md §6.5, M-1).
//!
//! Ignored unless a broker is there: see tests/kafka_common/mod.rs for how to run them.

#![cfg(feature = "kafka")]

mod common;
mod kafka_common;

use delta_delta_ingest::pipeline::{Pipeline, StepOutcome};

use kafka_common::bronze::*;
use kafka_common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs Redpanda; set DDI_TEST_KAFKA"]
async fn the_production_envelope_lands_as_the_twelve_bronze_columns() {
    let f = Fixture::bare(1).await;
    create_table_with(&f.target, bronze_columns(), &["ingest_date"], &[]).await;
    create_dq(&f.dq).await;
    let id = register(&f.subject(), ENVELOPE).await;
    let producer = Producer::new();
    // Either side of midnight UTC, and the second message's correlation id is NULL.
    let late = 1_791_935_999_999i64; // 2026-10-13T23:59:59.999Z
    let early = 1_791_936_000_000i64; // 2026-10-14T00:00:00.000Z
    for (m, value) in [
        ("m-1", message("m-1", late, Some("c-1"), Some("5.00"))),
        ("m-2", message("m-2", early, None, None)),
    ] {
        let _ = m;
        producer
            .send(&f.topic, 0, None, Some(&framed(ENVELOPE, id, value)))
            .await;
    }
    let toml = f.toml(
        "bronze",
        "",
        &format!("kdi_compat = true\ntransform_sql = \"\"\"{BRONZE_SQL}\"\"\""),
    );
    let mut p = Pipeline::open_with(resolve(&toml, true), None)
        .await
        .unwrap();
    for _ in 0..1000 {
        if p.step().await.unwrap() == StepOutcome::CaughtUp {
            break;
        }
    }
    drop(p);

    let mut rows = Vec::new();
    for b in scan(&f.target).await {
        let col = |n: &str| {
            b.column_by_name(n)
                .unwrap_or_else(|| panic!("column {n}"))
                .clone()
        };
        for i in 0..b.num_rows() {
            rows.push((
                text(col("messageid").as_ref(), i).unwrap(),
                text(col("correlationid").as_ref(), i),
                text(col("ingest_date").as_ref(), i).unwrap(),
                text(col("data").as_ref(), i).unwrap(),
                text(col("kafka_offset").as_ref(), i).unwrap(),
                text(col("messagetime").as_ref(), i).unwrap(),
            ));
        }
        assert_eq!(b.num_columns(), 12, "{:?}", b.schema());
    }
    rows.sort();
    assert_eq!(rows.len(), 2, "{rows:?}");

    // The partition value is the UTC date of the producer's time, whatever Kafka's clock said.
    assert_eq!(rows[0].2, "2026-10-13");
    assert_eq!(rows[1].2, "2026-10-14");
    // A union's null is a NULL, never the text "null": an alias of the SQL is computed, and a computed null is NULL.
    assert_eq!(rows[0].1.as_deref(), Some("c-1"));
    assert_eq!(rows[1].1, None);
    assert!(
        rows[0].5.starts_with("2026-10-13T23:59:59.999"),
        "{}",
        rows[0].5
    );

    // `data` is kdi's text: keys sorted at every depth, no blanks, a decimal as its two's
    // complement bytes, a null as null, a number kept a number.
    assert_eq!(
        rows[0].3,
        r#"{"additionalFees":"5.00","attributes":{"a":"1","b":"2"},"captureGrossAmount":{"amount":[1,44],"currency":"EUR"},"createdAt":1791935999999,"orderId":"o-m-1","tags":["x","y"]}"#
    );
    assert_eq!(
        rows[1].3,
        r#"{"additionalFees":null,"attributes":{"a":"1","b":"2"},"captureGrossAmount":{"amount":[1,44],"currency":"EUR"},"createdAt":1791936000000,"orderId":"o-m-2","tags":["x","y"]}"#
    );
}
