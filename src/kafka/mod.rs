//! The Kafka source, compiled only with the `kafka` feature (docs/kafka-design.md).
//!
//! The consumer itself lands in M3. What is here now:
//! - [`registry`]: the Schema Registry client (§5.2);
//! - [`decode`]: Confluent-framed Avro into Arrow, one decoder per writer schema, with every
//!   message checked to be exactly one row and a malformed one isolated (§5.1, §5.4, §6.1);
//! - [`conform`]: the union `U` the SQL path aligns its batches to (§5.5), and the conform
//!   step that fits a batch to the target by name before the coercer (§5.6);
//! - `kdi_text`: kafka-delta-ingest's text for values going into STRING columns, which the
//!   conform step writes under `kdi_compat` and `kdi_null_text` (§5.6 steps 5 and 6);
//! - [`KafkaSource`], what a Kafka pipeline's `process` needs of its source: so far the
//!   conform step;
//! - what the build has to prove: that librdkafka is the version the design was checked
//!   against, and that it was built with what ddi's clusters need (TLS, SCRAM, zstd).
//!
//! All of it is pure: no consumer, no Delta commit. Configuration is not here: it is always
//! compiled, in `config::kafka`, so a build without this feature can still read and validate
//! it, and hold its pipelines back by name.

pub mod conform;
pub mod decode;
pub(crate) mod kdi_text;
pub mod registry;
#[cfg(test)]
pub(crate) mod sr_stub;

/// A Kafka pipeline's source, as `Pipeline::process` sees it (docs/kafka-design.md §1.2).
///
/// M3 adds the consumer, the partitions' state and the decoder; what is here is what a batch
/// needs on its way to the target: the conform step its rows go through before the coercer
/// (§5.6).
#[derive(Debug)]
pub(crate) struct KafkaSource {
    pub(crate) conformer: conform::Conformer,
}

/// The librdkafka this binary was linked against, as librdkafka spells it (`"2.12.1"`).
pub fn librdkafka_version() -> String {
    rdkafka::util::get_rdkafka_version().1
}

/// What this librdkafka was built with: its `builtin.features`, e.g. `ssl`, `sasl_scram`,
/// `zstd`.
pub fn builtin_features() -> crate::Result<Vec<String>> {
    let native = rdkafka::config::ClientConfig::new()
        .create_native_config()
        .map_err(|e| crate::Error::Other(format!("librdkafka: {e}")))?;
    let features = native
        .get("builtin.features")
        .map_err(|e| crate::Error::Other(format!("librdkafka: builtin.features: {e}")))?;
    Ok(features.split(',').map(|f| f.trim().to_string()).collect())
}

/// What a ddi deployment needs from librdkafka: TLS, SASL/SCRAM over it (kdi's
/// `KAFKA_DELTA_INGEST_SCRAM_JSON`), and zstd-compressed topics (§3.1).
pub const REQUIRED_FEATURES: [&str; 3] = ["ssl", "sasl_scram", "zstd"];

#[cfg(test)]
mod tests {
    use super::*;

    /// The version every librdkafka reference in docs/kafka-design.md was checked against.
    /// A bump of rdkafka-sys must revisit those, so it fails here first.
    #[test]
    fn librdkafka_is_the_version_the_design_was_checked_against() {
        assert_eq!(librdkafka_version(), "2.12.1");
    }

    /// A regression check on the build, not an experiment (§8): the C build is configured
    /// by cargo features, and losing `ssl` or `zstd` would otherwise surface only when a
    /// consumer met a SASL_SSL cluster or a zstd batch.
    #[test]
    fn librdkafka_has_ssl_sasl_scram_and_zstd() {
        let features = builtin_features().unwrap();
        for f in REQUIRED_FEATURES {
            assert!(
                features.iter().any(|x| x == f),
                "{f} missing from {features:?}"
            );
        }
        // No libsasl2 in the build, so no GSSAPI; config refuses it (§2.5 rule 9).
        assert!(!features.iter().any(|x| x == "sasl_gssapi"), "{features:?}");
    }

    /// arrow-avro must speak deltalake's arrow, not a second copy: this is a type error, not
    /// a test failure, the day the two drift apart.
    #[test]
    fn arrow_avro_shares_deltalakes_arrow() {
        use deltalake::arrow::datatypes::{DataType, Field, Schema};
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let avro = arrow_avro::schema::AvroSchema::try_from(&schema).unwrap();
        assert!(avro.json_string.contains("\"id\""), "{}", avro.json_string);
    }
}
