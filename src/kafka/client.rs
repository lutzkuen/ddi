//! The librdkafka client a Kafka pipeline reads with (docs/kafka-design.md §2.4, §3.1).
//!
//! Its settings are assembled by [`ResolvedKafka::client_settings`], a pure function of the
//! config and the environment that config resolution already tests: ddi's defaults, kdi's
//! credential variables, the settings tables, `settings_env`, then the keys ddi forces. Here
//! they become an rdkafka `ClientConfig`, logged with every secret redacted, and errors that
//! come back from consuming are sorted into the classes of §3.6.

use std::sync::Arc;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::types::RDKafkaErrorCode;
use tracing::info;

use crate::config::kafka::{KafkaSettings, ResolvedKafka};
use crate::error::{Error, KafkaClientFailure, Result};
use crate::kafka::context::DdiContext;

/// The consumer a Kafka pipeline reads with.
pub(crate) type Consumer = StreamConsumer<DdiContext>;

/// The environment, as [`ResolvedKafka::client_settings`] reads it at open.
pub(crate) fn env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// The pipeline's librdkafka settings (§2.4), read from the environment now.
pub(crate) fn settings(name: &str, kafka: &ResolvedKafka) -> Result<KafkaSettings> {
    kafka.client_settings(name, &env)
}

fn client_config(settings: &KafkaSettings, without: &[&str]) -> ClientConfig {
    let mut config = ClientConfig::new();
    for (key, value) in settings.iter() {
        if !without.contains(&key) {
            config.set(key, value);
        }
    }
    config
}

/// The group's consumer for pipeline `name`, with `context` answering its callbacks.
///
/// Creating it reaches no broker: a cluster that cannot be reached shows on the first
/// metadata request.
pub(crate) fn consumer(
    name: &str,
    settings: &KafkaSettings,
    context: DdiContext,
) -> Result<Arc<Consumer>> {
    info!(
        pipeline = name,
        settings = ?settings,
        "librdkafka settings (secrets redacted)"
    );
    client_config(settings, &[])
        .create_with_context(context)
        .map(Arc::new)
        .map_err(|e| Error::KafkaClient {
            failure: KafkaClientFailure::Config,
            detail: format!("cannot create the consumer of pipeline {name:?}: {e}"),
        })
}

/// A consumer that belongs to no group, for reading metadata and watermarks only: without a
/// `group.id` librdkafka does not redirect its main queue, and it never joins anything, so
/// `ddi status` cannot disturb a running group (rdkafka base_consumer.rs:85-95).
pub(crate) fn group_less(name: &str, settings: &KafkaSettings) -> Result<BaseConsumer> {
    client_config(settings, &["group.id", "group.protocol"])
        .create()
        .map_err(|e| Error::KafkaClient {
            failure: KafkaClientFailure::Config,
            detail: format!("cannot create a consumer for pipeline {name:?}: {e}"),
        })
}

/// What an error from librdkafka is to a Kafka pipeline (§3.6).
///
/// - librdkafka's own fatal error, and a fenced static member: [`KafkaClientFailure::Fatal`];
/// - a failed authentication, a missing authorisation, a topic that does not exist: facts about
///   the configuration, [`KafkaClientFailure::Config`];
/// - anything else, a broker down or a timeout above all: [`KafkaClientFailure::Transport`],
///   which librdkafka retries by itself.
pub(crate) fn classify(e: &KafkaError, what: &str) -> Error {
    use RDKafkaErrorCode as C;
    let failure = match (e, e.rdkafka_error_code()) {
        (KafkaError::MessageConsumptionFatal(_), _) => KafkaClientFailure::Fatal,
        (_, Some(C::Fatal | C::FencedInstanceId | C::Fenced)) => KafkaClientFailure::Fatal,
        (
            _,
            Some(
                C::Authentication
                | C::SaslAuthenticationFailed
                | C::TopicAuthorizationFailed
                | C::GroupAuthorizationFailed
                | C::ClusterAuthorizationFailed
                | C::UnknownTopicOrPartition
                | C::UnknownTopic,
            ),
        ) => KafkaClientFailure::Config,
        _ => KafkaClientFailure::Transport,
    };
    Error::KafkaClient {
        failure,
        detail: format!("{what}: {e}"),
    }
}

/// Whether a consumption error ends the consumer, rather than being one librdkafka retries.
pub(crate) fn ends_the_consumer(e: &KafkaError) -> bool {
    !matches!(
        classify(e, ""),
        Error::KafkaClient {
            failure: KafkaClientFailure::Transport,
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn consumption_errors_are_classified_by_code() {
        let class = |e: KafkaError| match classify(&e, "consuming") {
            Error::KafkaClient { failure, .. } => failure,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            class(KafkaError::MessageConsumption(
                RDKafkaErrorCode::AllBrokersDown
            )),
            KafkaClientFailure::Transport
        );
        assert_eq!(
            class(KafkaError::MessageConsumption(
                RDKafkaErrorCode::TopicAuthorizationFailed
            )),
            KafkaClientFailure::Config
        );
        assert_eq!(
            class(KafkaError::MessageConsumption(
                RDKafkaErrorCode::UnknownTopicOrPartition
            )),
            KafkaClientFailure::Config
        );
        assert_eq!(
            class(KafkaError::MessageConsumptionFatal(
                RDKafkaErrorCode::InvalidRecord
            )),
            KafkaClientFailure::Fatal
        );
        assert_eq!(
            class(KafkaError::MessageConsumption(
                RDKafkaErrorCode::FencedInstanceId
            )),
            KafkaClientFailure::Fatal
        );
        assert!(!ends_the_consumer(&KafkaError::MessageConsumption(
            RDKafkaErrorCode::BrokerTransportFailure
        )));
    }
}
