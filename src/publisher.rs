use std::any::Any;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use faucet_cli::registry;
use faucet_core::{FaucetError, Sink};
use mq_bridge::errors::PublisherError;
use mq_bridge::traits::MessagePublisher;
use mq_bridge::{CanonicalMessage, SentBatch};
use serde_json::Value;

use crate::config::{invalid, unknown_connector, FaucetConfig};

struct FaucetPublisher {
    sink: Box<dyn Sink>,
}

pub(crate) async fn create(
    route_name: &str,
    value: &Value,
) -> anyhow::Result<Box<dyn MessagePublisher>> {
    let config = FaucetConfig::parse(value)?;
    let kind = config.connector.as_str();
    if !registry::sink_exists(kind) {
        return Err(unknown_connector("sink", kind, registry::sink_kinds()).into());
    }
    registry::validate_sink_config(kind, route_name, config.config.clone()).map_err(invalid)?;
    let auth = config.auth_catalog()?;
    let sink = registry::build_sink(kind, config.config.clone(), &auth)
        .await
        .with_context(|| format!("failed to create the faucet `{kind}` sink"))?;
    Ok(Box::new(FaucetPublisher { sink }))
}

#[async_trait]
impl MessagePublisher for FaucetPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let mut records = Vec::with_capacity(messages.len());
        let mut outcomes: Vec<Option<PublisherError>> = Vec::with_capacity(messages.len());
        for message in &messages {
            match serde_json::from_slice::<Value>(&message.payload) {
                Ok(record) => {
                    records.push(record);
                    outcomes.push(None);
                }
                Err(error) => outcomes.push(Some(PublisherError::NonRetryable(anyhow!(
                    "a faucet sink takes JSON records: {error}"
                )))),
            }
        }

        let mut written = self
            .sink
            .write_batch_partial(&records)
            .await
            .map_err(publisher_error)?
            .into_iter();
        // Several sinks only buffer in `write_batch`; an ack must mean written.
        self.sink.flush().await.map_err(publisher_error)?;

        for outcome in outcomes.iter_mut().filter(|outcome| outcome.is_none()) {
            *outcome = match written.next() {
                Some(Ok(())) => None,
                Some(Err(error)) => Some(publisher_error(error)),
                None => Some(PublisherError::Retryable(anyhow!(
                    "the faucet sink reported fewer outcomes than records"
                ))),
            };
        }
        SentBatch::from_outcomes(messages, outcomes)
    }

    async fn flush(&self) -> anyhow::Result<()> {
        self.sink.flush().await.map_err(anyhow::Error::new)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Retries unless the record or the configuration is at fault. faucet reports
/// most connector failures as plain strings, so the unknown ones count as transient.
fn publisher_error(error: FaucetError) -> PublisherError {
    let permanent = match &error {
        FaucetError::HttpStatus { status, .. } => {
            (400..500).contains(status) && !matches!(*status, 408 | 429)
        }
        FaucetError::Json(_)
        | FaucetError::JsonPath(_)
        | FaucetError::Url(_)
        | FaucetError::Transform(_)
        | FaucetError::Config(_)
        | FaucetError::QualityFailure { .. }
        | FaucetError::SchemaDrift { .. }
        | FaucetError::PolicyViolation { .. }
        | FaucetError::ContractViolation { .. } => true,
        _ => false,
    };
    let error = anyhow::Error::new(error);
    if permanent {
        PublisherError::NonRetryable(error)
    } else {
        PublisherError::Retryable(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bad_record_is_not_retried_and_an_unknown_sink_failure_is() {
        let rejected = publisher_error(FaucetError::Config("no such column".into()));
        assert!(matches!(rejected, PublisherError::NonRetryable(_)));
        let dropped = publisher_error(FaucetError::Sink("connection reset".into()));
        assert!(matches!(dropped, PublisherError::Retryable(_)));
    }
}
