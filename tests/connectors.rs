//! Bookmark and per-record behaviour, against connectors registered in this
//! process: faucet's registry is global, so these live apart from `plugin.rs`.

use std::collections::HashMap;
use std::sync::{Mutex, Once};

use faucet_cli::registry::PluginRegistry;
use faucet_core::{async_trait, FaucetError, Sink, Source, Value};
use mq_bridge::errors::{ConsumerError, PublisherError};
use mq_bridge::traits::{CustomEndpointFactory, MessageDisposition};
use mq_bridge::{CanonicalMessage, SentBatch};
use mq_bridge_faucet::FaucetFactory;
use serde_json::json;

/// Emits `{"n": start+1}` up to `{"n": 5}` and bookmarks the last number.
struct Counter {
    start: Mutex<u64>,
}

#[async_trait]
impl Source for Counter {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let start = *self.start.lock().unwrap();
        Ok((start + 1..=5).map(|n| json!({"n": n})).collect())
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        Ok((self.fetch_with_context(context).await?, Some(json!(5))))
    }

    fn state_key(&self) -> Option<String> {
        Some("counter".to_owned())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        *self.start.lock().unwrap() = bookmark.as_u64().unwrap_or(0);
        Ok(())
    }
}

/// Refuses records that carry `"bad": true`.
struct Picky;

#[async_trait]
impl Sink for Picky {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<Result<(), FaucetError>>, FaucetError> {
        Ok(records
            .iter()
            .map(|record| match record["bad"].as_bool() {
                Some(true) => Err(FaucetError::Config("bad record".into())),
                _ => Ok(()),
            })
            .collect())
    }
}

fn install() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        PluginRegistry::with_builtins()
            .register_source("counter", |_| {
                Ok(Box::new(Counter {
                    start: Mutex::new(0),
                }))
            })
            .register_sink("picky", |_| Ok(Box::new(Picky)))
            .install()
            .expect("install the test connectors");
    });
}

/// Reads the source to its end in batches of two; `ack` decides each batch's fate by its index.
async fn drain(state: &std::path::Path, ack: impl Fn(usize) -> bool) -> Vec<Value> {
    let mut consumer = FaucetFactory
        .create_consumer(
            "route",
            &json!({"connector": "counter", "state_path": state, "page_size": 3}),
        )
        .await
        .expect("create the consumer");
    let mut records = Vec::new();
    for index in 0.. {
        let batch = match consumer.receive_batch(2).await {
            Ok(batch) => batch,
            Err(ConsumerError::EndOfStream) => break,
            Err(error) => panic!("the source failed: {error}"),
        };
        assert!(batch.messages.len() <= 2);
        let disposition = if ack(index) {
            MessageDisposition::Ack
        } else {
            MessageDisposition::Nack
        };
        let dispositions = vec![disposition; batch.messages.len()];
        for message in &batch.messages {
            records.push(serde_json::from_slice(&message.payload).expect("a JSON record"));
        }
        (batch.commit)(dispositions).await.expect("commit");
    }
    records
}

#[tokio::test]
async fn a_committed_run_resumes_after_its_bookmark_and_a_nacked_one_starts_over() {
    install();
    let state = tempfile::tempdir().expect("a temp dir");
    let all: Vec<Value> = (1..=5).map(|n| json!({"n": n})).collect();

    assert_eq!(drain(state.path(), |_| false).await, all);
    assert_eq!(drain(state.path(), |_| true).await, all);
    assert_eq!(drain(state.path(), |_| true).await, Vec::<Value>::new());
}

#[tokio::test]
async fn a_nack_early_in_a_page_keeps_the_bookmark_its_later_batches_carry() {
    install();
    let state = tempfile::tempdir().expect("a temp dir");
    let all: Vec<Value> = (1..=5).map(|n| json!({"n": n})).collect();

    assert_eq!(drain(state.path(), |index| index != 0).await, all);
    assert_eq!(drain(state.path(), |_| true).await, all);
}

#[tokio::test]
async fn only_the_refused_and_the_unparsable_records_fail() {
    install();
    let publisher = FaucetFactory
        .create_publisher("route", &json!({"connector": "picky"}))
        .await
        .expect("create the publisher");
    let batch = vec![
        CanonicalMessage::from(json!({"id": 1}).to_string()),
        CanonicalMessage::from("not json"),
        CanonicalMessage::from(json!({"id": 3, "bad": true}).to_string()),
        CanonicalMessage::from(json!({"id": 4}).to_string()),
    ];
    let SentBatch::Partial { failed, .. } = publisher.send_batch(batch).await.expect("send") else {
        panic!("two records were refused");
    };
    let failed: Vec<Option<Value>> = failed
        .iter()
        .map(|(message, error)| {
            assert!(matches!(error, PublisherError::NonRetryable(_)));
            serde_json::from_slice(&message.payload).ok()
        })
        .collect();
    assert_eq!(failed, [None, Some(json!({"id": 3, "bad": true}))]);
}
