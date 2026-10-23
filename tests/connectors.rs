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

/// Commit tokens of the `ledger` sink, per scope: the token and the rows written under it.
static LEDGER: Mutex<Option<HashMap<String, (String, usize)>>> = Mutex::new(None);

/// Commits a token with its rows; a write with a `"fail": true` record fails whole,
/// and a `"lost": true` record is silently not written.
struct Ledger;

#[async_trait]
#[allow(deprecated)]
impl Sink for Ledger {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        if records.iter().any(|record| record["fail"] == true) {
            return Err(FaucetError::Sink("connection reset".into()));
        }
        let mut ledger = LEDGER.lock().unwrap();
        let entry = ledger
            .get_or_insert_default()
            .entry(scope.into())
            .or_default();
        let written = records
            .iter()
            .filter(|record| record["lost"] != true)
            .count();
        *entry = (token.into(), entry.1 + written);
        Ok(written)
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let ledger = LEDGER.lock().unwrap();
        Ok(ledger
            .as_ref()
            .and_then(|ledger| ledger.get(scope))
            .map(|(token, _)| token.clone()))
    }
}

/// Buffers nothing and fails every flush.
struct Leaky;

#[async_trait]
impl Sink for Leaky {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        Ok(records.len())
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        Err(FaucetError::Sink("disk full".into()))
    }
}

fn committed(scope: &str) -> Option<(String, usize)> {
    LEDGER
        .lock()
        .unwrap()
        .as_ref()
        .and_then(|ledger| ledger.get(scope).cloned())
}

fn at(payload: impl Into<String>, partition: u32, offset: u64) -> CanonicalMessage {
    CanonicalMessage::from(payload.into())
        .with_metadata_kv("mqb.src.kafka_topic", "t")
        .with_metadata_kv("mqb.src.kafka_partition", partition.to_string())
        .with_metadata_kv("mqb.src.kafka_offset", offset.to_string())
}

async fn exactly_once(scope: &str) -> Box<dyn mq_bridge::traits::MessagePublisher> {
    FaucetFactory
        .create_publisher(
            "route",
            &json!({"connector": "ledger", "exactly_once": true, "state_key": scope}),
        )
        .await
        .expect("create the publisher")
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
            .register_sink("ledger", |_| Ok(Box::new(Ledger)))
            .register_sink("leaky", |_| Ok(Box::new(Leaky)))
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

#[tokio::test]
async fn exactly_once_fails_only_the_partition_whose_write_failed() {
    install();
    let publisher = exactly_once("split").await;
    let batch = vec![
        at(json!({"id": 1}).to_string(), 0, 7),
        at(json!({"id": 2, "fail": true}).to_string(), 1, 3),
        at(json!({"id": 3}).to_string(), 0, 8),
    ];
    let SentBatch::Partial { failed, .. } = publisher.send_batch(batch).await.expect("send") else {
        panic!("partition 1 failed");
    };
    assert_eq!(failed.len(), 1);
    assert!(matches!(failed[0].1, PublisherError::Retryable(_)));
    assert_eq!(committed("split::t::0").map(|(_, rows)| rows), Some(2));
    assert_eq!(committed("split::t::1"), None);
}

#[tokio::test]
async fn a_replayed_unparsable_message_is_rejected_again() {
    install();
    let batch = || vec![at(json!({"id": 1}).to_string(), 0, 1), at("not json", 0, 2)];
    for _run in 0..2 {
        let publisher = exactly_once("replay").await;
        let SentBatch::Partial { failed, .. } = publisher.send_batch(batch()).await.expect("send")
        else {
            panic!("the unparsable message fails");
        };
        assert_eq!(failed.len(), 1);
        assert_eq!(&failed[0].0.payload[..], b"not json");
        assert!(matches!(failed[0].1, PublisherError::NonRetryable(_)));
    }
    assert_eq!(committed("replay::t::0").map(|(_, rows)| rows), Some(1));
}

#[tokio::test]
async fn exactly_once_refuses_a_check_that_quarantines() {
    install();
    let config = json!({
        "connector": "ledger",
        "exactly_once": true,
        "quality": {"record": [{"type": "not_null", "field": "id", "on_failure": "quarantine"}]},
    });
    let error = FaucetFactory
        .create_publisher("route", &config)
        .await
        .err()
        .expect("refused");
    assert!(error.to_string().contains("quarantine"), "{error:#}");
}

#[tokio::test]
async fn exactly_once_holds_later_batches_until_a_failed_one_is_sent_again() {
    install();
    let publisher = exactly_once("gap").await;
    let send = |batch: Vec<CanonicalMessage>| publisher.send_batch(batch);
    let retried = |sent: SentBatch| match sent {
        SentBatch::Partial { failed, .. } => failed
            .iter()
            .all(|(_, error)| matches!(error, PublisherError::Retryable(_))),
        _ => false,
    };

    let failing = vec![at(json!({"id": 1, "fail": true}).to_string(), 0, 1)];
    assert!(retried(send(failing).await.expect("send")));
    let later = || vec![at(json!({"id": 2}).to_string(), 0, 2)];
    assert!(retried(send(later()).await.expect("send")));
    assert_eq!(committed("gap::t::0"), None);

    let again = vec![at(json!({"id": 1}).to_string(), 0, 1)];
    assert!(matches!(send(again).await.expect("send"), SentBatch::Ack));
    assert!(matches!(send(later()).await.expect("send"), SentBatch::Ack));
    assert_eq!(committed("gap::t::0").map(|(_, rows)| rows), Some(2));
}

#[tokio::test]
async fn a_batch_below_the_failed_position_keeps_later_ones_waiting() {
    install();
    let publisher = exactly_once("below").await;
    let row = |id: u64, offset: u64| at(json!({"id": id}).to_string(), 0, offset);
    let failing = vec![at(json!({"id": 2, "fail": true}).to_string(), 0, 2)];
    assert!(matches!(
        publisher.send_batch(failing).await.expect("send"),
        SentBatch::Partial { .. }
    ));
    assert!(matches!(
        publisher.send_batch(vec![row(1, 1)]).await.expect("send"),
        SentBatch::Ack
    ));
    let SentBatch::Partial { failed, .. } =
        publisher.send_batch(vec![row(3, 3)]).await.expect("send")
    else {
        panic!("position 3 waits for position 2");
    };
    assert!(matches!(failed[0].1, PublisherError::Retryable(_)));
    assert_eq!(committed("below::t::0").map(|(_, rows)| rows), Some(1));
}

#[tokio::test]
async fn every_failed_position_is_waited_for_until_it_is_sent_again() {
    install();
    let publisher = exactly_once("gaps").await;
    let row = |offset: u64| at(json!({"id": offset}).to_string(), 0, offset);
    let failing = |offset: u64| {
        vec![at(
            json!({"id": offset, "fail": true}).to_string(),
            0,
            offset,
        )]
    };
    let send = |batch: Vec<CanonicalMessage>| publisher.send_batch(batch);

    assert!(matches!(
        send(failing(5)).await.expect("send"),
        SentBatch::Partial { .. }
    ));
    assert!(matches!(
        send(failing(3)).await.expect("send"),
        SentBatch::Partial { .. }
    ));
    assert!(matches!(
        send(vec![row(3)]).await.expect("send"),
        SentBatch::Ack
    ));
    let SentBatch::Partial { failed, .. } = send(vec![row(6)]).await.expect("send") else {
        panic!("position 6 waits for position 5");
    };
    assert!(matches!(failed[0].1, PublisherError::Retryable(_)));
    assert_eq!(committed("gaps::t::0").map(|(_, rows)| rows), Some(1));

    assert!(matches!(
        send(vec![row(5)]).await.expect("send"),
        SentBatch::Ack
    ));
    assert!(matches!(
        send(vec![row(6)]).await.expect("send"),
        SentBatch::Ack
    ));
    assert_eq!(committed("gaps::t::0").map(|(_, rows)| rows), Some(3));
}

#[tokio::test]
async fn rows_the_sink_did_not_write_go_to_the_error_handler() {
    install();
    let publisher = exactly_once("lost").await;
    let batch = vec![
        at(json!({"id": 1}).to_string(), 0, 1),
        at(json!({"id": 2, "lost": true}).to_string(), 0, 2),
    ];
    let SentBatch::Partial { failed, .. } = publisher.send_batch(batch).await.expect("send") else {
        panic!("the sink lost a row");
    };
    assert!(failed
        .iter()
        .all(|(_, error)| matches!(error, PublisherError::NonRetryable(_))));
}

#[tokio::test]
async fn a_failed_periodic_flush_fails_the_routes_next_flush() {
    install();
    let publisher = FaucetFactory
        .create_publisher(
            "route",
            &json!({"connector": "leaky", "flush": "periodic", "flush_interval_ms": 1}),
        )
        .await
        .expect("create the publisher");
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let error = publisher
        .flush()
        .await
        .expect_err("the periodic flush failed");
    assert!(error.to_string().contains("periodic"), "{error:#}");
}
