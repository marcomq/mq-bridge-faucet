use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use faucet_core::state::validate_state_key;
use faucet_core::{FaucetError, FileStateStore, Source, StateStore, StreamPage};
use faucet_registry as registry;
use futures::StreamExt;
use mq_bridge::canonical_message::message_id_from_str;
use mq_bridge::errors::{ConsumerError, InvalidConfig};
use mq_bridge::traits::{BatchCommitFunc, EndpointStatus, MessageConsumer, MessageDisposition};
use mq_bridge::{CanonicalMessage, ReceivedBatch};
use serde_json::{Map, Value};
use tokio::sync::mpsc;

use crate::config::{FaucetConfig, invalid, unknown_connector};

type PageResult = Result<StreamPage, FaucetError>;

/// Pages read ahead of the route, as faucet's own source-to-sink channel holds.
const PAGES_AHEAD: usize = 4;

/// A source whose records name their partition and offset, and whose bookmark
/// lists the next offset to read per partition. A nack can then hold back its
/// own partition instead of the whole run.
struct PartitionedBookmark {
    /// The bookmark's array of per-partition entries.
    entries: &'static str,
    /// The fields naming a partition, in records and bookmark entries alike.
    partition: &'static [&'static str],
    /// A record's offset; in a bookmark entry, the next offset to read.
    offset: &'static str,
}

fn partitioned_bookmark(kind: &str) -> Option<&'static PartitionedBookmark> {
    match kind {
        "kafka" => Some(&PartitionedBookmark {
            entries: "partition_offsets",
            partition: &["topic", "partition"],
            offset: "offset",
        }),
        _ => None,
    }
}

/// Where a record sits in a [`PartitionedBookmark`] source.
#[derive(Clone)]
struct Position {
    partition: Map<String, Value>,
    offset: i64,
}

impl Position {
    fn to_json(&self) -> Value {
        let mut json = self.partition.clone();
        json.insert("offset".into(), self.offset.into());
        Value::Object(json)
    }
}

struct Checkpoint {
    store: FileStateStore,
    key: String,
    /// A bookmark covers every record before it, so after a nack none may be stored this run.
    nacked: AtomicBool,
    /// Set when the source's bookmark is per partition; then `nacked` stays unset.
    partitioned: Option<&'static PartitionedBookmark>,
    /// The lowest nacked position per partition, keyed by the partition's JSON.
    /// Every later bookmark keeps that partition there.
    held: Mutex<HashMap<String, Position>>,
}

struct FaucetConsumer {
    connector: String,
    pages: mpsc::Receiver<PageResult>,
    /// Records of the current page not handed out yet.
    pending: VecDeque<Value>,
    /// Stored once the batch that empties `pending` is committed.
    bookmark: Option<Value>,
    checkpoint: Option<Arc<Checkpoint>>,
    exit_on_empty: bool,
    idle_timeout: Option<Duration>,
    /// The source failed; its page stream is gone, so every later receive fails too.
    /// The flag is whether the failure is permanent, see [`is_permanent`].
    failed: Option<(String, bool)>,
}

pub(crate) async fn create(
    route_name: &str,
    value: &Value,
) -> anyhow::Result<Box<dyn MessageConsumer>> {
    let config = FaucetConfig::parse(value)?;
    let kind = config.connector.as_str();
    if !registry::source_exists(kind) {
        return Err(unknown_connector("source", kind, registry::source_kinds()).into());
    }
    registry::validate_source_config(kind, route_name, config.config.clone()).map_err(invalid)?;
    check_idle_timeout(&config)?;
    let auth = config.auth_catalog()?;
    let source = registry::build_source_with(
        kind,
        config.config.clone(),
        &auth,
        None,
        &registry::BuildPolicy::trusted(),
    )
    .await
    .with_context(|| format!("failed to create the faucet `{kind}` source"))?;

    let checkpoint = match &config.state_path {
        Some(path) => Some(Arc::new(
            resume(source.as_ref(), path.clone(), &config, route_name).await?,
        )),
        None => None,
    };
    Ok(Box::new(FaucetConsumer {
        connector: kind.to_owned(),
        pages: spawn_pages(source, config.page_size),
        pending: VecDeque::new(),
        bookmark: None,
        checkpoint,
        exit_on_empty: false,
        idle_timeout: config.idle_timeout_ms.map(Duration::from_millis),
        failed: None,
    }))
}

/// The setting with which a source ends its run by itself, after a quiet spell
/// or a collection window, and its default in seconds. Such a source holds back
/// its last, partial page (webhook: every page) until that wait is over.
fn source_idle_wait(kind: &str) -> Option<(&'static str, Option<u64>)> {
    Some(match kind {
        "webhook" => ("timeout_secs", Some(30)),
        "nats" | "rabbitmq" | "sqs" => ("idle_timeout_secs", None),
        "kinesis" | "pubsub" | "dynamodb" => ("idle_termination_secs", None),
        "kafka" | "websocket" => ("idle_timeout", None),
        "postgres-cdc" | "mysql-cdc" | "mongodb-cdc" | "mssql-cdc" | "oracle-cdc" => {
            ("idle_timeout", Some(30))
        }
        _ => return None,
    })
}

/// A drain that gives up before the source's own idle wait ends would stop
/// before the last page arrives, report success, and leave those records unread.
fn check_idle_timeout(config: &FaucetConfig) -> Result<(), InvalidConfig> {
    let Some(idle_ms) = config.idle_timeout_ms else {
        return Ok(());
    };
    let Some((field, default)) = source_idle_wait(&config.connector) else {
        return Ok(());
    };
    let Some(secs) = config.config.get(field).and_then(Value::as_u64).or(default) else {
        return Ok(());
    };
    if idle_ms > secs.saturating_mul(1000) {
        return Ok(());
    }
    Err(InvalidConfig(anyhow!(
        "`idle_timeout_ms` ({idle_ms}) must be longer than the `{}` source's `{field}` \
         ({secs} s): the source holds back its last page until that wait is over, so the \
         drain would end first. Leave `idle_timeout_ms` unset: the source ends the run itself",
        config.connector
    )))
}

/// Opens the state store and hands the source the bookmark of the last run.
async fn resume(
    source: &dyn Source,
    path: std::path::PathBuf,
    config: &FaucetConfig,
    route_name: &str,
) -> anyhow::Result<Checkpoint> {
    let key = config
        .state_key
        .clone()
        .or_else(|| source.state_key())
        .unwrap_or_else(|| state_key_from(route_name));
    validate_state_key(&key).map_err(invalid)?;
    let store = FileStateStore::new(path);
    if let Some(bookmark) = store
        .get(&key)
        .await
        .context("failed to read the faucet bookmark")?
    {
        source
            .apply_start_bookmark(bookmark)
            .await
            .context("the faucet source rejected its stored bookmark")?;
    }
    Ok(Checkpoint {
        store,
        key,
        nacked: AtomicBool::new(false),
        partitioned: partitioned_bookmark(&config.connector),
        held: Mutex::default(),
    })
}

fn state_key_from(route_name: &str) -> String {
    let name: String = route_name
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '-' | '.' => c,
            _ => '_',
        })
        .collect();
    format!("mq-bridge.{name}")
}

/// Runs the source on its own task: `stream_pages` borrows the source, so the
/// stream cannot be stored next to it. Stops when the consumer is dropped.
fn spawn_pages(source: Box<dyn Source>, page_size: usize) -> mpsc::Receiver<PageResult> {
    let (tx, rx) = mpsc::channel(PAGES_AHEAD);
    tokio::spawn(async move {
        let context = HashMap::new();
        let mut pages = source.stream_pages(&context, page_size);
        loop {
            let page = tokio::select! {
                _ = tx.closed() => break,
                page = pages.next() => page,
            };
            let Some(page) = page else { break };
            let failed = page.is_err();
            if tx.send(page).await.is_err() || failed {
                break;
            }
        }
    });
    rx
}

impl FaucetConsumer {
    /// `None` when a draining route's source stayed quiet for `idle_timeout`.
    async fn next_page(&mut self) -> Result<Option<StreamPage>, ConsumerError> {
        if let Some((message, permanent)) = &self.failed {
            return Err(source_error(*permanent, anyhow!(message.clone())));
        }
        let received = match self.idle_timeout.filter(|_| self.exit_on_empty) {
            Some(wait) => match tokio::time::timeout(wait, self.pages.recv()).await {
                Ok(received) => received,
                Err(_) => return Ok(None),
            },
            None => self.pages.recv().await,
        };
        match received {
            None => Err(ConsumerError::EndOfStream),
            Some(Ok(page)) => Ok(Some(page)),
            Some(Err(error)) => {
                let permanent = is_permanent(&error);
                let mut error = anyhow::Error::new(error);
                if permanent {
                    error = error.context(
                        "the faucet source stopped on a record it cannot use, which a reconnect \
                         would read again. For a decode failure set `on_decode_error: skip` or a \
                         fitting `value_format` in the source's `config`",
                    );
                }
                self.failed = Some((format!("{error:#}"), permanent));
                Err(source_error(permanent, error))
            }
        }
    }
}

/// Errors in the data or the configuration: a reconnect reads the same record
/// and fails again, so the route must stop instead of looping. Other `Source`
/// errors reconnect: connectors report lost connections that way too.
fn is_permanent(error: &FaucetError) -> bool {
    matches!(error, FaucetError::Source(message) if is_decode_failure(message))
        || matches!(
            error,
            FaucetError::Json(_)
                | FaucetError::JsonPath(_)
                | FaucetError::Url(_)
                | FaucetError::Transform(_)
                | FaucetError::Config(_)
                | FaucetError::QualityFailure { .. }
                | FaucetError::SchemaDrift { .. }
                | FaucetError::PolicyViolation { .. }
                | FaucetError::ProfileDrift { .. }
                | FaucetError::ContractViolation { .. }
        )
}

/// faucet-core has no decode variant: connectors report a record they cannot
/// decode as `Source("<connector> <format> decode: <cause>")`, e.g. kafka's
/// `kafka json decode: …` or the schema registry's `avro decode: …`.
fn is_decode_failure(message: &str) -> bool {
    message.contains(" decode: ")
}

fn source_error(permanent: bool, error: anyhow::Error) -> ConsumerError {
    if permanent {
        ConsumerError::Permanent(error)
    } else {
        ConsumerError::Connection(error)
    }
}

/// The same record gets the same id on every read, so `deduplication` can
/// recognise a redelivery. Identical records therefore share an id.
fn record_message(record: &Value) -> CanonicalMessage {
    let payload = serde_json::to_vec(record).expect("a JSON value always serializes");
    let id = std::str::from_utf8(&payload)
        .ok()
        .and_then(|text| message_id_from_str(text).ok());
    CanonicalMessage::new(payload, id)
}

#[async_trait]
impl MessageConsumer for FaucetConsumer {
    fn set_exit_on_empty(&mut self, exit_on_empty: bool) {
        self.exit_on_empty = exit_on_empty;
    }

    async fn receive_batch(&mut self, max_messages: usize) -> Result<ReceivedBatch, ConsumerError> {
        if max_messages == 0 {
            return Ok(ReceivedBatch::empty());
        }
        while self.pending.is_empty() {
            let Some(page) = self.next_page().await? else {
                return Ok(ReceivedBatch::empty());
            };
            // An empty page's bookmark rides on the next records: storing it
            // here could overtake batches that are not committed yet.
            if page.bookmark.is_some() {
                self.bookmark = page.bookmark;
            }
            self.pending = page.records.into();
        }

        let count = max_messages.min(self.pending.len());
        let records: Vec<Value> = self.pending.drain(..count).collect();
        let positions = match self.checkpoint.as_ref().and_then(|c| c.partitioned) {
            Some(scheme) => records.iter().map(|r| scheme.position(r)).collect(),
            None => Vec::new(),
        };
        let messages = records.iter().map(record_message).collect();
        let bookmark = if self.pending.is_empty() {
            self.bookmark.take()
        } else {
            None
        };
        Ok(ReceivedBatch {
            messages,
            commit: commit(self.checkpoint.clone(), bookmark, positions),
        })
    }

    /// `pending` counts the records of the current page not handed out yet.
    async fn status(&self) -> EndpointStatus {
        let mut details = Map::new();
        if let Some(checkpoint) = &self.checkpoint {
            let paused = checkpoint.nacked.load(Ordering::Acquire);
            details.insert("bookmark_paused".into(), paused.into());
            let held = checkpoint.held.lock().expect("held positions lock");
            let held: Vec<Value> = held.values().map(Position::to_json).collect();
            if !held.is_empty() {
                details.insert("held_partitions".into(), held.into());
            }
        }
        EndpointStatus {
            healthy: self.failed.is_none(),
            target: self.connector.clone(),
            pending: Some(self.pending.len()),
            error: self.failed.as_ref().map(|(error, _)| error.clone()),
            details: Value::Object(details),
            ..Default::default()
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PartitionedBookmark {
    fn partition(&self, value: &Value) -> Option<Map<String, Value>> {
        self.partition
            .iter()
            .map(|field| Some(((*field).to_owned(), value.get(*field)?.clone())))
            .collect()
    }

    fn position(&self, record: &Value) -> Option<Position> {
        Some(Position {
            partition: self.partition(record)?,
            offset: record.get(self.offset)?.as_i64()?,
        })
    }

    /// Keeps each held partition of `bookmark` at or below its first nacked
    /// offset, so a restart reads that partition from there and the others move on.
    fn hold_back(&self, bookmark: &mut Value, held: &HashMap<String, Position>) {
        if held.is_empty() {
            return;
        }
        let Some(entries) = bookmark.get_mut(self.entries).and_then(Value::as_array_mut) else {
            return;
        };
        let mut missing: HashMap<&str, &Position> =
            held.iter().map(|(key, p)| (key.as_str(), p)).collect();
        for entry in entries.iter_mut() {
            let Some(partition) = self.partition(entry) else {
                continue;
            };
            let Some(nacked) = missing.remove(partition_key(&partition).as_str()) else {
                continue;
            };
            let offset = &mut entry[self.offset];
            if offset.as_i64().is_none_or(|next| next > nacked.offset) {
                *offset = nacked.offset.into();
            }
        }
        for nacked in missing.into_values() {
            let mut entry = nacked.partition.clone();
            entry.insert(self.offset.to_owned(), nacked.offset.into());
            entries.push(Value::Object(entry));
        }
    }
}

fn partition_key(partition: &Map<String, Value>) -> String {
    serde_json::to_string(partition).expect("a JSON map always serializes")
}

/// Stores the page's bookmark, unless a message of this run was nacked. With a
/// [`PartitionedBookmark`] a nack only holds back its partition. Commits run in
/// receive order, so an earlier batch's nack is seen here.
fn commit(
    checkpoint: Option<Arc<Checkpoint>>,
    bookmark: Option<Value>,
    positions: Vec<Option<Position>>,
) -> BatchCommitFunc {
    Box::new(move |dispositions| {
        Box::pin(async move {
            let Some(checkpoint) = checkpoint else {
                return Ok(());
            };
            for (index, disposition) in dispositions.iter().enumerate() {
                if !matches!(disposition, MessageDisposition::Nack) {
                    continue;
                }
                match positions.get(index).cloned().flatten() {
                    Some(position) => {
                        let mut held = checkpoint.held.lock().unwrap_or_else(|e| e.into_inner());
                        let lowest = held
                            .entry(partition_key(&position.partition))
                            .or_insert_with(|| position.clone());
                        lowest.offset = lowest.offset.min(position.offset);
                    }
                    None => checkpoint.nacked.store(true, Ordering::Release),
                }
            }
            let Some(mut bookmark) = bookmark else {
                return Ok(());
            };
            if checkpoint.nacked.load(Ordering::Acquire) {
                return Ok(());
            }
            if let Some(scheme) = checkpoint.partitioned {
                scheme.hold_back(
                    &mut bookmark,
                    &checkpoint.held.lock().unwrap_or_else(|e| e.into_inner()),
                );
            }
            checkpoint
                .store
                .put(&checkpoint.key, &bookmark)
                .await
                .context("failed to store the faucet bookmark")
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_record_keeps_its_id_across_reads() {
        let record = json!({"topic": "t", "partition": 0, "offset": 7, "value": {"a": 1}});
        let first = record_message(&record);
        assert_eq!(first.message_id, record_message(&record).message_id);
        assert_eq!(first.payload, serde_json::to_vec(&record).unwrap());
        let next = json!({"topic": "t", "partition": 0, "offset": 8, "value": {"a": 1}});
        assert_ne!(first.message_id, record_message(&next).message_id);
    }

    #[test]
    fn data_errors_stop_the_route_and_source_errors_reconnect() {
        let json_error = serde_json::from_str::<Value>("x").unwrap_err();
        assert!(is_permanent(&FaucetError::Json(json_error)));
        assert!(is_permanent(&FaucetError::Transform("bad".into())));
        assert!(!is_permanent(&FaucetError::Source(
            "kafka recv: down".into()
        )));
        assert!(is_permanent(&FaucetError::Source(
            "kafka json decode: expected value at line 1 column 1".into()
        )));
        for upstream in [
            "kafka json decode: expected value",
            "kafka raw_string decode: invalid utf-8",
            "avro decode: unexpected end",
            "protobuf decode: buffer underflow",
            "json-schema decode: invalid type",
        ] {
            assert!(
                is_permanent(&FaucetError::Source(upstream.into())),
                "{upstream}"
            );
        }
        assert!(matches!(
            source_error(true, anyhow!("x")),
            ConsumerError::Permanent(_)
        ));
        assert!(matches!(
            source_error(false, anyhow!("x")),
            ConsumerError::Connection(_)
        ));
    }

    fn offsets(bookmark: &Value) -> Vec<(i64, i64)> {
        bookmark["partition_offsets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| {
                (
                    e["partition"].as_i64().unwrap(),
                    e["offset"].as_i64().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn a_nack_holds_back_only_its_partition() {
        let mut bookmark = json!({"partition_offsets": [
            {"topic": "t", "partition": 0, "offset": 10},
            {"topic": "t", "partition": 1, "offset": 5},
        ]});
        let kafka = partitioned_bookmark("kafka").unwrap();
        let held = [(0, 7), (1, 9), (2, 3)]
            .into_iter()
            .map(|(partition, offset)| {
                let record = json!({"topic": "t", "partition": partition, "offset": offset});
                let position = kafka.position(&record).unwrap();
                (partition_key(&position.partition), position)
            })
            .collect();
        kafka.hold_back(&mut bookmark, &held);
        assert_eq!(offsets(&bookmark), [(0, 7), (1, 5), (2, 3)]);
    }

    #[tokio::test]
    async fn acked_records_before_a_nack_are_not_replayed() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = Arc::new(Checkpoint {
            store: FileStateStore::new(dir.path().to_path_buf()),
            key: "k".into(),
            nacked: AtomicBool::new(false),
            partitioned: partitioned_bookmark("kafka"),
            held: Mutex::default(),
        });
        let kafka = partitioned_bookmark("kafka").unwrap();
        let positions = (0..5)
            .map(|o| kafka.position(&json!({"topic": "t", "partition": 0, "offset": o})))
            .collect();
        let bookmark = json!({"partition_offsets": [{"topic": "t", "partition": 0, "offset": 5}]});
        let (ack, nack) = (MessageDisposition::Ack, MessageDisposition::Nack);
        commit(Some(checkpoint.clone()), Some(bookmark), positions)(vec![
            ack.clone(),
            ack.clone(),
            nack.clone(),
            ack,
            nack,
        ])
        .await
        .unwrap();
        let stored = checkpoint.store.get("k").await.unwrap().unwrap();
        assert_eq!(offsets(&stored), [(0, 2)]);

        let consumer = FaucetConsumer {
            connector: "kafka".into(),
            pages: mpsc::channel(1).1,
            pending: VecDeque::from([json!({})]),
            bookmark: None,
            checkpoint: Some(checkpoint),
            exit_on_empty: false,
            idle_timeout: None,
            failed: Some(("gone".into(), false)),
        };
        let status = consumer.status().await;
        assert!(!status.healthy);
        assert_eq!((status.target.as_str(), status.pending), ("kafka", Some(1)));
        assert_eq!(status.error.as_deref(), Some("gone"));
        assert_eq!(
            status.details["held_partitions"],
            json!([{"topic": "t", "partition": 0, "offset": 2}])
        );
    }

    fn checked(value: Value) -> bool {
        check_idle_timeout(&FaucetConfig::parse(&value).unwrap()).is_ok()
    }

    #[test]
    fn a_drain_must_outwait_the_sources_own_idle_wait() {
        let nats = |idle_ms: u64| json!({"connector": "nats", "config": {"idle_timeout_secs": 2}, "idle_timeout_ms": idle_ms});
        assert!(!checked(nats(500)));
        assert!(!checked(nats(2000)));
        assert!(checked(nats(2001)));
        assert!(checked(
            json!({"connector": "nats", "config": {"idle_timeout_secs": 2}})
        ));

        let cdc = |idle_ms: u64| json!({"connector": "postgres-cdc", "config": {}, "idle_timeout_ms": idle_ms});
        assert!(!checked(cdc(10_000)), "the 30 s default applies");
        assert!(checked(cdc(31_000)));

        assert!(!checked(
            json!({"connector": "webhook", "config": {}, "idle_timeout_ms": 500})
        ));
        assert!(checked(
            json!({"connector": "rest", "config": {}, "idle_timeout_ms": 500})
        ));
    }
}
