use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use faucet_cli::registry;
use faucet_core::state::validate_state_key;
use faucet_core::{FaucetError, FileStateStore, Source, StateStore, StreamPage};
use futures::StreamExt;
use mq_bridge::errors::{ConsumerError, InvalidConfig};
use mq_bridge::traits::{BatchCommitFunc, MessageConsumer, MessageDisposition};
use mq_bridge::{CanonicalMessage, ReceivedBatch};
use serde_json::Value;
use tokio::sync::mpsc;

use crate::config::{invalid, unknown_connector, FaucetConfig};

type PageResult = Result<StreamPage, FaucetError>;

/// Pages read ahead of the route, as faucet's own source-to-sink channel holds.
const PAGES_AHEAD: usize = 4;

struct Checkpoint {
    store: FileStateStore,
    key: String,
    /// A bookmark covers every record before it, so after a nack none may be stored this run.
    nacked: AtomicBool,
}

struct FaucetConsumer {
    pages: mpsc::Receiver<PageResult>,
    /// Records of the current page not handed out yet.
    pending: VecDeque<Value>,
    /// Stored once the batch that empties `pending` is committed.
    bookmark: Option<Value>,
    checkpoint: Option<Arc<Checkpoint>>,
    exit_on_empty: bool,
    idle_timeout: Option<Duration>,
    /// The source failed; its page stream is gone, so every later receive fails too.
    failed: Option<String>,
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
    let source = registry::build_source(kind, config.config.clone(), &auth, None)
        .await
        .with_context(|| format!("failed to create the faucet `{kind}` source"))?;

    let checkpoint = match &config.state_path {
        Some(path) => Some(Arc::new(
            resume(source.as_ref(), path.clone(), &config, route_name).await?,
        )),
        None => None,
    };
    Ok(Box::new(FaucetConsumer {
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
        if let Some(message) = &self.failed {
            return Err(ConsumerError::Connection(anyhow!(message.clone())));
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
                self.failed = Some(error.to_string());
                Err(ConsumerError::Connection(anyhow::Error::new(error)))
            }
        }
    }
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
        let messages = self
            .pending
            .drain(..count)
            .map(|record| {
                CanonicalMessage::from(
                    serde_json::to_vec(&record).expect("a JSON value always serializes"),
                )
            })
            .collect();
        let bookmark = if self.pending.is_empty() {
            self.bookmark.take()
        } else {
            None
        };
        Ok(ReceivedBatch {
            messages,
            commit: commit(self.checkpoint.clone(), bookmark),
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Stores the page's bookmark, unless a message of this run was nacked.
/// Commits run in receive order, so an earlier batch's nack is seen here.
fn commit(checkpoint: Option<Arc<Checkpoint>>, bookmark: Option<Value>) -> BatchCommitFunc {
    Box::new(move |dispositions| {
        Box::pin(async move {
            let Some(checkpoint) = checkpoint else {
                return Ok(());
            };
            if dispositions
                .iter()
                .any(|d| matches!(d, MessageDisposition::Nack))
            {
                checkpoint.nacked.store(true, Ordering::Release);
            }
            let Some(bookmark) = bookmark else {
                return Ok(());
            };
            if checkpoint.nacked.load(Ordering::Acquire) {
                return Ok(());
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
