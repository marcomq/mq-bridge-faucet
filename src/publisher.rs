use std::any::Any;
use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use faucet_cli::registry;
use faucet_core::{format_token, parse_token, FaucetError, Sink};
use mq_bridge::errors::{InvalidConfig, PublisherError};
use mq_bridge::support::source_ranges::{SourcePartition, SourcePosition};
use mq_bridge::traits::{disconnect_outcome, BoxFuture, DisconnectOutcome, MessagePublisher};
use mq_bridge::{CanonicalMessage, SentBatch};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::checks::Checks;
use crate::config::{invalid, unknown_connector, FaucetConfig, FlushMode};

const EO_WITH_OVERWRITE: &str =
    "exactly_once skips records an earlier run committed, so it cannot overwrite";
/// The token commits past a rejected record, so a crash before the error handler
/// (`dlq`) saw it would lose that record. faucet itself refuses the same combination.
const EO_WITH_QUARANTINE: &str = "exactly_once cannot quarantine single records: \
     set the quality `on_failure` and the contract `on_breach` to `abort`";

struct FaucetPublisher {
    sink: Arc<dyn Sink>,
    flush: FlushMode,
    /// Set with `flush: periodic`: the route commits a batch only after a flush.
    flush_interval: Option<Duration>,
    /// Set with `exactly_once`: the prefix of every commit-token scope.
    token_scope: Option<String>,
    watermarks: Mutex<Watermarks>,
    flush_failure: Arc<StdMutex<FlushFailure>>,
    checks: Option<Checks>,
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
    let checks = Checks::compile(&config)?;
    if config.exactly_once {
        if config.overwrites() {
            return Err(InvalidConfig(anyhow!(EO_WITH_OVERWRITE)).into());
        }
        if checks.as_ref().is_some_and(Checks::quarantines) {
            return Err(InvalidConfig(anyhow!(EO_WITH_QUARANTINE)).into());
        }
    }
    let auth = config.auth_catalog()?;
    let sink: Arc<dyn Sink> = Arc::from(
        registry::build_sink(kind, config.config.clone(), &auth)
            .await
            .with_context(|| format!("failed to create the faucet `{kind}` sink"))?,
    );
    let token_scope = if config.exactly_once {
        if sink.is_overwrite() {
            return Err(InvalidConfig(anyhow!(EO_WITH_OVERWRITE)).into());
        }
        if !sink.supports_idempotent_writes() {
            return Err(InvalidConfig(anyhow!(
                "exactly_once: the faucet `{kind}` sink cannot commit a token with its rows"
            ))
            .into());
        }
        Some(
            config
                .state_key
                .clone()
                .unwrap_or_else(|| route_name.to_string()),
        )
    } else {
        None
    };
    let flush_failure = Arc::new(StdMutex::new(FlushFailure::default()));
    let flush_interval = (config.flush == FlushMode::Periodic)
        .then(|| Duration::from_millis(config.flush_interval_ms.max(1)));
    // The route flushes on this interval too; the own timer covers hosts without deferred commits.
    if let Some(interval) = flush_interval {
        spawn_periodic_flush(Arc::downgrade(&sink), interval, flush_failure.clone());
    }
    Ok(Box::new(FaucetPublisher {
        sink,
        flush: config.flush,
        flush_interval,
        token_scope,
        watermarks: Mutex::default(),
        flush_failure,
        checks,
    }))
}

/// Ends with the publisher, once the sink is dropped.
fn spawn_periodic_flush(
    sink: Weak<dyn Sink>,
    interval: Duration,
    flush_failure: Arc<StdMutex<FlushFailure>>,
) {
    tokio::spawn(async move {
        let mut ticks = tokio::time::interval(interval);
        ticks.tick().await;
        loop {
            ticks.tick().await;
            let Some(sink) = sink.upgrade() else { break };
            if let Err(error) = sink.flush().await {
                let error = error.to_string();
                let mut failure = flush_failure.lock().expect("flush failure lock");
                failure.send = Some(error.clone());
                failure.flush = Some(error);
            }
        }
    });
}

#[async_trait]
impl MessagePublisher for FaucetPublisher {
    async fn send_batch(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let failed = self
            .flush_failure
            .lock()
            .expect("flush failure lock")
            .send
            .take();
        if let Some(error) = failed {
            return Err(PublisherError::Retryable(anyhow!(
                "a periodic flush of the faucet sink failed: {error}"
            )));
        }
        match &self.token_scope {
            Some(scope) => self.send_exactly_once(scope, messages).await,
            None => self.send_at_least_once(messages).await,
        }
    }

    /// Also fails after a failed periodic flush, so the route commits nothing it may have lost.
    async fn flush(&self) -> anyhow::Result<()> {
        let failed = self
            .flush_failure
            .lock()
            .expect("flush failure lock")
            .flush
            .take();
        if let Some(error) = failed {
            return Err(anyhow!(
                "a periodic flush of the faucet sink failed: {error}"
            ));
        }
        self.sink.flush().await.map_err(anyhow::Error::new)
    }

    fn deferred_commit_interval(&self) -> Option<Duration> {
        self.flush_interval
    }

    /// Above `concurrency: 1` batches would otherwise reach the sink out of source order, as
    /// faucet never delivers them; the `exactly_once` watermark also skips everything below it.
    fn requires_ordered_publish(&self) -> bool {
        true
    }

    /// An overwrite writes into staging until the route completes.
    #[allow(deprecated)]
    fn on_connect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        if !self.sink.is_overwrite() {
            return None;
        }
        Some(Box::pin(async move {
            self.sink
                .begin_overwrite()
                .await
                .map_err(anyhow::Error::new)
        }))
    }

    /// faucet's end of a successful run: swap in an overwrite, then `complete_run`.
    /// A stopped or failed route discards the overwrite staging instead.
    #[allow(deprecated)]
    fn on_disconnect_hook(&self) -> Option<BoxFuture<'_, anyhow::Result<()>>> {
        Some(Box::pin(async move {
            let completed = disconnect_outcome() == Some(DisconnectOutcome::Completed);
            if self.sink.is_overwrite() && !completed {
                self.sink.abort_overwrite().await?;
                return Ok(());
            }
            self.flush().await?;
            if completed {
                if self.sink.is_overwrite() {
                    self.sink.commit_overwrite().await?;
                }
                self.sink.complete_run().await?;
            }
            Ok(())
        }))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FaucetPublisher {
    async fn send_at_least_once(
        &self,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        // TEMP benchmark switch, remove before commit.
        match std::env::var("XP_STAGE").as_deref() {
            Ok("noop") => {
                let n = messages.len();
                return SentBatch::from_outcomes(messages, (0..n).map(|_| None::<PublisherError>).collect::<Vec<_>>());
            }
            Ok("parse") => {
                let n = parse_payloads(&messages).await.len();
                return SentBatch::from_outcomes(messages, (0..n).map(|_| None::<PublisherError>).collect::<Vec<_>>());
            }
            _ => {}
        }
        let mut records = Vec::with_capacity(messages.len());
        let mut indexes = Vec::with_capacity(messages.len());
        let mut outcomes: Vec<Option<PublisherError>> = Vec::with_capacity(messages.len());
        for (index, parsed) in parse_payloads(&messages).await.into_iter().enumerate() {
            match parsed {
                Ok(record) => {
                    records.push(record);
                    indexes.push(index);
                    outcomes.push(None);
                }
                Err(error) => outcomes.push(Some(not_json(error))),
            }
        }
        let (records, _) = self.check(records, indexes, &mut outcomes)?;

        let mut written = self
            .sink
            .write_batch_partial(&records)
            .await
            .map_err(publisher_error)?
            .into_iter();
        // Several sinks only buffer in `write_batch`; with `flush: batch` an ack means written.
        if self.flush == FlushMode::Batch {
            self.sink.flush().await.map_err(publisher_error)?;
        }

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

    /// One atomic write per source partition, its token the highest position written.
    /// Positions at or below the committed token were written by an earlier attempt.
    // Deprecated for sink authors only; faucet's own 1.x pipeline calls the same methods.
    #[allow(deprecated)]
    async fn send_exactly_once(
        &self,
        scope_prefix: &str,
        messages: Vec<CanonicalMessage>,
    ) -> Result<SentBatch, PublisherError> {
        let positions = messages
            .iter()
            .map(SourcePosition::from_message)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|error| {
                PublisherError::NonRetryable(error.context(
                    "exactly_once needs source positions: set `source_metadata: true` on the input",
                ))
            })?;
        let mut outcomes: Vec<Option<PublisherError>> = std::iter::repeat_with(|| None)
            .take(messages.len())
            .collect();
        let scopes: Vec<(String, Vec<usize>)> = group_by_source(&positions)
            .into_iter()
            .map(|(source, indexes)| {
                let scope = format!("{scope_prefix}::{}::{}", source.topic, source.partition);
                (scope, indexes)
            })
            .collect();
        let values = parse_payloads(&messages).await;
        let mut marks = self.watermarks.lock().await;
        let written = self
            .write_exactly_once(&mut marks, &scopes, &positions, values, &mut outcomes)
            .await;
        // Every position to be sent again is a gap; every settled one closes its gap.
        for (scope, indexes) in &scopes {
            for &index in indexes {
                let error = written.as_ref().err().or(outcomes[index].as_ref());
                let resent =
                    error.is_some_and(|error| !matches!(error, PublisherError::NonRetryable(_)));
                marks.track(scope, positions[index].offset, resent);
            }
        }
        written?;
        SentBatch::from_outcomes(messages, outcomes)
    }

    // Deprecated for sink authors only; faucet's own 1.x pipeline calls the same methods.
    #[allow(deprecated)]
    async fn write_exactly_once(
        &self,
        marks: &mut Watermarks,
        scopes: &[(String, Vec<usize>)],
        positions: &[SourcePosition],
        values: Vec<serde_json::Result<Value>>,
        outcomes: &mut [Option<PublisherError>],
    ) -> Result<(), PublisherError> {
        let mut partitions = Vec::with_capacity(scopes.len());
        let mut partition_of = vec![0; values.len()];
        let mut records = Vec::with_capacity(values.len());
        let mut parsed = Vec::with_capacity(values.len());
        let mut values: Vec<_> = values.into_iter().map(Some).collect();
        for (scope, indexes) in scopes {
            let offsets: Vec<u64> = indexes
                .iter()
                .map(|&index| positions[index].offset)
                .collect();
            let highest = offsets.iter().copied().max().unwrap_or(0);
            let gaps = marks.gaps.get(scope);
            // A batch must send again every failed position up to its highest one.
            let waits_for =
                gaps.and_then(|gaps| gaps.range(..=highest).find(|gap| !offsets.contains(gap)));
            if let Some(gap) = waits_for {
                for &index in indexes {
                    outcomes[index] = Some(PublisherError::Retryable(anyhow!(
                        "exactly_once: `{scope}` waits for position {gap}, whose write failed, \
                         to be sent again"
                    )));
                }
                continue;
            }
            let done = self.committed_position(&mut marks.committed, scope).await?;
            let mut partition = Partition {
                scope: scope.clone(),
                pending: Vec::new(),
                last: None,
                records: Vec::new(),
            };
            for &index in indexes {
                let offset = positions[index].offset;
                // A covered message was written or rejected before; one rejected then is rejected again.
                if done.is_some_and(|done| offset <= done) {
                    if let Some(Err(error)) = values[index].take() {
                        outcomes[index] = Some(not_json(error));
                    }
                    continue;
                }
                partition_of[index] = partitions.len();
                partition.pending.push(index);
                partition.last = partition.last.max(Some(offset));
                match values[index]
                    .take()
                    .expect("a message belongs to one partition")
                {
                    Ok(record) => {
                        records.push(record);
                        parsed.push(index);
                    }
                    Err(error) => outcomes[index] = Some(not_json(error)),
                }
            }
            partitions.push(partition);
        }

        // Once per batch, before any write, so an abort leaves every partition unwritten.
        let (records, parsed) = self.check(records, parsed, outcomes)?;
        for (record, index) in records.into_iter().zip(parsed) {
            partitions[partition_of[index]].records.push(record);
        }

        for partition in partitions {
            let Some(last) = partition.last.filter(|_| !partition.records.is_empty()) else {
                continue;
            };
            let written = self
                .sink
                .write_batch_idempotent_reported(
                    &partition.records,
                    &partition.scope,
                    &format_token(last),
                )
                .await;
            match written {
                Ok(report) => {
                    marks.committed.insert(partition.scope.clone(), Some(last));
                    // The token is in, so a retry would skip the lost rows: hand them to the error handler.
                    if let Err(error) = report.check(partition.records.len()) {
                        let error = PublisherError::NonRetryable(anyhow::Error::new(error));
                        fail_pending(outcomes, &partition.pending, &error);
                    }
                }
                // Each partition commits on its own, so only this one's messages fail.
                Err(error) => {
                    fail_pending(outcomes, &partition.pending, &publisher_error(error));
                }
            }
        }
        Ok(())
    }

    /// The last source position committed in `scope`, looked up once per scope.
    #[allow(deprecated)]
    async fn committed_position(
        &self,
        committed: &mut HashMap<String, Option<u64>>,
        scope: &str,
    ) -> Result<Option<u64>, PublisherError> {
        if let Some(done) = committed.get(scope) {
            return Ok(*done);
        }
        let token = self
            .sink
            .last_committed_token(scope)
            .await
            .map_err(publisher_error)?;
        let done = token.as_deref().and_then(parse_token);
        committed.insert(scope.to_string(), done);
        Ok(done)
    }
}

/// A failed periodic flush, reported once to the next batch and once to the next route flush.
#[derive(Default)]
struct FlushFailure {
    send: Option<String>,
    flush: Option<String>,
}

/// `exactly_once` bookkeeping per commit-token scope.
#[derive(Default)]
struct Watermarks {
    /// Last committed source position; `None` once looked up and never committed.
    committed: HashMap<String, Option<u64>>,
    /// Positions whose write failed and will be retried. Later positions wait for
    /// them: written first, they would move the watermark past them.
    gaps: HashMap<String, BTreeSet<u64>>,
}

impl Watermarks {
    fn track(&mut self, scope: &str, at: u64, resent: bool) {
        if resent {
            self.gaps.entry(scope.to_string()).or_default().insert(at);
            return;
        }
        let Some(gaps) = self.gaps.get_mut(scope) else {
            return;
        };
        gaps.remove(&at);
        if gaps.is_empty() {
            self.gaps.remove(scope);
        }
    }
}

/// One source partition's share of an `exactly_once` batch.
struct Partition {
    scope: String,
    /// Messages past the committed position.
    pending: Vec<usize>,
    /// The token to commit: the highest pending position, rejected messages included.
    last: Option<u64>,
    records: Vec<Value>,
}

impl FaucetPublisher {
    /// Runs the configured checks; a rejected record fails its message without retry.
    /// Returns the records left to write and the batch index of each.
    fn check(
        &self,
        records: Vec<Value>,
        indexes: Vec<usize>,
        outcomes: &mut [Option<PublisherError>],
    ) -> Result<(Vec<Value>, Vec<usize>), PublisherError> {
        let Some(checks) = &self.checks else {
            return Ok((records, indexes));
        };
        let checked = checks.apply(records, indexes).map_err(publisher_error)?;
        for (index, message) in checked.rejected {
            outcomes[index] = Some(PublisherError::NonRetryable(anyhow!(message)));
        }
        Ok((checked.records, checked.indexes))
    }
}

/// Fewer messages per thread cost more in hand-off than parsing saves.
const PARSE_CHUNK_MIN: usize = 128;

/// Parses the payloads on blocking threads, in message order. Batches reach the sink one at
/// a time, so parsing on the route's task would hold up every write behind it.
async fn parse_payloads(messages: &[CanonicalMessage]) -> Vec<serde_json::Result<Value>> {
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    let chunk = messages.len().div_ceil(threads).max(PARSE_CHUNK_MIN);
    let tasks: Vec<_> = messages
        .chunks(chunk)
        .map(|chunk| {
            let payloads: Vec<_> = chunk
                .iter()
                .map(|message| message.payload.clone())
                .collect();
            tokio::task::spawn_blocking(move || {
                payloads
                    .iter()
                    .map(|payload| serde_json::from_slice(payload))
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut parsed = Vec::with_capacity(messages.len());
    for task in tasks {
        parsed.extend(task.await.expect("parsing a payload does not panic"));
    }
    parsed
}

fn not_json(error: serde_json::Error) -> PublisherError {
    PublisherError::NonRetryable(anyhow!("a faucet sink takes JSON records: {error}"))
}

/// Fails the pending messages that have no outcome yet and returns them.
fn fail_pending(
    outcomes: &mut [Option<PublisherError>],
    pending: &[usize],
    error: &PublisherError,
) -> Vec<usize> {
    let failed: Vec<usize> = pending
        .iter()
        .copied()
        .filter(|&index| outcomes[index].is_none())
        .collect();
    for &index in &failed {
        outcomes[index] = Some(duplicate(error));
    }
    failed
}

/// The same failure for another message of the batch.
fn duplicate(error: &PublisherError) -> PublisherError {
    match error {
        PublisherError::Retryable(error) => PublisherError::Retryable(anyhow!("{error:#}")),
        PublisherError::NonRetryable(error) => PublisherError::NonRetryable(anyhow!("{error:#}")),
        PublisherError::Connection(error) => PublisherError::Connection(anyhow!("{error:#}")),
    }
}

/// Message indexes per source partition, in batch order.
fn group_by_source(positions: &[SourcePosition]) -> Vec<(&SourcePartition, Vec<usize>)> {
    let mut groups: Vec<(&SourcePartition, Vec<usize>)> = Vec::new();
    for (index, position) in positions.iter().enumerate() {
        match groups
            .iter_mut()
            .find(|(source, _)| **source == position.source)
        {
            Some((_, indexes)) => indexes.push(index),
            None => groups.push((&position.source, vec![index])),
        }
    }
    groups
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
