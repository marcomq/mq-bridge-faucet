# mq-bridge-faucet

[faucet-stream](https://github.com/PawanSikawat/faucet-stream)'s sources and
sinks as one [mq-bridge](https://github.com/marcomq/mq-bridge) endpoint, `faucet`.
It adds the connectors mq-bridge has no endpoint for (REST and GraphQL APIs,
Snowflake, BigQuery, Iceberg, Delta, Singer taps and more) to any mq-bridge
route, next to the middleware and endpoints mq-bridge already has.

It needs mq-bridge 1.1 or later and Rust 1.96 to build.

## Configuration

```yaml
input:
  custom:
    name: faucet
    config:
      connector: rest          # faucet's connector `type:`
      config:                  # that connector's own `config:` block, unchanged
        base_url: https://api.example.com
        path: /orders
      state_path: ./state      # keeps the bookmark between runs
```

| Field | Side | Meaning |
| --- | --- | --- |
| `connector` | both | faucet connector type, e.g. `rest`, `postgres-cdc`, `snowflake` |
| `config` | both | the connector's configuration, as documented by faucet |
| `auth` | both | named auth providers a connector refers to with `auth: { ref: <name> }` |
| `state_path` | input | directory the source's bookmark is stored in; unset reads from the start each run |
| `state_key` | both | input: bookmark key; defaults to the connector's own key, then to the route name. Output: prefix of the `exactly_once` commit-token scope; defaults to the route name |
| `page_size` | input | records per page requested from the source, default 1000 |
| `idle_timeout_ms` | input | ends a `--drain` run once the source was quiet this long |
| `flush` | output | `periodic` (default) flushes every `flush_interval_ms` and when the route ends; `batch` flushes the sink before every ack |
| `flush_interval_ms` | output | interval for `flush: periodic`, default 1000 |
| `exactly_once` | output | commit each batch atomically with its source position and skip what was already committed; see below |
| `masking` | output | faucet's `masking:` block, applied to each batch first |
| `quality` | output | faucet's `quality:` block, run after masking |
| `contract` | output | faucet's `contract:` block, run after the quality checks |

`examples/rest_to_jsonl.yaml` is a complete route.

## Connectors

The default build compiles faucet-cli's `source` and `sink` feature groups, so
the connectors available are whatever the resolved faucet-cli version puts in
those groups (`cargo tree -p faucet-cli` shows the version). Each connector's
`config:` fields are in faucet's
[connector reference](https://faucet-hq.github.io/faucet-stream/reference/connectors.html),
which documents the latest faucet release.

As of faucet-cli 1.13 to 1.15:

- Sources: `rest`, `graphql`, `xml`, `grpc`, `postgres`, `postgres-cdc`,
  `mysql`, `mysql-cdc`, `mssql`, `mssql-cdc`, `sqlite`, `duckdb`, `redshift`,
  `clickhouse`, `mongodb`, `mongodb-cdc`, `redis`, `elasticsearch`, `bigquery`,
  `snowflake`, `spanner`, `databricks`, `dynamodb`, `delta`, `iceberg`,
  `parquet`, `csv`, `file`, `s3`, `gcs`, `azure-blob`, `sftp`, `kafka`,
  `kinesis`, `pubsub`, `sqs`, `nats`, `rabbitmq`, `webhook`, `websocket`,
  `singer`
- Sinks: `postgres`, `mysql`, `mssql`, `sqlite`, `duckdb`, `redshift`,
  `clickhouse`, `mongodb`, `redis`, `elasticsearch`, `bigquery`, `snowflake`,
  `spanner`, `databricks`, `dynamodb`, `delta`, `iceberg`, `parquet`, `csv`,
  `jsonl`, `file`, `s3`, `gcs`, `azure-blob`, `sftp`, `kafka`, `kinesis`,
  `pubsub`, `sqs`, `nats`, `rabbitmq`, `http`, `stdout`, `singer`

The released packages (npm, PyPI, Homebrew, conda and the release archives)
also include the following, like faucet's own prebuilt binaries. A source build
gets them with `--features full`, or one at a time with faucet's own features:

- `oracle`, `oracle-cdc` (sources) and `oracle` (sink). These need Oracle
  Instant Client at runtime.
- Connector extras: Kafka schema registry, Delta on S3, Azure and GCS, the
  Iceberg Glue, HMS and SQL catalogs, Excel for `rest` and `xml`, staging for
  the `clickhouse`, `mssql` and `databricks` sinks, the XML, Excel, Avro and
  ORC file formats, Arrow, mTLS, RabbitMQ TLS, OAuth 1, encryption and JSON
  Schema quality checks.

If a route names a connector that is not compiled in, the error lists the
connectors this build has.

## Behaviour

- A record is one message with the record as its JSON payload. On the output
  side a payload that is not JSON fails that message as non-retryable.
- A source's bookmark is stored when the batch holding the last record of its
  page is committed without a nack, so a crash replays at most that page
  (at-least-once). The file is the one faucet's own file state store writes.
- The output receives batches in source order at any `concurrency`, as in faucet.
  Above 1, mq-bridge still reads ahead and prepares and commits batches in
  parallel, which overlaps reading with writing.
- The input ends when the source does. A source that never ends (Kafka, CDC,
  webhook) needs `idle_timeout_ms` to finish under `--drain`.
- With `flush: periodic` (the default) mq-bridge holds the source's commits
  and releases them after each flush, so a crash redelivers what was not
  flushed yet instead of losing it. faucet likewise stores a bookmark only
  after a flush. `flush: batch` flushes the sink after every batch, so an
  acknowledged message is written. That is expensive for file sinks: JSONL
  fsyncs on every flush, and a format that cannot be appended to (Parquet)
  rewrites the whole file on the next write after a flush, so the cost grows
  with the square of the file size. For long Parquet runs also set faucet's
  `max_records_per_file`.
- `exactly_once: true` writes each batch with faucet's commit-token protocol:
  rows and the batch's highest source position commit in one transaction,
  and on restart everything at or below the committed position is skipped.
  It needs a sink with commit tokens (SQL databases such as `sqlite`,
  `postgres`, `mysql`, `mssql`, and others such as `kafka`, `bigquery`,
  `snowflake`), `source_metadata: true` on an mq-bridge input that records
  positions (file, Kafka, Postgres CDC, MongoDB, sqlx). A later batch
  committing first would mark an earlier one as done, so the output makes
  mq-bridge send in source order at any `concurrency`, and after a failed
  write it refuses later batches of that partition (to be retried) until the
  failed position is sent again. Set `state_key`: the route name mq-bridge
  passes is internal.
  Each source partition commits on its own, so a failed write fails only that
  partition's messages. Checks cannot quarantine here, as in faucet: the
  position commits past a rejected record, and a crash before the `dlq` saw
  it would lose it. A replayed message that is not JSON is rejected again.
- A sink failure is retried unless faucet reports the record or the
  configuration as the cause.
- `write_mode`, `key` and `delete_marker` go in the sink's `config`, as in
  faucet. Upsert and delete need `column_mapping: auto_map` on the SQL sinks;
  with a key the route reports itself as effectively-once. Staged loads are
  the sink's own setting as well.
- `write_mode: overwrite` stages the route's writes and swaps them in when the
  route completes; a stopped or failed route discards the staging. A reconnect
  starts a new staging, so the input has to read from the start again, and
  mq-bridge only logs a failed swap as a warning. It cannot be combined with
  `exactly_once`. Use an input that reads the whole dataset on every run: one
  that resumes from a stored position (a consumer group, a durable consumer,
  `state_path`) or removes what it reads (a queue) replaces the table with
  only the new records. mq-bridge (1.1 and later) refuses such a route at
  start, as faucet does; a faucet input counts as reading everything unless
  it reads a queue or a live subscription, or resumes from its `state_path`.
- `masking`, `quality` and `contract` run per batch, in faucet's order. A
  record a check quarantines fails its message without retry, so it reaches
  mq-bridge's `dlq` middleware; a check set to `abort` or `fail` fails the
  whole batch. Batch-level quality checks see one mq-bridge batch, not a
  faucet page. The DLQ receives the original message, before masking.

Not driven: faucet's transforms (use mq-bridge middleware), schema-drift
policies, rollback and lineage.

## Using it

Install the library where mq-bridge looks for plugins and a route can name the
endpoint without further setup; see
[Installing a plugin by name](https://github.com/marcomq/mq-bridge/blob/main/docs/PLUGINS.md#installing-a-plugin-by-name).
Or load it explicitly:

```rust
// Rust, loading the compiled library
mq_bridge::plugin::load_endpoint_plugin("./libmq_bridge_faucet.so")?;
// Rust, linking this crate
mq_bridge_faucet::register()?;
```

```python
import mq_bridge_faucet
mq_bridge_faucet.register()
```

```javascript
import { register } from "mq-bridge-faucet";
register();
```

```c
#include "mq_bridge.h"

if (mqb_load_plugin("./libmq_bridge_faucet.so") != MQB_OK) {
    fprintf(stderr, "%s\n", mqb_last_error());
}
```

## Building a smaller library

The default build compiles every faucet connector, which takes a while and
needs their native toolchains. To pick connectors, turn the defaults off and
name faucet's own features:

```sh
cargo build --release --no-default-features \
  --features plugin,faucet-cli/source-rest,faucet-cli/sink-snowflake
```

A connector that is not compiled in is reported as an invalid configuration,
with the list of the ones that are.

## What is where

| Path | Purpose |
| --- | --- |
| `src/consumer.rs` | faucet `Source` as an mq-bridge input, bookmark handling |
| `src/publisher.rs` | faucet `Sink` as an mq-bridge output, error mapping |
| `tests/plugin.rs` | CSV to JSONL, linked directly and loaded as a plugin |
| `tests/connectors.rs` | bookmarks and per-record failures against test connectors |
| `node/`, `python/` | packages that load the prebuilt library through mq-bridge |
| `packaging/` | conda recipe and Homebrew formula, both repackaging the release archive |
| `scripts/set_version.py` | `set_version.py 0.2.0` sets every package version; `--check` compares them |

## Releasing

`.github/workflows/release.yml` builds five platforms on a version tag and
publishes to crates.io, npm, PyPI, Homebrew and conda. It fails until the
trusted publishers, the `homebrew-tap` deploy key (`HOMEBREW_TAP_DEPLOY_KEY`),
the `ANACONDA_API_TOKEN` secret and the GitHub environments `crates-io`, `npm`
and `pypi` exist; the first crates.io and npm versions must be published by
hand. Remove the jobs for channels you do not publish to.

## Testing against an unreleased mq-bridge

Add `.cargo/config.toml` beside `Cargo.toml`:

```toml
[patch.crates-io]
mq-bridge = { path = "../mq-bridge" }
```

## License

MIT OR Apache-2.0. faucet-stream is MIT OR Apache-2.0 as well.
