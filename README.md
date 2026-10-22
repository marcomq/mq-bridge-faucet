# mq-bridge-faucet

[faucet-stream](https://github.com/PawanSikawat/faucet-stream)'s sources and
sinks as one [mq-bridge](https://github.com/marcomq/mq-bridge) endpoint, `faucet`.
It adds the connectors mq-bridge has no endpoint for (REST and GraphQL APIs,
Snowflake, BigQuery, Iceberg, Delta, Singer taps and more) to any mq-bridge
route, next to the middleware and endpoints mq-bridge already has.

It needs mq-bridge 0.4.18 or later and Rust 1.96 to build.

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
| `state_key` | input | bookmark key; defaults to the connector's own key, then to the route name |
| `page_size` | input | records per page requested from the source, default 1000 |
| `idle_timeout_ms` | input | ends a `--drain` run once the source was quiet this long |

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

Not in the default build, the same as in faucet's own default build:

- `oracle`, `oracle-cdc` (sources) and `oracle` (sink). These need Oracle
  Instant Client at runtime. Enable them with `--features
  faucet-cli/source-oracle,faucet-cli/source-oracle-cdc,faucet-cli/sink-oracle`.
- Optional connector extras, for example `faucet-cli/delta-s3`,
  `faucet-cli/source-iceberg-glue`, `faucet-cli/kafka-schema-registry`,
  `faucet-cli/rabbitmq-tls`, `faucet-cli/mtls`, `faucet-cli/oauth1` and the
  XML, Excel, Avro and ORC file formats (`faucet-cli/file-formats`). Turn them
  on the same way.

If a route names a connector that is not compiled in, the error lists the
connectors this build has.

## Behaviour

- A record is one message with the record as its JSON payload. On the output
  side a payload that is not JSON fails that message as non-retryable.
- A source's bookmark is stored when the batch holding the last record of its
  page is committed without a nack, so a crash replays at most that page
  (at-least-once). The file is the one faucet's own file state store writes.
- The input ends when the source does. A source that never ends (Kafka, CDC,
  webhook) needs `idle_timeout_ms` to finish under `--drain`.
- The output flushes the sink after every batch, so an acknowledged message is
  written. File-shaped sinks (Parquet, S3) therefore write one object per batch;
  raise the route's batch size for them.
- A sink failure is retried unless faucet reports the record or the
  configuration as the cause.

Not driven: faucet's run-level features (overwrite and staged-load write modes,
exactly-once commit tokens, quality checks, contracts, masking, transforms).
Use mq-bridge middleware for the per-message ones.

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
