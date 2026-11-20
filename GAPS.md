# Gaps

Recreated 2026-10-31 from notes; the original file was lost. Source: CLI copy
comparison (2026-10-26) of mq-bridge-app 1.2.0 + faucet 0.1.1, native vs faucet
vs cross, on postgres, mongodb and nats. All cases delivered every row exactly once.
Kafka, redis, amqp and mysql were not compared.

Triaged 2026-11-20 against plugin 0.2.0 (`faucet-core` 1.15.0, `faucet-registry`
1.0.0, `faucet-source-kafka` 1.6.0, `mq-bridge` 1.2.0 in `Cargo.lock`), by reading
source only, nothing rebuilt. Each item says where it lives:

- **plugin**: fix in this repo
- **faucet**: faucet behaviour; document it here or raise it upstream
- **both**: plugin works around a faucet constraint

## Open

### Plugin fixes 2026-11-20 (unit-tested, not yet run against Kafka; see "Verify")
None of 7a–7d is a faucet bug: faucet behaves as documented. Decided not to post
upstream; the plugin solves them (`src/consumer.rs`).

- **7a. A non-decodable record looped the route forever.** faucet's default
  `on_decode_error: fail` ends the stream with `Source("kafka json decode: …")`; the
  plugin used to reconnect on it and read the same record again. Now `is_permanent`
  maps data/config errors (`Json`, `Transform`, `Config`, quality, contract, …) and any
  `Source` error containing `" decode: "` to `ConsumerError::Permanent`: the route
  stops, with a hint to `on_decode_error: skip` / `value_format`. Other `Source`
  errors still reconnect (connectors report lost connections that way). Depends on
  faucet's message text; the test `data_errors_stop_the_route_and_source_errors_reconnect`
  pins it, re-check it on faucet updates. Only kafka's wording is verified; nats,
  rabbitmq, pubsub, kinesis also have `on_decode_error` with unchecked wording.
- **7b. `message_id` changed on redelivery.** `record_message` derives the id from
  the record JSON via mq-bridge's `message_id_from_str` (FNV-1a, `MQB_ID_HASH` switches
  to SHA-256). Identical records share an id; kafka records never are identical.
- **7c. Acked messages came back after a nack.** faucet's kafka bookmark is
  `{"partition_offsets":[{topic, partition, offset}]}`, offset = next to read. For
  `kafka` the plugin now keeps the lowest nacked offset per partition (`Checkpoint.held`)
  and clamps every later bookmark to it (`hold_back`), instead of storing no bookmark
  for the rest of the run. Ack 0,1,3 / nack 2,4 now stores offset 2, as native kafka.
  Other connectors keep the old page-wide rule. Test: `acked_records_before_a_nack_are_not_replayed`.
  Expressed per connector as a `PartitionedBookmark` (field names), only kafka listed.
  Candidates: `kinesis`/`dynamodb` (`shard_id`/`sequence_number`) store the *last
  emitted* string sequence, not the next offset, so they need the previous record's
  sequence as the hold-back point. CDC sources have one ordered stream; page-wide is fine.
- **7d. Kafka behaviour, documented in README Behaviour.** `page_size` is ignored
  (kafka's own `batch_size` sets the page, default 1000); without `state_path` nothing
  resumes, because faucet commits group offsets only in cluster member mode; a page is
  released when `batch_size` fills or `idle_timeout` passes; records are
  `{topic, partition, offset, key, value}`.

### 6. Embedder semantics
mq-bridge drives batching, ack and state itself. Per connector, check what its
bookmark means and whether the plugin can do better than page-wide, as for kafka (7c).

### 7. Contract steps not reached yet
Run with mq-bridge's `tests/integration/contract.rs` (`test_plugin_contract`, uncommitted
in mq-bridge), brew `mq-bridge-faucet` 0.1.1, faucet `kafka` connector at both ends.
Passes: 1 nack redelivers, 2 a dead consumer's messages come back. Stops at step 3.
Not reached: 4 metadata, 5 backpressure, 6 route stop, 8 sink refuses.

```sh
MQB_TEST_BACKEND=faucet_kafka \
MQB_TEST_PLUGIN=/opt/homebrew/lib/mq-bridge/libmq_bridge_faucet.dylib \
MQB_TEST_CONTRACT='{"name":"faucet_kafka","compose":"kafka","ordered":false,"json":true,
 "stable_id":false,"hop_keeps_id":false,
 "input":{"custom":{"name":"faucet","config":{"connector":"kafka","page_size":16,"state_path":"/tmp/faucet_state",
   "config":{"brokers":"localhost:9092","topics":["contract"],"group_id":"contract","auto_offset_reset":"earliest","idle_timeout":1}}}},
 "output":{"custom":{"name":"faucet","config":{"connector":"kafka","flush":"batch",
   "config":{"brokers":"localhost:9092","topic":{"type":"fixed","name":"contract"}}}}}}' \
cargo test --test integration_test --features test-utils,plugin -- --ignored --nocapture \
  integration::contract::test_plugin_contract
```

Next run: against this plugin build instead of brew 0.1.1, with `batch_size` in the
source config. Step 3 sends a non-JSON record: it should now stop the route (7a)
instead of looping; adjust the contract's expectation or set `on_decode_error: skip`.

## Done

- **1. `idle_timeout_ms` vs connector idle wait** (plugin): routes refuse an
  `idle_timeout_ms` not longer than the source's own idle wait (`check_idle_timeout`),
  README Behaviour explains it.
- **2. `faucet://` URIs** (plugin): `faucet+<connector>://?config=…` works now
  (`src/lib.rs` test `a_uri_names_the_connector_after_the_plus_…`).
- **3. faucet nats creates no stream or durable** (faucet): by design; README
  Behaviour and the test table say both must exist beforehand.
- **4. `faucet-cli` API not stability-checked** (faucet): the plugin depends on
  `faucet-registry` 1.0 since #995.
- **5. Old repo link** (plugin): README links `github.com/faucet-hq/faucet-stream`.
- **7d sink takes only JSON** (faucet): README Behaviour documents it.

## faucet update (2026-11-20)

`cargo update` within the `Cargo.toml` ranges: `faucet-core` 1.15.0 → 1.17.0,
`faucet-registry` 1.0.0 → 1.0.2, `faucet-source-kafka` 1.6.0 → 1.6.2, `mq-bridge`
1.2.0 → 1.6.1, all faucet connectors to their latest patch. Source diff, read only:

- kafka 1.6.2 only adds checks on `extra_client_config` (broker/URL/file/command keys,
  Kerberos values). Decode, bookmark, `page_size` and group-offset behaviour are
  unchanged, so 7a–7d still needed the plugin fixes above.
- New host-credentials gate in faucet-core: allowed unless the host denies it, so
  nothing changes for the plugin. `BuildPolicy::trusted()` unchanged.
- mq-bridge 1.2 → 1.6.1: no `fn`/type removed from `traits.rs` or `errors.rs`.

Upstream: nothing to report; 7a–7d are solved in the plugin.

## Verify

`cargo test --lib consumer::` (new tests `a_record_keeps_its_id_across_reads`,
`data_errors_stop_the_route_and_source_errors_reconnect`, `a_nack_holds_back_only_its_partition`,
`acked_records_before_a_nack_are_not_replayed`).
2026-11-20: `cargo ltest consumer::` passes (5 tests) against the updated lock.
Only the lean build was checked; the full `connectors` build was not.
