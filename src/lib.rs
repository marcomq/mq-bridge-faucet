//! [faucet-stream](https://github.com/PawanSikawat/faucet-stream) connectors as
//! an input/output endpoint extension for `mq-bridge`.
//!
//! One endpoint, `faucet`, wraps any faucet source or sink: `connector` names
//! it and `config` is passed to it unchanged. Records cross as JSON payloads.
//!
//! The same implementation is used three ways:
//!
//! * linked directly by a Rust program, which calls [`register`];
//! * loaded from the compiled `cdylib` by any mq-bridge host through
//!   `mq_bridge::plugin::load_endpoint_plugin`;
//! * from Python or Node.js, whose `mq-bridge-faucet` packages ship that
//!   same library and call the host's generic loader.

mod checks;
mod config;
mod consumer;
mod publisher;

use std::sync::Arc;

use async_trait::async_trait;
use mq_bridge::traits::{CustomEndpointFactory, MessageConsumer, MessagePublisher};

pub use config::FaucetConfig;

#[derive(Debug, Default)]
pub struct FaucetFactory;

// Exports the same factory as a loadable plugin. `register()` below covers the
// directly linked case; this covers every host that loads the compiled library.
#[cfg(feature = "plugin")]
mq_bridge::export_endpoint_plugin! {
    name: "faucet",
    factory: FaucetFactory,
}

/// Registers this crate's factory under `faucet`. Only needed when linking
/// this crate directly; a host that loads the plugin registers it while loading.
pub fn register() -> anyhow::Result<()> {
    mq_bridge::extensions::register_endpoint_factory("faucet", Arc::new(FaucetFactory))
}

#[async_trait]
impl CustomEndpointFactory for FaucetFactory {
    fn config_schema(&self) -> Option<serde_json::Value> {
        let mut schema = serde_json::to_value(schemars::schema_for!(FaucetConfig)).ok()?;
        schema["x-mqb-default-batch-size"] = faucet_core::DEFAULT_BATCH_SIZE.into();
        Some(schema)
    }

    /// A replay leaves one effect with `exactly_once`, or with a keyed upsert or delete.
    fn idempotent_sink(&self, value: &serde_json::Value) -> bool {
        let Ok(config) = FaucetConfig::parse(value) else {
            return false;
        };
        config.exactly_once
            || serde_json::from_value::<faucet_core::write_mode::WriteSpec>(config.config)
                .is_ok_and(|write| write.dedups_by_key())
    }

    /// `write_mode: overwrite` swaps in what the run wrote, so the input must read everything.
    fn requires_full_replay(&self, value: &serde_json::Value) -> bool {
        FaucetConfig::parse(value).is_ok_and(|config| config.overwrites())
    }

    fn replays_from_start(&self, value: &serde_json::Value) -> bool {
        FaucetConfig::parse(value).is_ok_and(|config| config.replays_from_start())
    }

    async fn create_consumer(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessageConsumer>> {
        consumer::create(route_name, value).await
    }

    async fn create_publisher(
        &self,
        route_name: &str,
        value: &serde_json::Value,
    ) -> anyhow::Result<Box<dyn MessagePublisher>> {
        publisher::create(route_name, value).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn only_a_keyed_or_exactly_once_sink_counts_as_idempotent() {
        let sink = |extra: serde_json::Value| {
            let mut config = json!({"database_url": "x.db", "table_name": "t"});
            config
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            json!({"connector": "sqlite", "config": config})
        };
        let factory = FaucetFactory;
        assert!(factory.idempotent_sink(&sink(json!({"write_mode": "upsert", "key": ["id"]}))));
        assert!(!factory.idempotent_sink(&sink(json!({}))));
        let mut exactly_once = sink(json!({}));
        exactly_once["exactly_once"] = true.into();
        assert!(factory.idempotent_sink(&exactly_once));
    }

    #[test]
    fn an_overwrite_needs_an_input_that_reads_everything_each_run() {
        let factory = FaucetFactory;
        let sink = |mode: &str| json!({"connector": "sqlite", "config": {"database_url": "x.db", "table_name": "t", "write_mode": mode}});
        assert!(factory.requires_full_replay(&sink("overwrite")));
        assert!(!factory.requires_full_replay(&sink("append")));

        let file = |extra: serde_json::Value| {
            let mut source = json!({"connector": "file", "config": {"path": "in.jsonl", "incremental": {"cursor": "mtime"}}});
            source
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            source
        };
        assert!(factory.replays_from_start(&file(json!({}))));
        assert!(!factory.replays_from_start(&file(json!({"state_path": "state"}))));
        assert!(!factory.replays_from_start(&json!({"connector": "sqs", "config": {}})));
    }
}
