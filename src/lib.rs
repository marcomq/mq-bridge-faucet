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
