use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::anyhow;
use faucet_cli::auth_catalog::{build_auth_catalog, AuthCatalog};
use mq_bridge::errors::InvalidConfig;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FaucetConfig {
    /// faucet connector type, as in a faucet YAML `type:`, e.g. `rest` or `snowflake`.
    pub connector: String,
    /// The connector's own configuration, as in a faucet YAML `config:` block.
    #[serde(default)]
    pub config: Value,
    /// Named auth providers a connector refers to with `auth: { ref: <name> }`.
    #[serde(default)]
    pub auth: HashMap<String, Value>,
    /// Input only: directory the source's bookmark is kept in. Unset reads from the start each run.
    #[serde(default)]
    pub state_path: Option<PathBuf>,
    /// Input only: bookmark key. Defaults to the connector's own key, then the route name.
    #[serde(default)]
    pub state_key: Option<String>,
    /// Input only: records per page requested from the source.
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// Input only: ends a `--drain` run once the source was quiet this long. Unset waits for the source to finish.
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
}

fn default_page_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

impl FaucetConfig {
    pub(crate) fn parse(value: &Value) -> Result<Self, InvalidConfig> {
        serde_json::from_value(value.clone()).map_err(|e| InvalidConfig(e.into()))
    }

    pub(crate) fn auth_catalog(&self) -> Result<AuthCatalog, InvalidConfig> {
        build_auth_catalog(Some(&self.auth)).map_err(invalid)
    }
}

pub(crate) fn invalid(error: impl std::error::Error + Send + Sync + 'static) -> InvalidConfig {
    InvalidConfig(anyhow::Error::new(error))
}

pub(crate) fn unknown_connector(role: &str, name: &str, available: Vec<&str>) -> InvalidConfig {
    InvalidConfig(anyhow!(
        "unknown faucet {role} connector `{name}`; this build has: {}",
        available.join(", ")
    ))
}
