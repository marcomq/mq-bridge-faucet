use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::anyhow;
use faucet_cli::auth_catalog::{build_auth_catalog, AuthCatalog};
use faucet_cli::registry;
use faucet_core::contract::ContractSpec;
use faucet_core::masking::MaskingSpec;
use faucet_core::quality::QualitySpec;
use faucet_core::write_mode::WriteSpec;
use mq_bridge::errors::InvalidConfig;
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::Value;

/// Any faucet-stream source or sink: `connector` names it, `config` is passed to it unchanged.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FaucetConfig {
    /// faucet connector type, as in a faucet YAML `type:`, e.g. `rest` or `snowflake`.
    /// In a URI it is the scheme's part after `faucet+`: `faucet+rest://?config=<json>`.
    #[schemars(extend("x-mqb-uri" = "subscheme"))]
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
    /// Input: bookmark key. Defaults to the connector's own key, then the route name.
    /// Output with `exactly_once`: prefix of the commit-token scope. Defaults to the route name.
    #[serde(default)]
    pub state_key: Option<String>,
    /// Input only: records per page requested from the source.
    #[serde(default = "default_page_size")]
    pub page_size: usize,
    /// Input only: ends a `--drain` run once the source was quiet this long. Unset waits for the source to finish.
    #[serde(default)]
    pub idle_timeout_ms: Option<u64>,
    /// Output only: when the sink is flushed. `periodic` (default) flushes every
    /// `flush_interval_ms`, and the route commits the source only after a flush, as faucet
    /// persists a bookmark only after one; `batch` flushes before every ack.
    #[serde(default)]
    pub flush: FlushMode,
    /// Output only: interval for `flush: periodic`.
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Output only: commit each batch together with its source position, skipping what an
    /// earlier run already committed. Needs a sink with commit tokens (SQL databases and
    /// others), `source_metadata: true` on the input.
    #[serde(default)]
    pub exactly_once: bool,
    /// Output only: faucet's masking policy, applied to each batch before the checks.
    #[serde(default)]
    pub masking: Option<MaskingSpec>,
    /// Output only: faucet's quality checks. A quarantined record fails its message
    /// without retry, so mq-bridge's `dlq` middleware receives it. Not with `exactly_once`.
    #[serde(default)]
    pub quality: Option<QualitySpec>,
    /// Output only: faucet's data contract, checked after the quality checks.
    #[serde(default)]
    pub contract: Option<ContractSpec>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum FlushMode {
    Batch,
    #[default]
    Periodic,
}

fn default_page_size() -> usize {
    faucet_core::DEFAULT_BATCH_SIZE
}

fn default_flush_interval_ms() -> u64 {
    1000
}

impl FaucetConfig {
    pub(crate) fn parse(value: &Value) -> Result<Self, InvalidConfig> {
        let mut value = value.clone();
        // A URI query carries these objects as JSON text.
        for field in ["config", "auth", "masking", "quality", "contract"] {
            let Some(Value::String(text)) = value.get(field) else {
                continue;
            };
            let parsed = serde_json::from_str(text)
                .map_err(|e| InvalidConfig(anyhow!("`{field}` is not a JSON object: {e}")))?;
            value[field] = parsed;
        }
        serde_json::from_value(value).map_err(|e| InvalidConfig(e.into()))
    }

    /// Output: whether the sink replaces its target with each run's data.
    pub(crate) fn overwrites(&self) -> bool {
        serde_json::from_value::<WriteSpec>(self.config.clone())
            .is_ok_and(|write| write.is_overwrite())
    }

    /// Input: whether every run reads the whole data set, the same test faucet applies
    /// before it lets a source feed `write_mode: overwrite`.
    pub(crate) fn replays_from_start(&self) -> bool {
        let kind = self.connector.as_str();
        !registry::source_reads_a_delta(kind, &self.config)
            && !(self.state_path.is_some()
                && registry::source_resumes_from_bookmark(kind, &self.config))
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
