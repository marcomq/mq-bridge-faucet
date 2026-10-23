use faucet_core::contract::{apply_contract, CompiledContract};
use faucet_core::masking::{apply_masking, CompiledMasking};
use faucet_core::quality::{apply_quality, CompiledQuality};
use faucet_core::FaucetError;
use mq_bridge::errors::InvalidConfig;
use serde_json::Value;

use crate::config::{invalid, FaucetConfig};

/// faucet's per-page passes, run on each batch in faucet's order: masking, quality, contract.
pub(crate) struct Checks {
    masking: Option<CompiledMasking>,
    quality: Option<CompiledQuality>,
    contract: Option<CompiledContract>,
}

/// The records left to write, the batch index of each, and the rejected indexes.
#[derive(Debug)]
pub(crate) struct Checked {
    pub records: Vec<Value>,
    pub indexes: Vec<usize>,
    pub rejected: Vec<(usize, String)>,
}

impl Checks {
    pub(crate) fn compile(config: &FaucetConfig) -> Result<Option<Self>, InvalidConfig> {
        let checks = Self {
            masking: config
                .masking
                .as_ref()
                .map(CompiledMasking::compile)
                .transpose()
                .map_err(invalid)?,
            quality: config
                .quality
                .as_ref()
                .map(CompiledQuality::compile)
                .transpose()
                .map_err(invalid)?,
            contract: config
                .contract
                .as_ref()
                .map(CompiledContract::compile)
                .transpose()
                .map_err(invalid)?,
        };
        let any = checks.masking.is_some() || checks.quality.is_some() || checks.contract.is_some();
        Ok(any.then_some(checks))
    }

    /// Whether a check can reject single records instead of aborting the batch.
    pub(crate) fn quarantines(&self) -> bool {
        self.quality
            .as_ref()
            .is_some_and(CompiledQuality::requires_dlq)
            || self
                .contract
                .as_ref()
                .is_some_and(CompiledContract::requires_dlq)
    }

    /// `Err` when a check is set to abort, which fails the whole batch.
    pub(crate) fn apply(
        &self,
        records: Vec<Value>,
        indexes: Vec<usize>,
    ) -> Result<Checked, FaucetError> {
        let mut checked = Checked {
            records,
            indexes,
            rejected: Vec::new(),
        };
        if let Some(masking) = &self.masking {
            checked.records = apply_masking(checked.records, masking).records;
        }
        if let Some(quality) = &self.quality {
            let outcome = apply_quality(std::mem::take(&mut checked.records), quality)?;
            let failed = outcome.quarantined.into_iter().map(|record| {
                (
                    record.page_index,
                    format!(
                        "quality check `{}` failed: {}",
                        record.check, record.message
                    ),
                )
            });
            checked.drop(failed);
            checked.records = outcome.survivors;
        }
        if let Some(contract) = &self.contract {
            let outcome = apply_contract(std::mem::take(&mut checked.records), contract)?;
            let failed = outcome.quarantined.into_iter().map(|record| {
                let violation = record.violation;
                (
                    violation.page_index,
                    format!("data contract breached: {}", violation.describe()),
                )
            });
            checked.drop(failed);
            checked.records = outcome.survivors;
        }
        Ok(checked)
    }
}

impl Checked {
    /// Removes the quarantined positions, which index the records before this pass.
    fn drop(&mut self, quarantined: impl Iterator<Item = (usize, String)>) {
        let mut dropped = vec![false; self.indexes.len()];
        for (position, message) in quarantined {
            dropped[position] = true;
            self.rejected.push((self.indexes[position], message));
        }
        let mut dropped = dropped.into_iter();
        self.indexes.retain(|_| !dropped.next().unwrap_or(false));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn checks(extra: Value) -> Checks {
        let mut config = json!({"connector": "stdout"});
        config
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let config = FaucetConfig::parse(&config).unwrap();
        Checks::compile(&config).unwrap().unwrap()
    }

    #[test]
    fn quarantined_records_keep_their_batch_index() {
        let checks = checks(json!({
            "masking": {"rules": [{"match": {"fields": ["email"]}, "action": {"type": "redact"}}]},
            "quality": {"record": [{"type": "not_null", "field": "id", "on_failure": "quarantine"}]},
        }));
        let records = vec![
            json!({"id": 1, "email": "a@b.c"}),
            json!({"email": "x"}),
            json!({"id": 3}),
        ];
        let checked = checks.apply(records, vec![10, 11, 12]).unwrap();
        assert_eq!(checked.indexes, vec![10, 12]);
        assert_eq!(checked.records.len(), 2);
        assert_ne!(checked.records[0]["email"], "a@b.c");
        assert_eq!(checked.rejected.len(), 1);
        assert_eq!(checked.rejected[0].0, 11);
    }
}
