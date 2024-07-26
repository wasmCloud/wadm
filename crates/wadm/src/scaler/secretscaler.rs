use anyhow::{Context, Result};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};
use tokio::sync::RwLock;
use tracing::{debug, error, instrument, trace};
use wadm_types::{
    api::{StatusInfo, StatusType},
    Policy, SecretSourceProperty, TraitProperty,
};
use wasmcloud_secrets_types::SecretConfig;

use crate::{
    commands::{Command, DeleteConfig, PutConfig},
    events::{ConfigDeleted, ConfigSet, Event},
    scaler::Scaler,
    workers::SecretSource,
};

pub struct SecretScaler<SecretSource> {
    secret_source: SecretSource,
    id: String,
    secret_name: String,
    source: SecretSourceProperty,
    status: RwLock<StatusInfo>,
    policy: Policy,
}

impl<S: SecretSource> SecretScaler<S> {
    pub fn new(
        secret_name: String,
        source: SecretSourceProperty,
        secret_source: S,
        policy: Policy,
    ) -> Self {
        let mut id = secret_name.clone();
        let mut hasher = DefaultHasher::new();
        source.hash(&mut hasher);
        id.extend(format!("-{}", hasher.finish()).chars());

        Self {
            id,
            secret_name,
            source,
            secret_source,
            status: RwLock::new(StatusInfo::reconciling("")),
            policy,
        }
    }
}

#[async_trait]
impl<S: SecretSource + Send + Sync + Clone> Scaler for SecretScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, _config: TraitProperty) -> Result<Vec<Command>> {
        debug!("SecretScaler does not support updating config, ignoring");
        Ok(vec![])
    }

    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        match event {
            Event::ConfigSet(ConfigSet { config_name })
            | Event::ConfigDeleted(ConfigDeleted { config_name }) => {
                if config_name == &self.secret_name {
                    return self.reconcile().await;
                }
            }
            // This is a workaround to ensure that the config has a chance to periodically
            // update itself if it is out of sync. For efficiency, we only fetch configuration
            // again if the status is not deployed.
            Event::HostHeartbeat(_) => {
                if !matches!(self.status.read().await.status_type, StatusType::Deployed) {
                    return self.reconcile().await;
                }
            }
            _ => {
                trace!("SecretScaler does not support this event, ignoring");
            }
        }
        Ok(Vec::new())
    }

    #[instrument(level = "trace", skip_all, scaler_id = %self.id)]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        debug!(self.secret_name, "Fetching configuration");
        match (
            self.secret_source.get_secret(&self.secret_name).await,
            self.source.clone(),
        ) {
            // If configuration matches what's supplied, this scaler is deployed
            (Ok(Some(config)), scaler_config) if config == scaler_config => {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            // If configuration is out of sync, we put the configuration
            (Ok(_config), scaler_config) => {
                debug!(self.secret_name, "Putting secret");

                let cfg = config_from_manifest_structures(&self.policy, scaler_config)?;

                *self.status.write().await = StatusInfo::reconciling("Secret out of sync");
                Ok(vec![Command::PutConfig(PutConfig {
                    config_name: self.secret_name.clone(),
                    config: cfg,
                })])
            }
            (Err(e), _) => {
                error!(error = %e, "SecretScaler failed to fetch configuration");
                *self.status.write().await = StatusInfo::failed(&e.to_string());
                Ok(Vec::new())
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        Ok(vec![Command::DeleteConfig(DeleteConfig {
            config_name: self.secret_name.clone(),
        })])
    }
}

/// Merge policy and properties into a secret reference of the following format:
///
/// ```json
/// {
///   "type": "secret.wasmcloud.dev/v1alpha1",
///   "backend": "baobun",
///   "key": "/path/to/secret",
///   "version": "vX.Y.Z",
///   "policy": "{\"type\":\"properties.secret.wasmcloud.dev/v1alpha1\",\"properties\":{\"key\":\"value\"}}
/// }
/// ```
///
/// Note that the policy are completely free-form and can be any valid JSON, but should include the type field
fn config_from_manifest_structures(
    policy: &Policy,
    reference: SecretSourceProperty,
) -> anyhow::Result<HashMap<String, String>> {
    let mut policy_properties = policy.properties.clone();
    let backend = policy_properties
        .remove("backend")
        .context("policy did not have a backend property")?;
    let secret_config = SecretConfig::new(
        backend,
        reference.key,
        reference.version,
        policy_properties
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect(),
    );

    secret_config.try_into()
}

#[cfg(test)]
mod test {
    use super::config_from_manifest_structures;

    use crate::{
        commands::{Command, PutConfig},
        events::{ConfigDeleted, Event, HostHeartbeat},
        scaler::Scaler,
        test_util::TestLatticeSource,
    };
    use std::collections::{BTreeMap, HashMap};
    use wadm_types::{api::StatusType, Policy, SecretProperty, SecretSourceProperty};

    #[tokio::test]
    async fn test_secret_scaler() {
        let lattice = TestLatticeSource {
            claims: HashMap::new(),
            inventory: Default::default(),
            links: Vec::new(),
            config: HashMap::new(),
        };

        let policy = Policy {
            name: "nats-kv".to_string(),
            policy_type: "secrets-backend".to_string(),
            properties: BTreeMap::from([("backend".to_string(), "nats-kv".to_string())]),
        };

        let secret = SecretProperty {
            name: "test".to_string(),
            properties: SecretSourceProperty {
                policy: "nats-kv".to_string(),
                key: "test".to_string(),
                version: None,
            },
        };

        let secret_scaler = super::SecretScaler::new(
            secret.name.clone(),
            secret.properties.clone(),
            lattice.clone(),
            policy.clone(),
        );

        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        let cfg = config_from_manifest_structures(&policy, secret.properties)
            .expect("failed to merge policy");

        assert_eq!(
            secret_scaler
                .reconcile()
                .await
                .expect("reconcile did not succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: secret.name.clone(),
                config: cfg.clone(),
            })],
        );

        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        // Configuration deleted, relevant
        assert_eq!(
            secret_scaler
                .handle_event(&Event::ConfigDeleted(ConfigDeleted {
                    config_name: secret.name.clone()
                }))
                .await
                .expect("handle_event should succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: secret.name.clone(),
                config: cfg.clone(),
            })]
        );
        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        // Configuration deleted, irrelevant
        assert_eq!(
            secret_scaler
                .handle_event(&Event::ConfigDeleted(ConfigDeleted {
                    config_name: "some_other_config".to_string()
                }))
                .await
                .expect("handle_event should succeed"),
            vec![]
        );
        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        // Periodic reconcile with host heartbeat
        assert_eq!(
            secret_scaler
                .handle_event(&Event::HostHeartbeat(HostHeartbeat {
                    components: Vec::new(),
                    providers: Vec::new(),
                    host_id: String::default(),
                    issuer: String::default(),
                    friendly_name: String::default(),
                    labels: HashMap::new(),
                    version: semver::Version::new(0, 0, 0),
                    uptime_human: String::default(),
                    uptime_seconds: 0,
                }))
                .await
                .expect("handle_event should succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: secret.name.clone(),
                config: cfg.clone(),
            })]
        );
        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );
    }
}
