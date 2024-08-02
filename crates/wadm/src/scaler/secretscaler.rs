use anyhow::{Context, Result};
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{debug, error, instrument, trace};
use wadm_types::{
    api::{StatusInfo, StatusType},
    Policy, SecretProperty, TraitProperty,
};
use wasmcloud_secrets_types::SecretConfig;

use crate::{
    commands::{Command, DeleteConfig, PutConfig},
    events::{ConfigDeleted, ConfigSet, Event},
    scaler::Scaler,
    workers::SecretSource,
};

use super::compute_id_sha256;

const SECRET_SCALER_KIND: &str = "SecretScaler";

pub struct SecretScaler<SecretSource> {
    secret_source: SecretSource,
    /// The key to use in the configdata bucket for this secret
    secret_name: String,
    secret_config: SecretConfig,
    id: String,
    status: RwLock<StatusInfo>,
}

impl<S: SecretSource> SecretScaler<S> {
    pub fn new(
        secret_name: String,
        policy: Policy,
        secret_property: SecretProperty,
        secret_source: S,
    ) -> Self {
        // Compute the id of this scaler based on all of the values that make it unique.
        // This is used during upgrades to determine if a scaler is the same as a previous one.
        let mut id_parts = vec![
            secret_name.as_str(),
            policy.name.as_str(),
            policy.policy_type.as_str(),
            secret_property.name.as_str(),
            secret_property.properties.policy.as_str(),
            secret_property.properties.key.as_str(),
        ];
        if let Some(version) = secret_property.properties.version.as_ref() {
            id_parts.push(version.as_str());
        }
        id_parts.extend(
            policy
                .properties
                .iter()
                .flat_map(|(k, v)| vec![k.as_str(), v.as_str()]),
        );
        let id = compute_id_sha256(&id_parts);

        let secret_config = config_from_manifest_structures(policy, secret_property)
            .expect("failed to create secret config from policy and secret properties");

        Self {
            id,
            secret_name,
            secret_config,
            secret_source,
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}

#[async_trait]
impl<S: SecretSource + Send + Sync + Clone> Scaler for SecretScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        SECRET_SCALER_KIND
    }

    fn name(&self) -> String {
        self.secret_config.name.to_string()
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
        match self.secret_source.get_secret(&self.secret_name).await {
            // If configuration matches what's supplied, this scaler is deployed
            Ok(Some(config)) if config == self.secret_config => {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            // If configuration is out of sync, we put the configuration
            Ok(_config) => {
                debug!(self.secret_name, "Putting secret");

                match self.secret_config.clone().try_into() {
                    Ok(config) => {
                        *self.status.write().await = StatusInfo::reconciling("Secret out of sync");

                        Ok(vec![Command::PutConfig(PutConfig {
                            config_name: self.secret_name.clone(),
                            config,
                        })])
                    }
                    Err(e) => {
                        *self.status.write().await = StatusInfo::failed(&format!(
                            "Failed to convert secret config to map: {}.",
                            e
                        ));
                        Ok(vec![])
                    }
                }
            }
            Err(e) => {
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

/// Merge policy and properties into a [`SecretConfig`] for later use.
fn config_from_manifest_structures(
    policy: Policy,
    reference: SecretProperty,
) -> anyhow::Result<SecretConfig> {
    let mut policy_properties = policy.properties.clone();
    let backend = policy_properties
        .remove("backend")
        .context("policy did not have a backend property")?;
    Ok(SecretConfig::new(
        reference.name.clone(),
        backend,
        reference.properties.key.clone(),
        reference.properties.field.clone(),
        reference.properties.version.clone(),
        policy_properties
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect(),
    ))
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
                field: None,
                version: None,
            },
        };

        let secret_scaler = super::SecretScaler::new(
            secret.name.clone(),
            policy.clone(),
            secret.clone(),
            lattice.clone(),
        );

        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        let cfg = config_from_manifest_structures(policy, secret.clone())
            .expect("failed to merge policy");

        assert_eq!(
            secret_scaler
                .reconcile()
                .await
                .expect("reconcile did not succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: secret.name.clone(),
                config: cfg.clone().try_into().expect("should convert to map"),
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
                config: cfg.clone().try_into().expect("should convert to map"),
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
                config: cfg.clone().try_into().expect("should convert to map"),
            })]
        );
        assert_eq!(
            secret_scaler.status().await.status_type,
            StatusType::Reconciling
        );
    }
}
