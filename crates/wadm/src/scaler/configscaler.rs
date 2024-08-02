use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::error;
use tracing::instrument;
use tracing::trace;
use wadm_types::{
    api::{StatusInfo, StatusType},
    TraitProperty,
};

use crate::commands::{DeleteConfig, PutConfig};
use crate::events::{ConfigDeleted, ConfigSet};
use crate::workers::ConfigSource;
use crate::{commands::Command, events::Event, scaler::Scaler};

const CONFIG_SCALER_KIND: &str = "ConfigScaler";

pub struct ConfigScaler<ConfigSource> {
    config_bucket: ConfigSource,
    id: String,
    config_name: String,
    // NOTE(#263): Introducing storing the entire configuration in-memory has the potential to get
    // fairly heavy if the configuration is large. We should consider a more efficient way to store
    // this by fetching configuration from the manifest when it's needed, for example.
    config: Option<HashMap<String, String>>,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<C: ConfigSource + Send + Sync + Clone> Scaler for ConfigScaler<C> {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        CONFIG_SCALER_KIND
    }

    fn name(&self) -> String {
        self.config_name.to_string()
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, _config: TraitProperty) -> Result<Vec<Command>> {
        debug!("ConfigScaler does not support updating config, ignoring");
        Ok(vec![])
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id))]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        match event {
            Event::ConfigSet(ConfigSet { config_name })
            | Event::ConfigDeleted(ConfigDeleted { config_name }) => {
                if config_name == &self.config_name {
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
                trace!("ConfigScaler does not support this event, ignoring");
            }
        }
        Ok(Vec::new())
    }

    #[instrument(level = "trace", skip_all, scaler_id = %self.id)]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        debug!(self.config_name, "Fetching configuration");
        match (
            self.config_bucket.get_config(&self.config_name).await,
            self.config.as_ref(),
        ) {
            // If configuration is not supplied to the scaler, we just ensure that it exists
            (Ok(Some(_config)), None) => {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            // If configuration is not supplied and doesn't exist, we enter a failed state
            (Ok(None), None) => {
                *self.status.write().await = StatusInfo::failed(&format!(
                    "Specified configuration {} does not exist",
                    self.config_name
                ));
                Ok(Vec::new())
            }
            // If configuration matches what's supplied, this scaler is deployed
            (Ok(Some(config)), Some(scaler_config)) if &config == scaler_config => {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            // If configuration is out of sync, we put the configuration
            (Ok(_config), Some(scaler_config)) => {
                debug!(self.config_name, "Putting configuration");
                *self.status.write().await = StatusInfo::reconciling("Configuration out of sync");
                Ok(vec![Command::PutConfig(PutConfig {
                    config_name: self.config_name.clone(),
                    config: scaler_config.clone(),
                })])
            }
            (Err(e), _) => {
                error!(error = %e, "Configscaler failed to fetch configuration");
                *self.status.write().await = StatusInfo::failed(&e.to_string());
                Ok(Vec::new())
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        if self.config.is_some() {
            Ok(vec![Command::DeleteConfig(DeleteConfig {
                config_name: self.config_name.clone(),
            })])
        } else {
            // This configuration is externally managed, don't delete it
            Ok(Vec::new())
        }
    }
}

impl<C: ConfigSource> ConfigScaler<C> {
    /// Construct a new ConfigScaler with specified values
    pub fn new(
        config_bucket: C,
        config_name: &str,
        config: Option<&HashMap<String, String>>,
    ) -> Self {
        let mut id = config_name.to_string();
        // Hash the config to generate a unique id, used to compare scalers for uniqueness when updating
        if let Some(config) = config.as_ref() {
            let mut config_hasher = std::collections::hash_map::DefaultHasher::new();
            BTreeMap::from_iter(config.iter()).hash(&mut config_hasher);
            id.extend(format!("-{}", config_hasher.finish()).chars());
        }

        Self {
            config_bucket,
            id,
            config_name: config_name.to_string(),
            config: config.cloned(),
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashMap};

    use wadm_types::{api::StatusType, ConfigProperty};

    use crate::{
        commands::{Command, PutConfig},
        events::{ComponentScaled, ConfigDeleted, Event, HostHeartbeat},
        scaler::{configscaler::ConfigScaler, Scaler},
        test_util::TestLatticeSource,
    };

    #[tokio::test]
    /// Ensure that the config scaler reacts properly to events, fetching configuration
    /// when it is out of sync and ignoring irrelevant events.
    async fn test_configscaler() {
        let lattice = TestLatticeSource {
            claims: HashMap::new(),
            inventory: Default::default(),
            links: Vec::new(),
            config: HashMap::new(),
        };

        let config = ConfigProperty {
            name: "test_config".to_string(),
            properties: Some(HashMap::from_iter(vec![(
                "key".to_string(),
                "value".to_string(),
            )])),
        };

        let config_scaler =
            ConfigScaler::new(lattice.clone(), &config.name, config.properties.as_ref());

        assert_eq!(
            config_scaler.status().await.status_type,
            StatusType::Reconciling
        );
        assert_eq!(
            config_scaler
                .reconcile()
                .await
                .expect("reconcile should succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: config.name.clone(),
                config: config.properties.clone().expect("properties not found"),
            })]
        );
        assert_eq!(
            config_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        // Configuration deleted, relevant
        assert_eq!(
            config_scaler
                .handle_event(&Event::ConfigDeleted(ConfigDeleted {
                    config_name: config.name.clone()
                }))
                .await
                .expect("handle_event should succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: config.name.clone(),
                config: config.properties.clone().expect("properties not found"),
            })]
        );
        assert_eq!(
            config_scaler.status().await.status_type,
            StatusType::Reconciling
        );
        // Configuration deleted, irrelevant
        assert_eq!(
            config_scaler
                .handle_event(&Event::ConfigDeleted(ConfigDeleted {
                    config_name: "some_other_config".to_string()
                }))
                .await
                .expect("handle_event should succeed"),
            vec![]
        );
        assert_eq!(
            config_scaler.status().await.status_type,
            StatusType::Reconciling
        );
        // Periodic reconcile with host heartbeat
        assert_eq!(
            config_scaler
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
                config_name: config.name.clone(),
                config: config.properties.clone().expect("properties not found"),
            })]
        );
        assert_eq!(
            config_scaler.status().await.status_type,
            StatusType::Reconciling
        );
        // Ignore other event
        assert_eq!(
            config_scaler
                .handle_event(&Event::ComponentScaled(ComponentScaled {
                    annotations: BTreeMap::new(),
                    claims: None,
                    image_ref: "foo".to_string(),
                    max_instances: 0,
                    component_id: "fooo".to_string(),
                    host_id: "hostid".to_string()
                }))
                .await
                .expect("handle_event should succeed"),
            vec![]
        );
        assert_eq!(
            config_scaler.status().await.status_type,
            StatusType::Reconciling
        );

        // Create lattice where config is present
        let lattice2 = TestLatticeSource {
            claims: HashMap::new(),
            inventory: Default::default(),
            links: Vec::new(),
            config: HashMap::from_iter(vec![(
                config.name.clone(),
                config.properties.clone().expect("properties not found"),
            )]),
        };

        let config_scaler2 = ConfigScaler::new(lattice2, &config.name, config.properties.as_ref());

        assert_eq!(
            config_scaler2
                .reconcile()
                .await
                .expect("reconcile should succeed"),
            vec![]
        );
        assert_eq!(
            config_scaler2.status().await.status_type,
            StatusType::Deployed
        );
        // Periodic reconcile with host heartbeat
        assert_eq!(
            config_scaler2
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
            vec![]
        );
        assert_eq!(
            config_scaler2.status().await.status_type,
            StatusType::Deployed
        );

        // Create lattice where config is present but with the wrong values
        let lattice3 = TestLatticeSource {
            claims: HashMap::new(),
            inventory: Default::default(),
            links: Vec::new(),
            config: HashMap::from_iter(vec![(
                config.name.clone(),
                HashMap::from_iter(vec![("key".to_string(), "wrong_value".to_string())]),
            )]),
        };
        let config_scaler3 =
            ConfigScaler::new(lattice3.clone(), &config.name, config.properties.as_ref());

        assert_eq!(
            config_scaler3
                .reconcile()
                .await
                .expect("reconcile should succeed"),
            vec![Command::PutConfig(PutConfig {
                config_name: config.name.clone(),
                config: config.properties.clone().expect("properties not found"),
            })]
        );
        assert_eq!(
            config_scaler3.status().await.status_type,
            StatusType::Reconciling
        );

        // Test supplied name but not supplied config
        let config_scaler4 = ConfigScaler::new(lattice3, &config.name, None);
        assert_eq!(
            config_scaler4
                .reconcile()
                .await
                .expect("reconcile should succeed"),
            vec![]
        );
        assert_eq!(
            config_scaler4.status().await.status_type,
            StatusType::Deployed
        );

        let config_scaler5 = ConfigScaler::new(lattice, &config.name, None);
        assert_eq!(
            config_scaler5
                .reconcile()
                .await
                .expect("reconcile should succeed"),
            vec![]
        );
        assert_eq!(
            config_scaler5.status().await.status_type,
            StatusType::Failed
        );
    }
}
