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

use crate::commands::{DeleteConfig, PutConfig};
use crate::events::ConfigDeleted;
use crate::events::ConfigSet;
use crate::server::StatusInfo;
use crate::server::StatusType;
use crate::workers::ConfigSource;
use crate::{commands::Command, events::Event, model::TraitProperty, scaler::Scaler};

pub struct ConfigScaler<ConfigSource> {
    config_bucket: ConfigSource,
    id: String,
    config_name: String,
    config: HashMap<String, String>,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<C: ConfigSource + Send + Sync + Clone> Scaler for ConfigScaler<C> {
    fn id(&self) -> &str {
        &self.id
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
        match self.config_bucket.get_config(&self.config_name).await {
            Ok(Some(config)) if config == self.config => {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            Ok(_config) => {
                debug!(self.config_name, "Putting configuration");
                *self.status.write().await = StatusInfo::reconciling("Configuration out of sync");
                Ok(vec![Command::PutConfig(PutConfig {
                    config_name: self.config_name.clone(),
                    config: self.config.clone(),
                })])
            }
            Err(e) => {
                error!(error = %e, "Configscaler failed to fetch configuration");
                *self.status.write().await = StatusInfo::failed(&e.to_string());
                Ok(Vec::new())
            }
        }
    }

    #[instrument(level = "trace", skip_all)]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        Ok(vec![Command::DeleteConfig(DeleteConfig {
            config_name: self.config_name.clone(),
        })])
    }
}

impl<C: ConfigSource> ConfigScaler<C> {
    /// Construct a new ConfigScaler with specified values
    pub fn new(config_bucket: C, config_name: &str, config: &HashMap<String, String>) -> Self {
        // Hash the config to generate a unique id, used to compare scalers for uniqueness when updating
        let mut config_hasher = std::collections::hash_map::DefaultHasher::new();
        BTreeMap::from_iter(config.iter()).hash(&mut config_hasher);
        let id = format!("{config_name}-{}", config_hasher.finish());

        Self {
            config_bucket,
            id,
            config_name: config_name.to_string(),
            config: config.clone(),
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}
