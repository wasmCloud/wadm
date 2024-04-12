use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::debug;
use tracing::instrument;
use tracing::trace;

use crate::commands::{DeleteConfig, PutConfig};
use crate::events::ConfigDeleted;
use crate::server::StatusInfo;
use crate::{
    commands::Command, events::Event, model::TraitProperty, scaler::Scaler, storage::ReadStore,
};

pub struct ConfigScaler<S> {
    store: S,
    id: String,
    lattice_id: String,
    config_name: String,
    config: HashMap<String, String>,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ConfigScaler<S> {
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
            Event::ConfigSet(config_set) => {
                if config_set.config_name == self.config_name {
                    // TODO: This will only be handled by one wadm instance, need to either check the store for status
                    // or send a notification.
                    *self.status.write().await = StatusInfo::deployed("");
                }
            }
            Event::ConfigDeleted(ConfigDeleted { config_name }) => {
                if config_name == &self.config_name {
                    return self.reconcile().await;
                }
            }
            _ => {
                trace!("ConfigScaler does not support this event, ignoring");
            }
        }
        if let Event::ConfigDeleted(ConfigDeleted { config_name }) = event {
            if config_name == &self.config_name {
                return self.reconcile().await;
            }
        }
        Ok(Vec::new())
    }

    #[instrument(level = "trace", skip_all, scaler_id = %self.id)]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        // TODO: Check for config existence in the store

        debug!(self.config_name, "Putting configuration");
        *self.status.write().await = StatusInfo::reconciling("Putting configuration");
        Ok(vec![Command::PutConfig(PutConfig {
            config_name: self.config_name.clone(),
            config: self.config.clone(),
        })])
    }

    #[instrument(level = "trace", skip_all)]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        Ok(vec![Command::DeleteConfig(DeleteConfig {
            config_name: self.config_name.clone(),
        })])
    }
}

impl<S: ReadStore + Send + Sync> ConfigScaler<S> {
    /// Construct a new ConfigScaler with specified values
    pub fn new(
        store: S,
        lattice_id: &str,
        config_name: &str,
        config: &HashMap<String, String>,
    ) -> Self {
        // Hash the config to generate a unique id, used to compare scalers for uniqueness when updating
        let mut config_hasher = std::collections::hash_map::DefaultHasher::new();
        BTreeMap::from_iter(config.iter()).hash(&mut config_hasher);
        let id = format!("{config_name}-{}", config_hasher.finish());

        Self {
            store,
            id,
            lattice_id: lattice_id.to_string(),
            config_name: config_name.to_string(),
            config: config.clone(),
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}
