use anyhow::Result;
use async_trait::async_trait;
use std::hash::{DefaultHasher, Hash, Hasher};
use tokio::sync::RwLock;
use tracing::{debug, error, instrument, trace};
use wadm_types::{
    api::{StatusInfo, StatusType},
    SecretSourceProperty, TraitProperty,
};

use crate::{
    commands::{Command, DeleteConfig, PutConfig},
    events::{ConfigDeleted, ConfigSet, Event},
    scaler::Scaler,
    workers::SecretSource,
};

const SECRET_PREFIX: &str = "secret";

pub struct SecretScaler<SecretSource> {
    secret_source: SecretSource,
    id: String,
    secret_name: String,
    source: SecretSourceProperty,
    status: RwLock<StatusInfo>,
}

impl<S: SecretSource> SecretScaler<S> {
    pub fn new(secret_name: String, source: SecretSourceProperty, secret_source: S) -> Self {
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
        }
    }
}

#[async_trait]
impl<S: SecretSource + Send + Sync + Clone> Scaler for SecretScaler<S> {
    fn id(&self) -> &str {
        todo!()
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
                trace!("ConfigScaler does not support this event, ignoring");
            }
        }
        Ok(Vec::new())
    }

    #[instrument(level = "trace", skip_all, scaler_id = %self.id)]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        debug!(self.secret_name, "Fetching configuration");
        match (
            self.secret_source.get_secret(&self.secret_name).await,
            self.source,
        ) {
            // If configuration matches what's supplied, this scaler is deployed
            (Ok(Some(config)), scaler_config) if config == scaler_config => {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            // If configuration is out of sync, we put the configuration
            (Ok(_config), scaler_config) => {
                debug!(self.secret_name, "Putting secret");
                *self.status.write().await = StatusInfo::reconciling("Secret out of sync");
                Ok(vec![Command::PutConfig(PutConfig {
                    config_name: self.secret_name.clone(),
                    config: scaler_config.try_into()?,
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
