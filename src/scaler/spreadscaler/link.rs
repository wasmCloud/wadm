use std::collections::HashMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::OnceCell;

use crate::{
    commands::{Command, PutLinkdef},
    events::{Event, LinkdefDeleted},
    scaler::Scaler,
    storage::ReadStore,
    DEFAULT_LINK_NAME,
};

/// Config for a LinkSpreadConfig
pub struct LinkSpreadConfig {
    /// OCI, Bindle, or File reference for the actor to link
    actor_reference: String,
    /// OCI, Bindle, or File reference for the provider to link
    provider_reference: String,
    /// Contract ID the provider implements
    provider_contract_id: String,
    /// Contract ID the provider implements
    provider_link_name: String,
    /// The name of the wadm model this SpreadScaler is under
    model_name: String,
    /// Link configuration values
    values: HashMap<String, String>,
}

/// The LinkSpreadScaler ensures that link configuration exists on a specified lattice.
pub struct LinkSpreadScaler<S: ReadStore + Send + Sync> {
    pub config: LinkSpreadConfig,
    store: S,
    /// Actor ID, stored in a OnceCell to facilitate more efficient fetches
    actor_id: OnceCell<String>,
    /// Provider ID, stored in a OnceCell to facilitate more efficient fetches
    provider_id: OnceCell<String>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync> Scaler for LinkSpreadScaler<S> {
    type Config = LinkSpreadConfig;

    async fn update_config(&mut self, config: Self::Config) -> Result<Vec<Command>> {
        self.config = config;
        self.reconcile().await
    }

    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        match event {
            // We can only publish links once we know actor and provider IDs,
            // so we should pay attention to the Started events if we don't know them yet
            Event::ActorStarted(actor_started)
                if !self.actor_id.initialized()
                    && actor_started.image_ref == self.config.actor_reference =>
            {
                self.reconcile().await
            }
            Event::ProviderStarted(provider_started)
                if !self.provider_id.initialized()
                    && provider_started.contract_id == self.config.provider_contract_id
                    && provider_started.image_ref == self.config.provider_reference
                    && provider_started.link_name == self.config.provider_link_name =>
            {
                self.reconcile().await
            }
            Event::LinkdefDeleted(LinkdefDeleted { linkdef })
                if linkdef.contract_id == self.config.provider_contract_id
                //TODO: need functions
                    // && linkdef.actor_id == self.actor_id()
                    // && linkdef.provider_id == self.provider_id()
                    && linkdef.link_name == self.config.provider_link_name =>
            {
                self.reconcile().await
            }
            _ => Ok(Vec::new()),
        }
    }

    async fn reconcile(&self) -> Result<Vec<Command>> {
        if let (Some(actor_id), Some(provider_id)) = (self.actor_id.get(), self.provider_id.get()) {
            //TODO: check to see if linkdef exists. Might need to be deleted first if values differ
            Ok(vec![Command::PutLinkdef(PutLinkdef {
                actor_id: actor_id.to_owned(),
                provider_id: provider_id.to_owned(),
                link_name: self.config.provider_link_name.to_owned(),
                contract_id: self.config.provider_contract_id.to_owned(),
                values: self.config.values.to_owned(),
                model_name: self.config.model_name.to_owned(),
            })])
        } else {
            Ok(Vec::new())
        }
    }
}

impl<S: ReadStore + Send + Sync> LinkSpreadScaler<S> {
    /// Construct a new LinkSpreadScaler with specified configuration values
    pub fn new(
        store: S,
        actor_reference: String,
        provider_reference: String,
        provider_contract_id: String,
        provider_link_name: Option<String>,
        values: HashMap<String, String>,
        model_name: String,
    ) -> Self {
        Self {
            store,
            actor_id: OnceCell::new(),
            provider_id: OnceCell::new(),
            config: LinkSpreadConfig {
                actor_reference,
                provider_reference,
                provider_contract_id,
                provider_link_name: provider_link_name
                    .unwrap_or_else(|| DEFAULT_LINK_NAME.to_string()),
                model_name,
                values,
            },
        }
    }
}
