use async_nats::jetstream::stream::Stream;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;

use tracing::{debug, instrument, trace, warn};
use wadm_types::{api::StatusInfo, SecretSourceProperty};
use wasmcloud_control_interface::{CtlResponse, HostInventory, InterfaceLinkDefinition};

use crate::{
    commands::Command, publisher::Publisher, scaler::secretscaler::SECRET_CONFIG_PREFIX,
    APP_SPEC_ANNOTATION,
};

/// A subset of needed claims to help populate state
#[derive(Debug, Clone)]
pub struct Claims {
    pub name: String,
    pub capabilities: Vec<String>,
    pub issuer: String,
}

/// A trait for anything that can fetch a set of claims information about components.
///
/// NOTE: This trait right now exists as a convenience for two things: First, testing. Without
/// something like this we require a network connection to unit test. Second, there is no concrete
/// claims type returned from the control interface client. This allows us to abstract that away
/// until such time that we do export one and we'll be able to do so without breaking our API
#[async_trait::async_trait]
pub trait ClaimsSource {
    async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>>;
}

/// NOTE(brooksmtownsend): This trait exists in order to query the hosts inventory
/// upon receiving a heartbeat since the heartbeat doesn't contain enough
/// information to properly update the stored data for components
#[async_trait::async_trait]
pub trait InventorySource {
    async fn get_inventory(&self, host_id: &str) -> anyhow::Result<HostInventory>;
}

/// A trait for anything that can fetch the links in a lattice
///
/// NOTE: This trait right now exists as a convenience for testing. It isn't ideal to have this just
/// due to testing, but it does allow us to abstract away the concrete type of the client
#[async_trait::async_trait]
pub trait LinkSource {
    async fn get_links(&self) -> anyhow::Result<Vec<InterfaceLinkDefinition>>;
}

/// A trait for anything that can fetch a piece of named configuration
///
/// In the future this could be expanded to fetch more than just a single piece of configuration,
/// but for now it's limited to a single config in an attempt to keep the scope of fetching
/// configuration small, and efficiently pass around data.
#[async_trait::async_trait]
pub trait ConfigSource {
    async fn get_config(&self, name: &str) -> anyhow::Result<Option<HashMap<String, String>>>;
}

/// A trait for anything that can fetch a secret.
#[async_trait::async_trait]
pub trait SecretSource {
    async fn get_secret(&self, name: &str) -> anyhow::Result<Option<SecretSourceProperty>>;
}

#[async_trait::async_trait]
impl ClaimsSource for wasmcloud_control_interface::Client {
    async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>> {
        match self.get_claims().await.map_err(|e| anyhow::anyhow!("{e}")) {
            Ok(CtlResponse {
                success: true,
                response: Some(claims),
                ..
            }) => {
                Ok(claims
                    .into_iter()
                    .filter_map(|mut claim| {
                        // NOTE(thomastaylor312): I'm removing instead of getting since we own the data and I
                        // don't want to clone every time we do this

                        // If we don't find a subject, we can't actually get the component ID, so skip this one
                        Some((
                            claim.remove("sub")?,
                            Claims {
                                name: claim.remove("name").unwrap_or_default(),
                                capabilities: claim
                                    .remove("caps")
                                    .map(|raw| raw.split(',').map(|s| s.to_owned()).collect())
                                    .unwrap_or_default(),
                                issuer: claim.remove("iss").unwrap_or_default(),
                            },
                        ))
                    })
                    .collect())
            }
            _ => Err(anyhow::anyhow!("Failed to get claims")),
        }
    }
}

#[async_trait::async_trait]
impl InventorySource for wasmcloud_control_interface::Client {
    async fn get_inventory(&self, host_id: &str) -> anyhow::Result<HostInventory> {
        match self
            .get_host_inventory(host_id)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            CtlResponse {
                success: true,
                response: Some(host_inventory),
                ..
            } => Ok(host_inventory),
            CtlResponse { message, .. } => Err(anyhow::anyhow!(
                "Failed to get inventory for host {host_id}, {message}"
            )),
        }
    }
}

// NOTE(thomastaylor312): A future improvement here that would make things more efficient is if this
// was just a cache of the links. On startup, it could fetch once, and then it could subscribe to
// the KV store for updates. This would allow us to not have to fetch every time we need to get
// links
#[async_trait::async_trait]
impl LinkSource for wasmcloud_control_interface::Client {
    async fn get_links(&self) -> anyhow::Result<Vec<InterfaceLinkDefinition>> {
        match self
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            CtlResponse {
                success: true,
                response: Some(links),
                ..
            } => Ok(links),
            CtlResponse { message, .. } => Err(anyhow::anyhow!("Failed to get links, {message}")),
        }
    }
}

#[async_trait::async_trait]
impl ConfigSource for wasmcloud_control_interface::Client {
    async fn get_config(&self, name: &str) -> anyhow::Result<Option<HashMap<String, String>>> {
        match self
            .get_config(name)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            CtlResponse {
                success: true,
                response: Some(config),
                ..
            } => Ok(Some(config)),
            // TODO(https://github.com/wasmCloud/wasmCloud/issues/1906): The control interface should return a None when config isn't found
            // instead of returning an error.
            CtlResponse { message, .. } => {
                debug!("Failed to get config for {name}, {message}");
                Ok(None)
            }
        }
    }
}

#[async_trait::async_trait]
impl SecretSource for wasmcloud_control_interface::Client {
    async fn get_secret(&self, name: &str) -> anyhow::Result<Option<SecretSourceProperty>> {
        match self
            .get_config(format!("{SECRET_CONFIG_PREFIX}_{name}").as_str())
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            CtlResponse {
                success: true,
                response: Some(secret),
                ..
            } => Ok(Some(secret.try_into()?)),
            CtlResponse {
                message,
                response: None,
                ..
            } => {
                debug!("Failed to get secret for {name}, {message}");
                Ok(None)
            }
            CtlResponse { message, .. } => {
                debug!("Failed to get secret for {name}, {message}");
                Ok(None)
            }
        }
    }
}

/// A struct for publishing status updates
#[derive(Clone)]
pub struct StatusPublisher<Pub> {
    publisher: Pub,
    // Stream for querying current status to avoid duplicate updates
    status_stream: Option<Stream>,
    // Topic prefix, e.g. wadm.status.default
    topic_prefix: String,
}

impl<Pub> StatusPublisher<Pub> {
    /// Creates an new status publisher configured with the given publisher that will send to the
    /// manifest status topic using the given prefix
    pub fn new(
        publisher: Pub,
        status_stream: Option<Stream>,
        topic_prefix: &str,
    ) -> StatusPublisher<Pub> {
        StatusPublisher {
            publisher,
            status_stream,
            topic_prefix: topic_prefix.to_owned(),
        }
    }
}

impl<Pub: Publisher> StatusPublisher<Pub> {
    #[instrument(level = "trace", skip(self))]
    pub async fn publish_status(&self, name: &str, status: StatusInfo) -> anyhow::Result<()> {
        let topic = format!("{}.{name}", self.topic_prefix);

        // NOTE(brooksmtownsend): This direct get may not always query the jetstream leader. In the
        // worst case where the last message isn't all the way updated, we may publish a duplicate
        // status. This is an acceptable tradeoff to not have to query the leader directly every time.
        let prev_status = if let Some(status_stream) = &self.status_stream {
            status_stream
                .direct_get_last_for_subject(&topic)
                .await
                .map(|m| serde_json::from_slice::<StatusInfo>(&m.payload).ok())
                .ok()
                .flatten()
        } else {
            None
        };

        match prev_status {
            // If the status hasn't changed, skip publishing
            Some(prev_status) if prev_status == status => {
                trace!(%name, "Status hasn't changed since last update. Skipping");
                Ok(())
            }
            _ => {
                self.publisher
                    .publish(serde_json::to_vec(&status)?, Some(&topic))
                    .await
            }
        }
    }
}

/// A struct for publishing commands
#[derive(Clone)]
pub struct CommandPublisher<Pub> {
    publisher: Pub,
    topic: String,
}

impl<Pub> CommandPublisher<Pub> {
    /// Creates an new command publisher configured with the given publisher that will send to the
    /// specified topic
    pub fn new(publisher: Pub, topic: &str) -> CommandPublisher<Pub> {
        CommandPublisher {
            publisher,
            topic: topic.to_owned(),
        }
    }
}

impl<Pub: Publisher> CommandPublisher<Pub> {
    #[instrument(level = "trace", skip(self))]
    pub async fn publish_commands(&self, commands: Vec<Command>) -> anyhow::Result<()> {
        futures::future::join_all(
            commands
                .into_iter()
                // Generally commands are purely internal to wadm and so shouldn't have an error serializing. If it does, warn and continue onward
                .filter_map(|command| {
                    match serde_json::to_vec(&command) {
                        Ok(data) => Some(data),
                        Err(e) => {
                            warn!(error = %e, ?command, "Got malformed command when trying to serialize. Skipping this command");
                            None
                        }
                    }
                })
                .map(|data| self.publisher.publish(data, Some(&self.topic))),
        )
        .await
        .into_iter()
        .collect::<anyhow::Result<()>>()
    }
}

/// Inserts managed annotations to the given `annotations` HashMap.
pub fn insert_managed_annotations(annotations: &mut BTreeMap<String, String>, model_name: &str) {
    annotations.extend([
        (
            crate::MANAGED_BY_ANNOTATION.to_owned(),
            crate::MANAGED_BY_IDENTIFIER.to_owned(),
        ),
        (APP_SPEC_ANNOTATION.to_owned(), model_name.to_owned()),
    ])
}
