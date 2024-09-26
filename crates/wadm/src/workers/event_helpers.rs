use anyhow::{bail, Context};
use async_nats::jetstream::stream::Stream;
use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use wasmcloud_secrets_types::SecretConfig;

use tracing::{debug, instrument, trace, warn};
use wadm_types::api::Status;
use wasmcloud_control_interface::{HostInventory, Link};

use crate::{commands::Command, publisher::Publisher, APP_SPEC_ANNOTATION};

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
    async fn get_links(&self) -> anyhow::Result<Vec<Link>>;
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
    async fn get_secret(&self, name: &str) -> anyhow::Result<Option<SecretConfig>>;
}

/// Converts the configuration map of strings to a secret config
pub fn secret_config_from_map(map: HashMap<String, String>) -> anyhow::Result<SecretConfig> {
    match (
        map.get("name"),
        map.get("backend"),
        map.get("key"),
        map.get("policy"),
        map.get("type"),
    ) {
        (None, _, _, _, _) => bail!("missing name field in secret config"),
        (_, None, _, _, _) => bail!("missing backend field in secret config"),
        (_, _, None, _, _) => bail!("missing key field in secret config"),
        (_, _, _, None, _) => bail!("missing policy field in secret config"),
        (_, _, _, _, None) => bail!("missing type field in secret config"),
        (Some(name), Some(backend), Some(key), Some(policy), Some(secret_type)) => {
            Ok(SecretConfig {
                name: name.to_string(),
                backend: backend.to_string(),
                key: key.to_string(),
                field: map.get("field").map(|f| f.to_string()),
                version: map.get("version").map(|v| v.to_string()),
                policy: serde_json::from_str(policy)
                    .context("failed to deserialize policy from string")?,
                secret_type: secret_type.to_string(),
            })
        }
    }
}

#[async_trait::async_trait]
impl ClaimsSource for wasmcloud_control_interface::Client {
    async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>> {
        match self.get_claims().await.map_err(|e| anyhow::anyhow!("{e}")) {
            Ok(ctl_resp) if ctl_resp.succeeded() => {
                let claims = ctl_resp.data().context("missing claims data")?.to_owned();
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
            ctl_resp if ctl_resp.succeeded() && ctl_resp.data().is_some() => Ok(ctl_resp
                .into_data()
                .context("missing host inventory data")?),
            ctl_resp => Err(anyhow::anyhow!(
                "Failed to get inventory for host {host_id}, {}",
                ctl_resp.message()
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
    async fn get_links(&self) -> anyhow::Result<Vec<Link>> {
        match self
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            ctl_resp if ctl_resp.succeeded() && ctl_resp.data().is_some() => {
                Ok(ctl_resp.into_data().context("missing link data")?)
            }
            ctl_resp => Err(anyhow::anyhow!(
                "Failed to get links, {}",
                ctl_resp.message()
            )),
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
            ctl_resp if ctl_resp.succeeded() && ctl_resp.data().is_some() => {
                Ok(ctl_resp.into_data())
            }
            // TODO(https://github.com/wasmCloud/wasmCloud/issues/1906): The control interface should return a None when config isn't found
            // instead of returning an error.
            ctl_resp => {
                debug!("Failed to get config for {name}, {}", ctl_resp.message());
                Ok(None)
            }
        }
    }
}

#[async_trait::async_trait]
impl SecretSource for wasmcloud_control_interface::Client {
    async fn get_secret(&self, name: &str) -> anyhow::Result<Option<SecretConfig>> {
        match self
            .get_config(name)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            ctl_resp if ctl_resp.succeeded() && ctl_resp.data().is_some() => {
                secret_config_from_map(ctl_resp.into_data().context("missing secret data")?)
                    .map(Some)
            }
            ctl_resp if ctl_resp.data().is_none() => {
                debug!("Failed to get secret for {name}, {}", ctl_resp.message());
                Ok(None)
            }
            ctl_resp => {
                debug!("Failed to get secret for {name}, {}", ctl_resp.message());
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
    pub async fn publish_status(&self, name: &str, status: Status) -> anyhow::Result<()> {
        let topic = format!("{}.{name}", self.topic_prefix);

        // NOTE(brooksmtownsend): This direct get may not always query the jetstream leader. In the
        // worst case where the last message isn't all the way updated, we may publish a duplicate
        // status. This is an acceptable tradeoff to not have to query the leader directly every time.
        let prev_status = if let Some(status_stream) = &self.status_stream {
            status_stream
                .direct_get_last_for_subject(&topic)
                .await
                .map(|m| serde_json::from_slice::<Status>(&m.payload).ok())
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
