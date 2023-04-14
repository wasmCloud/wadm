//! Type implementations for commands issued to compensate for state changes

use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};

macro_rules! from_impl {
    ($t:ident) => {
        impl From<$t> for Command {
            fn from(value: $t) -> Command {
                Command::$t(value)
            }
        }
    };
}

/// All possible compensatory commands for a lattice
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum Command {
    StartActor(StartActor),
    StopActor(StopActor),
    StartProvider(StartProvider),
    StopProvider(StopProvider),
    PutLinkdef(PutLinkdef),
    DeleteLinkdef(DeleteLinkdef),
}

/// Struct for the StartActor command
#[derive(Clone, Debug, Serialize, Deserialize, Default, Eq)]
pub struct StartActor {
    /// The OCI or bindle reference to start
    pub reference: String,
    /// The host id on which to start the actor(s)
    pub host_id: String,
    /// Number of actors to start
    pub count: usize,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    /// Additional annotations to attach on this command
    pub annotations: HashMap<String, String>,
}

from_impl!(StartActor);

impl PartialEq for StartActor {
    fn eq(&self, other: &Self) -> bool {
        self.reference == other.reference
            && self.host_id == other.host_id
            && self.count == other.count
            && self.model_name == other.model_name
            && self.annotations == other.annotations
    }
}

/// Struct for the StopActor command
#[derive(Clone, Debug, Serialize, Deserialize, Default, Eq)]
pub struct StopActor {
    /// The ID of the actor to stop
    pub actor_id: String,
    /// The host id on which to stop the actors
    pub host_id: String,
    /// The number of actors to stop
    pub count: usize,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    /// Additional annotations to attach on this command
    pub annotations: HashMap<String, String>,
}

from_impl!(StopActor);

impl PartialEq for StopActor {
    fn eq(&self, other: &Self) -> bool {
        self.actor_id == other.actor_id
            && self.host_id == other.host_id
            && self.count == other.count
            && self.model_name == other.model_name
            && self.annotations == other.annotations
    }
}

/// Struct for the StartProvider command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default)]
pub struct StartProvider {
    /// The OCI or bindle reference to start
    pub reference: String,
    /// The host id on which to start the actor(s)
    pub host_id: String,
    /// The link name for the provider
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_name: Option<String>,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    // TODO: Do we need to support config_json paths?
    /// Additional annotations to attach on this command
    pub annotations: HashMap<String, String>,
}

from_impl!(StartProvider);

impl PartialEq for StartProvider {
    fn eq(&self, other: &StartProvider) -> bool {
        self.reference == other.reference
            && self.host_id == other.host_id
            && self.link_name == other.link_name
            && self.model_name == other.model_name
    }
}

impl Hash for StartProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.reference.hash(state);
        self.host_id.hash(state);
        self.link_name.hash(state);
    }
}

/// Struct for the StopProvider command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default)]
pub struct StopProvider {
    /// The ID of the provider to stop
    pub provider_id: String,
    /// The host ID on which to stop the provider
    pub host_id: String,
    /// The link name of the provider to stop
    pub link_name: Option<String>,
    /// The contract ID of the provider to stop
    pub contract_id: String,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    /// Additional annotations to attach on this command
    pub annotations: HashMap<String, String>,
}

from_impl!(StopProvider);

impl PartialEq for StopProvider {
    fn eq(&self, other: &StopProvider) -> bool {
        self.provider_id == other.provider_id
            && self.host_id == other.host_id
            && self.link_name == other.link_name
            && self.model_name == other.model_name
    }
}

impl Hash for StopProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.provider_id.hash(state);
        self.host_id.hash(state);
        self.link_name.hash(state);
    }
}

/// Struct for the PutLinkdef command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default)]
pub struct PutLinkdef {
    /// The ID of the actor to link
    pub actor_id: String,
    /// The ID of the provider to link
    pub provider_id: String,
    /// The link name of the provider to link
    pub link_name: String,
    /// The contract ID of the provider to link
    pub contract_id: String,
    /// Values to set for the link
    pub values: HashMap<String, String>,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
}

from_impl!(PutLinkdef);

impl PartialEq for PutLinkdef {
    fn eq(&self, other: &PutLinkdef) -> bool {
        self.actor_id == other.actor_id
            && self.provider_id == other.provider_id
            && self.link_name == other.link_name
            && self.contract_id == other.contract_id
    }
}

impl Hash for PutLinkdef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.actor_id.hash(state);
        self.provider_id.hash(state);
        self.link_name.hash(state);
        self.contract_id.hash(state);
    }
}

/// Struct for the DeleteLinkdef command
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct DeleteLinkdef {
    /// The ID of the actor to unlink
    pub actor_id: String,
    /// The contract ID of the provider to unlink
    pub contract_id: String,
    /// The link name to unlink
    pub link_name: String,
    /// The provider ID of the provider to unlink
    pub provider_id: String,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
}

from_impl!(DeleteLinkdef);
