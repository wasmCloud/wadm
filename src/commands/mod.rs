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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
// In order to keep things clean, we are using untagged enum serialization. So it will just try to
// match on data without tagging (see https://serde.rs/enum-representations.html for more info on
// what the other options look like). These types are purely internal to wadm, but for greater
// flexibility in the future, we may want to write custom serialize and deserialize for people who
// may write other schedulers
#[serde(untagged)]
pub enum Command {
    StartActor(StartActor),
    StopActor(StopActor),
    StartProvider(StartProvider),
    StopProvider(StopProvider),
    PutLinkdef(PutLinkdef),
    DeleteLinkdef(DeleteLinkdef),
}

/// Struct for the StartActor command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default)]
pub struct StartActor {
    /// The OCI or bindle reference to start
    pub reference: String,
    /// The host id on which to start the actor(s)
    pub host_id: String,
    /// Number of actors to start
    pub count: usize,
}

from_impl!(StartActor);

impl PartialEq for StartActor {
    fn eq(&self, other: &StartActor) -> bool {
        self.reference == other.reference && self.host_id == other.host_id
    }
}

impl Hash for StartActor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.reference.hash(state);
        self.host_id.hash(state);
    }
}

/// Struct for the StopActor command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default)]
pub struct StopActor {
    /// The ID of the actor to stop
    pub actor_id: String,
    /// The host id on which to stop the actors
    pub host_id: String,
    /// The number of actors to stop
    pub count: usize,
}

from_impl!(StopActor);

impl PartialEq for StopActor {
    fn eq(&self, other: &StopActor) -> bool {
        self.actor_id == other.actor_id && self.host_id == other.host_id
    }
}

impl Hash for StopActor {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.actor_id.hash(state);
        self.host_id.hash(state);
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
    // TODO: Do we need to support config_json paths?
}

from_impl!(StartProvider);

impl PartialEq for StartProvider {
    fn eq(&self, other: &StartProvider) -> bool {
        self.reference == other.reference
            && self.host_id == other.host_id
            && self.link_name == other.link_name
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct StopProvider {
    /// The ID of the provider to stop
    pub provider_id: String,
    /// The host ID on which to stop the provider
    pub host_id: String,
    /// The link name of the provider to stop
    pub link_name: Option<String>,
    /// The contract ID of the provider to stop
    pub contract_id: String,
}

from_impl!(StopProvider);

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
}

from_impl!(DeleteLinkdef);
