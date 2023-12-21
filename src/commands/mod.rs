//! Type implementations for commands issued to compensate for state changes

use std::{
    collections::{BTreeMap, HashMap},
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};

use crate::{
    events::{Event, ProviderClaims, ProviderStartFailed, ProviderStarted},
    model::CapabilityConfig,
    workers::insert_managed_annotations,
};

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
    ScaleActor(ScaleActor),
    StartProvider(StartProvider),
    StopProvider(StopProvider),
    PutLinkdef(PutLinkdef),
    DeleteLinkdef(DeleteLinkdef),
}

impl Command {
    /// Generates the corresponding event for a [Command](Command) in the form of a two-tuple ([Event](Event), Option<Event>)
    ///
    /// # Arguments
    /// `model_name` - The model name that the command satisfies, needed to compute the proper annotations
    ///
    /// # Return
    /// - The first element in the tuple corresponds to the "success" event a host would output after completing this command
    /// - The second element in the tuple corresponds to an optional "failure" event that a host could output if processing fails
    pub fn corresponding_event(&self, model_name: &str) -> Option<(Event, Option<Event>)> {
        match self {
            Command::StartProvider(StartProvider {
                annotations,
                reference,
                host_id,
                link_name,
                ..
            }) => {
                let mut annotations = annotations.to_owned();
                insert_managed_annotations(&mut annotations, model_name);
                Some((
                    Event::ProviderStarted(ProviderStarted {
                        annotations: annotations.to_owned(),
                        claims: ProviderClaims::default(),
                        image_ref: reference.to_owned(),
                        link_name: link_name
                            .to_owned()
                            .unwrap_or_else(|| "default".to_string()),
                        host_id: host_id.to_owned(),
                        // We don't know these fields from the command
                        contract_id: String::with_capacity(0),
                        instance_id: String::with_capacity(0),
                        public_key: String::with_capacity(0),
                    }),
                    Some(Event::ProviderStartFailed(ProviderStartFailed {
                        provider_ref: reference.to_owned(),
                        link_name: link_name
                            .to_owned()
                            .unwrap_or_else(|| "default".to_string()),
                        host_id: host_id.to_owned(),
                        // We don't know this field from the command
                        error: String::with_capacity(0),
                    })),
                ))
            }
            _ => None,
        }
    }
}

/// Struct for the ScaleActor command
#[derive(Clone, Debug, Serialize, Deserialize, Default, Eq)]
pub struct ScaleActor {
    /// The ID of the actor to scale. This is optional as if the actor hasn't started yet
    /// then wadm scalers will not know the ID.
    pub actor_id: Option<String>,
    /// The host id on which to scale the actors
    pub host_id: String,
    /// The number of actors to scale to
    pub count: usize,
    /// The OCI or bindle reference to scale
    pub reference: String,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    /// Additional annotations to attach on this command
    pub annotations: BTreeMap<String, String>,
}

from_impl!(ScaleActor);

impl PartialEq for ScaleActor {
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
    /// Optional config to pass to the provider.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<CapabilityConfig>,
    /// Additional annotations to attach on this command
    pub annotations: BTreeMap<String, String>,
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
    pub annotations: BTreeMap<String, String>,
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
