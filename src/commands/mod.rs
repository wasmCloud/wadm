//! Type implementations for commands issued to compensate for state changes

use std::{
    collections::BTreeMap,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};
use wasmcloud_control_interface::InterfaceLinkDefinition;

use crate::{
    events::{Event, ProviderStartFailed, ProviderStarted},
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
                provider_id,
                ..
            }) => {
                let mut annotations = annotations.to_owned();
                insert_managed_annotations(&mut annotations, model_name);
                Some((
                    Event::ProviderStarted(ProviderStarted {
                        provider_id: provider_id.to_owned(),
                        annotations: annotations.to_owned(),
                        claims: None,
                        image_ref: reference.to_owned(),
                        host_id: host_id.to_owned(),
                    }),
                    Some(Event::ProviderStartFailed(ProviderStartFailed {
                        provider_id: provider_id.to_owned(),
                        provider_ref: reference.to_owned(),
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
    /// The ID of the actor to scale. This should be computed by wadm as a combination
    /// of the manifest name and the actor name.
    pub actor_id: String,
    /// The host id on which to scale the actors
    pub host_id: String,
    /// The number of actors to scale to
    pub count: u32,
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
    /// The ID of the provider to scale. This should be computed by wadm as a combination
    /// of the manifest name and the provider name.
    pub provider_id: String,
    /// The host id on which to start the actor(s)
    pub host_id: String,
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
            && self.model_name == other.model_name
    }
}

impl Hash for StartProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.reference.hash(state);
        self.host_id.hash(state);
    }
}

/// Struct for the StopProvider command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default)]
pub struct StopProvider {
    /// The ID of the provider to stop
    pub provider_id: String,
    /// The host ID on which to stop the provider
    pub host_id: String,
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
            && self.model_name == other.model_name
    }
}

impl Hash for StopProvider {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.provider_id.hash(state);
        self.host_id.hash(state);
    }
}

/// Struct for the PutLinkdef command
#[derive(Clone, Debug, Eq, Serialize, Deserialize, Default, PartialEq, Hash)]
pub struct PutLinkdef {
    /// Source identifier for the link
    pub source_id: String,
    /// Target for the link, which can be a unique identifier or (future) a routing group
    pub target: String,
    /// Name of the link. Not providing this is equivalent to specifying "default"
    pub name: String,
    /// WIT namespace of the link operation, e.g. `wasi` in `wasi:keyvalue/readwrite.get`
    pub wit_namespace: String,
    /// WIT package of the link operation, e.g. `keyvalue` in `wasi:keyvalue/readwrite.get`
    pub wit_package: String,
    /// WIT Interfaces to be used for the link, e.g. `readwrite`, `atomic`, etc.
    pub interfaces: Vec<String>,
    /// List of named configurations to provide to the source upon request
    #[serde(default)]
    pub source_config: Vec<String>,
    /// List of named configurations to provide to the target upon request
    #[serde(default)]
    pub target_config: Vec<String>,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
}

impl From<PutLinkdef> for InterfaceLinkDefinition {
    fn from(value: PutLinkdef) -> InterfaceLinkDefinition {
        InterfaceLinkDefinition {
            source_id: value.source_id,
            target: value.target,
            name: value.name,
            wit_namespace: value.wit_namespace,
            wit_package: value.wit_package,
            interfaces: value.interfaces,
            source_config: value.source_config,
            target_config: value.target_config,
        }
    }
}

from_impl!(PutLinkdef);

/// Struct for the DeleteLinkdef command
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct DeleteLinkdef {
    /// The ID of the component to unlink
    pub source_id: String,
    /// The WIT namespace of the component to unlink
    pub wit_namespace: String,
    /// The WIT package of the component to unlink
    pub wit_package: String,
    /// The link name to unlink
    pub link_name: String,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
}

from_impl!(DeleteLinkdef);
