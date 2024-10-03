//! Type implementations for commands issued to compensate for state changes

use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    hash::{Hash, Hasher},
};

use serde::{Deserialize, Serialize};
use wasmcloud_control_interface::Link;

use crate::{
    events::{ComponentScaleFailed, ComponentScaled, Event, ProviderStartFailed, ProviderStarted},
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
    ScaleComponent(ScaleComponent),
    StartProvider(StartProvider),
    StopProvider(StopProvider),
    PutLink(PutLink),
    DeleteLink(DeleteLink),
    PutConfig(PutConfig),
    DeleteConfig(DeleteConfig),
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
    pub fn corresponding_event(&self) -> Option<(Event, Option<Event>)> {
        match self {
            Command::StartProvider(StartProvider {
                annotations,
                reference,
                host_id,
                provider_id,
                model_name,
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
            Command::ScaleComponent(ScaleComponent {
                component_id,
                host_id,
                count,
                reference,
                annotations,
                model_name,
                ..
            }) => {
                let mut annotations = annotations.to_owned();
                insert_managed_annotations(&mut annotations, model_name);
                Some((
                    Event::ComponentScaled(ComponentScaled {
                        component_id: component_id.to_owned(),
                        host_id: host_id.to_owned(),
                        max_instances: *count as usize,
                        image_ref: reference.to_owned(),
                        annotations: annotations.to_owned(),
                        // We don't know this field from the command
                        claims: None,
                    }),
                    Some(Event::ComponentScaleFailed(ComponentScaleFailed {
                        component_id: component_id.to_owned(),
                        host_id: host_id.to_owned(),
                        max_instances: *count as usize,
                        image_ref: reference.to_owned(),
                        annotations: annotations.to_owned(),
                        // We don't know these fields from the command
                        error: String::with_capacity(0),
                        claims: None,
                    })),
                ))
            }
            _ => None,
        }
    }
}

/// Struct for the ScaleComponent command
#[derive(Clone, Debug, Serialize, Deserialize, Default, Eq)]
pub struct ScaleComponent {
    /// The ID of the component to scale. This should be computed by wadm as a combination
    /// of the manifest name and the component name.
    pub component_id: String,
    /// The host id on which to scale the components
    pub host_id: String,
    /// The number of components to scale to
    pub count: u32,
    /// The OCI or bindle reference to scale
    pub reference: String,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    /// Additional annotations to attach on this command
    pub annotations: BTreeMap<String, String>,
    /// Named configuration to pass to the component.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<String>,
}

from_impl!(ScaleComponent);

impl PartialEq for ScaleComponent {
    fn eq(&self, other: &Self) -> bool {
        self.component_id == other.component_id
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
    /// The host id on which to start the provider
    pub host_id: String,
    /// The name of the model/manifest that generated this command
    pub model_name: String,
    /// Named configuration to pass to the provider.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub config: Vec<String>,
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
pub struct PutLink {
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

impl TryFrom<PutLink> for Link {
    type Error = Box<dyn Error + Send + Sync>;

    fn try_from(value: PutLink) -> Result<Link, Self::Error> {
        Link::builder()
            .source_id(&value.source_id)
            .target(&value.target)
            .name(&value.name)
            .wit_namespace(&value.wit_namespace)
            .wit_package(&value.wit_package)
            .interfaces(value.interfaces)
            .source_config(value.source_config)
            .target_config(value.target_config)
            .build()
    }
}

from_impl!(PutLink);

/// Struct for the DeleteLinkdef command
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Default)]
pub struct DeleteLink {
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

from_impl!(DeleteLink);

/// Struct for the PutConfig command
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct PutConfig {
    /// The name of the configuration to put
    pub config_name: String,
    /// The configuration properties to put
    pub config: HashMap<String, String>,
}

from_impl!(PutConfig);

/// Struct for the DeleteConfig command
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
pub struct DeleteConfig {
    /// The name of the configuration to delete
    pub config_name: String,
}

from_impl!(DeleteConfig);
