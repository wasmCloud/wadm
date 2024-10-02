use std::borrow::{Borrow, ToOwned};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

use super::StateKind;
use crate::events::{ComponentScaled, HostHeartbeat, HostStarted, ProviderInfo, ProviderStarted};

/// A wasmCloud Capability provider
// NOTE: We probably aren't going to use this _right now_ so we've kept it pretty minimal. But it is
// possible that we could query wadm for more general data about the lattice in the future, so we do
// want to store this
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Provider {
    /// ID of the provider, normally a public nkey
    pub id: String,

    /// Name of the provider
    pub name: String,

    /// Issuer of the (signed) provider
    pub issuer: String,

    /// The reference used to start the provider. Can be empty if it was started from a file
    pub reference: String,

    /// The hosts this provider is running on
    pub hosts: HashMap<String, ProviderStatus>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ProviderStatus {
    /// The provider is starting and hasn't returned a heartbeat yet
    Pending,
    /// The provider is running
    Running,
    /// The provider failed to start
    // TODO(thomastaylor312): In the future, we'll probably want to decay out a provider from state
    // if it hasn't had a heartbeat
    Failed,
}

impl Default for ProviderStatus {
    fn default() -> Self {
        Self::Pending
    }
}

impl std::fmt::Display for ProviderStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Pending => "pending".to_string(),
                Self::Running => "running".to_string(),
                Self::Failed => "failed".to_string(),
            }
        )
    }
}

impl StateKind for Provider {
    const KIND: &'static str = "provider";
}

impl From<ProviderStarted> for Provider {
    fn from(value: ProviderStarted) -> Self {
        let (name, issuer) = value.claims.map(|c| (c.name, c.issuer)).unwrap_or_default();
        Provider {
            id: value.provider_id,
            name,
            issuer,
            reference: value.image_ref,
            ..Default::default()
        }
    }
}

impl From<&ProviderStarted> for Provider {
    fn from(value: &ProviderStarted) -> Self {
        Provider {
            id: value.provider_id.clone(),
            name: value
                .claims
                .as_ref()
                .map(|c| c.name.clone())
                .unwrap_or_default(),
            issuer: value
                .claims
                .as_ref()
                .map(|c| c.issuer.clone())
                .unwrap_or_default(),
            reference: value.image_ref.clone(),
            ..Default::default()
        }
    }
}

/// A representation of a unique component (as defined by its annotations) and its count. This struct
/// has a custom implementation of PartialEq and Hash that _only_ compares the annotations. This is
/// not a very "pure" way of doing things, but it lets us access current counts of components without
/// having to do a bunch of extra work.
#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq)]
pub struct WadmComponentInfo {
    pub annotations: BTreeMap<String, String>,
    pub count: usize,
}

impl PartialEq for WadmComponentInfo {
    fn eq(&self, other: &Self) -> bool {
        self.annotations == other.annotations
    }
}

impl Hash for WadmComponentInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.annotations.hash(state);
    }
}

impl Borrow<BTreeMap<String, String>> for WadmComponentInfo {
    fn borrow(&self) -> &BTreeMap<String, String> {
        &self.annotations
    }
}

/// A wasmCloud Component
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Component {
    /// ID of the component
    pub id: String,

    /// Name of the component
    pub name: String,

    /// Issuer of the (signed) component
    pub issuer: String,

    /// All instances of this component running in the lattice, keyed by the host ID and contains a hash
    /// map of annotations -> count for each set of unique annotations
    pub instances: HashMap<String, HashSet<WadmComponentInfo>>,

    /// The reference used to start the component. Can be empty if it was started from a file
    pub reference: String,
}

impl Component {
    /// A helper method that returns the total count of running copies of this component, regardless of
    /// which host they are running on
    pub fn count(&self) -> usize {
        self.instances
            .values()
            .map(|instances| instances.iter().map(|info| info.count).sum::<usize>())
            .sum()
    }

    /// A helper method that returns the total count of running copies of this component on a specific
    /// host
    pub fn count_for_host(&self, host_id: &str) -> usize {
        self.instances
            .get(host_id)
            .map(|instances| instances.iter().map(|info| info.count).sum::<usize>())
            .unwrap_or_default()
    }
}

impl StateKind for Component {
    const KIND: &'static str = "component";
}

impl From<ComponentScaled> for Component {
    fn from(value: ComponentScaled) -> Self {
        let (name, issuer) = value.claims.map(|c| (c.name, c.issuer)).unwrap_or_default();
        Component {
            id: value.component_id,
            name,
            issuer,
            reference: value.image_ref,
            instances: HashMap::from_iter([(
                value.host_id,
                HashSet::from_iter([WadmComponentInfo {
                    annotations: value.annotations,
                    count: value.max_instances,
                }]),
            )]),
        }
    }
}

impl From<&ComponentScaled> for Component {
    fn from(value: &ComponentScaled) -> Self {
        Component {
            id: value.component_id.clone(),
            name: value
                .claims
                .as_ref()
                .map(|c| c.name.clone())
                .unwrap_or_default(),
            issuer: value
                .claims
                .as_ref()
                .map(|c| c.issuer.clone())
                .unwrap_or_default(),
            reference: value.image_ref.clone(),
            instances: HashMap::from_iter([(
                value.host_id.clone(),
                HashSet::from_iter([WadmComponentInfo {
                    annotations: value.annotations.clone(),
                    count: value.max_instances,
                }]),
            )]),
        }
    }
}

/// A wasmCloud host
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Host {
    /// A map of component IDs to the number of instances of the component running on the host
    #[serde(alias = "actors")]
    pub components: HashMap<String, usize>,

    /// The randomly generated friendly name of the host
    pub friendly_name: String,

    /// An arbitrary hashmap of string labels attached to the host
    pub labels: HashMap<String, String>,

    /// A set of running providers on the host
    pub providers: HashSet<ProviderInfo>,

    /// The current uptime of the host in seconds
    pub uptime_seconds: usize,

    /// The host version that is running
    // NOTE(thomastaylor312): Right now a host started event doesn't emit the version, so a newly
    // started host can't be registered with one. We should probably add that to the host started
    // event and then modify it here
    pub version: Option<Version>,

    /// The ID of this host, in the form of its nkey encoded public key
    pub id: String,

    /// The time when this host was last seen, as a RFC3339 timestamp
    pub last_seen: DateTime<Utc>,
}

impl StateKind for Host {
    const KIND: &'static str = "host";
}

impl From<HostStarted> for Host {
    fn from(value: HostStarted) -> Self {
        Host {
            friendly_name: value.friendly_name,
            id: value.id,
            labels: value.labels,
            last_seen: Utc::now(),
            ..Default::default()
        }
    }
}

impl From<&HostStarted> for Host {
    fn from(value: &HostStarted) -> Self {
        Host {
            friendly_name: value.friendly_name.clone(),
            id: value.id.clone(),
            labels: value.labels.clone(),
            last_seen: Utc::now(),
            ..Default::default()
        }
    }
}

impl From<HostHeartbeat> for Host {
    fn from(value: HostHeartbeat) -> Self {
        let components = value
            .components
            .into_iter()
            .map(|component| {
                (
                    component.id().into(), // SAFETY: Unlikely to not fit into a usize, but fallback just in case
                    component.max_instances().try_into().unwrap_or(usize::MAX),
                )
            })
            .collect();

        let providers = value
            .providers
            .into_iter()
            .map(|provider| ProviderInfo {
                provider_id: provider.id().to_string(),
                // NOTE: Provider should _always_ have an image ref. The control interface type should be updated.
                provider_ref: provider.image_ref().map(String::from).unwrap_or_default(),
                annotations: provider
                    .annotations()
                    .map(ToOwned::to_owned)
                    .map(BTreeMap::from_iter)
                    .unwrap_or_default(),
            })
            .collect();

        Host {
            components,
            friendly_name: value.friendly_name,
            labels: value.labels,
            providers,
            uptime_seconds: value.uptime_seconds as usize,
            version: Some(value.version),
            id: value.host_id,
            last_seen: Utc::now(),
        }
    }
}

impl From<&HostHeartbeat> for Host {
    fn from(value: &HostHeartbeat) -> Self {
        let components = value
            .components
            .iter()
            .map(|component| {
                (
                    component.id().to_owned(),
                    // SAFETY: Unlikely to not fit into a usize, but fallback just in case
                    component.max_instances().try_into().unwrap_or(usize::MAX),
                )
            })
            .collect();

        let providers = value
            .providers
            .iter()
            .map(|provider| ProviderInfo {
                provider_id: provider.id().to_owned(),
                provider_ref: provider.image_ref().map(String::from).unwrap_or_default(),
                annotations: provider
                    .annotations()
                    .map(ToOwned::to_owned)
                    .map(BTreeMap::from_iter)
                    .unwrap_or_default(),
            })
            .collect();

        Host {
            components,
            friendly_name: value.friendly_name.clone(),
            labels: value.labels.clone(),
            providers,
            uptime_seconds: value.uptime_seconds as usize,
            version: Some(value.version.clone()),
            id: value.host_id.clone(),
            last_seen: Utc::now(),
        }
    }
}
