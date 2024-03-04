use std::{
    borrow::Borrow,
    collections::{BTreeMap, HashMap, HashSet},
    hash::{Hash, Hasher},
};

use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};

use super::StateKind;
use crate::events::{
    ActorScaled, ActorsStarted, HostHeartbeat, HostStarted, ProviderInfo, ProviderStarted,
};

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

impl ToString for ProviderStatus {
    fn to_string(&self) -> String {
        match self {
            Self::Pending => "pending".to_string(),
            Self::Running => "running".to_string(),
            Self::Failed => "failed".to_string(),
        }
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

/// A representation of a unique actor (as defined by its annotations) and its count. This struct
/// has a custom implementation of PartialEq and Hash that _only_ compares the annotations. This is
/// not a very "pure" way of doing things, but it lets us access current counts of actors without
/// having to do a bunch of extra work.
#[derive(Debug, Serialize, Deserialize, Clone, Default, Eq)]
pub struct WadmActorInfo {
    pub annotations: BTreeMap<String, String>,
    pub count: usize,
}

impl PartialEq for WadmActorInfo {
    fn eq(&self, other: &Self) -> bool {
        self.annotations == other.annotations
    }
}

impl Hash for WadmActorInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.annotations.hash(state);
    }
}

impl Borrow<BTreeMap<String, String>> for WadmActorInfo {
    fn borrow(&self) -> &BTreeMap<String, String> {
        &self.annotations
    }
}

/// A wasmCloud Actor
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Actor {
    /// ID of the actor, normally a public (n)key
    pub id: String,

    /// Name of the provider
    pub name: String,

    /// Capabilities that the actor requires
    pub capabilities: Vec<String>,

    /// Issuer of the (signed) actor
    pub issuer: String,

    /// All instances of this actor running in the lattice, keyed by the host ID and contains a hash
    /// map of annotations -> count for each set of unique annotations
    pub instances: HashMap<String, HashSet<WadmActorInfo>>,

    /// The reference used to start the actor. Can be empty if it was started from a file
    pub reference: String,
}

impl Actor {
    /// A helper method that returns the total count of running copies of this actor, regardless of
    /// which host they are running on
    pub fn count(&self) -> usize {
        self.instances
            .values()
            .map(|instances| instances.iter().map(|info| info.count).sum::<usize>())
            .sum()
    }

    /// A helper method that returns the total count of running copies of this actor on a specific
    /// host
    pub fn count_for_host(&self, host_id: &str) -> usize {
        self.instances
            .get(host_id)
            .map(|instances| instances.iter().map(|info| info.count).sum::<usize>())
            .unwrap_or_default()
    }
}

impl StateKind for Actor {
    const KIND: &'static str = "actor";
}

impl From<ActorsStarted> for Actor {
    fn from(value: ActorsStarted) -> Self {
        Actor {
            id: value.public_key,
            name: value.claims.name,
            capabilities: value.claims.capabilites,
            issuer: value.claims.issuer,
            reference: value.image_ref,
            instances: HashMap::from_iter([(
                value.host_id,
                HashSet::from_iter([WadmActorInfo {
                    annotations: value.annotations,
                    count: value.count,
                }]),
            )]),
        }
    }
}

impl From<&ActorsStarted> for Actor {
    fn from(value: &ActorsStarted) -> Self {
        Actor {
            id: value.public_key.clone(),
            name: value.claims.name.clone(),
            capabilities: value.claims.capabilites.clone(),
            issuer: value.claims.issuer.clone(),
            reference: value.image_ref.clone(),
            instances: HashMap::from_iter([(
                value.host_id.clone(),
                HashSet::from_iter([WadmActorInfo {
                    annotations: value.annotations.clone(),
                    count: value.count,
                }]),
            )]),
        }
    }
}

impl From<ActorScaled> for Actor {
    fn from(value: ActorScaled) -> Self {
        let (name, capabilities, issuer) = value
            .claims
            .map(|c| (c.name, c.capabilites, c.issuer))
            .unwrap_or_default();
        Actor {
            id: value.actor_id,
            name,
            capabilities,
            issuer,
            reference: value.image_ref,
            instances: HashMap::from_iter([(
                value.host_id,
                HashSet::from_iter([WadmActorInfo {
                    annotations: value.annotations,
                    count: value.max_instances,
                }]),
            )]),
        }
    }
}

impl From<&ActorScaled> for Actor {
    fn from(value: &ActorScaled) -> Self {
        Actor {
            id: value.actor_id.clone(),
            name: value
                .claims
                .as_ref()
                .map(|c| c.name.clone())
                .unwrap_or_default(),
            capabilities: value
                .claims
                .as_ref()
                .map(|c| c.capabilites.clone())
                .unwrap_or_default(),
            issuer: value
                .claims
                .as_ref()
                .map(|c| c.issuer.clone())
                .unwrap_or_default(),
            reference: value.image_ref.clone(),
            instances: HashMap::from_iter([(
                value.host_id.clone(),
                HashSet::from_iter([WadmActorInfo {
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
    /// A map of actor IDs to the number of instances of the actor running on the host
    // NOTE(thomastaylor312): If we ever start storing a _ton_ of actors and it gets slow, we might
    // want to consider switching out the hash algorithm to something like `ahash` to speed up
    // lookups and deserialization
    pub actors: HashMap<String, usize>,

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
        let actors = value
            .actors
            .into_iter()
            .map(|actor| {
                (
                    actor.id, // SAFETY: Unlikely to not fit into a usize, but fallback just in case
                    actor.max_instances.try_into().unwrap_or(usize::MAX),
                )
            })
            .collect();

        let providers = value
            .providers
            .into_iter()
            .map(|provider| ProviderInfo {
                provider_id: provider.id,
                // NOTE: Provider should _always_ have an image ref. The control interface type should be updated.
                provider_ref: provider.image_ref.unwrap_or_default(),
                annotations: provider
                    .annotations
                    .map(|a| a.into_iter().collect())
                    .unwrap_or_default(),
            })
            .collect();

        Host {
            actors,
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
        let actors = value
            .actors
            .iter()
            .map(|actor| {
                (
                    actor.id.to_owned(),
                    // SAFETY: Unlikely to not fit into a usize, but fallback just in case
                    actor.max_instances.try_into().unwrap_or(usize::MAX),
                )
            })
            .collect();

        let providers = value
            .providers
            .iter()
            .map(|provider| ProviderInfo {
                provider_id: provider.id.to_owned(),
                provider_ref: provider.image_ref.to_owned().unwrap_or_default(),
                annotations: provider
                    .annotations
                    .clone()
                    .map(|a| a.into_iter().collect())
                    .unwrap_or_default(),
            })
            .collect();

        Host {
            actors,
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
