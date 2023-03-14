use std::collections::HashMap;

use semver::Version;
use serde::{Deserialize, Serialize};

use crate::events::Linkdef;

////////////
// Claims //
////////////

/// A Claim that can be applied to a given lattice
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Claim {
    /// Name of the claim
    pub name: String,

    /// Version of the claim (semver)
    pub version: Version,

    /// Revision of the claim
    pub revision: usize,

    /// Subscriber public (n)key
    pub subscriber: String,

    /// Issuer public (n)key
    pub issuer: String,

    /// Call alias that can be used for the claim (if present)
    pub call_alias: Option<String>,

    /// Capabilities that this claim uses
    #[serde(rename = "caps")]
    pub capabilities: Vec<String>,

    /// Tags that have been set on the claim
    pub tags: HashMap<String, ()>,
}

///////////////
// Providers //
///////////////

/// A wasmCloud Capability provider
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Provider {
    /// ID of the provider, normally a public (n)key
    pub id: String,

    /// Name of the provider
    pub name: String,

    /// Issuer of the (signed) provider
    pub issuer: String,

    /// Contract ID
    pub contract_id: String,

    /// Tags set on the provider
    pub tags: Vec<String>,
}

////////////
// Actors //
////////////

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Actor {
    /// ID of the actor, normally a public (n)key
    pub id: String,

    /// Name of the provider
    pub name: String,

    /// Capabilities that the actor requires
    pub capabilities: Vec<String>,

    /// Issuer of the (signed) actor
    pub issuer: String,

    /// Tags set on the provider
    pub tags: Vec<String>,

    /// Call alias to use for the actor
    pub call_alias: String,
}

////////////
// Hosts //
////////////

/// A wasmCLoud host
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Host {
    /// ID of the host
    pub id: String,

    /// Name (human-friendly) of the provider
    pub name: String,

    /// Host Uptime
    pub uptime_seconds: u64,

    /// Labels set on the provider
    pub labels: Vec<String>,

    /// Version of the host
    pub version: Version,
}

/////////////
// Lattice //
/////////////

/// Lattice-wide parameters
#[derive(Debug, Serialize, Deserialize, Clone, Default, PartialEq, Eq)]
pub struct LatticeParameters {
    host_status_decay_rate_seconds: Option<u32>,
}

/// Struct that represents lattice state
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct LatticeState {
    /// ID of the lattice
    pub id: String,

    /// Parameters for the lattice itself
    pub parameters: LatticeParameters,

    /// Actors that are in the lattice, keyed by actor ID
    pub actors: HashMap<String, Actor>,

    /// Hosts that are members of the lattice, keyed by host ID
    pub hosts: HashMap<String, Host>,

    /// Providers that are used by the lattice keyed by contract name and provider ID
    pub providers: HashMap<String, HashMap<String, Provider>>,

    /// Link Definitions active in the lattice, keyed by link ID
    pub link_defs: HashMap<String, Linkdef>,

    /// Claims that have been applied to the lattice, keyed by claim name
    pub claims: HashMap<String, Claim>,
}
