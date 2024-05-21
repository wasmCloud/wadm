use core::hash::{Hash, Hasher};
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// All unique data needed to identify a provider. For this reason, this type implements PartialEq
/// and Hash since it can serve as a key
#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq)]
pub struct ProviderInfo {
    #[serde(alias = "public_key")]
    pub provider_id: String,
    #[serde(default)]
    pub provider_ref: String,
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
}

impl PartialEq for ProviderInfo {
    fn eq(&self, other: &Self) -> bool {
        self.provider_id == other.provider_id
    }
}

// We don't hash on annotations here because this is only hashed for a hosts
// inventory where these three pieces need to be unique regardless of annotations
impl Hash for ProviderInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.provider_id.hash(state);
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
pub struct ProviderClaims {
    pub expires_human: String,
    // TODO: Should we actually parse the nkey?
    pub issuer: String,
    pub name: String,
    pub not_before_human: String,
    #[serde(
        serialize_with = "super::ser::tags",
        deserialize_with = "super::deser::tags"
    )]
    pub tags: Option<Vec<String>>,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
pub struct ProviderHealthCheckInfo {
    pub provider_id: String,
    pub host_id: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
pub struct ComponentClaims {
    pub call_alias: Option<String>,
    #[serde(default)]
    pub expires_human: String,
    // TODO: parse as nkey?
    #[serde(default)]
    pub issuer: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub not_before_human: String,
    pub revision: Option<usize>,
    // NOTE: This doesn't need a custom deserialize because unlike provider claims, these come out
    // in an array
    pub tags: Option<Vec<String>>,
    pub version: Option<String>,
}
