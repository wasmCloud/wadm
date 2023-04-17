use core::hash::{Hash, Hasher};
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// All unique data needed to identify a provider. For this reason, this type implements PartialEq
/// and Hash since it can serve as a key
#[derive(Debug, Serialize, Deserialize, Default, Clone, Eq)]
pub struct ProviderInfo {
    pub contract_id: String,
    pub link_name: String,
    // TODO: Should we actually parse the nkey?
    pub public_key: String,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

impl PartialEq for ProviderInfo {
    fn eq(&self, other: &Self) -> bool {
        self.public_key == other.public_key
            && self.contract_id == other.contract_id
            && self.link_name == other.link_name
    }
}

// We don't hash on annotations here because this is only hashed for a hosts
// inventory where these three pieces need to be unique regardless of annotations
impl Hash for ProviderInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.public_key.hash(state);
        self.contract_id.hash(state);
        self.link_name.hash(state);
    }
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
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

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ProviderHealthCheckInfo {
    pub link_name: String,
    // TODO: Should we make this a parsed nkey?
    pub public_key: String,
    pub contract_id: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ActorClaims {
    pub call_alias: Option<String>,
    #[serde(rename = "caps")]
    pub capabilites: Vec<String>,
    pub expires_human: String,
    // TODO: parse as nkey?
    pub issuer: String,
    pub name: String,
    pub not_before_human: String,
    pub revision: usize,
    // NOTE: This doesn't need a custom deserialize because unlike provider claims, these come out
    // in an array
    pub tags: Option<Vec<String>>,
    pub version: String,
}

#[derive(Debug, Serialize, Deserialize, Default, Eq, PartialEq, Clone)]
pub struct Linkdef {
    // TODO: parse as an nkey?
    pub actor_id: String,
    pub contract_id: String,
    pub id: String,
    pub link_name: String,
    // TODO: parse as an nkey?
    pub provider_id: String,
    pub values: HashMap<String, String>,
}
