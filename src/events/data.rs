use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProviderInfo {
    pub contract_id: String,
    pub link_name: String,
    // TODO: Should we actually parse the nkey?
    pub public_key: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
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

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ProviderHealthCheckInfo {
    pub link_name: String,
    // TODO: Should we make this a parsed nkey?
    pub public_key: String,
}

#[derive(Debug, Serialize, Deserialize, Default)]
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

#[derive(Debug, Serialize, Deserialize, Default)]
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
