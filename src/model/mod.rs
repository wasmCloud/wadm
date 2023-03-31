use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub(crate) mod internal;

/// The default weight for a spread
pub const DEFAULT_SPREAD_WEIGHT: usize = 100;
/// The default link name
pub const DEFAULT_LINK_NAME: &str = "default";
/// The expected OAM api version
pub const OAM_VERSION: &str = "core.oam.dev/v1beta1";
/// The currently supported kind for OAM manifests.
// NOTE(thomastaylor312): If we ever end up supporting more than one kind, we should use an enum for
// this
pub const APPLICATION_KIND: &str = "Application";
/// The version key, as predefined by the [OAM
/// spec](https://github.com/oam-dev/spec/blob/master/metadata.md#annotations-format)
pub const VERSION_ANNOTATION_KEY: &str = "version";
/// The description key, as predefined by the [OAM
/// spec](https://github.com/oam-dev/spec/blob/master/metadata.md#annotations-format)
pub const DESCRIPTION_ANNOTATION_KEY: &str = "description";

/// An OAM manifest
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Manifest {
    /// The OAM version of the manifest
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    /// The kind or type of manifest described by the spec
    pub kind: String,
    /// Metadata describing the manifest
    pub metadata: Metadata,
    /// The specification for this manifest
    pub spec: Specification,
}

impl Manifest {
    /// Returns a reference to the current version
    pub fn version(&self) -> &str {
        self.metadata
            .annotations
            .get(VERSION_ANNOTATION_KEY)
            .map(|v| v.as_str())
            .unwrap_or_default()
    }

    /// Returns a reference to the current description if it exists
    pub fn description(&self) -> Option<&str> {
        self.metadata
            .annotations
            .get(DESCRIPTION_ANNOTATION_KEY)
            .map(|v| v.as_str())
    }
}

/// The metadata describing the manifest
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Metadata {
    /// The name of the manifest. This should be unique
    pub name: String,
    /// Optional data for annotating this manifest
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
}

/// A representation of an OAM specification
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Specification {
    /// The list of components for describing an application
    pub components: Vec<Component>,
}

/// A component definition
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Component {
    /// The name of this component
    pub name: String,
    /// The type of component
    #[serde(rename = "type")]
    pub component_type: ComponentType,
    /// The properties for this component
    // NOTE(thomastaylor312): It would probably be better for us to implement a custom deserialze
    // and serialize that combines this and the component type. This is good enough for first draft
    pub properties: Properties,
    /// A list of various traits assigned to this component
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traits: Option<Vec<Trait>>,
}

/// All possible component types we support
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ComponentType {
    /// An Actor component
    #[serde(rename = "actor")]
    Actor,
    /// A Capability component
    #[serde(rename = "capability")]
    Capability,
}

/// Properties that can be defined for a component
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Properties {
    Actor(ActorProperties),
    Capability(CapabilityProperties),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ActorProperties {
    /// The image reference to use
    pub image: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CapabilityProperties {
    /// The image reference to use
    pub image: String,
    /// The contract ID of this capability
    pub contract: String,
    /// An optional link name to use for this capability
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trait {
    /// The type of trait specified
    // NOTE(thomastaylor312): Same thing goes here as described in the note for Component. We should
    // probably combine these and do a custom deserialize
    #[serde(rename = "type")]
    pub trait_type: TraitType,
    /// The properties of this trait
    pub properties: TraitProperty,
}

/// All supported trait types in wadm
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum TraitType {
    /// A spreadscaler trait
    #[serde(rename = "spreadscaler")]
    SpreadScaler,
    /// A linkdef trait
    #[serde(rename = "linkdef")]
    Linkdef,
}

/// Properties for defining traits
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TraitProperty {
    Linkdef(LinkdefProperty),
    SpreadScaler(SpreadScalerProperty),
}

/// Properties for linkdefs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LinkdefProperty {
    /// The target this linkdef applies to. This should be the name of an actor component
    pub target: String,
    /// Values to use for this linkdef
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub values: BTreeMap<String, String>,
}

/// Properties for spread scalers
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpreadScalerProperty {
    /// Number of replicas to scale
    pub replicas: usize,
    /// Requirements for spreading throse replicas
    pub spread: Vec<Spread>,
}

/// Configuration for various spreading requirements
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Spread {
    /// The name of this spread requirement
    pub name: String,
    /// An arbitrary map of labels to match on for scaling requirements
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub requirements: BTreeMap<String, String>,
    /// An optional weight for this spread. Higher weights are given more precedence
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<usize>,
}

#[cfg(test)]
mod test {
    use std::io::BufReader;
    use std::path::PathBuf;

    use anyhow::Result;
    use serde_json;
    use serde_yaml;

    use super::*;

    pub(crate) fn deserialize_yaml(filepath: PathBuf) -> Result<Manifest> {
        let file = std::fs::File::open(filepath)?;
        let reader = BufReader::new(file);
        let yaml_string: Manifest = serde_yaml::from_reader(reader)?;
        println!("Read YAML string: {:?}", yaml_string);
        Ok(yaml_string)
    }

    pub(crate) fn deserialize_json(filepath: PathBuf) -> Result<Manifest> {
        let file = std::fs::File::open(filepath)?;
        let reader = BufReader::new(file);
        let json_string: Manifest = serde_json::from_reader(reader)?;
        println!("Read JSON string: {:?}", json_string);
        Ok(json_string)
    }

    #[test]
    fn test_oam_deserializer() {
        let filepath = PathBuf::from("./oam/simple1.json");
        let res = deserialize_json(filepath);
        match res {
            Ok(parse_results) => parse_results,
            Err(error) => panic!("Error {:?}", error),
        };

        let filepath = PathBuf::from("./oam/simple1.yaml");
        let res = deserialize_yaml(filepath);
        match res {
            Ok(parse_results) => parse_results,
            Err(error) => panic!("Error {:?}", error),
        };
    }
    #[test]
    fn test_oam_serializer() {
        let mut spread_vec: Vec<Spread> = Vec::new();
        let spread_item = Spread {
            name: "eastcoast".to_string(),
            requirements: BTreeMap::from([("zone".to_string(), "us-east-1".to_string())]),
            weight: Some(80),
        };
        spread_vec.push(spread_item);
        let spread_item = Spread {
            name: "westcoast".to_string(),
            requirements: BTreeMap::from([("zone".to_string(), "us-west-1".to_string())]),
            weight: Some(20),
        };
        spread_vec.push(spread_item);
        let mut trait_vec: Vec<Trait> = Vec::new();
        let spreadscalerprop = SpreadScalerProperty {
            replicas: 4,
            spread: spread_vec,
        };
        let trait_item = Trait {
            trait_type: TraitType::SpreadScaler,
            properties: TraitProperty::SpreadScaler(spreadscalerprop),
        };
        trait_vec.push(trait_item);
        let linkdefprop = LinkdefProperty {
            target: "webcap".to_string(),
            values: BTreeMap::from([("port".to_string(), "4000".to_string())]),
        };
        let trait_item = Trait {
            trait_type: TraitType::Linkdef,
            properties: TraitProperty::Linkdef(linkdefprop),
        };
        trait_vec.push(trait_item);
        let mut component_vec: Vec<Component> = Vec::new();
        let component_item = Component {
            name: "userinfo".to_string(),
            component_type: ComponentType::Actor,
            properties: Properties::Actor(ActorProperties {
                image: "wasmcloud.azurecr.io/fake:1".to_string(),
            }),
            traits: Some(trait_vec),
        };
        component_vec.push(component_item);
        let component_item = Component {
            name: "webcap".to_string(),
            component_type: ComponentType::Capability,
            properties: Properties::Capability(CapabilityProperties {
                image: "wasmcloud.azurecr.io/httpserver:0.13.1".to_string(),
                contract: "wasmcloud:httpserver".to_string(),
                link_name: Some("default".to_string()),
            }),
            traits: None,
        };
        component_vec.push(component_item);

        let mut spread_vec: Vec<Spread> = Vec::new();
        let spread_item = Spread {
            name: "haslights".to_string(),
            requirements: BTreeMap::from([("zone".to_string(), "enabled".to_string())]),
            weight: Some(DEFAULT_SPREAD_WEIGHT),
        };
        spread_vec.push(spread_item);
        let spreadscalerprop = SpreadScalerProperty {
            replicas: 1,
            spread: spread_vec,
        };
        let mut trait_vec: Vec<Trait> = Vec::new();
        let trait_item = Trait {
            trait_type: TraitType::SpreadScaler,
            properties: TraitProperty::SpreadScaler(spreadscalerprop),
        };
        trait_vec.push(trait_item);
        let component_item = Component {
            name: "ledblinky".to_string(),
            component_type: ComponentType::Capability,
            properties: Properties::Capability(CapabilityProperties {
                image: "wasmcloud.azurecr.io/ledblinky:0.0.1".to_string(),
                contract: "wasmcloud:blinkenlights".to_string(),
                link_name: Some(DEFAULT_LINK_NAME.to_owned()),
            }),
            traits: Some(trait_vec),
        };
        component_vec.push(component_item);

        let spec = Specification {
            components: component_vec,
        };
        let metadata = Metadata {
            name: "my-example-app".to_string(),
            annotations: BTreeMap::from([
                (VERSION_ANNOTATION_KEY.to_string(), "v0.0.1".to_string()),
                (
                    DESCRIPTION_ANNOTATION_KEY.to_string(),
                    "This is my app".to_string(),
                ),
            ]),
        };
        let manifest = Manifest {
            api_version: OAM_VERSION.to_owned(),
            kind: APPLICATION_KIND.to_owned(),
            metadata,
            spec,
        };
        let serialized_json = serde_json::to_string(&manifest);

        match serialized_json {
            Ok(parse_results) => parse_results,
            Err(error) => panic!("Error {:?}", error),
        };
        let serialized_yaml = serde_yaml::to_string(&manifest);

        match serialized_yaml {
            Ok(parse_results) => parse_results,
            Err(error) => panic!("Error {:?}", error),
        };
    }
}