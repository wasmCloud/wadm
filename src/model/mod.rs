use std::collections::{BTreeMap, HashMap};

use serde::{
    de::{self, Deserializer, MapAccess, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};

pub(crate) mod internal;

/// The default weight for a spread
pub const DEFAULT_SPREAD_WEIGHT: usize = 100;
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
/// The identifier for the builtin spreadscaler trait type
pub const SPREADSCALER_TRAIT: &str = "spreadscaler";
/// The identifier for the builtin linkdef trait type
pub const LINKDEF_TRAIT: &str = "linkdef";
/// The string used for indicating a latest version. It is explicitly forbidden to use as a version
/// for a manifest
pub const LATEST_VERSION: &str = "latest";

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
    /// The properties for this component
    // NOTE(thomastaylor312): It would probably be better for us to implement a custom deserialze
    // and serialize that combines this and the component type. This is good enough for first draft
    #[serde(flatten)]
    pub properties: Properties,
    /// A list of various traits assigned to this component
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traits: Option<Vec<Trait>>,
}

/// Properties that can be defined for a component
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum Properties {
    #[serde(rename = "actor")]
    Actor { properties: ActorProperties },
    #[serde(rename = "capability")]
    Capability { properties: CapabilityProperties },
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
    /// Optional config to pass to the provider. This can be either a raw string encoded config, or
    /// a JSON or YAML object
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<CapabilityConfig>,
}

/// Right now providers can technically use any config format they want, although most use JSON.
/// This enum takes that into account and allows either type of data to be passed
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapabilityConfig {
    Json(serde_json::Value),
    Opaque(String),
}

impl<'de> Deserialize<'de> for CapabilityConfig {
    fn deserialize<D>(deserializer: D) -> Result<CapabilityConfig, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(CapabilityConfigVisitor)
    }
}

struct CapabilityConfigVisitor;

impl<'de> Visitor<'de> for CapabilityConfigVisitor {
    type Value = CapabilityConfig;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("string or json/yaml object")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(CapabilityConfig::Opaque(value.to_owned()))
    }

    fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let data = serde_json::Value::deserialize(de::value::MapAccessDeserializer::new(map))?;
        Ok(CapabilityConfig::Json(data))
    }
}

impl Serialize for CapabilityConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CapabilityConfig::Json(v) => v.serialize(serializer),
            CapabilityConfig::Opaque(v) => serializer.serialize_str(v),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Trait {
    /// The type of trait specified. This should be a unique string for the type of scaler. As we
    /// plan on supporting custom scalers, these traits are not enumerated
    #[serde(rename = "type")]
    pub trait_type: String,
    /// The properties of this trait
    pub properties: TraitProperty,
}

impl Trait {
    /// Helper that creates a new linkdef type trait with the given properties
    pub fn new_linkdef(props: LinkdefProperty) -> Trait {
        Trait {
            trait_type: LINKDEF_TRAIT.to_owned(),
            properties: TraitProperty::Linkdef(props),
        }
    }

    /// Helper that creates a new spreadscaler type trait with the given properties
    pub fn new_spreadscaler(props: SpreadScalerProperty) -> Trait {
        Trait {
            trait_type: SPREADSCALER_TRAIT.to_owned(),
            properties: TraitProperty::SpreadScaler(props),
        }
    }
}

/// Properties for defining traits
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum TraitProperty {
    Linkdef(LinkdefProperty),
    SpreadScaler(SpreadScalerProperty),
    // TODO(thomastaylor312): This is still broken right now with deserializing. If the incoming
    // type specifies replicas, it matches with spreadscaler first. So we need to implement a custom
    // parser here
    Custom(serde_json::Value),
}

impl From<LinkdefProperty> for TraitProperty {
    fn from(value: LinkdefProperty) -> Self {
        Self::Linkdef(value)
    }
}

impl From<SpreadScalerProperty> for TraitProperty {
    fn from(value: SpreadScalerProperty) -> Self {
        Self::SpreadScaler(value)
    }
}

impl From<serde_json::Value> for TraitProperty {
    fn from(value: serde_json::Value) -> Self {
        Self::Custom(value)
    }
}

/// Properties for linkdefs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LinkdefProperty {
    /// The target this linkdef applies to. This should be the name of an actor component
    pub target: String,
    /// Values to use for this linkdef
    #[serde(skip_serializing_if = "Option::is_none")]
    pub values: Option<HashMap<String, String>>,
}

/// Properties for spread scalers
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpreadScalerProperty {
    /// Number of replicas to scale
    pub replicas: usize,
    /// Requirements for spreading throse replicas
    #[serde(default)]
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

impl Default for Spread {
    fn default() -> Self {
        Spread {
            name: "default".to_string(),
            requirements: BTreeMap::default(),
            weight: None,
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::BufReader;
    use std::path::Path;

    use anyhow::Result;
    use serde_json;
    use serde_yaml;

    use super::*;

    pub(crate) fn deserialize_yaml(filepath: impl AsRef<Path>) -> Result<Manifest> {
        let file = std::fs::File::open(filepath)?;
        let reader = BufReader::new(file);
        let yaml_string: Manifest = serde_yaml::from_reader(reader)?;
        Ok(yaml_string)
    }

    pub(crate) fn deserialize_json(filepath: impl AsRef<Path>) -> Result<Manifest> {
        let file = std::fs::File::open(filepath)?;
        let reader = BufReader::new(file);
        let json_string: Manifest = serde_json::from_reader(reader)?;
        Ok(json_string)
    }

    #[test]
    fn test_oam_deserializer() {
        let res = deserialize_json("./oam/simple1.json");
        match res {
            Ok(parse_results) => parse_results,
            Err(error) => panic!("Error {:?}", error),
        };

        let res = deserialize_yaml("./oam/simple1.yaml");
        match res {
            Ok(parse_results) => parse_results,
            Err(error) => panic!("Error {:?}", error),
        };
    }

    #[test]
    #[ignore] // see TODO in TraitProperty enum
    fn test_custom_traits() {
        let manifest = deserialize_yaml("./oam/custom.yaml").expect("Should be able to parse");
        let actor_component = manifest
            .spec
            .components
            .into_iter()
            .find(|comp| matches!(comp.properties, Properties::Actor { .. }))
            .expect("Should be able to find actor component");
        let traits = actor_component.traits.expect("Should have Vec of traits");
        assert!(
            traits
                .iter()
                .any(|t| matches!(t.properties, TraitProperty::Custom(_))),
            "Should have found custom property trait: {traits:?}"
        );
    }

    #[test]
    fn test_provider_config() {
        let manifest =
            deserialize_yaml("./oam/provider_config.yaml").expect("Should be able to parse");
        let props = match &manifest.spec.components[0].properties {
            Properties::Capability { properties } => properties,
            _ => panic!("Should have found capability component"),
        };

        match props.config.as_ref().expect("Config should have been set") {
            CapabilityConfig::Opaque(data) => assert_eq!(data, "{\"raw\": \"json\", \"data\": {}}"),
            _ => panic!("Should have found opaque config"),
        }

        // YAML should work
        let props = match &manifest.spec.components[1].properties {
            Properties::Capability { properties } => properties,
            _ => panic!("Should have found capability component"),
        };
        let config = match props.config.as_ref().expect("Config should have been set") {
            CapabilityConfig::Json(data) => data,
            _ => panic!("Should have found json config"),
        };
        assert_eq!(
            config
                .get("some")
                .expect("Should have the right key in the config")
                .as_str()
                .expect("Should have parsed the right data type"),
            "config"
        );
        assert_eq!(
            config
                .get("number")
                .expect("Should have the right key in the config")
                .as_u64()
                .expect("Should have parsed the right data type"),
            1
        );

        // So should raw JSON
        let props = match &manifest.spec.components[2].properties {
            Properties::Capability { properties } => properties,
            _ => panic!("Should have found capability component"),
        };
        let config = match props.config.as_ref().expect("Config should have been set") {
            CapabilityConfig::Json(data) => data,
            _ => panic!("Should have found json config"),
        };
        assert_eq!(
            config
                .get("some")
                .expect("Should have the right key in the config")
                .as_str()
                .expect("Should have parsed the right data type"),
            "config"
        );
        assert_eq!(
            config
                .get("number")
                .expect("Should have the right key in the config")
                .as_u64()
                .expect("Should have parsed the right data type"),
            1
        );
    }

    #[test]
    fn test_component_matching() {
        let manifest = deserialize_yaml("./oam/simple2.yaml").expect("Should be able to parse");
        assert_eq!(
            manifest
                .spec
                .components
                .iter()
                .filter(|component| matches!(component.properties, Properties::Actor { .. }))
                .count(),
            1,
            "Should have found 1 actor property"
        );
        assert_eq!(
            manifest
                .spec
                .components
                .iter()
                .filter(|component| matches!(component.properties, Properties::Capability { .. }))
                .count(),
            2,
            "Should have found 2 capability properties"
        );
    }

    #[test]
    fn test_trait_matching() {
        let manifest = deserialize_yaml("./oam/simple2.yaml").expect("Should be able to parse");
        let traits = manifest
            .spec
            .components
            .into_iter()
            .find(|component| matches!(component.properties, Properties::Actor { .. }))
            .expect("Should find actor component")
            .traits
            .expect("Should have traits object");
        assert_eq!(traits.len(), 2, "Should have 2 traits");
        assert!(
            matches!(traits[0].properties, TraitProperty::SpreadScaler(_)),
            "Should have spreadscaler properties"
        );
        assert!(
            matches!(traits[1].properties, TraitProperty::Linkdef(_)),
            "Should have linkdef properties"
        );
        if let TraitProperty::Linkdef(ld) = &traits[1].properties {
            assert_eq!(ld.values, None);
            assert_eq!(ld.target, "webcap".to_string());
        } else {
            panic!("trait property was not a link definition");
        }
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
        let trait_item = Trait::new_spreadscaler(spreadscalerprop);
        trait_vec.push(trait_item);
        let linkdefprop = LinkdefProperty {
            target: "webcap".to_string(),
            values: Some(HashMap::from([("port".to_string(), "4000".to_string())])),
        };
        let trait_item = Trait::new_linkdef(linkdefprop);
        trait_vec.push(trait_item);
        let mut component_vec: Vec<Component> = Vec::new();
        let component_item = Component {
            name: "userinfo".to_string(),
            properties: Properties::Actor {
                properties: ActorProperties {
                    image: "wasmcloud.azurecr.io/fake:1".to_string(),
                },
            },
            traits: Some(trait_vec),
        };
        component_vec.push(component_item);
        let component_item = Component {
            name: "webcap".to_string(),
            properties: Properties::Capability {
                properties: CapabilityProperties {
                    image: "wasmcloud.azurecr.io/httpserver:0.13.1".to_string(),
                    contract: "wasmcloud:httpserver".to_string(),
                    link_name: Some("default".to_string()),
                    config: None,
                },
            },
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
        let trait_item = Trait::new_spreadscaler(spreadscalerprop);
        trait_vec.push(trait_item);
        let component_item = Component {
            name: "ledblinky".to_string(),
            properties: Properties::Capability {
                properties: CapabilityProperties {
                    image: "wasmcloud.azurecr.io/ledblinky:0.0.1".to_string(),
                    contract: "wasmcloud:blinkenlights".to_string(),
                    link_name: Some(crate::DEFAULT_LINK_NAME.to_owned()),
                    config: None,
                },
            },
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
        let serialized_json =
            serde_json::to_vec(&manifest).expect("Should be able to serialize JSON");

        let serialized_yaml = serde_yaml::to_string(&manifest)
            .expect("Should be able to serialize YAML")
            .into_bytes();

        // Test the round trip back in
        let json_manifest: Manifest = serde_json::from_slice(&serialized_json)
            .expect("Should be able to deserialize JSON roundtrip");
        let yaml_manifest: Manifest = serde_yaml::from_slice(&serialized_yaml)
            .expect("Should be able to deserialize YAML roundtrip");

        // Make sure the manifests don't contain any custom traits (to test that we aren't parsing
        // the tagged enum poorly)
        assert!(
            !json_manifest
                .spec
                .components
                .into_iter()
                .any(|component| component
                    .traits
                    .unwrap_or_default()
                    .into_iter()
                    .any(|t| matches!(t.properties, TraitProperty::Custom(_)))),
            "Should have found custom properties"
        );

        assert!(
            !yaml_manifest
                .spec
                .components
                .into_iter()
                .any(|component| component
                    .traits
                    .unwrap_or_default()
                    .into_iter()
                    .any(|t| matches!(t.properties, TraitProperty::Custom(_)))),
            "Should have found custom properties"
        );
    }
}
