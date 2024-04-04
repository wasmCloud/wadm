use std::collections::{BTreeMap, HashMap};

use anyhow::Context;
use serde::{
    de::{self, Deserializer, MapAccess, Visitor},
    ser::Serializer,
    Deserialize, Serialize,
};

use base64::{engine::general_purpose, Engine as _};

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
/// The identifier for the builtin daemonscaler trait type
pub const DAEMONSCALER_TRAIT: &str = "daemonscaler";
/// The identifier for the builtin linkdef trait type
pub const LINK_TRAIT: &str = "link";
/// The string used for indicating a latest version. It is explicitly forbidden to use as a version
/// for a manifest
pub const LATEST_VERSION: &str = "latest";

/// An OAM manifest
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// The name of the manifest. This must be unique per lattice
    pub name: String,
    /// Optional data for annotating this manifest see <https://github.com/oam-dev/spec/blob/master/metadata.md#annotations-format>
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<String, String>,
    /// Optional data for labeling this manifest, see <https://github.com/oam-dev/spec/blob/master/metadata.md#label-format>
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<String, String>,
}

/// A representation of an OAM specification
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Specification {
    /// The list of components for describing an application
    pub components: Vec<Component>,
}

/// A component definition
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Component {
    /// The name of this component
    pub name: String,
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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum Properties {
    #[serde(rename = "component", alias = "actor")]
    Component { properties: ComponentProperties },
    #[serde(rename = "capability")]
    Capability { properties: CapabilityProperties },
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ComponentProperties {
    /// The image reference to use
    pub image: String,
    /// The component ID to use for this actor. If not supplied, it will be generated
    /// as a combination of the [Metadata::name] and the image reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CapabilityProperties {
    /// The image reference to use
    pub image: String,
    /// The component ID to use for this provider. If not supplied, it will be generated
    /// as a combination of the [Metadata::name] and the image reference.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
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

impl CapabilityConfig {
    pub fn try_base64_encoding(&self) -> anyhow::Result<String> {
        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &self)
            .context("failed to serialize capability config into bytes for base64 encoding")?;
        Ok(general_purpose::STANDARD.encode(&bytes))
    }

    pub fn try_base64_encoding_with_engine<T: base64::Engine>(
        &self,
        engine: &T,
    ) -> anyhow::Result<String> {
        let mut bytes: Vec<u8> = Vec::new();
        serde_json::to_writer(&mut bytes, &self)
            .context("failed to serialize capability config into bytes for base64 encoding")?;
        Ok(engine.encode(&bytes))
    }
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
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
    pub fn new_link(props: LinkProperty) -> Trait {
        Trait {
            trait_type: LINK_TRAIT.to_owned(),
            properties: TraitProperty::Link(props),
        }
    }

    /// Helper that creates a new spreadscaler type trait with the given properties
    pub fn new_spreadscaler(props: SpreadScalerProperty) -> Trait {
        Trait {
            trait_type: SPREADSCALER_TRAIT.to_owned(),
            properties: TraitProperty::SpreadScaler(props),
        }
    }

    pub fn new_daemonscaler(props: SpreadScalerProperty) -> Trait {
        Trait {
            trait_type: DAEMONSCALER_TRAIT.to_owned(),
            properties: TraitProperty::SpreadScaler(props),
        }
    }
}

/// Properties for defining traits
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum TraitProperty {
    Link(LinkProperty),
    SpreadScaler(SpreadScalerProperty),
    // TODO(thomastaylor312): This is still broken right now with deserializing. If the incoming
    // type specifies instances, it matches with spreadscaler first. So we need to implement a custom
    // parser here
    Custom(serde_json::Value),
}

impl From<LinkProperty> for TraitProperty {
    fn from(value: LinkProperty) -> Self {
        Self::Link(value)
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

/// Properties for the config list associated with components, providers, and links
///
/// ## Usage
/// Defining a config block, like so:
/// ```yaml
/// source_config:
/// - name: "external-secret-kv"
/// - name: "default-port"
///   properties:
///      port: "8080"
/// ```
///
/// TODO(#252): Consider if we want to scope this by application
/// Will result in two config scalers being created, one with the name `basic-kv` and one with the
/// name `default-port`. Wadm will not resolve collisions with configuration names between manifests.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ConfigProperty {
    /// Name of the config to ensure exists
    pub name: String,
    /// Optional properties to put with the configuration. If the properties are
    /// omitted in the manifest, wadm will assume that the configuration is externally managed
    /// and will not attempt to create it, only reporting the status as failed if not found.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<HashMap<String, String>>,
}

/// This impl is a helper to help compare a `Vec<String>` to a `Vec<ConfigProperty>`
impl PartialEq<ConfigProperty> for String {
    fn eq(&self, other: &ConfigProperty) -> bool {
        self == &other.name
    }
}

/// Properties for links
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LinkProperty {
    /// The target this link applies to. This should be the name of a component in the manifest
    pub target: String,
    /// WIT namespace for the link
    pub namespace: String,
    /// WIT package for the link
    pub package: String,
    /// WIT interfaces for the link
    pub interfaces: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_config: Vec<ConfigProperty>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub target_config: Vec<ConfigProperty>,
    /// The name of this link
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Properties for spread scalers
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct SpreadScalerProperty {
    /// Number of instances to spread across matching requirements
    #[serde(alias = "replicas")]
    pub instances: usize,
    /// Requirements for spreading those instances
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub spread: Vec<Spread>,
}

/// Configuration for various spreading requirements
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
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
            .find(|comp| matches!(comp.properties, Properties::Component { .. }))
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
                .filter(|component| matches!(component.properties, Properties::Component { .. }))
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
        // Validate actor component traits
        let traits = manifest
            .spec
            .components
            .clone()
            .into_iter()
            .find(|component| matches!(component.properties, Properties::Component { .. }))
            .expect("Should find actor component")
            .traits
            .expect("Should have traits object");
        assert_eq!(traits.len(), 1, "Should have 1 trait");
        assert!(
            matches!(traits[0].properties, TraitProperty::SpreadScaler(_)),
            "Should have spreadscaler properties"
        );
        // Validate capability component traits
        let traits = manifest
            .spec
            .components
            .into_iter()
            .find(|component| {
                matches!(
                    &component.properties,
                    Properties::Capability {
                        properties: CapabilityProperties { image, .. }
                    } if image == "wasmcloud.azurecr.io/httpserver:0.13.1"
                )
            })
            .expect("Should find capability component")
            .traits
            .expect("Should have traits object");
        assert_eq!(traits.len(), 1, "Should have 1 trait");
        assert!(
            matches!(traits[0].properties, TraitProperty::Link(_)),
            "Should have link property"
        );
        if let TraitProperty::Link(ld) = &traits[0].properties {
            assert_eq!(ld.source_config, vec![]);
            assert_eq!(ld.target, "userinfo".to_string());
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
            instances: 4,
            spread: spread_vec,
        };
        let trait_item = Trait::new_spreadscaler(spreadscalerprop);
        trait_vec.push(trait_item);
        let linkdefprop = LinkProperty {
            target: "webcap".to_string(),
            namespace: "wasi".to_string(),
            package: "http".to_string(),
            interfaces: vec!["incoming-handler".to_string()],
            source_config: vec![ConfigProperty {
                name: "http".to_string(),
                properties: Some(HashMap::from([("port".to_string(), "8080".to_string())])),
            }],
            target_config: vec![],
            name: Some("default".to_string()),
        };
        let trait_item = Trait::new_link(linkdefprop);
        trait_vec.push(trait_item);
        let mut component_vec: Vec<Component> = Vec::new();
        let component_item = Component {
            name: "userinfo".to_string(),
            properties: Properties::Component {
                properties: ComponentProperties {
                    image: "wasmcloud.azurecr.io/fake:1".to_string(),
                    id: None,
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
                    id: None,
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
            instances: 1,
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
                    id: None,
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
            labels: BTreeMap::from([(
                "prefix.dns.prefix/name-for_a.123".to_string(),
                "this is a valid label".to_string(),
            )]),
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
