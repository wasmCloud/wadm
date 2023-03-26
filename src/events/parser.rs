use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const DEFAULT_WEIGHT: i32 = 100;

#[derive(Debug, Serialize, Deserialize)]
pub enum ComponentType {
    #[serde(rename = "actor")]
    Actor,
    #[serde(rename = "capability")]
    Capability,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Annotations {
    pub version: String,
    pub description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
    pub annotations: Annotations,
}

fn default_link_name() -> Option<String> {
    Some("default".to_string())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Properties {
    pub image: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract: Option<String>,
    #[serde(default = "default_link_name", skip_serializing_if = "Option::is_none")]
    pub link_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TraitType {
    #[serde(rename = "spreadscaler")]
    Spreadscaler,
    #[serde(rename = "linkdef")]
    Linkdef,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LinkdefValueItems {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub port: Option<i32>,
}

fn default_weight() -> Option<i32> {
    Some(DEFAULT_WEIGHT)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpreadRequirementValues {
    String(String),
    Bool(bool),
    I32(i32),
    Char(char),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Spread {
    pub name: String,
    pub requirements: BTreeMap<String, SpreadRequirementValues>,
    #[serde(default = "default_weight", skip_serializing_if = "Option::is_none")]
    pub weight: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LinkdefProperty {
    pub target: String,
    pub values: LinkdefValueItems,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SpreadScalerProperty {
    pub replicas: i32,
    pub spread: Vec<Spread>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TraitPropertyItem {
    LinkdefProperty(LinkdefProperty),
    SpreadScalerProperty(SpreadScalerProperty),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Traits {
    #[serde(rename = "type")]
    pub trait_type: TraitType,
    pub properties: TraitPropertyItem,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComponentItem {
    pub name: String,
    #[serde(rename = "type")]
    pub component_type: ComponentType,
    pub properties: Properties,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traits: Option<Vec<Traits>>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Components {
    pub components: Vec<ComponentItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    #[serde(rename = "apiVersion")]
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    pub spec: Components,
}

#[cfg(test)]
mod test {

    use super::*;
    use anyhow::Result;
    use serde_json;
    use serde_yaml;
    use std::io::BufReader;
    use std::path::PathBuf;
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
            requirements: BTreeMap::from([(
                "zone".to_string(),
                SpreadRequirementValues::String("us-east-1".to_string()),
            )]),
            weight: Some(80),
        };
        spread_vec.push(spread_item);
        let spread_item = Spread {
            name: "westcoast".to_string(),
            requirements: BTreeMap::from([(
                "zone".to_string(),
                SpreadRequirementValues::String("us-west-1".to_string()),
            )]),
            weight: Some(20),
        };
        spread_vec.push(spread_item);
        let mut trait_vec: Vec<Traits> = Vec::new();
        let spreadscalerprop = SpreadScalerProperty {
            replicas: 4,
            spread: spread_vec,
        };
        let trait_item = Traits {
            trait_type: TraitType::Spreadscaler,
            properties: TraitPropertyItem::SpreadScalerProperty(spreadscalerprop),
        };
        trait_vec.push(trait_item);
        let linkdefprop = LinkdefProperty {
            target: "webcap".to_string(),
            values: LinkdefValueItems {
                uri: None,
                address: None,
                port: Some(4000),
            },
        };
        let trait_item = Traits {
            trait_type: TraitType::Linkdef,
            properties: TraitPropertyItem::LinkdefProperty(linkdefprop),
        };
        trait_vec.push(trait_item);
        let mut component_vec: Vec<ComponentItem> = Vec::new();
        let component_item = ComponentItem {
            name: "userinfo".to_string(),
            component_type: ComponentType::Actor,
            properties: Properties {
                image: "wasmcloud.azurecr.io/fake:1".to_string(),
                contract: None,
                link_name: None,
            },
            traits: Some(trait_vec),
        };
        component_vec.push(component_item);
        let component_item = ComponentItem {
            name: "webcap".to_string(),
            component_type: ComponentType::Capability,
            properties: Properties {
                image: "wasmcloud.azurecr.io/httpserver:0.13.1".to_string(),
                contract: Some("wasmcloud:httpserver".to_string()),
                link_name: Some("default".to_string()),
            },
            traits: None,
        };
        component_vec.push(component_item);

        let mut spread_vec: Vec<Spread> = Vec::new();
        let spread_item = Spread {
            name: "haslights".to_string(),
            requirements: BTreeMap::from([(
                "zone".to_string(),
                SpreadRequirementValues::Bool(true),
            )]),
            weight: default_weight(),
        };
        spread_vec.push(spread_item);
        let spreadscalerprop = SpreadScalerProperty {
            replicas: 1,
            spread: spread_vec,
        };
        let mut trait_vec: Vec<Traits> = Vec::new();
        let trait_item = Traits {
            trait_type: TraitType::Spreadscaler,
            properties: TraitPropertyItem::SpreadScalerProperty(spreadscalerprop),
        };
        trait_vec.push(trait_item);
        let component_item = ComponentItem {
            name: "ledblinky".to_string(),
            component_type: ComponentType::Capability,
            properties: Properties {
                image: "wasmcloud.azurecr.io/ledblinky:0.0.1".to_string(),
                contract: Some("wasmcloud:blinkenlights".to_string()),
                link_name: default_link_name(),
            },
            traits: Some(trait_vec),
        };
        component_vec.push(component_item);

        let spec = Components {
            components: component_vec,
        };
        let metadata = Metadata {
            name: "my-example-app".to_string(),
            annotations: Annotations {
                version: "v0.0.1".to_string(),
                description: "This is my app".to_string(),
            },
        };
        let manifest = Manifest {
            api_version: "core.oam.dev/v1beta1".to_string(),
            kind: "application".to_string(),
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
