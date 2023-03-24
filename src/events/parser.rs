use anyhow::Result;
use serde_json;
use serde_yaml;
use std::collections::BTreeMap;
use std::io::BufReader;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum ComponentType {
    actor,
    capability,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Annotations {
    version: String,
    description: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Metadata {
    name: String,
    annotations: Annotations,
}

fn default_link_name() -> Option<String> {
    Some("default".to_string())
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Properties {
    image: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    contract: Option<String>,
    #[serde(default = "default_link_name", skip_serializing_if = "Option::is_none")]
    link_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TraitType {
    spreadscaler,
    linkdef,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum SpreadRequirements {
    app(String),
    zone(String),
    ledenabled(bool),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum LinkdefValueItems {
    uri(String),
    address(String),
    port(i32),
}

fn default_weight() -> Option<i32> {
    Some(100)
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpreadRequirementValues {
    String(String),
    bool(bool),
    i32(i32),
    char(char),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Spread {
    name: String,
    requirements: BTreeMap<String, SpreadRequirementValues>,
    #[serde(default = "default_weight", skip_serializing_if = "Option::is_none")]
    weight: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TraitPropertyItem {
    LinkdefProperty {
        target: String,
        values: LinkdefValueItems,
    },
    SpreadScalerProperty {
        replicas: i32,
        spread: Vec<Spread>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Traits {
    #[serde(rename = "type")]
    trait_type: TraitType,
    properties: TraitPropertyItem,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ComponentItem {
    name: String,
    #[serde(rename = "type")]
    component_type: ComponentType,
    properties: Properties,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    traits: Option<Vec<Traits>>,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Components {
    components: Vec<ComponentItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    apiVersion: String,
    kind: String,
    metadata: Metadata,
    spec: Components,
}

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

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_oam_desrializer() {
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
        let mut spreadVec: Vec<Spread> = Vec::new();
        let spreadItem = Spread {
            name: "eastcoast".to_string(),
            requirements: BTreeMap::from([(
                "zone".to_string(),
                SpreadRequirementValues::String("us-east-1".to_string()),
            )]),
            weight: Some(80),
        };
        spreadVec.push(spreadItem);
        let spreadItem = Spread {
            name: "westcoast".to_string(),
            requirements: BTreeMap::from([(
                "zone".to_string(),
                SpreadRequirementValues::String("us-west-1".to_string()),
            )]),
            weight: Some(20),
        };
        spreadVec.push(spreadItem);
        let mut traitVec: Vec<Traits> = Vec::new();
        let traitItem = Traits {
            trait_type: TraitType::spreadscaler,
            properties: TraitPropertyItem::SpreadScalerProperty {
                replicas: 4,
                spread: spreadVec,
            },
        };
        traitVec.push(traitItem);
        let traitItem = Traits {
            trait_type: TraitType::linkdef,
            properties: TraitPropertyItem::LinkdefProperty {
                target: "webcap".to_string(),
                values: LinkdefValueItems::port(4000),
            },
        };
        traitVec.push(traitItem);
        let mut componentVec: Vec<ComponentItem> = Vec::new();
        let componentItem = ComponentItem {
            name: "userinfo".to_string(),
            component_type: ComponentType::actor,
            properties: Properties {
                image: "wasmcloud.azurecr.io/fake:1".to_string(),
                contract: None,
                link_name: None,
            },
            traits: Some(traitVec),
        };
        componentVec.push(componentItem);
        let componentItem = ComponentItem {
            name: "webcap".to_string(),
            component_type: ComponentType::capability,
            properties: Properties {
                image: "wasmcloud.azurecr.io/httpserver:0.13.1".to_string(),
                contract: Some("wasmcloud:httpserver".to_string()),
                link_name: Some("default".to_string()),
            },
            traits: None,
        };
        componentVec.push(componentItem);

        let mut spreadVec: Vec<Spread> = Vec::new();
        let spreadItem = Spread {
            name: "haslights".to_string(),
            requirements: BTreeMap::from([(
                "zone".to_string(),
                SpreadRequirementValues::bool(true),
            )]),
            weight: default_weight(),
        };
        spreadVec.push(spreadItem);
        let mut traitVec: Vec<Traits> = Vec::new();
        let traitItem = Traits {
            trait_type: TraitType::spreadscaler,
            properties: TraitPropertyItem::SpreadScalerProperty {
                replicas: 1,
                spread: spreadVec,
            },
        };
        traitVec.push(traitItem);
        let componentItem = ComponentItem {
            name: "ledblinky".to_string(),
            component_type: ComponentType::capability,
            properties: Properties {
                image: "wasmcloud.azurecr.io/ledblinky:0.0.1".to_string(),
                contract: Some("wasmcloud:blinkenlights".to_string()),
                link_name: default_link_name(),
            },
            traits: Some(traitVec),
        };
        componentVec.push(componentItem);

        let spec = Components {
            components: componentVec,
        };
        let metadata = Metadata {
            name: "my-example-app".to_string(),
            annotations: Annotations {
                version: "v0.0.1".to_string(),
                description: "This is my app".to_string(),
            },
        };
        let manifest = Manifest {
            apiVersion: "core.oam.dev/v1beta1".to_string(),
            kind: "application".to_string(),
            metadata: metadata,
            spec: spec,
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
