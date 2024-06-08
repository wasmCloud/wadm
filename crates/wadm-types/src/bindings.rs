use crate::{
    api::{
        ComponentStatus, DeleteResult, GetResult, ModelSummary, PutResult, Status, StatusInfo,
        StatusResult, StatusType, TraitStatus, VersionInfo,
    },
    CapabilityProperties, Component, ComponentProperties, ConfigProperty, LinkProperty, Manifest,
    Metadata, Properties, Specification, Spread, SpreadScalerProperty, Trait, TraitProperty,
};
use wasmcloud::oam;
use wasmcloud::wadm;

wit_bindgen_wrpc::generate!({
    additional_derives: [
        serde::Serialize,
        serde::Deserialize,
    ],
});

impl From<Manifest> for oam::types::OamManifest {
    fn from(manifest: Manifest) -> Self {
        oam::types::OamManifest {
            api_version: manifest.api_version.to_string(),
            kind: manifest.kind.to_string(),
            metadata: manifest.metadata.into(),
            spec: manifest.spec.into(),
        }
    }
}

impl From<Metadata> for oam::types::Metadata {
    fn from(metadata: Metadata) -> Self {
        oam::types::Metadata {
            name: metadata.name,
            annotations: metadata.annotations.into_iter().collect(),
            labels: metadata.labels.into_iter().collect(),
        }
    }
}

impl From<Specification> for oam::types::Specification {
    fn from(spec: Specification) -> Self {
        oam::types::Specification {
            components: spec.components.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<Component> for oam::types::Component {
    fn from(component: Component) -> Self {
        oam::types::Component {
            name: component.name,
            properties: component.properties.into(),
            traits: component
                .traits
                .unwrap_or_default()
                .into_iter()
                .map(|t| t.into())
                .collect(),
        }
    }
}

impl From<Properties> for oam::types::Properties {
    fn from(properties: Properties) -> Self {
        match properties {
            Properties::Component { properties } => {
                oam::types::Properties::Component(properties.into())
            }
            Properties::Capability { properties } => {
                oam::types::Properties::Capability(properties.into())
            }
        }
    }
}

impl From<ComponentProperties> for oam::types::ComponentProperties {
    fn from(properties: ComponentProperties) -> Self {
        oam::types::ComponentProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<CapabilityProperties> for oam::types::CapabilityProperties {
    fn from(properties: CapabilityProperties) -> Self {
        oam::types::CapabilityProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<ConfigProperty> for oam::types::ConfigProperty {
    fn from(property: ConfigProperty) -> Self {
        oam::types::ConfigProperty {
            name: property.name,
            properties: property.properties.map(|props| props.into_iter().collect()),
        }
    }
}

impl From<Trait> for oam::types::Trait {
    fn from(trait_: Trait) -> Self {
        oam::types::Trait {
            trait_type: trait_.trait_type,
            properties: trait_.properties.into(),
        }
    }
}

impl From<TraitProperty> for oam::types::TraitProperty {
    fn from(property: TraitProperty) -> Self {
        match property {
            TraitProperty::Link(link) => oam::types::TraitProperty::Link(link.into()),
            TraitProperty::SpreadScaler(spread) => {
                oam::types::TraitProperty::Spreadscaler(spread.into())
            }
            TraitProperty::Custom(custom) => oam::types::TraitProperty::Custom(custom.to_string()),
        }
    }
}

impl From<LinkProperty> for oam::types::LinkProperty {
    fn from(property: LinkProperty) -> Self {
        oam::types::LinkProperty {
            target: property.target,
            namespace: property.namespace,
            package: property.package,
            interfaces: property.interfaces,
            source_config: property
                .source_config
                .into_iter()
                .map(|c| c.into())
                .collect(),
            target_config: property
                .target_config
                .into_iter()
                .map(|c| c.into())
                .collect(),
            name: property.name,
        }
    }
}

impl From<SpreadScalerProperty> for oam::types::SpreadscalerProperty {
    fn from(property: SpreadScalerProperty) -> Self {
        oam::types::SpreadscalerProperty {
            instances: property.instances as u32,
            spread: property.spread.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<Spread> for oam::types::Spread {
    fn from(spread: Spread) -> Self {
        oam::types::Spread {
            name: spread.name,
            requirements: spread.requirements.into_iter().collect(),
            weight: spread.weight.map(|w| w as u32),
        }
    }
}

impl From<ModelSummary> for wadm::types::ModelSummary {
    fn from(summary: ModelSummary) -> Self {
        wadm::types::ModelSummary {
            name: summary.name,
            version: summary.version,
            description: summary.description,
            deployed_version: summary.deployed_version,
            status: summary.status.into(),
            status_message: summary.status_message,
        }
    }
}

impl From<DeleteResult> for wadm::types::DeleteResult {
    fn from(result: DeleteResult) -> Self {
        match result {
            DeleteResult::Deleted => wadm::types::DeleteResult::Deleted,
            DeleteResult::Error => wadm::types::DeleteResult::Error,
            DeleteResult::Noop => wadm::types::DeleteResult::Noop,
        }
    }
}

impl From<GetResult> for wadm::types::GetResult {
    fn from(result: GetResult) -> Self {
        match result {
            GetResult::Error => wadm::types::GetResult::Error,
            GetResult::Success => wadm::types::GetResult::Success,
            GetResult::NotFound => wadm::types::GetResult::NotFound,
        }
    }
}

impl From<PutResult> for wadm::types::PutResult {
    fn from(result: PutResult) -> Self {
        match result {
            PutResult::Error => wadm::types::PutResult::Error,
            PutResult::Created => wadm::types::PutResult::Created,
            PutResult::NewVersion => wadm::types::PutResult::NewVersion,
        }
    }
}

impl From<StatusType> for wadm::types::StatusType {
    fn from(status: StatusType) -> Self {
        match status {
            StatusType::Undeployed => wadm::types::StatusType::Undeployed,
            StatusType::Reconciling => wadm::types::StatusType::Reconciling,
            StatusType::Deployed => wadm::types::StatusType::Deployed,
            StatusType::Failed => wadm::types::StatusType::Failed,
        }
    }
}

impl From<wadm::types::StatusType> for StatusType {
    fn from(status: wadm::types::StatusType) -> Self {
        match status {
            wadm::types::StatusType::Undeployed => StatusType::Undeployed,
            wadm::types::StatusType::Reconciling => StatusType::Reconciling,
            wadm::types::StatusType::Deployed => StatusType::Deployed,
            wadm::types::StatusType::Failed => StatusType::Failed,
        }
    }
}

impl From<wadm::types::StatusInfo> for StatusInfo {
    fn from(info: wadm::types::StatusInfo) -> Self {
        StatusInfo {
            status_type: info.status_type.into(),
            message: info.message,
        }
    }
}

impl From<wadm::types::ComponentStatus> for ComponentStatus {
    fn from(status: wadm::types::ComponentStatus) -> Self {
        ComponentStatus {
            name: status.name,
            component_type: status.component_type,
            info: status.info.into(),
            traits: status
                .traits
                .into_iter()
                .map(|t| TraitStatus {
                    trait_type: t.trait_type,
                    info: t.info.into(),
                })
                .collect(),
        }
    }
}

impl From<wadm::types::TraitStatus> for TraitStatus {
    fn from(status: wadm::types::TraitStatus) -> Self {
        TraitStatus {
            trait_type: status.trait_type,
            info: status.info.into(),
        }
    }
}

impl From<wadm::types::StatusResult> for StatusResult {
    fn from(result: wadm::types::StatusResult) -> Self {
        match result {
            wadm::types::StatusResult::Error => StatusResult::Error,
            wadm::types::StatusResult::Ok => StatusResult::Ok,
            wadm::types::StatusResult::NotFound => StatusResult::NotFound,
        }
    }
}

impl From<oam::types::OamManifest> for Manifest {
    fn from(manifest: oam::types::OamManifest) -> Self {
        Manifest {
            api_version: manifest.api_version,
            kind: manifest.kind,
            metadata: manifest.metadata.into(),
            spec: manifest.spec.into(),
        }
    }
}

impl From<oam::types::Metadata> for Metadata {
    fn from(metadata: oam::types::Metadata) -> Self {
        Metadata {
            name: metadata.name,
            annotations: metadata.annotations.into_iter().collect(),
            labels: metadata.labels.into_iter().collect(),
        }
    }
}

impl From<oam::types::Specification> for Specification {
    fn from(spec: oam::types::Specification) -> Self {
        Specification {
            components: spec.components.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<oam::types::Component> for Component {
    fn from(component: oam::types::Component) -> Self {
        Component {
            name: component.name,
            properties: component.properties.into(),
            traits: Some(component.traits.into_iter().map(|t| t.into()).collect()), // Always wrap in Some
        }
    }
}

impl From<oam::types::Properties> for Properties {
    fn from(properties: oam::types::Properties) -> Self {
        match properties {
            oam::types::Properties::Component(properties) => Properties::Component {
                properties: properties.into(),
            },
            oam::types::Properties::Capability(properties) => Properties::Capability {
                properties: properties.into(),
            },
        }
    }
}

impl From<oam::types::ComponentProperties> for ComponentProperties {
    fn from(properties: oam::types::ComponentProperties) -> Self {
        ComponentProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<oam::types::CapabilityProperties> for CapabilityProperties {
    fn from(properties: oam::types::CapabilityProperties) -> Self {
        CapabilityProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<oam::types::ConfigProperty> for ConfigProperty {
    fn from(property: oam::types::ConfigProperty) -> Self {
        ConfigProperty {
            name: property.name,
            properties: property.properties.map(|props| props.into_iter().collect()),
        }
    }
}

impl From<oam::types::Trait> for Trait {
    fn from(trait_: oam::types::Trait) -> Self {
        Trait {
            trait_type: trait_.trait_type,
            properties: trait_.properties.into(),
        }
    }
}

impl From<oam::types::TraitProperty> for TraitProperty {
    fn from(property: oam::types::TraitProperty) -> Self {
        match property {
            oam::types::TraitProperty::Link(link) => TraitProperty::Link(link.into()),
            oam::types::TraitProperty::Spreadscaler(spread) => {
                TraitProperty::SpreadScaler(spread.into())
            }
            oam::types::TraitProperty::Custom(custom) => {
                TraitProperty::Custom(serde_json::value::Value::String(custom))
            }
        }
    }
}

impl From<oam::types::LinkProperty> for LinkProperty {
    fn from(property: oam::types::LinkProperty) -> Self {
        LinkProperty {
            target: property.target,
            namespace: property.namespace,
            package: property.package,
            interfaces: property.interfaces,
            source_config: property
                .source_config
                .into_iter()
                .map(|c| c.into())
                .collect(),
            target_config: property
                .target_config
                .into_iter()
                .map(|c| c.into())
                .collect(),
            name: property.name,
        }
    }
}

impl From<oam::types::SpreadscalerProperty> for SpreadScalerProperty {
    fn from(property: oam::types::SpreadscalerProperty) -> Self {
        SpreadScalerProperty {
            instances: property.instances as usize,
            spread: property.spread.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<oam::types::Spread> for Spread {
    fn from(spread: oam::types::Spread) -> Self {
        Spread {
            name: spread.name,
            requirements: spread.requirements.into_iter().collect(),
            weight: spread.weight.map(|w| w as usize),
        }
    }
}

impl From<VersionInfo> for wadm::types::VersionInfo {
    fn from(info: VersionInfo) -> Self {
        wasmcloud::wadm::types::VersionInfo {
            version: info.version,
            deployed: info.deployed,
        }
    }
}

// Implement the From trait for StatusInfo
impl From<StatusInfo> for wadm::types::StatusInfo {
    fn from(info: StatusInfo) -> Self {
        wadm::types::StatusInfo {
            status_type: info.status_type.into(),
            message: info.message,
        }
    }
}

// Implement the From trait for Status
impl From<Status> for wadm::types::Status {
    fn from(status: Status) -> Self {
        wadm::types::Status {
            version: status.version,
            info: status.info.into(),
            components: status.components.into_iter().map(|c| c.into()).collect(),
        }
    }
}

// Implement the From trait for ComponentStatus
impl From<ComponentStatus> for wadm::types::ComponentStatus {
    fn from(component_status: ComponentStatus) -> Self {
        wadm::types::ComponentStatus {
            name: component_status.name,
            component_type: component_status.component_type,
            info: component_status.info.into(),
            traits: component_status
                .traits
                .into_iter()
                .map(|t| t.into())
                .collect(),
        }
    }
}

// Implement the From trait for TraitStatus
impl From<TraitStatus> for wadm::types::TraitStatus {
    fn from(trait_status: TraitStatus) -> Self {
        wadm::types::TraitStatus {
            trait_type: trait_status.trait_type,
            info: trait_status.info.into(),
        }
    }
}
