use crate::{
    api::{
        ComponentStatus, DeleteResult, GetResult, ModelSummary, PutResult, Status, StatusInfo,
        StatusResult, StatusType, TraitStatus, VersionInfo,
    },
    CapabilityProperties, Component, ComponentProperties, ConfigDefinition, ConfigProperty,
    LinkProperty, Manifest, Metadata, Policy, Properties, SecretProperty, SecretSourceProperty,
    Specification, Spread, SpreadScalerProperty, TargetConfig, Trait, TraitProperty,
};
use wasmcloud::wadm;

wit_bindgen_wrpc::generate!({
    generate_unused_types: true,
    additional_derives: [
        serde::Serialize,
        serde::Deserialize,
    ],
});

// Trait implementations for converting types in the API module to the generated types

impl From<Manifest> for wadm::types::OamManifest {
    fn from(manifest: Manifest) -> Self {
        wadm::types::OamManifest {
            api_version: manifest.api_version.to_string(),
            kind: manifest.kind.to_string(),
            metadata: manifest.metadata.into(),
            spec: manifest.spec.into(),
        }
    }
}

impl From<Metadata> for wadm::types::Metadata {
    fn from(metadata: Metadata) -> Self {
        wadm::types::Metadata {
            name: metadata.name,
            annotations: metadata.annotations.into_iter().collect(),
            labels: metadata.labels.into_iter().collect(),
        }
    }
}

impl From<Specification> for wadm::types::Specification {
    fn from(spec: Specification) -> Self {
        wadm::types::Specification {
            components: spec.components.into_iter().map(|c| c.into()).collect(),
            policies: spec.policies.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<Component> for wadm::types::Component {
    fn from(component: Component) -> Self {
        wadm::types::Component {
            name: component.name,
            properties: component.properties.into(),
            traits: component
                .traits
                .map(|traits| traits.into_iter().map(|t| t.into()).collect()),
        }
    }
}

impl From<Policy> for wadm::types::Policy {
    fn from(policy: Policy) -> Self {
        wadm::types::Policy {
            name: policy.name,
            properties: policy.properties.into_iter().collect(),
            type_: policy.policy_type,
        }
    }
}

impl From<Properties> for wadm::types::Properties {
    fn from(properties: Properties) -> Self {
        match properties {
            Properties::Component { properties } => {
                wadm::types::Properties::Component(properties.into())
            }
            Properties::Capability { properties } => {
                wadm::types::Properties::Capability(properties.into())
            }
        }
    }
}

impl From<ComponentProperties> for wadm::types::ComponentProperties {
    fn from(properties: ComponentProperties) -> Self {
        wadm::types::ComponentProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
            secrets: properties.secrets.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<CapabilityProperties> for wadm::types::CapabilityProperties {
    fn from(properties: CapabilityProperties) -> Self {
        wadm::types::CapabilityProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
            secrets: properties.secrets.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<ConfigProperty> for wadm::types::ConfigProperty {
    fn from(property: ConfigProperty) -> Self {
        wadm::types::ConfigProperty {
            name: property.name,
            properties: property.properties.map(|props| props.into_iter().collect()),
        }
    }
}

impl From<SecretProperty> for wadm::types::SecretProperty {
    fn from(property: SecretProperty) -> Self {
        wadm::types::SecretProperty {
            name: property.name,
            properties: property.properties.into(),
        }
    }
}

impl From<SecretSourceProperty> for wadm::types::SecretSourceProperty {
    fn from(property: SecretSourceProperty) -> Self {
        wadm::types::SecretSourceProperty {
            policy: property.policy,
            key: property.key,
            field: property.field,
            version: property.version,
        }
    }
}

impl From<Trait> for wadm::types::Trait {
    fn from(trait_: Trait) -> Self {
        wadm::types::Trait {
            trait_type: trait_.trait_type,
            properties: trait_.properties.into(),
        }
    }
}

impl From<TraitProperty> for wadm::types::TraitProperty {
    fn from(property: TraitProperty) -> Self {
        match property {
            TraitProperty::Link(link) => wadm::types::TraitProperty::Link(link.into()),
            TraitProperty::SpreadScaler(spread) => {
                wadm::types::TraitProperty::Spreadscaler(spread.into())
            }
            TraitProperty::Custom(custom) => wadm::types::TraitProperty::Custom(custom.to_string()),
        }
    }
}

impl From<LinkProperty> for wadm::types::LinkProperty {
    fn from(property: LinkProperty) -> Self {
        wadm::types::LinkProperty {
            source: property.source.map(|c| c.into()),
            target: property.target.into(),
            namespace: property.namespace,
            package: property.package,
            interfaces: property.interfaces,
            name: property.name,
        }
    }
}

impl From<ConfigDefinition> for wadm::types::ConfigDefinition {
    fn from(definition: ConfigDefinition) -> Self {
        wadm::types::ConfigDefinition {
            config: definition.config.into_iter().map(|c| c.into()).collect(),
            secrets: definition.secrets.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<TargetConfig> for wadm::types::TargetConfig {
    fn from(config: TargetConfig) -> Self {
        wadm::types::TargetConfig {
            name: config.name,
            config: config.config.into_iter().map(|c| c.into()).collect(),
            secrets: config.secrets.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<SpreadScalerProperty> for wadm::types::SpreadscalerProperty {
    fn from(property: SpreadScalerProperty) -> Self {
        wadm::types::SpreadscalerProperty {
            instances: property.instances as u32,
            spread: property.spread.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<Spread> for wadm::types::Spread {
    fn from(spread: Spread) -> Self {
        wadm::types::Spread {
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
            StatusType::Waiting => wadm::types::StatusType::Waiting,
        }
    }
}

// Trait implementations for converting generated types to the types in the API module

impl From<wadm::types::StatusType> for StatusType {
    fn from(status: wadm::types::StatusType) -> Self {
        match status {
            wadm::types::StatusType::Undeployed => StatusType::Undeployed,
            wadm::types::StatusType::Reconciling => StatusType::Reconciling,
            wadm::types::StatusType::Deployed => StatusType::Deployed,
            wadm::types::StatusType::Failed => StatusType::Failed,
            wadm::types::StatusType::Waiting => StatusType::Waiting,
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

impl From<wadm::types::OamManifest> for Manifest {
    fn from(manifest: wadm::types::OamManifest) -> Self {
        Manifest {
            api_version: manifest.api_version,
            kind: manifest.kind,
            metadata: manifest.metadata.into(),
            spec: manifest.spec.into(),
        }
    }
}

impl From<wadm::types::Metadata> for Metadata {
    fn from(metadata: wadm::types::Metadata) -> Self {
        Metadata {
            name: metadata.name,
            annotations: metadata.annotations.into_iter().collect(),
            labels: metadata.labels.into_iter().collect(),
        }
    }
}

impl From<wadm::types::Specification> for Specification {
    fn from(spec: wadm::types::Specification) -> Self {
        Specification {
            components: spec.components.into_iter().map(|c| c.into()).collect(),
            policies: spec.policies.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<wadm::types::Component> for Component {
    fn from(component: wadm::types::Component) -> Self {
        Component {
            name: component.name,
            properties: component.properties.into(),
            traits: component
                .traits
                .map(|traits| traits.into_iter().map(|t| t.into()).collect()),
        }
    }
}

impl From<wadm::types::Policy> for Policy {
    fn from(policy: wadm::types::Policy) -> Self {
        Policy {
            name: policy.name,
            properties: policy.properties.into_iter().collect(),
            policy_type: policy.type_,
        }
    }
}

impl From<wadm::types::Properties> for Properties {
    fn from(properties: wadm::types::Properties) -> Self {
        match properties {
            wadm::types::Properties::Component(properties) => Properties::Component {
                properties: properties.into(),
            },
            wadm::types::Properties::Capability(properties) => Properties::Capability {
                properties: properties.into(),
            },
        }
    }
}

impl From<wadm::types::ComponentProperties> for ComponentProperties {
    fn from(properties: wadm::types::ComponentProperties) -> Self {
        ComponentProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
            secrets: properties.secrets.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<wadm::types::CapabilityProperties> for CapabilityProperties {
    fn from(properties: wadm::types::CapabilityProperties) -> Self {
        CapabilityProperties {
            image: properties.image,
            id: properties.id,
            config: properties.config.into_iter().map(|c| c.into()).collect(),
            secrets: properties.secrets.into_iter().map(|c| c.into()).collect(),
        }
    }
}

impl From<wadm::types::ConfigProperty> for ConfigProperty {
    fn from(property: wadm::types::ConfigProperty) -> Self {
        ConfigProperty {
            name: property.name,
            properties: property.properties.map(|props| props.into_iter().collect()),
        }
    }
}

impl From<wadm::types::SecretProperty> for SecretProperty {
    fn from(property: wadm::types::SecretProperty) -> Self {
        SecretProperty {
            name: property.name,
            properties: property.properties.into(),
        }
    }
}

impl From<wadm::types::SecretSourceProperty> for SecretSourceProperty {
    fn from(property: wadm::types::SecretSourceProperty) -> Self {
        SecretSourceProperty {
            policy: property.policy,
            key: property.key,
            field: property.field,
            version: property.version,
        }
    }
}

impl From<wadm::types::Trait> for Trait {
    fn from(trait_: wadm::types::Trait) -> Self {
        Trait {
            trait_type: trait_.trait_type,
            properties: trait_.properties.into(),
        }
    }
}

impl From<wadm::types::TraitProperty> for TraitProperty {
    fn from(property: wadm::types::TraitProperty) -> Self {
        match property {
            wadm::types::TraitProperty::Link(link) => TraitProperty::Link(link.into()),
            wadm::types::TraitProperty::Spreadscaler(spread) => {
                TraitProperty::SpreadScaler(spread.into())
            }
            wadm::types::TraitProperty::Custom(custom) => {
                TraitProperty::Custom(serde_json::value::Value::String(custom))
            }
        }
    }
}

impl From<wadm::types::LinkProperty> for LinkProperty {
    fn from(property: wadm::types::LinkProperty) -> Self {
        #[allow(deprecated)]
        LinkProperty {
            source: property.source.map(|c| c.into()),
            target: property.target.into(),
            namespace: property.namespace,
            package: property.package,
            interfaces: property.interfaces,
            name: property.name,
            source_config: None,
            target_config: None,
        }
    }
}

impl From<wadm::types::ConfigDefinition> for ConfigDefinition {
    fn from(definition: wadm::types::ConfigDefinition) -> Self {
        ConfigDefinition {
            config: definition.config.into_iter().map(|c| c.into()).collect(),
            secrets: definition.secrets.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<wadm::types::TargetConfig> for TargetConfig {
    fn from(config: wadm::types::TargetConfig) -> Self {
        TargetConfig {
            name: config.name,
            config: config.config.into_iter().map(|c| c.into()).collect(),
            secrets: config.secrets.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<wadm::types::SpreadscalerProperty> for SpreadScalerProperty {
    fn from(property: wadm::types::SpreadscalerProperty) -> Self {
        SpreadScalerProperty {
            instances: property.instances as usize,
            spread: property.spread.into_iter().map(|s| s.into()).collect(),
        }
    }
}

impl From<wadm::types::Spread> for Spread {
    fn from(spread: wadm::types::Spread) -> Self {
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
