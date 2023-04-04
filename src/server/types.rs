use serde::{Deserialize, Serialize};

use crate::model::{ComponentType, Manifest};

/// The request body for getting a manifest
#[derive(Debug, Serialize, Deserialize)]
pub struct GetModelRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

/// The response from a get request
#[derive(Debug, Serialize, Deserialize)]
pub struct GetModelResponse {
    pub result: GetResult,
    #[serde(default)]
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<Manifest>,
}

/// Possible outcomes of a get request
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum GetResult {
    Error,
    Success,
    NotFound,
}

/// The type returned when putting a model
#[derive(Debug, Serialize, Deserialize)]
pub struct PutModelResponse {
    pub result: PutResult,
    #[serde(default)]
    pub total_versions: usize,
    #[serde(default)]
    pub current_version: String,
    #[serde(default)]
    pub message: String,
}

/// Possible outcomes of a put request
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum PutResult {
    Error,
    Created,
    NewVersion,
}

/// Summary of a given model returned when listing
#[derive(Debug, Serialize, Deserialize)]
pub struct ModelSummary {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub deployed: bool,
    pub status: StatusType,
}

/// The response to a versions request
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionResponse {
    pub result: GetResult,
    #[serde(default)]
    pub message: String,
    pub versions: Vec<VersionInfo>,
}

/// Information about a given version of a model, returned as part of a list of all versions
#[derive(Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub deployed: bool,
}

/// A request for deleting a model
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteModelRequest {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub delete_all: bool,
}

/// A response from a delete request
#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteModelResponse {
    pub result: DeleteResult,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub undeploy: bool,
}

/// All possible outcomes of a delete operation
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeleteResult {
    Deleted,
    Error,
    Noop,
}

/// A request for deploying a model.
///
/// If the given version is empty (or the body is empty), it will deploy the latest version. If the
/// version is set to "latest", it will also deploy the latest version
#[derive(Debug, Serialize, Deserialize)]
pub struct DeployModelRequest {
    pub version: Option<String>,
}

/// A response from a deploy request
#[derive(Debug, Serialize, Deserialize)]
pub struct DeployModelResponse {
    pub result: DeployResult,
    #[serde(default)]
    pub message: String,
}

/// All possible outcomes of a deploy operation
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeployResult {
    Error,
    Acknowledged,
    NotFound,
}

/// A request to undeploy a model
#[derive(Debug, Serialize, Deserialize)]
pub struct UndeployModelRequest {
    pub non_destructive: bool,
}

/// A response to a status request
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub result: StatusResult,
    #[serde(default)]
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
}

/// All possible outcomes of a status operation
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StatusResult {
    Error,
    Ok,
    NotFound,
}

/// The current status of a model
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Status {
    pub version: String,
    #[serde(rename = "status")]
    pub info: StatusInfo,
    pub components: Vec<ComponentStatus>,
}

/// The current status of a component
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ComponentStatus {
    pub name: String,
    #[serde(rename = "type")]
    pub component_type: ComponentType,
    #[serde(rename = "status")]
    pub info: StatusInfo,
    pub traits: Vec<TraitStatus>,
}

/// The current status of a trait
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TraitStatus {
    #[serde(rename = "type")]
    pub trait_type: String,
    #[serde(rename = "status")]
    pub info: StatusInfo,
}

/// Common high-level status information
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct StatusInfo {
    #[serde(rename = "type")]
    pub status_type: StatusType,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub message: String,
}

/// All possible status types
#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum StatusType {
    Undeployed,
    Compensating,
    Ready,
    Failed,
}

impl Default for StatusType {
    fn default() -> Self {
        StatusType::Undeployed
    }
}

// Implementing add makes it easy for use to get an aggregate status by summing all of them together
impl std::ops::Add for StatusType {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        // If any match, return the same status
        if self == rhs {
            return self;
        }

        // Because we match on exact matches above, we don't have to handle them in the match below.
        // For all of the statuses _except_ ready, they will override the other status. Order of the
        // matching matters below
        match (self, rhs) {
            // Anything that is failed means the whole thing is failed
            (Self::Failed, _) => Self::Failed,
            (_, Self::Failed) => Self::Failed,
            // If anything is undeployed, the whole thing is
            (Self::Undeployed, _) => Self::Undeployed,
            (_, Self::Undeployed) => Self::Undeployed,
            (Self::Compensating, _) => Self::Compensating,
            (_, Self::Compensating) => Self::Compensating,
            _ => unreachable!("aggregating StatusType failure. This is programmer error"),
        }
    }
}

impl std::iter::Sum for StatusType {
    fn sum<I: Iterator<Item = Self>>(mut iter: I) -> Self {
        // Grab the first status to use as our seed, defaulting to undeployed
        let first = iter.next().unwrap_or_default();
        iter.fold(first, |a, b| a + b)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_status_aggregate() {
        assert!(matches!(
            [StatusType::Ready, StatusType::Ready].into_iter().sum(),
            StatusType::Ready
        ));

        assert!(matches!(
            [StatusType::Undeployed, StatusType::Undeployed]
                .into_iter()
                .sum(),
            StatusType::Undeployed
        ));

        assert!(matches!(
            [StatusType::Undeployed, StatusType::Failed]
                .into_iter()
                .sum(),
            StatusType::Failed
        ));

        assert!(matches!(
            [StatusType::Compensating, StatusType::Undeployed]
                .into_iter()
                .sum(),
            StatusType::Undeployed
        ));

        assert!(matches!(
            [StatusType::Ready, StatusType::Undeployed]
                .into_iter()
                .sum(),
            StatusType::Undeployed
        ));

        assert!(matches!(
            [
                StatusType::Ready,
                StatusType::Compensating,
                StatusType::Undeployed,
                StatusType::Failed
            ]
            .into_iter()
            .sum(),
            StatusType::Failed
        ));

        let empty: Vec<StatusType> = Vec::new();
        assert!(matches!(empty.into_iter().sum(), StatusType::Undeployed));
    }
}
