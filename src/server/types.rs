use serde::{Deserialize, Serialize};

/// The request body for getting a manifest
#[derive(Debug, Serialize, Deserialize)]
pub struct GetModelRequest {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ModelSummary {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub deployed: bool,
    pub status: StatusType,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct VersionInfo {
    pub version: String,
    pub deployed: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteModelRequest {
    #[serde(default)]
    pub version: String,
    #[serde(default)]
    pub delete_all: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteModelResponse {
    pub result: DeleteResult,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub undeploy: bool,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DeleteResult {
    Deleted,
    Error,
    Noop,
}

/// The current status of a model
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct Status {
    #[serde(rename = "status")]
    pub info: StatusInfo,
    pub deployed: bool,
    // TODO: Fill out the rest of the status stuff
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StatusInfo {
    #[serde(rename = "type")]
    pub status_type: StatusType,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq)]
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
    fn sum<I: Iterator<Item = Self>>(iter: I) -> Self {
        iter.fold(
            // Start with Ready because it is the first thing overridden
            Self::Ready,
            |a, b| a + b,
        )
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
    }
}
