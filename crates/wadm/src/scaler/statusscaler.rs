use anyhow::Result;
use async_trait::async_trait;
use wadm_types::{api::StatusInfo, TraitProperty};

use crate::{commands::Command, events::Event, scaler::Scaler};

/// The StatusScaler is a scaler that only reports a predefined status and does not perform any actions.
/// It's primarily used as a placeholder for a scaler that wadm failed to initialize for reasons that
/// couldn't be caught during deployment, and will not be fixed until a new version of the app is deployed.
pub struct StatusScaler {
    id: String,
    kind: String,
    name: String,
    status: StatusInfo,
}

#[async_trait]
impl Scaler for StatusScaler {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        &self.kind
    }

    fn name(&self) -> String {
        self.name.to_string()
    }

    async fn status(&self) -> StatusInfo {
        self.status.clone()
    }

    async fn update_config(&mut self, _config: TraitProperty) -> Result<Vec<Command>> {
        Ok(vec![])
    }

    async fn handle_event(&self, _event: &Event) -> Result<Vec<Command>> {
        Ok(Vec::with_capacity(0))
    }

    async fn reconcile(&self) -> Result<Vec<Command>> {
        Ok(Vec::with_capacity(0))
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        Ok(Vec::with_capacity(0))
    }
}

impl StatusScaler {
    pub fn new(
        id: impl AsRef<str>,
        kind: impl AsRef<str>,
        name: impl AsRef<str>,
        status: StatusInfo,
    ) -> Self {
        StatusScaler {
            id: id.as_ref().to_string(),
            kind: kind.as_ref().to_string(),
            name: name.as_ref().to_string(),
            status,
        }
    }
}
