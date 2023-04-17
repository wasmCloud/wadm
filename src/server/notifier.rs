use cloudevents::{EventBuilder, EventBuilderV10};
use tracing::{instrument, trace};

use crate::{
    events::{EventType, ManifestPublished, ManifestUnpublished},
    model::Manifest,
    publisher::Publisher,
};

const WADM_SOURCE: &str = "wadm";

/// A notifier that publishes changes about manifests with the given publisher
pub struct ManifestNotifier<P> {
    prefix: String,
    publisher: P,
}

impl<P: Publisher> ManifestNotifier<P> {
    /// Creates a new notifier with the given prefix and publisher. This prefix should be something like
    /// `wadm.evt` that is used to form the full topic to send to
    pub fn new(prefix: &str, publisher: P) -> ManifestNotifier<P> {
        let trimmer: &[_] = &['.', '>', '*'];
        ManifestNotifier {
            prefix: prefix.trim().trim_matches(trimmer).to_owned(),
            publisher,
        }
    }

    #[instrument(level = "trace", skip(self, data))]
    async fn send_event(
        &self,
        lattice_id: &str,
        ty: &str,
        data: serde_json::Value,
    ) -> anyhow::Result<()> {
        let event = EventBuilderV10::new()
            .id(uuid::Uuid::new_v4().to_string())
            .source(WADM_SOURCE)
            .time(chrono::Utc::now())
            .ty(ty)
            .data("application/json", data)
            .build()?;
        // NOTE(thomastaylor312): A future improvement could be retries here
        trace!("Sending notification event");
        self.publisher
            .publish(
                serde_json::to_vec(&event)?,
                Some(&format!("{}.{lattice_id}", self.prefix)),
            )
            .await
    }

    pub async fn deployed(&self, lattice_id: &str, manifest: Manifest) -> anyhow::Result<()> {
        self.send_event(
            lattice_id,
            ManifestPublished::TYPE,
            serde_json::to_value(manifest)?,
        )
        .await
    }

    pub async fn undeployed(&self, lattice_id: &str, name: &str) -> anyhow::Result<()> {
        self.send_event(
            lattice_id,
            ManifestUnpublished::TYPE,
            serde_json::json!({
                "name": name,
            }),
        )
        .await
    }
}
