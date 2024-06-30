use cloudevents::Event as CloudEvent;
use tracing::{instrument, trace};
use wadm_types::Manifest;

use crate::{
    events::{Event, ManifestPublished, ManifestUnpublished},
    publisher::Publisher,
};

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

    #[instrument(level = "trace", skip(self))]
    async fn send_event(
        &self,
        lattice_id: &str,
        event_subject_key: &str,
        event: Event,
    ) -> anyhow::Result<()> {
        let event: CloudEvent = event.try_into()?;
        // NOTE(thomastaylor312): A future improvement could be retries here
        trace!("Sending notification event");
        self.publisher
            .publish(
                serde_json::to_vec(&event)?,
                Some(&format!("{}.{lattice_id}.{event_subject_key}", self.prefix)),
            )
            .await
    }

    pub async fn deployed(&self, lattice_id: &str, manifest: Manifest) -> anyhow::Result<()> {
        self.send_event(
            lattice_id,
            "manifest_published",
            Event::ManifestPublished(ManifestPublished { manifest }),
        )
        .await
    }

    pub async fn undeployed(&self, lattice_id: &str, name: &str) -> anyhow::Result<()> {
        self.send_event(
            lattice_id,
            "manifest_unpublished",
            Event::ManifestUnpublished(ManifestUnpublished {
                name: name.to_owned(),
            }),
        )
        .await
    }
}
