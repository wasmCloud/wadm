use async_nats::jetstream::Context;
use cloudevents::{EventBuilder, EventBuilderV10};
use tracing::{instrument, trace};

use crate::{
    events::{EventType, ManifestPublished, ManifestUnpublished},
    model::Manifest,
};

const WADM_SOURCE: &str = "wadm";

/// A trait that indicates something can be used to notify via message or other forms of hooks about
/// manifest changes
#[async_trait::async_trait]
pub trait ManifestNotifier {
    /// Notifies about a recently deployed manifest. Returns an error if it was unable to send the
    /// notification.
    ///
    /// It isn't required to confirm that the notification is received, but is recommended
    async fn deployed(&self, lattice_id: &str, manifest: Manifest) -> anyhow::Result<()>;

    /// Notifies about a manifest with the given name that has been undeployed. This is all that is
    /// needed for undeploys because the name must be unique and it is possible that the manifest no
    /// longer exists in the store. Returns an error if it was unable to send the notification.
    ///
    /// It isn't required to confirm that the notification is received, but is recommended
    async fn undeployed(&self, lattice_id: &str, name: &str) -> anyhow::Result<()>;
}

/// A [`ManifestNotifier`] that publishes a NATS message using a jetstream context
pub struct StreamNotifier {
    prefix: String,
    context: Context,
}

impl StreamNotifier {
    /// Creates a new stream notifier with the given prefix. This prefix should be something like
    /// `wadm.evt` that is used to form the full topic to send to
    pub fn new(prefix: &str, context: Context) -> StreamNotifier {
        let trimmer: &[_] = &['.', '>', '*'];
        StreamNotifier {
            prefix: prefix.trim().trim_matches(trimmer).to_owned(),
            context,
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
        let acker = self
            .context
            .publish(
                format!("{}.{lattice_id}", self.prefix),
                serde_json::to_vec(&event)?.into(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("Unable to send notification: {e:?}"))?;
        acker
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("Error waiting for message acknowledgement {e:?}"))
    }
}

#[async_trait::async_trait]
impl ManifestNotifier for StreamNotifier {
    async fn deployed(&self, lattice_id: &str, manifest: Manifest) -> anyhow::Result<()> {
        self.send_event(
            lattice_id,
            ManifestPublished::TYPE,
            serde_json::to_value(manifest)?,
        )
        .await
    }

    async fn undeployed(&self, lattice_id: &str, name: &str) -> anyhow::Result<()> {
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
