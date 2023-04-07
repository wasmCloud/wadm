//! This is a temporary workaround to let us combine multiple topics into a single stream so
//! consumers work properly. This will be removed once NATS 2.10 is out and we have upgraded to
//! using it in wasmcloud projects

use std::{collections::HashMap, sync::Arc};

use async_nats::{
    jetstream::{
        consumer::pull::{Config as PullConfig, Stream as MessageStream},
        stream::Stream as JsStream,
        Context,
    },
    Error as NatsError, HeaderMap,
};
use bytes::Bytes;
use futures::StreamExt;
use tokio::{sync::RwLock, task::JoinHandle};
use tracing::{error, instrument, trace, warn};

type WorkHandles = Arc<RwLock<HashMap<String, JoinHandle<anyhow::Result<()>>>>>;

/// A simple NATS consumer that takes each incoming message and maps it to `{prefix}.{lattice-id}`
/// (e.g. `wadm.evt.default`)
pub struct Mirror {
    stream: JsStream,
    prefix: String,
    handles: WorkHandles,
}

impl Mirror {
    /// Returns a new Mirror for the given stream
    pub fn new(stream: JsStream, prefix: &str) -> Mirror {
        Mirror {
            stream,
            prefix: prefix.to_owned(),
            handles: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub async fn monitor_lattice(&self, subject: &str, lattice_id: &str) -> Result<(), NatsError> {
        if let Some(handle) = self.handles.read().await.get(lattice_id) {
            if !handle.is_finished() {
                return Ok(());
            }
            warn!("Handle was marked as completed. Starting monitor again");
        }
        let consumer_name = format!("wadm_mirror_{lattice_id}");
        trace!("Creating mirror consumer for lattice");
        let consumer = self
            .stream
            .get_or_create_consumer(
                &consumer_name,
                PullConfig {
                    durable_name: Some(consumer_name.clone()),
                    name: Some(consumer_name.clone()),
                    description: Some(format!("Durable wadm mirror consumer for {lattice_id}")),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: std::time::Duration::from_secs(2),
                    max_deliver: 3,
                    deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                    filter_subject: subject.to_owned(),
                    ..Default::default()
                },
            )
            .await?;
        let messages = consumer
            .stream()
            .max_messages_per_batch(1)
            .messages()
            .await?;
        let handle = tokio::spawn(mirror_worker(
            messages,
            format!("{}.{lattice_id}", self.prefix),
        ));

        self.handles
            .write()
            .await
            .insert(lattice_id.to_owned(), handle);
        Ok(())
    }
}

#[instrument(level = "info", skip(messages))]
async fn mirror_worker(mut messages: MessageStream, publish_topic: String) -> anyhow::Result<()> {
    loop {
        match messages.next().await {
            Some(Ok(msg)) => {
                // NOTE(thomastaylor312): I can't actually consume the payload because I can't ack
                // (without copy pasting the ack code) due to the partial move. I am working with
                // the async_nats maintainers to see if we can add something to get around this, but
                // in the meantime, we're just gonna deal with it
                if let Err(e) = republish(
                    &msg.context,
                    publish_topic.clone(),
                    msg.message.headers.clone().unwrap_or_default(),
                    msg.message.payload.clone(),
                )
                .await
                {
                    error!(error = ?e, "Unable to republish message. Will nak and retry");
                    if let Err(e) = msg
                        .ack_with(async_nats::jetstream::AckKind::Nak(None))
                        .await
                    {
                        warn!(error = ?e, "Unable to nak. This message will timeout and redeliver");
                    }
                } else if let Err(e) = msg.double_ack().await {
                    // There isn't much we can do if this happens as this means we
                    // successfully published, but couldn't nak
                    error!(error = ?e, "Unable to ack. This message will timeout and redeliver a duplicate message");
                }
            }
            Some(Err(e)) => {
                warn!(error = ?e, "Error when processing message to mirror");
                continue;
            }
            None => {
                error!("Mirror stream stopped processing");
                anyhow::bail!("Mirror stream stopped processing")
            }
        }
    }
}

async fn republish(
    context: &Context,
    topic: String,
    headers: HeaderMap,
    payload: Bytes,
) -> anyhow::Result<()> {
    // NOTE(thomastaylor312): A future improvement could be retries here
    let acker = context
        .publish_with_headers(topic, headers, payload)
        .await
        .map_err(|e| anyhow::anyhow!("Unable to republish message: {e:?}"))?;
    acker
        .await
        .map(|_| ())
        .map_err(|e| anyhow::anyhow!("Error waiting for message acknowledgement {e:?}"))
}
