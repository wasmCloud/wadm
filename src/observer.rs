//! Types for observing a nats cluster for new lattices

use async_nats::Subscriber;
use futures::{stream::SelectAll, StreamExt, TryFutureExt};
use tracing::{debug, error, instrument, trace, warn};

use wadm::{
    consumers::{
        manager::{ConsumerManager, WorkerCreator},
        CommandConsumer, EventConsumer,
    },
    events::{EventType, HostHeartbeat, HostStarted, ManifestPublished},
    nats_utils::LatticeIdParser,
    storage::{nats_kv::NatsKvStore, reaper::Reaper, Store},
    DEFAULT_COMMANDS_TOPIC, DEFAULT_WADM_EVENT_CONSUMER_TOPIC,
};

use super::{CommandWorkerCreator, EventWorkerCreator};

pub(crate) struct Observer<StateStore> {
    pub(crate) parser: LatticeIdParser,
    pub(crate) command_manager: ConsumerManager<CommandConsumer>,
    pub(crate) event_manager: ConsumerManager<EventConsumer>,
    pub(crate) client: async_nats::Client,
    pub(crate) reaper: Reaper<NatsKvStore>,
    pub(crate) event_worker_creator: EventWorkerCreator<StateStore>,
    pub(crate) command_worker_creator: CommandWorkerCreator,
}

impl<StateStore> Observer<StateStore>
where
    StateStore: Store + Send + Sync + Clone + 'static,
{
    /// Watches the given topic (with wildcards) for wasmbus events. If it finds a lattice that it
    /// isn't managing, it will start managing it immediately
    ///
    /// If this errors, it should be considered fatal
    #[instrument(level = "info", skip(self))]
    pub(crate) async fn observe(mut self, subscribe_topics: Vec<String>) -> anyhow::Result<()> {
        let mut sub = get_subscriber(&self.client, subscribe_topics.clone()).await?;
        loop {
            match sub.next().await {
                Some(msg) => {
                    if !is_event_we_care_about(&msg.payload) {
                        continue;
                    }

                    let Some(lattice_info) = self.parser.parse(&msg.subject) else {
                        trace!(subject = %msg.subject, "Found non-matching lattice subject");
                        continue;
                    };
                    let lattice_id = lattice_info.lattice_id();
                    let multitenant_prefix = lattice_info.multitenant_prefix();
                    let event_subject = lattice_info.event_subject();

                    // Create the reaper for this lattice. This operation returns early if it is
                    // already running
                    self.reaper.observe(lattice_id);

                    let command_topic = DEFAULT_COMMANDS_TOPIC.replace('*', lattice_id);
                    let events_topic = DEFAULT_WADM_EVENT_CONSUMER_TOPIC.replace('*', lattice_id);
                    let needs_command = !self.command_manager.has_consumer(&command_topic).await;
                    let needs_event = !self.event_manager.has_consumer(&events_topic).await;
                    if needs_command {
                        debug!(%lattice_id, subject = %event_subject, mapped_subject = %command_topic, "Found unmonitored lattice, adding command consumer");
                        let worker = match self
                            .command_worker_creator
                            .create(lattice_id, multitenant_prefix)
                            .await
                        {
                            Ok(w) => w,
                            Err(e) => {
                                error!(error = %e, %lattice_id, "Couldn't construct worker for command consumer. Will retry on next heartbeat");
                                continue;
                            }
                        };
                        self.command_manager
                            .add_for_lattice(&command_topic, lattice_id, multitenant_prefix, worker)
                            .await
                            .unwrap_or_else(|e| {
                                error!(error = %e, %lattice_id, "Couldn't add command consumer. Will retry on next heartbeat");
                            })
                    }
                    if needs_event {
                        debug!(%lattice_id, subject = %event_subject, mapped_subject = %events_topic,  "Found unmonitored lattice, adding event consumer");
                        let worker = match self
                            .event_worker_creator
                            .create(lattice_id, multitenant_prefix)
                            .await
                        {
                            Ok(w) => w,
                            Err(e) => {
                                error!(error = %e, %lattice_id, "Couldn't construct worker for event consumer. Will retry on next heartbeat");
                                continue;
                            }
                        };
                        self.event_manager
                            .add_for_lattice(&events_topic, lattice_id, multitenant_prefix, worker)
                            .await
                            .unwrap_or_else(|e| {
                                error!(error = %e, %lattice_id, "Couldn't add event consumer. Will retry on next heartbeat");
                            })
                    }
                }
                None => {
                    warn!("Observer subscriber hang up. Attempting to restart");
                    sub = get_subscriber(&self.client, subscribe_topics.clone()).await?;
                }
            }
        }
    }
}

// This is a stupid hacky function to check that this is a host started, host heartbeat, or
// manifest_published event without actually parsing
fn is_event_we_care_about(data: &[u8]) -> bool {
    let string_data = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return false,
    };

    string_data.contains(HostStarted::TYPE)
        || string_data.contains(HostHeartbeat::TYPE)
        || string_data.contains(ManifestPublished::TYPE)
}

async fn get_subscriber(
    client: &async_nats::Client,
    subscribe_topics: Vec<String>,
) -> anyhow::Result<SelectAll<Subscriber>> {
    let futs = subscribe_topics
        .clone()
        .into_iter()
        .map(|t| client.subscribe(t).map_err(|e| anyhow::anyhow!("{e:?}")));
    let subs: Vec<Subscriber> = futures::future::join_all(futs)
        .await
        .into_iter()
        .collect::<Result<_, anyhow::Error>>()?;
    Ok(futures::stream::select_all(subs))
}
