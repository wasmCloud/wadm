//! Types for observing a nats cluster for new lattices

use async_nats::Subscriber;
use futures::{stream::SelectAll, StreamExt, TryFutureExt};
use tracing::{debug, error, instrument, trace, warn};

use wadm::{
    consumers::{manager::ConsumerManager, CommandConsumer, EventConsumer},
    events::{EventType, HostHeartbeat, HostStarted},
    nats_utils::LatticeIdParser,
    storage::{nats_kv::NatsKvStore, reaper::Reaper, Store},
    workers::{CommandWorker, EventWorker},
    DEFAULT_COMMANDS_TOPIC,
};

use super::connections::ControlClientConstructor;

pub(crate) struct Observer<S> {
    pub(crate) client_builder: ControlClientConstructor,
    pub(crate) parser: LatticeIdParser,
    pub(crate) command_manager: ConsumerManager<CommandConsumer>,
    pub(crate) event_manager: ConsumerManager<EventConsumer>,
    pub(crate) client: async_nats::Client,
    pub(crate) store: S,
    pub(crate) reaper: Reaper<NatsKvStore>,
}

impl<S: Store + Send + Sync + Clone + 'static> Observer<S> {
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
                    let lattice_id = match self.parser.parse(&msg.subject) {
                        Some(id) => id,
                        None => {
                            trace!(subject = %msg.subject, "Found non-matching lattice subject");
                            continue;
                        }
                    };

                    // Create the reaper for this lattice. This operation returns early if it is
                    // already running
                    self.reaper.observe(lattice_id);

                    let needs_command = !self.command_manager.has_consumer(&msg.subject).await;
                    let needs_event = !self.event_manager.has_consumer(&msg.subject).await;
                    let client = if needs_command || needs_event {
                        match self.client_builder.get_connection(lattice_id).await {
                            Ok(c) => c,
                            Err(e) => {
                                error!(error = %e, %lattice_id, "Couldn't construct control client for consumer. Will retry on next heartbeat");
                                continue;
                            }
                        }
                    } else {
                        continue;
                    };
                    if needs_command {
                        debug!(%lattice_id, subject = %msg.subject, "Found unmonitored lattice, adding command consumer");
                        self.command_manager.add_for_lattice(&DEFAULT_COMMANDS_TOPIC.replace('*', lattice_id), lattice_id, CommandWorker::new(client.clone())).await.unwrap_or_else(|e| {
                            error!(error = %e, %lattice_id, "Couldn't add command consumer. Will retry on next heartbeat");
                        })
                    }
                    if needs_event {
                        debug!(%lattice_id, subject = %msg.subject, "Found unmonitored lattice, adding event consumer");
                        self.event_manager.add_for_lattice(&msg.subject, lattice_id, EventWorker::new(self.store.clone(), client)).await.unwrap_or_else(|e| {
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

// This is a stupid hacky function to check that this is a host started or host heartbeat event
// without actually parsing
fn is_event_we_care_about(data: &[u8]) -> bool {
    let string_data = match std::str::from_utf8(data) {
        Ok(s) => s,
        Err(_) => return false,
    };

    string_data.contains(HostStarted::TYPE) || string_data.contains(HostHeartbeat::TYPE)
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
