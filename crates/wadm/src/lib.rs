use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use async_nats::jetstream::{stream::Stream, Context};
use config::WadmConfig;
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::log::debug;

#[cfg(feature = "http_admin")]
use anyhow::Context as _;
#[cfg(feature = "http_admin")]
use hyper::body::Bytes;
#[cfg(feature = "http_admin")]
use hyper_util::rt::{TokioExecutor, TokioIo};
#[cfg(feature = "http_admin")]
use tokio::net::TcpListener;

use crate::{
    connections::ControlClientConstructor,
    consumers::{
        manager::{ConsumerManager, WorkerCreator},
        *,
    },
    nats_utils::LatticeIdParser,
    scaler::manager::{ScalerManager, WADM_NOTIFY_PREFIX},
    server::{ManifestNotifier, Server},
    storage::{nats_kv::NatsKvStore, reaper::Reaper},
    workers::{CommandPublisher, CommandWorker, EventWorker, StatusPublisher},
};

pub use nats::StreamPersistence;

pub mod commands;
pub mod config;
pub mod consumers;
pub mod events;
pub mod nats_utils;
pub mod publisher;
pub mod scaler;
pub mod server;
pub mod storage;
pub mod workers;

mod connections;
pub(crate) mod model;
mod nats;
mod observer;
#[cfg(test)]
pub mod test_util;

/// Default amount of time events should stay in the stream. This is the 2x heartbeat interval, plus
/// some wiggle room. Exported to make setting defaults easy
pub const DEFAULT_EXPIRY_TIME: Duration = Duration::from_secs(70);
/// Default topic to listen to for all lattice events
pub const DEFAULT_EVENTS_TOPIC: &str = "wasmbus.evt.*.>";
/// Default topic to listen to for all lattice events in a multitenant deployment
pub const DEFAULT_MULTITENANT_EVENTS_TOPIC: &str = "*.wasmbus.evt.*.>";
/// Default topic to listen to for all commands
pub const DEFAULT_COMMANDS_TOPIC: &str = "wadm.cmd.*";
/// Default topic to listen to for all status updates. wadm.status.<lattice_id>.<manifest_name>
pub const DEFAULT_STATUS_TOPIC: &str = "wadm.status.*.*";
/// Default topic to listen to for all wadm event updates
pub const DEFAULT_WADM_EVENTS_TOPIC: &str = "wadm.evt.*.>";
/// Default internal wadm event consumer listen topic for the merged wadm and wasmbus events stream.
pub const DEFAULT_WADM_EVENT_CONSUMER_TOPIC: &str = "wadm_event_consumer.evt.*.>";
/// Managed by annotation used for labeling things properly in wadm
pub const MANAGED_BY_ANNOTATION: &str = "wasmcloud.dev/managed-by";
/// Identifier for managed by annotation. This is the value [`MANAGED_BY_ANNOTATION`] is set to
pub const MANAGED_BY_IDENTIFIER: &str = "wadm";
/// An annotation that denotes which model a resource belongs to
pub const APP_SPEC_ANNOTATION: &str = "wasmcloud.dev/appspec";
/// An annotation that denotes which scaler is managing a resource
pub const SCALER_KEY: &str = "wasmcloud.dev/scaler";
/// The default link name. In the future, this will likely be pulled in from another crate
pub const DEFAULT_LINK_NAME: &str = "default";
/// Default stream name for wadm events
pub const DEFAULT_WADM_EVENT_STREAM_NAME: &str = "wadm_events";
/// Default stream name for wadm event consumer
pub const DEFAULT_WADM_EVENT_CONSUMER_STREAM_NAME: &str = "wadm_event_consumer";
/// Default stream name for wadm commands
pub const DEFAULT_COMMAND_STREAM_NAME: &str = "wadm_commands";
/// Default stream name for wadm status
pub const DEFAULT_STATUS_STREAM_NAME: &str = "wadm_status";
/// Default stream name for wadm notifications
pub const DEFAULT_NOTIFY_STREAM_NAME: &str = "wadm_notify";
/// Default stream name for wasmbus events
pub const DEFAULT_WASMBUS_EVENT_STREAM_NAME: &str = "wasmbus_events";

/// Start wadm with the provided [WadmConfig], returning [JoinSet] with two tasks:
/// 1. The server task that listens for API requests
/// 2. The observer task that listens for events and commands
///
/// When embedding wadm in another application, this function should be called to start the wadm
/// server and observer tasks.
///
/// # Usage
///
/// ```no_run
/// async {
///     let config = wadm::config::WadmConfig::default();
///     let mut wadm = wadm::start_wadm(config).await.expect("should start wadm");
///     tokio::select! {
///         res = wadm.join_next() => {
///             match res {
///                 Some(Ok(_)) => {
///                     tracing::info!("WADM has exited successfully");
///                     std::process::exit(0);
///                 }
///                 Some(Err(e)) => {
///                     tracing::error!("WADM has exited with an error: {:?}", e);
///                     std::process::exit(1);
///                 }
///                 None => {
///                     tracing::info!("WADM server did not start");
///                     std::process::exit(0);
///                 }
///             }
///         }
///         _ = tokio::signal::ctrl_c() => {
///             tracing::info!("Received Ctrl+C, shutting down");
///             std::process::exit(0);
///         }
///     }
/// };
/// ```
pub async fn start_wadm(config: WadmConfig) -> Result<JoinSet<Result<()>>> {
    // Build storage adapter for lattice state (on by default)
    let (client, context) = nats::get_client_and_context(
        config.nats_server.clone(),
        config.domain.clone(),
        config.nats_seed.clone(),
        config.nats_jwt.clone(),
        config.nats_creds.clone(),
        config.nats_tls_ca_file.clone(),
    )
    .await?;

    // TODO: We will probably need to set up all the flags (like lattice prefix and topic prefix) down the line
    let connection_pool = ControlClientConstructor::new(client.clone(), None);

    let trimmer: &[_] = &['.', '>', '*'];

    let store = nats::ensure_kv_bucket(
        &context,
        config.state_bucket,
        1,
        config.max_state_bucket_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    let state_storage = NatsKvStore::new(store);

    let manifest_storage = nats::ensure_kv_bucket(
        &context,
        config.manifest_bucket,
        1,
        config.max_manifest_bucket_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    let internal_stream_name = |stream_name: &str| -> String {
        match config.stream_prefix.clone() {
            Some(stream_prefix) => {
                format!(
                    "{}.{}",
                    stream_prefix.trim_end_matches(trimmer),
                    stream_name
                )
            }
            None => stream_name.to_string(),
        }
    };

    debug!("Ensuring wadm event stream");

    let event_stream = nats::ensure_limits_stream(
        &context,
        internal_stream_name(DEFAULT_WADM_EVENT_STREAM_NAME),
        vec![DEFAULT_WADM_EVENTS_TOPIC.to_owned()],
        Some(
            "A stream that stores all events coming in on the wadm.evt subject in a cluster"
                .to_string(),
        ),
        config.max_event_stream_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    debug!("Ensuring command stream");

    let command_stream = nats::ensure_stream(
        &context,
        internal_stream_name(DEFAULT_COMMAND_STREAM_NAME),
        vec![DEFAULT_COMMANDS_TOPIC.to_owned()],
        Some("A stream that stores all commands for wadm".to_string()),
        config.max_command_stream_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    let status_stream = nats::ensure_status_stream(
        &context,
        internal_stream_name(DEFAULT_STATUS_STREAM_NAME),
        vec![DEFAULT_STATUS_TOPIC.to_owned()],
        config.max_status_stream_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    debug!("Ensuring wasmbus event stream");

    // Remove the previous wadm_(multitenant)_mirror streams so that they don't
    // prevent us from creating the new wasmbus_(multitenant)_events stream
    // TODO(joonas): Remove this some time in the future once we're confident
    // enough that there are no more wadm_(multitenant)_mirror streams around.
    for mirror_stream_name in &["wadm_mirror", "wadm_multitenant_mirror"] {
        if (context.get_stream(mirror_stream_name).await).is_ok() {
            context.delete_stream(mirror_stream_name).await?;
        }
    }

    let wasmbus_event_subjects = match config.multitenant {
        true => vec![DEFAULT_MULTITENANT_EVENTS_TOPIC.to_owned()],
        false => vec![DEFAULT_EVENTS_TOPIC.to_owned()],
    };

    let wasmbus_event_stream = nats::ensure_limits_stream(
        &context,
        DEFAULT_WASMBUS_EVENT_STREAM_NAME.to_string(),
        wasmbus_event_subjects.clone(),
        Some(
            "A stream that stores all events coming in on the wasmbus.evt subject in a cluster"
                .to_string(),
        ),
        config.max_wasmbus_event_stream_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    debug!("Ensuring notify stream");

    let notify_stream = nats::ensure_notify_stream(
        &context,
        DEFAULT_NOTIFY_STREAM_NAME.to_owned(),
        vec![format!("{WADM_NOTIFY_PREFIX}.*")],
        config.max_notify_stream_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    debug!("Ensuring event consumer stream");

    let event_consumer_stream = nats::ensure_event_consumer_stream(
        &context,
        DEFAULT_WADM_EVENT_CONSUMER_STREAM_NAME.to_owned(),
        DEFAULT_WADM_EVENT_CONSUMER_TOPIC.to_owned(),
        vec![&wasmbus_event_stream, &event_stream],
        Some(
            "A stream that sources from wadm_events and wasmbus_events for wadm event consumer's use"
                .to_string(),
        ),
        config.max_event_consumer_stream_bytes,
        config.stream_persistence.into(),
    )
    .await?;

    debug!("Creating event consumer manager");

    let permit_pool = Arc::new(Semaphore::new(
        config.max_jobs.unwrap_or(Semaphore::MAX_PERMITS),
    ));
    let event_worker_creator = EventWorkerCreator {
        state_store: state_storage.clone(),
        manifest_store: manifest_storage.clone(),
        pool: connection_pool.clone(),
        command_topic_prefix: DEFAULT_COMMANDS_TOPIC.trim_matches(trimmer).to_owned(),
        publisher: context.clone(),
        notify_stream,
        status_stream: status_stream.clone(),
    };
    let events_manager: ConsumerManager<EventConsumer> = ConsumerManager::new(
        permit_pool.clone(),
        event_consumer_stream,
        event_worker_creator.clone(),
        config.multitenant,
    )
    .await;

    debug!("Creating command consumer manager");

    let command_worker_creator = CommandWorkerCreator {
        pool: connection_pool,
    };
    let commands_manager: ConsumerManager<CommandConsumer> = ConsumerManager::new(
        permit_pool.clone(),
        command_stream,
        command_worker_creator.clone(),
        config.multitenant,
    )
    .await;

    // TODO(thomastaylor312): We might want to figure out how not to run this globally. Doing a
    // synthetic event sent to the stream could be nice, but all the wadm processes would still fire
    // off that tick, resulting in multiple people handling. We could maybe get it to work with the
    // right duplicate window, but we have no idea when each process could fire a tick. Worst case
    // scenario right now is that multiple fire simultaneously and a few of them just delete nothing
    let reaper = Reaper::new(
        state_storage.clone(),
        Duration::from_secs(config.cleanup_interval / 2),
        [],
    );

    let wadm_event_prefix = DEFAULT_WADM_EVENTS_TOPIC.trim_matches(trimmer);

    debug!("Creating lattice observer");

    let observer = observer::Observer {
        parser: LatticeIdParser::new("wasmbus", config.multitenant),
        command_manager: commands_manager,
        event_manager: events_manager,
        reaper,
        client: client.clone(),
        command_worker_creator,
        event_worker_creator,
    };

    debug!("Subscribing to API topic");

    let server = Server::new(
        manifest_storage,
        client,
        Some(&config.api_prefix),
        config.multitenant,
        status_stream,
        ManifestNotifier::new(wadm_event_prefix, context),
    )
    .await?;

    let mut tasks = JoinSet::new();

    #[cfg(feature = "http_admin")]
    if let Some(addr) = config.http_admin {
        debug!("Setting up HTTP administration endpoint");
        let socket = TcpListener::bind(addr)
            .await
            .context("failed to bind on HTTP administation endpoint")?;
        let svc = hyper::service::service_fn(move |req| {
            const OK: &str = r#"{"status":"ok"}"#;
            async move {
                let (http::request::Parts { method, uri, .. }, _) = req.into_parts();
                match (method.as_str(), uri.path()) {
                    ("HEAD", "/livez") => Ok(http::Response::default()),
                    ("GET", "/livez") => Ok(http::Response::new(http_body_util::Full::new(
                        Bytes::from(OK),
                    ))),
                    (method, "/livez") => http::Response::builder()
                        .status(http::StatusCode::METHOD_NOT_ALLOWED)
                        .body(http_body_util::Full::new(Bytes::from(format!(
                            "method `{method}` not supported for path `/livez`"
                        )))),
                    ("HEAD", "/readyz") => Ok(http::Response::default()),
                    ("GET", "/readyz") => Ok(http::Response::new(http_body_util::Full::new(
                        Bytes::from(OK),
                    ))),
                    (method, "/readyz") => http::Response::builder()
                        .status(http::StatusCode::METHOD_NOT_ALLOWED)
                        .body(http_body_util::Full::new(Bytes::from(format!(
                            "method `{method}` not supported for path `/readyz`"
                        )))),
                    (.., path) => http::Response::builder()
                        .status(http::StatusCode::NOT_FOUND)
                        .body(http_body_util::Full::new(Bytes::from(format!(
                            "unknown endpoint `{path}`"
                        )))),
                }
            }
        });
        let srv = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
        tasks.spawn(async move {
            loop {
                let stream = match socket.accept().await {
                    Ok((stream, _)) => stream,
                    Err(err) => {
                        tracing::error!(?err, "failed to accept HTTP administration connection");
                        continue;
                    }
                };
                let svc = svc.clone();
                if let Err(err) = srv.serve_connection(TokioIo::new(stream), svc).await {
                    tracing::error!(?err, "failed to serve HTTP administration connection");
                }
            }
        });
    }

    // Subscribe and handle API requests
    tasks.spawn(server.serve());
    // Observe and handle events
    tasks.spawn(observer.observe(wasmbus_event_subjects));

    Ok(tasks)
}

#[derive(Clone)]
struct CommandWorkerCreator {
    pool: ControlClientConstructor,
}

#[async_trait::async_trait]
impl WorkerCreator for CommandWorkerCreator {
    type Output = CommandWorker;

    async fn create(
        &self,
        lattice_id: &str,
        multitenant_prefix: Option<&str>,
    ) -> anyhow::Result<Self::Output> {
        let client = self.pool.get_connection(lattice_id, multitenant_prefix);

        Ok(CommandWorker::new(client))
    }
}

#[derive(Clone)]
struct EventWorkerCreator<StateStore> {
    state_store: StateStore,
    manifest_store: async_nats::jetstream::kv::Store,
    pool: ControlClientConstructor,
    command_topic_prefix: String,
    publisher: Context,
    notify_stream: Stream,
    status_stream: Stream,
}

#[async_trait::async_trait]
impl<StateStore> WorkerCreator for EventWorkerCreator<StateStore>
where
    StateStore: crate::storage::Store + Send + Sync + Clone + 'static,
{
    type Output = EventWorker<StateStore, wasmcloud_control_interface::Client, Context>;

    async fn create(
        &self,
        lattice_id: &str,
        multitenant_prefix: Option<&str>,
    ) -> anyhow::Result<Self::Output> {
        let client = self.pool.get_connection(lattice_id, multitenant_prefix);
        let command_publisher = CommandPublisher::new(
            self.publisher.clone(),
            &format!("{}.{lattice_id}", self.command_topic_prefix),
        );
        let status_publisher = StatusPublisher::new(
            self.publisher.clone(),
            Some(self.status_stream.clone()),
            &format!("wadm.status.{lattice_id}"),
        );
        let manager = ScalerManager::new(
            self.publisher.clone(),
            self.notify_stream.clone(),
            lattice_id,
            multitenant_prefix,
            self.state_store.clone(),
            self.manifest_store.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            client.clone(),
        )
        .await?;
        Ok(EventWorker::new(
            self.state_store.clone(),
            client,
            command_publisher,
            status_publisher,
            manager,
        ))
    }
}
