use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{stream::Stream, Context};
use clap::Parser;
use tokio::sync::Semaphore;
use tracing::log::debug;
use wadm_types::api::DEFAULT_WADM_TOPIC_PREFIX;

use wadm::{
    consumers::{
        manager::{ConsumerManager, WorkerCreator},
        *,
    },
    nats_utils::LatticeIdParser,
    scaler::manager::{ScalerManager, WADM_NOTIFY_PREFIX},
    server::{ManifestNotifier, Server},
    storage::{nats_kv::NatsKvStore, reaper::Reaper},
    workers::{CommandPublisher, CommandWorker, EventWorker, StatusPublisher},
    DEFAULT_COMMANDS_TOPIC, DEFAULT_EVENTS_TOPIC, DEFAULT_MULTITENANT_EVENTS_TOPIC,
    DEFAULT_STATUS_TOPIC, DEFAULT_WADM_EVENTS_TOPIC, DEFAULT_WADM_EVENT_CONSUMER_TOPIC,
};

mod connections;
mod logging;
mod nats;
mod observer;

use connections::ControlClientConstructor;

const WADM_EVENT_STREAM_NAME: &str = "wadm_events";
const WADM_EVENT_CONSUMER_STREAM_NAME: &str = "wadm_event_consumer";
const COMMAND_STREAM_NAME: &str = "wadm_commands";
const STATUS_STREAM_NAME: &str = "wadm_status";
const NOTIFY_STREAM_NAME: &str = "wadm_notify";
const WASMBUS_EVENT_STREAM_NAME: &str = "wasmbus_events";

#[derive(Parser, Debug)]
#[command(name = clap::crate_name!(), version = clap::crate_version!(), about = "wasmCloud Application Deployment Manager", long_about = None)]
struct Args {
    /// The ID for this wadm process. Defaults to a random UUIDv4 if none is provided. This is used
    /// to help with debugging when identifying which process is doing the work
    #[arg(short = 'i', long = "host-id", env = "WADM_HOST_ID")]
    host_id: Option<String>,

    /// Whether or not to use structured log output (as JSON)
    #[arg(
        short = 'l',
        long = "structured-logging",
        default_value = "false",
        env = "WADM_STRUCTURED_LOGGING"
    )]
    structured_logging: bool,

    /// Whether or not to enable opentelemetry tracing
    #[arg(
        short = 't',
        long = "tracing",
        default_value = "false",
        env = "WADM_TRACING_ENABLED"
    )]
    tracing_enabled: bool,

    /// The endpoint to use for tracing. Setting this flag enables tracing, even if --tracing is set
    /// to false. Defaults to http://localhost:4318/v1/traces if not set and tracing is enabled
    #[arg(short = 'e', long = "tracing-endpoint", env = "WADM_TRACING_ENDPOINT")]
    tracing_endpoint: Option<String>,

    /// The NATS JetStream domain to connect to
    #[arg(short = 'd', env = "WADM_JETSTREAM_DOMAIN")]
    domain: Option<String>,

    /// (Advanced) Tweak the maximum number of jobs to run for handling events and commands. Be
    /// careful how you use this as it can affect performance
    #[arg(short = 'j', long = "max-jobs", env = "WADM_MAX_JOBS")]
    max_jobs: Option<usize>,

    /// The URL of the nats server you want to connect to
    #[arg(
        short = 's',
        long = "nats-server",
        env = "WADM_NATS_SERVER",
        default_value = "127.0.0.1:4222"
    )]
    nats_server: String,

    /// Use the specified nkey file or seed literal for authentication. Must be used in conjunction with --nats-jwt
    #[arg(
        long = "nats-seed",
        env = "WADM_NATS_NKEY",
        conflicts_with = "nats_creds",
        requires = "nats_jwt"
    )]
    nats_seed: Option<String>,

    /// Use the specified jwt file or literal for authentication. Must be used in conjunction with --nats-nkey
    #[arg(
        long = "nats-jwt",
        env = "WADM_NATS_JWT",
        conflicts_with = "nats_creds",
        requires = "nats_seed"
    )]
    nats_jwt: Option<String>,

    /// (Optional) NATS credential file to use when authenticating
    #[arg(
        long = "nats-creds-file",
        env = "WADM_NATS_CREDS_FILE",
        conflicts_with_all = ["nats_seed", "nats_jwt"],
    )]
    nats_creds: Option<PathBuf>,

    /// (Optional) NATS TLS certificate file to use when authenticating
    #[arg(long = "nats-tls-ca-file", env = "WADM_NATS_TLS_CA_FILE")]
    nats_tls_ca_file: Option<PathBuf>,

    /// Name of the bucket used for storage of lattice state
    #[arg(
        long = "state-bucket-name",
        env = "WADM_STATE_BUCKET_NAME",
        default_value = "wadm_state"
    )]
    state_bucket: String,

    /// The amount of time in seconds to give for hosts to fail to heartbeat and be removed from the
    /// store. By default, this is 70s because it is 2x the host heartbeat interval plus a little padding
    #[arg(
        long = "cleanup-interval",
        env = "WADM_CLEANUP_INTERVAL",
        default_value = "70"
    )]
    cleanup_interval: u64,

    /// The API topic prefix to use. This is an advanced setting that should only be used if you
    /// know what you are doing
    #[arg(
        long = "api-prefix",
        env = "WADM_API_PREFIX",
        default_value = DEFAULT_WADM_TOPIC_PREFIX
    )]
    api_prefix: String,

    /// This prefix to used for the internal streams. When running in a multitenant environment,
    /// clients share the same JS domain (since messages need to come from lattices).
    /// Setting a stream prefix makes it possible to have a separate stream for different wadms running in a multitenant environment.
    /// This is an advanced setting that should only be used if you know what you are doing.
    #[arg(long = "stream-prefix", env = "WADM_STREAM_PREFIX")]
    stream_prefix: Option<String>,

    /// Name of the bucket used for storage of manifests
    #[arg(
        long = "manifest-bucket-name",
        env = "WADM_MANIFEST_BUCKET_NAME",
        default_value = "wadm_manifests"
    )]
    manifest_bucket: String,

    /// Run wadm in multitenant mode. This is for advanced multitenant use cases with segmented NATS
    /// account traffic and not simple cases where all lattices use credentials from the same
    /// account. See the deployment guide for more information
    #[arg(long = "multitenant", env = "WADM_MULTITENANT", hide = true)]
    multitenant: bool,

    //
    // Max bytes configuration for streams. Primarily configurable to enable deployment on NATS infra
    // with limited resources.
    //
    /// Maximum bytes to keep for the state bucket
    #[arg(
        long = "state-bucket-max-bytes",
        env = "WADM_STATE_BUCKET_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_state_bucket_bytes: i64,
    /// Maximum bytes to keep for the manifest bucket
    #[arg(
        long = "manifest-bucket-max-bytes",
        env = "WADM_MANIFEST_BUCKET_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_manifest_bucket_bytes: i64,
    /// Maximum bytes to keep for the command stream
    #[arg(
        long = "command-stream-max-bytes",
        env = "WADM_COMMAND_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_command_stream_bytes: i64,
    /// Maximum bytes to keep for the event stream
    #[arg(
        long = "event-stream-max-bytes",
        env = "WADM_EVENT_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_event_stream_bytes: i64,
    /// Maximum bytes to keep for the event consumer stream
    #[arg(
        long = "event-consumer-stream-max-bytes",
        env = "WADM_EVENT_CONSUMER_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_event_consumer_stream_bytes: i64,
    /// Maximum bytes to keep for the status stream
    #[arg(
        long = "status-stream-max-bytes",
        env = "WADM_STATUS_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_status_stream_bytes: i64,
    /// Maximum bytes to keep for the notify stream
    #[arg(
        long = "notify-stream-max-bytes",
        env = "WADM_NOTIFY_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_notify_stream_bytes: i64,
    /// Maximum bytes to keep for the wasmbus event stream
    #[arg(
        long = "wasmbus-event-stream-max-bytes",
        env = "WADM_WASMBUS_EVENT_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    )]
    max_wasmbus_event_stream_bytes: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    logging::configure_tracing(
        args.structured_logging,
        args.tracing_enabled,
        args.tracing_endpoint,
    );

    // Build storage adapter for lattice state (on by default)
    let (client, context) = nats::get_client_and_context(
        args.nats_server.clone(),
        args.domain.clone(),
        args.nats_seed.clone(),
        args.nats_jwt.clone(),
        args.nats_creds.clone(),
        args.nats_tls_ca_file.clone(),
    )
    .await?;

    // TODO: We will probably need to set up all the flags (like lattice prefix and topic prefix) down the line
    let connection_pool = ControlClientConstructor::new(client.clone(), None);

    let trimmer: &[_] = &['.', '>', '*'];

    let store =
        nats::ensure_kv_bucket(&context, args.state_bucket, 1, args.max_state_bucket_bytes).await?;

    let state_storage = NatsKvStore::new(store);

    let manifest_storage = nats::ensure_kv_bucket(
        &context,
        args.manifest_bucket,
        1,
        args.max_manifest_bucket_bytes,
    )
    .await?;

    let internal_stream_name = |stream_name: &str| -> String {
        match args.stream_prefix.clone() {
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
        internal_stream_name(WADM_EVENT_STREAM_NAME),
        vec![DEFAULT_WADM_EVENTS_TOPIC.to_owned()],
        Some(
            "A stream that stores all events coming in on the wadm.evt subject in a cluster"
                .to_string(),
        ),
        args.max_event_stream_bytes,
    )
    .await?;

    debug!("Ensuring command stream");

    let command_stream = nats::ensure_stream(
        &context,
        internal_stream_name(COMMAND_STREAM_NAME),
        vec![DEFAULT_COMMANDS_TOPIC.to_owned()],
        Some("A stream that stores all commands for wadm".to_string()),
        args.max_command_stream_bytes,
    )
    .await?;

    let status_stream = nats::ensure_status_stream(
        &context,
        internal_stream_name(STATUS_STREAM_NAME),
        vec![DEFAULT_STATUS_TOPIC.to_owned()],
        args.max_status_stream_bytes,
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

    let wasmbus_event_subjects = match args.multitenant {
        true => vec![DEFAULT_MULTITENANT_EVENTS_TOPIC.to_owned()],
        false => vec![DEFAULT_EVENTS_TOPIC.to_owned()],
    };

    let wasmbus_event_stream = nats::ensure_limits_stream(
        &context,
        WASMBUS_EVENT_STREAM_NAME.to_string(),
        wasmbus_event_subjects.clone(),
        Some(
            "A stream that stores all events coming in on the wasmbus.evt subject in a cluster"
                .to_string(),
        ),
        args.max_wasmbus_event_stream_bytes,
    )
    .await?;

    debug!("Ensuring notify stream");

    let notify_stream = nats::ensure_notify_stream(
        &context,
        NOTIFY_STREAM_NAME.to_owned(),
        vec![format!("{WADM_NOTIFY_PREFIX}.*")],
        args.max_notify_stream_bytes,
    )
    .await?;

    debug!("Ensuring event consumer stream");

    let event_consumer_stream = nats::ensure_event_consumer_stream(
        &context,
        WADM_EVENT_CONSUMER_STREAM_NAME.to_owned(),
        DEFAULT_WADM_EVENT_CONSUMER_TOPIC.to_owned(),
        vec![&wasmbus_event_stream, &event_stream],
        Some(
            "A stream that sources from wadm_events and wasmbus_events for wadm event consumer's use"
                .to_string(),
        ),
        args.max_event_consumer_stream_bytes,
    )
    .await?;

    debug!("Creating event consumer manager");

    let permit_pool = Arc::new(Semaphore::new(
        args.max_jobs.unwrap_or(Semaphore::MAX_PERMITS),
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
        args.multitenant,
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
        args.multitenant,
    )
    .await;

    // TODO(thomastaylor312): We might want to figure out how not to run this globally. Doing a
    // synthetic event sent to the stream could be nice, but all the wadm processes would still fire
    // off that tick, resulting in multiple people handling. We could maybe get it to work with the
    // right duplicate window, but we have no idea when each process could fire a tick. Worst case
    // scenario right now is that multiple fire simultaneously and a few of them just delete nothing
    let reaper = Reaper::new(
        state_storage.clone(),
        Duration::from_secs(args.cleanup_interval / 2),
        [],
    );

    let wadm_event_prefix = DEFAULT_WADM_EVENTS_TOPIC.trim_matches(trimmer);

    debug!("Creating lattice observer");

    let observer = observer::Observer {
        parser: LatticeIdParser::new("wasmbus", args.multitenant),
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
        Some(&args.api_prefix),
        args.multitenant,
        status_stream,
        ManifestNotifier::new(wadm_event_prefix, context),
    )
    .await?;
    tokio::select! {
        res = server.serve() => {
            res?
        }
        res = observer.observe(wasmbus_event_subjects) => {
            res?
        }
        _ = tokio::signal::ctrl_c() => {}
    }
    Ok(())
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
    StateStore: wadm::storage::Store + Send + Sync + Clone + 'static,
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
