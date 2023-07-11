use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::{stream::Stream, Context};
use clap::Parser;
use tokio::sync::Semaphore;
use tracing::log::debug;

use wadm::{
    consumers::{
        manager::{ConsumerManager, WorkerCreator},
        *,
    },
    mirror::Mirror,
    nats_utils::LatticeIdParser,
    scaler::manager::{ScalerManager, WADM_NOTIFY_PREFIX},
    server::{ManifestNotifier, Server, DEFAULT_WADM_TOPIC_PREFIX},
    storage::{nats_kv::NatsKvStore, reaper::Reaper},
    workers::{CommandPublisher, CommandWorker, EventWorker},
    DEFAULT_COMMANDS_TOPIC, DEFAULT_EVENTS_TOPIC, DEFAULT_MULTITENANT_EVENTS_TOPIC,
    DEFAULT_WADM_EVENTS_TOPIC,
};

mod connections;
mod logging;
mod nats;
mod observer;

use connections::{ControlClientConfig, ControlClientConstructor};

const EVENT_STREAM_NAME: &str = "wadm_events";
const COMMAND_STREAM_NAME: &str = "wadm_commands";
const MIRROR_STREAM_NAME: &str = "wadm_mirror";
const NOTIFY_STREAM_NAME: &str = "wadm_notify";

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
    /// to false. Defaults to http://localhost:55681/v1/traces if not set and tracing is enabled
    #[arg(short = 'e', long = "tracing-endpoint", env = "WADM_TRACING_ENDPOINT")]
    tracing_endpoint: Option<String>,

    /// The NATS JetStream domain to connect to
    #[arg(short = 'd', env = "WADM_JETSTREAM_DOMAIN")]
    domain: Option<String>,

    /// (Advanced) Tweak the maximum number of jobs to run for handling events and commands. Be
    /// careful how you use this as it can affect performance
    #[arg(
        short = 'j',
        long = "max-jobs",
        default_value = "256",
        env = "WADM_MAX_JOBS"
    )]
    max_jobs: usize,

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

    /// Name of the bucket used for storage of lattice state
    #[arg(
        long = "state-bucket-name",
        env = "WADM_STATE_BUCKET_NAME",
        default_value = "wadm_state"
    )]
    state_bucket: String,

    /// The amount of time in seconds to give for hosts to fail to heartbeat and be removed from the
    /// store. By default, this is 120s because it is 4x the host heartbeat interval
    #[arg(
        long = "cleanup-interval",
        env = "WADM_CLEANUP_INTERVAL",
        default_value = "120"
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
    )
    .await?;

    // TODO: We will probably need to set up all the flags (like lattice prefix and topic prefix) down the line
    let connection_pool = ControlClientConstructor::new(
        client.clone(),
        ControlClientConfig {
            js_domain: args.domain,
            topic_prefix: None,
        },
    );

    let trimmer: &[_] = &['.', '>', '*'];

    let store = nats::ensure_kv_bucket(&context, args.state_bucket, 1).await?;

    let state_storage = NatsKvStore::new(store);

    let manifest_storage = nats::ensure_kv_bucket(&context, args.manifest_bucket, 1).await?;

    let event_stream = nats::ensure_stream(
        &context,
        EVENT_STREAM_NAME.to_owned(),
        vec![DEFAULT_WADM_EVENTS_TOPIC.to_owned()],
        Some(
            "A stream that stores all events coming in on the wasmbus.evt topics in a cluster"
                .to_string(),
        ),
    )
    .await?;

    let command_stream = nats::ensure_stream(
        &context,
        COMMAND_STREAM_NAME.to_owned(),
        vec![DEFAULT_COMMANDS_TOPIC.to_owned()],
        Some("A stream that stores all commands for wadm".to_string()),
    )
    .await?;

    let event_stream_topics = if args.multitenant {
        debug!("Running in multitenant mode");
        vec![DEFAULT_MULTITENANT_EVENTS_TOPIC.to_owned()]
    } else {
        vec![DEFAULT_EVENTS_TOPIC.to_owned()]
    };

    let mirror_stream = nats::ensure_stream(
        &context,
        MIRROR_STREAM_NAME.to_owned(),
        event_stream_topics.clone(),
        Some("A stream that publishes all events to the same stream".to_string()),
    )
    .await?;

    let notify_stream = nats::ensure_notify_stream(
        &context,
        NOTIFY_STREAM_NAME.to_owned(),
        vec![format!("{WADM_NOTIFY_PREFIX}.*")],
    )
    .await?;

    let permit_pool = Arc::new(Semaphore::new(args.max_jobs));
    let event_worker_creator = EventWorkerCreator {
        state_store: state_storage.clone(),
        manifest_store: manifest_storage.clone(),
        pool: connection_pool.clone(),
        command_topic_prefix: DEFAULT_COMMANDS_TOPIC.trim_matches(trimmer).to_owned(),
        publisher: context.clone(),
        notify_stream,
    };
    let events_manager: ConsumerManager<EventConsumer> = ConsumerManager::new(
        permit_pool.clone(),
        event_stream,
        event_worker_creator.clone(),
    )
    .await;

    let command_worker_creator = CommandWorkerCreator {
        pool: connection_pool,
    };
    let commands_manager: ConsumerManager<CommandConsumer> = ConsumerManager::new(
        permit_pool.clone(),
        command_stream,
        command_worker_creator.clone(),
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

    let observer = observer::Observer {
        parser: LatticeIdParser::new("wasmbus", args.multitenant),
        command_manager: commands_manager,
        event_manager: events_manager,
        mirror: Mirror::new(mirror_stream, wadm_event_prefix),
        reaper,
        client: client.clone(),
        command_worker_creator,
        event_worker_creator,
    };

    let server = Server::new(
        manifest_storage,
        client,
        Some(&args.api_prefix),
        ManifestNotifier::new(wadm_event_prefix, context),
    )
    .await?;
    tokio::select! {
        res = server.serve() => {
            res?
        }
        res = observer.observe(event_stream_topics) => {
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
        self.pool
            .get_connection(lattice_id, multitenant_prefix)
            .await
            .map(CommandWorker::new)
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
        match self
            .pool
            .get_connection(lattice_id, multitenant_prefix)
            .await
        {
            Ok(client) => {
                let publisher = CommandPublisher::new(
                    self.publisher.clone(),
                    &format!("{}.{lattice_id}", self.command_topic_prefix),
                );
                let manager = ScalerManager::new(
                    self.publisher.clone(),
                    self.notify_stream.clone(),
                    lattice_id,
                    self.state_store.clone(),
                    self.manifest_store.clone(),
                    publisher.clone(),
                    client.clone(),
                )
                .await?;
                Ok(EventWorker::new(
                    self.state_store.clone(),
                    client,
                    publisher,
                    manager,
                ))
            }
            Err(e) => Err(e),
        }
    }
}
