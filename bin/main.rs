use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::sync::Semaphore;

use wadm::{
    consumers::{
        manager::{ConsumerManager, WorkerCreator},
        *,
    },
    nats_utils::LatticeIdParser,
    server::{Server, DEFAULT_WADM_TOPIC_PREFIX},
    storage::{nats_kv::NatsKvStore, reaper::Reaper},
    workers::{CommandWorker, EventWorker},
    DEFAULT_COMMANDS_TOPIC, DEFAULT_EVENTS_TOPIC, DEFAULT_MULTITENANT_EVENTS_TOPIC,
};

mod connections;
mod logging;
mod nats;
mod observer;

use connections::{ControlClientConfig, ControlClientConstructor};

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

    /// Name of the events stream to use
    #[arg(
        long = "event-stream",
        default_value = "wadm_events",
        env = "WADM_EVENT_STREAM"
    )]
    event_stream_name: String,

    /// Name of the commands stream to use
    #[arg(
        long = "command-stream",
        default_value = "wadm_commands",
        env = "WADM_COMMAND_STREAM"
    )]
    command_stream_name: String,

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

    /// Use the specified jwt file for authentication. Must be used in conjunction with --nats-nkey
    #[arg(
        long = "nats-jwt",
        env = "WADM_NATS_JWT",
        conflicts_with = "nats_creds",
        requires = "nats_seed"
    )]
    nats_jwt: Option<PathBuf>,

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
    #[arg(long = "multitenant", env = "WADM_MULTITENANT")]
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

    let store = nats::ensure_kv_bucket(&context, args.state_bucket, 1).await?;

    let state_storage = NatsKvStore::new(store);

    let store = nats::ensure_kv_bucket(&context, args.manifest_bucket, 1).await?;

    let manifest_storage = NatsKvStore::new(store);

    let event_stream_topics = if args.multitenant {
        vec![
            DEFAULT_EVENTS_TOPIC.to_owned(),
            DEFAULT_MULTITENANT_EVENTS_TOPIC.to_owned(),
        ]
    } else {
        vec![DEFAULT_EVENTS_TOPIC.to_owned()]
    };

    let event_stream = nats::ensure_stream(
        &context,
        args.event_stream_name,
        event_stream_topics.clone(),
        Some(
            "A stream that stores all events coming in on the wasmbus.evt topics in a cluster"
                .to_string(),
        ),
    )
    .await?;

    let command_stream = nats::ensure_stream(
        &context,
        args.command_stream_name,
        vec![DEFAULT_COMMANDS_TOPIC.to_owned()],
        Some("A stream that stores all commands for wadm".to_string()),
    )
    .await?;

    let permit_pool = Arc::new(Semaphore::new(args.max_jobs));
    let events_manager: ConsumerManager<EventConsumer> = ConsumerManager::new(
        permit_pool.clone(),
        event_stream,
        EventWorkerCreator {
            store: state_storage.clone(),
            pool: connection_pool.clone(),
        },
    )
    .await;
    let commands_manager: ConsumerManager<CommandConsumer> = ConsumerManager::new(
        permit_pool.clone(),
        command_stream,
        CommandWorkerCreator {
            pool: connection_pool.clone(),
        },
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

    let observer = observer::Observer {
        client_builder: connection_pool,
        parser: LatticeIdParser::new("wasmbus", args.multitenant),
        command_manager: commands_manager,
        event_manager: events_manager,
        reaper,
        client: client.clone(),
        store: state_storage,
    };

    let server = Server::new(manifest_storage, client, Some(&args.api_prefix)).await?;
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

struct CommandWorkerCreator {
    pool: ControlClientConstructor,
}

#[async_trait::async_trait]
impl WorkerCreator for CommandWorkerCreator {
    type Output = CommandWorker;

    async fn create(&self, lattice_id: &str) -> anyhow::Result<Self::Output> {
        self.pool
            .get_connection(lattice_id)
            .await
            .map(CommandWorker::new)
    }
}

struct EventWorkerCreator<S> {
    store: S,
    pool: ControlClientConstructor,
}

#[async_trait::async_trait]
impl<S> WorkerCreator for EventWorkerCreator<S>
where
    S: wadm::storage::Store + Send + Sync + Clone + 'static,
{
    type Output = EventWorker<S, wasmcloud_control_interface::Client>;

    async fn create(&self, lattice_id: &str) -> anyhow::Result<Self::Output> {
        self.pool
            .get_connection(lattice_id)
            .await
            .map(|client| EventWorker::new(self.store.clone(), client))
    }
}
