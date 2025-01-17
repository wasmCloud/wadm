use std::path::PathBuf;

use anyhow::Context as _;
use clap::Parser;
use wadm::{config::WadmConfig, start_wadm, StreamPersistence};
use wadm_types::api::DEFAULT_WADM_TOPIC_PREFIX;

mod logging;

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
    /// Nats streams storage type
    #[arg(
        long = "stream-persistence",
        env = "WADM_STREAM_PERSISTENCE",
        default_value_t = StreamPersistence::File
    )]
    stream_persistence: StreamPersistence,
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

impl From<Args> for WadmConfig {
    fn from(config: Args) -> WadmConfig {
        WadmConfig {
            host_id: config.host_id,
            domain: config.domain,
            max_jobs: config.max_jobs,
            nats_server: config.nats_server,
            nats_seed: config.nats_seed,
            nats_jwt: config.nats_jwt,
            nats_creds: config.nats_creds,
            nats_tls_ca_file: config.nats_tls_ca_file,
            state_bucket: config.state_bucket,
            cleanup_interval: config.cleanup_interval,
            api_prefix: config.api_prefix,
            stream_prefix: config.stream_prefix,
            manifest_bucket: config.manifest_bucket,
            multitenant: config.multitenant,
            max_state_bucket_bytes: config.max_state_bucket_bytes,
            max_manifest_bucket_bytes: config.max_manifest_bucket_bytes,
            stream_persistence: config.stream_persistence,
            max_command_stream_bytes: config.max_command_stream_bytes,
            max_event_stream_bytes: config.max_event_stream_bytes,
            max_event_consumer_stream_bytes: config.max_event_consumer_stream_bytes,
            max_status_stream_bytes: config.max_status_stream_bytes,
            max_notify_stream_bytes: config.max_notify_stream_bytes,
            max_wasmbus_event_stream_bytes: config.max_wasmbus_event_stream_bytes,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    logging::configure_tracing(
        args.structured_logging,
        args.tracing_enabled,
        args.tracing_endpoint.clone(),
    );

    let mut wadm = start_wadm(args.into())
        .await
        .context("failed to run wadm")?;
    tokio::select! {
        res = wadm.join_next() => {
            match res {
                Some(Ok(_)) => {
                    tracing::info!("WADM has exited successfully");
                    std::process::exit(0);
                }
                Some(Err(e)) => {
                    tracing::error!("WADM has exited with an error: {:?}", e);
                    std::process::exit(1);
                }
                None => {
                    tracing::info!("WADM server did not start");
                    std::process::exit(0);
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("Received Ctrl+C, shutting down");
            std::process::exit(0);
        }
    }
}
