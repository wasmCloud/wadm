use core::net::SocketAddr;
use std::path::PathBuf;

#[cfg(feature = "cli")]
use clap::Parser;
use wadm_types::api::DEFAULT_WADM_TOPIC_PREFIX;

use crate::nats::StreamPersistence;

#[derive(Clone, Debug)]
#[cfg_attr(feature = "cli", derive(Parser))]
#[cfg_attr(feature = "cli", command(name = clap::crate_name!(), version = clap::crate_version!(), about = "wasmCloud Application Deployment Manager", long_about = None))]
pub struct WadmConfig {
    /// The ID for this wadm process. Defaults to a random UUIDv4 if none is provided. This is used
    /// to help with debugging when identifying which process is doing the work
    #[cfg_attr(
        feature = "cli",
        arg(short = 'i', long = "host-id", env = "WADM_HOST_ID")
    )]
    pub host_id: Option<String>,

    /// Whether or not to use structured log output (as JSON)
    #[cfg_attr(
        feature = "cli",
        arg(
            short = 'l',
            long = "structured-logging",
            default_value = "false",
            env = "WADM_STRUCTURED_LOGGING"
        )
    )]
    pub structured_logging: bool,

    /// Whether or not to enable opentelemetry tracing
    #[cfg_attr(
        feature = "cli",
        arg(
            short = 't',
            long = "tracing",
            default_value = "false",
            env = "WADM_TRACING_ENABLED"
        )
    )]
    pub tracing_enabled: bool,

    /// The endpoint to use for tracing. Setting this flag enables tracing, even if --tracing is set
    /// to false. Defaults to http://localhost:4318/v1/traces if not set and tracing is enabled
    #[cfg_attr(
        feature = "cli",
        arg(short = 'e', long = "tracing-endpoint", env = "WADM_TRACING_ENDPOINT")
    )]
    pub tracing_endpoint: Option<String>,

    /// The NATS JetStream domain to connect to
    #[cfg_attr(feature = "cli", arg(short = 'd', env = "WADM_JETSTREAM_DOMAIN"))]
    pub domain: Option<String>,

    /// (Advanced) Tweak the maximum number of jobs to run for handling events and commands. Be
    /// careful how you use this as it can affect performance
    #[cfg_attr(
        feature = "cli",
        arg(short = 'j', long = "max-jobs", env = "WADM_MAX_JOBS")
    )]
    pub max_jobs: Option<usize>,

    /// The URL of the nats server you want to connect to
    #[cfg_attr(
        feature = "cli",
        arg(
            short = 's',
            long = "nats-server",
            env = "WADM_NATS_SERVER",
            default_value = "127.0.0.1:4222"
        )
    )]
    pub nats_server: String,

    /// Use the specified nkey file or seed literal for authentication. Must be used in conjunction with --nats-jwt
    #[cfg_attr(
        feature = "cli",
        arg(
            long = "nats-seed",
            env = "WADM_NATS_NKEY",
            conflicts_with = "nats_creds",
            requires = "nats_jwt"
        )
    )]
    pub nats_seed: Option<String>,

    /// Use the specified jwt file or literal for authentication. Must be used in conjunction with --nats-nkey
    #[cfg_attr(
        feature = "cli",
        arg(
            long = "nats-jwt",
            env = "WADM_NATS_JWT",
            conflicts_with = "nats_creds",
            requires = "nats_seed"
        )
    )]
    pub nats_jwt: Option<String>,

    /// (Optional) NATS credential file to use when authenticating
    #[cfg_attr(
        feature = "cli", arg(
        long = "nats-creds-file",
        env = "WADM_NATS_CREDS_FILE",
        conflicts_with_all = ["nats_seed", "nats_jwt"],
    ))]
    pub nats_creds: Option<PathBuf>,

    /// (Optional) NATS TLS certificate file to use when authenticating
    #[cfg_attr(
        feature = "cli",
        arg(long = "nats-tls-ca-file", env = "WADM_NATS_TLS_CA_FILE")
    )]
    pub nats_tls_ca_file: Option<PathBuf>,

    /// Name of the bucket used for storage of lattice state
    #[cfg_attr(
        feature = "cli",
        arg(
            long = "state-bucket-name",
            env = "WADM_STATE_BUCKET_NAME",
            default_value = "wadm_state"
        )
    )]
    pub state_bucket: String,

    /// The amount of time in seconds to give for hosts to fail to heartbeat and be removed from the
    /// store. By default, this is 70s because it is 2x the host heartbeat interval plus a little padding
    #[cfg_attr(
        feature = "cli",
        arg(
            long = "cleanup-interval",
            env = "WADM_CLEANUP_INTERVAL",
            default_value = "70"
        )
    )]
    pub cleanup_interval: u64,

    /// The API topic prefix to use. This is an advanced setting that should only be used if you
    /// know what you are doing
    #[cfg_attr(
        feature = "cli", arg(
        long = "api-prefix",
        env = "WADM_API_PREFIX",
        default_value = DEFAULT_WADM_TOPIC_PREFIX
    ))]
    pub api_prefix: String,

    /// This prefix to used for the internal streams. When running in a multitenant environment,
    /// clients share the same JS domain (since messages need to come from lattices).
    /// Setting a stream prefix makes it possible to have a separate stream for different wadms running in a multitenant environment.
    /// This is an advanced setting that should only be used if you know what you are doing.
    #[cfg_attr(
        feature = "cli",
        arg(long = "stream-prefix", env = "WADM_STREAM_PREFIX")
    )]
    pub stream_prefix: Option<String>,

    /// Name of the bucket used for storage of manifests
    #[cfg_attr(
        feature = "cli",
        arg(
            long = "manifest-bucket-name",
            env = "WADM_MANIFEST_BUCKET_NAME",
            default_value = "wadm_manifests"
        )
    )]
    pub manifest_bucket: String,

    /// Run wadm in multitenant mode. This is for advanced multitenant use cases with segmented NATS
    /// account traffic and not simple cases where all lattices use credentials from the same
    /// account. See the deployment guide for more information
    #[cfg_attr(
        feature = "cli",
        arg(long = "multitenant", env = "WADM_MULTITENANT", hide = true)
    )]
    pub multitenant: bool,

    //
    // Max bytes configuration for streams. Primarily configurable to enable deployment on NATS infra
    // with limited resources.
    //
    /// Maximum bytes to keep for the state bucket
    #[cfg_attr(
        feature = "cli", arg(
        long = "state-bucket-max-bytes",
        env = "WADM_STATE_BUCKET_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_state_bucket_bytes: i64,
    /// Maximum bytes to keep for the manifest bucket
    #[cfg_attr(
        feature = "cli", arg(
        long = "manifest-bucket-max-bytes",
        env = "WADM_MANIFEST_BUCKET_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_manifest_bucket_bytes: i64,
    /// Nats streams storage type
    #[cfg_attr(
        feature = "cli", arg(
        long = "stream-persistence",
        env = "WADM_STREAM_PERSISTENCE",
        default_value_t = StreamPersistence::File
    ))]
    pub stream_persistence: StreamPersistence,
    /// Maximum bytes to keep for the command stream
    #[cfg_attr(
        feature = "cli", arg(
        long = "command-stream-max-bytes",
        env = "WADM_COMMAND_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_command_stream_bytes: i64,
    /// Maximum bytes to keep for the event stream
    #[cfg_attr(
        feature = "cli", arg(
        long = "event-stream-max-bytes",
        env = "WADM_EVENT_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_event_stream_bytes: i64,
    /// Maximum bytes to keep for the event consumer stream
    #[cfg_attr(
        feature = "cli", arg(
        long = "event-consumer-stream-max-bytes",
        env = "WADM_EVENT_CONSUMER_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_event_consumer_stream_bytes: i64,
    /// Maximum bytes to keep for the status stream
    #[cfg_attr(
        feature = "cli", arg(
        long = "status-stream-max-bytes",
        env = "WADM_STATUS_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_status_stream_bytes: i64,
    /// Maximum bytes to keep for the notify stream
    #[cfg_attr(
        feature = "cli", arg(
        long = "notify-stream-max-bytes",
        env = "WADM_NOTIFY_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_notify_stream_bytes: i64,
    /// Maximum bytes to keep for the wasmbus event stream
    #[cfg_attr(
        feature = "cli", arg(
        long = "wasmbus-event-stream-max-bytes",
        env = "WADM_WASMBUS_EVENT_STREAM_MAX_BYTES",
        default_value_t = -1,
        hide = true
    ))]
    pub max_wasmbus_event_stream_bytes: i64,

    #[cfg(feature = "http_admin")]
    #[cfg_attr(feature = "cli", clap(long = "http-admin", env = "WADM_HTTP_ADMIN"))]
    /// HTTP administration endpoint address
    pub http_admin: Option<SocketAddr>,
}

impl Default for WadmConfig {
    fn default() -> Self {
        Self {
            host_id: None,
            domain: None,
            max_jobs: None,
            nats_server: "127.0.0.1:4222".to_string(),
            nats_seed: None,
            nats_jwt: None,
            nats_creds: None,
            nats_tls_ca_file: None,
            state_bucket: "wadm_state".to_string(),
            cleanup_interval: 70,
            api_prefix: DEFAULT_WADM_TOPIC_PREFIX.to_string(),
            stream_prefix: None,
            manifest_bucket: "wadm_manifests".to_string(),
            multitenant: false,
            max_state_bucket_bytes: -1,
            max_manifest_bucket_bytes: -1,
            stream_persistence: StreamPersistence::File,
            max_command_stream_bytes: -1,
            max_event_stream_bytes: -1,
            max_event_consumer_stream_bytes: -1,
            max_status_stream_bytes: -1,
            max_notify_stream_bytes: -1,
            max_wasmbus_event_stream_bytes: -1,
            http_admin: None,
            structured_logging: false,
            tracing_enabled: false,
            tracing_endpoint: None,
        }
    }
}
