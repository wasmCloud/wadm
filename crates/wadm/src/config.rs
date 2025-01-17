use std::path::PathBuf;

use wadm_types::api::DEFAULT_WADM_TOPIC_PREFIX;

use crate::nats::StreamPersistence;

#[derive(Clone, Debug)]
pub struct WadmConfig {
    /// The ID for this wadm process. Defaults to a random UUIDv4 if none is provided. This is used
    /// to help with debugging when identifying which process is doing the work
    pub host_id: Option<String>,

    /// The NATS JetStream domain to connect to
    pub domain: Option<String>,

    /// (Advanced) Tweak the maximum number of jobs to run for handling events and commands. Be
    /// careful how you use this as it can affect performance
    pub max_jobs: Option<usize>,

    /// The URL of the nats server you want to connect to
    pub nats_server: String,

    /// Use the specified nkey file or seed literal for authentication. Must be used in conjunction with --nats-jwt
    pub nats_seed: Option<String>,

    /// Use the specified jwt file or literal for authentication. Must be used in conjunction with --nats-nkey
    pub nats_jwt: Option<String>,

    /// (Optional) NATS credential file to use when authenticating
    pub nats_creds: Option<PathBuf>,

    /// (Optional) NATS TLS certificate file to use when authenticating
    pub nats_tls_ca_file: Option<PathBuf>,

    /// Name of the bucket used for storage of lattice state
    pub state_bucket: String,

    /// The amount of time in seconds to give for hosts to fail to heartbeat and be removed from the
    /// store. By default, this is 70s because it is 2x the host heartbeat interval plus a little padding
    pub cleanup_interval: u64,

    /// The API topic prefix to use. This is an advanced setting that should only be used if you
    /// know what you are doing
    pub api_prefix: String,

    /// This prefix to used for the internal streams. When running in a multitenant environment,
    /// clients share the same JS domain (since messages need to come from lattices).
    /// Setting a stream prefix makes it possible to have a separate stream for different wadms running in a multitenant environment.
    /// This is an advanced setting that should only be used if you know what you are doing.
    pub stream_prefix: Option<String>,

    /// Name of the bucket used for storage of manifests
    pub manifest_bucket: String,

    /// Run wadm in multitenant mode. This is for advanced multitenant use cases with segmented NATS
    /// account traffic and not simple cases where all lattices use credentials from the same
    /// account. See the deployment guide for more information
    pub multitenant: bool,

    //
    // Max bytes configuration for streams. Primarily configurable to enable deployment on NATS infra
    // with limited resources.
    //
    /// Maximum bytes to keep for the state bucket
    pub max_state_bucket_bytes: i64,
    /// Maximum bytes to keep for the manifest bucket
    pub max_manifest_bucket_bytes: i64,
    /// Nats streams storage type
    pub stream_persistence: StreamPersistence,
    /// Maximum bytes to keep for the command stream
    pub max_command_stream_bytes: i64,
    /// Maximum bytes to keep for the event stream
    pub max_event_stream_bytes: i64,
    /// Maximum bytes to keep for the event consumer stream
    pub max_event_consumer_stream_bytes: i64,
    /// Maximum bytes to keep for the status stream
    pub max_status_stream_bytes: i64,
    /// Maximum bytes to keep for the notify stream
    pub max_notify_stream_bytes: i64,
    /// Maximum bytes to keep for the wasmbus event stream
    pub max_wasmbus_event_stream_bytes: i64,
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
        }
    }
}
