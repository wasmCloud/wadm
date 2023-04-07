#![allow(dead_code)]

use std::net::{Ipv4Addr, SocketAddrV4, TcpListener};

use async_nats::{
    jetstream::{
        self,
        kv::{Config as KvConfig, Store},
    },
    Client,
};
use futures::TryStreamExt;
use tokio::process::Child;
use tokio::process::Command;

use anyhow::Result;
use wadm::consumers::{CommandConsumer, ScopedMessage};

const DEFAULT_WASMCLOUD_PORT: u16 = 4000;
const DEFAULT_NATS_PORT: u16 = 4222;

/// Get a TCP random port
fn get_random_tcp_port() -> u16 {
    TcpListener::bind(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 0))
        .expect("Unable to bind to check for port")
        .local_addr()
        .unwrap()
        .port()
}

#[derive(Debug)]
pub struct CleanupGuard {
    child: Option<Child>,

    already_running: bool,
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if !self.already_running {
            match std::process::Command::new("wash").args(["down"]).output() {
                Ok(o) if !o.status.success() => {
                    eprintln!(
                        "Error stopping wasmcloud host: {}",
                        String::from_utf8_lossy(&o.stderr)
                    )
                }
                Err(e) => eprintln!("Error stopping wasmcloud host: {e}"),
                _ => (),
            }
        }
    }
}

/// Configuration struct for wash instances that are used for testing
#[derive(Debug, Default)]
pub struct TestWashConfig {
    /// Port on which to run wasmCloud
    pub nats_port: Option<u16>,

    /// Only connect to pre-existing NATS instance
    pub nats_connect_only: bool,

    /// Port on which to run wasmCloud (via `wash up`)
    pub wasmcloud_port: Option<u16>,
}

impl TestWashConfig {
    /// Build a test wash configuration with randomized ports
    pub async fn random() -> Result<TestWashConfig> {
        let nats_port = Some(get_random_tcp_port());
        let wasmcloud_port = Some(get_random_tcp_port());

        Ok(TestWashConfig {
            nats_port,
            wasmcloud_port,
            ..TestWashConfig::default()
        })
    }

    /// Get the washboard URL for this config
    pub fn washboard_url(&self) -> String {
        format!(
            "localhost:{}",
            self.wasmcloud_port.unwrap_or(DEFAULT_WASMCLOUD_PORT)
        )
    }

    /// Get the NATS URL for this config
    pub fn nats_url(&self) -> String {
        format!("127.0.0.1:{}", self.nats_port.unwrap_or(DEFAULT_NATS_PORT))
    }
}

/// Start a local wash instance
async fn start_wash_instance(cfg: &TestWashConfig) -> Result<CleanupGuard> {
    let nats_port = cfg.nats_port.unwrap_or(DEFAULT_NATS_PORT).to_string();
    let wasmcloud_port = cfg
        .wasmcloud_port
        .unwrap_or(DEFAULT_WASMCLOUD_PORT)
        .to_string();

    // Build args
    let mut args: Vec<&str> = Vec::from(["up", "-d", "--nats-port", &nats_port]);
    if cfg.nats_connect_only {
        args.push("--nats-connect-only");
    }

    // Build the command
    let mut cmd = Command::new("wash");
    cmd.args(&args)
        .env("WASMCLOUD_PORT", &wasmcloud_port)
        .env("WASMCLOUD_DASHBOARD_PORT", &wasmcloud_port)
        .stderr(std::process::Stdio::null())
        .stdout(std::process::Stdio::null());

    // Build the cleanup guard that will be returned
    let guard = CleanupGuard {
        child: None,
        already_running: false,
    };

    let output = cmd.status().await.expect("Unable to run detached wash up");
    assert!(output.success(), "Error trying to start host",);

    // Make sure we can connect to washboard
    wait_for_server(&cfg.washboard_url()).await;

    // Give the host just a bit more time to get totally ready
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    Ok(guard)
}

/// Set up and run a wash instance that can be used for a test
pub async fn setup_test_wash(cfg: &TestWashConfig) -> CleanupGuard {
    match tokio::net::TcpStream::connect(cfg.washboard_url()).await {
        // NOTE: DO NOT use unwrap_or here. Otherwise we allocate a cleanup guard that then gets
        // dropped, which runs `wash down`
        #[allow(clippy::unnecessary_lazy_evaluations)]
        Err(_) => start_wash_instance(cfg)
            .await
            .unwrap_or_else(|_| CleanupGuard {
                child: None,
                already_running: false,
            }),
        Ok(_) => CleanupGuard {
            child: None,
            already_running: true,
        },
    }
}

pub async fn wait_for_server(url: &str) {
    let mut wait_count = 1;
    loop {
        // Magic number: 10 + 1, since we are starting at 1 for humans
        if wait_count >= 11 {
            panic!("Ran out of retries waiting for host to start");
        }
        match tokio::net::TcpStream::connect(url).await {
            Ok(_) => break,
            Err(e) => {
                eprintln!("Waiting for server {url} to come up, attempt {wait_count}. Will retry in 1 second. Got error {e:?}");
                wait_count += 1;
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    }
}

/// Runs wash with the given args and makes sure it runs successfully. Returns the contents of
/// stdout
pub async fn run_wash_command<I, S>(args: I) -> Vec<u8>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = Command::new("wash")
        .args(args)
        .output()
        .await
        .expect("Unable to run wash command");
    if !output.status.success() {
        panic!(
            "wash command didn't exit successfully: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    output.stdout
}

/// Helper function that sets up a store with the given ID as its name. This ID should be unique per
/// test
pub async fn create_test_store(id: String) -> Store {
    let client = async_nats::connect("127.0.0.1:4222")
        .await
        .expect("Should be able to connect to NATS");

    create_test_store_with_client(client, id).await
}

/// Same as [`create_test_store`], but allows you to pass an existing client instead
pub async fn create_test_store_with_client(client: Client, id: String) -> Store {
    let context = jetstream::new(client);

    // First make sure we clean up the store. We can't just do a cleanup on `Drop` because
    // `tokio::test` uses a single threaded runtime and blocks forever and it isn't really worth
    // spinning up more cores to handle this. We don't care about the result because it could not
    // exist
    let _ = context.delete_key_value(&id).await;

    context
        .create_key_value(KvConfig {
            bucket: id,
            history: 1,
            num_replicas: 1,
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .expect("Unable to create KV bucket")
}

/// A wrapper around a command consumer
pub struct StreamWrapper {
    pub topic: String,
    pub client: async_nats::Client,
    pub stream: CommandConsumer,
}

impl StreamWrapper {
    /// Sets up a new command consumer stream using the given id as the stream name
    pub async fn new(id: String, port: Option<u16>) -> StreamWrapper {
        let client = async_nats::connect(format!("127.0.0.1:{}", port.unwrap_or(4222)))
            .await
            .expect("Unable to setup nats command consumer client");
        let context = async_nats::jetstream::new(client.clone());
        let topic = format!("{id}.cmd.default");
        // If the stream exists, purge it
        let stream = if let Ok(stream) = context.get_stream(&id).await {
            stream
                .purge()
                .await
                .expect("Should be able to purge stream");
            stream
        } else {
            context
            .create_stream(async_nats::jetstream::stream::Config {
                name: id.to_owned(),
                description: Some(
                    "A stream that stores all events coming in on the wasmbus.evt topics in a cluster"
                        .to_string(),
                ),
                num_replicas: 1,
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                subjects: vec![topic.clone()],
                max_age: wadm::DEFAULT_EXPIRY_TIME,
                storage: async_nats::jetstream::stream::StorageType::Memory,
                allow_rollup: false,
                ..Default::default()
            })
            .await
            .expect("Should be able to create test stream")
        };
        let stream = CommandConsumer::new(stream, &topic)
            .await
            .expect("Unable to setup stream");
        StreamWrapper {
            topic,
            client,
            stream,
        }
    }

    /// Publish the given command
    pub async fn publish_command(&self, cmd: impl Into<wadm::commands::Command>) {
        self.client
            .publish(
                self.topic.clone(),
                serde_json::to_vec(&cmd.into()).unwrap().into(),
            )
            .await
            .expect("Unable to send command");
    }

    pub async fn wait_for_command(&mut self) -> ScopedMessage<wadm::commands::Command> {
        tokio::time::timeout(std::time::Duration::from_secs(2), self.stream.try_next())
            .await
            .expect("Should have received event before timeout")
            .expect("Stream shouldn't have had an error")
            .expect("Stream shouldn't have ended")
    }
}
