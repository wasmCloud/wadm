#![cfg(feature = "_e2e_tests")]
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use async_nats::Client;
use futures::Future;
use tokio::{
    process::{Child, Command},
    time::{interval, sleep},
};
use wadm::{
    model::Manifest,
    server::{DeployModelRequest, DeployModelResponse, PutModelResponse, UndeployModelRequest},
    APP_SPEC_ANNOTATION, MANAGED_BY_ANNOTATION, MANAGED_BY_IDENTIFIER,
};
use wasmcloud_control_interface::HostInventory;

const LOG_DIR: &str = "test/e2e_log";
const DEFAULT_LATTICE_ID: &str = "default";
// Due to download times and the time needed to stabilize, we still need to wait for just a little bit
// This number comes from 30s (max backoff time) + 5s of wiggle room
pub const DEFAULT_WAIT_TIME: Duration = Duration::from_secs(35);
// This is the first try, plus 2 additional tries after waiting
pub const DEFAULT_MAX_TRIES: usize = 3;

/// A wrapper/guard around a fully configured lattice with multiple hosts with helper functions for
/// interacting with wadm. On drop, it will cleanup all resources that it created
pub struct ClientInfo {
    pub client: Client,
    // Map of lattice prefix to control client
    ctl_clients: HashMap<String, wasmcloud_control_interface::Client>,
    manifest_dir: PathBuf,
    compose_file: PathBuf,
    commands: Vec<Child>,
}

// NOTE: We are likely to need this to be reusable for future e2e tests so I am trying to future
// proof with some of the functions here
#[allow(unused)]
impl ClientInfo {
    /// Create a new ClientInfo, which launches docker compose and connects to NATS
    pub async fn new(manifest_dir: impl AsRef<Path>, compose_file: impl AsRef<Path>) -> ClientInfo {
        let status = Command::new("docker")
            .args([
                "compose",
                "-f",
                compose_file.as_ref().to_str().unwrap(),
                "up",
                "-d",
            ])
            .status()
            .await
            .expect("Unable to run docker compose up");
        if !status.success() {
            panic!("Docker compose up didn't exit successfully")
        }

        let repo_root =
            PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
        // Create the logging directory
        let log_dir = repo_root.join(LOG_DIR);
        tokio::fs::create_dir_all(&log_dir)
            .await
            .expect("Unable to create log dir");

        // Start a process for capturing docker logs
        let log_path = log_dir.join("docker-compose");
        let file = tokio::fs::File::create(log_path)
            .await
            .expect("Unable to create log file")
            .into_std()
            .await;
        let child = Command::new("docker")
            .args([
                "compose",
                "-f",
                compose_file.as_ref().to_str().unwrap(),
                "logs",
                "--follow",
            ])
            .stdout(file)
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("Unable to watch docker logs");
        // Connect to NATS
        let client = async_nats::connect("127.0.0.1:4222")
            .await
            .expect("Unable to connect to nats");

        ClientInfo {
            client,
            ctl_clients: HashMap::new(),
            manifest_dir: manifest_dir.as_ref().to_owned(),
            compose_file: compose_file.as_ref().to_owned(),
            commands: Vec::from_iter([child]),
        }
    }

    pub fn ctl_client(&self, lattice_prefix: &str) -> &wasmcloud_control_interface::Client {
        &self
            .ctl_clients
            .get(lattice_prefix)
            .expect("Should have ctl client for specified lattice")
    }

    pub async fn add_ctl_client(&mut self, lattice_prefix: &str, topic_prefix: Option<&str>) {
        let builder = wasmcloud_control_interface::ClientBuilder::new(self.client.clone())
            .lattice_prefix(lattice_prefix);

        let builder = if let Some(topic_prefix) = topic_prefix {
            builder.topic_prefix(topic_prefix)
        } else {
            builder
        };

        self.ctl_clients.insert(
            lattice_prefix.to_string(),
            builder
                .build()
                .await
                .expect("Unable to construct ctl client"),
        );
    }

    pub async fn launch_wadm(&mut self) {
        let repo_root =
            PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
        // Create the logging directory
        let log_dir = repo_root.join(LOG_DIR);
        tokio::fs::create_dir_all(&log_dir)
            .await
            .expect("Unable to create log dir");
        let wadm_binary_path = repo_root.join("target/debug/wadm");
        if !tokio::fs::try_exists(&wadm_binary_path)
            .await
            .unwrap_or(false)
        {
            panic!(
                "Wadm binary doesn't exist at {}",
                wadm_binary_path.display()
            )
        }

        for i in 0..3 {
            let log_path = log_dir.join(format!("wadm-{i}"));
            let file = tokio::fs::File::create(log_path)
                .await
                .expect("Unable to create log file")
                .into_std()
                .await;
            let child = Command::new(&wadm_binary_path)
                .stderr(file)
                .stdout(Stdio::null())
                .kill_on_drop(true)
                .env(
                    "RUST_LOG",
                    "info,wadm=debug,wadm::scaler=trace,wadm::workers::event=trace",
                )
                .spawn()
                .expect("Unable to spawn wadm binary");
            self.commands.push(child);
        }

        // Let everything start up
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    /// Loads a manifest with the given file name. This will look in the configured `manifest_dir`
    /// to find the file
    pub async fn load_manifest(&self, file_name: &str) -> Manifest {
        let raw = self.load_raw_manifest(file_name).await;
        serde_yaml::from_slice(&raw).expect("Unable to parse manifest")
    }

    /// Loads raw manifest bytes with the given file name. This will look in the configured
    /// `manifest_dir` to find the file
    pub async fn load_raw_manifest(&self, file_name: &str) -> Vec<u8> {
        tokio::fs::read(self.manifest_dir.join(file_name))
            .await
            .expect("Unable to load file")
    }

    /// Sends the given manifest to wadm, returning the response object
    pub async fn put_manifest(
        &self,
        manifest: &Manifest,
        account_id: Option<&str>,
        lattice_id: Option<&str>,
    ) -> PutModelResponse {
        self.put_manifest_raw(
            serde_yaml::to_string(manifest).unwrap().into_bytes(),
            account_id,
            lattice_id,
        )
        .await
    }

    /// Same as `put_manifest`, but does all the file loading for you
    pub async fn put_manifest_from_file(
        &self,
        file_name: &str,
        account_id: Option<&str>,
        lattice_id: Option<&str>,
    ) -> PutModelResponse {
        self.put_manifest_raw(
            self.load_raw_manifest(file_name).await,
            account_id,
            lattice_id,
        )
        .await
    }

    async fn put_manifest_raw(
        &self,
        data: Vec<u8>,
        account_id: Option<&str>,
        lattice_id: Option<&str>,
    ) -> PutModelResponse {
        let subject = if let Some(account) = account_id {
            format!(
                "{}.wadm.api.{}.model.put",
                account,
                lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
            )
        } else {
            format!(
                "wadm.api.{}.model.put",
                lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
            )
        };
        let msg = self
            .client
            .request(subject, data.into())
            .await
            .expect("Unable to put manifest");
        serde_json::from_slice(&msg.payload).expect("Unable to decode put model response")
    }

    /// Deploys the manifest to the given version. If no version is set, it will deploy the latest version
    pub async fn deploy_manifest(
        &self,
        name: &str,
        account_id: Option<&str>,
        lattice_id: Option<&str>,
        version: Option<&str>,
    ) -> DeployModelResponse {
        let subject = if let Some(account) = account_id {
            format!(
                "{}.wadm.api.{}.model.deploy.{name}",
                account,
                lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
            )
        } else {
            format!(
                "wadm.api.{}.model.deploy.{name}",
                lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
            )
        };
        let data = serde_json::to_vec(&DeployModelRequest {
            version: version.map(|s| s.to_owned()),
        })
        .unwrap();
        let msg = self
            .client
            .request(subject, data.into())
            .await
            .expect("Unable to deploy manifest");
        serde_json::from_slice(&msg.payload).expect("Unable to decode deploy model response")
    }

    /// Undeploys the manifest in the lattice
    pub async fn undeploy_manifest(
        &self,
        name: &str,
        account_id: Option<&str>,
        lattice_id: Option<&str>,
    ) -> DeployModelResponse {
        let subject = if let Some(account) = account_id {
            format!(
                "{}.wadm.api.{}.model.undeploy.{name}",
                account,
                lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
            )
        } else {
            format!(
                "wadm.api.{}.model.undeploy.{name}",
                lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
            )
        };
        let data = serde_json::to_vec(&UndeployModelRequest {
            non_destructive: false,
        })
        .unwrap();
        let msg = self
            .client
            .request(subject, data.into())
            .await
            .expect("Unable to undeploy manifest");
        serde_json::from_slice(&msg.payload).expect("Unable to decode undeploy model response")
    }

    /********************* HELPER FUNCTIONS *********************/

    /// Returns all host inventories in a hashmap keyed by host ID. This returns a result so it can
    /// be used inside of a `assert_status` without any problems
    pub async fn get_all_inventory(
        &self,
        lattice_prefix: &str,
    ) -> anyhow::Result<HashMap<String, HostInventory>> {
        let futs = self
            .ctl_client(lattice_prefix)
            .get_hosts()
            .await
            .expect("Should be able to fetch hosts")
            .into_iter()
            .map(|host| (self.ctl_client(lattice_prefix).clone(), host.id))
            .map(|(client, host_id)| async move {
                let inventory = client
                    .get_host_inventory(&host_id)
                    .await
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                Ok((host_id, inventory))
            });
        futures::future::join_all(futs).await.into_iter().collect()
    }
}

impl Drop for ClientInfo {
    fn drop(&mut self) {
        self.commands.drain(..).for_each(|mut child| {
            if let Err(e) = child.start_kill() {
                eprintln!("WARN: Error when killing wadm process: {e}")
            }
        });
        match std::process::Command::new("docker")
            .args([
                "compose",
                "-f",
                self.compose_file.to_str().unwrap(),
                "down",
                "--volumes",
            ])
            .output()
        {
            Ok(output) => {
                if !output.status.success() {
                    eprintln!("WARN: Unable to stop docker compose during cleanup. Manual cleanup needed. Stderr: {}", String::from_utf8_lossy(&output.stderr))
                }
            }
            Err(e) => {
                eprintln!("WARN: Unable to stop docker compose during cleanup. Manual cleanup needed: {e}")
            }
        }
    }
}

/// Attempts to assert that the given function returns without an error (indicating success). It
/// will automatically handle retries up to a max stabilization time (max_tries * wait_time + wiggle
/// room). This operates like a normal assert method in that it panics with the returned error at
/// the end of the stabilization period
pub async fn assert_status<F, Fut>(wait_time: Option<Duration>, max_tries: Option<usize>, check: F)
where
    F: Fn() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
{
    let wiggle = Duration::from_secs(2);
    // Wait for wiggle time before running the loop
    sleep(wiggle).await;
    let mut ticker = interval(wait_time.unwrap_or(DEFAULT_WAIT_TIME));
    let mut num_tries = 0usize;
    let mut last_result = Err(anyhow::anyhow!("Unknown Error"));
    let max_tries = max_tries.unwrap_or(DEFAULT_MAX_TRIES);
    while num_tries < max_tries {
        ticker.tick().await;
        last_result = check().await;
        // Break early if we got an Ok
        if last_result.is_ok() {
            break;
        }
        num_tries += 1;
    }

    // If we didn't get an Ok, panic with the last error
    if let Err(e) = last_result {
        panic!("Failed to get ok response from check: {e}");
    }
}

pub fn check_actors(
    inventory: &HashMap<String, HostInventory>,
    image_ref: &str,
    manifest_name: &str,
    expected_count: usize,
) -> anyhow::Result<()> {
    let all_actors = inventory
        .values()
        .flat_map(|inv| &inv.actors)
        .filter_map(|actor| {
            (actor.image_ref.as_deref().unwrap_or_default() == image_ref)
                .then_some(&actor.instances)
        })
        .flatten();
    let actor_count = all_actors
        .filter(|actor| {
            actor
                .annotations
                .as_ref()
                .and_then(|annotations| {
                    annotations
                        .get(APP_SPEC_ANNOTATION)
                        .map(|val| val == manifest_name)
                })
                .unwrap_or(false)
        })
        .count();
    if actor_count != expected_count {
        anyhow::bail!(
            "Should have had {expected_count} actors managed by wadm running, found {actor_count}"
        )
    }
    Ok(())
}

// I could use the Ordering enum here, but I feel like that would be more confusing to follow along
pub enum ExpectedCount {
    #[allow(dead_code)]
    AtLeast(usize),
    Exactly(usize),
}

pub fn check_providers(
    inventory: &HashMap<String, HostInventory>,
    image_ref: &str,
    expected_count: ExpectedCount,
) -> anyhow::Result<()> {
    let provider_count = inventory
        .values()
        .flat_map(|inv| &inv.providers)
        .filter(|provider| {
            // You can only have 1 provider per host and that could be created by any manifest,
            // so we can just check the image ref and that it is managed by wadm
            provider
                .image_ref
                .as_deref()
                .map(|image| image == image_ref)
                .unwrap_or(false)
                && provider
                    .annotations
                    .as_ref()
                    .and_then(|annotations| {
                        annotations
                            .get(MANAGED_BY_ANNOTATION)
                            .map(|val| val == MANAGED_BY_IDENTIFIER)
                    })
                    .unwrap_or(false)
        })
        .count();

    match expected_count {
        ExpectedCount::AtLeast(expected_count) => {
            if provider_count < expected_count {
                anyhow::bail!(
                    "Should have had at least {expected_count} providers managed by wadm running, found {provider_count}"
                )
            }
        }
        ExpectedCount::Exactly(expected_count) => {
            if provider_count != expected_count {
                anyhow::bail!(
                    "Should have had {expected_count} providers managed by wadm running, found {provider_count}"
                )
            }
        }
    }
    Ok(())
}
