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
};
use wasmcloud_control_interface::HostInventory;

const LOG_DIR: &str = "test/e2e_log";
const DEFAULT_LATTICE_ID: &str = "default";
// This time only has to be this long until we fix the issue with jittery-ness due to host events.
// This number comes from 35s (max backoff time) + 1s of wiggle room
pub const DEFAULT_WAIT_TIME: Duration = Duration::from_secs(36);
// This is the first try, plus 2 additional tries after waiting
pub const DEFAULT_MAX_TRIES: usize = 3;

/// A wrapper/guard around a fully configured lattice with multiple hosts with helper functions for
/// interacting with wadm. On drop, it will cleanup all resources that it created
pub struct ClientInfo {
    pub client: Client,
    pub ctl_client: wasmcloud_control_interface::Client,
    manifest_dir: PathBuf,
    compose_file: PathBuf,
    commands: Vec<Child>,
}

// NOTE: We are likely to need this to be reusable for future e2e tests so I am trying to future
// proof with some of the functions here
#[allow(unused)]
impl ClientInfo {
    /// Create a new ClientInfo. This will start the docker compose file, start 3 wadm instances,
    /// and then when dropped clean everything up.
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

        let mut commands = Vec::with_capacity(4);

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
            .expect("Unable to spawn wadm binary");
        commands.push(child);

        // Wait for hosts to start
        let client = async_nats::connect("127.0.0.1:4222")
            .await
            .expect("Unable to connect to nats");
        // TODO(thomastaylor312): When we use this elsewhere, we will need to handle having a map of clients to lattice ids
        let ctl_client = wasmcloud_control_interface::ClientBuilder::new(client.clone())
            .lattice_prefix(DEFAULT_LATTICE_ID)
            .build()
            .await
            .expect("Unable to construct ctl client");

        let mut did_start = false;
        for _ in 0..10 {
            match ctl_client.get_hosts().await {
                Ok(hosts) if hosts.len() == 5 => {
                    did_start = true;
                    break;
                }
                Ok(hosts) => {
                    eprintln!(
                        "Waiting for all hosts to be available {}/5 currently available",
                        hosts.len()
                    );
                }
                Err(e) => {
                    eprintln!("Error when fetching hosts: {e}",)
                }
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        if !did_start {
            panic!("Hosts didn't start")
        }

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
            commands.push(child);
        }

        // Let everything start up
        tokio::time::sleep(Duration::from_secs(2)).await;

        ClientInfo {
            client,
            ctl_client,
            manifest_dir: manifest_dir.as_ref().to_owned(),
            compose_file: compose_file.as_ref().to_owned(),
            commands,
        }
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
        lattice_id: Option<&str>,
    ) -> PutModelResponse {
        self.put_manifest_raw(
            serde_yaml::to_string(manifest).unwrap().into_bytes(),
            lattice_id,
        )
        .await
    }

    /// Same as `put_manifest`, but does all the file loading for you
    pub async fn put_manifest_from_file(
        &self,
        file_name: &str,
        lattice_id: Option<&str>,
    ) -> PutModelResponse {
        self.put_manifest_raw(self.load_raw_manifest(file_name).await, lattice_id)
            .await
    }

    async fn put_manifest_raw(&self, data: Vec<u8>, lattice_id: Option<&str>) -> PutModelResponse {
        let subject = format!(
            "wadm.api.{}.model.put",
            lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
        );
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
        lattice_id: Option<&str>,
        version: Option<&str>,
    ) -> DeployModelResponse {
        let subject = format!(
            "wadm.api.{}.model.deploy.{name}",
            lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
        );
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
        lattice_id: Option<&str>,
    ) -> DeployModelResponse {
        let subject = format!(
            "wadm.api.{}.model.undeploy.{name}",
            lattice_id.unwrap_or(DEFAULT_LATTICE_ID)
        );
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
    pub async fn get_all_inventory(&self) -> anyhow::Result<HashMap<String, HostInventory>> {
        let futs = self
            .ctl_client
            .get_hosts()
            .await
            .expect("Should be able to fetch hosts")
            .into_iter()
            .map(|host| (self.ctl_client.clone(), host.id))
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
            .args(["compose", "-f", self.compose_file.to_str().unwrap(), "down"])
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
