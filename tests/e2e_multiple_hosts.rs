#![cfg(feature = "_e2e_tests")]
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};

use anyhow::Context as _;
use futures::{FutureExt, StreamExt};
use wadm_types::api::StatusType;

mod e2e;
mod helpers;

use e2e::{
    assert_status, check_components, check_providers, ClientInfo, ExpectedCount, DEFAULT_LATTICE_ID,
};
use helpers::{HELLO_COMPONENT_ID, HTTP_SERVER_COMPONENT_ID};

use crate::{
    e2e::check_status,
    helpers::{HELLO_IMAGE_REF, HTTP_SERVER_IMAGE_REF},
};

const MANIFESTS_PATH: &str = "tests/fixtures/manifests";
const DOCKER_COMPOSE_FILE: &str = "tests/docker-compose-e2e_multiple_hosts.yaml";
const BLOBSTORE_FS_IMAGE_REF: &str = "ghcr.io/wasmcloud/blobstore-fs:0.6.0";
const BLOBSTORE_FS_PROVIDER_ID: &str = "fileserver";
const BLOBBY_IMAGE_REF: &str = "ghcr.io/wasmcloud/components/blobby-rust:0.4.0";
const BLOBBY_COMPONENT_ID: &str = "littleblobbytables";

// NOTE(thomastaylor312): This exists because we need to have setup happen only once for all tests
// and then we want cleanup to run with `Drop`. I tried doing this with a `OnceCell`, but `static`s
// don't run drop, they only drop the memory (I also think OnceCell does the same thing too). So to
// get around this we have a top level test that runs everything
#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_multiple_host_tests() {
    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);
    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    let mut client_info = ClientInfo::new(manifest_dir, compose_file).await;
    client_info.add_ctl_client(DEFAULT_LATTICE_ID, None).await;
    client_info.add_wadm_client(DEFAULT_LATTICE_ID).await;
    client_info.launch_wadm().await;

    // Wait for the first event on the lattice prefix before we start deploying and checking
    // statuses. Wadm can absolutely handle hosts starting before you start the wadm process, but the first event
    // on the lattice will initialize the lattice monitor and for the following test we quickly assert things.
    let mut sub = client_info
        .client
        .subscribe("wasmbus.evt.default.>".to_string())
        .await
        .expect("Should be able to subscribe to default events");
    // Host heartbeats happen every 30 seconds, if we don't get a heartbeat in 2 minutes, bail.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(120), sub.next())
        .await
        .expect("should have received a host heartbeat event before timeout");

    // Wait for hosts to start
    let mut did_start = false;
    for _ in 0..10 {
        match client_info.ctl_client(DEFAULT_LATTICE_ID).get_hosts().await {
            Ok(hosts) if hosts.len() == 5 => {
                eprintln!("Hosts {}/5 currently available", hosts.len());
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

    // NOTE(thomastaylor312): A nice to have here, but what I didn't want to figure out now, would
    // be to catch the panics from tests and label the backtrace with the appropriate information
    // about which test failed. Another issue is that only the first panic will be returned, so
    // capturing the backtraces and then printing them nicely would probably be good

    // We run this test first by itself because it is a basic test that wadm only spins up the exact
    // number of resources requested. If we were to run it in parallel, some of the shared resources
    // will be created with other tests (namely providers) and this test will fail
    test_no_requirements(&client_info).boxed().await;

    // The futures must be boxed or they're technically different types
    let tests = [
        test_spread_all_hosts(&client_info).boxed(),
        test_lotta_components(&client_info).boxed(),
        test_complex_app(&client_info).boxed(),
    ];
    futures::future::join_all(tests).await;

    test_stop_host_rebalance(&client_info).await;
}

// This test does a basic check that all things exist in isolation and should be run first before
// other tests run
async fn test_no_requirements(client_info: &ClientInfo) {
    let stream = client_info.get_status_stream().await;
    stream
        .purge()
        .await
        .expect("shouldn't have errored purging stream");
    let client = client_info.wadm_client(DEFAULT_LATTICE_ID);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("simple.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        // Ensure all configuration is set correctly
        let config = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_config("hello_simple-httpaddr")
            .await
            .map_err(|e| anyhow::anyhow!("should have http provider source config {e}"))?
            .into_data()
            .context("should have http provider source config response")?;
        assert_eq!(
            config,
            HashMap::from_iter(vec![("address".to_string(), "0.0.0.0:8080".to_string())])
        );

        check_components(&inventory, HELLO_IMAGE_REF, "hello-simple", 4)?;
        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::Exactly(1))?;

        let links = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .into_data()
            .context("Should have links")?;

        if !links.iter().any(|ld| {
            ld.source_id() == HTTP_SERVER_COMPONENT_ID
                && ld.target() == HELLO_COMPONENT_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "http"
                && ld.name() == "default"
                && *ld.interfaces() == vec!["incoming-handler"]
        }) {
            anyhow::bail!(
                "Link between http provider and hello component should exist: {:#?}",
                links
            )
        }

        check_status(
            &stream,
            DEFAULT_LATTICE_ID,
            "hello-simple",
            StatusType::Deployed,
        )
        .await
        .unwrap();

        Ok(())
    })
    .await;

    // Undeploy manifest
    client
        .undeploy_manifest("hello-simple")
        .await
        .expect("Shouldn't have errored when undeploying manifest");

    // Once manifest is undeployed, status should be undeployed
    check_status(
        &stream,
        DEFAULT_LATTICE_ID,
        "hello-simple",
        StatusType::Undeployed,
    )
    .await
    .unwrap();

    // assert that no components or providers with annotations exist
    assert_status(None, Some(3), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        check_components(&inventory, HELLO_IMAGE_REF, "hello-simple", 0)?;
        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::Exactly(0))?;

        let links = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .into_data()
            .context("Should have links")?;

        if !links.is_empty() {
            anyhow::bail!(
                "The link between the http provider and hello component should be removed"
            )
        }

        check_status(
            &stream,
            DEFAULT_LATTICE_ID,
            "hello-simple",
            StatusType::Undeployed,
        )
        .await
        .unwrap();

        Ok(())
    })
    .await;
}

async fn test_lotta_components(client_info: &ClientInfo) {
    let client = client_info.wadm_client(DEFAULT_LATTICE_ID);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("lotta_components.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        check_components(&inventory, HELLO_IMAGE_REF, "lotta-components", 9001)?;
        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::AtLeast(1))?;

        Ok(())
    })
    .await;
}

async fn test_spread_all_hosts(client_info: &ClientInfo) {
    let client = client_info.wadm_client(DEFAULT_LATTICE_ID);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("all_hosts.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        check_components(&inventory, HELLO_IMAGE_REF, "hello-all-hosts", 5)?;
        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::AtLeast(5))?;

        Ok(())
    })
    .await;
}

async fn test_complex_app(client_info: &ClientInfo) {
    let client = client_info.wadm_client(DEFAULT_LATTICE_ID);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("complex.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    // Put configuration that's mentioned in manifest but without properties
    client_info
        .ctl_client(DEFAULT_LATTICE_ID)
        .put_config(
            "blobby-default-configuration-values",
            HashMap::from_iter([("littleblobby".to_string(), "tables".to_string())]),
        )
        .await
        .expect("should be able to put blobby config");

    // Deploy manifest
    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        // Ensure all configuration is set correctly
        let blobby_config = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_config("complex-defaultcode")
            .await
            .map_err(|e| anyhow::anyhow!("should have blobby component config: {e:?}"))?
            .into_data()
            .context("should have blobby component config response")?;
        assert_eq!(
            blobby_config,
            HashMap::from_iter(vec![("http".to_string(), "404".to_string())])
        );
        let blobby_target_config = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_config("complex-rootfs")
            .await
            .map_err(|e| anyhow::anyhow!("should have target link config {e:?}"))?
            .into_data()
            .context("should have target link config response")?;
        assert_eq!(
            blobby_target_config,
            HashMap::from_iter(vec![("root".to_string(), "/tmp".to_string())])
        );
        let http_source_config = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_config("complex-httpaddr")
            .await
            .map_err(|e| anyhow::anyhow!("should have source link config {e:?}"))?
            .into_data()
            .context("should have target link config response")?;
        assert_eq!(
            http_source_config,
            HashMap::from_iter(vec![("address".to_string(), "0.0.0.0:8081".to_string())])
        );
        let fileserver_config = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_config("complex-defaultfs")
            .await
            .map_err(|e| anyhow::anyhow!("should have provider config {e:?}"))?
            .into_data()
            .context("should have provider config response")?;
        assert_eq!(
            fileserver_config,
            HashMap::from_iter(vec![("root".to_string(), "/tmp/blobby".to_string())])
        );

        check_components(&inventory, BLOBBY_IMAGE_REF, "complex", 5)?;
        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::AtLeast(3))?;
        check_providers(
            &inventory,
            BLOBSTORE_FS_IMAGE_REF,
            ExpectedCount::Exactly(1),
        )?;

        let links = client_info
            .ctl_client(DEFAULT_LATTICE_ID)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .into_data()
            .context("Should have links")?;

        if !links.iter().any(|ld| {
            ld.source_id() == HTTP_SERVER_COMPONENT_ID
                && ld.target() == BLOBBY_COMPONENT_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "http"
                && ld.name() == "default"
                && *ld.interfaces() == vec!["incoming-handler"]
        }) {
            anyhow::bail!(
                "Link between blobby component and http provider should exist: {:#?}",
                links
            );
        }

        if !links.iter().any(|ld| {
            ld.source_id() == BLOBBY_COMPONENT_ID
                && ld.target() == BLOBSTORE_FS_PROVIDER_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "blobstore"
                && ld.name() == "default"
                && *ld.interfaces() == vec!["blobstore"]
        }) {
            anyhow::bail!(
                "Link between blobby component and blobstore-fs provider should exist: {:#?}",
                links
            );
        }

        // Make sure nothing is running on things it shouldn't be on
        if inventory.values().any(|inv| {
            inv.labels()
                .get("region")
                .map(|region| region == "us-taylor-west" || region == "us-brooks-east")
                .unwrap_or(false)
                && inv
                    .providers()
                    .iter()
                    .any(|prov| prov.id() == BLOBSTORE_FS_PROVIDER_ID)
        }) {
            anyhow::bail!("Provider should only be running on the moon");
        }
        let moon_inventory = inventory
            .values()
            .find(|inv| {
                inv.labels()
                    .get("region")
                    .map(|region| region == "moon")
                    .unwrap_or(false)
            })
            .unwrap();

        if moon_inventory
            .components()
            .iter()
            .any(|component| component.id() == BLOBBY_COMPONENT_ID)
        {
            anyhow::bail!("Actors shouldn't be running on the moon");
        }

        Ok(())
    })
    .await;
}

// This test should be run after other tests have finished since we are stopping one of the hosts
async fn test_stop_host_rebalance(client_info: &ClientInfo) {
    let client = client_info.wadm_client(DEFAULT_LATTICE_ID);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("host_stop.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    // Make sure everything deploys first
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        check_components(&inventory, HELLO_IMAGE_REF, "host-stop", 5)?;

        Ok(())
    })
    .await;

    // Now get the inventory and figure out which host is running the most components of the spread and
    // stop that one
    let host_to_stop = client_info
        .get_all_inventory(DEFAULT_LATTICE_ID)
        .await
        .expect("Unable to fetch inventory")
        .into_iter()
        .filter(|(_, inv)| {
            inv.labels()
                .get("region")
                .map(|region| region == "us-brooks-east")
                .unwrap_or(false)
        })
        .max_by_key(|(_, inv)| {
            inv.components()
                .iter()
                .find(|component| component.id() == HELLO_COMPONENT_ID)
                .map(|desc| desc.max_instances())
                .unwrap_or(0)
        })
        .map(|(host_id, _)| host_id)
        .unwrap();

    client_info
        .ctl_client(DEFAULT_LATTICE_ID)
        .stop_host(&host_to_stop, None)
        .await
        .expect("Should have stopped host");

    // Just to make sure state has time to update and the host shuts down, wait for a bit before
    // checking
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Now wait for us to get to 5 again
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory(DEFAULT_LATTICE_ID).await?;

        check_components(&inventory, HELLO_IMAGE_REF, "host-stop", 5)?;

        Ok(())
    })
    .await;
}

// NOTE(thomastaylor312): Future tests could include actually making sure the app works as expected
