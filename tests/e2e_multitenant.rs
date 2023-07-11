#![cfg(feature = "_e2e_tests")]
use std::path::PathBuf;

use wadm::server::{DeployResult, PutResult};

mod e2e;
mod helpers;

use e2e::{assert_status, check_actors, check_providers, ClientInfo, ExpectedCount};
use helpers::{ECHO_ACTOR_ID, HTTP_SERVER_PROVIDER_ID};

const MANIFESTS_PATH: &str = "test/data";
const DOCKER_COMPOSE_FILE: &str = "test/docker-compose-e2e-multitenant.yaml";

const MESSAGE_PUB_ACTOR_ID: &str = "MC3QONHYH3FY4KYFCOSVJWIDJG4WA2PVD6FHKR7FFT457GVUTZJYR2TJ";
const NATS_PROVIDER_ID: &str = "VADNMSIML2XGO2X4TPIONTIC55R2UUQGPPDZPAVSC2QD7E76CR77SPW7";
const LATTICE_EAST: &str = "wasmcloud-east";
const LATTICE_WEST: &str = "wasmcloud-west";

#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_multitenant_tests() {
    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);

    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    // Enable multitenancy
    std::env::set_var("WADM_MULTITENANT", "true");
    let mut client_info = ClientInfo::new(manifest_dir, compose_file).await;
    client_info
        .add_ctl_client(LATTICE_EAST, Some("Axxx.wasmbus.ctl"))
        .await;
    client_info
        .add_ctl_client(LATTICE_WEST, Some("Ayyy.wasmbus.ctl"))
        .await;
    client_info.launch_wadm().await;

    // NOTE(thomastaylor312): A nice to have here, but what I didn't want to figure out now, would
    // be to catch the panics from tests and label the backtrace with the appropriate information
    // about which test failed. Another issue is that only the first panic will be returned, so
    // capturing the backtraces and then printing them nicely would probably be good

    // We run this test first by itself because it is a basic test that wadm only spins up the exact
    // number of resources requested. If we were to run it in parallel, some of the shared resources
    // will be created with other tests (namely providers) and this test will fail
    test_basic_separation(&client_info)
        .await
        .expect("basic multitenant separation to work");
    assert!(true)
}

async fn test_basic_separation(client_info: &ClientInfo) -> anyhow::Result<()> {
    let resp = client_info
        .put_manifest_from_file("simple.yaml", Some(LATTICE_EAST))
        .await;
    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info
        .put_manifest_from_file("simple2.yaml", Some(LATTICE_WEST))
        .await;
    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info
        .deploy_manifest("echo-simple", Some(LATTICE_EAST), None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    let resp = client_info
        .deploy_manifest("messaging-simple", Some(LATTICE_WEST), None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    // Ensure echo deployed in east and messaging deployed in west
    assert_status(None, Some(7), || async {
        let east_inventory = client_info.get_all_inventory(LATTICE_EAST).await?;
        let west_inventory = client_info.get_all_inventory(LATTICE_WEST).await?;

        // Check for echo actor and httpserver in east, as well as the link between them
        check_actors(
            &east_inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            4,
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(1),
        )?;
        let links = client_info
            .ctl_client(LATTICE_EAST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if !links.links.iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should exist: {:#?}",
                links
            )
        }

        // Check for messaging actor, httpserver and messaging in west, as well as the links between them
        check_actors(
            &west_inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "messaging-simple",
            1,
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/httpserver:0.18.2",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/nats_messaging:0.17.2",
            ExpectedCount::Exactly(1),
        )?;
        let links = client_info
            .ctl_client(LATTICE_WEST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if !links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should exist: {:#?}",
                links
            )
        }
        if !links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == NATS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:messaging"
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should exist: {:#?}",
                links
            )
        }

        // Check to ensure that no resources from west are running in east and vice versa
        check_actors(
            &west_inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            0,
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(0),
        )?;
        let links = client_info
            .ctl_client(LATTICE_WEST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if links.links.iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should not exist: {:#?}",
                links
            )
        }
        check_actors(
            &east_inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "messaging-simple",
            0,
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/httpserver:0.18.2",
            ExpectedCount::Exactly(0),
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/nats_messaging:0.17.2",
            ExpectedCount::Exactly(0),
        )?;
        let links = client_info
            .ctl_client(LATTICE_EAST)
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between messagepub actor and http provider should not exist: {:#?}",
                links
            )
        }
        if links.links.iter().any(|ld| {
            ld.actor_id == MESSAGE_PUB_ACTOR_ID
                && ld.provider_id == NATS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:messaging"
        }) {
            anyhow::bail!(
                "Link between messagepub actor and http provider should not exist: {:#?}",
                links
            )
        }

        Ok(())
    })
    .await;

    // Undeploy manifests
    let resp = client_info
        .undeploy_manifest("echo-simple", Some(LATTICE_EAST))
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when undeploying manifest: {resp:?}"
    );

    let resp = client_info
        .undeploy_manifest("messaging-simple", Some(LATTICE_WEST))
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when undeploying manifest: {resp:?}"
    );

    // assert that no actors or providers with annotations exist
    assert_status(None, None, || async {
        let east_inventory = client_info.get_all_inventory(LATTICE_EAST).await?;
        let west_inventory = client_info.get_all_inventory(LATTICE_WEST).await?;

        check_actors(
            &east_inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            0,
        )?;
        check_providers(
            &east_inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(0),
        )?;

        check_actors(
            &west_inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "messaging-simple",
            0,
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/httpserver:0.18.2",
            ExpectedCount::Exactly(0),
        )?;
        check_providers(
            &west_inventory,
            "wasmcloud.azurecr.io/nats_messaging:0.17.2",
            ExpectedCount::Exactly(0),
        )?;

        Ok(())
    })
    .await;

    Ok(())
}
