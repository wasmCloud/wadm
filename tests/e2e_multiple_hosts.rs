#![cfg(feature = "_e2e_tests")]
use std::collections::HashMap;
use std::path::PathBuf;

use futures::FutureExt;
use wadm::server::{DeployResult, PutResult};
use wadm::{APP_SPEC_ANNOTATION, MANAGED_BY_ANNOTATION, MANAGED_BY_IDENTIFIER};

mod e2e;

use e2e::{assert_status, ClientInfo};
use wasmcloud_control_interface::HostInventory;

const MANIFESTS_PATH: &str = "test/data";
const DOCKER_COMPOSE_FILE: &str = "test/docker-compose-e2e.yaml";

// NOTE(thomastaylor312): This exists because we need to have setup happen only once for all tests
// and then we want cleanup to run with `Drop`. I tried doing this with a `OnceCell`, but `static`s
// don't run drop, they only drop the memory (I also think OnceCell does the same thing too). So to
// get around this we have a top level test that runs everything
#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_all_tests() {
    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);
    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    let client_info = ClientInfo::new(manifest_dir, compose_file).await;

    // NOTE(thomastaylor312): A nice to have here, but what I didn't want to figure out now, would
    // be to catch the panics from tests and label the backtrace with the appropriate information
    // about which test failed. Another issue is that only the first panic will be returned, so
    // capturing the backtraces and then printing them nicely would probably be good

    // We run this test first by itself because it is a basic test that wadm only spins up the exact
    // number of resources requested. If we were to run it in parallel, some of the shared resources
    // will be created with other tests (namely providers) and this test will fail
    test_no_requirements(&client_info).boxed().await;

    // The futures must be boxed or they're technically different types
    let tests = [test_spread_all_hosts(&client_info).boxed()];
    futures::future::join_all(tests).await;
}

async fn test_no_requirements(client_info: &ClientInfo) {
    let resp = client_info
        .put_manifest_from_file("simple.yaml", None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info.deploy_manifest("echo-simple", None, None).await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    assert_status(None, Some(5), || async {
        let inventory = client_info.get_all_inventory().await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            4,
        )?;
        check_providers(&inventory, "wasmcloud.azurecr.io/httpserver:0.17.0", 1)?;

        Ok(())
    })
    .await;

    // Undeploy manifest
    let resp = client_info.undeploy_manifest("echo-simple", None).await;

    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when undeploying manifest: {resp:?}"
    );

    // assert that no actors or providers with annotations exist
    assert_status(None, None, || async {
        let inventory = client_info.get_all_inventory().await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            0,
        )?;
        check_providers(&inventory, "wasmcloud.azurecr.io/httpserver:0.17.0", 0)?;

        Ok(())
    })
    .await;
}

async fn test_spread_all_hosts(client_info: &ClientInfo) {
    let resp = client_info
        .put_manifest_from_file("all_hosts.yaml", None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    // Deploy manifest
    let resp = client_info
        .deploy_manifest("echo-all-hosts", None, None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    assert_status(None, Some(5), || async {
        let inventory = client_info.get_all_inventory().await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-all-hosts",
            5,
        )?;
        check_providers(&inventory, "wasmcloud.azurecr.io/httpserver:0.17.0", 5)?;

        Ok(())
    })
    .await;
}

// NOTE(thomastaylor312): Future tests could include actually making sure the app works as expected and also manually killing hosts and seeing if things recover

fn check_actors(
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

fn check_providers(
    inventory: &HashMap<String, HostInventory>,
    image_ref: &str,
    expected_count: usize,
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

    if provider_count != expected_count {
        anyhow::bail!(
            "Should have had {expected_count} providers managed by wadm running, found {provider_count}"
        )
    }
    Ok(())
}
