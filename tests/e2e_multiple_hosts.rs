#![cfg(feature = "_e2e_tests")]
use std::path::PathBuf;
use std::time::Duration;

use futures::FutureExt;
use wadm::server::{DeployResult, PutResult, StatusType};

mod e2e;
mod helpers;

use e2e::{assert_status, check_actors, check_providers, ClientInfo, ExpectedCount};
use helpers::{ECHO_ACTOR_ID, HTTP_SERVER_PROVIDER_ID};

use crate::e2e::get_manifest_status;

const MANIFESTS_PATH: &str = "test/data";
const DOCKER_COMPOSE_FILE: &str = "test/docker-compose-e2e.yaml";
const BLOBSTORE_FS_PROVIDER_ID: &str = "VBBQNNCGUKIXEWLL5HL5XJE57BS3GU5DMDOKZS6ROEWPQFHEDP6NGVZM";
const BLOBBY_ACTOR_ID: &str = "MBY3COMRDLQYTX2AUTNB5D2WYAH5TUKNIMELDSQ5BUFZVV7CBUUIKEDR";
const KV_COUNTER_ACTOR_ID: &str = "MCFMFDWFHGKELOXPCNCDXKK5OFLHBVEWRAOXR5JSQUD2TOFRE3DFPM7E";

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
    client_info.add_ctl_client("default", None).await;
    client_info.launch_wadm().await;

    // Wait for hosts to start
    let mut did_start = false;
    for _ in 0..10 {
        match client_info.ctl_client("default").get_hosts().await {
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
        // See the comment on the function below
        // test_lotta_actors(&client_info).boxed(),
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
    let resp = client_info
        .put_manifest_from_file("simple.yaml", None, None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info
        .deploy_manifest("echo-simple", None, None, None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // Once manifest is deployed, first status should be compensating
    for _ in 0..5 {
        if let Some(status) = get_manifest_status(&stream, "default", "echo-simple").await {
            assert_eq!(status.status_type, StatusType::Compensating);
            break;
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
    // which can take a bit
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            4,
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(1),
        )?;
        let links = client_info
            .ctl_client("default")
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

        // SAFETY: we already know some status existed when we checked for compensating. If there's no status now, it means
        // we borked our stream and this _should_ fail
        let status = get_manifest_status(&stream, "default", "echo-simple")
            .await
            .unwrap();
        assert_eq!(status.status_type, StatusType::Ready);

        Ok(())
    })
    .await;

    // Undeploy manifest
    let resp = client_info
        .undeploy_manifest("echo-simple", None, None)
        .await;

    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when undeploying manifest: {resp:?}"
    );

    // Once manifest is undeployed, status should be undeployed
    for _ in 0..5 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if let Some(status) = get_manifest_status(&stream, "default", "echo-simple").await {
            assert_eq!(status.status_type, StatusType::Undeployed);
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            break;
        }
    }

    // assert that no actors or providers with annotations exist
    assert_status(None, None, || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-simple",
            0,
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(0),
        )?;

        let status = get_manifest_status(&stream, "default", "echo-simple")
            .await
            .unwrap();

        assert_eq!(status.status_type, StatusType::Undeployed);

        Ok(())
    })
    .await;
}

// This test does work locally but on CI it flakes consistently due to
// https://github.com/wasmCloud/wadm/issues/125. We will address this as a follow up once 0.4 is
// released
// async fn test_lotta_actors(client_info: &ClientInfo) {
//     let resp = client_info
//         .put_manifest_from_file("lotta_actors.yaml", None)
//         .await;

//     assert_ne!(
//         resp.result,
//         PutResult::Error,
//         "Shouldn't have errored when creating manifest: {resp:?}"
//     );

//     let resp = client_info
//         .deploy_manifest("lotta-actors", None, None)
//         .await;
//     assert_ne!(
//         resp.result,
//         DeployResult::Error,
//         "Shouldn't have errored when deploying manifest: {resp:?}"
//     );

//     // NOTE: This runs for a while, but it's because we're waiting for the provider to download,
//     // which can take a bit
//     assert_status(None, Some(7), || async {
//         let inventory = client_info.get_all_inventory("default").await?;

//         check_actors(
//             &inventory,
//             "wasmcloud.azurecr.io/echo:0.3.7",
//             "lotta-actors",
//             200,
//         )?;
//         check_providers(
//             &inventory,
//             "wasmcloud.azurecr.io/httpserver:0.17.0",
//             ExpectedCount::AtLeast(1),
//         )?;

//         Ok(())
//     })
//     .await;
// }

async fn test_spread_all_hosts(client_info: &ClientInfo) {
    let resp = client_info
        .put_manifest_from_file("all_hosts.yaml", None, None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    // Deploy manifest
    let resp = client_info
        .deploy_manifest("echo-all-hosts", None, None, None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.7",
            "echo-all-hosts",
            5,
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(5),
        )?;

        Ok(())
    })
    .await;
}

async fn test_complex_app(client_info: &ClientInfo) {
    let resp = client_info
        .put_manifest_from_file("complex.yaml", None, None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    // Deploy manifest
    let resp = client_info
        .deploy_manifest("complex", None, None, None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/blobby:0.2.0",
            "complex",
            5,
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::AtLeast(3),
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/blobstore_fs:0.3.2",
            ExpectedCount::Exactly(1),
        )?;

        let links = client_info
            .ctl_client("default")
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if !links.links.iter().any(|ld| {
            ld.actor_id == BLOBBY_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }) {
            anyhow::bail!(
                "Link between blobby actor and http provider should exist: {:#?}",
                links
            );
        }

        if !links.links.iter().any(|ld| {
            ld.actor_id == BLOBBY_ACTOR_ID
                && ld.provider_id == BLOBSTORE_FS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:blobstore"
        }) {
            anyhow::bail!(
                "Link between blobby actor and blobstore-fs provider should not exist: {:#?}",
                links
            );
        }

        // Make sure nothing is running on things it shouldn't be on
        if inventory.values().any(|inv| {
            inv.labels
                .get("region")
                .map(|region| region == "us-taylor-west" || region == "us-brooks-east")
                .unwrap_or(false)
                && inv
                    .providers
                    .iter()
                    .any(|prov| prov.id == BLOBSTORE_FS_PROVIDER_ID)
        }) {
            anyhow::bail!("Provider should only be running on the moon");
        }
        let moon_inventory = inventory
            .values()
            .find(|inv| {
                inv.labels
                    .get("region")
                    .map(|region| region == "moon")
                    .unwrap_or(false)
            })
            .unwrap();

        if moon_inventory
            .actors
            .iter()
            .any(|actor| actor.id == BLOBBY_ACTOR_ID)
        {
            anyhow::bail!("Actors shouldn't be running on the moon");
        }

        Ok(())
    })
    .await;
}

// This test should be run after other tests have finished since we are stopping one of the hosts
async fn test_stop_host_rebalance(client_info: &ClientInfo) {
    let resp = client_info
        .put_manifest_from_file("host_stop.yaml", None, None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    // Deploy manifest
    let resp = client_info
        .deploy_manifest("host-stop", None, None, None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // Make sure everything deploys first
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/kvcounter:0.4.2",
            "host-stop",
            5,
        )?;

        Ok(())
    })
    .await;

    // Now get the inventory and figure out which host is running the most actors of the spread and
    // stop that one
    let host_to_stop = client_info
        .get_all_inventory("default")
        .await
        .expect("Unable to fetch inventory")
        .into_iter()
        .filter(|(_, inv)| {
            inv.labels
                .get("region")
                .map(|region| region == "us-brooks-east")
                .unwrap_or(false)
        })
        .max_by_key(|(_, inv)| {
            inv.actors
                .iter()
                .find(|actor| actor.id == KV_COUNTER_ACTOR_ID)
                .map(|desc| desc.instances.len())
                .unwrap_or(0)
        })
        .map(|(host_id, _)| host_id)
        .unwrap();

    client_info
        .ctl_client("default")
        .stop_host(&host_to_stop, None)
        .await
        .expect("Should have stopped host");

    // Just to make sure state has time to update and the host shuts down, wait for a bit before
    // checking
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Now wait for us to get to 5 again
    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/kvcounter:0.4.2",
            "host-stop",
            5,
        )?;

        Ok(())
    })
    .await;
}

// NOTE(thomastaylor312): Future tests could include actually making sure the app works as expected
