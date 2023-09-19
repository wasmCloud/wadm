#![cfg(feature = "_e2e_tests")]
use std::path::PathBuf;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use wadm::server::{DeployResult, PutResult, StatusType};

mod e2e;
mod helpers;

use e2e::{assert_status, check_actors, check_providers, ClientInfo, ExpectedCount};
use helpers::{ECHO_ACTOR_ID, HTTP_SERVER_PROVIDER_ID};

use crate::e2e::check_status;

const MANIFESTS_PATH: &str = "test/data";
const DOCKER_COMPOSE_FILE: &str = "test/docker-compose-e2e-upgrade.yaml";
const KV_COUNTER_ACTOR_ID: &str = "MCFMFDWFHGKELOXPCNCDXKK5OFLHBVEWRAOXR5JSQUD2TOFRE3DFPM7E";
const KV_REDIS_PROVIDER_ID: &str = "VAZVC4RX54J2NVCMCW7BPCAHGGG5XZXDBXFUMDUXGESTMQEJLC3YVZWB";

#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_upgrade_tests() {
    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);
    // NOTE(brooksmtownsend) reusing the e2e docker compose file for now but I'll only
    // really be concerned with the application on a single host.
    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    let mut client_info = ClientInfo::new(manifest_dir, compose_file).await;
    client_info.add_ctl_client("default", None).await;
    client_info.launch_wadm().await;

    // Wait for the first event on the lattice prefix before we start deploying and checking
    // statuses. Wadm can absolutely handle hosts starting before you start the wadm process, but the first event
    // on the lattice will initialize the lattice monitor and for the following test we quickly assert things.
    let mut sub = client_info
        .client
        .subscribe("wadm.evt.default".to_string())
        .await
        .expect("Should be able to subscribe to default events");
    let _ = sub.next().await;

    // Wait for hosts to start
    let mut did_start = false;
    for _ in 0..10 {
        match client_info.ctl_client("default").get_hosts().await {
            Ok(hosts) if hosts.len() == 1 => {
                eprintln!("Host {}/1 currently available", hosts.len());
                did_start = true;
                break;
            }
            Ok(hosts) => {
                eprintln!(
                    "Waiting for host to be available {}/1 currently available",
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

    test_upgrade(&client_info).boxed().await;
}

async fn test_upgrade(client_info: &ClientInfo) {
    let stream = client_info.get_status_stream().await;
    stream
        .purge()
        .await
        .expect("shouldn't have errored purging stream");
    let resp = client_info
        .put_manifest_from_file("outdatedapp.yaml", None, None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info
        .deploy_manifest("updateapp", None, None, None)
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // Once manifest is deployed, first status should be compensating
    check_status(&stream, "default", "updateapp", StatusType::Compensating)
        .await
        .unwrap();

    assert_status(None, Some(7), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/xkcd:0.1.1",
            "updateapp",
            5,
        )?;
        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.4",
            "updateapp",
            3,
        )?;
        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/kvcounter:0.4.0",
            "updateapp",
            3,
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/httpserver:0.17.0",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/kvredis:0.22.0",
            ExpectedCount::Exactly(1),
        )?;

        // Oh no a sleep! How horrible!
        // Actually, this is a good thing! If we reach this point because the httpserver
        // provider upgraded really quickly, that means we still have to wait 5 seconds
        // for the provider health check to trigger linkdef creation. So, after everything
        // gets created, give the linkdef scaler time to react to the provider health check.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let links = client_info
            .ctl_client("default")
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        println!("Links: {:?}", links);

        if !links.iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
                && ld
                    .values
                    .get("address")
                    .map(|v| v == "0.0.0.0:8080")
                    .expect("Linkdef values should have an address")
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should exist on port 8080: {:#?}",
                links
            )
        }

        if !links.iter().any(|ld| {
            ld.actor_id == KV_COUNTER_ACTOR_ID
                && ld.provider_id == KV_REDIS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:keyvalue"
                && ld
                    .values
                    .get("URL")
                    .map(|v| v == "redis://127.0.0.1:6379")
                    .expect("Linkdef values should have a redis URL")
        }) {
            anyhow::bail!(
                "Link between kvcounter actor and redis provider should exist: {:#?}",
                links
            )
        }

        check_status(&stream, "default", "updateapp", StatusType::Ready)
            .await
            .unwrap();

        Ok(())
    })
    .await;

    // Deploy updated manifest
    let resp = client_info
        .put_manifest_from_file("upgradedapp.yaml", None, None)
        .await;

    assert_ne!(
        resp.result,
        PutResult::Error,
        "Shouldn't have errored when creating manifest: {resp:?}"
    );

    let resp = client_info
        .deploy_manifest("updateapp", None, None, Some("v0.0.2"))
        .await;
    assert_ne!(
        resp.result,
        DeployResult::Error,
        "Shouldn't have errored when deploying manifest: {resp:?}"
    );

    // Once manifest is updated, status should be compensating
    check_status(&stream, "default", "updateapp", StatusType::Compensating)
        .await
        .unwrap();

    assert_status(None, None, || async {
        let inventory = client_info.get_all_inventory("default").await?;
        println!("Inventory: {:?}", inventory);

        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/xkcd:0.1.1",
            "updateapp",
            5,
        )?;
        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/message-pub:0.1.3",
            "updateapp",
            1,
        )?;
        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/echo:0.3.8",
            "updateapp",
            3,
        )?;
        check_actors(
            &inventory,
            "wasmcloud.azurecr.io/kvcounter:0.4.0",
            "updateapp",
            0,
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/httpserver:0.19.0",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &inventory,
            "wasmcloud.azurecr.io/kvredis:0.22.0",
            ExpectedCount::Exactly(0),
        )?;

        // Oh no a sleep! How horrible!
        // Actually, this is a good thing! If we reach this point because the httpserver
        // provider upgraded really quickly, that means we still have to wait 5 seconds
        // for the provider health check to trigger linkdef creation. So, after everything
        // gets created, give the linkdef scaler time to react to the provider health check.
        tokio::time::sleep(Duration::from_secs(5)).await;

        let links = client_info
            .ctl_client("default")
            .query_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        if !links.iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
                && ld
                    .values
                    .get("address")
                    .map(|v| v == "0.0.0.0:8082")
                    .expect("Linkdef values should have an address")
        }) {
            anyhow::bail!(
                "Link between echo actor and http provider should exist on port 8082: {:#?}",
                links
            )
        }

        if links.iter().any(|ld| {
            ld.actor_id == KV_COUNTER_ACTOR_ID
                && ld.provider_id == KV_REDIS_PROVIDER_ID
                && ld.contract_id == "wasmcloud:keyvalue"
        }) {
            anyhow::bail!(
                "Link between kvcounter actor and redis provider should not exist: {:#?}",
                links
            )
        }

        check_status(&stream, "default", "updateapp", StatusType::Ready)
            .await
            .unwrap();

        Ok(())
    })
    .await;
}
