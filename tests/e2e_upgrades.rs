#![cfg(feature = "_e2e_tests")]
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};

use anyhow::Context;
use async_nats::jetstream::consumer::pull::Config;
use futures::{FutureExt, StreamExt, TryStreamExt};
use wadm_types::api::{Status, StatusType};

mod e2e;
mod helpers;

use e2e::{
    assert_status, check_components, check_config, check_providers, check_status, ClientInfo,
    ExpectedCount, DEFAULT_LATTICE_ID,
};
use helpers::{HELLO_COMPONENT_ID, HELLO_IMAGE_REF, HTTP_SERVER_COMPONENT_ID};

const MANIFESTS_PATH: &str = "tests/fixtures/manifests";
const DOCKER_COMPOSE_FILE: &str = "tests/docker-compose-e2e_upgrades.yaml";
const KEYVALUE_REDIS_COMPONENT_ID: &str = "keyvalue_redis";
const DOG_FETCHER_GENERATED_ID: &str = "dog_fetcher";
const KVCOUNTER_GENERATED_ID: &str = "kvcounter";

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
    client_info.add_wadm_client("default").await;
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
    let client = client_info.wadm_client(DEFAULT_LATTICE_ID);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("outdatedapp.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    // Once manifest is deployed, first status should be compensating
    check_status(&stream, "default", "updateapp", StatusType::Reconciling)
        .await
        .unwrap();

    let generated_kvcounter_id = format!("updateapp-{KVCOUNTER_GENERATED_ID}");
    let generated_dogfetcher_id = format!("updateapp-{DOG_FETCHER_GENERATED_ID}");

    assert_status(None, Some(5), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/dog-fetcher-rust:0.1.0",
            "updateapp",
            5,
        )?;
        check_components(&inventory, HELLO_IMAGE_REF, "updateapp", 3)?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/http-keyvalue-counter-rust:0.1.0",
            "updateapp",
            3,
        )?;
        check_providers(
            &inventory,
            "ghcr.io/wasmcloud/http-server:0.20.1",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &inventory,
            "ghcr.io/wasmcloud/keyvalue-redis:0.26.0",
            ExpectedCount::Exactly(1),
        )?;

        let links = client_info
            .ctl_client("default")
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .into_data()
            .context("should have links")?;

        let http_link = links
            .iter()
            .find(|link| {
                link.target() == HELLO_COMPONENT_ID
                    && link.source_id() == HTTP_SERVER_COMPONENT_ID
                    && link.wit_namespace() == "wasi"
                    && link.wit_package() == "http"
            })
            .context("Should have http link with hello")?;
        if let Err(e) = check_config(
            client_info.ctl_client("default"),
            &http_link.source_config()[0],
            &HashMap::from_iter([("address".to_string(), "0.0.0.0:8080".to_string())]),
        )
        .await
        {
            anyhow::bail!(
                "Link between http provider and hello component should exist on port 8080: {:?}",
                e
            )
        };

        let dog_link = links
            .iter()
            .find(|link| {
                link.target() == generated_dogfetcher_id
                    && link.source_id() == HTTP_SERVER_COMPONENT_ID
                    && link.wit_namespace() == "wasi"
                    && link.wit_package() == "http"
            })
            .context("Should have http link with dog-fetcher")?;
        if let Err(e) = check_config(
            client_info.ctl_client("default"),
            &dog_link.source_config()[0],
            &HashMap::from_iter([("address".to_string(), "0.0.0.0:8081".to_string())]),
        )
        .await
        {
            anyhow::bail!(
                "Link between http provider and dog fetcher component should exist on port 8081: {:?}",
                e
            )
        };

        let kv_link = links
            .iter()
            .find(|link| {
                link.source_id() == generated_kvcounter_id
                    && link.target() == KEYVALUE_REDIS_COMPONENT_ID
                    && link.wit_namespace() == "wasi"
                    && link.wit_package() == "keyvalue"
            })
            .context("Should have redis link with kvcounter")?;
        if let Err(e) = check_config(
            client_info.ctl_client("default"),
            &kv_link.target_config()[0],
            &HashMap::from_iter([("URL".to_string(), "redis://127.0.0.1:6379".to_string())]),
        )
        .await
        {
            anyhow::bail!(
                "Link between kvcounter component and kvredis provider should exist with local URL: {:?}",
                e
            )
        };

        check_status(&stream, "default", "updateapp", StatusType::Deployed)
            .await
            .expect("application should be deployed after all components and links are deployed");

        Ok(())
    })
    .await;

    let consumer = stream
        .get_or_create_consumer(
            "test_upgrade_ephemeral",
            Config {
                name: Some("test_upgrade_ephemeral".to_string()),
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::New,
                ..Default::default()
            },
        )
        .await
        .expect("Should be able to get consumer");

    let (name, version) = client
        .put_manifest(client_info.load_raw_manifest("upgradedapp.yaml").await)
        .await
        .expect("Shouldn't have errored when creating updated manifest");

    client
        .deploy_manifest(&name, Some(&version))
        .await
        .expect("Shouldn't have errored when deploying updated manifest");

    let mut messages = consumer
        .messages()
        .await
        .expect("Unable to get status stream");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let val = messages
                .try_next()
                .await
                .expect("Got error when consuming status stream")
                .expect("Status stream ended early");
            let stat: Status = serde_json::from_slice(&val.payload)
                .expect("Should be able to decode status body from JSON");
            if matches!(stat.info.status_type, StatusType::Reconciling) {
                break;
            }
        }
    })
    .await
    .expect("Timed out waiting for reconciling status after deploying");

    // Once manifest is updated, status should be reconciling
    check_status(&stream, "default", "updateapp", StatusType::Reconciling)
        .await
        .unwrap();

    assert_status(None, Some(5), || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/dog-fetcher-rust:0.1.1",
            "updateapp",
            5,
        )?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/echo-messaging-rust:0.1.0",
            "updateapp",
            1,
        )?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/http-hello-world-rust:0.1.0",
            "updateapp",
            3,
        )?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/http-keyvalue-counter-rust:0.1.0",
            "updateapp",
            0,
        )?;
        check_providers(
            &inventory,
            "ghcr.io/wasmcloud/http-server:0.21.0",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &inventory,
            "ghcr.io/wasmcloud/keyvalue-redis:0.26.0",
            ExpectedCount::Exactly(0),
        )?;

        let links = client_info
            .ctl_client("default")
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .into_data()
            .context("should've had links")?;

        let http_link = links
            .iter()
            .find(|link| {
                link.target() == HELLO_COMPONENT_ID
                    && link.source_id() == HTTP_SERVER_COMPONENT_ID
                    && link.wit_namespace() == "wasi"
                    && link.wit_package() == "http"
            })
            .ok_or_else(|| anyhow::anyhow!("Should have http link with hello"))?;

        if let Err(e) = check_config(
            client_info.ctl_client("default"),
            &http_link.source_config()[0],
            &HashMap::from([("address".to_string(), "0.0.0.0:8082".to_string())]),
        )
        .await
        {
            anyhow::bail!(
                "{e} Link between hello component and http provider should exist on port 8082: {:#?}",
                links
            )
        }

        if links.iter().any(|ld| {
            ld.source_id() == generated_kvcounter_id
                && ld.target() == KEYVALUE_REDIS_COMPONENT_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "keyvalue"
        }) {
            anyhow::bail!(
                "Link between kvcounter component and redis provider should not exist: {:#?}",
                links
            )
        }

        check_status(&stream, "default", "updateapp", StatusType::Deployed)
            .await
            .unwrap();

        Ok(())
    })
    .await;

    // Deploy another updated manifest -- this time just w/ link values and provider config modifications
    let (name, version) = client
        .put_manifest(client_info.load_raw_manifest("upgradedapp2.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, Some(&version))
        .await
        .expect("Shouldn't have errored when deploying manifest");

    // Once manifest is updated, status should be reconciling
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let val = messages
                .try_next()
                .await
                .expect("Got error when consuming status stream")
                .expect("Status stream ended early");
            let stat: Status = serde_json::from_slice(&val.payload)
                .expect("Should be able to decode status body from JSON");
            if matches!(stat.info.status_type, StatusType::Reconciling) {
                break;
            }
        }
    })
    .await
    .expect("Timed out waiting for reconciling status after deploying");

    assert_status(None, None, || async {
        let inventory = client_info.get_all_inventory("default").await?;

        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/dog-fetcher-rust:0.1.1",
            "updateapp",
            5,
        )?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/echo-messaging-rust:0.1.0",
            "updateapp",
            1,
        )?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/http-hello-world-rust:0.1.0",
            "updateapp",
            3,
        )?;
        check_components(
            &inventory,
            "ghcr.io/wasmcloud/components/http-keyvalue-counter-rust:0.1.0",
            "updateapp",
            0,
        )?;
        check_providers(
            &inventory,
            "ghcr.io/wasmcloud/http-server:0.21.0",
            ExpectedCount::Exactly(1),
        )?;
        check_providers(
            &inventory,
            "ghcr.io/wasmcloud/keyvalue-redis:0.26.0",
            ExpectedCount::Exactly(0),
        )?;

        let links = client_info
            .ctl_client("default")
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .into_data()
            .context("should've had links")?;

        let http_link = links
            .iter()
            .find(|link| {
                link.target() == HELLO_COMPONENT_ID
                    && link.source_id() == HTTP_SERVER_COMPONENT_ID
                    && link.wit_namespace() == "wasi"
                    && link.wit_package() == "http"
            })
            .ok_or_else(|| anyhow::anyhow!("Should have http link with hello"))?;

        if let Err(e) = check_config(
            client_info.ctl_client("default"),
            &http_link.source_config()[0],
            &HashMap::from([("address".to_string(), "0.0.0.0:8080".to_string())]),
        )
        .await
        {
            anyhow::bail!(
            "{e} Link between hello component and http provider should exist on port 8080: {:#?}",
            links
        )
        }

        let dog_link = links
            .iter()
            .find(|link| {
                link.target() == generated_dogfetcher_id
                    && link.source_id() == HTTP_SERVER_COMPONENT_ID
                    && link.wit_namespace() == "wasi"
                    && link.wit_package() == "http"
            })
            .ok_or_else(|| anyhow::anyhow!("Should have dog link with hello"))?;

        if let Err(e) = check_config(
            client_info.ctl_client("default"),
            &dog_link.source_config()[0],
            &HashMap::from([("address".to_string(), "0.0.0.0:8081".to_string())]),
        )
        .await
        {
            anyhow::bail!(
                "{e} Link between dog component and http provider should exist on port 8080: {:#?}",
                links
            )
        }

        if links.iter().any(|ld| {
            ld.source_id() == generated_kvcounter_id
                && ld.target() == KEYVALUE_REDIS_COMPONENT_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "keyvalue"
        }) {
            anyhow::bail!(
                "Link between kvcounter component and redis provider should not exist: {:#?}",
                links
            )
        }

        check_status(&stream, "default", "updateapp", StatusType::Deployed)
            .await
            .unwrap();

        Ok(())
    })
    .await;

    // TODO(#298): Enable this section of the test to ensure we fail deployments with duplicate IDs
    // let (name, _version) = client
    //     .put_manifest(client_info.load_raw_manifest("upgradedapp3.yaml").await)
    //     .await
    //     .expect("Shouldn't have errored when creating manifest");

    // let failed_deploy = client.deploy_manifest(&name, None).await;

    // assert!(
    //     failed_deploy.is_err(),
    //     "Should have errored when deploying manifest"
    // );

    // assert_status(None, None, || async {
    //     eprintln!("Checking should've failed deploy ...");
    //     let inventory = client_info.get_all_inventory("default").await?;
    //     check_providers(
    //         &inventory,
    //         "ghcr.io/wasmcloud/http-server:0.21.0",
    //         ExpectedCount::Exactly(1),
    //     )?;
    //     check_providers(
    //         &inventory,
    //         "ghcr.io/wasmcloud/http-server:0.21.0",
    //         ExpectedCount::Exactly(0),
    //     )?;
    //     Ok(())
    // })
    // .await;
}
