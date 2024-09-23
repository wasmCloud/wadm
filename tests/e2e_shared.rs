#![cfg(feature = "_e2e_tests")]
use std::time::Duration;
use std::{collections::HashMap, path::PathBuf};

use anyhow::{ensure, Context as _};
use futures::StreamExt;
use helpers::HTTP_CLIENT_IMAGE_REF;
use wadm_types::api::StatusType;

mod e2e;
mod helpers;

use e2e::{assert_status, check_components, check_providers, ClientInfo, ExpectedCount};

use crate::{
    e2e::check_status,
    helpers::{HELLO_IMAGE_REF, HTTP_SERVER_IMAGE_REF},
};

const MANIFESTS_PATH: &str = "tests/fixtures/manifests";
const DOCKER_COMPOSE_FILE: &str = "tests/docker-compose-e2e-shared.yaml";

const SHARED_COMPONENTS_LATTICE: &str = "shared_components";
const SHARED_PROVIDERS_LATTICE: &str = "shared_providers";

#[cfg(feature = "_e2e_tests")]
#[tokio::test(flavor = "multi_thread")]
async fn run_shared_component_tests() {
    use futures::FutureExt;

    let root_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("Unable to find repo root"));
    let manifest_dir = root_dir.join(MANIFESTS_PATH);
    let compose_file = root_dir.join(DOCKER_COMPOSE_FILE);

    let mut client_info = ClientInfo::new(manifest_dir, compose_file).await;
    client_info
        .add_ctl_client(SHARED_COMPONENTS_LATTICE, None)
        .await;
    client_info.add_wadm_client(SHARED_COMPONENTS_LATTICE).await;
    client_info
        .add_ctl_client(SHARED_PROVIDERS_LATTICE, None)
        .await;
    client_info.add_wadm_client(SHARED_PROVIDERS_LATTICE).await;
    client_info.launch_wadm().await;

    // Wait for the first event on the lattice prefix before we start deploying and checking
    // statuses. Wadm can absolutely handle hosts starting before you start the wadm process, but the first event
    // on the lattice will initialize the lattice monitor and for the following test we quickly assert things.
    let mut sub = client_info
        .client
        .subscribe("wasmbus.evt.*.>".to_string())
        .await
        .expect("Should be able to subscribe to default events");
    // Host heartbeats happen every 30 seconds, if we don't get a heartbeat in 2 minutes, bail.
    let _ = tokio::time::timeout(std::time::Duration::from_secs(120), sub.next())
        .await
        .expect("should have received a host heartbeat event before timeout");

    // Wait for hosts to start
    let mut did_start = false;
    for _ in 0..10 {
        match (
            client_info
                .ctl_client(SHARED_COMPONENTS_LATTICE)
                .get_hosts()
                .await,
            client_info
                .ctl_client(SHARED_PROVIDERS_LATTICE)
                .get_hosts()
                .await,
        ) {
            (Ok(hosts_one), Ok(hosts_two)) if hosts_one.len() == 2 && hosts_two.len() == 2 => {
                eprintln!(
                    "Hosts {}/2, {}/2 currently available",
                    hosts_one.len(),
                    hosts_two.len()
                );
                did_start = true;
                break;
            }
            (Ok(hosts_one), Ok(hosts_two)) => {
                eprintln!(
                    "Waiting for all hosts to be available, {}/2, {}/2 currently available",
                    hosts_one.len(),
                    hosts_two.len()
                );
            }
            (Err(e), _) | (_, Err(e)) => {
                eprintln!("Error when fetching hosts: {e}",)
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }

    if !did_start {
        panic!("Hosts didn't start")
    }

    let stream = client_info.get_status_stream().await;
    stream
        .purge()
        .await
        .expect("shouldn't have errored purging stream");

    // The futures must be boxed or they're technically different types
    let tests = [
        test_shared_providers(&client_info).boxed(),
        test_shared_components(&client_info).boxed(),
    ];
    futures::future::join_all(tests).await;
}

async fn test_shared_providers(client_info: &ClientInfo) {
    let stream = client_info.get_status_stream().await;
    let client = client_info.wadm_client(SHARED_PROVIDERS_LATTICE);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("shared_http.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    assert_status(None, Some(5), || async {
        let inventory = client_info
            .get_all_inventory(SHARED_PROVIDERS_LATTICE)
            .await?;

        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::Exactly(1))?;
        check_providers(&inventory, HTTP_CLIENT_IMAGE_REF, ExpectedCount::Exactly(1))?;

        let links = client_info
            .ctl_client(SHARED_PROVIDERS_LATTICE)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .response
            .context("Should have links")?;

        ensure!(links.is_empty(), "Shouldn't have any links");

        check_status(
            &stream,
            SHARED_PROVIDERS_LATTICE,
            "shared-http",
            StatusType::Deployed,
        )
        .await
        .unwrap();

        Ok(())
    })
    .await;

    // Deploy manifest with HTTP component that depends on the shared manifest
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("shared_http_dev.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    assert_status(None, Some(5), || async {
        let inventory = client_info
            .get_all_inventory(SHARED_PROVIDERS_LATTICE)
            .await?;

        // Ensure all configuration is set correctly
        let config = client_info
            .ctl_client(SHARED_PROVIDERS_LATTICE)
            .get_config("shared_http_dev-httpaddr")
            .await
            .map_err(|e| anyhow::anyhow!("should have http provider source config {e}"))?
            .response
            .context("should have http provider source config response")?;
        assert_eq!(
            config,
            HashMap::from_iter(vec![("address".to_string(), "0.0.0.0:8080".to_string())])
        );

        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::Exactly(1))?;
        check_providers(&inventory, HTTP_CLIENT_IMAGE_REF, ExpectedCount::Exactly(1))?;
        check_components(&inventory, HELLO_IMAGE_REF, "shared-http-dev", 12)?;

        let links = client_info
            .ctl_client(SHARED_PROVIDERS_LATTICE)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .response
            .context("Should have links")?;

        ensure!(
            links.len() == 2,
            "Should have two links: http_server -> component -> http_client"
        );

        if !links.iter().any(|ld| {
            // This is checking that the source ID and the target
            // come from the correct generated manifest IDs
            ld.source_id == "shared_http-httpserver"
                && ld.target == "shared_http_dev-hello"
                && ld.wit_namespace == "wasi"
                && ld.wit_package == "http"
                && ld.interfaces == vec!["incoming-handler"]
                && ld.name == "default"
        }) {
            anyhow::bail!(
                "Link between http server provider and hello component should exist: {:#?}",
                links
            )
        }
        if !links.iter().any(|ld| {
            // This is checking that the source ID and the target
            // come from the correct generated manifest IDs
            ld.source_id == "shared_http_dev-hello"
                && ld.target == "shared_http-httpclient"
                && ld.wit_namespace == "wasi"
                && ld.wit_package == "http"
                && ld.interfaces == vec!["outgoing-handler"]
                && ld.name == "default"
        }) {
            anyhow::bail!(
                "Link between hello component and http client provider should exist: {:#?}",
                links
            )
        }

        check_status(
            &stream,
            SHARED_PROVIDERS_LATTICE,
            "shared-http",
            StatusType::Deployed,
        )
        .await
        .unwrap();
        check_status(
            &stream,
            SHARED_PROVIDERS_LATTICE,
            "shared-http-dev",
            StatusType::Deployed,
        )
        .await
        .unwrap();

        Ok(())
    })
    .await;
}

async fn test_shared_components(client_info: &ClientInfo) {
    let stream = client_info.get_status_stream().await;
    let client = client_info.wadm_client(SHARED_COMPONENTS_LATTICE);
    let (name, _version) = client
        .put_manifest(client_info.load_raw_manifest("shared_component.yaml").await)
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    assert_status(None, Some(5), || async {
        let inventory = client_info
            .get_all_inventory(SHARED_COMPONENTS_LATTICE)
            .await?;

        let config = client_info
            .ctl_client(SHARED_COMPONENTS_LATTICE)
            .get_config("shared_component-defaults")
            .await
            .map_err(|e| anyhow::anyhow!("should have http provider source config {e}"))?
            .response
            .context("should have http provider source config response")?;
        assert_eq!(
            config,
            HashMap::from_iter(vec![("left".to_string(), "right".to_string())])
        );

        check_components(&inventory, HELLO_IMAGE_REF, "shared-component", 1)?;

        let links = client_info
            .ctl_client(SHARED_COMPONENTS_LATTICE)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .response
            .context("Should have links")?;

        ensure!(links.is_empty(), "Shouldn't have any links");

        check_status(
            &stream,
            SHARED_COMPONENTS_LATTICE,
            "shared-component",
            StatusType::Deployed,
        )
        .await
        .unwrap();

        Ok(())
    })
    .await;

    // Deploy manifest with HTTP component that depends on the shared manifest
    let (name, _version) = client
        .put_manifest(
            client_info
                .load_raw_manifest("shared_component_dev.yaml")
                .await,
        )
        .await
        .expect("Shouldn't have errored when creating manifest");

    client
        .deploy_manifest(&name, None)
        .await
        .expect("Shouldn't have errored when deploying manifest");

    assert_status(None, Some(5), || async {
        let inventory = client_info
            .get_all_inventory(SHARED_COMPONENTS_LATTICE)
            .await?;

        check_providers(&inventory, HTTP_SERVER_IMAGE_REF, ExpectedCount::Exactly(1))?;
        check_components(&inventory, HELLO_IMAGE_REF, "shared-component", 1)?;
        check_components(&inventory, HELLO_IMAGE_REF, "shared-component-dev", 12)?;

        let config = client_info
            .ctl_client(SHARED_COMPONENTS_LATTICE)
            .get_config("shared_component_dev-someconfig")
            .await
            .map_err(|e| anyhow::anyhow!("should have http provider source config {e}"))?
            .response
            .context("should have http provider source config response")?;
        assert_eq!(
            config,
            HashMap::from_iter(vec![("foo".to_string(), "bar".to_string())])
        );

        let links = client_info
            .ctl_client(SHARED_COMPONENTS_LATTICE)
            .get_links()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .response
            .context("Should have links")?;

        ensure!(links.len() == 3, "Should have three links");

        if !links.iter().any(|ld| {
            ld.source_id == "shared_component_dev-hello"
                && ld.target == "shared_component-link_to_meee"
                && ld.wit_namespace == "custom"
                && ld.wit_package == "package"
                && ld.interfaces == vec!["inter", "face"]
                && ld.name == "default"
        }) {
            anyhow::bail!("Link between hello components should exist: {:#?}", links)
        }
        if !links.iter().any(|ld| {
            ld.source_id == "shared_component-link_to_meee"
                && ld.target == "shared_component_dev-hello"
                && ld.wit_namespace == "custom"
                && ld.wit_package == "package"
                && ld.interfaces == vec!["inter", "face"]
                && ld.name == "default"
        }) {
            anyhow::bail!("Link between hello components should exist: {:#?}", links)
        }
        if !links.iter().any(|ld| {
            ld.source_id == "shared_component_dev-httpserver"
                && ld.target == "shared_component-link_to_meee"
                && ld.wit_namespace == "wasi"
                && ld.wit_package == "http"
                && ld.interfaces == vec!["incoming-handler"]
                && ld.name == "default"
        }) {
            anyhow::bail!(
                "Link between http server provider and hello component should exist: {:#?}",
                links
            )
        }

        check_status(
            &stream,
            SHARED_COMPONENTS_LATTICE,
            "shared-component",
            StatusType::Deployed,
        )
        .await
        .unwrap();
        check_status(
            &stream,
            SHARED_COMPONENTS_LATTICE,
            "shared-component-dev",
            StatusType::Deployed,
        )
        .await
        .unwrap();

        Ok(())
    })
    .await;
}
