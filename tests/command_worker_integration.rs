use futures::StreamExt;
use serial_test::serial;

use wadm::{commands::*, consumers::manager::Worker, workers::CommandWorker};

mod helpers;
use helpers::{setup_test_wash, StreamWrapper, TestWashConfig};

const ECHO_ACTOR_ID: &str = "MBCFOPM6JW2APJLXJD3Z5O4CN7CPYJ2B4FTKLJUR5YR5MITIU7HD3WD5";
const HTTP_SERVER_PROVIDER_ID: &str = "VAG3QITQQ2ODAOWB5TTQSDJ53XK3SHBEIFNK4AYJ5RKAX2UNSCAPHA5M";

#[tokio::test]
// TODO: Run in parallel once https://github.com/wasmCloud/wash/issues/402 is fixed. Please
// note this test should probably be changed to an e2e test as the order of events is somewhat flaky
#[serial]
async fn test_commands() {
    let config = TestWashConfig::random().await.unwrap();
    let _guard = setup_test_wash(&config).await;

    let mut wrapper = StreamWrapper::new("commands_integration".into(), config.nats_port).await;

    let ctl_client = wasmcloud_control_interface::ClientBuilder::new(wrapper.client.clone())
        .build()
        .await
        .expect("Should be able to create ctl client");
    let worker = CommandWorker::new(ctl_client.clone());

    let host_id = ctl_client
        .get_hosts()
        .await
        .unwrap()
        .get(0)
        .expect("Should be able to find hosts")
        .id
        .to_owned();

    let mut sub = wrapper
        .client
        .subscribe("wasmbus.evt.default".to_string())
        .await
        .unwrap();

    // Start an actor
    wrapper
        .publish_command(StartActor {
            reference: "wasmcloud.azurecr.io/echo:0.3.4".to_string(),
            host_id: host_id.clone(),
            count: 2,
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    // We are starting two actors so wait for both
    wait_for_event(&mut sub, "actor_started").await;
    wait_for_event(&mut sub, "actor_started").await;
    // Sorry for the lazy de-racing, but for some reason if we don't wait for a bit the host hasn't
    // finished updating its inventory
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Get the current actors and make sure stuff was started
    let inventory = ctl_client
        .get_host_inventory(&host_id)
        .await
        .unwrap()
        .actors;
    assert_eq!(
        inventory.len(),
        1,
        "Should only have 1 actor: {:?}",
        inventory
    );
    assert_eq!(
        inventory[0].image_ref.as_deref().unwrap(),
        "wasmcloud.azurecr.io/echo:0.3.4",
        "Should have started the correct actor"
    );
    assert_eq!(
        inventory[0].instances.len(),
        2,
        "Should have started the correct number of actors"
    );
    assert_eq!(
        inventory[0].instances[0]
            .annotations
            .as_ref()
            .unwrap()
            .get(wadm::MANAGED_BY_ANNOTATION)
            .expect("Should have the managed by annotation"),
        wadm::MANAGED_BY_IDENTIFIER,
        "Should have the proper identifier"
    );
    assert_eq!(
        inventory[0].instances[0]
            .annotations
            .as_ref()
            .unwrap()
            .get(wadm::APP_SPEC_ANNOTATION)
            .expect("Should have the managed by annotation"),
        "fake",
        "Should have the proper identifier"
    );

    // Start a provider
    wrapper
        .publish_command(StartProvider {
            reference: "wasmcloud.azurecr.io/httpserver:0.17.0".to_string(),
            host_id: host_id.clone(),
            link_name: None,
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "provider_started").await;
    // Make sure we see the provider has passed health check, at which point it should show up in
    // inventory
    wait_for_event(&mut sub, "health_check_passed").await;

    // Get the current providers and make sure stuff was started
    let inventory = ctl_client
        .get_host_inventory(&host_id)
        .await
        .unwrap()
        .providers;
    assert_eq!(inventory.len(), 1, "Should only have 1 provider");
    assert_eq!(
        inventory[0].image_ref.as_deref().unwrap(),
        "wasmcloud.azurecr.io/httpserver:0.17.0",
        "Should have started the correct actor"
    );
    assert_eq!(
        inventory[0]
            .annotations
            .as_ref()
            .unwrap()
            .get(wadm::MANAGED_BY_ANNOTATION)
            .expect("Should have the managed by annotation"),
        wadm::MANAGED_BY_IDENTIFIER,
        "Should have the proper identifier"
    );
    assert_eq!(
        inventory[0]
            .annotations
            .as_ref()
            .unwrap()
            .get(wadm::APP_SPEC_ANNOTATION)
            .expect("Should have the managed by annotation"),
        "fake",
        "Should have the proper identifier"
    );

    // Put a linkdef
    wrapper
        .publish_command(PutLinkdef {
            actor_id: ECHO_ACTOR_ID.to_owned(),
            provider_id: HTTP_SERVER_PROVIDER_ID.to_owned(),
            link_name: wadm::DEFAULT_LINK_NAME.to_owned(),
            contract_id: "wasmcloud:httpserver".to_string(),
            values: [("ADDRESS".to_string(), "0.0.0.0:9999".to_string())].into(),
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "linkdef_set").await;

    // Get the current actors and make sure stuff was started
    let inventory = ctl_client.query_links().await.unwrap().links;
    // We could have more than one link due to local testing, so search for the proper link
    inventory
        .into_iter()
        .find(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        })
        .expect("Linkdef should exist");

    // Delete the linkdef
    wrapper
        .publish_command(DeleteLinkdef {
            actor_id: ECHO_ACTOR_ID.to_owned(),
            provider_id: HTTP_SERVER_PROVIDER_ID.to_owned(),
            link_name: wadm::DEFAULT_LINK_NAME.to_owned(),
            contract_id: "wasmcloud:httpserver".to_string(),
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "linkdef_deleted").await;

    // Get the current actors and make sure stuff was started
    let inventory = ctl_client.query_links().await.unwrap().links;
    // We could have more than one link due to local testing, so search for the proper link
    assert!(
        !inventory.into_iter().any(|ld| {
            ld.actor_id == ECHO_ACTOR_ID
                && ld.provider_id == HTTP_SERVER_PROVIDER_ID
                && ld.contract_id == "wasmcloud:httpserver"
        }),
        "Linkdef should be deleted"
    );

    // Stop the provider
    wrapper
        .publish_command(StopProvider {
            provider_id: HTTP_SERVER_PROVIDER_ID.to_owned(),
            contract_id: "wasmcloud:httpserver".to_owned(),
            link_name: None,
            host_id: host_id.clone(),
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "provider_stopped").await;

    // Get the current providers and make sure stuff was started
    let inventory = ctl_client
        .get_host_inventory(&host_id)
        .await
        .unwrap()
        .providers;
    assert!(inventory.is_empty(), "Should have no providers");

    // Stop the actor
    wrapper
        .publish_command(StopActor {
            actor_id: ECHO_ACTOR_ID.to_owned(),
            count: 2,
            host_id: host_id.clone(),
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    // We're stopping two actors so we need to wait for both events
    wait_for_event(&mut sub, "actor_stopped").await;
    wait_for_event(&mut sub, "actor_stopped").await;

    // Get the current providers and make sure stuff was started
    let inventory = ctl_client
        .get_host_inventory(&host_id)
        .await
        .unwrap()
        .actors;
    assert!(inventory.is_empty(), "Should have no actors");
}

#[tokio::test]
// TODO: Run in parallel once https://github.com/wasmCloud/wash/issues/402 is fixed. Please
// note this test should probably be changed to an e2e test as the order of events is somewhat flaky
#[serial]
async fn test_annotation_stop() {
    // This test is a sanity check that we only stop annotated actors
    let config = TestWashConfig::random().await.unwrap();
    let _guard = setup_test_wash(&config).await;

    let mut wrapper = StreamWrapper::new("annotation_stop".into(), config.nats_port).await;

    let ctl_client = wasmcloud_control_interface::ClientBuilder::new(wrapper.client.clone())
        .build()
        .await
        .expect("Should be able to create ctl client");
    let worker = CommandWorker::new(ctl_client.clone());

    let mut sub = wrapper
        .client
        .subscribe("wasmbus.evt.default".to_string())
        .await
        .unwrap();

    let host_id = ctl_client
        .get_hosts()
        .await
        .unwrap()
        .get(0)
        .expect("Should be able to find hosts")
        .id
        .to_owned();

    // Start an actor
    wrapper
        .publish_command(StartActor {
            reference: "wasmcloud.azurecr.io/echo:0.3.4".to_string(),
            host_id: host_id.clone(),
            count: 2,
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    // We are starting two actors so wait for both
    wait_for_event(&mut sub, "actor_started").await;
    wait_for_event(&mut sub, "actor_started").await;
    // Sorry for the lazy de-racing, but for some reason if we don't wait for a bit the host hasn't
    // finished updating its inventory
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Get the current actors and make sure stuff was started
    let inventory = ctl_client
        .get_host_inventory(&host_id)
        .await
        .unwrap()
        .actors;
    assert_eq!(inventory.len(), 1, "Should only have 1 actor");
    assert_eq!(
        inventory[0].image_ref.as_deref().unwrap(),
        "wasmcloud.azurecr.io/echo:0.3.4",
        "Should have started the correct actor"
    );
    assert_eq!(
        inventory[0].instances.len(),
        2,
        "Should have started the correct number of actors"
    );

    // Start another actor without the annotation
    ctl_client
        .start_actor(&host_id, "wasmcloud.azurecr.io/echo:0.3.4", 1, None)
        .await
        .unwrap();

    wait_for_event(&mut sub, "actor_started").await;
    // Sorry for the lazy de-racing, but for some reason if we don't wait for a bit the host hasn't
    // finished updating its inventory
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Stop the actor
    wrapper
        .publish_command(StopActor {
            actor_id: ECHO_ACTOR_ID.to_owned(),
            count: 2,
            host_id: host_id.clone(),
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    // We are stopping two actors so wait for both
    wait_for_event(&mut sub, "actor_stopped").await;
    wait_for_event(&mut sub, "actor_stopped").await;

    // Get the current providers and make sure stuff was started
    let inventory = ctl_client
        .get_host_inventory(&host_id)
        .await
        .unwrap()
        .actors;
    assert_eq!(inventory.len(), 1, "Should only have 1 actor");
    assert_eq!(
        inventory[0].image_ref.as_deref().unwrap(),
        "wasmcloud.azurecr.io/echo:0.3.4",
        "Should have started the correct actor"
    );
    assert_eq!(
        inventory[0].instances.len(),
        1,
        "Should have 1 unmanaged actor still running"
    );
}

async fn wait_for_event(sub: &mut async_nats::Subscriber, match_text: &str) {
    // Providers can take a bit to start if they are downloading
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
    // Consume the initial tick
    interval.tick().await;
    loop {
        tokio::select! {
            res = sub.next() => {
                let msg = res.expect("Stream shouldn't have ended");
                if String::from_utf8_lossy(&msg.payload).contains(match_text) {
                    return
                }
            }
            _ = interval.tick() => panic!("Timed out waiting for event {}", match_text)
        }
    }
}
