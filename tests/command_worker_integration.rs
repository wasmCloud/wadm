use std::collections::{BTreeMap, HashMap};

use futures::StreamExt;

use wadm::{
    commands::*, consumers::manager::Worker, workers::CommandWorker, MANAGED_BY_ANNOTATION,
};

mod helpers;
use helpers::{
    setup_env, StreamWrapper, HELLO_COMPONENT_ID, HELLO_IMAGE_REF, HTTP_SERVER_COMPONENT_ID,
    HTTP_SERVER_IMAGE_REF,
};

#[tokio::test]
async fn test_commands() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client for the test setup");

    let mut wrapper = StreamWrapper::new("commands_integration".into(), nats_client.clone()).await;

    let ctl_client = wasmcloud_control_interface::ClientBuilder::new(nats_client.clone()).build();
    let worker = CommandWorker::new(ctl_client.clone());

    let host_id = ctl_client
        .get_hosts()
        .await
        .expect("should get hosts back")
        .first()
        .expect("Should be able to find hosts")
        .data()
        .map(|h| h.id())
        .expect("should be able to get host")
        .to_owned();

    let mut sub = nats_client
        .subscribe("wasmbus.evt.default.>".to_string())
        .await
        .unwrap();

    // Start a component
    wrapper
        .publish_command(ScaleComponent {
            component_id: HELLO_COMPONENT_ID.to_string(),
            reference: HELLO_IMAGE_REF.to_string(),
            host_id: host_id.clone(),
            count: 2,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
            config: vec![],
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "component_scaled").await;
    // Sorry for the lazy de-racing, but for some reason if we don't wait for a bit the host hasn't
    // finished updating its inventory
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Get the current components and make sure stuff was started
    let resp_data = ctl_client
        .get_host_inventory(&host_id)
        .await
        .expect("should get host inventory back")
        .into_data()
        .expect("should have host inventory");
    let inventory = resp_data.components();
    assert_eq!(
        inventory.len(),
        1,
        "Should only have 1 component: {:?}",
        inventory
    );
    assert_eq!(
        inventory[0].image_ref(),
        HELLO_IMAGE_REF,
        "Should have started the correct component"
    );
    assert_eq!(
        inventory[0].max_instances(),
        2,
        "Should have started the component with correct concurrency"
    );
    assert_eq!(
        inventory[0]
            .annotations()
            .unwrap()
            .get(wadm::MANAGED_BY_ANNOTATION)
            .expect("Should have the managed by annotation"),
        wadm::MANAGED_BY_IDENTIFIER,
        "Should have the proper identifier"
    );
    assert_eq!(
        inventory[0]
            .annotations()
            .unwrap()
            .get(wadm::APP_SPEC_ANNOTATION)
            .expect("Should have the managed by annotation"),
        "fake",
        "Should have the proper identifier"
    );

    // Create configuration for provider
    wrapper
        .publish_command(PutConfig {
            config_name: "fake-http_address".to_string(),
            config: HashMap::from_iter([("address".to_string(), "0.0.0.0:8080".to_string())]),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    // Start a provider
    wrapper
        .publish_command(StartProvider {
            reference: HTTP_SERVER_IMAGE_REF.to_string(),
            provider_id: HTTP_SERVER_COMPONENT_ID.to_owned(),
            host_id: host_id.clone(),
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
            config: vec!["fake-http_address".to_string()],
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
    let resp_data = ctl_client
        .get_host_inventory(&host_id)
        .await
        .expect("should get host inventory back")
        .into_data()
        .expect("should have host inventory");
    let inventory = resp_data.providers();
    assert_eq!(inventory.len(), 1, "Should only have 1 provider");
    assert_eq!(
        inventory[0].image_ref().unwrap(),
        HTTP_SERVER_IMAGE_REF,
        "Should have started the correct provider"
    );
    assert_eq!(
        inventory[0]
            .annotations()
            .unwrap()
            .get(wadm::MANAGED_BY_ANNOTATION)
            .expect("Should have the managed by annotation"),
        wadm::MANAGED_BY_IDENTIFIER,
        "Should have the proper identifier"
    );
    assert_eq!(
        inventory[0]
            .annotations()
            .unwrap()
            .get(wadm::APP_SPEC_ANNOTATION)
            .expect("Should have the managed by annotation"),
        "fake",
        "Should have the proper identifier"
    );

    // Put a linkdef
    wrapper
        .publish_command(PutLink {
            source_id: HTTP_SERVER_COMPONENT_ID.to_owned(),
            target: HELLO_COMPONENT_ID.to_owned(),
            name: wadm::DEFAULT_LINK_NAME.to_owned(),
            wit_namespace: "wasi".to_string(),
            wit_package: "http".to_string(),
            interfaces: vec!["incoming-handler".to_string()],
            model_name: "fake".to_string(),
            ..Default::default()
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "linkdef_set").await;

    // Get the current components and make sure stuff was started
    let inventory = ctl_client
        .get_links()
        .await
        .expect("should get links back")
        .into_data()
        .expect("should have links");
    // We could have more than one link due to local testing, so search for the proper link
    inventory
        .into_iter()
        .find(|ld| {
            ld.source_id() == HTTP_SERVER_COMPONENT_ID
                && ld.target() == HELLO_COMPONENT_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "http"
        })
        .expect("Linkdef should exist");

    // Delete the linkdef
    wrapper
        .publish_command(DeleteLink {
            source_id: HTTP_SERVER_COMPONENT_ID.to_owned(),
            link_name: wadm::DEFAULT_LINK_NAME.to_owned(),
            wit_namespace: "wasi".to_string(),
            wit_package: "http".to_string(),
            model_name: "fake".into(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "linkdef_deleted").await;

    // Get the current components and make sure stuff was started
    let inventory = ctl_client
        .get_links()
        .await
        .expect("should get links back")
        .into_data()
        .expect("should have links");
    // We could have more than one link due to local testing, so search for the proper link
    assert!(
        !inventory.into_iter().any(|ld| {
            ld.target() == HELLO_COMPONENT_ID
                && ld.source_id() == HTTP_SERVER_COMPONENT_ID
                && ld.wit_namespace() == "wasi"
                && ld.wit_package() == "http"
        }),
        "Linkdef should be deleted"
    );

    // Stop the provider
    wrapper
        .publish_command(StopProvider {
            provider_id: HTTP_SERVER_COMPONENT_ID.to_owned(),
            host_id: host_id.clone(),
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "provider_stopped").await;

    // Get the current providers and make sure stuff was started
    let resp_data = ctl_client
        .get_host_inventory(&host_id)
        .await
        .expect("should get host inventory back")
        .into_data()
        .expect("should have host inventory");
    let inventory = resp_data.providers();
    assert!(inventory.is_empty(), "Should have no providers");

    // Stop the component
    wrapper
        .publish_command(ScaleComponent {
            component_id: HELLO_COMPONENT_ID.to_owned(),
            reference: HELLO_IMAGE_REF.to_string(),
            count: 0,
            host_id: host_id.clone(),
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
            config: vec![],
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "component_scaled").await;

    // Get the current providers and make sure stuff was started
    let resp_data = ctl_client
        .get_host_inventory(&host_id)
        .await
        .expect("should get host inventory back")
        .into_data()
        .expect("should have host inventory");
    let inventory = resp_data.components();
    assert!(inventory.is_empty(), "Should have no components");
}

#[tokio::test]
// TODO: Run in parallel once https://github.com/wasmCloud/wash/issues/402 is fixed. Please
// note this test should probably be changed to an e2e test as the order of events is somewhat flaky
async fn test_annotation_stop() {
    // This test is a sanity check that we only stop annotated components
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client for the test setup");

    let mut wrapper = StreamWrapper::new("annotation_stop".into(), nats_client.clone()).await;

    let ctl_client = wasmcloud_control_interface::ClientBuilder::new(nats_client.clone()).build();
    let worker = CommandWorker::new(ctl_client.clone());

    let mut sub = nats_client
        .subscribe("wasmbus.evt.default.>".to_string())
        .await
        .unwrap();

    let responses = ctl_client.get_hosts().await.unwrap();
    let host_id = responses
        .first()
        .expect("Should be able to find hosts")
        .data()
        .map(|v| v.id())
        .unwrap();

    // Start an unmangaged component
    // NOTE(thomastaylor312): This is a workaround with current behavior where empty annotations
    // acts on _everything_. We could technically move this back down after the initial scale up of
    // the managed components after https://github.com/wasmCloud/wasmCloud/issues/746 is resolved
    ctl_client
        .scale_component(host_id, HELLO_IMAGE_REF, "unmanaged-hello", 1, None, vec![])
        .await
        .unwrap();

    wait_for_event(&mut sub, "component_scaled").await;
    // Sorry for the lazy de-racing, but for some reason if we don't wait for a bit the host hasn't
    // finished updating its inventory
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Start a component
    wrapper
        .publish_command(ScaleComponent {
            component_id: HELLO_COMPONENT_ID.to_string(),
            reference: HELLO_IMAGE_REF.to_string(),
            host_id: host_id.into(),
            count: 2,
            model_name: "fake".into(),
            annotations: BTreeMap::from_iter([("fake".to_string(), "wake".to_string())]),
            ..Default::default()
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    // Wait for the component_scaled event
    wait_for_event(&mut sub, "component_scaled").await;
    // Sorry for the lazy de-racing, but for some reason if we don't wait for a bit the host hasn't
    // finished updating its inventory
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Get the current components and make sure stuff was started
    let resp_data = ctl_client
        .get_host_inventory(host_id)
        .await
        .expect("should get host inventory back")
        .into_data()
        .expect("should have host inventory");
    let inventory = resp_data.components();
    let managed_inventory = inventory
        .iter()
        .filter(|c| {
            c.annotations()
                .is_some_and(|a| a.contains_key(MANAGED_BY_ANNOTATION))
        })
        .collect::<Vec<_>>();
    assert_eq!(inventory.len(), 2, "Should only have 2 components");
    assert_eq!(
        managed_inventory.len(),
        1,
        "Should only have 1 managed component"
    );
    assert_eq!(
        managed_inventory[0].image_ref(),
        HELLO_IMAGE_REF,
        "Should have started the correct component"
    );
    assert!(managed_inventory[0]
        .annotations()
        .map(|annotations| annotations.contains_key(MANAGED_BY_ANNOTATION))
        .unwrap_or(false));
    assert_eq!(
        managed_inventory[0].max_instances(),
        2,
        "Should have started the correct concurrency of components"
    );

    // Stop the managed components
    wrapper
        .publish_command(ScaleComponent {
            component_id: HELLO_COMPONENT_ID.to_owned(),
            reference: HELLO_IMAGE_REF.to_string(),
            count: 0,
            host_id: host_id.into(),
            model_name: "fake".into(),
            annotations: BTreeMap::from_iter([("fake".to_string(), "wake".to_string())]),
            config: vec![],
        })
        .await;

    let msg = wrapper.wait_for_command().await;
    worker
        .do_work(msg)
        .await
        .expect("Should be able to handle command properly");

    wait_for_event(&mut sub, "component_scaled").await;

    // Get the current providers and make sure stuff was started
    let resp_data = ctl_client
        .get_host_inventory(host_id)
        .await
        .unwrap()
        .into_data()
        .unwrap();
    let inventory = resp_data.components();
    assert_eq!(inventory.len(), 1, "Should only have 1 component");
    assert_eq!(
        inventory[0].image_ref(),
        HELLO_IMAGE_REF,
        "Should have started the correct component"
    );
    assert_eq!(
        inventory[0].max_instances(),
        1,
        "Should have 1 unmanaged component still running"
    );
}

async fn wait_for_event(sub: &mut async_nats::Subscriber, match_text: &str) {
    // Providers can take a bit to start if they are downloading
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
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
