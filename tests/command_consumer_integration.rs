use std::collections::BTreeMap;

use futures::TryStreamExt;
use tokio::time::{timeout, Duration};

use wadm::commands::*;

mod helpers;
use helpers::{setup_env, StreamWrapper};

#[tokio::test]
async fn test_consumer_stream() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client for the test setup");
    let mut wrapper = StreamWrapper::new("consumer_stream".into(), nats_client.clone()).await;

    // Publish a whole bunch of commands to the stream
    wrapper
        .publish_command(ScaleComponent {
            component_id: "barfood".to_string(),
            reference: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 3,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
            config: vec![],
        })
        .await;
    wrapper
        .publish_command(StartProvider {
            reference: "baz".to_string(),
            provider_id: "fakepay".to_string(),
            host_id: "fakehost".to_string(),
            model_name: "fake".into(),
            config: vec![],
            annotations: BTreeMap::new(),
        })
        .await;
    wrapper
        .publish_command(PutLink {
            source_id: "barfood".to_string(),
            target: "fakepay".to_string(),
            name: "default".to_string(),
            wit_namespace: "dontreallycare".to_string(),
            wit_package: "doesitmatter".to_string(),
            model_name: "fake".to_string(),
            interfaces: vec!["something".to_string()],
            ..Default::default()
        })
        .await;

    // Make sure we get the right data back, in the right order
    let mut cmd = wrapper.wait_for_command().await;
    if let Command::ScaleComponent(event) = cmd.as_ref() {
        assert_eq!(
            event.reference,
            "foobar",
            "Expected to get a valid start component command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!(
            "Event wasn't a start component command. Got {:?}",
            cmd.as_ref()
        );
    }
    cmd.ack().await.expect("Should be able to ack message");

    let mut cmd = wrapper.wait_for_command().await;
    if let Command::StartProvider(prov) = cmd.as_ref() {
        assert_eq!(
            prov.reference,
            "baz",
            "Expected to get a valid start provider command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!(
            "Event wasn't a start provder command. Got {:?}",
            cmd.as_ref()
        );
    }
    cmd.ack().await.expect("Should be able to ack message");

    let mut cmd = wrapper.wait_for_command().await;
    if let Command::PutLink(link) = cmd.as_ref() {
        assert_eq!(
            link.wit_namespace,
            "dontreallycare",
            "Expected to get a valid put linkdef command, got command: {:?}",
            cmd.as_ref()
        );
        assert_eq!(
            link.wit_package,
            "doesitmatter",
            "Expected to get a valid put linkdef command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!("Event wasn't a put linkdef command. Got {:?}", cmd.as_ref());
    }
    cmd.ack().await.expect("Should be able to ack message");

    // Now make sure there aren't any more coming to make sure our acks worked properly
    timeout(Duration::from_secs(1), wrapper.stream.try_next())
        .await
        .expect_err("No more commands should have been received");

    // Send some garbage data, then some normal data and make sure it just skips
    nats_client
        .publish(wrapper.topic.clone(), "{\"fake\": \"json\"}".into())
        .await
        .expect("Should be able to publish data");

    wrapper
        .publish_command(ScaleComponent {
            component_id: "foobar".to_string(),
            reference: "foobarref".to_string(),
            host_id: "fakehost".to_string(),
            count: 0,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
            ..Default::default()
        })
        .await;

    let mut cmd = wrapper.wait_for_command().await;
    if let Command::ScaleComponent(event) = cmd.as_ref() {
        assert_eq!(
            event.component_id,
            "foobar".to_string(),
            "Expected to get a valid stop component command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!(
            "Event wasn't a stop component command. Got {:?}",
            cmd.as_ref()
        );
    }
    cmd.ack().await.expect("Should be able to ack message");
}

#[tokio::test]
async fn test_nack_and_rereceive() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client for the test setup");
    let mut wrapper = StreamWrapper::new("nack_and_rereceive".into(), nats_client).await;
    // Send an event
    wrapper
        .publish_command(ScaleComponent {
            component_id: "barfood".to_string(),
            reference: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 3,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
            config: vec![],
        })
        .await;

    // Get the event and then nack it
    let mut cmd = wrapper.wait_for_command().await;
    // Make sure we got the right event
    if let Command::ScaleComponent(event) = cmd.as_ref() {
        assert_eq!(
            event.reference,
            "foobar",
            "Expected to get a valid start component command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!(
            "Event wasn't a start component command. Got {:?}",
            cmd.as_ref()
        );
    }

    cmd.nack().await;
    // Now do it again and make sure we get the same event
    if let Command::ScaleComponent(event) = wrapper.wait_for_command().await.as_ref() {
        assert_eq!(
            event.reference,
            "foobar",
            "Expected to get a valid start component command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!(
            "Event wasn't a start component command. Got {:?}",
            cmd.as_ref()
        );
    }
    cmd.ack().await.expect("Should be able to ack");
}
