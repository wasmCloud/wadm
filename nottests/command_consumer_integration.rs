use std::collections::BTreeMap;

use futures::TryStreamExt;
use serial_test::serial;
use tokio::time::{timeout, Duration};

use wadm::commands::*;

mod helpers;
use helpers::StreamWrapper;

#[tokio::test]
async fn test_consumer_stream() {
    let mut wrapper = StreamWrapper::new("consumer_stream".into(), None).await;

    // Publish a whole bunch of commands to the stream
    wrapper
        .publish_command(ScaleComponent {
            component_id: None,
            reference: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 3,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
        })
        .await;
    wrapper
        .publish_command(StartProvider {
            reference: "baz".to_string(),
            host_id: "fakehost".to_string(),
            model_name: "fake".into(),
            ..Default::default()
        })
        .await;
    wrapper
        .publish_command(PutLinkdef {
            component_id: "foobar".to_string(),
            provider_id: "fakehost".to_string(),
            contract_id: "wasmcloud:httpserver".to_string(),
            model_name: "fake".into(),
            ..Default::default()
        })
        .await;

    // Make sure we get the right data back, in the right order
    let mut cmd = wrapper.wait_for_command().await;
    if let Command::ScaleComponent(actor) = cmd.as_ref() {
        assert_eq!(
            actor.reference,
            "foobar",
            "Expected to get a valid start actor command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!("Event wasn't a start actor command. Got {:?}", cmd.as_ref());
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
    if let Command::PutLinkdef(ld) = cmd.as_ref() {
        assert_eq!(
            ld.contract_id,
            "wasmcloud:httpserver",
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
    wrapper
        .client
        .publish(wrapper.topic.clone(), "{\"fake\": \"json\"}".into())
        .await
        .expect("Should be able to publish data");

    wrapper
        .publish_command(ScaleComponent {
            component_id: Some("foobar".to_string()),
            reference: "foobarref".to_string(),
            host_id: "fakehost".to_string(),
            count: 0,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
        })
        .await;

    let mut cmd = wrapper.wait_for_command().await;
    if let Command::ScaleComponent(actor) = cmd.as_ref() {
        assert_eq!(
            actor.component_id,
            Some("foobar".to_string()),
            "Expected to get a valid stop actor command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!("Event wasn't a stop actor command. Got {:?}", cmd.as_ref());
    }
    cmd.ack().await.expect("Should be able to ack message");
}

#[tokio::test]
#[serial]
async fn test_nack_and_rereceive() {
    let mut wrapper = StreamWrapper::new("nack_and_rereceive".into(), None).await;
    // Send an event
    wrapper
        .publish_command(ScaleComponent {
            component_id: None,
            reference: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 3,
            model_name: "fake".into(),
            annotations: BTreeMap::new(),
        })
        .await;

    // Get the event and then nack it
    let mut cmd = wrapper.wait_for_command().await;
    // Make sure we got the right event
    if let Command::ScaleComponent(actor) = cmd.as_ref() {
        assert_eq!(
            actor.reference,
            "foobar",
            "Expected to get a valid start actor command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!("Event wasn't a start actor command. Got {:?}", cmd.as_ref());
    }

    cmd.nack().await;
    // Now do it again and make sure we get the same event
    if let Command::ScaleComponent(actor) = wrapper.wait_for_command().await.as_ref() {
        assert_eq!(
            actor.reference,
            "foobar",
            "Expected to get a valid start actor command, got command: {:?}",
            cmd.as_ref()
        );
    } else {
        panic!("Event wasn't a start actor command. Got {:?}", cmd.as_ref());
    }
    cmd.ack().await.expect("Should be able to ack");
}
