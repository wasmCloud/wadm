use anyhow::Result;
use futures::{Stream, TryStreamExt};
use serial_test::serial;
use tokio::time::{timeout, Duration};

use wadm::{
    consumers::{EventConsumer, ScopedMessage},
    events::*,
};

mod helpers;
use helpers::{
    setup_test_wash, TestWashConfig, HELLO_COMPONENT_ID, HELLO_IMAGE_REF, HTTP_SERVER_COMPONENT_ID,
    HTTP_SERVER_IMAGE_REF,
};

const WASI: &str = "wasi";
const HTTP: &str = "http";
const HTTP_INTERFACE: &str = "incoming-handler";
const WASMBUS_EVENT_TOPIC: &str = "wasmbus.evt.default.>";
const STREAM_NAME: &str = "test_wadm_events";

// Timeout accounts for time to pull stuff from registry
const DEFAULT_TIMEOUT_DURATION: Duration = Duration::from_secs(10);
// Link operations take a slightly longer time to work through
const LINK_OPERATION_TIMEOUT_DURATION: Duration = Duration::from_secs(30);

async fn get_event_consumer(nats_url: String) -> EventConsumer {
    let client = async_nats::connect(&nats_url)
        .await
        .expect("Unable to setup nats event consumer client");
    let context = async_nats::jetstream::new(client);
    // HACK: Other tests may create the mirror stream, which overlaps with the consumers here for
    // our test, so delete it
    let _ = context.delete_stream("wadm_mirror").await;
    // If the stream exists, purge it and remove all consumers
    let stream = if let Ok(stream) = context.get_stream(STREAM_NAME).await {
        stream
            .purge()
            .await
            .expect("Unable to cleanup stream before test");

        while let Some(consumer) = stream
            .consumer_names()
            .try_next()
            .await
            .expect("Unable to get consumer name")
        {
            stream
                .delete_consumer(&consumer)
                .await
                .expect("Unable to delete consumer");
        }
        stream
    } else {
        // Create it if it doesn't exist
        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: STREAM_NAME.to_owned(),
                description: Some("A stream that stores all events coming in on the wasmbus.evt topics in a cluster".to_string()),
                num_replicas: 1,
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                subjects: vec![WASMBUS_EVENT_TOPIC.to_owned()],
                max_age: wadm::DEFAULT_EXPIRY_TIME,
                storage: async_nats::jetstream::stream::StorageType::Memory,
                allow_rollup: false,
                ..Default::default()
            })
            .await
            .expect("Should be able to create test stream")
    };
    EventConsumer::new(stream, WASMBUS_EVENT_TOPIC, "default", None)
        .await
        .expect("Unable to setup stream")
}

#[derive(serde::Deserialize)]
struct HostResponse {
    hosts: Vec<serde_json::Value>,
}

#[tokio::test]
// TODO: Run in parallel once https://github.com/wasmCloud/wash/issues/402 is fixed. Please
// note this test should probably be changed to an e2e test as the order of events is somewhat flaky
#[serial]
async fn test_event_stream() -> Result<()> {
    let config = TestWashConfig::random().await?;
    let _guard = setup_test_wash(&config).await;

    let mut stream = get_event_consumer(config.nats_url()).await;

    // NOTE: the first heartbeat doesn't come for 30s so we are ignoring it for now
    let ctl_port = config
        .nats_port
        .unwrap_or(crate::helpers::DEFAULT_NATS_PORT)
        .to_string();

    // Start a component
    helpers::run_wash_command([
        "start",
        "component",
        HELLO_IMAGE_REF,
        HELLO_COMPONENT_ID,
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    if let Event::ComponentScaled(event) = evt.as_ref() {
        assert_eq!(
            event.component_id, HELLO_COMPONENT_ID,
            "Expected to get a scaledevent for the right component, got ID: {}",
            event.component_id
        );
    } else {
        panic!("Event wasn't an component started event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Start a provider
    helpers::run_wash_command([
        "start",
        "provider",
        HTTP_SERVER_IMAGE_REF,
        HTTP_SERVER_COMPONENT_ID,
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    if let Event::ProviderStarted(provider) = evt.as_ref() {
        assert_eq!(
            provider.provider_id, HTTP_SERVER_COMPONENT_ID,
            "Expected to get a started event for the right provider, got ID: {}",
            provider.provider_id
        );
    } else {
        println!("EVT: {:?}", evt);
        panic!("Event wasn't an provider started event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Create a link
    helpers::run_wash_command([
        "link",
        "put",
        HELLO_COMPONENT_ID,
        HTTP_SERVER_COMPONENT_ID,
        WASI,
        HTTP,
        "--interface",
        HTTP_INTERFACE,
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream, LINK_OPERATION_TIMEOUT_DURATION).await;
    if let Event::LinkdefSet(event) = evt.as_ref() {
        assert_eq!(
            event.linkdef.source_id, HELLO_COMPONENT_ID,
            "Expected to get a linkdef event for the right component and provider, got component ID: {}",
            event.linkdef.source_id,
        );
        assert_eq!(
            event.linkdef.target, HTTP_SERVER_COMPONENT_ID,
            "Expected to get a linkdef event for the right component and provider, got provider ID: {}",
            event.linkdef.target,
        );
    } else {
        panic!("Event wasn't an link set event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // 0.60 still has a bug with duplicate linkdef put events, so grab an extra event
    let mut evt = wait_for_event(&mut stream, LINK_OPERATION_TIMEOUT_DURATION).await;
    evt.ack().await.expect("Should be able to ack event");

    // Delete link
    helpers::run_wash_command([
        "link",
        "del",
        HELLO_COMPONENT_ID,
        "wasi",
        "http",
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream, LINK_OPERATION_TIMEOUT_DURATION).await;
    if let Event::LinkdefDeleted(event) = evt.as_ref() {
        assert_eq!(
            event.source_id, HELLO_COMPONENT_ID,
            "Expected to get a linkdef event for the right component and provider, got component ID: {}",
            event.source_id,
        );
    } else {
        panic!("Event wasn't an link del event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Stop provider
    let host_id = serde_json::from_slice::<HostResponse>(
        &helpers::run_wash_command(["get", "hosts", "-o", "json", "--ctl-port", &ctl_port]).await,
    )
    .unwrap()
    .hosts[0]
        .get("id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    helpers::run_wash_command([
        "stop",
        "provider",
        HTTP_SERVER_COMPONENT_ID,
        "--host-id",
        &host_id,
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    if let Event::ProviderStopped(event) = evt.as_ref() {
        assert_eq!(
            event.provider_id, HTTP_SERVER_COMPONENT_ID,
            "Expected to get a stopped event for the right provider, got ID: {}",
            event.provider_id
        );
    } else {
        panic!("Event wasn't an provider stopped event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Stop a component
    helpers::run_wash_command([
        "stop",
        "component",
        HELLO_COMPONENT_ID,
        "--host-id",
        &host_id,
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    if let Event::ComponentScaled(event) = evt.as_ref() {
        assert_eq!(
            event.component_id, HELLO_COMPONENT_ID,
            "Expected to get a stopped event for the right component, got ID: {}",
            event.component_id
        );
    } else {
        panic!("Event wasn't an component stopped event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Stop the host
    helpers::run_wash_command(["stop", "host", &host_id, "--ctl-port", &ctl_port]).await;

    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    if let Event::HostStopped(host) = evt.as_ref() {
        assert_eq!(
            host.id, host_id,
            "Expected to get a stopped event for the host, got ID: {}",
            host.id
        );
    } else {
        panic!("Event wasn't an component stopped event");
    }
    evt.ack().await.expect("Should be able to ack event");

    Ok(())
}

#[tokio::test]
// TODO: Run in parallel once https://github.com/wasmCloud/wash/issues/402 is fixed. This
// does work when you run it individually. Please note that there is problems when running this
// against 0.60+ hosts as the KV bucket for linkdefs makes it so that all those linkdefs are emitted
// as published events when the host starts
#[serial]
async fn test_nack_and_rereceive() -> Result<()> {
    let config = TestWashConfig::random().await?;
    let _guard = setup_test_wash(&config).await;

    let mut stream = get_event_consumer(config.nats_url()).await;

    let ctl_port = config
        .nats_port
        .unwrap_or(crate::helpers::DEFAULT_NATS_PORT)
        .to_string();
    // Start a component
    helpers::run_wash_command([
        "start",
        "component",
        HELLO_IMAGE_REF,
        HELLO_COMPONENT_ID,
        "--ctl-port",
        &ctl_port,
    ])
    .await;

    // Get the event and then nack it
    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    // Make sure we got the right event
    if let Event::ComponentScaled(component) = evt.as_ref() {
        assert_eq!(
            component.component_id, HELLO_COMPONENT_ID,
            "Expected to get a started event for the right component, got ID: {}",
            component.component_id
        );
    } else {
        panic!("Event wasn't an component started event");
    }

    evt.nack().await;

    // Now do it again and make sure we get the same event
    let mut evt = wait_for_event(&mut stream, DEFAULT_TIMEOUT_DURATION).await;
    if let Event::ComponentScaled(component) = evt.as_ref() {
        assert_eq!(
            component.component_id, HELLO_COMPONENT_ID,
            "Expected to get a started event for the right component, got ID: {}",
            component.component_id
        );
    } else {
        panic!("Event wasn't an component scaled event");
    }

    evt.ack().await.expect("Should be able to ack event");

    Ok(())
}

async fn wait_for_event(
    mut stream: impl Stream<Item = Result<ScopedMessage<Event>, async_nats::Error>> + Unpin,
    duration: Duration,
) -> ScopedMessage<Event> {
    let mut evt = timeout(duration, stream.try_next())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Should have received event before timeout {}s",
                duration.as_secs()
            )
        })
        .expect("Stream shouldn't have had an error")
        .expect("Stream shouldn't have ended");

    // As a safety feature, we will throw away any heartbeats and listen for the next event
    if matches!(
        *evt,
        Event::HostHeartbeat(_)
            | Event::ProviderHealthCheckPassed(_)
            | Event::ProviderHealthCheckFailed(_)
    ) {
        evt.ack().await.expect("Should be able to ack message");
        // Just a copy paste here so we don't have to deal with async recursion
        timeout(duration, stream.try_next())
            .await
            .expect("Should have received event before timeout")
            .expect("Stream shouldn't have had an error")
            .expect("Stream shouldn't have ended")
    } else {
        evt
    }
}
