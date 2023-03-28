use futures::{Stream, TryStreamExt};
use serial_test::serial;
use tokio::time::{timeout, Duration};

use wadm::{
    consumers::{EventConsumer, ScopedMessage},
    events::*,
};

mod helpers;

const HTTP_SERVER_PROVIDER_ID: &str = "VAG3QITQQ2ODAOWB5TTQSDJ53XK3SHBEIFNK4AYJ5RKAX2UNSCAPHA5M";
const HTTP_SERVER_REFERENCE: &str = "wasmcloud.azurecr.io/httpserver:0.16.0";
const ECHO_ACTOR_ID: &str = "MBCFOPM6JW2APJLXJD3Z5O4CN7CPYJ2B4FTKLJUR5YR5MITIU7HD3WD5";
const ECHO_REFERENCE: &str = "wasmcloud.azurecr.io/echo:0.3.4";
const CONTRACT_ID: &str = "wasmcloud:httpserver";
const LINK_NAME: &str = "default";
const WASMBUS_EVENT_TOPIC: &str = "wasmbus.evt.default";
const STREAM_NAME: &str = "test_wadm_events";

// Timeout accounts for time to pull stuff from registry
const TIMEOUT_DURATION: Duration = Duration::from_secs(10);

async fn get_event_consumer() -> EventConsumer {
    let client = async_nats::connect("127.0.0.1:4222")
        .await
        .expect("Unable to setup nats client");
    let context = async_nats::jetstream::new(client);
    // If the stream exists, purge it
    let stream = if let Ok(stream) = context.get_stream(STREAM_NAME).await {
        stream
            .purge()
            .await
            .expect("Unable to cleanup stream before test");
        stream
    } else {
        // Create it if it doesn't exist
        context.create_stream(async_nats::jetstream::stream::Config {
            name: STREAM_NAME.to_owned(),
            description: Some("A stream that stores all events coming in on the wasmbus.evt topics in a cluster".to_string()),
            num_replicas: 1,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            subjects: vec![WASMBUS_EVENT_TOPIC.to_owned()],
            max_age: wadm::DEFAULT_EXPIRY_TIME,
            storage: async_nats::jetstream::stream::StorageType::Memory,
            allow_rollup: false,
            ..Default::default()
        }).await.expect("Should be able to create test stream")
    };
    EventConsumer::new(stream, WASMBUS_EVENT_TOPIC)
        .await
        .expect("Unable to setup stream")
}

#[derive(serde::Deserialize)]
struct HostResponse {
    hosts: Vec<serde_json::Value>,
}

#[tokio::test]
// TODO: Stop ignoring this test once https://github.com/wasmCloud/wash/issues/402 is fixed. Please
// note this test should probably be changed to an e2e test as the order of events is somewhat flaky
#[ignore]
#[serial]
async fn test_event_stream() {
    let _guard = helpers::setup_test().await;

    let mut stream = get_event_consumer().await;

    // NOTE: the first heartbeat doesn't come for 30s so we are ignoring it for now

    // Start an actor
    helpers::run_wash_command(["ctl", "start", "actor", ECHO_REFERENCE]).await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::ActorStarted(actor) = evt.as_ref() {
        assert_eq!(
            actor.public_key, ECHO_ACTOR_ID,
            "Expected to get a started event for the right actor, got ID: {}",
            actor.public_key
        );
    } else {
        panic!("Event wasn't an actor started event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Start a provider
    helpers::run_wash_command(["ctl", "start", "provider", HTTP_SERVER_REFERENCE]).await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::ProviderStarted(provider) = evt.as_ref() {
        assert_eq!(
            provider.public_key, HTTP_SERVER_PROVIDER_ID,
            "Expected to get a started event for the right provider, got ID: {}",
            provider.public_key
        );
    } else {
        panic!("Event wasn't an provider started event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Create a link
    helpers::run_wash_command([
        "ctl",
        "link",
        "put",
        ECHO_ACTOR_ID,
        HTTP_SERVER_PROVIDER_ID,
        CONTRACT_ID,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::LinkdefSet(link) = evt.as_ref() {
        assert_eq!(
            link.linkdef.actor_id, ECHO_ACTOR_ID,
            "Expected to get a linkdef event for the right actor and provider, got actor ID: {}",
            link.linkdef.actor_id,
        );
        assert_eq!(
            link.linkdef.provider_id, HTTP_SERVER_PROVIDER_ID,
            "Expected to get a linkdef event for the right actor and provider, got provider ID: {}",
            link.linkdef.provider_id,
        );
    } else {
        panic!("Event wasn't an link set event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // 0.60 still has a bug with duplicate linkdef put events, so grab an extra event
    let mut evt = wait_for_event(&mut stream).await;
    evt.ack().await.expect("Should be able to ack event");

    // Delete link
    helpers::run_wash_command(["ctl", "link", "del", ECHO_ACTOR_ID, CONTRACT_ID]).await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::LinkdefDeleted(link) = evt.as_ref() {
        assert_eq!(
            link.linkdef.actor_id, ECHO_ACTOR_ID,
            "Expected to get a linkdef event for the right actor and provider, got actor ID: {}",
            link.linkdef.actor_id,
        );
        assert_eq!(
            link.linkdef.provider_id, HTTP_SERVER_PROVIDER_ID,
            "Expected to get a linkdef event for the right actor and provider, got provider ID: {}",
            link.linkdef.provider_id,
        );
    } else {
        panic!("Event wasn't an link del event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Stop provider
    let host_id = serde_json::from_slice::<HostResponse>(
        &helpers::run_wash_command(["ctl", "get", "hosts", "-o", "json"]).await,
    )
    .unwrap()
    .hosts[0]
        .get("id")
        .unwrap()
        .as_str()
        .unwrap()
        .to_owned();
    helpers::run_wash_command([
        "ctl",
        "stop",
        "provider",
        &host_id,
        HTTP_SERVER_PROVIDER_ID,
        LINK_NAME,
        CONTRACT_ID,
    ])
    .await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::ProviderStopped(provider) = evt.as_ref() {
        assert_eq!(
            provider.public_key, HTTP_SERVER_PROVIDER_ID,
            "Expected to get a stopped event for the right provider, got ID: {}",
            provider.public_key
        );
    } else {
        panic!("Event wasn't an provider stopped event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Stop an actor
    helpers::run_wash_command(["ctl", "stop", "actor", &host_id, ECHO_ACTOR_ID]).await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::ActorStopped(actor) = evt.as_ref() {
        assert_eq!(
            actor.public_key, ECHO_ACTOR_ID,
            "Expected to get a stopped event for the right actor, got ID: {}",
            actor.public_key
        );
    } else {
        panic!("Event wasn't an actor stopped event");
    }
    evt.ack().await.expect("Should be able to ack event");

    // Stop the host
    helpers::run_wash_command(["ctl", "stop", "host", &host_id]).await;

    let mut evt = wait_for_event(&mut stream).await;
    if let Event::HostStopped(host) = evt.as_ref() {
        assert_eq!(
            host.id, host_id,
            "Expected to get a stopped event for the host, got ID: {}",
            host.id
        );
    } else {
        panic!("Event wasn't an actor stopped event");
    }
    evt.ack().await.expect("Should be able to ack event");
}

#[tokio::test]
// TODO: Stop ignoring this test once https://github.com/wasmCloud/wash/issues/402 is fixed. This
// does work when you run it individually. Please note that there is problems when running this
// against 0.60+ hosts as the KV bucket for linkdefs makes it so that all those linkdefs are emitted
// as published events when the host starts
#[ignore]
#[serial]
async fn test_nack_and_rereceive() {
    let _guard = helpers::setup_test().await;

    let mut stream = get_event_consumer().await;

    // Start an actor
    helpers::run_wash_command(["ctl", "start", "actor", ECHO_REFERENCE]).await;

    // Get the event and then nack it
    let mut evt = wait_for_event(&mut stream).await;
    // Make sure we got the right event
    if let Event::ActorStarted(actor) = evt.as_ref() {
        assert_eq!(
            actor.public_key, ECHO_ACTOR_ID,
            "Expected to get a started event for the right actor, got ID: {}",
            actor.public_key
        );
    } else {
        panic!("Event wasn't an actor started event");
    }

    evt.nack().await;

    // Now do it again and make sure we get the same event
    let mut evt = wait_for_event(&mut stream).await;
    if let Event::ActorStarted(actor) = evt.as_ref() {
        assert_eq!(
            actor.public_key, ECHO_ACTOR_ID,
            "Expected to get a started event for the right actor, got ID: {}",
            actor.public_key
        );
    } else {
        panic!("Event wasn't an actor started event");
    }

    evt.ack().await.expect("Should be able to ack event");
}

async fn wait_for_event(
    mut stream: impl Stream<Item = Result<ScopedMessage<Event>, async_nats::Error>> + Unpin,
) -> ScopedMessage<Event> {
    let mut evt = timeout(TIMEOUT_DURATION, stream.try_next())
        .await
        .expect("Should have received event before timeout")
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
        timeout(TIMEOUT_DURATION, stream.try_next())
            .await
            .expect("Should have received event before timeout")
            .expect("Stream shouldn't have had an error")
            .expect("Stream shouldn't have ended")
    } else {
        evt
    }
}