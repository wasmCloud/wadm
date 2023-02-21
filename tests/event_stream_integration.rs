use futures::{Stream, StreamExt};
use serial_test::serial;
use tokio::time::{timeout, Duration};

use alt_wadm::{event_stream::EventStream, events::*};

mod helpers;

const HTTP_SERVER_PROVIDER_ID: &str = "VAG3QITQQ2ODAOWB5TTQSDJ53XK3SHBEIFNK4AYJ5RKAX2UNSCAPHA5M";
const HTTP_SERVER_REFERENCE: &str = "wasmcloud.azurecr.io/httpserver:0.16.0";
const ECHO_ACTOR_ID: &str = "MBCFOPM6JW2APJLXJD3Z5O4CN7CPYJ2B4FTKLJUR5YR5MITIU7HD3WD5";
const ECHO_REFERENCE: &str = "wasmcloud.azurecr.io/echo:0.3.4";
const CONTRACT_ID: &str = "wasmcloud:httpserver";
const LINK_NAME: &str = "default";
const WASMBUS_EVENT_TOPIC: &str = "wasmbus.evt.default";
// Timeout accounts for time to pull stuff from registry
const TIMEOUT_DURATION: Duration = Duration::from_secs(10);

async fn get_event_stream() -> EventStream {
    let client = async_nats::connect("127.0.0.1:4222")
        .await
        .expect("Unable to setup nats client");
    EventStream::new(client, WASMBUS_EVENT_TOPIC)
        .await
        .expect("Unable to setup stream")
}

#[derive(serde::Deserialize)]
struct HostResponse {
    hosts: Vec<serde_json::Value>,
}

#[tokio::test]
#[serial]
async fn test_event_stream() {
    let _guard = helpers::setup_test().await;

    let mut stream = get_event_stream().await;

    // NOTE: the first heartbeat doesn't come for 30s so we are ignoring it for now

    // Start an actor
    helpers::run_wash_command(["ctl", "start", "actor", ECHO_REFERENCE]).await;

    if let Event::ActorStarted(actor) = wait_for_event(&mut stream).await {
        assert_eq!(
            actor.public_key, ECHO_ACTOR_ID,
            "Expected to get a started event for the right actor, got ID: {}",
            actor.public_key
        );
    } else {
        panic!("Event wasn't an actor started event");
    }

    // Start a provider
    helpers::run_wash_command(["ctl", "start", "provider", HTTP_SERVER_REFERENCE]).await;

    if let Event::ProviderStarted(provider) = wait_for_event(&mut stream).await {
        assert_eq!(
            provider.public_key, HTTP_SERVER_PROVIDER_ID,
            "Expected to get a started event for the right provider, got ID: {}",
            provider.public_key
        );
    } else {
        panic!("Event wasn't an provider started event");
    }

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

    if let Event::LinkdefSet(link) = wait_for_event(&mut stream).await {
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

    // 0.60 still has a bug with duplicate linkdef put events, so grab an extra event
    wait_for_event(&mut stream).await;

    // Delete link
    helpers::run_wash_command(["ctl", "link", "del", ECHO_ACTOR_ID, CONTRACT_ID]).await;

    if let Event::LinkdefDeleted(link) = wait_for_event(&mut stream).await {
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

    if let Event::ProviderStopped(provider) = wait_for_event(&mut stream).await {
        assert_eq!(
            provider.public_key, HTTP_SERVER_PROVIDER_ID,
            "Expected to get a stopped event for the right provider, got ID: {}",
            provider.public_key
        );
    } else {
        panic!("Event wasn't an provider stopped event");
    }

    // Stop an actor
    helpers::run_wash_command(["ctl", "stop", "actor", &host_id, ECHO_ACTOR_ID]).await;

    if let Event::ActorStopped(actor) = wait_for_event(&mut stream).await {
        assert_eq!(
            actor.public_key, ECHO_ACTOR_ID,
            "Expected to get a stopped event for the right actor, got ID: {}",
            actor.public_key
        );
    } else {
        panic!("Event wasn't an actor stopped event");
    }

    // Stop the host
    helpers::run_wash_command(["ctl", "stop", "host", &host_id]).await;

    if let Event::HostStopped(host) = wait_for_event(&mut stream).await {
        assert_eq!(
            host.id, host_id,
            "Expected to get a stopped event for the host, got ID: {}",
            host.id
        );
    } else {
        panic!("Event wasn't an actor stopped event");
    }
}

#[tokio::test]
#[serial]
async fn test_filtered_event_stream() {
    let _guard = helpers::setup_test().await;
    let stream = get_event_stream().await;

    let mut stream = stream.into_filtered(vec![
        Event::ActorStarted(ActorStarted::default()),
        Event::ActorStopped(ActorStopped::default()),
    ]);

    // Start an actor
    helpers::run_wash_command(["ctl", "start", "actor", ECHO_REFERENCE]).await;

    // Now wait for an event
    let evt = wait_for_event(&mut stream).await;
    assert!(
        matches!(evt, Event::ActorStarted(_)),
        "Should have gotten actor started event: {:?}",
        evt
    );

    // Start a provider
    helpers::run_wash_command(["ctl", "start", "provider", HTTP_SERVER_REFERENCE]).await;

    // Stop an actor
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
    helpers::run_wash_command(["ctl", "stop", "actor", &host_id, ECHO_ACTOR_ID]).await;

    // The next event in the stream should be the actor stopped, skipping over the provider start
    let evt = wait_for_event(&mut stream).await;
    assert!(
        matches!(evt, Event::ActorStopped(_)),
        "Should have skipped the provider event: {:?}",
        evt
    );
}

async fn wait_for_event(mut stream: impl Stream<Item = Event> + Unpin) -> Event {
    let evt = timeout(TIMEOUT_DURATION, stream.next())
        .await
        .expect("Should have received event before timeout")
        .expect("Stream shouldn't have ended");

    // As a safety feature, we will throw away any heartbeats and listen for the next event
    if matches!(
        evt,
        Event::HostHeartbeat(_)
            | Event::ProviderHealthCheckPassed(_)
            | Event::ProviderHealthCheckFailed(_)
    ) {
        // Just a copy paste here so we don't have to deal with async recursion
        timeout(TIMEOUT_DURATION, stream.next())
            .await
            .expect("Should have received event before timeout")
            .expect("Stream shouldn't have ended")
    } else {
        evt
    }
}
