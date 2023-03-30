use futures::{Stream, TryStreamExt};
use serial_test::serial;
use tokio::time::{timeout, Duration};

use wadm::{
    commands::*,
    consumers::{CommandConsumer, ScopedMessage},
};

mod helpers;

const COMMAND_TOPIC: &str = "wadm.cmd.default";
const STREAM_NAME: &str = "test_wadm_commands";

// Timeout accounts for time to send message
const TIMEOUT_DURATION: Duration = Duration::from_secs(2);

async fn get_command_consumer() -> (async_nats::Client, CommandConsumer) {
    let client = async_nats::connect("127.0.0.1:4222")
        .await
        .expect("Unable to setup nats command consumer client");
    let context = async_nats::jetstream::new(client.clone());
    // If the stream exists, purge it
    let stream = if let Ok(stream) = context.get_stream(STREAM_NAME).await {
        stream
            .purge()
            .await
            .expect("Should be able to purge stream");
        stream
    } else {
        context
            .create_stream(async_nats::jetstream::stream::Config {
                name: STREAM_NAME.to_owned(),
                description: Some(
                    "A stream that stores all events coming in on the wasmbus.evt topics in a cluster"
                        .to_string(),
                ),
                num_replicas: 1,
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                subjects: vec![COMMAND_TOPIC.to_owned()],
                max_age: wadm::DEFAULT_EXPIRY_TIME,
                storage: async_nats::jetstream::stream::StorageType::Memory,
                allow_rollup: false,
                ..Default::default()
            })
            .await
            .expect("Should be able to create test stream")
    };
    (
        client,
        CommandConsumer::new(stream, COMMAND_TOPIC)
            .await
            .expect("Unable to setup stream"),
    )
}

#[tokio::test]
#[serial]
async fn test_consumer_stream() {
    let (client, mut stream) = get_command_consumer().await;

    // Publish a whole bunch of commands to the stream
    publish_command(
        &client,
        StartActor {
            reference: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 3,
        },
    )
    .await;
    publish_command(
        &client,
        StartProvider {
            reference: "baz".to_string(),
            host_id: "fakehost".to_string(),
            ..Default::default()
        },
    )
    .await;
    publish_command(
        &client,
        PutLinkdef {
            actor_id: "foobar".to_string(),
            provider_id: "fakehost".to_string(),
            contract_id: "wasmcloud:httpserver".to_string(),
            ..Default::default()
        },
    )
    .await;

    // Make sure we get the right data back, in the right order
    let mut cmd = wait_for_command(&mut stream).await;
    if let Command::StartActor(actor) = cmd.as_ref() {
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

    let mut cmd = wait_for_command(&mut stream).await;
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

    let mut cmd = wait_for_command(&mut stream).await;
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
    timeout(Duration::from_secs(1), stream.try_next())
        .await
        .expect_err("No more commands should have been received");

    // Send some garbage data, then some normal data and make sure it just skips
    client
        .publish(COMMAND_TOPIC.to_owned(), "{\"fake\": \"json\"}".into())
        .await
        .expect("Should be able to publish data");

    publish_command(
        &client,
        StopActor {
            actor_id: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 1,
        },
    )
    .await;

    let mut cmd = wait_for_command(&mut stream).await;
    if let Command::StopActor(actor) = cmd.as_ref() {
        assert_eq!(
            actor.actor_id,
            "foobar",
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
    let (client, mut stream) = get_command_consumer().await;
    // Send an event
    publish_command(
        &client,
        StartActor {
            reference: "foobar".to_string(),
            host_id: "fakehost".to_string(),
            count: 3,
        },
    )
    .await;

    // Get the event and then nack it
    let mut cmd = wait_for_command(&mut stream).await;
    // Make sure we got the right event
    if let Command::StartActor(actor) = cmd.as_ref() {
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
    if let Command::StartActor(actor) = wait_for_command(&mut stream).await.as_ref() {
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

async fn wait_for_command(
    mut stream: impl Stream<Item = Result<ScopedMessage<Command>, async_nats::Error>> + Unpin,
) -> ScopedMessage<Command> {
    timeout(TIMEOUT_DURATION, stream.try_next())
        .await
        .expect("Should have received event before timeout")
        .expect("Stream shouldn't have had an error")
        .expect("Stream shouldn't have ended")
}

async fn publish_command(client: &async_nats::Client, cmd: impl Into<Command>) {
    client
        .publish(
            COMMAND_TOPIC.to_owned(),
            serde_json::to_vec(&cmd.into()).unwrap().into(),
        )
        .await
        .expect("Unable to send command");
}
