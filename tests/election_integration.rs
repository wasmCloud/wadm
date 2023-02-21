use std::time::Duration;

use alt_wadm::election::*;
use async_nats::jetstream::{self, Context};
use serial_test::serial;
use tokio::sync::oneshot;

/// Creates an elector with the given TTL and ID. Also returns the jetstream context for testing. If
/// you don't care about the TTL, just use [`get_elector`]
async fn get_elector_with_ttl(ttl: Duration, id: String) -> (Elector, Context) {
    let context = jetstream::new(
        async_nats::connect("127.0.0.1:4222")
            .await
            .expect("Unable to get nats client"),
    );
    (
        Elector::new(&context, ttl, id)
            .await
            .expect("Unable to create elector"),
        context,
    )
}

/// Creates an elector with the given ID and the minimum TTL value. Also returns the jetstream
/// context for testing.
async fn get_elector(id: String) -> (Elector, Context) {
    get_elector_with_ttl(MIN_TTL, id).await
}

#[tokio::test]
#[serial]
async fn test_single_leader() {
    let (elector, context) = get_elector("single-leader".to_string()).await;
    let leader = tokio::time::timeout(Duration::from_secs(2), elector.elect())
        .await
        .expect("Should have been elected leader before timeout")
        .expect("Leader election shouldn't have failed");

    // Now make sure we are actually the leader in the KV store
    let leader_id = get_leader_key(&context)
        .await
        .expect("Leader key should exist");
    assert_eq!(
        leader_id, "single-leader",
        "Leader value should be the correct ID"
    );

    // Step down and make sure the key doesn't exist any more
    leader
        .step_down()
        .await
        .expect("Should not have errored when stepping down");
    assert!(
        get_leader_key(&context).await.is_none(),
        "Leader key should have been deleted"
    );
}

#[tokio::test]
#[serial]
async fn test_maintains_leadership() {
    let (elector, context) = get_elector("maintains-leadership".to_string()).await;
    let leader = tokio::time::timeout(Duration::from_secs(2), elector.elect())
        .await
        .expect("Should have been elected leader before timeout")
        .expect("Leader election shouldn't have failed");

    // Wait for MIN_TTL + 2s to make sure that the key still exists
    tokio::time::sleep(MIN_TTL + Duration::from_secs(2)).await;

    let leader_id = get_leader_key(&context)
        .await
        .expect("Leader key should still exist after TTL expiry");
    assert_eq!(
        leader_id, "maintains-leadership",
        "Leader value should be the correct ID"
    );

    // Clean up for next test
    leader
        .step_down()
        .await
        .expect("Should be able to step down");
}

#[tokio::test]
#[serial]
async fn test_multiple_leaders() {
    // Yes, I know this isn't exactly how things went down historically, but gotta make the tests
    // fun somehow
    let (elector1, _context) = get_elector("mark_antony".to_string()).await;

    let (interrupt_tx, interrupt_rx) = oneshot::channel();

    let antony = tokio::spawn(elector1.elect_and_run(async move { interrupt_rx.await.unwrap() }));

    // Give the first elector time to try and run the election. I know we could just call `elect`
    // instead of `elect_and_run` but I wanted to exercise that functionality as well, so this is a
    // dumb sleep
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Now try to elect Octavian, but start on a thread
    let (elector2, context) = get_elector("octavian".to_string()).await;
    let octavian = tokio::spawn(elector2.elect());

    // Give the second elector time to try and run the election
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Make sure the election process hasn't exited
    assert!(
        !octavian.is_finished(),
        "Other process shouldn't have grabbed leadership"
    );

    // Now check that Antony is still the leader
    let leader_id = get_leader_key(&context)
        .await
        .expect("Leader key should exist");
    assert_eq!(
        leader_id, "mark_antony",
        "Leader value should be the correct ID"
    );

    // Interrupt Antony (by capturing Alexandria)
    interrupt_tx
        .send(())
        .expect("Should be able to interrupt task");
    tokio::time::timeout(Duration::from_secs(2), antony)
        .await
        .expect("First leader should have finished processing")
        .expect("Error when awaiting task")
        .expect("Shouldn't have errored when stepping down");

    // Give the second elector time to take leadership
    let octavian = tokio::time::timeout(Duration::from_secs(2), octavian)
        .await
        .expect("Second leader should have finished election")
        .expect("Error when awaiting task")
        .expect("Shouldn't have errored when electing");

    // Now check that Octavian is leader
    let leader_id = get_leader_key(&context)
        .await
        .expect("Leader key should still exist");
    assert_eq!(
        leader_id, "octavian",
        "Leader value should be the correct ID"
    );

    // Clean up the leader so it doesn't interfere with next test
    octavian
        .step_down()
        .await
        .expect("Should be able to step down");
}

#[tokio::test]
#[serial]
async fn test_dead_leader() {
    let (elector, context) = get_elector("dead-leader".to_string()).await;
    let leader = tokio::time::timeout(Duration::from_secs(2), elector.elect())
        .await
        .expect("Should have been elected leader before timeout")
        .expect("Leader election shouldn't have failed");

    // Double check the leader is set properly
    let leader_id = get_leader_key(&context)
        .await
        .expect("Leader key should still exist");
    assert_eq!(
        leader_id, "dead-leader",
        "Leader value should be the correct ID"
    );

    // start a second one
    let (elector2, context) = get_elector("live-leader".to_string()).await;
    let leader2 = tokio::spawn(elector2.elect());

    // Now drop the first leader to simulate it crashing (as it just aborts the thread)
    drop(leader);

    // Wait for the other election to return for MIN_TTL + 3s of wiggle room
    let leader2 = tokio::time::timeout(MIN_TTL + Duration::from_secs(3), leader2)
        .await
        .expect("Second leader should have finished election")
        .expect("Error when awaiting task")
        .expect("Shouldn't have errored when electing");

    let leader_id = get_leader_key(&context)
        .await
        .expect("Leader key should be correct after TTL expiry");
    assert_eq!(
        leader_id, "live-leader",
        "Leader value should be the correct ID"
    );

    // Cleanup for next test
    leader2
        .step_down()
        .await
        .expect("Should be able to step down");
}

async fn get_leader_key(context: &Context) -> Option<String> {
    context
        .get_key_value(ELECTION_BUCKET)
        .await
        .expect("Leader KV bucket should exist")
        .get("leader")
        .await
        .expect("Unable to query bucket")
        .map(|raw| String::from_utf8(raw).unwrap())
}
