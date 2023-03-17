use std::collections::{HashMap, HashSet};

use async_nats::jetstream::{
    self,
    kv::{Config as KvConfig, Store},
};
use chrono::Utc;

use wadm::{
    events::ProviderInfo,
    storage::{nats_kv::NatsKvStore, Actor, Host, Provider, Store as WadmStore},
};

/// Helper function that sets up a store with the given ID as its name. This ID should be unique per
/// test
async fn create_test_store(id: String) -> Store {
    let client = async_nats::connect("127.0.0.1:4222")
        .await
        .expect("Should be able to connect to NATS");

    let context = jetstream::new(client);

    // First make sure we clean up the store. We can't just do a cleanup on `Drop` because
    // `tokio::test` uses a single threaded runtime and blocks forever and it isn't really worth
    // spinning up more cores to handle this. We don't care about the result because it could not
    // exist
    let _ = context.delete_key_value(&id).await;

    context
        .create_key_value(KvConfig {
            bucket: id,
            history: 1,
            num_replicas: 1,
            storage: jetstream::stream::StorageType::Memory,
            ..Default::default()
        })
        .await
        .expect("Unable to create KV bucket")
}

#[tokio::test]
async fn test_round_trip() {
    let store = NatsKvStore::new(create_test_store("round_trip_test".to_string()).await);

    let lattice_id = "roundtrip";

    let actor1 = Actor {
        id: "testactor".to_string(),
        name: "Test Actor".to_string(),
        capabilities: vec!["wasmcloud:httpserver".to_string()],
        issuer: "afakekey".to_string(),
        count: 1,
        reference: "fake.oci.repo/testactor:0.1.0".to_string(),
        ..Default::default()
    };

    let actor2 = Actor {
        id: "anotheractor".to_string(),
        name: "Another Actor".to_string(),
        capabilities: vec!["wasmcloud:httpserver".to_string()],
        issuer: "afakekey".to_string(),
        count: 1,
        reference: "fake.oci.repo/anotheractor:0.1.0".to_string(),
        ..Default::default()
    };

    let host = Host {
        actors: HashMap::from([("testactor".to_string(), 1)]),
        id: "testhost".to_string(),
        providers: HashSet::from([ProviderInfo {
            public_key: "testprovider".to_string(),
            contract_id: "wasmcloud:httpserver".to_owned(),
            link_name: "default".to_owned(),
        }]),
        friendly_name: "test-host".to_string(),
        uptime_seconds: 30,
        last_seen: Utc::now(),
        ..Default::default()
    };

    let provider = Provider {
        id: "testprovider".to_string(),
        name: "Test Provider".to_string(),
        issuer: "afakekey".to_string(),
        contract_id: "wasmcloud:httpserver".to_string(),
        reference: "fake.oci.repo/testprovider:0.1.0".to_string(),
        link_name: "default".to_string(),
    };

    store
        .store(lattice_id, host.id.clone(), host.clone())
        .await
        .expect("Should be able to store a host");

    let provider_id =
        wadm::storage::provider_id(&provider.id, &provider.link_name, &provider.contract_id);
    store
        .store(lattice_id, provider_id.clone(), provider.clone())
        .await
        .expect("Should be able to store a provider");

    store
        .store(lattice_id, actor1.id.clone(), actor1.clone())
        .await
        .expect("Should be able to store actor");

    // Now test we can retrieve all the data properly
    let stored_host: Host = store
        .get(lattice_id, &host.id)
        .await
        .expect("Unable to fetch stored host")
        .expect("Host should exist");
    assert_eq!(
        stored_host.friendly_name, host.friendly_name,
        "Host should be correct"
    );

    let stored_provider: Provider = store
        .get(lattice_id, &provider_id)
        .await
        .expect("Unable to fetch stored provider")
        .expect("Provider should exist");
    assert_eq!(
        stored_provider.name, provider.name,
        "Provider should be correct"
    );

    let stored_actor: Actor = store
        .get(lattice_id, &actor1.id)
        .await
        .expect("Unable to fetch stored actor")
        .expect("Actor should exist");
    assert_eq!(stored_actor.name, actor1.name, "Actor should be correct");

    // Add something to the state and then fetch it to make sure it updated properly
    store
        .store(lattice_id, actor2.id.clone(), actor2.clone())
        .await
        .expect("Should be able to add a new actor");

    let all_actors = store
        .list::<Actor>(lattice_id)
        .await
        .expect("Should be able to get all actors");

    assert_eq!(
        all_actors.len(),
        2,
        "Should have found the correct number of actors"
    );
    assert!(
        all_actors.contains_key(&actor1.id),
        "Should have found actor with id {}",
        actor1.id
    );
    assert!(
        all_actors.contains_key(&actor2.id),
        "Should have found actor with id {}",
        actor2.id
    );

    // Delete one of the actors and make sure the data is correct
    store
        .delete::<Actor>(lattice_id, &actor1.id)
        .await
        .expect("Should be able to delete an actor");

    let all_actors = store
        .list::<Actor>(lattice_id)
        .await
        .expect("Should be able to get all actors");

    assert_eq!(
        all_actors.len(),
        1,
        "Should have found the correct number of actors"
    );
    assert!(
        !all_actors.contains_key(&actor1.id),
        "Should not have found actor with id {}",
        actor1.id
    );
}

#[tokio::test]
async fn test_no_data() {
    let store = NatsKvStore::new(create_test_store("nodata_test".to_string()).await);

    let lattice_id = "nodata";

    assert!(
        store
            .get::<Actor>(lattice_id, "doesnotexist")
            .await
            .expect("Should be able to query store")
            .is_none(),
        "Should get None for missing data"
    );

    let all = store
        .list::<Provider>(lattice_id)
        .await
        .expect("Should not error when fetching list");
    assert!(
        all.is_empty(),
        "An empty hash map should be returned when no data is present"
    );

    store
        .delete::<Host>(lattice_id, "doesnotexist")
        .await
        .expect("Should be able to delete something that is non-existent without an error");
}

#[tokio::test]
async fn test_multiple_lattice() {
    let store = NatsKvStore::new(create_test_store("multiple_lattice_test".to_string()).await);

    let lattice_id1 = "multiple_lattice";
    let lattice_id2 = "other_lattice";
    let actor1 = Actor {
        id: "testactor".to_string(),
        name: "Test Actor".to_string(),
        capabilities: vec!["wasmcloud:httpserver".to_string()],
        issuer: "afakekey".to_string(),
        count: 1,
        reference: "fake.oci.repo/testactor:0.1.0".to_string(),
        ..Default::default()
    };

    let actor2 = Actor {
        id: "anotheractor".to_string(),
        name: "Another Actor".to_string(),
        capabilities: vec!["wasmcloud:httpserver".to_string()],
        issuer: "afakekey".to_string(),
        count: 1,
        reference: "fake.oci.repo/anotheractor:0.1.0".to_string(),
        ..Default::default()
    };

    // Store both actors first with the different lattice id
    store
        .store(lattice_id1, actor1.id.clone(), actor1.clone())
        .await
        .expect("Should be able to store data");
    store
        .store(lattice_id2, actor2.id.clone(), actor2.clone())
        .await
        .expect("Should be able to store data");

    let first = store
        .list::<Actor>(lattice_id1)
        .await
        .expect("Should be able to list data");
    assert_eq!(first.len(), 1, "First lattice should have exactly 1 actor");
    let actor = first
        .get(&actor1.id)
        .expect("First lattice should have the right actor");
    assert_eq!(
        actor.name, actor1.name,
        "Should have returned the correct actor"
    );

    let second = store
        .list::<Actor>(lattice_id2)
        .await
        .expect("Should be able to list data");
    assert_eq!(
        second.len(),
        1,
        "Second lattice should have exactly 1 actor"
    );
    let actor = second
        .get(&actor2.id)
        .expect("Second lattice should have the right actor");
    assert_eq!(
        actor.name, actor2.name,
        "Should have returned the correct actor"
    );
}
