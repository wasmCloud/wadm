use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::Utc;

use wadm::{
    events::ProviderInfo,
    storage::{
        nats_kv::NatsKvStore, Component, Host, Provider, ProviderStatus, ReadStore,
        Store as WadmStore, WadmComponentInfo,
    },
};

mod helpers;

use helpers::{create_test_store_with_client, setup_env};

#[tokio::test]
async fn test_round_trip() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let store =
        NatsKvStore::new(create_test_store_with_client("round_trip_test", nats_client).await);

    let lattice_id = "roundtrip";

    let component1 = Component {
        id: "testcomponent".to_string(),
        name: "Test Component".to_string(),
        issuer: "afakekey".to_string(),
        instances: HashMap::from([(
            "testhost".to_string(),
            HashSet::from_iter([WadmComponentInfo {
                count: 1,
                annotations: BTreeMap::new(),
            }]),
        )]),
        reference: "fake.oci.repo/testcomponent:0.1.0".to_string(),
    };

    let component2 = Component {
        id: "anothercomponent".to_string(),
        name: "Another Component".to_string(),
        issuer: "afakekey".to_string(),
        instances: HashMap::from([(
            "testhost".to_string(),
            HashSet::from_iter([WadmComponentInfo {
                count: 1,
                annotations: BTreeMap::new(),
            }]),
        )]),
        reference: "fake.oci.repo/anothercomponent:0.1.0".to_string(),
    };

    let host = Host {
        components: HashMap::from([("testcomponent".to_string(), 1)]),
        id: "testhost".to_string(),
        providers: HashSet::from([ProviderInfo {
            provider_id: "testprovider".to_string(),
            provider_ref: "fake.oci.repo/testprovider:0.1.0".to_string(),
            annotations: BTreeMap::new(),
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
        reference: "fake.oci.repo/testprovider:0.1.0".to_string(),
        hosts: [("testhost".to_string(), ProviderStatus::default())].into(),
    };

    store
        .store(lattice_id, host.id.clone(), host.clone())
        .await
        .expect("Should be able to store a host");

    let provider_id = &provider.id;
    store
        .store(lattice_id, provider_id.clone(), provider.clone())
        .await
        .expect("Should be able to store a provider");

    store
        .store(lattice_id, component1.id.clone(), component1.clone())
        .await
        .expect("Should be able to store component");

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
        .get(lattice_id, provider_id)
        .await
        .expect("Unable to fetch stored provider")
        .expect("Provider should exist");
    assert_eq!(
        stored_provider.name, provider.name,
        "Provider should be correct"
    );

    let stored_component: Component = store
        .get(lattice_id, &component1.id)
        .await
        .expect("Unable to fetch stored component")
        .expect("Component should exist");
    assert_eq!(
        stored_component.name, component1.name,
        "Component should be correct"
    );

    // Add something to the state and then fetch it to make sure it updated properly
    store
        .store(lattice_id, component2.id.clone(), component2.clone())
        .await
        .expect("Should be able to add a new component");

    let all_components = store
        .list::<Component>(lattice_id)
        .await
        .expect("Should be able to get all components");

    assert_eq!(
        all_components.len(),
        2,
        "Should have found the correct number of components"
    );
    assert!(
        all_components.contains_key(&component1.id),
        "Should have found component with id {}",
        component1.id
    );
    assert!(
        all_components.contains_key(&component2.id),
        "Should have found component with id {}",
        component2.id
    );

    // Delete one of the components and make sure the data is correct
    store
        .delete::<Component>(lattice_id, &component1.id)
        .await
        .expect("Should be able to delete a component");

    let all_components = store
        .list::<Component>(lattice_id)
        .await
        .expect("Should be able to get all components");

    assert_eq!(
        all_components.len(),
        1,
        "Should have found the correct number of components"
    );
    assert!(
        !all_components.contains_key(&component1.id),
        "Should not have found component with id {}",
        component1.id
    );
}

#[tokio::test]
async fn test_no_data() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let store = NatsKvStore::new(create_test_store_with_client("nodata_test", nats_client).await);

    let lattice_id = "nodata";

    assert!(
        store
            .get::<Component>(lattice_id, "doesnotexist")
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
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let store =
        NatsKvStore::new(create_test_store_with_client("multiple_lattice_test", nats_client).await);

    let lattice_id1 = "multiple_lattice";
    let lattice_id2 = "other_lattice";
    let component1 = Component {
        id: "testcomponent".to_string(),
        name: "Test Component".to_string(),
        issuer: "afakekey".to_string(),
        instances: HashMap::from([(
            "testhost".to_string(),
            HashSet::from_iter([WadmComponentInfo {
                count: 1,
                annotations: BTreeMap::new(),
            }]),
        )]),
        reference: "fake.oci.repo/testcomponent:0.1.0".to_string(),
    };

    let component2 = Component {
        id: "anothercomponent".to_string(),
        name: "Another Component".to_string(),
        issuer: "afakekey".to_string(),
        instances: HashMap::from([(
            "testhost".to_string(),
            HashSet::from_iter([WadmComponentInfo {
                count: 1,
                annotations: BTreeMap::new(),
            }]),
        )]),
        reference: "fake.oci.repo/anothercomponent:0.1.0".to_string(),
    };

    // Store both components first with the different lattice id
    store
        .store(lattice_id1, component1.id.clone(), component1.clone())
        .await
        .expect("Should be able to store data");
    store
        .store(lattice_id2, component2.id.clone(), component2.clone())
        .await
        .expect("Should be able to store data");

    let first = store
        .list::<Component>(lattice_id1)
        .await
        .expect("Should be able to list data");
    assert_eq!(
        first.len(),
        1,
        "First lattice should have exactly 1 component"
    );
    let component = first
        .get(&component1.id)
        .expect("First lattice should have the right component");
    assert_eq!(
        component.name, component1.name,
        "Should have returned the correct component"
    );

    let second = store
        .list::<Component>(lattice_id2)
        .await
        .expect("Should be able to list data");
    assert_eq!(
        second.len(),
        1,
        "Second lattice should have exactly 1 component"
    );
    let component = second
        .get(&component2.id)
        .expect("Second lattice should have the right component");
    assert_eq!(
        component.name, component2.name,
        "Should have returned the correct component"
    );
}

#[tokio::test]
async fn test_store_and_delete_many() {
    let env = setup_env()
        .await
        .expect("should have set up the test environment");
    let nats_client = env
        .nats_client()
        .await
        .expect("should have created a nats client");
    let store =
        NatsKvStore::new(create_test_store_with_client("store_many_test", nats_client).await);

    let lattice_id = "storemany";

    let component1 = Component {
        id: "testcomponent".to_string(),
        name: "Test Component".to_string(),
        issuer: "afakekey".to_string(),
        instances: HashMap::from([(
            "testhost".to_string(),
            HashSet::from_iter([WadmComponentInfo {
                count: 1,
                annotations: BTreeMap::new(),
            }]),
        )]),
        reference: "fake.oci.repo/testcomponent:0.1.0".to_string(),
    };

    let component2 = Component {
        id: "anothercomponent".to_string(),
        name: "Another Component".to_string(),
        issuer: "afakekey".to_string(),
        instances: HashMap::from([(
            "testhost".to_string(),
            HashSet::from_iter([WadmComponentInfo {
                count: 1,
                annotations: BTreeMap::new(),
            }]),
        )]),
        reference: "fake.oci.repo/anothercomponent:0.1.0".to_string(),
    };

    store
        .store_many(
            lattice_id,
            [
                (component1.id.clone(), component1.clone()),
                (component2.id.clone(), component2.clone()),
            ],
        )
        .await
        .expect("Should be able to store multiple components");

    let all_components = store
        .list::<Component>(lattice_id)
        .await
        .expect("Should be able to get all components");

    assert_eq!(
        all_components.len(),
        2,
        "Should have found the correct number of components"
    );
    assert!(
        all_components.contains_key(&component1.id),
        "Should have found component with id {}",
        component1.id
    );
    assert!(
        all_components.contains_key(&component2.id),
        "Should have found component with id {}",
        component2.id
    );

    // Now try to delete them all
    store
        .delete_many::<Component, _, _>(lattice_id, [&component1.id, &component2.id])
        .await
        .expect("Should be able to delete many");

    // Double check that the list is empty now
    let all_components = store
        .list::<Component>(lattice_id)
        .await
        .expect("Should be able to get all components");

    assert!(
        all_components.is_empty(),
        "All components should have no items"
    );
}
