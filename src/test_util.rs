use std::convert::Infallible;
use std::{collections::HashMap, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::RwLock;
use wasmcloud_control_interface::{HostInventory, LinkDefinition};

use crate::publisher::Publisher;
use crate::storage::StateKind;
use crate::workers::{Claims, ClaimsSource, InventorySource, LinkSource};

fn generate_key<T: StateKind>(lattice_id: &str) -> String {
    format!("{}_{lattice_id}", T::KIND)
}

/// A [`Store`] implementation for use in testing
#[derive(Default)]
pub struct TestStore {
    pub inner: tokio::sync::RwLock<HashMap<String, Vec<u8>>>,
}

#[async_trait::async_trait]
impl crate::storage::ReadStore for TestStore {
    type Error = Infallible;

    async fn get<T>(&self, lattice_id: &str, id: &str) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        let key = generate_key::<T>(lattice_id);
        let mut all: HashMap<String, T> = self
            .inner
            .read()
            .await
            .get(&key)
            .map(|raw| serde_json::from_slice(raw).unwrap())
            .unwrap_or_default();
        // T isn't clone, so I can't use get
        Ok(all.remove(id))
    }

    async fn list<T>(&self, lattice_id: &str) -> Result<HashMap<String, T>, Self::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        let key = generate_key::<T>(lattice_id);
        Ok(self
            .inner
            .read()
            .await
            .get(&key)
            .map(|raw| serde_json::from_slice(raw).unwrap())
            .unwrap_or_default())
    }
}

#[async_trait::async_trait]
impl crate::storage::Store for TestStore {
    async fn store_many<T, D>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send,
        D: IntoIterator<Item = (String, T)> + Send,
    {
        let key = generate_key::<T>(lattice_id);
        let mut all: HashMap<String, T> = self
            .inner
            .read()
            .await
            .get(&key)
            .map(|raw| serde_json::from_slice(raw).unwrap())
            .unwrap_or_default();
        all.extend(data);
        self.inner
            .write()
            .await
            .insert(key, serde_json::to_vec(&all).unwrap());
        Ok(())
    }

    async fn delete_many<T, D, K>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send,
        D: IntoIterator<Item = K> + Send,
        K: AsRef<str>,
    {
        let key = generate_key::<T>(lattice_id);
        let mut all: HashMap<String, T> = self
            .inner
            .read()
            .await
            .get(&key)
            .map(|raw| serde_json::from_slice(raw).unwrap())
            .unwrap_or_default();
        for k in data.into_iter() {
            all.remove(k.as_ref());
        }
        self.inner
            .write()
            .await
            .insert(key, serde_json::to_vec(&all).unwrap());
        Ok(())
    }
}

#[derive(Clone, Default, Debug)]
/// A test "lattice source" for use with testing
pub struct TestLatticeSource {
    pub claims: HashMap<String, Claims>,
    pub inventory: Arc<RwLock<HashMap<String, HostInventory>>>,
    pub links: Vec<LinkDefinition>,
}

#[async_trait::async_trait]
impl ClaimsSource for TestLatticeSource {
    async fn get_claims(&self) -> anyhow::Result<HashMap<String, Claims>> {
        Ok(self.claims.clone())
    }
}

#[async_trait::async_trait]
impl InventorySource for TestLatticeSource {
    async fn get_inventory(&self, host_id: &str) -> anyhow::Result<HostInventory> {
        Ok(self.inventory.read().await.get(host_id).cloned().unwrap())
    }
}

#[async_trait::async_trait]
impl LinkSource for TestLatticeSource {
    async fn get_links(&self) -> anyhow::Result<Vec<LinkDefinition>> {
        Ok(self.links.clone())
    }
}

/// A publisher that does nothing
#[derive(Clone, Default)]
pub struct NoopPublisher;

#[async_trait::async_trait]
impl Publisher for NoopPublisher {
    async fn publish(&self, _: Vec<u8>, _: Option<&str>) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A publisher that records all data sent to it (as the given type deserialized from JSON)
pub struct RecorderPublisher<T> {
    pub received: Arc<RwLock<Vec<T>>>,
}

#[async_trait::async_trait]
impl<T: DeserializeOwned + Send + Sync> Publisher for RecorderPublisher<T> {
    async fn publish(&self, data: Vec<u8>, _: Option<&str>) -> anyhow::Result<()> {
        let data: T = serde_json::from_slice(&data)?;
        self.received.write().await.push(data);
        Ok(())
    }
}
