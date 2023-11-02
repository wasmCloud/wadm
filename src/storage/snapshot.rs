use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use wasmcloud_control_interface::LinkDefinition;

use crate::storage::{Actor, Host, Provider, ReadStore, StateKind};
use crate::workers::LinkSource;

// NOTE(thomastaylor312): This type is real ugly and we should probably find a better way to
// structure the ReadStore trait so it doesn't have the generic T we have to work around here. This
// is essentially a map of "state kind" -> map of ID to partially serialized state. I did try to
// implement some sort of getter trait but it has to be generic across T
type InMemoryData = HashMap<String, HashMap<String, serde_json::Value>>;

/// A store and claims/links source implementation that contains a static snapshot of the data that
/// can be refreshed periodically. Please note that this is scoped to a specific lattice ID and
/// should be constructed separately for each lattice ID.
///
/// NOTE: This is a temporary workaround until we get a proper caching store in place
pub struct SnapshotStore<S, L> {
    store: S,
    link_source: L,
    lattice_id: String,
    stored_state: Arc<RwLock<InMemoryData>>,
    links: Arc<RwLock<Vec<LinkDefinition>>>,
}

impl<S, L> Clone for SnapshotStore<S, L>
where
    S: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
            link_source: self.link_source.clone(),
            lattice_id: self.lattice_id.clone(),
            stored_state: self.stored_state.clone(),
            links: self.links.clone(),
        }
    }
}

impl<S, L> SnapshotStore<S, L>
where
    S: ReadStore,
    L: LinkSource,
{
    /// Creates a new snapshot store that is scoped to the given lattice ID
    pub fn new(store: S, link_source: L, lattice_id: String) -> Self {
        Self {
            store,
            link_source,
            lattice_id,
            stored_state: Default::default(),
            links: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Refreshes the snapshotted data, returning an error if it couldn't update the data
    pub async fn refresh(&self) -> anyhow::Result<()> {
        // SAFETY: All of these unwraps are safe because we _just_ deserialized from JSON
        let providers = self
            .store
            .list::<Provider>(&self.lattice_id)
            .await?
            .into_iter()
            .map(|(key, val)| (key, serde_json::to_value(val).unwrap()))
            .collect::<HashMap<_, _>>();
        let actors = self
            .store
            .list::<Actor>(&self.lattice_id)
            .await?
            .into_iter()
            .map(|(key, val)| (key, serde_json::to_value(val).unwrap()))
            .collect::<HashMap<_, _>>();
        let hosts = self
            .store
            .list::<Host>(&self.lattice_id)
            .await?
            .into_iter()
            .map(|(key, val)| (key, serde_json::to_value(val).unwrap()))
            .collect::<HashMap<_, _>>();
        let links = self.link_source.get_links().await?;

        {
            let mut stored_state = self.stored_state.write().await;
            stored_state.insert(Provider::KIND.to_owned(), providers);
            stored_state.insert(Actor::KIND.to_owned(), actors);
            stored_state.insert(Host::KIND.to_owned(), hosts);
        }

        *self.links.write().await = links;

        Ok(())
    }
}

#[async_trait::async_trait]
impl<S, L> ReadStore for SnapshotStore<S, L>
where
    // NOTE(thomastaylor312): We need this bound so we can pass through the error type.
    S: ReadStore + Send + Sync,
    L: Send + Sync,
{
    type Error = S::Error;

    // NOTE(thomastaylor312): See other note about the generic T above, but this is hardcore lolsob
    async fn get<T>(&self, _lattice_id: &str, id: &str) -> Result<Option<T>, Self::Error>
    where
        T: serde::de::DeserializeOwned + StateKind,
    {
        Ok(self
            .stored_state
            .read()
            .await
            .get(T::KIND)
            .and_then(|data| {
                data.get(id).map(|data| {
                    serde_json::from_value::<T>(data.clone()).expect(
                        "Failed to deserialize data from snapshot, this is programmer error",
                    )
                })
            }))
    }

    async fn list<T>(&self, _lattice_id: &str) -> Result<HashMap<String, T>, Self::Error>
    where
        T: serde::de::DeserializeOwned + StateKind,
    {
        Ok(self
            .stored_state
            .read()
            .await
            .get(T::KIND)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(|(key, val)| {
                (
                    key,
                    serde_json::from_value::<T>(val).expect(
                        "Failed to deserialize data from snapshot, this is programmer error",
                    ),
                )
            })
            .collect())
    }
}

#[async_trait::async_trait]
impl<S, L> LinkSource for SnapshotStore<S, L>
where
    S: Send + Sync,
    L: Send + Sync,
{
    async fn get_links(&self) -> anyhow::Result<Vec<LinkDefinition>> {
        Ok(self.links.read().await.clone())
    }
}
