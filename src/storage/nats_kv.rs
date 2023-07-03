//! Storage engine backed by NATS KV
//!
//! This storage engine enables storing any WADM related state information using NATS KV as a
//! backend
//!
//! ## Data Structure
//!
//! Currently this provider stores state serialized as JSON, though we reserve the right to change
//! the encoding in the future. Because of this, DO NOT depend on accessing this data other than
//! through this module
//!
//! All data is currently stored in a single encoded map per type (host, actor, provider), where the
//! keys are the ID as given by [`StateId::id`]. Once again, we reserve the right to change this
//! structure in the future
use std::collections::HashMap;
use std::io::Error as IoError;

use async_nats::{
    jetstream::kv::{Operation, Store as KvStore},
    Error as NatsError,
};
use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use tracing::{debug, error, field::Empty, instrument, trace};
use tracing_futures::Instrument;

use super::{ReadStore, StateKind, Store};

/// Errors that can be encountered by NATS KV Store implemenation
#[derive(Debug, thiserror::Error)]
pub enum NatsStoreError {
    /// An I/O error occured
    #[error("I/O Error: {0}")]
    Io(#[from] IoError),

    /// An error occured when performing a NATS operation
    #[error("NATS error: {0:?}")]
    Nats(#[from] NatsError),

    /// Errors that result from serializing or deserializing the data from the store
    #[error("Error when encoding or decoding data to store: {0}")]
    SerDe(#[from] serde_json::Error),

    /// A catch all error for uenxpected non-fatal errors
    #[error("{0}")]
    Other(String),
}

/// A [`Store`] implementation backed by NATS KV.
#[derive(Debug, Clone)]
pub struct NatsKvStore {
    store: KvStore,
}

impl NatsKvStore {
    /// Returns a new [`Store`] implementation backed by the given KV NATS bucket
    pub fn new(store: KvStore) -> NatsKvStore {
        NatsKvStore { store }
    }

    /// Returns the map of the data with the revision of the current data
    async fn internal_list<T>(
        &self,
        lattice_id: &str,
    ) -> Result<(HashMap<String, T>, u64), NatsStoreError>
    where
        T: DeserializeOwned + StateKind,
    {
        let key = generate_key::<T>(lattice_id);
        tracing::Span::current().record("key", &key);
        debug!("Fetching data from store");
        match self.store.entry(key).await {
            Ok(Some(entry)) if !matches!(entry.operation, Operation::Delete | Operation::Purge) => {
                trace!(len = %entry.value.len(), "Fetched bytes from store...deserializing");
                serde_json::from_slice::<'_, HashMap<String, T>>(&entry.value)
                    .map(|d| (d, entry.revision))
                    .map_err(NatsStoreError::from)
            }
            // If it was a delete entry, we still need to return the revision
            Ok(Some(entry)) => {
                trace!("Data was deleted, returning last revision");
                debug!("No data found for key, returning empty");
                Ok((HashMap::with_capacity(0), entry.revision))
            }
            Ok(None) => {
                debug!("No data found for key, returning empty");
                Ok((HashMap::with_capacity(0), 0))
            }
            Err(e) => Err(NatsStoreError::Nats(e.into())),
        }
    }
}

// NOTE(thomastaylor312): This implementation should be good enough to start. If we need to optimize
// this for large multitenant deployments, the easiest things to start with would be to swap out the
// default hashes in our HashMaps/Sets to use something like `ahash` (which can help with
// deserialization, see this blog post for more details:
// https://morestina.net/blog/1843/the-stable-hashmap-trap). We can also swap out encoding formats
// to something like bincode or cbor and focus on things like avoiding allocations
#[async_trait]
impl ReadStore for NatsKvStore {
    type Error = NatsStoreError;

    /// Get the state for the specified kind with the given ID.
    ///
    /// The ID can vary depending on the type, but should be the unique ID for the object (e.g. a
    /// host key)
    #[instrument(level = "debug", skip(self))]
    async fn get<T>(&self, lattice_id: &str, id: &str) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        let mut all_data = self.list::<T>(lattice_id).await?;

        // Just pop out the owned data since we are gonna drop
        Ok(all_data.remove(id))
    }

    /// Returns a map of all items of the given type.
    ///
    /// The map key is the value as given by [`StateId::id`]
    #[instrument(level = "debug", skip(self), fields(key = Empty))]
    async fn list<T>(&self, lattice_id: &str) -> Result<HashMap<String, T>, Self::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        let key = generate_key::<T>(lattice_id);
        tracing::Span::current().record("key", &key);
        debug!("Fetching data from store");
        self.internal_list::<T>(lattice_id)
            .in_current_span()
            .await
            .map(|(data, _)| data)
    }
}

#[async_trait]
impl Store for NatsKvStore {
    /// Store multiple items of the same type. This should overwrite existing state entries. This
    /// allows for stores to perform multiple writes simultaneously or to leverage transactions
    ///
    /// The given data can be anything that can be turned into an iterator of (key, value). This
    /// means you can pass a [`HashMap`](std::collections::HashMap) or something like
    /// `["key".to_string(), Actor{...}]`
    ///
    /// This function has several required bounds. It needs to be serialize and deserialize because
    /// some implementations will need to deserialize the current data before modifying it.
    /// [`StateKind`] is needed in order to store and access the data correctly,
    /// and, lastly, [`Send`] is needed because this is an async function and the data needs to be
    /// sendable between threads
    #[instrument(level = "debug", skip(self, data), fields(key = Empty))]
    async fn store_many<T, D>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send,
        D: IntoIterator<Item = (String, T)> + Send,
    {
        let key = generate_key::<T>(lattice_id);
        tracing::Span::current().record("key", &key);
        let (mut current_data, revision) = self
            .internal_list::<T>(lattice_id)
            .in_current_span()
            .await?;
        debug!("Updating data in store");
        for (id, item) in data.into_iter() {
            if current_data.insert(id, item).is_some() {
                // NOTE: We may want to return the old data in the future. For now, keeping it simple
                trace!("Replaced existing data");
            } else {
                trace!("Inserted new entry");
            };
        }
        let serialized = serde_json::to_vec(&current_data)?;
        // NOTE(thomastaylor312): This could not matter, but because this is JSON and not consuming
        // the data it is serializing, we are now holding a vec of the serialized data and the
        // actual struct in memory. So this drops it immediately to hopefully keep memory usage down
        // on busy servers
        drop(current_data);
        trace!(len = serialized.len(), "Writing bytes to store");
        self.store
            .update(key, serialized.into(), revision)
            .await
            .map(|_| ())
            .map_err(|e| NatsStoreError::Nats(e.into()))
    }

    #[instrument(level = "debug", skip(self, data), fields(key = Empty))]
    async fn delete_many<T, D, K>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send,
        D: IntoIterator<Item = K> + Send,
        K: AsRef<str>,
    {
        let key = generate_key::<T>(lattice_id);
        tracing::Span::current().record("key", &key);
        let (mut current_data, revision) = self
            .internal_list::<T>(lattice_id)
            .in_current_span()
            .await?;
        debug!("Updating data in store");
        let mut updated = false;
        for id in data.into_iter() {
            if current_data.remove(id.as_ref()).is_some() {
                // NOTE: We may want to return the old data in the future. For now, keeping it simple
                trace!(id = %id.as_ref(), "Removing existing data");
                updated = true;
            } else {
                trace!(id = %id.as_ref(), "ID doesn't exist in store, ignoring");
            };
        }
        // If we updated nothing, return early
        if !updated {
            return Ok(());
        }

        let serialized = serde_json::to_vec(&current_data)?;
        // NOTE(thomastaylor312): This could not matter, but because this is JSON and not consuming
        // the data it is serializing, we are now holding a vec of the serialized data and the
        // actual struct in memory. So this drops it immediately to hopefully keep memory usage down
        // on busy servers
        drop(current_data);
        trace!(len = serialized.len(), "Writing bytes to store");
        self.store
            .update(key, serialized.into(), revision)
            .await
            .map(|_| ())
            .map_err(|e| NatsStoreError::Nats(e.into()))
    }
}

fn generate_key<T: StateKind>(lattice_id: &str) -> String {
    format!("{}_{lattice_id}", T::KIND)
}
