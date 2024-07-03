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
//! All data is currently stored in a single encoded map per type (host, component, provider), where
//! the keys are the ID as given by [`StateId::id`]. Once again, we reserve the right to change this
//! structure in the future
use std::collections::HashMap;
use std::io::Error as IoError;
use std::time::Duration;

use async_nats::{
    jetstream::kv::{Operation, Store as KvStore},
    Error as NatsError,
};
use async_trait::async_trait;
use futures::Future;
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

    /// Helper that retries update operations
    // NOTE(thomastaylor312): We could probably make this even better with some exponential backoff,
    // but this is easy enough for now since generally there isn't a ton of competition for updating
    // a single lattice
    async fn update_with_retries<T, F, Fut>(
        &self,
        lattice_id: &str,
        key: &str,
        timeout: Duration,
        updater: F,
    ) -> Result<(), NatsStoreError>
    where
        T: Serialize + DeserializeOwned + StateKind + Send,
        F: Fn(HashMap<String, T>) -> Fut,
        Fut: Future<Output = Result<Vec<u8>, NatsStoreError>>,
    {
        let res = tokio::time::timeout(timeout, async {
            loop {
                let (current_data, revision) = self
                    .internal_list::<T>(lattice_id)
                    .in_current_span()
                    .await?;
                debug!(revision, "Updating data in store");
                let updated_data = updater(current_data).await?;
                trace!("Writing bytes to store");
                // If the function doesn't return any data (such as for deletes), just return early.
                // Everything is an update (right now), even for deletes so the only case we'd have
                // an empty vec is if we aren't updating anything
                if updated_data.is_empty() {
                    return Ok(())
                }
                match self.store.update(key, updated_data.into(), revision).await {
                    Ok(_) => return Ok(()),
                    Err(e) => {
                        if e.to_string().contains("wrong last sequence") {
                            debug!(%key, %lattice_id, "Got wrong last sequence when trying to update state. Retrying update operation");
                            continue;
                        }
                        return Err(NatsStoreError::Nats(e.into()));
                    }
                    // TODO(#316): Uncomment this code once we can update to the latest
                    // async-nats, which actually allows us to access the inner source of the error
                    // Err(e) => {
                    //     let source = match e.source() {
                    //         Some(s) => s,
                    //         None => return Err(NatsStoreError::Nats(e.into())),
                    //     };
                    //     match source.downcast_ref::<PublishError>() {
                    //         Some(e) if matches!(e.kind(), PublishErrorKind::WrongLastSequence) => {
                    //             debug!(%key, %lattice_id, "Got wrong last sequence when trying to update state. Retrying update operation");
                    //             continue;
                    //         },
                    //         _ => return Err(NatsStoreError::Nats(e.into())),
                    //     }
                    // }
                }
            }
        })
        .await;
        match res {
            Err(_e) => Err(NatsStoreError::Other(
                "Timed out while retrying updates to key".to_string(),
            )),
            Ok(res2) => res2,
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
    /// `["key".to_string(), Component{...}]`
    ///
    /// This function has several required bounds. It needs to be serialize and deserialize because
    /// some implementations will need to deserialize the current data before modifying it.
    /// [`StateKind`] is needed in order to store and access the data correctly,
    /// and, lastly, [`Send`] is needed because this is an async function and the data needs to be
    /// sendable between threads
    #[instrument(level = "debug", skip(self, data), fields(key = Empty))]
    async fn store_many<T, D>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync + Clone,
        D: IntoIterator<Item = (String, T)> + Send,
    {
        let key = generate_key::<T>(lattice_id);
        tracing::Span::current().record("key", &key);
        let data: Vec<(String, T)> = data.into_iter().collect();
        self.update_with_retries(
            lattice_id,
            &key,
            Duration::from_millis(1500),
            |mut current_data| async {
                let cloned = data.clone();
                async move {
                    for (id, item) in cloned.into_iter() {
                        if current_data.insert(id, item).is_some() {
                            // NOTE: We may want to return the old data in the future. For now, keeping it simple
                            trace!("Replaced existing data");
                        } else {
                            trace!("Inserted new entry");
                        };
                    }
                    serde_json::to_vec(&current_data).map_err(NatsStoreError::SerDe)
                }
                .await
            },
        )
        .in_current_span()
        .await
    }

    #[instrument(level = "debug", skip(self, data), fields(key = Empty))]
    async fn delete_many<T, D, K>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync,
        D: IntoIterator<Item = K> + Send,
        K: AsRef<str>,
    {
        let key = generate_key::<T>(lattice_id);
        tracing::Span::current().record("key", &key);

        let data: Vec<String> = data.into_iter().map(|s| s.as_ref().to_string()).collect();
        self.update_with_retries(
            lattice_id,
            &key,
            Duration::from_millis(1500),
            |mut current_data: HashMap<String, T>| async {
                let cloned = data.clone();
                async move {
                    let mut updated = false;
                    for id in cloned.into_iter() {
                        if current_data.remove(&id).is_some() {
                            // NOTE: We may want to return the old data in the future. For now, keeping it simple
                            trace!(%id, "Removing existing data");
                            updated = true;
                        } else {
                            trace!(%id, "ID doesn't exist in store, ignoring");
                        };
                    }
                    // If we updated nothing, return early
                    if !updated {
                        return Ok(Vec::with_capacity(0));
                    }

                    serde_json::to_vec(&current_data).map_err(NatsStoreError::SerDe)
                }
                .await
            },
        )
        .in_current_span()
        .await
    }
}

fn generate_key<T: StateKind>(lattice_id: &str) -> String {
    format!("{}_{lattice_id}", T::KIND)
}
