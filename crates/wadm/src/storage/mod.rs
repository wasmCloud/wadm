use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, ops::Deref};

pub mod nats_kv;
pub mod reaper;
pub(crate) mod snapshot;
mod state;

pub use state::{Component, Host, Provider, ProviderStatus, WadmComponentInfo};

/// A trait that must be implemented with a unique identifier for the given type. This is used in
/// the construction of keys for a store
pub trait StateKind {
    /// The type name of this storable state
    const KIND: &'static str;
}

#[async_trait]
pub trait ReadStore {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Get the state for the specified kind with the given ID. Returns None if it doesn't exist
    ///
    /// The ID can vary depending on the type, but should be the unique ID for the object (e.g. a
    /// host key)
    async fn get<T>(&self, lattice_id: &str, id: &str) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + StateKind;

    /// Returns a map of all items of the given type.
    ///
    /// The map key is the value as given by [`StateId::id`]
    async fn list<T>(&self, lattice_id: &str) -> Result<HashMap<String, T>, Self::Error>
    where
        T: DeserializeOwned + StateKind;
}

/// A trait that indicates the ability of a struct to store state
///
/// Internals of how to validate state (such as compare and swap semantics) are left up to
/// implementors and should not be the concern of consumers of any given store
// NOTE(thomastaylor312): To our future selves: I originally had a trait called `StateIdentity` that
// I used rather than having the consumer generate the key. However, that seemed to be a bit
// overkill for now, and it creates a bunch of extra work. Any type that needs to be used as an ID
// would need to implement the type and it could lead to the annoying issue of needing to define a
// struct just to get the ID. If we need the type guarantees in the future, we can always add it in
#[async_trait]
pub trait Store: ReadStore {
    /// Store a piece of state with the given ID. This should overwrite existing state entries
    ///
    /// By default this will just call [`Store::store_many`] with a single item in the list of data
    async fn store<T>(&self, lattice_id: &str, id: String, data: T) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync + Clone, // Needs to be clone in order to retry updates
    {
        self.store_many(lattice_id, [(id, data)]).await
    }

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
    async fn store_many<T, D>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync + Clone, // Needs to be clone in order to retry updates
        D: IntoIterator<Item = (String, T)> + Send;

    /// Delete a state entry
    ///
    /// By default this will just call [`Store::delete_many`] with a single item in the list of data
    async fn delete<T>(&self, lattice_id: &str, id: &str) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync,
    {
        self.delete_many::<T, _, _>(lattice_id, [id]).await
    }

    /// Delete multiple state entries. This allows for stores to perform multiple delete
    /// simultaneously or to leverage transactions. This is for advanced use cases
    ///
    /// The given data can be anything that can be turned into an iterator of (key, value). This
    /// means you can pass a [`Vec`](std::collections::Vec) or something like `["key"]`
    ///
    /// This function has several required bounds. It needs to be serialize and deserialize because
    /// some implementations will need to deserialize the current data before modifying it.
    /// [`StateKind`] is needed in order to store and access the data correctly, and, lastly,
    /// [`Send`] is needed because this is an async function and the modified data needs to be
    /// sendable between threads
    async fn delete_many<T, D, K>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync,
        D: IntoIterator<Item = K> + Send,
        K: AsRef<str>;
}

// Helper for making sure you can wrap any non-clonable store in an Arc
#[async_trait]
impl<S: Store + Send + Sync> Store for std::sync::Arc<S> {
    async fn store_many<T, D>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync + Clone,
        D: IntoIterator<Item = (String, T)> + Send,
    {
        self.as_ref().store_many(lattice_id, data).await
    }

    async fn delete_many<T, D, K>(&self, lattice_id: &str, data: D) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync,
        D: IntoIterator<Item = K> + Send,
        K: AsRef<str>,
    {
        self.as_ref().delete_many::<T, _, _>(lattice_id, data).await
    }
}

#[async_trait]
impl<S: Store + Send + Sync> ReadStore for std::sync::Arc<S> {
    type Error = S::Error;

    async fn get<T>(&self, lattice_id: &str, id: &str) -> Result<Option<T>, Self::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        self.as_ref().get(lattice_id, id).await
    }

    async fn list<T>(&self, lattice_id: &str) -> Result<HashMap<String, T>, Self::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        self.as_ref().list(lattice_id).await
    }
}

/// Scoped store is a convenience wrapper around a store that automatically passes the given
/// lattice_id to [`Store`] implementations on every call
pub struct ScopedStore<S> {
    lattice_id: String,
    inner: S,
}

impl<S> AsRef<S> for ScopedStore<S> {
    fn as_ref(&self) -> &S {
        &self.inner
    }
}

impl<S> Deref for ScopedStore<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<S: Clone> Clone for ScopedStore<S> {
    fn clone(&self) -> Self {
        ScopedStore {
            lattice_id: self.lattice_id.clone(),
            inner: self.inner.clone(),
        }
    }
}

impl<S: Store + Sync> ScopedStore<S> {
    /// Creates a new store scoped to the given lattice ID
    pub fn new(lattice_id: &str, store: S) -> ScopedStore<S> {
        ScopedStore {
            lattice_id: lattice_id.to_owned(),
            inner: store,
        }
    }

    /// Converts this scoped store back into its original store
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Get the state for the specified kind with the given ID. Returns None if it doesn't exist
    ///
    /// The ID can vary depending on the type, but should be the unique ID for the object (e.g. a
    /// host key)
    pub async fn get<T>(&self, id: &str) -> Result<Option<T>, S::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        self.inner.get(&self.lattice_id, id).await
    }

    /// Returns a map of all items of the given type.
    ///
    /// The map key is the value as given by [`StateId::id`]
    pub async fn list<T>(&self) -> Result<HashMap<String, T>, S::Error>
    where
        T: DeserializeOwned + StateKind,
    {
        self.inner.list(&self.lattice_id).await
    }

    /// Store a piece of state. This should overwrite existing state entries
    pub async fn store<T>(&self, id: String, data: T) -> Result<(), S::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync + Clone,
    {
        self.inner.store(&self.lattice_id, id, data).await
    }

    /// Store multiple items of the same type. This should overwrite existing state entries. This
    /// allows for stores to perform multiple writes simultaneously or to leverage transactions
    pub async fn store_many<T, D>(&self, data: D) -> Result<(), S::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync + Clone,
        D: IntoIterator<Item = (String, T)> + Send,
    {
        self.inner.store_many(&self.lattice_id, data).await
    }

    /// Delete a state entry
    pub async fn delete<T>(&self, id: &str) -> Result<(), S::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync,
    {
        self.inner.delete::<T>(&self.lattice_id, id).await
    }

    /// Delete multiple state entries. This allows for stores to perform multiple delete
    /// simultaneously or to leverage transactions
    pub async fn delete_many<T, D, K>(&self, data: D) -> Result<(), S::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send + Sync,
        D: IntoIterator<Item = K> + Send,
        K: AsRef<str>,
    {
        self.inner
            .delete_many::<T, _, _>(&self.lattice_id, data)
            .await
    }
}
