use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use std::{collections::HashMap, ops::Deref};

pub mod nats_kv;
mod state;

pub use state::{Actor, Host, Provider};

/// A trait that must be implemented with a unique identifier for the given type. This is used in
/// the construction of keys for a store
pub trait StateKind {
    /// The type name of this storable state
    const KIND: &'static str;
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
pub trait Store {
    type Error: std::error::Error;

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

    /// Store a piece of state with the given ID. This should overwrite existing state entries
    ///
    /// This function has several required bounds. It needs to be serialize and deserialize because
    /// some implementations will need to deserialize the current data before modifying it.
    /// [`StateKind`] is needed in order to store and access the data correctly,
    /// and, lastly, [`Send`] is needed because this is an async function and the data needs to be
    /// sendable between threads
    async fn store<T>(&self, lattice_id: &str, id: String, data: T) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send;

    /// Delete an existing piece of state
    ///
    /// This function has several required bounds. It needs to be serialize and deserialize because
    /// some implementations will need to deserialize the current data before modifying it.
    /// [`StateKind`] is needed in order to store and access the data correctly, and, lastly,
    /// [`Send`] is needed because this is an async function and the modified data needs to be
    /// sendable between threads
    async fn delete<T>(&self, lattice_id: &str, id: &str) -> Result<(), Self::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send;
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

impl<S: Store> ScopedStore<S> {
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
        T: Serialize + DeserializeOwned + StateKind + Send,
    {
        self.inner.store(&self.lattice_id, id, data).await
    }

    /// Delete an existing piece of state
    pub async fn delete<T>(&self, id: &str) -> Result<(), S::Error>
    where
        T: Serialize + DeserializeOwned + StateKind + Send,
    {
        self.inner.delete::<T>(&self.lattice_id, id).await
    }
}

/// A helper function for generating a unique ID for any given provider. This is exposed purely to
/// be a common way of creating a key to access/store provider information
pub fn provider_id(public_key: &str, link_name: &str, contract_id: &str) -> String {
    format!("{}/{}/{}", public_key, link_name, contract_id)
}
