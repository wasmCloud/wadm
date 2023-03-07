use async_trait::async_trait;
use semver::Version;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use thiserror::Error;

mod nats_kv;
mod state;

pub use nats_kv::{NatsAuthConfig, NatsKvStorageConfig, NatsKvStorageEngine};
pub use state::{Actor, Claim, Host, LatticeParameters, LatticeState, Provider};

#[derive(Debug, Serialize, Deserialize)]
pub struct WithStateMetadata<T> {
    /// Revision (if supported), often used for compare-and-set-semantics
    pub revision: Option<u64>,

    /// The state that was returned
    pub state: T,
}

/////////////
// Engines //
/////////////

/// Pluggable engines can be asked for their metadata
/// to enable generic (possibly dyn) usage
#[async_trait]
pub trait EngineMetadata {
    /// Get the name of the lattice storage
    async fn name() -> String;

    /// Get the version of the lattice storage
    async fn version() -> Version;
}

/////////////////////
// Storage Engines //
/////////////////////

/// Errors that are related to storage engines
#[derive(Debug, Error)]
pub enum StorageEngineError {
    /// When the underlying engine throws an error
    #[error("Storage engine encountered an error: {0:?}")]
    Engine(Box<dyn Debug + Send + Sync>),

    /// An error when the cause is unknown. This happens very rarely and should be considered fatal
    #[error("Unknown error has occured")]
    Unknown,
}

/////////////
// Storage //
/////////////

/// Errors that are related to storage
#[derive(Debug, Error)]
pub enum StorageError {
    /// Errors that arise at the engine layer
    #[error("Storage error: {0}")]
    Engine(String),

    /// Errors that arise at the network layer
    #[error("Network error: {0}")]
    NetworkError(String),

    /// An error when the cause is unknown. This happens very rarely and should be considered fatal
    #[error("Unknown error has occured: {0}")]
    Unknown(String),
}

/// Options and metadata that can be used to improve store operations
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct StoreOptions {
    /// Whether to force a store with compare-and-set semantics
    pub require_cas: bool,

    /// version identifier that enables stores to perform compare-and-set semantics
    pub revision: Option<u64>,
}

/// A trait that indicates the ability of a struct to store state
/// Objects that implement Store can save some given State, identifiable by StateId, and updatable with the given StateBuilder
///
#[async_trait]
pub trait Store {
    type State;
    type StateId;

    /// Get a particular piece of state
    async fn get(&self, id: Self::StateId) -> Result<WithStateMetadata<Self::State>, StorageError>;

    /// Store a piece of state
    async fn store(
        &mut self,
        state: Self::State,
        opts: StoreOptions,
    ) -> Result<WithStateMetadata<Self::StateId>, StorageError>;

    /// Delete an existing piece of state
    async fn delete(&self, id: String) -> Result<(), StorageError>;
}

/// LatticeStorages are state stores that support storing lattice information
pub trait LatticeStorage: Store<State = LatticeState, StateId = String> {}
