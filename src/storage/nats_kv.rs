//! Storage engine backed by NATS Kv
//!
//! This storage engine enables storing any WADM related information using NATS Kv as a backend

use async_trait::async_trait;
use std::io::Error as IoError;
use std::path::PathBuf;

use async_nats::{
    jetstream::kv::Config as JetstreamKvConfig, jetstream::kv::Store as JetstreamKvStore,
    Client as NatsClient, ConnectOptions as NatsConnectOptions,
};

use tokio::fs::read as read_file;

use semver::{BuildMetadata, Prerelease, Version};
use serde::ser::StdError;
use serde::{Deserialize, Serialize};
use serde_json::{from_slice, to_vec, Error as SerdeJsonError};

use super::{LatticeStorage, StorageEngineError, StorageError, StoreOptions};

use super::state::LatticeState;

use crate::storage::{Store, WithStateMetadata};

const STORAGE_ENGINE_NAME: &str = "nats_kv";
const STORAGE_ENGINE_VERSION: Version = Version {
    major: 0,
    minor: 1,
    patch: 0,
    pre: Prerelease::EMPTY,
    build: BuildMetadata::EMPTY,
};

const LATTICE_BUCKET_STATE_KEY: &str = "state";

impl From<SerdeJsonError> for StorageError {
    fn from(e: SerdeJsonError) -> Self {
        StorageError::Unknown(format!("de/serialization error occurred: {e}"))
    }
}

impl From<Box<dyn StdError + std::marker::Send + Sync>> for StorageError {
    fn from(e: Box<dyn StdError + std::marker::Send + Sync>) -> Self {
        StorageError::Unknown(format!("de/serialization error occurred: {e}"))
    }
}

//////////
// NATS //
//////////

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct NatsAuthConfig {
    pub creds_file: Option<String>,
    pub jwt_seed: Option<String>,
    pub jwt_path: Option<PathBuf>,
}

/// Build NATS connection options
async fn build_nats_options(
    cfg: &NatsAuthConfig,
) -> Result<NatsConnectOptions, StorageEngineError> {
    match cfg {
        // Use JWT + seed if present
        NatsAuthConfig {
            jwt_path: Some(path),
            jwt_seed: Some(seed),
            ..
        } => {
            let file_contents = read_file(path).await?;
            let jwt = String::from_utf8_lossy(&file_contents);
            let kp = std::sync::Arc::new(nkeys::KeyPair::from_seed(seed)?);

            Ok(async_nats::ConnectOptions::with_jwt(
                jwt.into(),
                move |nonce| {
                    let key_pair = kp.clone();
                    async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
                },
            ))
        }

        // Use creds file if present
        NatsAuthConfig {
            creds_file: Some(path),
            ..
        } => Ok(
            NatsConnectOptions::with_credentials_file(path.clone().into())
                .await
                .map_err(EngineError::from)?,
        ),

        _ => Ok(NatsConnectOptions::default()),
    }
}

////////////
// Engine //
////////////

/// Engine errors that can be encountered by NatsKvStorageEngine
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The connection to NATS could not be made
    #[error("Faield to connect to Nats: {0}")]
    NatsConnectionFailed(#[from] async_nats::Error),

    /// An I/O error
    #[error("I/O Error: {0}")]
    Io(#[from] IoError),

    /// A catch all error for uenxpected non-fatal errors
    #[error("{0}")]
    Other(String),
}

impl From<EngineError> for StorageEngineError {
    fn from(e: EngineError) -> StorageEngineError {
        StorageEngineError::Engine(Box::new(e))
    }
}

impl From<nkeys::error::Error> for StorageEngineError {
    fn from(err: nkeys::error::Error) -> StorageEngineError {
        StorageEngineError::Engine(Box::new(format!("failed to process nkeys: {err}")))
    }
}

impl From<std::io::Error> for StorageEngineError {
    fn from(err: std::io::Error) -> StorageEngineError {
        StorageEngineError::Engine(Box::new(format!("unexpected I/O error: {err}")))
    }
}

#[derive(Debug)]
pub struct NatsKvStorageEngine {
    /// Configuration for the NATS KV storage engine
    config: NatsKvStorageConfig,

    /// NATS client
    client: NatsClient,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NatsKvStorageConfig {
    /// URL to the nats instance
    pub nats_url: String,

    /// Prefix to use with lattice buckets
    pub lattice_bucket_prefix: Option<String>,

    /// Authentication config for use with NATS
    pub auth: Option<NatsAuthConfig>,
}

impl NatsKvStorageEngine {
    pub async fn new(
        config: NatsKvStorageConfig,
    ) -> Result<NatsKvStorageEngine, StorageEngineError> {
        let nats_config = match &config.auth {
            Some(ac) => build_nats_options(ac).await?,
            None => NatsConnectOptions::default(),
        };

        let client = async_nats::connect_with_options(&config.nats_url, nats_config)
            .await
            .map_err(|e| StorageEngineError::Engine(Box::new(format!("failed to connect: {e}"))))?;

        Ok(NatsKvStorageEngine { config, client })
    }

    /// Build the top level bucket for the lattice
    fn build_lattice_bucket_name(&self, lattice_id: String) -> String {
        let mut bucket = format!("lattice_{lattice_id}");
        if let Some(prefix) = &self.config.lattice_bucket_prefix {
            bucket = format!("{prefix}_{bucket}");
        }
        bucket
    }

    /// Get or create the KV store for NATS jetstream
    async fn get_or_create_kv_store(
        &self,
        bucket: String,
    ) -> Result<JetstreamKvStore, StorageError> {
        // Build jetstream client
        let jetstream = async_nats::jetstream::new(self.client.clone());

        // Use existing KV bucket if already present
        if let Ok(kv) = jetstream.get_key_value(&bucket).await {
            return Ok(kv);
        }

        // Create Jetstream KV store
        let kv = jetstream
            .create_key_value(JetstreamKvConfig {
                bucket,
                history: 10,
                ..Default::default()
            })
            .await
            .map_err(|e| StorageError::Engine(format!("failed to create jetstream kv: {e}")))?;

        Ok(kv)
    }

    /// Read a value out of the store
    async fn read(&self, id: String) -> Result<WithStateMetadata<LatticeState>, StorageError> {
        let bucket = self.build_lattice_bucket_name(id);
        let kv = self.get_or_create_kv_store(bucket.clone()).await?;

        let lattice;
        match kv.entry(LATTICE_BUCKET_STATE_KEY).await? {
            Some(entry) => {
                lattice = from_slice(&entry.value)?;
                Ok(WithStateMetadata {
                    state: lattice,
                    revision: Some(entry.revision),
                })
            }
            None => Err(StorageError::Engine(format!(
                "No lattice state in bucket [{bucket}]"
            ))),
        }
    }

    /// Write a value to the store
    async fn write(
        &mut self,
        obj: &LatticeState,
        id: Option<String>,
        opts: StoreOptions,
    ) -> Result<WithStateMetadata<String>, StorageError> {
        let id = id.unwrap_or(obj.id.clone());
        let bucket = self.build_lattice_bucket_name(id.clone());
        let kv = self.get_or_create_kv_store(bucket.clone()).await?;

        let revision = match opts {
            // Compare and set semantics
            StoreOptions {
                require_cas: true,
                revision: Some(rev),
                ..
            } => {
                kv.update(LATTICE_BUCKET_STATE_KEY, to_vec(&obj)?.into(), rev)
                    .await?
            }

            // Best-effort semantics
            _ => {
                kv.put(LATTICE_BUCKET_STATE_KEY, to_vec(&obj)?.into())
                    .await?
            }
        };

        Ok(WithStateMetadata {
            state: id,
            revision: Some(revision),
        })
    }
}

#[async_trait]
impl Store for NatsKvStorageEngine {
    type State = LatticeState;
    type StateId = String;

    async fn name() -> String {
        String::from(STORAGE_ENGINE_NAME)
    }

    async fn version() -> Version {
        STORAGE_ENGINE_VERSION
    }

    async fn get(&self, id: String) -> Result<WithStateMetadata<LatticeState>, StorageError> {
        Ok(self.read(id).await?)
    }

    async fn store(
        &mut self,
        state: LatticeState,
        store_opts: StoreOptions,
    ) -> Result<WithStateMetadata<String>, StorageError> {
        Ok(self.write(&state, None, store_opts).await?)
    }

    // Delete an existing piece of state
    async fn delete(&self, id: String) -> Result<(), StorageError> {
        // Retrieve the kv store for a given lattice
        let bucket = self.build_lattice_bucket_name(id);
        let kv = self.get_or_create_kv_store(bucket.clone()).await?;

        let _ = kv.delete(bucket).await.map_err(|err| {
            StorageError::Engine(format!("failed to delete lattice state: {err}"))
        })?;

        Ok(())
    }
}

impl LatticeStorage for NatsKvStorageEngine {}
