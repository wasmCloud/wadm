use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashSet;

use crate::{commands::Command, events::Event, storage::ReadStore};

mod simplescaler;

/// A trait describing a struct that can be configured to compute the difference between
/// desired state and configured state, returning a set of commands to approach desired state.
///
/// State is given to a scaler using a [ReadStore](crate::storage::ReadStore) so that it can retrieve
/// current information from the proper store without changing state directly.
///
/// Typically a Scaler should be configured with `update_config`, then use the `reconcile` method
/// for an inital set of commands. As events change the state, they should also be given to the Scaler
/// to determine if actions need to be taken in response to an event
#[async_trait]
pub trait Scaler {
    type Config: Send + Sync;

    /// Provide a scaler with configuration to use internally when computing commands
    fn update_config(&mut self, config: Self::Config) -> Result<bool>;

    /// Compute commands that must be taken given an event that changes the lattice state
    async fn handle_event<S: ReadStore + Send + Sync>(
        &self,
        store: S,
        event: Event,
    ) -> Result<HashSet<Command>>;

    /// Compute commands that must be taken to achieve desired state as specified in config
    async fn reconcile<S: ReadStore + Send + Sync>(&self, store: S) -> Result<HashSet<Command>>;
}