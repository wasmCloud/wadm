use anyhow::Result;
use async_trait::async_trait;

use crate::{commands::Command, events::Event};

mod simplescaler;
pub mod spreadscaler;

/// A trait describing a struct that can be configured to compute the difference between
/// desired state and configured state, returning a set of commands to approach desired state.
///
/// Implementers of this trait can choose how to access state, but it's generally recommended to
/// use a [ReadStore](crate::storage::ReadStore) so that it can retrieve current information about
/// state using a common trait that only allows store access and not modification
///
/// Typically a Scaler should be configured with `update_config`, then use the `reconcile` method
/// for an inital set of commands. As events change the state, they should also be given to the Scaler
/// to determine if actions need to be taken in response to an event
#[async_trait]
pub trait Scaler {
    type Config: Send + Sync;

    /// Provide a scaler with configuration to use internally when computing commands
    /// This should trigger a reconcile with the new configuration
    async fn update_config(&mut self, config: Self::Config) -> Result<Vec<Command>>;

    /// Compute commands that must be taken given an event that changes the lattice state
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>>;

    /// Compute commands that must be taken to achieve desired state as specified in config
    async fn reconcile(&self) -> Result<Vec<Command>>;
}
