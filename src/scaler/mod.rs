use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::mpsc::Sender;

use crate::{commands::Command, events::Event, model::TraitProperty};

pub mod manager;
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
    /// Provide a scaler with configuration to use internally when computing commands This should
    /// trigger a reconcile with the new configuration.
    ///
    /// This config can be anything that can be turned into a
    /// [`TraitProperty`](crate::model::TraitProperty). Additional configuration outside of what is
    /// available in a `TraitProperty` can be passed when constructing the scaler
    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>>;

    /// Compute commands that must be taken given an event that changes the lattice state
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>>;

    /// Compute commands that must be taken to achieve desired state as specified in config
    async fn reconcile(&self) -> Result<Vec<Command>>;

    /// Returns the list of commands needed to cleanup for a scaler
    ///
    /// This purposefully does not consume the scaler so that if there is a failure it can be kept
    /// around
    async fn cleanup(&self) -> Result<Vec<Command>>;

    /// An optional to implement function that can tell the scaler to enter a backoff period. When
    /// in backoff, a scaler should always return an empty `Vec<Command>` for all operations. Please
    /// be careful when calling backoff until you have sent the commands to their intended location
    /// as otherwise the might not rerun until they are reconciled again.
    ///
    /// This function should return as quickly as possible, spawning a task that can switch the
    /// state back from backoff. The passed in channel is meant to be used to notify the caller that
    /// the backoff is complete with the name of the model. This name can then be used by the caller
    /// to run a reconciliation pass to ensure everything is done. This is intended to workaround
    /// the fact that there is no way to tell when we finished stopping/starting actors
    ///
    /// Please note that we'd like this to be temporary in the long term
    async fn backoff(&self, notifier: Sender<String>);
}
