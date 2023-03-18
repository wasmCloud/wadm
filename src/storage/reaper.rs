//! Contains helpers for reaping Hosts that haven't received a heartbeat within a configured amount
//! of time

use std::collections::HashMap;

use chrono::{Duration, Utc};
use tokio::{task::JoinHandle, time};
use tracing::{debug, error, info, instrument, trace};

use super::{Host, Store};

/// A struct that can reap various pieces of data from the given store
pub struct Reaper<S> {
    store: S,
    interval: Duration,
    handles: HashMap<String, JoinHandle<()>>,
}

impl<S: Store + Clone + Send + Sync + 'static> Reaper<S> {
    /// Creates a new reaper using the given store configured to check for reaping every
    /// `check_interval` for all passed lattice IDs. This reaper will immediately begin executing
    /// spawned tasks. When the reaper is dropped, it will stop polling all tasks. This function
    /// will panic if you pass it a duration that is larger than the maximum value accepted by the
    /// `chrono` library. As this is a rare case, we don't actually return an error an panic instead
    ///
    /// The reaper will wait for 2 * `check_interval` before removing anything. For example, if
    /// `check_interval` is set to 30s, then after 30s, the item is considered to be in a "warning"
    /// state. This isn't actually reflected in state right now, but it will be logged. When the
    /// next tick fires (around 60s total), then the item will be removed from the store
    pub fn new(
        store: S,
        check_interval: std::time::Duration,
        lattices_to_observe: impl IntoIterator<Item = String>,
    ) -> Reaper<S> {
        let interval = Duration::from_std(check_interval)
            .expect("The given duration is out of bounds for a max duration value");
        let cloned_store = store.clone();
        let handles = lattices_to_observe.into_iter().map(move |id| {
            (
                id.clone(),
                tokio::spawn(reaper_fn(cloned_store.clone(), id, interval)),
            )
        });
        Reaper {
            store,
            interval,
            handles: handles.collect(),
        }
    }

    /// Adds a new lattice to be reaped
    pub fn observe(&mut self, lattice_id: String) {
        self.handles.insert(
            lattice_id.clone(),
            tokio::spawn(reaper_fn(self.store.clone(), lattice_id, self.interval)),
        );
    }

    /// Stops observing the given lattice
    pub fn remove(&mut self, lattice_id: &str) {
        if let Some(handle) = self.handles.remove(lattice_id) {
            handle.abort();
        }
    }
}

#[instrument(level = "debug", skip(store))]
async fn reaper_fn<S: Store>(store: S, lattice_id: String, check_interval: Duration) {
    debug!("Starting reaper");
    // SAFETY: We created this Duration from a std Duration, so it should unwrap back just fine
    let mut ticker = time::interval(check_interval.to_std().unwrap());
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        trace!("Tick fired, fetching host list");
        let nodes = match store.list::<Host>(&lattice_id).await {
            Ok(n) => n,
            Err(e) => {
                error!(error = %e, "Error when fetching hosts from store. Will retry on next tick");
                continue;
            }
        };

        let hosts_to_remove = nodes.into_iter().filter_map(|(id, host)| {
            let elapsed = Utc::now() - host.last_seen;
            if elapsed > (check_interval * 2) {
                info!(%id, friendly_name = %host.friendly_name, "Host has not been seen for 2 intervals. Will reap node");
                Some(id)
            } else if elapsed > check_interval {
                info!(%id, friendly_name = %host.friendly_name, "Host has not been seen for 1 interval. Next check will reap node from store");
                None
            } else {
                None
            }
        });

        // NOTE(thomastaylor): We probably will never be deleting more than a few at a time anyway,
        // so doing this serially is fine. If it does become a problem, we can add a `delete_many`
        // function to `Store`
        for id in hosts_to_remove {
            if let Err(e) = store.delete::<Host>(&lattice_id, &id).await {
                error!(error = %e, host_id = %id, "Error when deleting host from store. Will retry on next tick")
            }
            info!(host_id = %id, "Removed host from store");
        }
        trace!("Completed reap check");
    }
}
