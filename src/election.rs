//! A leader election library backed by NATS
//!
//! This module implements a basic leader election backed by a NATS KV bucket with a TTL set.
//!
//! ## How to use
//!
//! At its most basic, you can do leader election and run a loop with a single command
//!
//! ```rust,no_run
//! use std::time::Duration;
//!
//! use async_nats::jetstream;
//! use alt_wadm::election::Elector;
//!
//! #[tokio::main]
//! async fn main() {
//!     let context = jetstream::new(
//!         async_nats::connect("127.0.0.1:4222")
//!         .await
//!         .expect("Unable to get nats client"),
//!     );
//!     Elector::new(&context, Duration::from_secs(30), "my-node".to_string())
//!         .await
//!         .unwrap()
//!         .elect_and_run(async move {
//!             // work here
//!         })
//!         .await
//!         .unwrap();
//! }
//! ```
//!
//! If you want more control over leader election, you can return the actual leader and then manage
//! what happens after it exits or your work exits
//!
//! ```rust,no_run
//! use std::time::Duration;
//!
//! use async_nats::jetstream;
//! use alt_wadm::election::Elector;
//!
//! #[tokio::main]
//! async fn main() {
//!     let context = jetstream::new(
//!         async_nats::connect("127.0.0.1:4222")
//!         .await
//!         .expect("Unable to get nats client"),
//!     );
//!     let mut leader = Elector::new(&context, Duration::from_secs(30), "my-node".to_string())
//!         .await
//!         .unwrap()
//!         .elect()
//!         .await
//!         .unwrap();
//!
//!     let my_work = async move {
//!         // work here
//!     };
//!
//!     tokio::select! {
//!         e = leader.notify() => {
//!             // Custom error handling here
//!         }
//!         _ = my_work => {
//!             leader.step_down().await.unwrap();
//!             // Stuff after your work completes
//!         }
//!     }
//! }
//! ```

use std::time::Duration;

use async_nats::jetstream::{
    kv::{Config, Operation, Store},
    Context,
};
use async_nats::Error as NatsError;
use futures::StreamExt;
use thiserror::Error;
use tokio::sync::mpsc::{self, Receiver};
use tracing::{debug, error, info, instrument, trace, warn, Instrument};

/// The name of the KV bucket used for election. This is exported for convenience purposes
pub const ELECTION_BUCKET: &str = "nats_election";
/// The minimum allowable TTL for leader election
pub const MIN_TTL: Duration = Duration::from_secs(15);
const KEY_NAME: &str = "leader";
const TTL_LEEWAY: Duration = Duration::from_secs(5);
const MAX_ERRORS: usize = 5;

/// A convenience alias for results with election errors
pub type ElectionResult<T> = Result<T, ElectionError>;

/// Errors that can occur
#[derive(Debug, Error)]
pub enum ElectionError {
    /// Any NATS specific communication errors that may occur, but do not fall into other error
    /// categories
    #[error("NATS error")]
    Nats(#[from] NatsError),
    /// Errors that occur during step down. These errors are informational only as the leader key
    /// updater has stopped and the leader key will be deleted after the configured TTL value. All
    /// this error indicates is that something happened while trying to delete the key when stepping
    /// down
    #[error("Error when stepping down: {0:?}")]
    StepDown(NatsError),
    /// An error returned from a Leader when it attempts to renew its lease and discovers that
    /// another leader has been elected. This could happen in the case of network delays or some
    /// other sort of hang
    #[error("Lost leadership")]
    LostLeadership,
    /// An error when bad values were passed to the elector
    #[error("Unable to perform election: {0}")]
    Setup(String),
    /// An error when the cause is unknown. This happens very rarely and should be considered fatal
    #[error("Unknown error has occured")]
    Unknown,
}

/// A struct for attempting leader election. Returns a [`Leader`] once elected
pub struct Elector {
    store: Store,
    ttl: Duration,
    id: String,
}

impl Elector {
    /// Returns a new Elector configured with the given ttl for a candidate. This should be the
    /// amount of time before an elected leader expires. The `id` parameter should be a unique name
    /// for this elector (generally something like a hostname or UUID)
    ///
    /// Please note that the minimum TTL value is 15s. An error will be returned if the TTL value is
    /// below that amount or if an empty ID is given. If another error is returned it is likely
    /// unable to create or access the election key value store in NATS, in which case this should
    /// be a fatal error or retried by the caller
    pub async fn new(jetstream: &Context, ttl: Duration, id: String) -> ElectionResult<Elector> {
        // Begin with some validation
        if id.is_empty() {
            return Err(ElectionError::Setup(
                "The elector ID cannot be an empty string".to_string(),
            ));
        } else if ttl < MIN_TTL {
            return Err(ElectionError::Setup("Minimum TTL value is 30s".to_string()));
        }

        // NOTE(thomastaylor312): We have to use a separate bucket for leader election because NATS
        // KV doesn't allow you to set TTL on an individual entry
        let store = match jetstream.get_key_value(ELECTION_BUCKET).await {
            Ok(s) => s,
            Err(e) => {
                // This could be any error, but just try to create the bucket anyway
                info!(error = %e, "Unable to get key value bucket. Attempting to create");
                jetstream
                    .create_key_value(Config {
                        bucket: ELECTION_BUCKET.to_owned(),
                        description: "NATS leader election".to_string(),
                        history: 1,
                        max_age: ttl,
                        // Just giving it 1MB so it can't be abused. Probably only need a few bytes, but
                        // this is small enough
                        max_bytes: 1024,
                        storage: async_nats::jetstream::stream::StorageType::File,
                        ..Default::default()
                    })
                    .await?
            }
        };

        // Make sure the TTL is set to the same as the TTL of the bucket
        let current_ttl = store.status().await?.max_age();

        let ttl = if current_ttl != ttl {
            warn!(new_ttl = ?current_ttl, "Given TTL does not match the TTL for the bucket. Setting TTL to bucket value");
            current_ttl
        } else {
            ttl
        };

        Ok(Elector { store, ttl, id })
    }

    /// Attempt to take leadership. This function will not return until it is elected leader. It
    /// will continuously retry to take the lock if it detects any changes
    #[instrument(level = "info", skip(self), fields(leader_id = %self.id))]
    pub async fn elect(self) -> ElectionResult<Leader> {
        info!("Starting leader election");
        // Try to grab the election. If we don't succeed, start waiting for a deleted key
        loop {
            if put_key(&self.store, &self.id).await?.is_some() {
                break;
            }
            debug!("Other leader found, waiting for changes");
            self.wait_for_delete().await?;
            info!("Saw leader step down, attempting to grab power");
            // Once we wait for a delete, try to put the key again at the top of the loop
        }
        info!("Elected leader");
        // Once we are past the loop, we should be leader, so go ahead and start the updater loop
        Ok(Leader::start(self.store, self.ttl, self.id).await)
    }

    /// Helper that runs the given future once the leader is elected until the future returns or the
    /// leader update loop fails. If you want more control over what happens if an error occurs
    /// during leader election and running a process, use [`elect`](Elector::elect) and leverage
    /// the returned [`Leader`]
    ///
    /// This is meant to be used by the "top level" long running loop of an application. This
    /// consumes the elector and means that no other work should be done once this returns without
    /// performing another election. This can return an error if an error occurs during election or
    /// if the underlying leader update loop aborts with an error
    pub async fn elect_and_run<T, F>(self, fut: F) -> ElectionResult<T>
    where
        F: std::future::Future<Output = T>,
    {
        let leader = self.elect().await?;
        leader.run_until(fut).await
    }

    /// Wait until a delete key event is received
    #[instrument(level = "debug", skip(self), fields(leader_id = %self.id))]
    async fn wait_for_delete(&self) -> ElectionResult<()> {
        // NOTE(thomastaylor312): So it turns out that when a key expires due to TTL, nothing is
        // sent as a watch event. So we have to manually poll every TTL instead like a barbarian.
        // I've still kept the watch for when a step down happens, but this could be simplified if
        // NATS is updated to address this
        let mut ticker = tokio::time::interval(self.ttl);
        // We don't need to burst multiple times if for some reason this gets delayed (which it
        // shouldn't), just try again and continue from there
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        trace!("Beginning watch loop");
        let mut stream_exited = false;
        let mut watch = self.store.watch(KEY_NAME).await?;
        loop {
            // Sometimes the NATS streams just...terminate, so make sure if we have a stream termination, we retry the connection
            if stream_exited {
                stream_exited = false;
                watch = self.store.watch(KEY_NAME).await?;
            }
            tokio::select! {
                biased;

                _ = ticker.tick() => {
                    match self.store.get(KEY_NAME).await {
                        Ok(Some(other_leader)) => {
                            let other_leader = String::from_utf8_lossy(&other_leader);
                            trace!(%other_leader, "Another process is currently leader");
                        }
                        Ok(None) => return Ok(()),
                        Err(e) => {
                            error!(error = %e, "Got error from key request, will wait for next tick");
                            continue;
                        }
                    }
                }

                res = watch.next() => {
                    match res {
                        Some(r) => {
                            match r {
                                Ok(entry) => {
                                    let span = tracing::trace_span!("handle_entry", ?entry);
                                    let _enter = span.enter();
                                    trace!("Got event");
                                    // If it was a delete, return now
                                    if matches!(entry.operation, Operation::Delete | Operation::Purge) {
                                        trace!("Event was a delete operation, returning");
                                        return Ok(());
                                    }
                                    trace!("Event was not delete, continuing");
                                    continue;
                                }
                                Err(e) => {
                                    error!(error = %e, "Got error from key watcher, will wait for next event");
                                    continue;
                                }
                            }
                        }
                        None => {
                            stream_exited = true;
                            continue
                        }
                    }
                }
            }
        }
    }
}

/// A struct that maintains a leadership lock until dropped (or when manually calling `step_down`)
///
/// See module documentation for details examples of how to use
pub struct Leader {
    store: Store,
    id: String,
    handle: Option<tokio::task::JoinHandle<()>>,
    notifier: Receiver<ElectionError>,
}

impl Drop for Leader {
    fn drop(&mut self) {
        trace!("Dropping leader");
        if let Some(handle) = self.handle.take() {
            trace!("Aborting leader election process");
            handle.abort()
        }
    }
}

impl Leader {
    async fn start(store: Store, ttl: Duration, id: String) -> Leader {
        let store_clone = store.clone();
        let leader_id = id.clone();
        let (sender, notifier) = mpsc::channel(1);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(ttl - TTL_LEEWAY);
            // We don't need to burst multiple times if for some reason this gets delayed (which it
            // shouldn't), just try again and continue from there
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut num_errors = 0usize;
            let mut retry_immediately = false;
            loop {
                if !retry_immediately {
                    ticker.tick().await;
                }
                match put_key(&store_clone, &leader_id).await {
                    Ok(Some(_)) => {
                        trace!("Successfully renewed lease");
                        num_errors = 0;
                        retry_immediately = false
                    }
                    Ok(None) => {
                        warn!("No longer leader. Stopping updater process");
                        // If we can't send, there isn't much we can do with the error here, and the
                        // process is going to abort anyway
                        let _ = sender.send(ElectionError::LostLeadership).await;
                        return
                    }
                    Err(e) => {
                        num_errors += 1;
                        if num_errors >= MAX_ERRORS {
                            error!("Reached max number of retries for update. Aborting updater process");
                            // If we can't send, there isn't much we can do with the error here, and the
                            // process is going to abort anyway
                            let _ = sender.send(e).await;
                            return
                        }
                        error!(error = %e, %num_errors, "Got error when renewing lease, will retry immediately");
                        retry_immediately = true;
                    }
                }
            }
        }.instrument(tracing::debug_span!("key_updater", leader_id = %id, ?ttl)));

        Leader {
            store,
            id,
            handle: Some(handle),
            notifier,
        }
    }

    /// Returns an error if the leader lease updater aborts. This is meant to be used with something
    /// like `tokio::select!` to interrupt processing if this is no longer a leader. If you don't
    /// want to manage this yourself, use [`run_until`](Leader::run_until)
    pub async fn notify(&mut self) -> ElectionError {
        self.notifier.recv().await.unwrap_or(ElectionError::Unknown)
    }

    /// Helper that runs the given future until the future returns or the leader update loop fails.
    /// If you want more control over what happens if an error occurs during leader election, use
    /// [`notify`](Leader::notify)
    ///
    /// This is meant to be used by the "top level" long running loop of an application. This
    /// consumes the leader and means that no other work should be done once this returns without
    /// performing another election. If this returns due to leader update failure, it will return an
    /// error
    pub async fn run_until<T, F>(mut self, fut: F) -> ElectionResult<T>
    where
        F: std::future::Future<Output = T>,
    {
        tokio::select! {
            res = fut => {
                if let Err(error) = self.step_down().await {
                    // We don't want to conflate an error that comes from the update process with a
                    // cleanup error, so just warn here
                    warn!(%error, "Got error when trying to step down after job stopped");
                }
                Ok(res)
            }
            e = self.notify() => Err(e)
        }
    }

    /// Step down cleanly as the leader.
    ///
    /// `Leader` will automatically stop updating the leader key if it is dropped, but if you want
    /// an immediate transfer of leadership, it is recommended to call this method
    #[instrument(level = "debug", skip(self), fields(leader_id = %self.id))]
    pub async fn step_down(mut self) -> ElectionResult<()> {
        debug!("Stepping down as leader");
        // First things first, stop updating the lock
        if let Some(handle) = self.handle.take() {
            handle.abort();
            // Then make sure the process has exited, ignoring the error. If we don't do this, we could
            // be in a race condition where the update process sets the key right after we delete it.
            let _ = handle.await;
        }

        trace!("Leader key updater process aborted, checking that we are still leading");

        // Grab the current lock key to make sure it still matches us. There is a chance that
        // something could have happened and now someone else is leader, so let's not try a Coup
        // d'état with no one to take the emperor's place shall we?
        match self
            .store
            .get(KEY_NAME)
            .await
            .map_err(ElectionError::StepDown)?
        {
            Some(d) => {
                let leader = String::from_utf8_lossy(&d);
                if leader != self.id {
                    info!(current_leader = %leader, "Current leader ID does not match, assuming we are no longer leader");
                    return Ok(());
                }
            }
            None => {
                info!("No leader key was found during step down, assuming we are no longer leader");
                return Ok(());
            }
        };
        trace!("Confirmed we are current leader");

        // There is sometimes a weird race condition where something (don't know what) shuts down
        // before the purge is sent to the server, so we set up a watch to wait until we get the
        // purge
        let mut watch = self
            .store
            .watch(KEY_NAME)
            .await
            .map_err(ElectionError::StepDown)?;
        // Now delete the lock
        debug!("Deleting leader lock");
        self.store
            .purge(KEY_NAME)
            .await
            .map_err(ElectionError::StepDown)?;
        trace!("Waiting for purge result");
        while let Some(res) = watch.next().await {
            let entry = res.map_err(ElectionError::StepDown)?;
            if matches!(entry.operation, Operation::Purge) {
                trace!("Found purge result, stopping watch");
                break;
            }
        }
        debug!("Step down complete");
        Ok(())
    }
}

/// Wrapper around common logic for setting the key only if it is the right revision. This ensures
/// we don't end up with a race if someone updates the key with the new value before we do
///
/// Returns Some if the key was updated and `None` otherwise. Errors are only returned if something
/// happened with NATS
#[instrument(level = "trace", skip_all, fields(leader_id = %id))]
async fn put_key(store: &Store, id: &str) -> ElectionResult<Option<()>> {
    let expected_revision = match store.entry(KEY_NAME).await? {
        // Key exists, so make sure it matches the ID before putting. If it doesn't, bail out
        Some(v) => {
            let current_leader = String::from_utf8_lossy(&v.value);
            // Because we are getting the entry, we also return the purge entry. So if we have a
            // quick re-election, it will just cycle getting an empty "purge" entry until the TTL
            // expires it
            if current_leader != id && !matches!(v.operation, Operation::Purge | Operation::Delete)
            {
                trace!(%current_leader, "Current leader is not this one, not updating key");
                return Ok(None);
            }
            // If we got here, we are still the current leader, so return the current revision
            v.revision
        }
        // Key doesn't exist, so revision should be 0
        None => 0,
    };

    if let Err(e) = store
        .update(KEY_NAME, id.as_bytes().to_vec().into(), expected_revision)
        .await
    {
        // NATS errors right now are really opaque, so we just assume that an error means it isn't
        // the right revision. Could probably update this to at least text match on the error
        debug!(error = %e, "Did not update key due to error");
        return Ok(None);
    }
    Ok(Some(()))
}
