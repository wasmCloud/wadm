use std::fmt::Debug;
use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use async_nats::jetstream::stream::Stream as NatsStream;
use futures::{Stream, StreamExt};
use tokio::{
    sync::{RwLock, Semaphore},
    task::JoinHandle,
};
use tracing::{error, instrument, trace, warn, Instrument};

use super::{CreateConsumer, ScopedMessage};

/// A convenience type for returning work results
pub type WorkResult<T> = Result<T, WorkError>;
type WorkHandles = Arc<RwLock<HashMap<String, JoinHandle<WorkResult<()>>>>>;

/// An error that describes possible work failures when performing actions based on incoming messages
#[derive(Debug, thiserror::Error)]
pub enum WorkError {
    /// A consumer has stopped returning work in its stream and should be restarted
    #[error("Consumer has stopped work")]
    ConsumerStopped,
    /// Returned when the pool of permits has closed, which means work has stopped. This is not
    /// generally contructed by consumers of the crate
    #[error("Work pool has closed, unable to keep working")]
    WorkPoolClosed,
    /// A fatal error, generally returned by a [`Worker`] if it experiences some sort of failure it
    /// can't recover from. Should include the underlying error that caused the failure
    #[error(transparent)]
    Fatal(Box<dyn std::error::Error + Send>),
    /// An error occured when interacting with NATS
    #[error(transparent)]
    NatsError(#[from] async_nats::Error),
    /// A catch all error for non-described errors that are not fatal
    #[error(transparent)]
    Other(Box<dyn std::error::Error + Send>),
}

impl WorkError {
    /// Convenience method for turning any error message into a fatal error
    pub fn into_fatal(e: impl std::error::Error + Send + 'static) -> WorkError {
        WorkError::Fatal(Box::new(e))
    }

    /// Convenience method for turning any error message into an `Other` error
    pub fn into_other(e: impl std::error::Error + Send + 'static) -> WorkError {
        WorkError::Other(Box::new(e))
    }
}

impl From<tokio::sync::AcquireError> for WorkError {
    fn from(_value: tokio::sync::AcquireError) -> Self {
        WorkError::WorkPoolClosed
    }
}

/// Any type that can perform work based on a given [`ScopedMessage`]. This worker should contain
/// all state (or handles to state) that it needs to complete its work
#[async_trait::async_trait]
pub trait Worker {
    /// The actual message type to expect, such as [`Event`](crate::events::Event)
    type Message: Debug + Send;
    /// Process the given work to completion. Almost all errors returned are things that could be
    /// retried. But if for some reason a fatal error occurs, return `WorkError::Fatal` to indicate
    /// that work should stop. Any worker MUST handle acking the message (or passing it to another
    /// worker). By default, when a [`ScopedMessage`] is dropped, it will nack it
    async fn do_work(&self, message: ScopedMessage<Self::Message>) -> WorkResult<()>;
}

/// A trait used for dynamically creating workers.
///
/// This is mostly available as a workaround so that a manager can create a worker for a lattice
/// when reconciling. See the main wadm binary code for an example of how to do this
#[async_trait::async_trait]
pub trait WorkerCreator {
    type Output: Worker + Send + Sync + 'static;

    async fn create(&self, lattice_id: &str) -> anyhow::Result<Self::Output>;
}

/// A manager of a specific type of Consumer that handles giving out permits to work and managing
/// per lattice consumers.
///
/// This type is easily clonable and sharable as it is only cloning a reference to the underlying
/// types.
///
/// NOTE: We use a work permit semaphore pool here to make sure in large, multi-tenant deployments,
/// we aren't trying to simultaneously handle every single lattice event and command consumer
#[derive(Clone)]
pub struct ConsumerManager<C> {
    handles: WorkHandles,
    permits: Arc<Semaphore>,
    stream: NatsStream,
    phantom: PhantomData<C>,
}

impl<C> ConsumerManager<C> {
    /// Returns a new consumer manager set up to use the given permit pool. This meant to use a
    /// shared pool of permits with other consumer managers to manage the amount of simultaneous
    /// work, so the Semaphore must be wrapped in an [`Arc`].
    ///
    /// This function will attempt to populate this itself with all existing consumers. Any errors
    /// that occur during population will only log and not error out as it is recoverable. Because
    /// of this, it requires something that can generate the desired worker
    pub async fn new<W, F>(
        permit_pool: Arc<Semaphore>,
        stream: NatsStream,
        worker_generator: F,
    ) -> ConsumerManager<C>
    where
        W: Worker + Send + Sync + 'static,
        C: Stream<Item = Result<ScopedMessage<W::Message>, async_nats::Error>>
            + CreateConsumer<Output = C>
            + Send
            + Unpin
            + 'static,
        F: WorkerCreator<Output = W>,
    {
        let mut manager = ConsumerManager {
            handles: Arc::new(RwLock::new(HashMap::default())),
            permits: permit_pool,
            stream,
            phantom: PhantomData,
        };

        let handles: HashMap<String, JoinHandle<WorkResult<()>>> = manager
            .stream
            .consumers()
            .filter_map(|res| async {
                let info = match res {
                    Ok(info) => info,
                    Err(e) => {
                        error!(error = %e, "Error when trying to read current consumers");
                        return None;
                    }
                };
                // TODO: This is somewhat brittle as we could change naming schemes, but it is
                // good enough for now. We are just taking the name (which should be of the
                // format `<consumer_prefix>_<lattice_id>`), but this makes sure we are always
                // getting the last thing in case of other underscores
                let lattice_id = match info.name.split('_').last() {
                    Some(id) => id,
                    None => return None,
                };
                // NOTE(thomastaylor312): It might be nicer for logs if we add an extra param for a
                // friendly consumer manager name
                trace!(%lattice_id, subject = %info.config.filter_subject, "Adding consumer for lattice");

                let worker = match worker_generator.create(lattice_id).await {
                    Ok(w) => w,
                    Err(e) => {
                        error!(error = %e, %lattice_id, "Unable to add consumer for lattice. Error when generating worker");
                        return None;
                    }
                };

                match manager.spawn_handler(&info.config.filter_subject, lattice_id, worker).await {
                    Ok(handle) => Some((info.config.filter_subject.to_owned(), handle)),
                    Err(e) => {
                        error!(error = %e, %lattice_id, "Unable to add consumer for lattice");
                        None
                    }
                }
            })
            .collect()
            .await;

        manager.handles = Arc::new(RwLock::new(handles));
        manager
    }

    /// Starts a new consumer for the given topic. This method will only fail if there was an error
    /// setting up the consumer.
    ///
    /// The given work function should attempt to handle the event
    #[instrument(level = "trace", skip(self, worker))]
    pub async fn add_for_lattice<W>(
        &self,
        topic: &str,
        lattice_id: &str,
        worker: W,
    ) -> Result<(), async_nats::Error>
    where
        W: Worker + Send + Sync + 'static,
        C: Stream<Item = Result<ScopedMessage<W::Message>, async_nats::Error>>
            + CreateConsumer<Output = C>
            + Send
            + Unpin
            + 'static,
    {
        if !self.has_consumer(topic).await {
            trace!("Adding new consumer");
            let handle = self.spawn_handler(topic, lattice_id, worker).await?;
            let mut handles = self.handles.write().await;
            handles.insert(topic.to_owned(), handle);
        }
        Ok(())
    }

    async fn spawn_handler<W>(
        &self,
        topic: &str,
        lattice_id: &str,
        worker: W,
    ) -> Result<JoinHandle<WorkResult<()>>, async_nats::Error>
    where
        W: Worker + Send + Sync + 'static,
        C: Stream<Item = Result<ScopedMessage<W::Message>, async_nats::Error>>
            + CreateConsumer<Output = C>
            + Send
            + Unpin
            + 'static,
    {
        let consumer = C::create(self.stream.clone(), topic, lattice_id).await?;
        let permits = self.permits.clone();
        Ok(tokio::spawn(work_fn(consumer, permits, worker).instrument(
            tracing::info_span!("consumer_worker", %topic, worker_type = %std::any::type_name::<W>()),
        )))
    }

    /// Checks if this manager has a consumer for the given topic. Returns `false` if it doesn't
    /// exist or has stopped
    pub async fn has_consumer(&self, topic: &str) -> bool {
        self.handles
            .read()
            .await
            .get(topic)
            .map(|handle| {
                let is_finished = handle.is_finished();
                if is_finished {
                    warn!(%topic, "Work function stopped executing for topic")
                }
                !is_finished
            })
            .unwrap_or(false)
    }

    // NOTE(thomastaylor312): We could add a supervisory element to this by starting a notifier
    // thread that can restart work if a fatal error is received (or a join handle finishes), but
    // that is not necessary now
}

async fn work_fn<C, W>(mut consumer: C, permits: Arc<Semaphore>, worker: W) -> WorkResult<()>
where
    W: Worker + Send,
    C: Stream<Item = Result<ScopedMessage<W::Message>, async_nats::Error>> + Unpin,
{
    loop {
        // Grab a permit to do some work. This will only return errors if the pool is closed
        trace!("Getting work permit");
        let _permit = permits.acquire().await?;
        trace!("Received work permit, attempting to pull from consumer");
        // Get next value from stream, returning error if the consumer stopped
        let res = consumer.next().await.ok_or(WorkError::ConsumerStopped)?;
        let res = match res {
            Ok(msg) => {
                trace!(message = ?msg, "Got message from consumer");
                worker.do_work(msg).await
            }
            Err(e) => {
                error!(error = %e, "Got error from stream when reading from consumer. Will try again");
                continue;
            }
        };
        match res {
            // Return fatal errors if they occur
            Err(e) if matches!(e, WorkError::Fatal(_)) => return Err(e),
            // For the rest of the errors, right now we just log. Could do nicer retry behavior as this evolves
            Err(e) => error!(error = ?e, "Got error from worker"),
            _ => (),
        }
    }
}
