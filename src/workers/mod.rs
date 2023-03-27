//! Various [`Worker`](crate::consumers::manager::Worker) implementations for reconciling and
//! handling events and commands. These are essentially the default things that drive work forward
//! in wadm

mod event;

pub use event::EventWorker;
