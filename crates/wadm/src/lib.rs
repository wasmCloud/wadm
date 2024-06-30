use std::time::Duration;

pub mod commands;
pub mod consumers;
pub mod events;
pub mod nats_utils;
pub mod publisher;
pub mod scaler;
pub mod server;
pub mod storage;
pub mod workers;

pub(crate) mod model;
#[cfg(test)]
pub mod test_util;

/// Default amount of time events should stay in the stream. This is the 2x heartbeat interval, plus
/// some wiggle room. Exported to make setting defaults easy
pub const DEFAULT_EXPIRY_TIME: Duration = Duration::from_secs(70);
/// Default topic to listen to for all lattice events
pub const DEFAULT_EVENTS_TOPIC: &str = "wasmbus.evt.*.>";
/// Default topic to listen to for all lattice events in a multitenant deployment
pub const DEFAULT_MULTITENANT_EVENTS_TOPIC: &str = "*.wasmbus.evt.*.>";
/// Default topic to listen to for all commands
pub const DEFAULT_COMMANDS_TOPIC: &str = "wadm.cmd.*";
/// Default topic to listen to for all status updates. wadm.status.<lattice_id>.<manifest_name>
pub const DEFAULT_STATUS_TOPIC: &str = "wadm.status.*.*";
/// Default topic to listen to for all wadm event updates
pub const DEFAULT_WADM_EVENTS_TOPIC: &str = "wadm.evt.*.>";
/// Default internal wadm event consumer listen topic for the merged wadm and wasmbus events stream.
pub const DEFAULT_WADM_EVENT_CONSUMER_TOPIC: &str = "wadm_event_consumer.evt.*.>";
/// Managed by annotation used for labeling things properly in wadm
pub const MANAGED_BY_ANNOTATION: &str = "wasmcloud.dev/managed-by";
/// Identifier for managed by annotation. This is the value [`MANAGED_BY_ANNOTATION`] is set to
pub const MANAGED_BY_IDENTIFIER: &str = "wadm";
/// An annotation that denotes which model a resource belongs to
pub const APP_SPEC_ANNOTATION: &str = "wasmcloud.dev/appspec";
/// An annotation that denotes which scaler is managing a resource
pub const SCALER_KEY: &str = "wasmcloud.dev/scaler";
/// The default link name. In the future, this will likely be pulled in from another crate
pub const DEFAULT_LINK_NAME: &str = "default";
