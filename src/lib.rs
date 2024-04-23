use std::time::Duration;

#[cfg(feature = "full")]
pub mod commands;
#[cfg(feature = "full")]
pub mod consumers;
#[cfg(feature = "full")]
pub mod events;
#[cfg(feature = "full")]
pub mod mirror;
pub mod model;
#[cfg(feature = "full")]
pub mod nats_utils;
#[cfg(feature = "full")]
pub mod publisher;
#[cfg(feature = "full")]
pub mod scaler;
#[cfg(feature = "full")]
pub mod server;
#[cfg(feature = "full")]
pub mod storage;
#[cfg(feature = "full")]
pub mod workers;

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
/// The default listen topic for the merged wadm events stream. This topic is an amalgamation of
/// wasmbus.evt topics plus the wadm.internal topics
pub const DEFAULT_WADM_EVENTS_TOPIC: &str = "wadm.evt.*";
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
