use std::time::Duration;

pub mod commands;
pub mod consumers;
pub mod events;
pub mod nats_utils;
pub mod storage;

/// Default amount of time events should stay in the stream. This is the 2x heartbeat interval, plus
/// some wiggle room. Exported to make setting defaults easy
pub const DEFAULT_EXPIRY_TIME: Duration = Duration::from_secs(70);
/// Default topic to listen to for all lattice events
pub const DEFAULT_EVENTS_TOPIC: &str = "wasmbus.evt.*";
/// Default topic to listen to for all commands
pub const DEFAULT_COMMANDS_TOPIC: &str = "wadm.cmd.*";
