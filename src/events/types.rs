//! Common event types expected on wasmbus. These are the inner types generally sent in the `data`
//! attribute of a cloudevent
// TODO: These should probably be generated from a schema which we add into the actual cloud event

use std::{
    collections::{BTreeMap, HashMap},
    convert::TryFrom,
    fmt::Display,
};

use cloudevents::{AttributesReader, Data, Event as CloudEvent, EventBuilder, EventBuilderV10};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use wasmcloud_control_interface::{ActorDescription, LabelsMap, ProviderDescriptions};

use crate::model::Manifest;

use super::data::*;

/// The source used for cloud events that wadm emits
pub const WADM_SOURCE: &str = "wadm";

// NOTE: this macro is a helper so we don't have to copy/paste these impls for each type. The first
// argument is the struct name you are generating for and the second argument is the event type as
// expected in the cloud event.
//
// There is an optional variant that lets you pull a attribute from the cloud event (third arg) and
// set it to the value in the struct (fourth arg)
macro_rules! event_impl {
    ($t:ident, $type_name:expr) => {
        impl EventType for $t {
            const TYPE: &'static str = $type_name;
        }

        impl From<$t> for Event {
            fn from(value: $t) -> Event {
                Event::$t(value)
            }
        }

        impl std::convert::TryFrom<cloudevents::Event> for $t {
            type Error = ConversionError;

            fn try_from(mut value: cloudevents::Event) -> Result<Self, Self::Error> {
                if $t::TYPE != value.ty() {
                    return Err(ConversionError::WrongEvent(value));
                }
                let (_, _, data) = value.take_data();
                let data = data.ok_or(ConversionError::NoData)?;
                match data {
                    Data::Binary(raw) => serde_json::from_reader(std::io::Cursor::new(raw))
                        .map_err(ConversionError::from),
                    Data::Json(v) => serde_json::from_value(v).map_err(ConversionError::from),
                    Data::String(_) => Err(ConversionError::NoData),
                }
            }
        }
    };

    ($t:ident, $type_name:expr, $event_attr:ident, $data_attr:ident) => {
        impl EventType for $t {
            const TYPE: &'static str = $type_name;
        }

        impl std::convert::TryFrom<cloudevents::Event> for $t {
            type Error = ConversionError;

            fn try_from(mut value: cloudevents::Event) -> Result<Self, Self::Error> {
                if $t::TYPE != value.ty() {
                    return Err(ConversionError::WrongEvent(value));
                }
                let (_, _, data) = value.take_data();
                let data = data.ok_or(ConversionError::NoData)?;
                let mut parsed: Self = match data {
                    Data::Binary(raw) => serde_json::from_reader(std::io::Cursor::new(raw))
                        .map_err(ConversionError::from),
                    Data::Json(v) => serde_json::from_value(v).map_err(ConversionError::from),
                    Data::String(_) => Err(ConversionError::NoData),
                }?;

                parsed.$data_attr = value.$event_attr().to_string();
                Ok(parsed)
            }
        }
    };
}

/// A trait which all events must implement that specifies the string type of the event
pub trait EventType {
    const TYPE: &'static str;
}

/// A lattice event
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub enum Event {
    ActorsStarted(ActorsStarted),
    ActorsStartFailed(ActorsStartFailed),
    ActorsStopped(ActorsStopped),
    ActorScaled(ActorScaled),
    ActorScaleFailed(ActorScaleFailed),
    ProviderStarted(ProviderStarted),
    ProviderStopped(ProviderStopped),
    ProviderStartFailed(ProviderStartFailed),
    ProviderHealthCheckPassed(ProviderHealthCheckPassed),
    ProviderHealthCheckFailed(ProviderHealthCheckFailed),
    ProviderHealthCheckStatus(ProviderHealthCheckStatus),
    HostStarted(HostStarted),
    HostStopped(HostStopped),
    HostHeartbeat(HostHeartbeat),
    LinkdefSet(LinkdefSet),
    LinkdefDeleted(LinkdefDeleted),
    // NOTE(thomastaylor312): We may change where and how these get published, but it makes sense
    // for now to have them here even though they aren't technically lattice events
    ManifestPublished(ManifestPublished),
    ManifestUnpublished(ManifestUnpublished),
}

impl Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Event::ActorsStarted(_) => write!(f, "ActorsStarted"),
            Event::ActorsStartFailed(_) => write!(f, "ActorsStartFailed"),
            Event::ActorsStopped(_) => write!(f, "ActorsStopped"),
            Event::ActorScaled(_) => write!(f, "ActorScaled"),
            Event::ActorScaleFailed(_) => write!(f, "ActorScaleFailed"),
            Event::ProviderStarted(_) => write!(f, "ProviderStarted"),
            Event::ProviderStopped(_) => write!(f, "ProviderStopped"),
            Event::ProviderStartFailed(_) => write!(f, "ProviderStartFailed"),
            Event::ProviderHealthCheckPassed(_) => write!(f, "ProviderHealthCheckPassed"),
            Event::ProviderHealthCheckFailed(_) => write!(f, "ProviderHealthCheckFailed"),
            Event::ProviderHealthCheckStatus(_) => write!(f, "ProviderHealthCheckStatus"),
            Event::HostStarted(_) => write!(f, "HostStarted"),
            Event::HostStopped(_) => write!(f, "HostStopped"),
            Event::HostHeartbeat(_) => write!(f, "HostHeartbeat"),
            Event::LinkdefSet(_) => write!(f, "LinkdefSet"),
            Event::LinkdefDeleted(_) => write!(f, "LinkdefDeleted"),
            Event::ManifestPublished(_) => write!(f, "ManifestPublished"),
            Event::ManifestUnpublished(_) => write!(f, "ManifestUnpublished"),
        }
    }
}

impl TryFrom<CloudEvent> for Event {
    type Error = ConversionError;

    fn try_from(value: CloudEvent) -> Result<Self, Self::Error> {
        match value.ty() {
            ActorsStarted::TYPE => ActorsStarted::try_from(value).map(Event::ActorsStarted),
            ActorsStartFailed::TYPE => {
                ActorsStartFailed::try_from(value).map(Event::ActorsStartFailed)
            }
            ActorsStopped::TYPE => ActorsStopped::try_from(value).map(Event::ActorsStopped),
            ActorScaled::TYPE => ActorScaled::try_from(value).map(Event::ActorScaled),
            ActorScaleFailed::TYPE => {
                ActorScaleFailed::try_from(value).map(Event::ActorScaleFailed)
            }
            ProviderStarted::TYPE => ProviderStarted::try_from(value).map(Event::ProviderStarted),
            ProviderStopped::TYPE => ProviderStopped::try_from(value).map(Event::ProviderStopped),
            ProviderStartFailed::TYPE => {
                ProviderStartFailed::try_from(value).map(Event::ProviderStartFailed)
            }
            ProviderHealthCheckPassed::TYPE => {
                ProviderHealthCheckPassed::try_from(value).map(Event::ProviderHealthCheckPassed)
            }
            ProviderHealthCheckFailed::TYPE => {
                ProviderHealthCheckFailed::try_from(value).map(Event::ProviderHealthCheckFailed)
            }
            ProviderHealthCheckStatus::TYPE => {
                ProviderHealthCheckStatus::try_from(value).map(Event::ProviderHealthCheckStatus)
            }
            HostStarted::TYPE => HostStarted::try_from(value).map(Event::HostStarted),
            HostStopped::TYPE => HostStopped::try_from(value).map(Event::HostStopped),
            HostHeartbeat::TYPE => HostHeartbeat::try_from(value).map(Event::HostHeartbeat),
            LinkdefSet::TYPE => LinkdefSet::try_from(value).map(Event::LinkdefSet),
            LinkdefDeleted::TYPE => LinkdefDeleted::try_from(value).map(Event::LinkdefDeleted),
            ManifestPublished::TYPE => {
                ManifestPublished::try_from(value).map(Event::ManifestPublished)
            }
            ManifestUnpublished::TYPE => {
                ManifestUnpublished::try_from(value).map(Event::ManifestUnpublished)
            }
            _ => Err(ConversionError::WrongEvent(value)),
        }
    }
}

impl TryFrom<Event> for CloudEvent {
    type Error = anyhow::Error;

    fn try_from(value: Event) -> Result<Self, Self::Error> {
        let ty = match value {
            Event::ActorsStarted(_) => ActorsStarted::TYPE,
            Event::ActorsStartFailed(_) => ActorsStartFailed::TYPE,
            Event::ActorsStopped(_) => ActorsStopped::TYPE,
            Event::ActorScaled(_) => ActorScaled::TYPE,
            Event::ActorScaleFailed(_) => ActorScaleFailed::TYPE,
            Event::ProviderStarted(_) => ProviderStarted::TYPE,
            Event::ProviderStopped(_) => ProviderStopped::TYPE,
            Event::ProviderStartFailed(_) => ProviderStartFailed::TYPE,
            Event::ProviderHealthCheckPassed(_) => ProviderHealthCheckPassed::TYPE,
            Event::ProviderHealthCheckFailed(_) => ProviderHealthCheckFailed::TYPE,
            Event::ProviderHealthCheckStatus(_) => ProviderHealthCheckStatus::TYPE,
            Event::HostStarted(_) => HostStarted::TYPE,
            Event::HostStopped(_) => HostStopped::TYPE,
            Event::HostHeartbeat(_) => HostHeartbeat::TYPE,
            Event::LinkdefSet(_) => LinkdefSet::TYPE,
            Event::LinkdefDeleted(_) => LinkdefDeleted::TYPE,
            Event::ManifestPublished(_) => ManifestPublished::TYPE,
            Event::ManifestUnpublished(_) => ManifestUnpublished::TYPE,
        };

        EventBuilderV10::new()
            .id(uuid::Uuid::new_v4().to_string())
            .source(WADM_SOURCE)
            .time(chrono::Utc::now())
            .data("application/json", serde_json::to_value(value)?)
            .ty(ty)
            .build()
            .map_err(anyhow::Error::from)
    }
}

// Custom serialize that just delegates to the underlying event type
impl Serialize for Event {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Event::ActorsStarted(evt) => evt.serialize(serializer),
            Event::ActorsStartFailed(evt) => evt.serialize(serializer),
            Event::ActorsStopped(evt) => evt.serialize(serializer),
            Event::ActorScaled(evt) => evt.serialize(serializer),
            Event::ActorScaleFailed(evt) => evt.serialize(serializer),
            Event::ProviderStarted(evt) => evt.serialize(serializer),
            Event::ProviderStopped(evt) => evt.serialize(serializer),
            Event::ProviderStartFailed(evt) => evt.serialize(serializer),
            Event::ProviderHealthCheckPassed(evt) => evt.serialize(serializer),
            Event::ProviderHealthCheckFailed(evt) => evt.serialize(serializer),
            Event::ProviderHealthCheckStatus(evt) => evt.serialize(serializer),
            Event::HostStarted(evt) => evt.serialize(serializer),
            Event::HostStopped(evt) => evt.serialize(serializer),
            Event::HostHeartbeat(evt) => evt.serialize(serializer),
            Event::LinkdefSet(evt) => evt.serialize(serializer),
            Event::LinkdefDeleted(evt) => evt.serialize(serializer),
            Event::ManifestPublished(evt) => evt.serialize(serializer),
            Event::ManifestUnpublished(evt) => evt.serialize(serializer),
        }
    }
}

impl Event {
    /// Convenience shorthand for calling `TryFrom` on cloudevent
    pub fn new(evt: CloudEvent) -> Result<Event, ConversionError> {
        Event::try_from(evt)
    }

    /// Returns the underlying raw cloudevent type for the event
    pub fn raw_type(&self) -> &str {
        match self {
            Event::ActorsStarted(_) => ActorsStarted::TYPE,
            Event::ActorsStartFailed(_) => ActorsStartFailed::TYPE,
            Event::ActorsStopped(_) => ActorsStopped::TYPE,
            Event::ActorScaled(_) => ActorScaled::TYPE,
            Event::ActorScaleFailed(_) => ActorScaleFailed::TYPE,
            Event::ProviderStarted(_) => ProviderStarted::TYPE,
            Event::ProviderStopped(_) => ProviderStopped::TYPE,
            Event::ProviderStartFailed(_) => ProviderStopped::TYPE,
            Event::ProviderHealthCheckPassed(_) => ProviderHealthCheckPassed::TYPE,
            Event::ProviderHealthCheckFailed(_) => ProviderHealthCheckFailed::TYPE,
            Event::ProviderHealthCheckStatus(_) => ProviderHealthCheckStatus::TYPE,
            Event::HostStarted(_) => HostStarted::TYPE,
            Event::HostStopped(_) => HostStopped::TYPE,
            Event::HostHeartbeat(_) => HostHeartbeat::TYPE,
            Event::LinkdefSet(_) => LinkdefSet::TYPE,
            Event::LinkdefDeleted(_) => LinkdefDeleted::TYPE,
            Event::ManifestPublished(_) => ManifestPublished::TYPE,
            Event::ManifestUnpublished(_) => ManifestUnpublished::TYPE,
        }
    }
}

/// An error returned when attempting to convert a cloudevent to the desired type. If the event type
/// doesn't match, `WrongEvent` is returned with the original event
#[derive(Debug, Error)]
pub enum ConversionError {
    /// An unrecognized event was found when trying to convert it to the event type. Returns the
    /// original cloudevents event
    #[error("Wrong event type")]
    WrongEvent(CloudEvent),
    /// If an event of the right type was found, but no data was contained within that event
    #[error("No data found")]
    NoData,
    /// An error occured while trying to deserialize the data
    #[error("Error when deserializing: {0}")]
    Deser(#[from] serde_json::Error),
}

//
// EVENTS START HERE
//

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ActorsStarted {
    pub annotations: BTreeMap<String, String>,
    // Commented out for now because the host broken it and we actually don't use this right now
    // pub api_version: usize,
    pub claims: ActorClaims,
    pub image_ref: String,
    pub count: usize,
    // TODO: Parse as nkey?
    pub public_key: String,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ActorsStarted,
    "com.wasmcloud.lattice.actors_started",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ActorsStartFailed {
    pub annotations: BTreeMap<String, String>,
    pub image_ref: String,
    // TODO: Parse as nkey?
    pub public_key: String,
    #[serde(default)]
    pub host_id: String,
    pub error: String,
}

event_impl!(
    ActorsStartFailed,
    "com.wasmcloud.lattice.actors_start_failed",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ActorsStopped {
    #[serde(default)]
    pub annotations: BTreeMap<String, String>,
    // TODO: Parse as nkey?
    pub public_key: String,
    #[serde(default)]
    pub host_id: String,
    /// Number of actors stopped from this command
    pub count: usize,
    /// Remaining number of this actor running on the host
    pub remaining: usize,
}

event_impl!(
    ActorsStopped,
    "com.wasmcloud.lattice.actors_stopped",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ActorScaled {
    pub annotations: BTreeMap<String, String>,
    pub claims: ActorClaims,
    pub image_ref: String,
    pub max_instances: usize,
    // TODO: Parse as nkey?
    pub public_key: String,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ActorScaled,
    "com.wasmcloud.lattice.actor_scaled",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ActorScaleFailed {
    pub annotations: BTreeMap<String, String>,
    pub image_ref: String,
    pub max_instances: usize,
    // TODO: Parse as nkey?
    pub public_key: String,
    #[serde(default)]
    pub host_id: String,
    pub error: String,
}

event_impl!(
    ActorScaleFailed,
    "com.wasmcloud.lattice.actor_scale_failed",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProviderStarted {
    pub annotations: BTreeMap<String, String>,
    pub claims: ProviderClaims,
    pub contract_id: String,
    pub image_ref: String,
    // TODO: parse as UUID?
    pub instance_id: String,
    pub link_name: String,
    // TODO: parse as nkey?
    pub public_key: String,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ProviderStarted,
    "com.wasmcloud.lattice.provider_started",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProviderStartFailed {
    pub error: String,
    pub link_name: String,
    pub provider_ref: String,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ProviderStartFailed,
    "com.wasmcloud.lattice.provider_start_failed",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProviderStopped {
    #[serde(default)]
    // TODO(thomastaylor312): Yep, there was a spelling bug in the host is 0.62.1. Revert this once
    // 0.62.2 is out
    #[serde(rename = "annotaions")]
    pub annotations: BTreeMap<String, String>,
    pub contract_id: String,
    // TODO: parse as UUID?
    pub instance_id: String,
    pub link_name: String,
    // TODO: parse as nkey?
    pub public_key: String,
    // We should probably do an actual enum here, but elixir definitely isn't doing it
    pub reason: String,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ProviderStopped,
    "com.wasmcloud.lattice.provider_stopped",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProviderHealthCheckPassed {
    #[serde(flatten)]
    pub data: ProviderHealthCheckInfo,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ProviderHealthCheckPassed,
    "com.wasmcloud.lattice.health_check_passed",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProviderHealthCheckFailed {
    #[serde(flatten)]
    pub data: ProviderHealthCheckInfo,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ProviderHealthCheckFailed,
    "com.wasmcloud.lattice.health_check_failed",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ProviderHealthCheckStatus {
    #[serde(flatten)]
    pub data: ProviderHealthCheckInfo,
    #[serde(default)]
    pub host_id: String,
}

event_impl!(
    ProviderHealthCheckStatus,
    "com.wasmcloud.lattice.health_check_status",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LinkdefSet {
    #[serde(flatten)]
    pub linkdef: Linkdef,
}

event_impl!(LinkdefSet, "com.wasmcloud.lattice.linkdef_set");

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct LinkdefDeleted {
    #[serde(flatten)]
    pub linkdef: Linkdef,
}

event_impl!(LinkdefDeleted, "com.wasmcloud.lattice.linkdef_deleted");

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HostStarted {
    pub labels: HashMap<String, String>,
    pub friendly_name: String,
    // TODO: Parse as nkey?
    #[serde(default)]
    pub id: String,
}

event_impl!(
    HostStarted,
    "com.wasmcloud.lattice.host_started",
    source,
    id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HostStopped {
    pub labels: HashMap<String, String>,
    // TODO: Parse as nkey?
    #[serde(default)]
    pub id: String,
}

event_impl!(
    HostStopped,
    "com.wasmcloud.lattice.host_stopped",
    source,
    id
);

// TODO(#235): Remove once wasmCloud v0.82 is released
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum BackwardsCompatActors {
    V81(HashMap<String, usize>),
    V82(Vec<ActorDescription>),
}

// TODO(#235): Remove once wasmCloud v0.82 is released
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(untagged)]
pub enum BackwardsCompatProviders {
    V81(Vec<ProviderInfo>),
    V82(ProviderDescriptions),
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HostHeartbeat {
    /// Actors running on this host.
    pub actors: BackwardsCompatActors,
    /// Providers running on this host
    pub providers: BackwardsCompatProviders,
    /// The host's unique ID
    #[serde(default, alias = "id")]
    pub host_id: String,
    /// The host's cluster issuer public key
    #[serde(default)]
    pub issuer: String,
    /// The host's human-readable friendly name
    pub friendly_name: String,
    /// The host's labels
    pub labels: LabelsMap,
    /// The host version
    pub version: semver::Version,
    /// The host uptime in human-readable form
    pub uptime_human: String,
    /// The host uptime in seconds
    pub uptime_seconds: u64,
}

event_impl!(
    HostHeartbeat,
    "com.wasmcloud.lattice.host_heartbeat",
    source,
    host_id
);

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ManifestPublished {
    #[serde(flatten)]
    pub manifest: Manifest,
}

event_impl!(ManifestPublished, "com.wadm.manifest_published");

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ManifestUnpublished {
    pub name: String,
}

event_impl!(ManifestUnpublished, "com.wadm.manifest_unpublished");

#[cfg(test)]
mod test {
    use super::*;

    const NON_SUPPORTED_EVENT: &str = r#"
    {
        "data": {
            "oci_url": "wasmcloud.azurecr.io/httpserver:0.16.0",
            "public_key": "VAG3QITQQ2ODAOWB5TTQSDJ53XK3SHBEIFNK4AYJ5RKAX2UNSCAPHA5M"
        },
        "datacontenttype": "application/json",
        "id": "2435a9d8-8ff9-4715-8d21-2f0dc128ec48",
        "source": "NB6PMW4RGLBP3NAVUVO2IH34VFJFSX7LF7TJOQCDU4GGUGF3P57SZLPX",
        "specversion": "1.0",
        "time": "2023-02-14T19:21:09.018468Z",
        "type": "com.wasmcloud.lattice.refmap_set"
    }
    "#;

    #[test]
    fn test_non_supported_event() {
        let raw: cloudevents::Event = serde_json::from_str(NON_SUPPORTED_EVENT).unwrap();

        let err = Event::new(raw).expect_err("Should have errored on a non-supported event");

        assert!(
            matches!(err, ConversionError::WrongEvent(_)),
            "Should have returned wrong event error"
        );
    }

    #[test]
    fn test_all_supported_events() {
        let raw = std::fs::read("./test/data/events.json").expect("Unable to load test data");

        let all_events: Vec<cloudevents::Event> = serde_json::from_slice(&raw).unwrap();

        for evt in all_events.into_iter() {
            println!("EVT {:?}", evt);
            Event::new(evt).expect("Should be able to parse event");
        }
    }
}
