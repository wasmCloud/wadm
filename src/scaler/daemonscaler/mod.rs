use std::cmp::Ordering;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{instrument, trace};

use crate::events::HostHeartbeat;
use crate::model::Spread;
use crate::scaler::spreadscaler::{eligible_hosts, spreadscaler_annotations};
use crate::server::StatusInfo;
use crate::{
    commands::{Command, ScaleActor},
    events::{Event, HostStarted, HostStopped},
    model::{SpreadScalerProperty, TraitProperty},
    scaler::Scaler,
    storage::{Actor, Host, ReadStore},
};

pub mod provider;

// Annotation constants
pub const ACTOR_DAEMON_SCALER_TYPE: &str = "actordaemonscaler";

/// Config for an ActorDaemonScaler
#[derive(Clone, Debug)]
struct ActorSpreadConfig {
    /// OCI, Bindle, or File reference for an actor
    actor_reference: String,
    /// Unique component identifier for an actor
    actor_id: String,
    /// Lattice ID that this DaemonScaler monitors
    lattice_id: String,
    /// The name of the wadm model this DaemonScaler is under
    model_name: String,
    /// Configuration for this DaemonScaler
    spread_config: SpreadScalerProperty,
}

/// The ActorDaemonScaler ensures that a certain number of instances are running on every host, according to a
/// [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of instances
/// on every available host.
pub struct ActorDaemonScaler<S> {
    config: ActorSpreadConfig,
    store: S,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ActorDaemonScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        let spread_config = match config {
            TraitProperty::SpreadScaler(prop) => prop,
            _ => anyhow::bail!("Given config was not a daemon scaler config object"),
        };
        // If no spreads are specified, an empty spread is sufficient to match _every_ host
        // in a lattice
        let spread_config = if spread_config.spread.is_empty() {
            SpreadScalerProperty {
                instances: spread_config.instances,
                spread: vec![Spread::default()],
            }
        } else {
            spread_config
        };
        self.config.spread_config = spread_config;
        self.reconcile().await
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id))]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): We could be more efficient here and instead of running
        // the entire reconcile, smart compute exactly what needs to change, but it just
        // requires more code branches and would be fine as a future improvement
        match event {
            // TODO: React to ComponentScaleFailed with an exponential backoff, can't just immediately retry since that
            // would cause a very tight loop of failures
            Event::ComponentScaled(evt) if evt.actor_id == self.config.actor_id => {
                self.reconcile().await
            }
            Event::HostStopped(HostStopped { labels, .. })
            | Event::HostStarted(HostStarted { labels, .. })
            | Event::HostHeartbeat(HostHeartbeat { labels, .. }) => {
                // If the host labels match any spread requirement, perform reconcile
                if self.config.spread_config.spread.iter().any(|spread| {
                    spread.requirements.iter().all(|(key, value)| {
                        labels.get(key).map(|val| val == value).unwrap_or(false)
                    })
                }) {
                    trace!("Host event matches spread requirements. Will reconcile");
                    self.reconcile().await
                } else {
                    Ok(Vec::new())
                }
            }
            // No other event impacts the job of this scaler so we can ignore it
            _ => Ok(Vec::new()),
        }
    }

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name, scaler_id = %self.id))]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        let hosts = self.store.list::<Host>(&self.config.lattice_id).await?;

        let actor_id = &self.config.actor_id;
        let actor = self
            .store
            .get::<Actor>(&self.config.lattice_id, actor_id)
            .await?;

        let mut spread_status = vec![];

        trace!(spread = ?self.config.spread_config.spread, ?actor_id, "Computing commands");
        let commands = self
            .config
            .spread_config
            .spread
            .iter()
            .filter_map(|spread| {
                let eligible_hosts = eligible_hosts(&hosts, spread);
                if !eligible_hosts.is_empty() {
                    // Create a list of (host_id, current_count) tuples
                    // current_count is the number of actor instances that are running for this spread on this host
                    let actors_per_host = eligible_hosts
                        .into_keys()
                        .map(|id| {
                            let count = actor
                                .as_ref()
                                .and_then(|actor| {
                                    actor.instances.get(&id.to_string()).map(|instances| {
                                        instances
                                            .iter()
                                            .filter_map(|info| {
                                                spreadscaler_annotations(&spread.name, self.id())
                                                    .iter()
                                                    .all(|(key, value)| {
                                                        info.annotations
                                                            .get(key)
                                                            .map(|v| v == value)
                                                            .unwrap_or(false)
                                                    })
                                                    .then_some(info.count)
                                            })
                                            .sum()
                                    })
                                })
                                .unwrap_or(0);
                            (id, count)
                        })
                        .collect::<Vec<(&String, usize)>>();

                    Some(
                        actors_per_host
                            .iter()
                            .filter_map(|(host_id, current_count)| {
                                // Here we'll generate commands for the proper host depending on where they are running
                                match current_count.cmp(&self.config.spread_config.instances) {
                                    Ordering::Equal => None,
                                    // Scale actor can handle both up and down scaling
                                    Ordering::Less | Ordering::Greater => {
                                        Some(Command::ScaleActor(ScaleActor {
                                            reference: self.config.actor_reference.to_owned(),
                                            actor_id: actor_id.to_owned(),
                                            host_id: host_id.to_string(),
                                            count: self.config.spread_config.instances as u32,
                                            model_name: self.config.model_name.to_owned(),
                                            annotations: spreadscaler_annotations(
                                                &spread.name,
                                                self.id(),
                                            ),
                                        }))
                                    }
                                }
                            })
                            .collect::<Vec<Command>>(),
                    )
                } else {
                    // No hosts were eligible, so we can't attempt to add or remove actors
                    trace!(?spread.name, "Found no eligible hosts for daemon scaler");
                    spread_status.push(StatusInfo::failed(&format!(
                        "Could not satisfy daemonscaler {} for {}, 0 eligible hosts found.",
                        spread.name, self.config.actor_reference
                    )));
                    None
                }
            })
            .flatten()
            .collect::<Vec<Command>>();
        trace!(?commands, "Calculated commands for actor daemon scaler");

        let status = match (spread_status.is_empty(), commands.is_empty()) {
            (true, true) => StatusInfo::deployed(""),
            (_, false) => StatusInfo::reconciling(""),
            (false, true) => StatusInfo::failed(
                &spread_status
                    .into_iter()
                    .map(|s| s.message)
                    .collect::<Vec<String>>()
                    .join(" "),
            ),
        };
        trace!(?status, "Updating scaler status");
        *self.status.write().await = status;

        Ok(commands)
    }

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name))]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        let mut config_clone = self.config.clone();
        config_clone.spread_config.instances = 0;

        let cleanerupper = ActorDaemonScaler {
            config: config_clone,
            store: self.store.clone(),
            id: self.id.clone(),
            status: RwLock::new(StatusInfo::reconciling("")),
        };

        cleanerupper.reconcile().await
    }
}

impl<S: ReadStore + Send + Sync> ActorDaemonScaler<S> {
    /// Construct a new ActorDaemonScaler with specified configuration values
    pub fn new(
        store: S,
        actor_reference: String,
        actor_id: String,
        lattice_id: String,
        model_name: String,
        spread_config: SpreadScalerProperty,
        component_name: &str,
    ) -> Self {
        let id = format!("{ACTOR_DAEMON_SCALER_TYPE}-{model_name}-{component_name}-{actor_id}");
        // If no spreads are specified, an empty spread is sufficient to match _every_ host
        // in a lattice
        let spread_config = if spread_config.spread.is_empty() {
            SpreadScalerProperty {
                instances: spread_config.instances,
                spread: vec![Spread::default()],
            }
        } else {
            spread_config
        };
        Self {
            store,
            config: ActorSpreadConfig {
                actor_reference,
                actor_id,
                lattice_id,
                spread_config,
                model_name,
            },
            id,
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::Arc,
    };

    use anyhow::Result;
    use chrono::Utc;
    use wasmcloud_control_interface::{HostInventory, InterfaceLinkDefinition};

    use crate::{
        commands::Command,
        consumers::{manager::Worker, ScopedMessage},
        events::{Event, LinkdefDeleted, LinkdefSet, ProviderStarted, ProviderStopped},
        model::{Spread, SpreadScalerProperty},
        scaler::{daemonscaler::ActorDaemonScaler, manager::ScalerManager, Scaler},
        server::StatusType,
        storage::{Actor, Host, Store, WadmActorInfo},
        test_util::{NoopPublisher, TestLatticeSource, TestStore},
        workers::{CommandPublisher, EventWorker, StatusPublisher},
    };

    const MODEL_NAME: &str = "daemonscaler_test";

    #[tokio::test]
    async fn can_compute_spread_commands() -> Result<()> {
        let lattice_id = "one_host";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let actor_id = "fakecloud_azurecr_io_echo_0_3_4".to_string();
        let host_id = "NASDASDIMAREALHOST";

        let store = Arc::new(TestStore::default());

        // STATE SETUP BEGIN, ONE HOST
        store
            .store(
                lattice_id,
                host_id.to_string(),
                Host {
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::new(),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        // Daemonscalers ignore weight, so it should have no bearing
        let complex_spread = SpreadScalerProperty {
            instances: 13,
            spread: vec![
                Spread {
                    name: "ComplexOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(42),
                },
                Spread {
                    name: "ComplexTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(3),
                },
                Spread {
                    name: "ComplexThree".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(37),
                },
                Spread {
                    name: "ComplexFour".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(384),
                },
            ],
        };

        let daemonscaler = ActorDaemonScaler::new(
            store.clone(),
            actor_reference.to_string(),
            actor_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            complex_spread,
            "fake_component",
        );

        let cmds = daemonscaler.reconcile().await?;
        assert_eq!(cmds.len(), 4);
        assert!(cmds.contains(&Command::ScaleActor(ScaleActor {
            actor_id: actor_id.to_string(),
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 13,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexOne", daemonscaler.id())
        })));
        assert!(cmds.contains(&Command::ScaleActor(ScaleActor {
            actor_id: actor_id.to_string(),
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 13,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexTwo", daemonscaler.id())
        })));
        assert!(cmds.contains(&Command::ScaleActor(ScaleActor {
            actor_id: actor_id.to_string(),
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 13,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexThree", daemonscaler.id())
        })));
        assert!(cmds.contains(&Command::ScaleActor(ScaleActor {
            actor_id: actor_id.to_string(),
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 13,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexFour", daemonscaler.id())
        })));

        Ok(())
    }

    #[tokio::test]
    async fn can_scale_up_and_down() -> Result<()> {
        let lattice_id = "computing_spread_commands";
        let echo_ref = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let echo_id = "MASDASDIAMAREALACTORECHO";
        let blobby_ref = "fakecloud.azurecr.io/blobby:0.5.2".to_string();
        let blobby_id = "MASDASDIAMAREALACTORBLOBBY";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";
        let host_id_three = "NASDASDIMAREALHOSTTREE";

        let store = Arc::new(TestStore::default());

        let echo_spread_property = SpreadScalerProperty {
            instances: 412,
            spread: vec![
                Spread {
                    name: "RunInFakeCloud".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                    weight: None,
                },
                Spread {
                    name: "RunInRealCloud".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "real".to_string())]),
                    weight: None,
                },
                Spread {
                    name: "RunInPurgatoryCloud".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "cloud".to_string(),
                        "purgatory".to_string(),
                    )]),
                    weight: None,
                },
            ],
        };

        let blobby_spread_property = SpreadScalerProperty {
            instances: 3,
            spread: vec![
                Spread {
                    name: "CrossRegionCustom".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-brooks-1".to_string(),
                    )]),
                    weight: Some(123123),
                },
                Spread {
                    name: "CrossRegionReal".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-midwest-4".to_string(),
                    )]),
                    weight: None,
                },
                Spread {
                    name: "RunOnEdge".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "location".to_string(),
                        "edge".to_string(),
                    )]),
                    weight: Some(33),
                },
            ],
        };

        let echo_daemonscaler = ActorDaemonScaler::new(
            store.clone(),
            echo_ref.to_string(),
            echo_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            echo_spread_property,
            "fake_echo",
        );

        let blobby_daemonscaler = ActorDaemonScaler::new(
            store.clone(),
            blobby_ref.to_string(),
            blobby_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            blobby_spread_property,
            "fake_blobby",
        );

        // STATE SETUP BEGIN

        store
            .store(
                lattice_id,
                echo_id.to_string(),
                Actor {
                    id: echo_id.to_string(),
                    name: "Echo".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // One instance on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 1,
                                annotations: spreadscaler_annotations(
                                    "RunInFakeCloud",
                                    echo_daemonscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 103 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 103,
                                annotations: spreadscaler_annotations(
                                    "RunInRealCloud",
                                    echo_daemonscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_three.to_string(),
                            // 400 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 400,
                                annotations: spreadscaler_annotations(
                                    "RunInPurgatoryCloud",
                                    echo_daemonscaler.id(),
                                ),
                            }]),
                        ),
                    ]),
                    reference: echo_ref.to_string(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                blobby_id.to_string(),
                Actor {
                    id: blobby_id.to_string(),
                    name: "Blobby".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 3 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 3,
                                annotations: spreadscaler_annotations(
                                    "CrossRegionCustom",
                                    blobby_daemonscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 19 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 19,
                                annotations: spreadscaler_annotations(
                                    "CrossRegionReal",
                                    blobby_daemonscaler.id(),
                                ),
                            }]),
                        ),
                    ]),
                    reference: blobby_ref.to_string(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    actors: HashMap::from_iter([
                        (echo_id.to_string(), 1),
                        (blobby_id.to_string(), 3),
                        ("MSOMEOTHERACTOR".to_string(), 3),
                    ]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-brooks-1".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_one.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                host_id_two.to_string(),
                Host {
                    actors: HashMap::from_iter([
                        (echo_id.to_string(), 103),
                        (blobby_id.to_string(), 19),
                    ]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-midwest-4".to_string()),
                        ("label".to_string(), "value".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_two.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                host_id_three.to_string(),
                Host {
                    actors: HashMap::from_iter([(echo_id.to_string(), 400)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "purgatory".to_string()),
                        ("location".to_string(), "edge".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_three.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        // STATE SETUP END

        let cmds = echo_daemonscaler.reconcile().await?;
        assert_eq!(cmds.len(), 3);

        for cmd in cmds.iter() {
            match cmd {
                Command::ScaleActor(scale) =>
                {
                    #[allow(clippy::if_same_then_else)]
                    if scale.host_id == *host_id_one {
                        assert_eq!(scale.count, 412);
                        assert_eq!(scale.reference, echo_ref);
                    } else if scale.host_id == *host_id_two {
                        assert_eq!(scale.count, 412);
                        assert_eq!(scale.reference, echo_ref);
                    } else {
                        assert_eq!(scale.count, 412);
                        assert_eq!(scale.reference, echo_ref);
                    }
                }
                _ => panic!("Unexpected command in daemonscaler list"),
            }
        }

        let mut cmds = blobby_daemonscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);
        cmds.sort_by(|a, b| match (a, b) {
            (Command::ScaleActor(a), Command::ScaleActor(b)) => a.host_id.cmp(&b.host_id),
            _ => panic!("Unexpected command in daemonscaler list"),
        });

        let mut cmds_iter = cmds.iter();
        match (
            cmds_iter.next().expect("one command"),
            cmds_iter.next().expect("two commands"),
        ) {
            (Command::ScaleActor(scale1), Command::ScaleActor(scale2)) => {
                assert_eq!(scale1.host_id, host_id_three.to_string());
                assert_eq!(scale1.count, 3);
                assert_eq!(scale1.reference, blobby_ref);

                assert_eq!(scale2.host_id, host_id_two.to_string());
                assert_eq!(scale2.count, 3);
                assert_eq!(scale2.actor_id, blobby_id.to_string());
            }
            _ => panic!("Unexpected commands in daemonscaler list"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_react_to_host_events() -> Result<()> {
        let lattice_id = "computing_spread_commands";
        let blobby_ref = "fakecloud.azurecr.io/blobby:0.5.2".to_string();
        let blobby_id = "MASDASDIAMAREALACTORBLOBBY";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";
        let host_id_three = "NASDASDIMAREALHOSTTREE";

        let store = Arc::new(TestStore::default());

        let lattice_source = TestLatticeSource::default();
        // Inserting for heartbeat handling later
        lattice_source.inventory.write().await.insert(
            host_id_three.to_string(),
            HostInventory {
                components: vec![],
                friendly_name: "hey".to_string(),
                labels: HashMap::from_iter([
                    ("cloud".to_string(), "purgatory".to_string()),
                    ("location".to_string(), "edge".to_string()),
                    ("region".to_string(), "us-brooks-1".to_string()),
                ]),
                providers: vec![],
                host_id: host_id_three.to_string(),
                issuer: "NASDASD".to_string(),
                version: "1.0.0".to_string(),
                uptime_human: "what is time really anyway maaaan".to_string(),
                uptime_seconds: 42,
            },
        );
        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, None, "doesntmatter");
        let worker = EventWorker::new(
            store.clone(),
            lattice_source.clone(),
            command_publisher.clone(),
            status_publisher.clone(),
            ScalerManager::test_new(
                NoopPublisher,
                lattice_id,
                store.clone(),
                command_publisher,
                status_publisher.clone(),
                lattice_source,
            )
            .await,
        );
        let blobby_spread_property = SpreadScalerProperty {
            instances: 10,
            spread: vec![Spread {
                name: "HighAvailability".to_string(),
                requirements: BTreeMap::from_iter([(
                    "region".to_string(),
                    "us-brooks-1".to_string(),
                )]),
                weight: None,
            }],
        };
        let blobby_daemonscaler = ActorDaemonScaler::new(
            store.clone(),
            blobby_ref.to_string(),
            blobby_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            blobby_spread_property,
            "fake_blobby",
        );

        // STATE SETUP BEGIN
        store
            .store(
                lattice_id,
                blobby_id.to_string(),
                Actor {
                    id: blobby_id.to_string(),
                    name: "Blobby".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 10 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 10,
                                annotations: spreadscaler_annotations(
                                    "HighAvailability",
                                    blobby_daemonscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 10 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 10,
                                annotations: spreadscaler_annotations(
                                    "HighAvailability",
                                    blobby_daemonscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_three.to_string(),
                            // 0 instances on this host
                            HashSet::new(),
                        ),
                    ]),
                    reference: blobby_ref.to_string(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    actors: HashMap::from_iter([
                        (blobby_id.to_string(), 10),
                        ("MSOMEOTHERACTOR".to_string(), 3),
                    ]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-brooks-1".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_one.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                host_id_two.to_string(),
                Host {
                    actors: HashMap::from_iter([(blobby_id.to_string(), 10)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-brooks-1".to_string()),
                        ("label".to_string(), "value".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_two.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        // Don't care about these events
        assert!(blobby_daemonscaler
            .handle_event(&Event::ProviderStarted(ProviderStarted {
                claims: None,
                provider_id: "".to_string(),
                image_ref: "".to_string(),
                annotations: BTreeMap::default(),
                host_id: host_id_one.to_string()
            }))
            .await?
            .is_empty());
        assert!(blobby_daemonscaler
            .handle_event(&Event::ProviderStopped(ProviderStopped {
                annotations: BTreeMap::default(),
                provider_id: "".to_string(),
                reason: "".to_string(),
                host_id: host_id_two.to_string()
            }))
            .await?
            .is_empty());
        assert!(blobby_daemonscaler
            .handle_event(&Event::LinkdefSet(LinkdefSet {
                linkdef: InterfaceLinkDefinition::default()
            }))
            .await?
            .is_empty());
        assert!(blobby_daemonscaler
            .handle_event(&Event::LinkdefDeleted(LinkdefDeleted {
                source_id: "source".to_string(),
                name: "name".to_string(),
                wit_namespace: "wasi".to_string(),
                wit_package: "testy".to_string()
            }))
            .await?
            .is_empty());

        // Let a new host come online, should match the spread
        let modifying_event = HostHeartbeat {
            components: vec![],
            friendly_name: "hey".to_string(),
            issuer: "".to_string(),
            labels: HashMap::from_iter([
                ("cloud".to_string(), "purgatory".to_string()),
                ("location".to_string(), "edge".to_string()),
                ("region".to_string(), "us-brooks-1".to_string()),
            ]),
            providers: vec![],
            uptime_seconds: 123,
            version: semver::Version::new(0, 63, 1),
            host_id: host_id_three.to_string(),
            uptime_human: "time_is_a_human_construct".to_string(),
        };

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::HostHeartbeat(modifying_event.clone()),
                acker: None,
            })
            .await
            .expect("should be able to handle an event");

        let cmds = blobby_daemonscaler
            .handle_event(&Event::HostHeartbeat(modifying_event))
            .await?;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            blobby_daemonscaler.status().await.status_type,
            StatusType::Reconciling
        );

        for cmd in cmds.iter() {
            match cmd {
                Command::ScaleActor(scale) => {
                    assert_eq!(scale.host_id, host_id_three.to_string());
                    assert_eq!(scale.count, 10);
                    assert_eq!(scale.reference, blobby_ref);
                }
                _ => panic!("Unexpected command in daemonscaler list"),
            }
        }

        // Remove the host, blobby shouldn't be concerned as other hosts match
        store
            .delete_many::<Host, _, _>(lattice_id, vec![host_id_three])
            .await?;
        store
            .store(
                lattice_id,
                blobby_id.to_string(),
                Actor {
                    id: blobby_id.to_string(),
                    name: "Blobby".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 10 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 10,
                                annotations: spreadscaler_annotations(
                                    "HighAvailability",
                                    blobby_daemonscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 10 instances on this host
                            HashSet::from_iter([WadmActorInfo {
                                count: 10,
                                annotations: spreadscaler_annotations(
                                    "HighAvailability",
                                    blobby_daemonscaler.id(),
                                ),
                            }]),
                        ),
                    ]),
                    reference: blobby_ref.to_string(),
                },
            )
            .await?;
        let cmds = blobby_daemonscaler.reconcile().await?;
        assert_eq!(cmds.len(), 0);

        assert_eq!(
            blobby_daemonscaler.status().await.status_type,
            StatusType::Deployed
        );

        Ok(())
    }
}
