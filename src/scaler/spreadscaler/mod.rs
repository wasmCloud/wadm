use std::{cmp::Ordering, collections::HashMap};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{OnceCell, RwLock};
use tracing::{instrument, trace, warn};

use crate::events::HostHeartbeat;
use crate::server::StatusInfo;
use crate::{
    commands::{Command, StartActor, StopActor},
    events::{Event, HostStarted, HostStopped},
    model::{Spread, SpreadScalerProperty, TraitProperty, DEFAULT_SPREAD_WEIGHT},
    scaler::Scaler,
    storage::{Actor, Host, ReadStore},
    SCALER_KEY,
};

pub mod link;
pub mod provider;

// Annotation constants
const SPREAD_KEY: &str = "wasmcloud.dev/spread_name";

pub const ACTOR_SPREAD_SCALER_TYPE: &str = "actorspreadscaler";

/// Config for an ActorSpreadScaler
#[derive(Clone)]
struct ActorSpreadConfig {
    /// OCI, Bindle, or File reference for an actor
    actor_reference: String,
    /// Lattice ID that this SpreadScaler monitors
    lattice_id: String,
    /// The name of the wadm model this SpreadScaler is under
    model_name: String,
    /// Configuration for this SpreadScaler
    spread_config: SpreadScalerProperty,
}

/// The ActorSpreadScaler ensures that a certain number of replicas are running,
/// spread across a number of hosts according to a [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of replicas
/// on an available host
pub struct ActorSpreadScaler<S> {
    config: ActorSpreadConfig,
    spread_requirements: Vec<(Spread, usize)>,
    actor_id: OnceCell<String>,
    store: S,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ActorSpreadScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    async fn status(&self) -> StatusInfo {
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        let spread_config = match config {
            TraitProperty::SpreadScaler(prop) => prop,
            _ => anyhow::bail!("Given config was not a spread scaler config object"),
        };
        self.config.spread_config = spread_config;
        self.spread_requirements = compute_spread(&self.config.spread_config);
        self.reconcile().await
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id))]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): We could be more efficient here and instead of running
        // the entire reconcile, smart compute exactly what needs to change, but it just
        // requires more code branches and would be fine as a future improvement
        match event {
            Event::ActorsStarted(actor_started) => {
                if actor_started.image_ref == self.config.actor_reference {
                    trace!(image_ref = %actor_started.image_ref, "Found image we care about");
                    self.reconcile().await
                } else {
                    Ok(Vec::new())
                }
            }
            Event::ActorsStopped(actor_stopped) => {
                let actor_id = self.actor_id().await?;
                if actor_stopped.public_key == actor_id {
                    trace!(%actor_id, "Found actor we care about");
                    self.reconcile().await
                } else {
                    Ok(Vec::new())
                }
            }
            Event::HostStopped(HostStopped { labels, .. })
            | Event::HostStarted(HostStarted { labels, .. })
            | Event::HostHeartbeat(HostHeartbeat { labels, .. }) => {
                // If the host labels match any spread requirement, perform reconcile
                if self.spread_requirements.iter().any(|(spread, _count)| {
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

        let actor = if let Ok(id) = self.actor_id().await {
            self.store.get::<Actor>(&self.config.lattice_id, id).await?
        } else {
            // If this is the first time we have ever loaded the actor, there won't be a way to get
            // the ID, so an error will occur. We can just return None and let the scaler try to
            // start stuff
            None
        };

        let actor_id = actor.as_ref().map(|actor| actor.id.as_str());

        let mut spread_status = vec![];

        // NOTE(brooksmtownsend) it's easier to assign one host per list of requirements than
        // balance within those requirements. Users should be specific with their requirements
        // as wadm is not responsible for ambiguity, a future scaler like a DaemonScaler could handle this
        trace!(spread_requirements = ?self.spread_requirements, ?actor_id, "Computing commands");
        let commands = self
            .spread_requirements
            .iter()
            .filter_map(|(spread, count)| {
                let eligible_hosts = eligible_hosts(&hosts, spread);
                if !eligible_hosts.is_empty() {
                    // In the future we may want more information from this chain, but for now
                    // we just need the number of running actors that match this spread's annotations

                    // Parse the instances into a map of host_id -> number of running actors managed
                    // by this scaler. Ignoring ones where we aren't running anything
                    let running_actors_per_host: HashMap<&String, usize> = actor
                        .as_ref()
                        .map(|actor| &actor.instances)
                        .map(|instances| {
                            instances
                                .iter()
                                .filter_map(|(host_id, instances)| {
                                    let count = instances
                                        .iter()
                                        .filter(|instance| {
                                            spreadscaler_annotations(&spread.name, self.id()).iter().all(
                                                |(key, value)| {
                                                    instance
                                                        .annotations
                                                        .get(key)
                                                        .map(|v| v == value)
                                                        .unwrap_or(false)
                                                },
                                            )
                                        })
                                        .count();
                                    (count > 0).then_some((host_id, count))
                                }).collect()
                        })
                        .unwrap_or_default();
                    let current_count: usize = running_actors_per_host.values().sum();
                    trace!(current = %current_count, expected = %count, "Calculated running actors, reconciling with expected count");
                    // Here we'll generate commands for the proper host depending on where they are running
                    match current_count.cmp(count) {
                        Ordering::Equal => None,
                        // Start actors to reach desired replicas
                        Ordering::Less =>{
                            // Right now just start on the first available host. We can be smarter about it later
                            Some(vec![Command::StartActor(StartActor {
                                reference: self.config.actor_reference.to_owned(),
                                // SAFETY: We already checked that the list of hosts is not empty, so we can unwrap here
                                host_id: eligible_hosts.keys().next().unwrap().to_string(),
                                count: count - current_count,
                                model_name: self.config.model_name.to_owned(),
                                annotations: spreadscaler_annotations(&spread.name, self.id()),
                            })])
                        }
                        // Stop actors to reach desired replicas
                        Ordering::Greater => {
                            let count_to_stop = current_count - count;
                            let (_, commands) = running_actors_per_host.into_iter().fold((0usize, Vec::new()), |(mut current_stopped, mut commands), (host_id, instance_count)| {
                                let remaining_to_stop = count_to_stop - current_stopped;
                                let stop = if instance_count >= remaining_to_stop {
                                    remaining_to_stop
                                } else {
                                    instance_count
                                };
                                // If there aren't any on here then we don't need a command to stop
                                if stop > 0 {
                                    current_stopped += stop;
                                    commands.push(Command::StopActor(StopActor {
                                        // We shouldn't ever get a stop actor command if this is the first actor
                                        // to be started. In the o ff chance we do, this command will result in
                                        // nothing due to the empty id
                                        actor_id: actor_id.unwrap_or_default().to_owned(),
                                        host_id: host_id.to_owned(),
                                        count: stop,
                                        model_name: self.config.model_name.to_owned(),
                                        annotations: spreadscaler_annotations(&spread.name, self.id()),
                                    }));
                                }
                                (current_stopped, commands)
                            });
                            Some(commands)
                        }
                    }
                } else {
                    // No hosts were eligible, so we can't attempt to add or remove actors
                    trace!(?spread.name, "Found no eligible hosts for spread");
                    spread_status.push(StatusInfo::failed(&format!("Could not satisfy spread {} for {}, 0/1 eligible hosts found.", spread.name, self.config.actor_reference)));
                    None
                }
            })
            .flatten()
            .collect::<Vec<Command>>();
        trace!(?commands, "Calculated commands for actor scaler");

        let status = if spread_status.is_empty() {
            StatusInfo::ready("")
        } else {
            StatusInfo::failed(
                &spread_status
                    .into_iter()
                    .map(|s| s.message)
                    .collect::<Vec<String>>()
                    .join(" "),
            )
        };
        trace!(?status, "Updating scaler status");
        *self.status.write().await = status;

        Ok(commands)
    }

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name))]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        let mut config_clone = self.config.clone();
        config_clone.spread_config.replicas = 0;
        let spread_requirements = compute_spread(&config_clone.spread_config);

        let cleanerupper = ActorSpreadScaler {
            config: config_clone,
            store: self.store.clone(),
            spread_requirements,
            actor_id: self.actor_id.clone(),
            id: self.id.clone(),
            status: RwLock::new(StatusInfo::compensating("")),
        };

        cleanerupper.reconcile().await
    }
}

impl<S: ReadStore + Send + Sync> ActorSpreadScaler<S> {
    /// Construct a new ActorSpreadScaler with specified configuration values
    pub fn new(
        store: S,
        actor_reference: String,
        lattice_id: String,
        model_name: String,
        spread_config: SpreadScalerProperty,
        component_name: &str,
    ) -> Self {
        let id =
            format!("{ACTOR_SPREAD_SCALER_TYPE}-{model_name}-{component_name}-{actor_reference}");
        Self {
            store,
            spread_requirements: compute_spread(&spread_config),
            actor_id: OnceCell::new(),
            config: ActorSpreadConfig {
                actor_reference,
                lattice_id,
                spread_config,
                model_name,
            },
            id,
            status: RwLock::new(StatusInfo::compensating("")),
        }
    }

    /// Helper function to retrieve the actor ID for the configured actor
    async fn actor_id(&self) -> Result<&str> {
        self.actor_id
            .get_or_try_init(|| async {
                self.store
                    .list::<Actor>(&self.config.lattice_id)
                    .await?
                    .iter()
                    .find(|(_id, actor)| actor.reference == self.config.actor_reference)
                    .map(|(id, _actor)| id.to_owned())
                    // Default here means the below `get` will find zero running actors, which is fine because
                    // that accurately describes the current lattice having zero instances.
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Couldn't find an actor id for the actor reference {}",
                            self.config.actor_reference
                        )
                    })
            })
            .await
            .map(|id| id.as_str())
    }
}

/// Helper function to create a predictable annotations map for a spread
fn spreadscaler_annotations(spread_name: &str, scaler_id: &str) -> HashMap<String, String> {
    HashMap::from_iter([
        (SCALER_KEY.to_string(), scaler_id.to_string()),
        (SPREAD_KEY.to_string(), spread_name.to_string()),
    ])
}

/// Helper function that computes a list of eligible hosts to match with a spread
fn eligible_hosts<'a>(
    all_hosts: &'a HashMap<String, Host>,
    spread: &Spread,
) -> HashMap<&'a String, &'a Host> {
    all_hosts
        .iter()
        .filter(|(_id, host)| {
            spread
                .requirements
                .iter()
                .all(|(key, value)| host.labels.get(key).map(|v| v.eq(value)).unwrap_or(false))
        })
        .map(|(id, host)| (id, host))
        .collect()
}

/// Given a spread config, return a vector of tuples that represents the spread
/// and the actual number of actors to start for a specific spread requirement
fn compute_spread(spread_config: &SpreadScalerProperty) -> Vec<(Spread, usize)> {
    let replicas = spread_config.replicas;
    let total_weight = spread_config
        .spread
        .iter()
        .map(|s| s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT))
        .sum::<usize>();
    let spreads: Vec<(Spread, usize)> = spread_config
        .spread
        .iter()
        .map(|s| {
            (
                s.to_owned(),
                // Order is important here since usizes chop off remaining decimals
                (replicas * s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT)) / total_weight,
            )
        })
        .collect();

    let spreads = if spreads.is_empty() {
        vec![(Spread::default(), replicas)]
    } else {
        spreads
    };
    // Because of math, we may end up rounding a few instances away. Evenly distribute them
    // among the remaining hosts
    let total_replicas = spreads.iter().map(|(_s, count)| count).sum::<usize>();
    match total_replicas.cmp(&replicas) {
        Ordering::Less => {
            // Take the remainder of replicas and evenly distribute among remaining spreads
            let mut diff = replicas - total_replicas;
            spreads
                .into_iter()
                .map(|(spread, count)| {
                    let additional = if diff > 0 {
                        diff -= 1;
                        1
                    } else {
                        0
                    };
                    (spread, count + additional)
                })
                .collect()
        }
        // This isn't possible (usizes round down) but I added an arm _just in case_
        // there was a case that I didn't imagine
        Ordering::Greater => {
            warn!("Requesting more actor instances than were specified");
            spreads
        }
        Ordering::Equal => spreads,
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

    use crate::{
        commands::{Command, StartActor},
        consumers::{manager::Worker, ScopedMessage},
        events::{
            ActorStopped, ActorsStopped, Event, Linkdef, LinkdefDeleted, LinkdefSet,
            ProviderClaims, ProviderStarted, ProviderStopped,
        },
        model::{Spread, SpreadScalerProperty},
        scaler::{
            manager::ScalerManager,
            spreadscaler::{spreadscaler_annotations, ActorSpreadScaler},
            Scaler,
        },
        storage::{Actor, Host, Store, WadmActorInstance},
        test_util::{NoopPublisher, TestLatticeSource, TestStore},
        workers::{CommandPublisher, EventWorker, StatusPublisher},
    };

    const MODEL_NAME: &str = "spreadscaler_test";

    use super::compute_spread;

    #[test]
    fn can_spread_properly() -> Result<()> {
        // Basic test to ensure our types are correct
        let simple_spread = SpreadScalerProperty {
            replicas: 1,
            spread: vec![Spread {
                name: "Simple".to_string(),
                requirements: BTreeMap::new(),
                weight: Some(100),
            }],
        };

        let simple_spread_res = compute_spread(&simple_spread);
        assert_eq!(simple_spread_res[0].1, 1);

        // Ensure we spread evenly with equal weights, clean division
        let multi_spread_even = SpreadScalerProperty {
            replicas: 10,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(100),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(100),
                },
            ],
        };

        let multi_spread_even_res = compute_spread(&multi_spread_even);
        assert_eq!(multi_spread_even_res[0].1, 5);
        assert_eq!(multi_spread_even_res[1].1, 5);

        // Ensure we spread an odd number with clean dividing weights
        let multi_spread_odd = SpreadScalerProperty {
            replicas: 7,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(30),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(40),
                },
            ],
        };

        let multi_spread_even_res = compute_spread(&multi_spread_odd);
        assert_eq!(multi_spread_even_res[0].1, 3);
        assert_eq!(multi_spread_even_res[1].1, 4);

        // Ensure we spread an odd number with unclean dividing weights
        let multi_spread_odd = SpreadScalerProperty {
            replicas: 7,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(100),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(100),
                },
            ],
        };

        let multi_spread_even_res = compute_spread(&multi_spread_odd);
        assert_eq!(multi_spread_even_res[0].1, 4);
        assert_eq!(multi_spread_even_res[1].1, 3);

        // Ensure we compute if a weights aren't specified
        let multi_spread_even_no_weight = SpreadScalerProperty {
            replicas: 10,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: None,
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: None,
                },
            ],
        };

        let multi_spread_even_no_weight = compute_spread(&multi_spread_even_no_weight);
        assert_eq!(multi_spread_even_no_weight[0].1, 5);
        assert_eq!(multi_spread_even_no_weight[1].1, 5);

        // Ensure we compute if spread vec is empty
        let simple_spread_replica_only = SpreadScalerProperty {
            replicas: 12,
            spread: vec![],
        };

        let simple_replica_only = compute_spread(&simple_spread_replica_only);
        assert_eq!(simple_replica_only.len(), 1);
        assert_eq!(simple_replica_only[0].1, 12);
        // Ensure we handle an all around complex case

        // Ensure we compute if a weights aren't specified
        let complex_spread = SpreadScalerProperty {
            replicas: 103,
            spread: vec![
                Spread {
                    // 9 + 1 (remainder trip)
                    name: "ComplexOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(42),
                },
                Spread {
                    // 0 + 1 (remainder trip)
                    name: "ComplexTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(3),
                },
                Spread {
                    // 8
                    name: "ComplexThree".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(37),
                },
                Spread {
                    // 84
                    name: "ComplexFour".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(384),
                },
            ],
        };
        let complex_spread_res = compute_spread(&complex_spread);
        assert_eq!(complex_spread_res[0].1, 10);
        assert_eq!(complex_spread_res[1].1, 1);
        assert_eq!(complex_spread_res[2].1, 8);
        assert_eq!(complex_spread_res[3].1, 84);

        Ok(())
    }

    #[tokio::test]
    async fn can_compute_spread_commands() -> Result<()> {
        let lattice_id = "hoohah_multi_stop_actor";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
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
                    annotations: HashMap::new(),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        // Ensure we compute if a weights aren't specified
        let complex_spread = SpreadScalerProperty {
            replicas: 103,
            spread: vec![
                Spread {
                    // 9 + 1 (remainder trip)
                    name: "ComplexOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(42),
                },
                Spread {
                    // 0 + 1 (remainder trip)
                    name: "ComplexTwo".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(3),
                },
                Spread {
                    // 8
                    name: "ComplexThree".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(37),
                },
                Spread {
                    // 84
                    name: "ComplexFour".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(384),
                },
            ],
        };

        let spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            actor_reference.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            complex_spread,
            "fake_component",
        );

        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 4);
        assert!(cmds.contains(&Command::StartActor(StartActor {
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 10,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexOne", spreadscaler.id())
        })));
        assert!(cmds.contains(&Command::StartActor(StartActor {
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 1,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexTwo", spreadscaler.id())
        })));
        assert!(cmds.contains(&Command::StartActor(StartActor {
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 8,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexThree", spreadscaler.id())
        })));
        assert!(cmds.contains(&Command::StartActor(StartActor {
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 84,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexFour", spreadscaler.id())
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
            replicas: 412,
            spread: vec![
                Spread {
                    name: "RunInFakeCloud".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                    weight: Some(50), // 206
                },
                Spread {
                    name: "RunInRealCloud".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "real".to_string())]),
                    weight: Some(25), // 103
                },
                Spread {
                    name: "RunInPurgatoryCloud".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "cloud".to_string(),
                        "purgatory".to_string(),
                    )]),
                    weight: Some(25), // 103
                },
            ],
        };

        let blobby_spread_property = SpreadScalerProperty {
            replicas: 9,
            spread: vec![
                Spread {
                    name: "CrossRegionCustom".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-brooks-1".to_string(),
                    )]),
                    weight: Some(33), // 3
                },
                Spread {
                    name: "CrossRegionReal".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-midwest-4".to_string(),
                    )]),
                    weight: Some(33), // 3
                },
                Spread {
                    name: "RunOnEdge".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "location".to_string(),
                        "edge".to_string(),
                    )]),
                    weight: Some(33), // 3
                },
            ],
        };

        let echo_spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            echo_ref.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            echo_spread_property,
            "fake_echo",
        );

        let blobby_spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            blobby_ref.to_string(),
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
                    capabilities: vec![],
                    issuer: "AASDASDASDASD".to_string(),
                    call_alias: None,
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // One instance on this host
                            HashSet::from_iter([WadmActorInstance {
                                instance_id: "1".to_string(),
                                annotations: spreadscaler_annotations(
                                    "RunInFakeCloud",
                                    echo_spreadscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 103 instances on this host
                            HashSet::from_iter((2..105).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations(
                                    "RunInRealCloud",
                                    echo_spreadscaler.id(),
                                ),
                            })),
                        ),
                        (
                            host_id_three.to_string(),
                            // 400 instances on this host
                            HashSet::from_iter((105..505).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations(
                                    "RunInPurgatoryCloud",
                                    echo_spreadscaler.id(),
                                ),
                            })),
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
                    capabilities: vec![],
                    issuer: "AASDASDASDASD".to_string(),
                    call_alias: None,
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 3 instances on this host
                            HashSet::from_iter((0..3).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations(
                                    "CrossRegionCustom",
                                    blobby_spreadscaler.id(),
                                ),
                            })),
                        ),
                        (
                            host_id_two.to_string(),
                            // 19 instances on this host
                            HashSet::from_iter((3..22).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations(
                                    "CrossRegionReal",
                                    blobby_spreadscaler.id(),
                                ),
                            })),
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
                    annotations: HashMap::new(),
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
                    annotations: HashMap::new(),
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
                    annotations: HashMap::new(),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_three.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        // STATE SETUP END

        let cmds = echo_spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);

        for cmd in cmds.iter() {
            match cmd {
                Command::StartActor(start) => {
                    assert_eq!(start.host_id, host_id_one.to_string());
                    assert_eq!(start.count, 205);
                    assert_eq!(start.reference, echo_ref);
                }
                Command::StopActor(stop) => {
                    assert_eq!(stop.host_id, host_id_three.to_string());
                    assert_eq!(stop.count, 297);
                    assert_eq!(stop.actor_id, echo_id);
                }
                _ => panic!("Unexpected command in spreadscaler list"),
            }
        }

        let cmds = blobby_spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);

        for cmd in cmds.iter() {
            match cmd {
                Command::StartActor(start) => {
                    assert_eq!(start.host_id, host_id_three.to_string());
                    assert_eq!(start.count, 3);
                    assert_eq!(start.reference, blobby_ref);
                }
                Command::StopActor(stop) => {
                    assert_eq!(stop.host_id, host_id_two.to_string());
                    assert_eq!(stop.count, 16);
                    assert_eq!(stop.actor_id, blobby_id);
                }
                _ => panic!("Unexpected command in spreadscaler list"),
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_handle_multiple_spread_matches() -> Result<()> {
        let lattice_id = "multiple_spread_matches";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let actor_id = "MASDASDASDASDASDADSDDAAASDDD".to_string();
        let host_id = "NASDASDIMAREALHOST";

        let store = Arc::new(TestStore::default());

        // Run 75% in east, 25% on resilient hosts
        let real_spread = SpreadScalerProperty {
            replicas: 20,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::from_iter([("region".to_string(), "east".to_string())]),
                    weight: Some(75),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "resilient".to_string(),
                        "true".to_string(),
                    )]),
                    weight: Some(25),
                },
            ],
        };

        let spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            actor_reference.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            real_spread,
            "fake_component",
        );

        // STATE SETUP BEGIN, ONE HOST
        store
            .store(
                lattice_id,
                host_id.to_string(),
                Host {
                    actors: HashMap::from_iter([(actor_id.to_string(), 10)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("region".to_string(), "east".to_string()),
                        ("resilient".to_string(), "true".to_string()),
                    ]),
                    annotations: HashMap::new(),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                actor_id.to_string(),
                Actor {
                    id: actor_id.to_string(),
                    name: "Faketor".to_string(),
                    capabilities: vec![],
                    issuer: "AASDASDASDASD".to_string(),
                    call_alias: None,
                    instances: HashMap::from_iter([(
                        host_id.to_string(),
                        // 10 instances on this host under the first spread
                        HashSet::from_iter((0..10).map(|n| WadmActorInstance {
                            instance_id: format!("{n}"),
                            annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                        })),
                    )]),
                    reference: actor_reference.to_string(),
                },
            )
            .await?;

        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);

        // Should be starting 10 total, 5 for each spread to meet the requirements
        assert!(cmds.contains(&Command::StartActor(StartActor {
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 5,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id())
        })));
        assert!(cmds.contains(&Command::StartActor(StartActor {
            reference: actor_reference.to_string(),
            host_id: host_id.to_string(),
            count: 5,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("SimpleTwo", spreadscaler.id())
        })));

        Ok(())
    }

    #[tokio::test]
    async fn calculates_proper_stop_commands() -> Result<()> {
        let lattice_id = "calculates_proper_stop_commands";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let actor_id = "MASDASDASDASDASDADSDDAAASDDD".to_string();
        let host_id = "NASDASDIMAREALHOST";
        let host_id2 = "NASDASDIMAREALHOST2";

        let store = Arc::new(TestStore::default());

        let real_spread = SpreadScalerProperty {
            // Makes it so we always get at least 2 commands
            replicas: 9,
            spread: Vec::new(),
        };

        let spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            actor_reference.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            real_spread,
            "fake_component",
        );

        // STATE SETUP BEGIN, ONE HOST
        store
            .store_many(
                lattice_id,
                [
                    (
                        host_id.to_string(),
                        Host {
                            actors: HashMap::from_iter([(actor_id.to_string(), 10)]),
                            friendly_name: "hey".to_string(),
                            labels: HashMap::new(),
                            annotations: HashMap::new(),
                            providers: HashSet::new(),
                            uptime_seconds: 123,
                            version: None,
                            id: host_id.to_string(),
                            last_seen: Utc::now(),
                        },
                    ),
                    (
                        host_id2.to_string(),
                        Host {
                            actors: HashMap::from_iter([(actor_id.to_string(), 10)]),
                            friendly_name: "hey2".to_string(),
                            labels: HashMap::new(),
                            annotations: HashMap::new(),
                            providers: HashSet::new(),
                            uptime_seconds: 123,
                            version: None,
                            id: host_id2.to_string(),
                            last_seen: Utc::now(),
                        },
                    ),
                ],
            )
            .await?;

        store
            .store(
                lattice_id,
                actor_id.to_string(),
                Actor {
                    id: actor_id.to_string(),
                    name: "Faketor".to_string(),
                    capabilities: vec![],
                    issuer: "AASDASDASDASD".to_string(),
                    call_alias: None,
                    instances: HashMap::from_iter([
                        (
                            host_id.to_string(),
                            HashSet::from_iter((0..10).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations("default", spreadscaler.id()),
                            })),
                        ),
                        (
                            host_id2.to_string(),
                            HashSet::from_iter((0..10).map(|n| WadmActorInstance {
                                instance_id: format!("blah{n}"),
                                annotations: spreadscaler_annotations("default", spreadscaler.id()),
                            })),
                        ),
                    ]),
                    reference: actor_reference.to_string(),
                },
            )
            .await?;

        // Make sure they get at least 2 commands for stopping, one from each host. We don't know
        // which one will have more stopped, but both should show up
        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);
        assert!(
            cmds.iter().any(|command| {
                if let Command::StopActor(actor) = command {
                    actor.host_id == host_id
                } else {
                    false
                }
            }),
            "Should have found both hosts for stopping commands"
        );
        assert!(
            cmds.iter().any(|command| {
                if let Command::StopActor(actor) = command {
                    actor.host_id == host_id2
                } else {
                    false
                }
            }),
            "Should have found both hosts for stopping commands"
        );

        // Now check that cleanup removes everything
        let cmds = spreadscaler.cleanup().await?;

        // Should stop 10 on each host
        assert!(cmds.contains(&Command::StopActor(StopActor {
            actor_id: actor_id.clone(),
            host_id: host_id.to_string(),
            count: 10,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("default", spreadscaler.id())
        })));
        assert!(cmds.contains(&Command::StopActor(StopActor {
            actor_id: actor_id.clone(),
            host_id: host_id2.to_string(),
            count: 10,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("default", spreadscaler.id())
        })));
        Ok(())
    }

    #[tokio::test]
    async fn can_react_to_events() -> Result<()> {
        let lattice_id = "computing_spread_commands";
        let blobby_ref = "fakecloud.azurecr.io/blobby:0.5.2".to_string();
        let blobby_id = "MASDASDIAMAREALACTORBLOBBY";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";
        let host_id_three = "NASDASDIMAREALHOSTTREE";

        let store = Arc::new(TestStore::default());

        let lattice_source = TestLatticeSource::default();
        let command_publisher = CommandPublisher::new(NoopPublisher, "doesntmatter");
        let status_publisher = StatusPublisher::new(NoopPublisher, "doesntmatter");
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
                lattice_source,
            )
            .await,
        );
        let blobby_spread_property = SpreadScalerProperty {
            replicas: 9,
            spread: vec![
                Spread {
                    name: "CrossRegionCustom".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-brooks-1".to_string(),
                    )]),
                    weight: Some(33), // 3
                },
                Spread {
                    name: "CrossRegionReal".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-midwest-4".to_string(),
                    )]),
                    weight: Some(33), // 3
                },
                Spread {
                    name: "RunOnEdge".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "location".to_string(),
                        "edge".to_string(),
                    )]),
                    weight: Some(33), // 3
                },
            ],
        };

        let blobby_spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            blobby_ref.to_string(),
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
                    capabilities: vec![],
                    issuer: "AASDASDASDASD".to_string(),
                    call_alias: None,
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 3 instances on this host
                            HashSet::from_iter((0..3).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations(
                                    "CrossRegionCustom",
                                    blobby_spreadscaler.id(),
                                ),
                            })),
                        ),
                        (
                            host_id_two.to_string(),
                            // 19 instances on this host
                            HashSet::from_iter((3..22).map(|n| WadmActorInstance {
                                instance_id: format!("{n}"),
                                annotations: spreadscaler_annotations(
                                    "CrossRegionReal",
                                    blobby_spreadscaler.id(),
                                ),
                            })),
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
                        (blobby_id.to_string(), 3),
                        ("MSOMEOTHERACTOR".to_string(), 3),
                    ]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-brooks-1".to_string()),
                    ]),
                    annotations: HashMap::new(),
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
                    actors: HashMap::from_iter([(blobby_id.to_string(), 19)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-midwest-4".to_string()),
                        ("label".to_string(), "value".to_string()),
                    ]),
                    annotations: HashMap::new(),
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
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "purgatory".to_string()),
                        ("location".to_string(), "edge".to_string()),
                    ]),
                    annotations: HashMap::new(),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_three.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;
        // STATE SETUP END

        // Don't care about these events
        assert!(blobby_spreadscaler
            .handle_event(&Event::ProviderStarted(ProviderStarted {
                annotations: HashMap::new(),
                claims: ProviderClaims::default(),
                contract_id: "".to_string(),
                image_ref: "".to_string(),
                instance_id: "".to_string(),
                link_name: "".to_string(),
                public_key: "".to_string(),
                host_id: host_id_one.to_string()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(&Event::ProviderStopped(ProviderStopped {
                annotations: HashMap::default(),
                contract_id: "".to_string(),
                instance_id: "".to_string(),
                link_name: "".to_string(),
                public_key: "".to_string(),
                reason: "".to_string(),
                host_id: host_id_two.to_string()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(&Event::LinkdefSet(LinkdefSet {
                linkdef: Linkdef::default()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(&Event::LinkdefDeleted(LinkdefDeleted {
                linkdef: Linkdef::default()
            }))
            .await?
            .is_empty());

        let cmds = blobby_spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);

        for cmd in cmds.iter() {
            match cmd {
                Command::StartActor(start) => {
                    assert_eq!(start.host_id, host_id_three.to_string());
                    assert_eq!(start.count, 3);
                    assert_eq!(start.reference, blobby_ref);
                }
                Command::StopActor(stop) => {
                    assert_eq!(stop.host_id, host_id_two.to_string());
                    assert_eq!(stop.count, 16);
                    assert_eq!(stop.actor_id, blobby_id);
                }
                _ => panic!("Unexpected command in spreadscaler list"),
            }
        }

        // NOTE(brooksmtownsend): Regular ActorStopped modify state, ActorsStopped
        // is what scalers care about. Should be converted to ActorsStopped once state
        // updates don't rely on them anymore.

        // Stop an instance of an actor, and expect a modified list of commands
        let state_modifying_event = ActorStopped {
            annotations: HashMap::new(),
            instance_id: "12".to_string(),
            public_key: blobby_id.to_string(),
            host_id: host_id_two.to_string(),
        };
        let scaler_modifying_event = ActorsStopped {
            annotations: HashMap::new(),
            public_key: blobby_id.to_string(),
            host_id: host_id_two.to_string(),
            count: 1,
            remaining: 15,
        };

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::ActorStopped(state_modifying_event.clone()),
                acker: None,
            })
            .await
            .expect("should be able to handle an event");

        let cmds = blobby_spreadscaler
            .handle_event(&Event::ActorsStopped(scaler_modifying_event))
            .await?;
        assert_eq!(cmds.len(), 2);

        for cmd in cmds.iter() {
            match cmd {
                Command::StartActor(start) => {
                    assert_eq!(start.host_id, host_id_three.to_string());
                    assert_eq!(start.count, 3);
                    assert_eq!(start.reference, blobby_ref);
                }
                Command::StopActor(stop) => {
                    assert_eq!(stop.host_id, host_id_two.to_string());
                    assert_eq!(stop.count, 15);
                    assert_eq!(stop.actor_id, blobby_id);
                }
                _ => panic!("Unexpected command in spreadscaler list"),
            }
        }

        Ok(())
    }
}
