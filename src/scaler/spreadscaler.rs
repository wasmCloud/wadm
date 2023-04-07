use anyhow::Result;
use async_trait::async_trait;
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};
use tracing::log::warn;

use crate::{
    commands::{Command, StartActor, StopActor},
    events::{Event, HostStarted, HostStopped},
    model::{Spread, SpreadScalerProperty, DEFAULT_SPREAD_WEIGHT},
    scaler::Scaler,
    storage::{Actor, Host, ReadStore},
};

/// Config for an ActorSpreadScaler
struct ActorSpreadConfig {
    actor_reference: String,
    lattice_id: String,
    spread_config: SpreadScalerProperty,
}

/// The SimpleScaler ensures that a certain number of replicas are running
/// for a certain public key.
///
/// This is primarily to demonstrate the functionality and ergonomics of the
/// [Scaler](crate::scaler::Scaler) trait and doesn't make any guarantees
/// about spreading replicas evenly
struct ActorSpreadScaler<S: ReadStore + Send + Sync> {
    pub config: ActorSpreadConfig,
    store: S,
}

#[async_trait]
impl<S: ReadStore + Send + Sync> Scaler for ActorSpreadScaler<S> {
    type Config = ActorSpreadConfig;

    async fn update_config(&mut self, config: Self::Config) -> Result<HashSet<Command>> {
        self.config = config;
        self.reconcile().await
    }

    async fn handle_event(&self, event: Event) -> Result<HashSet<Command>> {
        match event {
            Event::ActorStarted(actor_started) => {
                if actor_started.image_ref == self.config.actor_reference {
                    self.reconcile().await
                } else {
                    Ok(HashSet::new())
                }
            }
            Event::ActorStopped(actor_stopped) => {
                let actor_id = self.actor_id().await?;
                // Different actor, no action needed
                if actor_stopped.public_key == actor_id {
                    self.reconcile().await
                } else {
                    Ok(HashSet::new())
                }
            }
            Event::HostStopped(HostStopped { labels, .. })
            | Event::HostStarted(HostStarted { labels, .. }) => {
                // If this host's labels match any spread requirement, perform reconcile
                if self
                    .config
                    .spread_config
                    .spread
                    .iter()
                    .find(|spread| {
                        spread.requirements.iter().all(|(key, value)| {
                            labels.get(key).map(|val| val == value).unwrap_or(false)
                        })
                    })
                    .is_some()
                {
                    Ok(HashSet::new())
                } else {
                    self.reconcile().await
                }
            }
            // No other event impacts the job of this scaler so we can ignore it
            _ => Ok(HashSet::new()),
        }
    }

    async fn reconcile(&self) -> Result<HashSet<Command>> {
        let spread_requirements = compute_spread(&self.config.spread_config)?;
        let hosts = self.store.list::<Host>(&self.config.lattice_id).await?;
        let actor_id = self.actor_id().await?;

        // NOTE(brooksmtownsend) it's easier to assign one host per list of requirements than
        // balance within those requirements. Users should be specific with their requirements
        // as wadm is not responsible for ambiguity, a future scaler like a DaemonScaler could handle this

        let commands = spread_requirements
            .iter()
            .filter_map(|(spread, count)| {
                let eligible_hosts = eligible_hosts(&hosts, spread);
                if let Some(first_host) = eligible_hosts.get(0) {
                    // NOTE(brooksmtownsend): Once we care about annotations, we'll need to check that annotations
                    // match here to compute the current count
                    // Compute all current actors running on this spread's eligible hosts
                    let current_count = eligible_hosts.iter().fold(0, |total, host| {
                        total + *host.actors.get(&actor_id).unwrap_or(&0)
                    });

                    match current_count.cmp(count) {
                        // No action needed
                        Ordering::Equal => None,
                        // Start actors to reach desired replicas
                        Ordering::Less => Some(Command::StartActor(StartActor {
                            reference: self.config.actor_reference.to_owned(),
                            host_id: first_host.id.to_owned(),
                            count: count - current_count,
                        })),
                        // Stop actors to reach desired replicas
                        Ordering::Greater => Some(Command::StopActor(StopActor {
                            actor_id: actor_id.to_owned(),
                            host_id: first_host.id.to_owned(),
                            count: current_count - count,
                        })),
                    }
                } else {
                    // No hosts were eligible, so we can't attempt to add or remove actors
                    None
                }
            })
            // Collapse multiple commands for the same actor and same host
            // into single StopActor commands
            .fold(HashSet::new(), |mut cmd_set, cmd| {
                if let Some(prev_cmd) = cmd_set.get(&cmd) {
                    match (prev_cmd, cmd) {
                        (Command::StartActor(prev), Command::StartActor(new)) => {
                            let thing_to_add = Command::StartActor(StartActor {
                                count: prev.count + new.count,
                                ..new
                            });
                            cmd_set.replace(thing_to_add);
                            cmd_set
                        }
                        _ => cmd_set,
                    }
                } else {
                    cmd_set.insert(cmd);
                    cmd_set
                }
            });

        Ok(commands)
    }
}

impl<S: ReadStore + Send + Sync> ActorSpreadScaler<S> {
    #[allow(unused)]
    /// Construct a new ActorSpreadScaler with specified configuration values
    fn new(
        store: S,
        actor_reference: String,
        lattice_id: String,
        spread_config: SpreadScalerProperty,
    ) -> Self {
        Self {
            store,
            config: ActorSpreadConfig {
                actor_reference,
                lattice_id,
                spread_config,
            },
        }
    }

    // TODO(brooksmtownsend): Need to consider a better way to grab this. It will fail to find
    // the actor ID if it's not running in the lattice currently, which seems brittle
    /// Helper function to retrieve the actor ID for the configured actor
    async fn actor_id(&self) -> Result<String> {
        Ok(self
            .store
            .list::<Actor>(&self.config.lattice_id)
            .await?
            .iter()
            .find(|(_id, actor)| actor.reference == self.config.actor_reference)
            .map(|(id, _actor)| id.to_owned())
            // Default here means the below `get` will find zero running actors, which is fine because
            // that accurately describes the current lattice having zero instances.
            .unwrap_or_default())
    }
}

/// Helper function that computes a list of eligible hosts to match with a spread
fn eligible_hosts<'a>(all_hosts: &'a HashMap<String, Host>, spread: &Spread) -> Vec<&'a Host> {
    all_hosts
        .iter()
        .filter(|(_id, host)| {
            spread
                .requirements
                .iter()
                .all(|(key, value)| host.labels.get(key).map(|v| v.eq(value)).unwrap_or(false))
        })
        .map(|(_id, host)| host)
        .collect::<Vec<&Host>>()
}

/// Given a spread config, return a vector of tuples that represents the spread
/// and the actual number of actors to start for a specific spread requirement
fn compute_spread(spread_config: &SpreadScalerProperty) -> Result<Vec<(&Spread, usize)>> {
    let replicas = spread_config.replicas;
    let total_weight = spread_config
        .spread
        .iter()
        .map(|s| s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT))
        .sum::<usize>();

    let spreads: Vec<(&Spread, usize)> = spread_config
        .spread
        .iter()
        .map(|s| {
            (
                s,
                // Order is important here since usizes chop off remaining decimals
                (replicas * s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT)) / total_weight,
            )
        })
        .collect();

    // Because of math, we may end up rounding a few instances away. Evenly distribute them
    // among the remaining hosts
    let total_replicas = spreads.iter().map(|(_s, count)| count).sum::<usize>();
    let spreads = match total_replicas.cmp(&replicas) {
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
    };

    Ok(spreads)
}

#[cfg(test)]
mod test {
    use anyhow::Result;
    use chrono::Utc;
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::Arc,
    };

    use crate::{
        commands::{Command, StartActor},
        consumers::{manager::Worker, ScopedMessage},
        events::{
            ActorStopped, Event, Linkdef, LinkdefDeleted, LinkdefSet, ProviderClaims,
            ProviderStarted, ProviderStopped,
        },
        model::{Spread, SpreadScalerProperty},
        scaler::{spreadscaler::ActorSpreadScaler, Scaler},
        storage::{Actor, Host, Store},
        test_util::TestStore,
        workers::EventWorker,
    };

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

        let simple_spread_res = compute_spread(&simple_spread)?;
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

        let multi_spread_even_res = compute_spread(&multi_spread_even)?;
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

        let multi_spread_even_res = compute_spread(&multi_spread_odd)?;
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

        let multi_spread_even_res = compute_spread(&multi_spread_odd)?;
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

        let multi_spread_even_no_weight = compute_spread(&multi_spread_even_no_weight)?;
        assert_eq!(multi_spread_even_no_weight[0].1, 5);
        assert_eq!(multi_spread_even_no_weight[1].1, 5);

        // Ensure we compute if weights are partially specified
        // Ensure we compute if spread vec is empty?
        let simple_spread_replica_only = SpreadScalerProperty {
            replicas: 12,
            spread: vec![],
        };

        let simple_replica_only = compute_spread(&simple_spread_replica_only)?;
        // NOTE(brooksmtownsend): The defaut behavior is to return no spreads, consumers
        // of this function should be responsible for knowing that there are no requirements
        assert_eq!(simple_replica_only.len(), 0);
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
        let complex_spread_res = compute_spread(&complex_spread)?;
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
            complex_spread,
        );

        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 1);
        assert_eq!(
            cmds.iter().next().expect("should have one command"),
            &Command::StartActor(StartActor {
                reference: actor_reference.to_string(),
                host_id: host_id.to_string(),
                count: 103,
            })
        );

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
                    count: HashMap::from_iter([
                        (host_id_one.to_string(), 1),
                        (host_id_two.to_string(), 103),
                        (host_id_three.to_string(), 400),
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
                    count: HashMap::from_iter([
                        (host_id_one.to_string(), 2),
                        (host_id_two.to_string(), 19),
                    ]),
                    reference: blobby_ref.to_string(),
                },
            )
            .await?;

        // STATE SETUP BEGIN
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
            echo_spread_property,
        );

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

        let blobby_spreadscaler = ActorSpreadScaler::new(
            store.clone(),
            blobby_ref.to_string(),
            lattice_id.to_string(),
            blobby_spread_property,
        );

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

    /// NOTE(brooksmtownsend): This test currently doesn't pass due to the need for annotations.
    /// It's included so we can bring the functionality in later and have it properly work.
    #[tokio::test]
    async fn can_handle_multiple_spread_matches() -> Result<()> {
        let lattice_id = "multiple_spread_matches";
        let actor_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let actor_id = "MASDASDASDASDASDADSDDAAASDDD".to_string();
        let host_id = "NASDASDIMAREALHOST";

        let store = Arc::new(TestStore::default());

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
                    count: HashMap::from_iter([(host_id.to_string(), 10)]),
                    reference: actor_reference.to_string(),
                },
            )
            .await?;

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
            real_spread,
        );

        let _cmds = spreadscaler.reconcile().await?;
        // NOTE(brooksmtownsend): IDEALLY we would start 10 instances on the host
        // in order to satisfy the requirements of 20 replicas, however each spread
        // is evaluated individually and the existing actors aren't properly divvied
        // up into the spreads. Need annotations to do this properly.
        assert!(true);

        // assert_eq!(cmds.len(), 1);
        // assert_eq!(
        //     cmds.iter().next().expect("should have one command"),
        //     &Command::StartActor(StartActor {
        //         reference: actor_reference.to_string(),
        //         host_id: host_id.to_string(),
        //         count: 10,
        //     })
        // );

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
        let worker = EventWorker::new(store.clone(), HashMap::default());

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
                    count: HashMap::from_iter([
                        (host_id_one.to_string(), 3),
                        (host_id_two.to_string(), 19),
                    ]),
                    reference: blobby_ref.to_string(),
                },
            )
            .await?;

        // STATE SETUP BEGIN
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
            blobby_spread_property,
        );

        // Don't care about these events
        assert!(blobby_spreadscaler
            .handle_event(Event::ProviderStarted(ProviderStarted {
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
            .handle_event(Event::ProviderStopped(ProviderStopped {
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
            .handle_event(Event::LinkdefSet(LinkdefSet {
                linkdef: Linkdef::default()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(Event::LinkdefDeleted(LinkdefDeleted {
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

        // Stop an instance of an actor, and expect a modified list of commands

        let modifying_event = ActorStopped {
            instance_id: "IAMANINSTANCE".to_string(),
            public_key: blobby_id.to_string(),
            host_id: host_id_two.to_string(),
        };

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::ActorStopped(modifying_event.clone()),
                acker: None,
            })
            .await
            .expect("should be able to handle an event");

        let cmds = blobby_spreadscaler
            .handle_event(Event::ActorStopped(modifying_event))
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
