use anyhow::Result;
use async_trait::async_trait;
use std::{
    cmp::{self, Ordering},
    collections::{HashMap, HashSet},
};
use tracing::log::warn;

use crate::{
    commands::{Command, StartActor, StopActor},
    events::Event,
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

    fn update_config(&mut self, config: Self::Config) -> Result<bool> {
        self.config = config;
        Ok(true)
    }

    async fn handle_event(&self, event: Event) -> Result<HashSet<Command>> {
        match event {
            // TODO Spread out to better encapsulate what needs to happen in each scenario

            // see if we need to reduce actor instances
            Event::ActorStarted(_)
            // see if we need to increase actor instances
            | Event::ActorStopped(_)
            // see if we need to adjust spread
            | Event::HostStopped(_)
            // see if we need to adjust spread
            | Event::HostStarted(_) => {
                todo!()
            }
            // No other event impacts the job of this scaler so we can ignore it
            _ => Ok(HashSet::new()),
        }
    }

    async fn reconcile(&self) -> Result<HashSet<Command>> {
        todo!()
    }
}

impl<S: ReadStore + Send + Sync> ActorSpreadScaler<S> {
    #[allow(unused)]
    /// Construct a new SimpleActorScaler with specified configuration values
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

    /// Given a spread config, compute the necessary commands to properly spread actors across a set
    /// of hosts
    async fn spread_commands(&self) -> Result<HashSet<Command>> {
        let spread_requirements = compute_spread(&self.config.spread_config)?;

        let hosts = self.store.list::<Host>(&self.config.lattice_id).await?;

        let actor_id = self
            .store
            .list::<Actor>(&self.config.lattice_id)
            .await?
            .iter()
            .find(|(_id, actor)| actor.reference == self.config.actor_reference)
            .map(|(id, _actor)| id.to_owned())
            // Default here means the below `get` will find zero running actors, which is fine because
            // that accurately describes the current lattice having zero instances.
            .unwrap_or_default();

        // NOTE(brooksmtownsend) it's easier to assign one host per list of requirements than
        // balance within those requirements. Users should be specific with their requirements
        // as wadm is not responsible for ambiguity

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

                    let final_cmd = match current_count.cmp(count) {
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
                    };
                    final_cmd
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
    // ideal return would be a Vec<(num_actors, Spread)>?
    let replicas = spread_config.replicas;
    // TODO: what should the default weight be? 100 would clobber anyone who uses 1, maybe default is the
    // same as the minimum weight specified?
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

    // Sanity check to see if we need to add more replicas
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
        model::{Spread, SpreadScalerProperty},
        scaler::spreadscaler::ActorSpreadScaler,
        storage::{Host, Store},
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
        let actor_id = "MASDASDIAMAREALACTOR";
        let host_id = "NASDASDIMAREALHOST";

        let store = Arc::new(TestStore::default());
        let worker = EventWorker::new(store.clone(), HashMap::default());

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

        let cmds = spreadscaler.spread_commands().await?;
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
}
