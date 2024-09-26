use std::collections::{BTreeMap, HashSet};
use std::{cmp::Ordering, cmp::Reverse, collections::HashMap};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{instrument, trace, warn};
use wadm_types::{
    api::StatusInfo, Spread, SpreadScalerProperty, TraitProperty, DEFAULT_SPREAD_WEIGHT,
};

use crate::events::HostHeartbeat;
use crate::{
    commands::{Command, ScaleComponent},
    events::{Event, HostStarted, HostStopped},
    scaler::Scaler,
    storage::{Component, Host, ReadStore},
    SCALER_KEY,
};

use super::compute_id_sha256;

pub mod link;
pub mod provider;

// Annotation constants
const SPREAD_KEY: &str = "wasmcloud.dev/spread_name";

pub const SPREAD_SCALER_KIND: &str = "SpreadScaler";

/// Config for a ComponentSpreadScaler
#[derive(Clone)]
struct ComponentSpreadConfig {
    /// OCI, Bindle, or File reference for a component
    component_reference: String,
    /// Unique component identifier for a component
    component_id: String,
    /// Lattice ID that this SpreadScaler monitors
    lattice_id: String,
    /// The name of the wadm model this SpreadScaler is under
    model_name: String,
    /// Configuration for this SpreadScaler
    spread_config: SpreadScalerProperty,
}

/// The ComponentSpreadScaler ensures that a certain number of instances are running,
/// spread across a number of hosts according to a [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of instances
/// on an available host
pub struct ComponentSpreadScaler<S> {
    spread_config: ComponentSpreadConfig,
    spread_requirements: Vec<(Spread, usize)>,
    store: S,
    id: String,
    status: RwLock<StatusInfo>,
    /// Named configuration to pass to the component.
    pub config: Vec<String>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ComponentSpreadScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        SPREAD_SCALER_KIND
    }

    fn name(&self) -> String {
        self.spread_config.component_id.to_string()
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        let spread_config = match config {
            TraitProperty::SpreadScaler(prop) => prop,
            _ => anyhow::bail!("Given config was not a spread scaler config object"),
        };
        self.spread_config.spread_config = spread_config;
        self.spread_requirements = compute_spread(&self.spread_config.spread_config);
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
            Event::ComponentScaled(evt) if evt.component_id == self.spread_config.component_id => {
                self.reconcile().await
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

    #[instrument(level = "trace", skip_all, fields(name = %self.spread_config.model_name, scaler_id = %self.id))]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        let component_id = &self.spread_config.component_id;
        let component = self
            .store
            .get::<Component>(&self.spread_config.lattice_id, component_id)
            .await?;

        let hosts = self
            .store
            .list::<Host>(&self.spread_config.lattice_id)
            .await?;

        let ineligible_hosts = compute_ineligible_hosts(
            &hosts,
            self.spread_requirements
                .iter()
                .map(|(s, _)| s)
                .collect::<Vec<&Spread>>(),
        );

        // Remove any components that are managed by this scaler and running on ineligible hosts
        let remove_ineligible: Vec<Command> = ineligible_hosts
            .iter()
            .filter_map(|(host_id, host)| {
                if host.components.contains_key(component_id) {
                    Some(Command::ScaleComponent(ScaleComponent {
                        component_id: component_id.to_owned(),
                        reference: self.spread_config.component_reference.to_owned(),
                        host_id: host_id.to_string(),
                        count: 0,
                        model_name: self.spread_config.model_name.to_owned(),
                        annotations: BTreeMap::new(),
                        config: self.config.clone(),
                    }))
                } else {
                    None
                }
            })
            .collect();
        // If we found any components running on ineligible hosts, remove them before
        // attempting to scale up or down.
        if !remove_ineligible.is_empty() {
            let status = StatusInfo::reconciling(
                "Found components running on ineligible hosts, removing them.",
            );
            trace!(?status, "Updating scaler status");
            *self.status.write().await = status;
            return Ok(remove_ineligible);
        }

        let mut spread_status = vec![];
        trace!(spread_requirements = ?self.spread_requirements, ?component_id, "Computing commands");
        let commands = self
            .spread_requirements
            .iter()
            .filter_map(|(spread, count)| {
                // Narrow down eligible hosts to those that match this spread's requirements
                let eligible_hosts = eligible_hosts(&hosts, spread);
                if !eligible_hosts.is_empty() {
                    // In the future we may want more information from this chain, but for now
                    // we just need the number of running components that match this spread's annotations

                    // Parse the instances into a map of host_id -> number of running components managed
                    // by this scaler. Ignoring ones where we aren't running anything
                    let running_components_per_host: HashMap<&String, usize> = component
                        .as_ref()
                        .map(|component| &component.instances)
                        .map(|instances| {
                            instances
                                .iter()
                                .filter_map(|(host_id, instances)| {
                                    let count = instances
                                        .iter()
                                        .filter_map(|info| {
                                            spreadscaler_annotations(&spread.name, self.id()).iter().all(
                                                |(key, value)| {
                                                    info.annotations
                                                        .get(key)
                                                        .map(|v| v == value)
                                                        .unwrap_or(false)
                                                },
                                            ).then_some(info.count)
                                        })
                                        .sum();
                                    (count > 0).then_some((host_id, count))
                                }).collect()
                        })
                        .unwrap_or_default();
                    let current_count: usize = running_components_per_host.values().sum();
                    trace!(current = %current_count, expected = %count, "Calculated running components, reconciling with expected count");
                    // Here we'll generate commands for the proper host depending on where they are running
                    match current_count.cmp(count) {
                        Ordering::Equal => None,
                        // Start components to reach desired instances
                        Ordering::Less =>{
                            // Right now just start on the first available host. We can be smarter about it later
                            Some(vec![Command::ScaleComponent(ScaleComponent {
                                component_id: component_id.to_owned(),
                                reference: self.spread_config.component_reference.to_owned(),
                                // SAFETY: We already checked that the list of hosts is not empty, so we can unwrap here
                                host_id: eligible_hosts.keys().next().unwrap().to_string(),
                                count: *count as u32,
                                model_name: self.spread_config.model_name.to_owned(),
                                annotations: spreadscaler_annotations(&spread.name, self.id()),
                                        config: self.config.clone(),
                            })])
                        }
                        // Stop components to reach desired instances
                        Ordering::Greater => {
                            // Components across all available hosts that exceed our desired number
                            let count_to_stop = current_count - count;
                            let (_, commands) = running_components_per_host.into_iter().fold((0usize, Vec::new()), |(mut current_stopped, mut commands), (host_id, instance_count)| {
                                let remaining_to_stop = count_to_stop - current_stopped;
                                // Desired count on the host, subtracting the number we need to stop
                                // from the total number of instances on the host, down to 0.
                                let count = instance_count.saturating_sub(remaining_to_stop);
                                // If there aren't any on here then we don't need a command to stop
                                if instance_count > 0 {
                                    // Keep track of how many we've stopped, which will be the smaller of the current
                                    // instance count or the number we need to stop
                                    current_stopped += std::cmp::min(instance_count, remaining_to_stop);
                                    commands.push(Command::ScaleComponent(ScaleComponent {
                                        component_id: component_id.to_owned(),
                                        reference: self.spread_config.component_reference.to_owned(),
                                        host_id: host_id.to_owned(),
                                        count: count as u32,
                                        model_name: self.spread_config.model_name.to_owned(),
                                        annotations: spreadscaler_annotations(&spread.name, self.id()),
                                        config: self.config.clone(),
                                    }));
                                }
                                (current_stopped, commands)
                            });
                            Some(commands)
                        }
                    }
                } else {
                    // No hosts were eligible, so we can't attempt to add or remove components
                    trace!(?spread.name, "Found no eligible hosts for spread");
                    spread_status.push(StatusInfo::failed(&format!("Could not satisfy spread {} for {}, 0/1 eligible hosts found.", spread.name, self.spread_config.component_reference)));
                    None
                }
            })
            .flatten()
            .collect::<Vec<Command>>();
        trace!(?commands, "Calculated commands for component scaler");

        let status = match (spread_status.is_empty(), commands.is_empty()) {
            // No failures, no commands, scaler satisfied
            (true, true) => StatusInfo::deployed(""),
            // No failures, commands generated, scaler is reconciling
            (true, false) => {
                StatusInfo::reconciling(&format!("Scaling component on {} host(s)", commands.len()))
            }
            // Failures occurred, scaler is in a failed state
            (false, _) => StatusInfo::failed(
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

    #[instrument(level = "trace", skip_all, fields(name = %self.spread_config.model_name))]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        let mut config_clone = self.spread_config.clone();
        config_clone.spread_config.instances = 0;
        let spread_requirements = compute_spread(&config_clone.spread_config);

        let cleanerupper = ComponentSpreadScaler {
            spread_config: config_clone,
            store: self.store.clone(),
            spread_requirements,
            id: self.id.clone(),
            status: RwLock::new(StatusInfo::reconciling("")),
            config: self.config.clone(),
        };

        cleanerupper.reconcile().await
    }
}

impl<S: ReadStore + Send + Sync> ComponentSpreadScaler<S> {
    /// Construct a new ComponentSpreadScaler with specified configuration values
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: S,
        component_reference: String,
        component_id: String,
        lattice_id: String,
        model_name: String,
        spread_config: SpreadScalerProperty,
        component_name: &str,
        config: Vec<String>,
    ) -> Self {
        // Compute the id of this scaler based on all of the configuration values
        // that make it unique. This is used during upgrades to determine if a
        // scaler is the same as a previous one.
        let mut id_parts = vec![
            SPREAD_SCALER_KIND,
            &model_name,
            component_name,
            &component_id,
            &component_reference,
        ];
        id_parts.extend(config.iter().map(std::string::String::as_str));
        let id = compute_id_sha256(&id_parts);

        Self {
            store,
            spread_requirements: compute_spread(&spread_config),
            spread_config: ComponentSpreadConfig {
                component_reference,
                component_id,
                lattice_id,
                spread_config,
                model_name,
            },
            id,
            config,
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}

/// Helper function to create a predictable annotations map for a spread
pub(crate) fn spreadscaler_annotations(
    spread_name: &str,
    scaler_id: &str,
) -> BTreeMap<String, String> {
    BTreeMap::from_iter([
        (SCALER_KEY.to_string(), scaler_id.to_string()),
        (SPREAD_KEY.to_string(), spread_name.to_string()),
    ])
}

/// Helper function that computes a list of eligible hosts to match with a spread
pub(crate) fn eligible_hosts<'a>(
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
        .collect()
}

/// Helper function that computes a list of ineligible hosts that match none of the spread requirements
pub(crate) fn compute_ineligible_hosts<'a>(
    all_hosts: &'a HashMap<String, Host>,
    spreads: Vec<&Spread>,
) -> HashMap<&'a String, &'a Host> {
    // Find all host IDs that are eligible for any spread
    let eligible_ids = spreads
        .iter()
        .flat_map(|spread| eligible_hosts(all_hosts, spread).into_keys())
        .collect::<HashSet<_>>();

    // Filter out all hosts that are eligible for any spread, leaving only ineligible hosts
    all_hosts
        .iter()
        .filter(|(id, _)| !eligible_ids.contains(id))
        .collect::<HashMap<_, _>>()
}

/// Given a spread config, return a vector of tuples that represents the spread
/// and the actual number of components to start for a specific spread requirement
fn compute_spread(spread_config: &SpreadScalerProperty) -> Vec<(Spread, usize)> {
    let requested_instances = spread_config.instances;
    let mut requested_spreads = spread_config.spread.clone();
    requested_spreads.sort_by_key(|s| Reverse(s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT)));

    let total_weight = requested_spreads
        .iter()
        .map(|s| s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT))
        .sum::<usize>();
    let computed_spreads: Vec<(Spread, usize)> = requested_spreads
        .iter()
        .map(|s| {
            (
                s.to_owned(),
                // Order is important here since usizes chop off remaining decimals
                (requested_instances * s.weight.unwrap_or(DEFAULT_SPREAD_WEIGHT)) / total_weight,
            )
        })
        .collect();

    let computed_spreads = if computed_spreads.is_empty() {
        vec![(Spread::default(), requested_instances)]
    } else {
        computed_spreads
    };

    // Because of math, we may end up rounding a few instances away. Evenly distribute them
    // among the remaining hosts
    let computed_instances = computed_spreads
        .iter()
        .map(|(_, count)| count)
        .sum::<usize>();
    let computed_spreads = match computed_instances.cmp(&requested_instances) {
        // To meet the specified/requested number of instances, evenly distribute the remaining unassigned instances among the computed spreads
        Ordering::Less => {
            let mut diff = requested_instances - computed_instances;
            computed_spreads
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
            warn!("Requesting more component instances than were specified");
            computed_spreads
        }
        Ordering::Equal => computed_spreads,
    };

    // If there are no spreads specified, we should only have one (default) spread with the total number of instances
    if requested_spreads.is_empty() && computed_spreads.len() == 1 {
        return computed_spreads;
    }

    // this is an invariant check; the predicate should not be true.
    if computed_spreads.len() != requested_spreads.len() {
        eprintln!("Computed spreads and requested spreads are not the same length. This should not happen.");
    }

    // The output has spreads ordered by weight; we may want to return them per the order in the config file at some point. For now, this will do.
    computed_spreads
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
    use wadm_types::{Spread, SpreadScalerProperty};
    use wasmcloud_control_interface::Link;

    use crate::{
        commands::Command,
        consumers::{manager::Worker, ScopedMessage},
        events::{
            ComponentScaled, Event, LinkdefDeleted, LinkdefSet, ProviderStarted, ProviderStopped,
        },
        scaler::{
            manager::ScalerManager,
            spreadscaler::{spreadscaler_annotations, ComponentSpreadScaler},
            Scaler,
        },
        storage::{Component, Host, Store, WadmComponentInfo},
        test_util::{NoopPublisher, TestLatticeSource, TestStore},
        workers::{CommandPublisher, EventWorker, StatusPublisher},
    };

    const MODEL_NAME: &str = "spreadscaler_test";

    use super::compute_spread;

    #[test]
    fn can_spread_properly() -> Result<()> {
        // Basic test to ensure our types are correct
        let simple_spread = SpreadScalerProperty {
            instances: 1,
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
            instances: 10,
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
            instances: 7,
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
        assert_eq!(multi_spread_even_res[0].1, 4);
        assert_eq!(multi_spread_even_res[1].1, 3);

        // Ensure we spread an odd number with unclean dividing weights
        let multi_spread_odd = SpreadScalerProperty {
            instances: 7,
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
            instances: 10,
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
            instances: 12,
            spread: vec![],
        };

        let simple_replica_only = compute_spread(&simple_spread_replica_only);
        assert_eq!(simple_replica_only.len(), 1);
        assert_eq!(simple_replica_only[0].1, 12);

        // Ensure we handle an all around complex case
        let complex_spread = SpreadScalerProperty {
            instances: 103,
            spread: vec![
                Spread {
                    // 9 + 1 (remainder trip)
                    name: "ComplexOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(42),
                },
                Spread {
                    // 0
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
                    // 84 + 1 (remainder trip)
                    name: "ComplexFour".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(384),
                },
            ],
        };
        let complex_spread_res = compute_spread(&complex_spread);
        assert_eq!(complex_spread_res[0].1, 85);
        assert_eq!(complex_spread_res[1].1, 10);
        assert_eq!(complex_spread_res[2].1, 8);
        assert_eq!(complex_spread_res[3].1, 0);

        Ok(())
    }

    #[tokio::test]
    async fn can_compute_spread_commands() -> Result<()> {
        let lattice_id = "hoohah_multi_stop_component";
        let component_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let component_id = "fakecloud_azurecr_io_echo_0_3_4".to_string();
        let host_id = "NASDASDIMAREALHOST";

        let store = Arc::new(TestStore::default());

        // STATE SETUP BEGIN, ONE HOST
        store
            .store(
                lattice_id,
                host_id.to_string(),
                Host {
                    components: HashMap::new(),
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

        // Ensure we compute if a weights aren't specified
        let complex_spread = SpreadScalerProperty {
            instances: 103,
            spread: vec![
                Spread {
                    // 9 + 1 (remainder trip)
                    name: "ComplexOne".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(42),
                },
                Spread {
                    // 0
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
                    // 84 + 1 (remainder trip)
                    name: "ComplexFour".to_string(),
                    requirements: BTreeMap::new(),
                    weight: Some(384),
                },
            ],
        };

        let spreadscaler = ComponentSpreadScaler::new(
            store.clone(),
            component_reference.to_string(),
            component_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            complex_spread,
            "fake_component",
            vec![],
        );

        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 3);
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: component_id.to_string(),
            reference: component_reference.to_string(),
            host_id: host_id.to_string(),
            count: 10,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexOne", spreadscaler.id()),
            config: vec![]
        })));
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: component_id.to_string(),
            reference: component_reference.to_string(),
            host_id: host_id.to_string(),
            count: 8,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexThree", spreadscaler.id()),
            config: vec![]
        })));
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: component_id.to_string(),
            reference: component_reference.to_string(),
            host_id: host_id.to_string(),
            count: 85,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("ComplexFour", spreadscaler.id()),
            config: vec![]
        })));

        Ok(())
    }

    #[tokio::test]
    async fn can_scale_up_and_down() -> Result<()> {
        let lattice_id = "computing_spread_commands";
        let echo_ref = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let echo_id = "MASDASDIAMAREALCOMPONENTECHO";
        let blobby_ref = "fakecloud.azurecr.io/blobby:0.5.2".to_string();
        let blobby_id = "MASDASDIAMAREALCOMPONENTBLOBBY";

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
            instances: 9,
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

        let echo_spreadscaler = ComponentSpreadScaler::new(
            store.clone(),
            echo_ref.to_string(),
            echo_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            echo_spread_property,
            "fake_echo",
            vec![],
        );

        let blobby_spreadscaler = ComponentSpreadScaler::new(
            store.clone(),
            blobby_ref.to_string(),
            blobby_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            blobby_spread_property,
            "fake_blobby",
            vec![],
        );

        // STATE SETUP BEGIN

        store
            .store(
                lattice_id,
                echo_id.to_string(),
                Component {
                    id: echo_id.to_string(),
                    name: "Echo".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // One instance on this host
                            HashSet::from_iter([WadmComponentInfo {
                                annotations: spreadscaler_annotations(
                                    "RunInFakeCloud",
                                    echo_spreadscaler.id(),
                                ),
                                count: 1,
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 103 instances on this host
                            HashSet::from_iter([WadmComponentInfo {
                                annotations: spreadscaler_annotations(
                                    "RunInRealCloud",
                                    echo_spreadscaler.id(),
                                ),
                                count: 103,
                            }]),
                        ),
                        (
                            host_id_three.to_string(),
                            // 400 instances on this host
                            HashSet::from_iter([WadmComponentInfo {
                                annotations: spreadscaler_annotations(
                                    "RunInPurgatoryCloud",
                                    echo_spreadscaler.id(),
                                ),
                                count: 400,
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
                Component {
                    id: blobby_id.to_string(),
                    name: "Blobby".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 3 instances on this host
                            HashSet::from_iter([WadmComponentInfo {
                                count: 3,
                                annotations: spreadscaler_annotations(
                                    "CrossRegionCustom",
                                    blobby_spreadscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 19 instances on this host
                            HashSet::from_iter([WadmComponentInfo {
                                count: 19,
                                annotations: spreadscaler_annotations(
                                    "CrossRegionReal",
                                    blobby_spreadscaler.id(),
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
                    components: HashMap::from_iter([
                        (echo_id.to_string(), 1),
                        (blobby_id.to_string(), 3),
                        ("MSOMEOTHERCOMPONENT".to_string(), 3),
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
                    components: HashMap::from_iter([
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
                    components: HashMap::from_iter([(echo_id.to_string(), 400)]),
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

        let mut cmds = echo_spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);
        cmds.sort_by(|a, b| match (a, b) {
            (Command::ScaleComponent(a), Command::ScaleComponent(b)) => a.host_id.cmp(&b.host_id),
            _ => panic!("Unexpected commands in spreadscaler list"),
        });

        let mut cmds_iter = cmds.iter();
        match (
            cmds_iter.next().expect("one scale command"),
            cmds_iter.next().expect("two scale commands"),
        ) {
            (Command::ScaleComponent(scale1), Command::ScaleComponent(scale2)) => {
                assert_eq!(scale1.host_id, host_id_one.to_string());
                assert_eq!(scale1.count, 206);
                assert_eq!(scale1.reference, echo_ref);

                assert_eq!(scale2.host_id, host_id_three.to_string());
                assert_eq!(scale2.count, 103);
                assert_eq!(scale2.component_id, echo_id.to_string());
            }
            _ => panic!("Unexpected commands in spreadscaler list"),
        }

        let mut cmds = blobby_spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);
        cmds.sort_by(|a, b| match (a, b) {
            (Command::ScaleComponent(a), Command::ScaleComponent(b)) => a.host_id.cmp(&b.host_id),
            _ => panic!("Unexpected commands in spreadscaler list"),
        });

        let mut cmds_iter = cmds.iter();
        match (
            cmds_iter.next().expect("one scale command"),
            cmds_iter.next().expect("two scale commands"),
        ) {
            (Command::ScaleComponent(scale1), Command::ScaleComponent(scale2)) => {
                assert_eq!(scale1.host_id, host_id_three.to_string());
                assert_eq!(scale1.count, 3);
                assert_eq!(scale1.reference, blobby_ref);

                assert_eq!(scale2.host_id, host_id_two.to_string());
                assert_eq!(scale2.count, 3);
                assert_eq!(scale2.component_id, blobby_id.to_string());
            }
            _ => panic!("Unexpected commands in spreadscaler list"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_handle_multiple_spread_matches() -> Result<()> {
        let lattice_id = "multiple_spread_matches";
        let component_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let component_id = "fakecloud_azurecr_io_echo_0_3_4".to_string();
        let host_id = "NASDASDIMAREALHOST";

        let store = Arc::new(TestStore::default());

        // Run 75% in east, 25% on resilient hosts
        let real_spread = SpreadScalerProperty {
            instances: 20,
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

        let spreadscaler = ComponentSpreadScaler::new(
            store.clone(),
            component_reference.to_string(),
            component_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            real_spread,
            "fake_component",
            vec![],
        );

        // STATE SETUP BEGIN, ONE HOST
        store
            .store(
                lattice_id,
                host_id.to_string(),
                Host {
                    components: HashMap::from_iter([(component_id.to_string(), 10)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("region".to_string(), "east".to_string()),
                        ("resilient".to_string(), "true".to_string()),
                    ]),
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
                component_id.to_string(),
                Component {
                    id: component_id.to_string(),
                    name: "Faketor".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([(
                        host_id.to_string(),
                        // 10 instances on this host under the first spread
                        HashSet::from_iter([WadmComponentInfo {
                            count: 10,
                            annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                        }]),
                    )]),
                    reference: component_reference.to_string(),
                },
            )
            .await?;

        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);

        // Should be enforcing 10 instances per spread
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: "fakecloud_azurecr_io_echo_0_3_4".to_string(),
            reference: component_reference.to_string(),
            host_id: host_id.to_string(),
            count: 15,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
            config: vec![]
        })));
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: "fakecloud_azurecr_io_echo_0_3_4".to_string(),
            reference: component_reference.to_string(),
            host_id: host_id.to_string(),
            count: 5,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("SimpleTwo", spreadscaler.id()),
            config: vec![]
        })));

        Ok(())
    }

    #[tokio::test]
    async fn calculates_proper_scale_commands() -> Result<()> {
        let lattice_id = "calculates_proper_scale_commands";
        let component_reference = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let component_id = "fakecloud_azurecr_io_echo_0_3_4".to_string();
        let host_id = "NASDASDIMAREALHOST";
        let host_id2 = "NASDASDIMAREALHOST2";

        let store = Arc::new(TestStore::default());

        let real_spread = SpreadScalerProperty {
            // Makes it so we always get at least 2 commands
            instances: 9,
            spread: Vec::new(),
        };

        let spreadscaler = ComponentSpreadScaler::new(
            store.clone(),
            component_reference.to_string(),
            component_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            real_spread,
            "fake_component",
            vec![],
        );

        // STATE SETUP BEGIN, ONE HOST
        store
            .store_many(
                lattice_id,
                [
                    (
                        host_id.to_string(),
                        Host {
                            components: HashMap::from_iter([(component_id.to_string(), 10)]),
                            friendly_name: "hey".to_string(),
                            labels: HashMap::new(),

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
                            components: HashMap::from_iter([(component_id.to_string(), 10)]),
                            friendly_name: "hey2".to_string(),
                            labels: HashMap::new(),

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
                component_id.to_string(),
                Component {
                    id: component_id.to_string(),
                    name: "Faketor".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id.to_string(),
                            HashSet::from_iter([WadmComponentInfo {
                                count: 10,
                                annotations: spreadscaler_annotations("default", spreadscaler.id()),
                            }]),
                        ),
                        (
                            host_id2.to_string(),
                            HashSet::from_iter([WadmComponentInfo {
                                count: 10,
                                annotations: spreadscaler_annotations("default", spreadscaler.id()),
                            }]),
                        ),
                    ]),
                    reference: component_reference.to_string(),
                },
            )
            .await?;

        // Make sure they get at least 2 commands for stopping, one from each host. We don't know
        // which one will have more stopped, but both should show up
        let cmds = spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);
        assert!(
            cmds.iter().any(|command| {
                if let Command::ScaleComponent(component) = command {
                    component.host_id == host_id
                } else {
                    false
                }
            }),
            "Should have found both hosts for stopping commands"
        );
        assert!(
            cmds.iter().any(|command| {
                if let Command::ScaleComponent(component) = command {
                    component.host_id == host_id2
                } else {
                    false
                }
            }),
            "Should have found both hosts for stopping commands"
        );

        // Now check that cleanup removes everything
        let cmds = spreadscaler.cleanup().await?;

        // Should stop 10 on each host
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: component_id.clone(),
            reference: component_reference.clone(),
            host_id: host_id.to_string(),
            count: 0,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("default", spreadscaler.id()),
            config: vec![]
        })));
        assert!(cmds.contains(&Command::ScaleComponent(ScaleComponent {
            component_id: component_id.clone(),
            reference: component_reference.clone(),
            host_id: host_id2.to_string(),
            count: 0,
            model_name: MODEL_NAME.to_string(),
            annotations: spreadscaler_annotations("default", spreadscaler.id()),
            config: vec![]
        })));
        Ok(())
    }

    #[tokio::test]
    async fn can_react_to_events() -> Result<()> {
        let lattice_id = "computing_spread_commands";
        let blobby_ref = "fakecloud.azurecr.io/blobby:0.5.2".to_string();
        let blobby_id = "MASDASDIAMAREALCOMPONENTBLOBBY";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";
        let host_id_three = "NASDASDIMAREALHOSTTREE";

        let store = Arc::new(TestStore::default());

        let lattice_source = TestLatticeSource::default();
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
            instances: 9,
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

        let blobby_spreadscaler = ComponentSpreadScaler::new(
            store.clone(),
            blobby_ref.to_string(),
            blobby_id.to_string(),
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            blobby_spread_property,
            "fake_blobby",
            vec![],
        );

        // STATE SETUP BEGIN
        store
            .store(
                lattice_id,
                blobby_id.to_string(),
                Component {
                    id: blobby_id.to_string(),
                    name: "Blobby".to_string(),
                    issuer: "AASDASDASDASD".to_string(),
                    instances: HashMap::from_iter([
                        (
                            host_id_one.to_string(),
                            // 3 instances on this host
                            HashSet::from_iter([WadmComponentInfo {
                                count: 3,
                                annotations: spreadscaler_annotations(
                                    "CrossRegionCustom",
                                    blobby_spreadscaler.id(),
                                ),
                            }]),
                        ),
                        (
                            host_id_two.to_string(),
                            // 19 instances on this host
                            HashSet::from_iter([WadmComponentInfo {
                                count: 19,
                                annotations: spreadscaler_annotations(
                                    "CrossRegionReal",
                                    blobby_spreadscaler.id(),
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
                    components: HashMap::from_iter([
                        (blobby_id.to_string(), 3),
                        ("MSOMEOTHERCOMPONENT".to_string(), 3),
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
                    components: HashMap::from_iter([(blobby_id.to_string(), 19)]),
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
                    components: HashMap::new(),
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

        // Don't care about these events
        assert!(blobby_spreadscaler
            .handle_event(&Event::ProviderStarted(ProviderStarted {
                claims: None,
                provider_id: "".to_string(),
                annotations: BTreeMap::default(),
                image_ref: "".to_string(),
                host_id: host_id_one.to_string()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(&Event::ProviderStopped(ProviderStopped {
                annotations: BTreeMap::default(),
                provider_id: "".to_string(),
                reason: "".to_string(),
                host_id: host_id_two.to_string()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(&Event::LinkdefSet(LinkdefSet {
                linkdef: Link::default()
            }))
            .await?
            .is_empty());
        assert!(blobby_spreadscaler
            .handle_event(&Event::LinkdefDeleted(LinkdefDeleted {
                source_id: "source".to_string(),
                name: "name".to_string(),
                wit_namespace: "wasi".to_string(),
                wit_package: "testy".to_string()
            }))
            .await?
            .is_empty());

        let mut cmds = blobby_spreadscaler.reconcile().await?;
        assert_eq!(cmds.len(), 2);
        cmds.sort_by(|a, b| match (a, b) {
            (Command::ScaleComponent(a), Command::ScaleComponent(b)) => a.host_id.cmp(&b.host_id),
            _ => panic!("Unexpected commands in spreadscaler list"),
        });

        let mut cmds_iter = cmds.iter();
        match (
            cmds_iter.next().expect("one scale command"),
            cmds_iter.next().expect("two scale commands"),
        ) {
            (Command::ScaleComponent(scale1), Command::ScaleComponent(scale2)) => {
                assert_eq!(scale1.host_id, host_id_three.to_string());
                assert_eq!(scale1.count, 3);
                assert_eq!(scale1.reference, blobby_ref);

                assert_eq!(scale2.host_id, host_id_two.to_string());
                assert_eq!(scale2.count, 3);
                assert_eq!(scale2.component_id, blobby_id.to_string());
            }
            _ => panic!("Unexpected commands in spreadscaler list"),
        }

        let modifying_event = ComponentScaled {
            annotations: spreadscaler_annotations("CrossRegionReal", blobby_spreadscaler.id()),
            component_id: blobby_id.to_string(),
            image_ref: blobby_ref.to_string(),
            host_id: host_id_two.to_string(),
            max_instances: 0,
            claims: None,
        };

        worker
            .do_work(ScopedMessage::<Event> {
                lattice_id: lattice_id.to_string(),
                inner: Event::ComponentScaled(modifying_event.clone()),
                acker: None,
            })
            .await
            .expect("should be able to handle an event");

        let mut cmds = blobby_spreadscaler
            .handle_event(&Event::ComponentScaled(modifying_event))
            .await?;
        assert_eq!(cmds.len(), 2);
        cmds.sort_by(|a, b| match (a, b) {
            (Command::ScaleComponent(a), Command::ScaleComponent(b)) => a.host_id.cmp(&b.host_id),
            _ => panic!("Unexpected commands in spreadscaler list"),
        });

        let mut cmds_iter = cmds.iter();
        match (
            cmds_iter.next().expect("one scale command"),
            cmds_iter.next().expect("two scale commands"),
        ) {
            (Command::ScaleComponent(scale1), Command::ScaleComponent(scale2)) => {
                assert_eq!(scale1.host_id, host_id_three.to_string());
                assert_eq!(scale1.count, 3);
                assert_eq!(scale1.reference, blobby_ref);

                assert_eq!(scale2.host_id, host_id_two.to_string());
                assert_eq!(scale2.count, 3);
                assert_eq!(scale2.component_id, blobby_id.to_string());
            }
            _ => panic!("Unexpected commands in spreadscaler list"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_calculate_ineligible_hosts() {
        let spreads = [
            Spread {
                name: "SimpleOne".to_string(),
                requirements: BTreeMap::from_iter([("region".to_string(), "east".to_string())]),
                weight: Some(75),
            },
            Spread {
                name: "SimpleTwo".to_string(),
                requirements: BTreeMap::from_iter([("resilient".to_string(), "true".to_string())]),
                weight: Some(25),
            },
        ];

        let hosts = HashMap::from_iter([
            (
                "NASDASDIMAREALHOST".to_string(),
                Host {
                    components: HashMap::from_iter([("fake".to_string(), 1)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("region".to_string(), "east".to_string()),
                        ("resilient".to_string(), "true".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: "NASDASDIMAREALHOST".to_string(),
                    last_seen: Utc::now(),
                },
            ),
            (
                "NASDASDIMAREALHOST2".to_string(),
                Host {
                    components: HashMap::from_iter([("fake".to_string(), 1)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("region".to_string(), "west".to_string()),
                        ("resilient".to_string(), "true".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: "NASDASDIMAREALHOST2".to_string(),
                    last_seen: Utc::now(),
                },
            ),
            (
                "NASDASDIMAREALHOST3".to_string(),
                Host {
                    components: HashMap::from_iter([("fake".to_string(), 1)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("region".to_string(), "east".to_string()),
                        ("resilient".to_string(), "false".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: "NASDASDIMAREALHOST3".to_string(),
                    last_seen: Utc::now(),
                },
            ),
            (
                "NASDASDIMAREALHOST4".to_string(),
                Host {
                    components: HashMap::from_iter([("fake".to_string(), 1)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("region".to_string(), "west".to_string()),
                        ("resilient".to_string(), "false".to_string()),
                        ("arch".to_string(), "nemesis".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: "NASDASDIMAREALHOST4".to_string(),
                    last_seen: Utc::now(),
                },
            ),
        ]);

        // The first three hosts match at least one of the spread requirements (resilient: true || region: east)
        // The last host is in west and not resilient.
        let ineligible = compute_ineligible_hosts(&hosts, spreads.iter().collect());

        assert_eq!(ineligible.len(), 1);
        assert!(ineligible
            .iter()
            .any(|(id, _host)| *id == "NASDASDIMAREALHOST4"));
    }
}
