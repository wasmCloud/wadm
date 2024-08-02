use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{OnceCell, RwLock};
use tracing::{instrument, trace};
use wadm_types::{api::StatusInfo, Spread, SpreadScalerProperty, TraitProperty};

use crate::{
    commands::{Command, StartProvider, StopProvider},
    events::{
        Event, HostHeartbeat, HostStarted, HostStopped, ProviderInfo, ProviderStarted,
        ProviderStopped,
    },
    scaler::{
        compute_id_sha256,
        spreadscaler::{
            compute_ineligible_hosts, compute_spread, eligible_hosts, spreadscaler_annotations,
        },
        Scaler,
    },
    storage::{Host, ReadStore},
    SCALER_KEY,
};

use super::SPREAD_SCALER_KIND;

/// Config for a ProviderSpreadConfig
#[derive(Clone)]
pub struct ProviderSpreadConfig {
    /// Lattice ID that this SpreadScaler monitors
    pub lattice_id: String,
    /// Unique identifier for the provider
    pub provider_id: String,
    /// OCI, Bindle, or File reference for a provider
    pub provider_reference: String,
    /// The name of the wadm model this SpreadScaler is under
    pub model_name: String,
    /// Configuration for this SpreadScaler
    pub spread_config: SpreadScalerProperty,
    /// Named configuration passed to the provider when it starts
    pub provider_config: Vec<String>,
}

/// The ProviderSpreadScaler ensures that a certain number of provider instances are running,
/// spread across a number of hosts according to a [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of instances
/// on an available host. It's important to note that only one instance of a provider id + link + contract
/// can run on each host, so it's possible to specify too many instances to be satisfied by the spread configs.
pub struct ProviderSpreadScaler<S: ReadStore + Send + Sync> {
    store: S,
    pub config: ProviderSpreadConfig,
    spread_requirements: Vec<(Spread, usize)>,
    /// Provider ID, stored in a OnceCell to facilitate more efficient fetches
    provider_id: OnceCell<String>,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ProviderSpreadScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        SPREAD_SCALER_KIND
    }

    fn name(&self) -> String {
        self.config.provider_id.to_string()
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    #[instrument(level = "debug", skip_all, fields(scaler_id = %self.id))]
    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        let spread_config = match config {
            TraitProperty::SpreadScaler(prop) => prop,
            _ => anyhow::bail!("Given config was not a spread scaler config object"),
        };
        self.spread_requirements = compute_spread(&spread_config);
        self.config.spread_config = spread_config;
        self.reconcile().await
    }

    #[instrument(level = "debug", skip_all, fields(scaler_id = %self.id))]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): We could be more efficient here and instead of running
        // the entire reconcile, smart compute exactly what needs to change, but it just
        // requires more code branches and would be fine as a future improvement
        match event {
            Event::ProviderStarted(ProviderStarted { provider_id, .. })
            | Event::ProviderStopped(ProviderStopped { provider_id, .. })
                if provider_id == &self.config.provider_id =>
            {
                self.reconcile().await
            }
            // If the host labels match any spread requirement, perform reconcile
            Event::HostStopped(HostStopped { labels, .. })
            | Event::HostStarted(HostStarted { labels, .. })
            | Event::HostHeartbeat(HostHeartbeat { labels, .. })
                if self.spread_requirements.iter().any(|(spread, _count)| {
                    spread.requirements.iter().all(|(key, value)| {
                        labels.get(key).map(|val| val == value).unwrap_or(false)
                    })
                }) =>
            {
                self.reconcile().await
            }
            // No other event impacts the job of this scaler so we can ignore it
            _ => Ok(Vec::new()),
        }
    }

    #[instrument(level = "debug", skip_all, fields(provider_ref = %self.config.provider_reference, provider_id = %self.config.provider_id, scaler_id = %self.id))]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        let hosts = self.store.list::<Host>(&self.config.lattice_id).await?;
        let provider_id = &self.config.provider_id;
        let provider_ref = &self.config.provider_reference;

        let ineligible_hosts = compute_ineligible_hosts(
            &hosts,
            self.spread_requirements
                .iter()
                .map(|(s, _)| s)
                .collect::<Vec<&Spread>>(),
        );
        // Remove any providers that are managed by this scaler and running on ineligible hosts
        let remove_ineligible: Vec<Command> = ineligible_hosts
            .iter()
            .filter_map(|(_host_id, host)| {
                if host
                    .providers
                    .get(&ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: BTreeMap::default(),
                    })
                    .is_some_and(|provider| {
                        provider
                            .annotations
                            .get(SCALER_KEY)
                            .is_some_and(|id| id == &self.id)
                    })
                {
                    Some(Command::StopProvider(StopProvider {
                        provider_id: provider_id.to_owned(),
                        host_id: host.id.to_string(),
                        model_name: self.config.model_name.to_owned(),
                        annotations: BTreeMap::default(),
                    }))
                } else {
                    None
                }
            })
            .collect();
        // If we found any providers running on ineligible hosts, remove them before
        // attempting to start new ones.
        if !remove_ineligible.is_empty() {
            let status = StatusInfo::reconciling(
                "Found providers running on ineligible hosts, removing them.",
            );
            trace!(?status, "Updating scaler status");
            *self.status.write().await = status;
            return Ok(remove_ineligible);
        }

        let mut spread_status = vec![];

        let commands = self
            .spread_requirements
            .iter()
            .flat_map(|(spread, count)| {
                let eligible_hosts = eligible_hosts(&hosts, spread);
                let eligible_count = eligible_hosts.len();
                // Partition hosts into ones running this provider (no matter what is running it), and others
                let (running, other): (HashMap<&String, &Host>, HashMap<&String, &Host>) =
                    eligible_hosts.into_iter().partition(|(_host_id, host)| {
                        host.providers
                            .contains(&ProviderInfo {
                                provider_id: provider_id.to_string(),
                                provider_ref: provider_ref.to_string(),
                                annotations: BTreeMap::default(),
                            })
                    });
                // Get the count of all running providers
                let current_running = running.len();
                // NOTE(#120): Now partition providers into ones running for this spread, and ones running for
                // either other spreads or no spread at all. Hosts cannot run multiple providers with the same
                // link name and contract id, so wadm currently will allow these to count up to the total.
                let (running_for_spread, running_for_other): (HashMap<&String, &Host>, HashMap<&String, &Host>) = running.into_iter().partition(|(_host_id, host)| {
                    host.providers
                        .get(&ProviderInfo {
                            provider_id: provider_id.to_string(),
                            provider_ref: provider_ref.to_string(),
                            annotations: BTreeMap::default(),
                        })
                        .map(|provider| {
                            spreadscaler_annotations(&spread.name, &self.id).iter().all(|(k, v)| {
                                let has_annotation = provider
                                    .annotations
                                    .get(k)
                                    .map(|val| val == v)
                                    .unwrap_or(false);
                                if !has_annotation {
                                    trace!(key = %k, value = %v, "Provider is missing annotation. Expected correct key and value");
                                }
                                has_annotation
                            })
                        }).unwrap_or(false)
                });
                trace!(current_for_spread = %running_for_spread.len(), %current_running, expected = %count, eligible_hosts = %eligible_count, %provider_id, "Calculated running providers, reconciling with expected count");
                match current_running.cmp(count) {
                    // We can only stop providers that we own, so if we have more than we need, we
                    // need to stop some
                    Ordering::Greater if !running_for_spread.is_empty() => {
                        let num_to_stop = current_running - count;
                        // Take `num_to_stop` commands from this iterator
                        running_for_spread
                            .into_values()
                            .map(|host| {
                                Command::StopProvider(StopProvider {
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: spreadscaler_annotations(&spread.name, &self.id),
                                })
                            })
                            .take(num_to_stop)
                            .collect::<Vec<Command>>()
                    }
                    Ordering::Less => {
                        // NOTE(#120): Providers running for other spreads or for no spreads can count for up to the total
                        // number of providers to satisfy a spread. We do not count them above because we don't want to
                        // end up in an infinite loop of stopping other managed providers.
                        let num_to_start = count.saturating_sub(current_running).saturating_sub(running_for_other.len());

                        // Take `num_to_start` commands from this iterator
                        let commands = other
                            .into_iter()
                            .filter(|(_host_id, host)| {
                                !host.providers.contains(&ProviderInfo {
                                    provider_id: provider_id.to_string(),
                                    provider_ref: provider_ref.to_string(),
                                    annotations: BTreeMap::default(),
                                })
                            })
                            .map(|(_host_id, host)| {
                                Command::StartProvider(StartProvider {
                                    reference: provider_ref.to_owned(),
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: spreadscaler_annotations(&spread.name, &self.id),
                                    config: self.config.provider_config.clone(),
                                })
                            })
                            .take(num_to_start)
                            .collect::<Vec<Command>>();

                        if commands.len() < num_to_start {
                            // NOTE(brooksmtownsend): We're reporting status for the entire spreadscaler here, not just this individual
                            // command, so we want to consider the providers that are already running for the spread over the 
                            // total expected count.
                            let msg = format!("Could not satisfy spread {} for {}, {}/{} eligible hosts found.", spread.name, self.config.provider_reference, running_for_spread.len(), count);
                            spread_status.push(StatusInfo::failed(&msg));
                        }
                        commands
                    }
                    // If we're equal or are greater and have nothing from our spread running, we do
                    // nothing
                    _ => Vec::new(),
                }
            })
            .collect::<Vec<Command>>();

        trace!(?commands, "Calculated commands for provider scaler");

        let status = match (spread_status.is_empty(), commands.is_empty()) {
            // No failures, no commands, scaler satisfied
            (true, true) => StatusInfo::deployed(""),
            // No failures, commands generated, scaler is reconciling
            (true, false) => {
                StatusInfo::reconciling(&format!("Scaling provider on {} host(s)", commands.len()))
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

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name))]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        let mut config_clone = self.config.clone();
        config_clone.spread_config.instances = 0;
        let spread_requirements = compute_spread(&config_clone.spread_config);

        let cleanerupper = ProviderSpreadScaler {
            store: self.store.clone(),
            config: config_clone,
            spread_requirements,
            provider_id: self.provider_id.clone(),
            id: self.id.clone(),
            status: RwLock::new(StatusInfo::reconciling("")),
        };

        cleanerupper.reconcile().await
    }
}

impl<S: ReadStore + Send + Sync> ProviderSpreadScaler<S> {
    /// Construct a new ProviderSpreadScaler with specified configuration values
    pub fn new(store: S, config: ProviderSpreadConfig, component_name: &str) -> Self {
        // Compute the id of this scaler based on all of the configuration values
        // that make it unique. This is used during upgrades to determine if a
        // scaler is the same as a previous one.
        let mut id_parts = vec![
            SPREAD_SCALER_KIND,
            &config.model_name,
            component_name,
            &config.provider_id,
            &config.provider_reference,
        ];
        id_parts.extend(
            config
                .provider_config
                .iter()
                .map(std::string::String::as_str),
        );
        let id = compute_id_sha256(&id_parts);

        Self {
            store,
            spread_requirements: compute_spread(&config.spread_config),
            provider_id: OnceCell::new(),
            config,
            id,
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::Arc,
    };

    use anyhow::Result;
    use chrono::Utc;
    use wadm_types::{Spread, SpreadScalerProperty};

    use crate::{
        commands::{Command, StartProvider, StopProvider},
        events::ProviderInfo,
        scaler::{
            spreadscaler::{provider::ProviderSpreadScaler, spreadscaler_annotations},
            Scaler,
        },
        storage::{Host, Provider, ProviderStatus, Store},
        test_util::TestStore,
    };

    use super::*;

    const MODEL_NAME: &str = "test_provider_spreadscaler";

    #[test]
    fn test_different_ids() {
        let config = ProviderSpreadConfig {
            lattice_id: "lattice".to_string(),
            provider_reference: "provider_ref".to_string(),
            provider_id: "provider_id".to_string(),
            model_name: MODEL_NAME.to_string(),
            spread_config: SpreadScalerProperty {
                instances: 1,
                spread: vec![],
            },
            provider_config: vec![],
        };

        let scaler1 =
            ProviderSpreadScaler::new(Arc::new(TestStore::default()), config, "component");

        let config = ProviderSpreadConfig {
            lattice_id: "lattice".to_string(),
            provider_reference: "provider_ref".to_string(),
            provider_id: "provider_id".to_string(),
            model_name: MODEL_NAME.to_string(),
            spread_config: SpreadScalerProperty {
                instances: 1,
                spread: vec![],
            },
            provider_config: vec!["foobar".to_string()],
        };

        let scaler2 =
            ProviderSpreadScaler::new(Arc::new(TestStore::default()), config, "component");
        assert_ne!(
            scaler1.id(),
            scaler2.id(),
            "ProviderSpreadScaler IDs should be different with different configuration"
        );
    }

    #[tokio::test]
    async fn can_spread_on_multiple_hosts() -> Result<()> {
        let lattice_id = "provider_spread_multi_host";
        let provider_ref = "fakecloud.azurecr.io/provider:3.2.1".to_string();
        let provider_id = "fakecloud_azurecr_io_provider_3_2_1".to_string();

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";

        let store = Arc::new(TestStore::default());

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
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
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
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
                provider_id.to_string(),
                Provider {
                    id: provider_id.to_string(),
                    name: "provider".to_string(),
                    issuer: "issuer".to_string(),
                    reference: provider_ref.to_string(),
                    hosts: HashMap::new(),
                },
            )
            .await?;

        // Ensure we spread evenly with equal weights, clean division
        let multi_spread_even = SpreadScalerProperty {
            instances: 2,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                    weight: Some(100),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "real".to_string())]),
                    weight: Some(100),
                },
            ],
        };

        let spreadscaler = ProviderSpreadScaler::new(
            store.clone(),
            ProviderSpreadConfig {
                lattice_id: lattice_id.to_string(),
                provider_id: provider_id.to_string(),
                provider_reference: provider_ref.to_string(),
                spread_config: multi_spread_even,
                model_name: MODEL_NAME.to_string(),
                provider_config: vec!["foobar".to_string()],
            },
            "fake_component",
        );

        let commands = spreadscaler.reconcile().await?;
        assert_eq!(commands.len(), 2);

        let cmd_one = commands.first().cloned();
        match cmd_one {
            None => panic!("command should have existed"),
            Some(Command::StartProvider(start)) => {
                assert_eq!(
                    start,
                    StartProvider {
                        reference: provider_ref.to_string(),
                        provider_id: provider_id.to_string(),
                        host_id: host_id_one.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                        config: vec!["foobar".to_string()],
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    start.annotations,
                    spreadscaler_annotations("SimpleOne", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        let cmd_two = commands.get(1).cloned();
        match cmd_two {
            None => panic!("command should have existed"),
            Some(Command::StartProvider(start)) => {
                assert_eq!(
                    start,
                    StartProvider {
                        reference: provider_ref.to_string(),
                        provider_id: provider_id.to_string(),
                        host_id: host_id_two.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("SimpleTwo", spreadscaler.id()),
                        config: vec!["foobar".to_string()],
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    start.annotations,
                    spreadscaler_annotations("SimpleTwo", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_do_multiple_instances_per_spread() -> Result<()> {
        let lattice_id = "provider_spread_multi_host";
        let provider_ref = "fakecloud.azurecr.io/provider:3.2.1".to_string();
        let provider_id = "VASDASDIAMAREALPROVIDERPROVIDER";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";
        let host_id_three = "NASDASDIMAREALHOSTTHREE";
        let host_id_four = "NASDASDIMAREALHOSTFOUR";

        let store = Arc::new(TestStore::default());

        // let lattice_source = TestLatticeSource {
        //     claims: HashMap::default(),
        //     inventory: Arc::new(RwLock::new(HashMap::default())),
        // };
        // let worker = EventWorker::new(store.clone(), lattice_source);

        // Needs to request
        // start proivder with 1 replica on 2
        // start proivder with 1 replica on 3
        // stop provider with 1 replica on 4
        let multi_spread_hard = SpreadScalerProperty {
            instances: 3,
            spread: vec![
                Spread {
                    name: "ComplexOne".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                    weight: Some(1),
                },
                Spread {
                    name: "ComplexTwo".to_string(),
                    requirements: BTreeMap::from_iter([(
                        "region".to_string(),
                        "us-yourhouse-1".to_string(),
                    )]),
                    weight: Some(2),
                },
            ],
        };

        let spreadscaler = ProviderSpreadScaler::new(
            store.clone(),
            ProviderSpreadConfig {
                lattice_id: lattice_id.to_string(),
                provider_id: provider_id.to_string(),
                provider_reference: provider_ref.to_string(),
                spread_config: multi_spread_hard,
                model_name: MODEL_NAME.to_string(),
                provider_config: vec![],
            },
            "fake_component",
        );

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: spreadscaler_annotations("ComplexOne", spreadscaler.id()),
                    }]),
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
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
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
                        ("cloud".to_string(), "inthemiddle".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
                    ]),
                    providers: HashSet::new(),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_three.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;
        store
            .store(
                lattice_id,
                host_id_four.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-east-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: spreadscaler_annotations("ComplexOne", spreadscaler.id()),
                    }]),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_four.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        store
            .store(
                lattice_id,
                provider_id.to_string(),
                Provider {
                    id: provider_id.to_string(),
                    name: "provider".to_string(),
                    issuer: "issuer".to_string(),
                    reference: provider_ref.to_string(),
                    hosts: HashMap::from_iter([
                        (host_id_one.to_string(), ProviderStatus::Running),
                        (host_id_four.to_string(), ProviderStatus::Running),
                    ]),
                },
            )
            .await?;

        let commands = spreadscaler.reconcile().await?;
        assert_eq!(commands.len(), 3);

        // Map for easy lookups for assertions
        let cmd_by_host = commands
            .iter()
            .map(|cmd| match cmd {
                Command::StartProvider(StartProvider { host_id, .. })
                | Command::StopProvider(StopProvider { host_id, .. }) => {
                    (host_id.clone(), cmd.clone())
                }
                _ => panic!("unexpected command"),
            })
            .collect::<HashMap<String, Command>>();

        match (
            cmd_by_host.get(host_id_one).cloned(),
            cmd_by_host.get(host_id_four).cloned(),
        ) {
            (None, None) => panic!("command should have existed"),
            // We're ok with stopping the provider on either host
            (Some(Command::StopProvider(stop)), None) => {
                assert_eq!(
                    stop,
                    StopProvider {
                        provider_id: provider_id.to_string(),
                        host_id: host_id_one.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("ComplexOne", spreadscaler.id())
                    }
                );
            }
            (None, Some(Command::StopProvider(stop))) => {
                assert_eq!(
                    stop,
                    StopProvider {
                        provider_id: provider_id.to_string(),
                        host_id: host_id_four.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("ComplexOne", spreadscaler.id())
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    stop.annotations,
                    spreadscaler_annotations("ComplexOne", spreadscaler.id())
                )
            }
            (_, _) => panic!("command should have been a stop provider"),
        }

        match cmd_by_host.get(host_id_two).cloned() {
            None => panic!("command should have existed"),
            Some(Command::StartProvider(start)) => {
                assert_eq!(
                    start,
                    StartProvider {
                        reference: provider_ref.to_string(),
                        provider_id: provider_id.to_string(),
                        host_id: host_id_two.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("ComplexTwo", spreadscaler.id()),
                        config: vec![],
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    start.annotations,
                    spreadscaler_annotations("ComplexTwo", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        match cmd_by_host.get(host_id_three).cloned() {
            None => panic!("command should have existed"),
            Some(Command::StartProvider(start)) => {
                assert_eq!(
                    start,
                    StartProvider {
                        reference: provider_ref.to_string(),
                        host_id: host_id_three.to_string(),
                        provider_id: provider_id.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("ComplexTwo", spreadscaler.id()),
                        config: vec![],
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    start.annotations,
                    spreadscaler_annotations("ComplexTwo", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_handle_too_few_preexisting() -> Result<()> {
        let lattice_id = "can_handle_too_few_preexisting";
        let provider_ref = "fakecloud.azurecr.io/provider:3.2.1".to_string();
        let provider_id = "VASDASDIAMAREALPROVIDERPROVIDER";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";

        let store = Arc::new(TestStore::default());

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
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
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: BTreeMap::default(),
                    }]),

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
                provider_id.to_string(),
                Provider {
                    id: provider_id.to_string(),
                    name: "provider".to_string(),
                    issuer: "issuer".to_string(),
                    reference: provider_ref.to_string(),
                    hosts: HashMap::new(),
                },
            )
            .await?;

        // Ensure we spread evenly with equal weights, clean division
        let multi_spread_even = SpreadScalerProperty {
            instances: 2,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                    weight: Some(100),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "real".to_string())]),
                    weight: Some(100),
                },
            ],
        };

        let spreadscaler = ProviderSpreadScaler::new(
            store.clone(),
            ProviderSpreadConfig {
                lattice_id: lattice_id.to_string(),
                provider_reference: provider_ref.to_string(),
                provider_id: provider_id.to_string(),
                spread_config: multi_spread_even,
                model_name: MODEL_NAME.to_string(),
                provider_config: vec!["foobar".to_string()],
            },
            "fake_component",
        );

        let commands = spreadscaler.reconcile().await?;
        assert_eq!(commands.len(), 1);

        let cmd_one = commands.first().cloned();
        match cmd_one {
            None => panic!("command should have existed"),
            Some(Command::StartProvider(start)) => {
                assert_eq!(
                    start,
                    StartProvider {
                        reference: provider_ref.to_string(),
                        provider_id: provider_id.to_string(),
                        host_id: host_id_one.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                        config: vec!["foobar".to_string()],
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    start.annotations,
                    spreadscaler_annotations("SimpleOne", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_handle_enough_preexisting() -> Result<()> {
        let lattice_id = "can_handle_enough_preexisting";
        let provider_ref = "fakecloud.azurecr.io/provider:3.2.1".to_string();
        let provider_id = "VASDASDIAMAREALPROVIDERPROVIDER";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";

        let store = Arc::new(TestStore::default());

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: BTreeMap::default(),
                    }]),
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
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: BTreeMap::default(),
                    }]),

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
                provider_id.to_string(),
                Provider {
                    id: provider_id.to_string(),
                    name: "provider".to_string(),
                    issuer: "issuer".to_string(),
                    reference: provider_ref.to_string(),
                    hosts: HashMap::new(),
                },
            )
            .await?;

        // Ensure we spread evenly with equal weights, clean division
        let multi_spread_even = SpreadScalerProperty {
            instances: 2,
            spread: vec![
                Spread {
                    name: "SimpleOne".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                    weight: Some(100),
                },
                Spread {
                    name: "SimpleTwo".to_string(),
                    requirements: BTreeMap::from_iter([("cloud".to_string(), "real".to_string())]),
                    weight: Some(100),
                },
            ],
        };

        let spreadscaler = ProviderSpreadScaler::new(
            store.clone(),
            ProviderSpreadConfig {
                lattice_id: lattice_id.to_string(),
                provider_id: provider_id.to_string(),
                provider_reference: provider_ref.to_string(),
                spread_config: multi_spread_even,
                model_name: MODEL_NAME.to_string(),
                provider_config: vec!["foobar".to_string()],
            },
            "fake_component",
        );

        let commands = spreadscaler.reconcile().await?;
        assert!(commands.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn can_handle_too_many_preexisting() -> Result<()> {
        let lattice_id = "can_handle_too_many_preexisting";
        let provider_ref = "fakecloud.azurecr.io/provider:3.2.1".to_string();
        let provider_id = "VASDASDIAMAREALPROVIDERPROVIDER";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";

        let store = Arc::new(TestStore::default());

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: BTreeMap::default(),
                    }]),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_one.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await?;

        // Ensure we spread evenly with equal weights, clean division
        let multi_spread_even = SpreadScalerProperty {
            instances: 1,
            spread: vec![Spread {
                name: "SimpleOne".to_string(),
                requirements: BTreeMap::from_iter([("cloud".to_string(), "fake".to_string())]),
                weight: Some(100),
            }],
        };

        let spreadscaler = ProviderSpreadScaler::new(
            store.clone(),
            ProviderSpreadConfig {
                lattice_id: lattice_id.to_string(),
                provider_reference: provider_ref.to_string(),
                provider_id: provider_id.to_string(),
                spread_config: multi_spread_even,
                model_name: MODEL_NAME.to_string(),
                provider_config: vec!["foobar".to_string()],
            },
            "fake_component",
        );

        store
            .store(
                lattice_id,
                host_id_two.to_string(),
                Host {
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: provider_id.to_string(),
                        provider_ref: provider_ref.to_string(),
                        annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                    }]),

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
                provider_id.to_string(),
                Provider {
                    id: provider_id.to_string(),
                    name: "provider".to_string(),
                    issuer: "issuer".to_string(),
                    reference: provider_ref.to_string(),
                    hosts: HashMap::new(),
                },
            )
            .await?;

        let commands = spreadscaler.reconcile().await?;
        assert_eq!(commands.len(), 1);

        let cmd = commands.first().cloned();
        match cmd {
            None => panic!("command should have existed"),
            Some(Command::StopProvider(stop)) => {
                assert_eq!(
                    stop,
                    StopProvider {
                        host_id: host_id_two.to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                        provider_id: provider_id.to_owned(),
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(
                    stop.annotations,
                    spreadscaler_annotations("SimpleOne", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }
}
