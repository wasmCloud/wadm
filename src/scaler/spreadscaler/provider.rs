use std::{cmp::Ordering, collections::HashMap};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::OnceCell;
use tracing::log::warn;

use crate::{
    commands::{Command, StartProvider, StopProvider},
    events::{Event, HostStarted, HostStopped, ProviderInfo},
    model::{Spread, SpreadScalerProperty, DEFAULT_SPREAD_WEIGHT},
    scaler::Scaler,
    storage::{Host, Provider, ReadStore},
    DEFAULT_LINK_NAME,
};

// Annotation constants
const SCALER_KEY: &str = "wasmcloud.dev/scaler";
const SCALER_VALUE: &str = "spreadscaler";
const SPREAD_KEY: &str = "wasmcloud.dev/spread_name";

/// Config for a ProviderSpreadConfig
pub struct ProviderSpreadConfig {
    /// Lattice ID that this SpreadScaler monitors
    lattice_id: String,
    /// OCI, Bindle, or File reference for a provider
    provider_reference: String,
    /// Link name for a provider
    provider_link_name: String,
    /// Contract ID the provider implements
    provider_contract_id: String,
    /// The name of the wadm model this SpreadScaler is under
    model_name: String,
    /// Configuration for this SpreadScaler
    spread_config: SpreadScalerProperty,
    /// Provider ID, stored in a OnceCell to facilitate more efficient fetches
    provider_id: OnceCell<String>,
}

/// The ProviderSpreadScaler ensures that a certain number of provider replicas are running,
/// spread across a number of hosts according to a [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of replicas
/// on an available host. It's important to note that only one instance of a provider id + link + contract
/// can run on each host, so it's possible to specify too many replicas to be satisfied by the spread configs.
pub struct ProviderSpreadScaler<S: ReadStore + Send + Sync> {
    pub config: ProviderSpreadConfig,
    spread_requirements: Vec<(Spread, usize)>,
    store: S,
}

#[async_trait]
impl<S: ReadStore + Send + Sync> Scaler for ProviderSpreadScaler<S> {
    type Config = ProviderSpreadConfig;

    async fn update_config(&mut self, config: Self::Config) -> Result<Vec<Command>> {
        self.spread_requirements = compute_spread(&config.spread_config);
        self.config = config;
        self.reconcile().await
    }

    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): We could be more efficient here and instead of running
        // the entire reconcile, smart compute exactly what needs to change, but it just
        // requires more code branches and would be fine as a future improvement
        match event {
            Event::ProviderStarted(provider_started) => {
                if provider_started.contract_id == self.config.provider_contract_id
                    && provider_started.image_ref == self.config.provider_reference
                    && provider_started.link_name == self.config.provider_link_name
                {
                    self.reconcile().await
                } else {
                    Ok(Vec::new())
                }
            }
            Event::ProviderStopped(provider_stopped) => {
                if provider_stopped.contract_id == self.config.provider_contract_id
                    && provider_stopped.link_name == self.config.provider_link_name
                    // If this is None, provider hasn't been started in the lattice yet, so we don't need to reconcile
                    && self.provider_id().await.map(|id| id == provider_stopped.public_key).unwrap_or(false)
                {
                    self.reconcile().await
                } else {
                    Ok(Vec::new())
                }
            }
            Event::HostStopped(HostStopped { labels, .. })
            | Event::HostStarted(HostStarted { labels, .. }) => {
                // If the host labels match any spread requirement, perform reconcile
                if self.spread_requirements.iter().any(|(spread, _count)| {
                    spread.requirements.iter().all(|(key, value)| {
                        labels.get(key).map(|val| val == value).unwrap_or(false)
                    })
                }) {
                    Ok(Vec::new())
                } else {
                    self.reconcile().await
                }
            }
            // No other event impacts the job of this scaler so we can ignore it
            _ => Ok(Vec::new()),
        }
    }

    async fn reconcile(&self) -> Result<Vec<Command>> {
        let hosts = self.store.list::<Host>(&self.config.lattice_id).await?;
        // Defaulting to empty String is ok here, since it's only going to happen if this provider has never
        // been started in the lattice
        let provider_id = self.provider_id().await.unwrap_or_default();
        let contract_id = &self.config.provider_contract_id;
        let link_name = &self.config.provider_link_name;
        let provider_ref = &self.config.provider_reference;

        Ok(self
            .spread_requirements
            .iter()
            .flat_map(|(spread, count)| {
                let eligible_hosts = eligible_hosts(&hosts, spread);

                // Partition hosts into ones running this provider for this spread, and others
                let (running_for_spread, other): (Vec<&Host>, Vec<&Host>) =
                    eligible_hosts.iter().partition(|host| {
                        host.providers
                            .get(&ProviderInfo {
                                contract_id: contract_id.to_string(),
                                link_name: link_name.to_string(),
                                public_key: provider_id.to_string(),
                                annotations: self.annotations(&spread.name),
                            })
                            .map(|provider| {
                                self.annotations(&spread.name).iter().all(|(k, v)| {
                                    provider
                                        .annotations
                                        .get(k)
                                        .map(|val| val == v)
                                        .unwrap_or(false)
                                })
                            })
                            .unwrap_or(false)
                    });

                match running_for_spread.len().cmp(count) {
                    Ordering::Equal => Vec::new(),
                    Ordering::Greater => {
                        let num_to_stop = running_for_spread.len() - count;
                        // Take `num_to_stop` commands from this iterator
                        let commands = running_for_spread
                            .iter()
                            .map(|host| {
                                Command::StopProvider(StopProvider {
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
                                    link_name: Some(self.config.provider_link_name.to_owned()),
                                    contract_id: self.config.provider_contract_id.to_owned(),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: self.annotations(&spread.name),
                                })
                            })
                            .take(num_to_stop)
                            .collect::<Vec<Command>>();
                        commands
                    }
                    Ordering::Less => {
                        let num_to_start = count - running_for_spread.len();

                        // NOTE(brooksmtownsend): It's possible that this does not fully satisfy
                        // the requirements if we are unable to form enough start commands. Update
                        // status accordingly once we have a way to.

                        // Take `num_to_start` commands from this iterator
                        let commands = other
                            .iter()
                            .filter(|host| {
                                !host.providers.contains(&ProviderInfo {
                                    contract_id: contract_id.to_string(),
                                    link_name: link_name.to_string(),
                                    public_key: provider_id.to_string(),
                                    annotations: HashMap::new(),
                                })
                            })
                            .map(|host| {
                                Command::StartProvider(StartProvider {
                                    reference: provider_ref.to_owned(),
                                    host_id: host.id.to_string(),
                                    link_name: Some(link_name.to_owned()),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: self.annotations(&spread.name),
                                })
                            })
                            .take(num_to_start)
                            .collect::<Vec<Command>>();

                        commands
                    }
                }
            })
            .collect::<Vec<Command>>())
    }
}

impl<S: ReadStore + Send + Sync> ProviderSpreadScaler<S> {
    /// Construct a new ProviderSpreadScaler with specified configuration values
    pub fn new(
        store: S,
        provider_reference: String,
        provider_contract_id: String,
        provider_link_name: Option<String>,
        lattice_id: String,
        model_name: String,
        spread_config: SpreadScalerProperty,
    ) -> Self {
        Self {
            store,
            spread_requirements: compute_spread(&spread_config),
            config: ProviderSpreadConfig {
                provider_reference,
                provider_contract_id,
                provider_link_name: provider_link_name
                    .unwrap_or_else(|| DEFAULT_LINK_NAME.to_string()),
                provider_id: OnceCell::new(),
                lattice_id,
                spread_config,
                model_name,
            },
        }
    }

    /// Helper function to retrieve the provider ID for the configured provider
    async fn provider_id(&self) -> Option<String> {
        //TODO: get or try init
        if let Some(id) = self.config.provider_id.get() {
            Some(id.to_string())
        } else if let Some(id) = self
            .store
            .list::<Provider>(&self.config.lattice_id)
            .await
            .unwrap_or_default()
            .iter()
            .find(|(_id, provider)| provider.reference == self.config.provider_reference)
            .map(|(id, _provider)| id.to_owned())
        {
            // We know it's not initialized, so we can ignore the error
            let _ = self.config.provider_id.set(id.to_owned());
            Some(id)
        } else {
            // Could not fetch provider ID, don't initialize OnceCell and return empty string
            None
        }
    }

    /// Helper function to create a predictable annotations map for a spread
    fn annotations(&self, spread_name: &str) -> HashMap<String, String> {
        HashMap::from_iter([
            (SCALER_KEY.to_string(), SCALER_VALUE.to_string()),
            (SPREAD_KEY.to_string(), spread_name.to_string()),
        ])
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
            warn!("Requesting more provider instances than were specified");
            spreads
        }
        Ordering::Equal => spreads,
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

    use crate::{
        commands::{Command, StartProvider, StopProvider},
        events::ProviderInfo,
        model::{Spread, SpreadScalerProperty},
        scaler::{spreadscaler::provider::ProviderSpreadScaler, Scaler},
        storage::{Host, Provider, ProviderStatus, Store},
        test_util::TestStore,
        DEFAULT_LINK_NAME,
    };

    const MODEL_NAME: &str = "test_provider_spreadscaler";

    #[tokio::test]
    async fn can_spread_on_multiple_hosts() -> Result<()> {
        let lattice_id = "provider_spread_multi_host";
        let provider_ref = "fakecloud.azurecr.io/provider:3.2.1".to_string();
        let provider_id = "VASDASDIAMAREALPROVIDERPROVIDER";

        let host_id_one = "NASDASDIMAREALHOSTONE";
        let host_id_two = "NASDASDIMAREALHOSTTWO";

        let store = Arc::new(TestStore::default());

        // let lattice_source = TestLatticeSource {
        //     claims: HashMap::default(),
        //     inventory: Arc::new(RwLock::new(HashMap::default())),
        // };
        // let worker = EventWorker::new(store.clone(), lattice_source);

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
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
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
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
                provider_id.to_string(),
                Provider {
                    id: provider_id.to_string(),
                    name: "provider".to_string(),
                    issuer: "issuer".to_string(),
                    contract_id: "prov:ider".to_string(),
                    reference: provider_ref.to_string(),
                    link_name: DEFAULT_LINK_NAME.to_string(),
                    hosts: HashMap::new(),
                },
            )
            .await?;

        // Ensure we spread evenly with equal weights, clean division
        let multi_spread_even = SpreadScalerProperty {
            replicas: 2,
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
            provider_ref.to_string(),
            "prov:ider".to_string(),
            None,
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            multi_spread_even,
        );

        let commands = spreadscaler.reconcile().await?;
        assert_eq!(commands.len(), 2);

        let cmd_one = commands.get(0).cloned();
        match cmd_one {
            None => panic!("command should have existed"),
            Some(Command::StartProvider(start)) => {
                assert_eq!(
                    start,
                    StartProvider {
                        reference: provider_ref.to_string(),
                        host_id: host_id_one.to_string(),
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler.annotations("SimpleOne"),
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler.annotations("SimpleOne"))
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
                        host_id: host_id_two.to_string(),
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler.annotations("SimpleTwo"),
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler.annotations("SimpleTwo"))
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }

    #[tokio::test]
    async fn can_do_multiple_replicas_per_spread() -> Result<()> {
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
            replicas: 3,
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
            provider_ref.to_string(),
            "prov:ider".to_string(),
            None,
            lattice_id.to_string(),
            MODEL_NAME.to_string(),
            multi_spread_hard,
        );

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
                    ]),
                    annotations: HashMap::new(),
                    providers: HashSet::from_iter([ProviderInfo {
                        contract_id: "prov:ider".to_string(),
                        link_name: DEFAULT_LINK_NAME.to_string(),
                        public_key: provider_id.to_string(),
                        annotations: spreadscaler.annotations("ComplexOne"),
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
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
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
                        ("cloud".to_string(), "inthemiddle".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
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
        store
            .store(
                lattice_id,
                host_id_four.to_string(),
                Host {
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-east-1".to_string()),
                    ]),
                    annotations: HashMap::new(),
                    providers: HashSet::from_iter([ProviderInfo {
                        contract_id: "prov:ider".to_string(),
                        link_name: DEFAULT_LINK_NAME.to_string(),
                        public_key: provider_id.to_string(),
                        annotations: spreadscaler.annotations("ComplexOne"),
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
                    contract_id: "prov:ider".to_string(),
                    reference: provider_ref.to_string(),
                    link_name: DEFAULT_LINK_NAME.to_string(),
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
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        contract_id: "prov:ider".to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler.annotations("ComplexOne")
                    }
                );
            }
            (None, Some(Command::StopProvider(stop))) => {
                assert_eq!(
                    stop,
                    StopProvider {
                        provider_id: provider_id.to_string(),
                        host_id: host_id_four.to_string(),
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        contract_id: "prov:ider".to_string(),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler.annotations("ComplexOne")
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(stop.annotations, spreadscaler.annotations("ComplexOne"))
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
                        host_id: host_id_two.to_string(),
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler.annotations("ComplexTwo")
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler.annotations("ComplexTwo"))
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
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler.annotations("ComplexTwo")
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler.annotations("ComplexTwo"))
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }
}
