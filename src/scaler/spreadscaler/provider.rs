use std::{cmp::Ordering, collections::HashMap};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{mpsc::Sender, OnceCell};
use tracing::{instrument, trace};

use crate::{
    commands::{Command, StartProvider, StopProvider},
    events::{Event, HostHeartbeat, HostStarted, HostStopped, ProviderInfo},
    model::{Spread, SpreadScalerProperty, TraitProperty},
    scaler::{
        spreadscaler::{compute_spread, eligible_hosts, spreadscaler_annotations},
        Scaler,
    },
    storage::{Host, Provider, ReadStore},
    DEFAULT_LINK_NAME,
};

/// Config for a ProviderSpreadConfig
#[derive(Clone)]
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
}

/// The ProviderSpreadScaler ensures that a certain number of provider replicas are running,
/// spread across a number of hosts according to a [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of replicas
/// on an available host. It's important to note that only one instance of a provider id + link + contract
/// can run on each host, so it's possible to specify too many replicas to be satisfied by the spread configs.
pub struct ProviderSpreadScaler<S: ReadStore + Send + Sync> {
    store: S,
    pub config: ProviderSpreadConfig,
    spread_requirements: Vec<(Spread, usize)>,
    /// Provider ID, stored in a OnceCell to facilitate more efficient fetches
    provider_id: OnceCell<String>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ProviderSpreadScaler<S> {
    #[instrument(level = "debug", skip_all)]
    async fn update_config(&mut self, config: TraitProperty) -> Result<Vec<Command>> {
        let spread_config = match config {
            TraitProperty::SpreadScaler(prop) => prop,
            _ => anyhow::bail!("Given config was not a spread scaler config object"),
        };
        self.spread_requirements = compute_spread(&spread_config);
        self.config.spread_config = spread_config;
        self.reconcile().await
    }

    #[instrument(level = "debug", skip_all)]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): We could be more efficient here and instead of running
        // the entire reconcile, smart compute exactly what needs to change, but it just
        // requires more code branches and would be fine as a future improvement
        match event {
            Event::ProviderStarted(provider_started)
                if provider_started.contract_id == self.config.provider_contract_id
                    && provider_started.image_ref == self.config.provider_reference
                    && provider_started.link_name == self.config.provider_link_name =>
            {
                self.reconcile().await
            }
            Event::ProviderStopped(provider_stopped)
                if provider_stopped.contract_id == self.config.provider_contract_id
                    && provider_stopped.link_name == self.config.provider_link_name
                    // If this is None, provider hasn't been started in the lattice yet, so we don't need to reconcile
                    && self.provider_id().await.map(|id| id == provider_stopped.public_key).unwrap_or(false) =>
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

    #[instrument(level = "debug", skip_all, fields(provider_ref = %self.config.provider_reference, link_name = %self.config.provider_link_name))]
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
                                annotations: HashMap::default(),
                            })
                            .map(|provider| {
                                spreadscaler_annotations(&spread.name).iter().all(|(k, v)| {
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
                            })
                            .unwrap_or_else(|| {
                                trace!(%provider_id, "Couldn't find matching provider in host provider list");
                                false}
                            )
                    });
                trace!(current = %running_for_spread.len(), expected = %count, "Calculated running providers, reconciling with expected count");
                match running_for_spread.len().cmp(count) {
                    Ordering::Equal => Vec::new(),
                    Ordering::Greater => {
                        let num_to_stop = running_for_spread.len() - count;
                        // Take `num_to_stop` commands from this iterator
                        running_for_spread
                            .iter()
                            .map(|host| {
                                Command::StopProvider(StopProvider {
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
                                    link_name: Some(self.config.provider_link_name.to_owned()),
                                    contract_id: self.config.provider_contract_id.to_owned(),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: spreadscaler_annotations(&spread.name),
                                })
                            })
                            .take(num_to_stop)
                            .collect::<Vec<Command>>()
                    }
                    Ordering::Less => {
                        let num_to_start = count - running_for_spread.len();

                        // NOTE(brooksmtownsend): It's possible that this does not fully satisfy
                        // the requirements if we are unable to form enough start commands. Update
                        // status accordingly once we have a way to.

                        // Take `num_to_start` commands from this iterator
                        other
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
                                    annotations: spreadscaler_annotations(&spread.name),
                                })
                            })
                            .take(num_to_start)
                            .collect::<Vec<Command>>()
                    }
                }
            })
            .collect::<Vec<Command>>())
    }

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name))]
    async fn cleanup(&self) -> Result<Vec<Command>> {
        let mut config_clone = self.config.clone();
        config_clone.spread_config.replicas = 0;
        let spread_requirements = compute_spread(&config_clone.spread_config);

        let cleanerupper = ProviderSpreadScaler {
            store: self.store.clone(),
            config: config_clone,
            spread_requirements,
            provider_id: self.provider_id.clone(),
        };

        cleanerupper.reconcile().await
    }

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name))]
    async fn backoff(&self, notifier: Sender<std::string::String>) {
        let _ = notifier.send(String::with_capacity(0)).await;
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
            provider_id: OnceCell::new(),
            config: ProviderSpreadConfig {
                provider_reference,
                provider_contract_id,
                provider_link_name: provider_link_name
                    .unwrap_or_else(|| DEFAULT_LINK_NAME.to_string()),
                lattice_id,
                spread_config,
                model_name,
            },
        }
    }

    /// Helper function to retrieve the provider ID for the configured provider
    async fn provider_id(&self) -> Result<&str> {
        self.provider_id
            .get_or_try_init(|| async {
                self.store
                    .list::<Provider>(&self.config.lattice_id)
                    .await
                    .unwrap_or_default()
                    .iter()
                    .find(|(_id, provider)| provider.reference == self.config.provider_reference)
                    .map(|(_id, provider)| provider.id.to_owned())
                    .ok_or_else(|| {
                        anyhow::anyhow!(
                            "Couldn't find a provider id for the provider reference {}",
                            self.config.provider_reference
                        )
                    })
            })
            .await
            .map(|id| id.as_str())
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
        scaler::{
            spreadscaler::{provider::ProviderSpreadScaler, spreadscaler_annotations},
            Scaler,
        },
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
                        annotations: spreadscaler_annotations("SimpleOne"),
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler_annotations("SimpleOne"))
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
                        annotations: spreadscaler_annotations("SimpleTwo"),
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler_annotations("SimpleTwo"))
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
                        annotations: spreadscaler_annotations("ComplexOne"),
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
                        annotations: spreadscaler_annotations("ComplexOne"),
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
                        annotations: spreadscaler_annotations("ComplexOne")
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
                        annotations: spreadscaler_annotations("ComplexOne")
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(stop.annotations, spreadscaler_annotations("ComplexOne"))
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
                        annotations: spreadscaler_annotations("ComplexTwo")
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler_annotations("ComplexTwo"))
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
                        annotations: spreadscaler_annotations("ComplexTwo")
                    }
                );
                // This manual assertion is because we don't hash on annotations and I want to be extra sure we have the
                // correct ones
                assert_eq!(start.annotations, spreadscaler_annotations("ComplexTwo"))
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }
}
