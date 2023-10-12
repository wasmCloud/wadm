use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::{OnceCell, RwLock};
use tracing::{instrument, trace};

use crate::commands::StopProvider;
use crate::events::{HostHeartbeat, ProviderInfo};
use crate::model::{CapabilityConfig, Spread};
use crate::scaler::spreadscaler::provider::ProviderSpreadConfig;
use crate::scaler::spreadscaler::{eligible_hosts, spreadscaler_annotations};
use crate::server::StatusInfo;
use crate::storage::Provider;
use crate::{
    commands::{Command, StartProvider},
    events::{Event, HostStarted, HostStopped},
    model::{SpreadScalerProperty, TraitProperty},
    scaler::Scaler,
    storage::{Host, ReadStore},
};

// Annotation constants
pub const PROVIDER_DAEMON_SCALER_TYPE: &str = "providerdaemonscaler";

/// The ProviderDaemonScaler ensures that a provider is running on every host, according to a
/// [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of replicas
/// on every available host.
pub struct ProviderDaemonScaler<S> {
    config: ProviderSpreadConfig,
    provider_id: OnceCell<String>,
    store: S,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ProviderDaemonScaler<S> {
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
                replicas: spread_config.replicas,
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
                if self.config.spread_config.spread.iter().any(|spread| {
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

    #[instrument(level = "trace", skip_all, fields(name = %self.config.model_name, scaler_id = %self.id))]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        let hosts = self.store.list::<Host>(&self.config.lattice_id).await?;

        let provider_id = self.provider_id().await.unwrap_or_default();
        let contract_id = &self.config.provider_contract_id;
        let link_name = &self.config.provider_link_name;
        let provider_ref = &self.config.provider_reference;

        let mut spread_status = vec![];

        trace!(spread = ?self.config.spread_config.spread, ?provider_id, "Computing commands");
        let commands = self
            .config
            .spread_config
            .spread
            .iter()
            .flat_map(|spread| {
                let eligible_hosts = eligible_hosts(&hosts, spread);
                if !eligible_hosts.is_empty() {
                    eligible_hosts
                        .iter()
                        // Filter out hosts that are already running this provider
                        .filter_map(|(_host_id, host)| {
                            let provider_on_host = host.providers.get(&ProviderInfo {
                                contract_id: contract_id.to_string(),
                                link_name: link_name.to_string(),
                                public_key: provider_id.to_string(),
                                annotations: BTreeMap::default(),
                            });
                            match (provider_on_host, self.config.spread_config.replicas) {
                                // Spread replicas set to 0 means we're cleaning up and should stop
                                // running providers
                                (Some(_), 0) => Some(Command::StopProvider(StopProvider {
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
                                    link_name: Some(self.config.provider_link_name.to_owned()),
                                    contract_id: self.config.provider_contract_id.to_owned(),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: spreadscaler_annotations(&spread.name, &self.id),
                                })),
                                // Whenever replicas > 0, we should start a provider if it's not already running
                                (None, _n) => Some(Command::StartProvider(StartProvider {
                                    reference: provider_ref.to_owned(),
                                    host_id: host.id.to_string(),
                                    link_name: Some(link_name.to_owned()),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: spreadscaler_annotations(&spread.name, &self.id),
                                    config: self.config.provider_config.clone(),
                                })),
                                _ => None,
                            }
                        })
                        .collect::<Vec<Command>>()
                } else {
                    // No hosts were eligible, so we can't attempt to add or remove providers
                    trace!(?spread.name, "Found no eligible hosts for daemon scaler");
                    spread_status.push(StatusInfo::failed(&format!(
                        "Could not satisfy daemonscaler {} for {}, 0 eligible hosts found.",
                        spread.name, self.config.provider_reference
                    )));
                    vec![]
                }
            })
            .collect::<Vec<Command>>();

        trace!(?commands, "Calculated commands for provider daemonscaler");

        let status = match (spread_status.is_empty(), commands.is_empty()) {
            (true, true) => StatusInfo::ready(""),
            (_, false) => StatusInfo::compensating(""),
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
        config_clone.spread_config.replicas = 0;

        let cleanerupper = ProviderDaemonScaler {
            config: config_clone,
            store: self.store.clone(),
            provider_id: self.provider_id.clone(),
            id: self.id.clone(),
            status: RwLock::new(StatusInfo::compensating("")),
        };

        cleanerupper.reconcile().await
    }
}

impl<S: ReadStore + Send + Sync> ProviderDaemonScaler<S> {
    /// Construct a new ProviderDaemonScaler with specified configuration values
    pub fn new(store: S, config: ProviderSpreadConfig, component_name: &str) -> Self {
        let id = {
            let default = format!(
                "{PROVIDER_DAEMON_SCALER_TYPE}-{}-{component_name}-{}-{}",
                config.model_name, config.provider_reference, config.provider_link_name,
            );

            match &config.provider_config {
                Some(provider_config) => {
                    if let Some(provider_config_hash) =
                        compute_provider_config_hash(provider_config)
                    {
                        format!(
                            "{PROVIDER_DAEMON_SCALER_TYPE}-{}-{component_name}-{}-{}-{}",
                            config.model_name,
                            config.provider_reference,
                            config.provider_link_name,
                            provider_config_hash
                        )
                    } else {
                        default
                    }
                }
                None => default,
            }
        };

        // If no spreads are specified, an empty spread is sufficient to match _every_ host
        // in a lattice
        let spread_config = if config.spread_config.spread.is_empty() {
            SpreadScalerProperty {
                replicas: config.spread_config.replicas,
                spread: vec![Spread::default()],
            }
        } else {
            config.spread_config
        };
        Self {
            store,
            provider_id: OnceCell::new(),
            config: ProviderSpreadConfig {
                spread_config,
                ..config
            },
            id,
            status: RwLock::new(StatusInfo::compensating("")),
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

fn compute_provider_config_hash(provider_config: &CapabilityConfig) -> Option<u64> {
    match provider_config.try_base64_encoding() {
        Ok(base64) => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            base64.hash(&mut hasher);
            Some(hasher.finish())
        }
        Err(_) => None,
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
        commands::{Command, StartProvider},
        model::{CapabilityConfig, Spread, SpreadScalerProperty},
        scaler::{spreadscaler::spreadscaler_annotations, Scaler},
        storage::{Host, Provider, Store},
        test_util::TestStore,
        DEFAULT_LINK_NAME,
    };

    use super::*;

    const MODEL_NAME: &str = "test_provider_spreadscaler";

    #[test]
    fn test_id_generator() {
        let config = ProviderSpreadConfig {
            lattice_id: "lattice".to_string(),
            provider_reference: "provider".to_string(),
            provider_link_name: "link".to_string(),
            provider_contract_id: "contract".to_string(),
            model_name: MODEL_NAME.to_string(),
            spread_config: SpreadScalerProperty {
                replicas: 1,
                spread: vec![],
            },
            provider_config: None,
        };

        let scaler = ProviderDaemonScaler::new(Arc::new(TestStore::default()), config, "component");
        assert_eq!(
            scaler.id(),
            format!(
                "{PROVIDER_DAEMON_SCALER_TYPE}-{}-component-provider-link",
                MODEL_NAME
            ),
            "ProviderDaemonScaler ID should be valid"
        );

        let config = ProviderSpreadConfig {
            lattice_id: "lattice".to_string(),
            provider_reference: "provider".to_string(),
            provider_link_name: "link".to_string(),
            provider_contract_id: "contract".to_string(),
            model_name: MODEL_NAME.to_string(),
            spread_config: SpreadScalerProperty {
                replicas: 1,
                spread: vec![],
            },
            provider_config: Some(CapabilityConfig::Opaque("foobar".to_string())),
        };

        let scaler = ProviderDaemonScaler::new(Arc::new(TestStore::default()), config, "component");
        assert_eq!(
            scaler.id(),
            format!(
                "{PROVIDER_DAEMON_SCALER_TYPE}-{}-component-provider-link-{}",
                MODEL_NAME,
                compute_provider_config_hash(&CapabilityConfig::Opaque("foobar".to_string()))
                    .unwrap()
            ),
            "ProviderDaemonScaler ID should be valid"
        );

        let mut scaler_id_tokens = scaler.id().split('-');
        scaler_id_tokens.next_back();
        let scaler_id_tokens = scaler_id_tokens.collect::<Vec<&str>>().join("-");
        assert_eq!(
            scaler_id_tokens,
            format!(
                "{PROVIDER_DAEMON_SCALER_TYPE}-{}-component-provider-link",
                MODEL_NAME
            ),
            "ProviderDaemonScaler ID should be valid and depends on provider_config"
        );
    }

    #[tokio::test]
    async fn can_spread_on_multiple_hosts() -> Result<()> {
        let lattice_id = "provider_spread_multi_host";
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
                    actors: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("inda".to_string(), "cloud".to_string()),
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-noneofyourbusiness-1".to_string()),
                    ]),
                    annotations: BTreeMap::new(),
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
                        ("inda".to_string(), "cloud".to_string()),
                        ("cloud".to_string(), "real".to_string()),
                        ("region".to_string(), "us-yourhouse-1".to_string()),
                    ]),
                    annotations: BTreeMap::new(),
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
            // Replicas are ignored so putting an absurd number
            replicas: 12312,
            spread: vec![Spread {
                name: "SimpleOne".to_string(),
                requirements: BTreeMap::from_iter([("inda".to_string(), "cloud".to_string())]),
                weight: Some(100),
            }],
        };

        let spreadscaler = ProviderDaemonScaler::new(
            store.clone(),
            ProviderSpreadConfig {
                lattice_id: lattice_id.to_string(),
                provider_reference: provider_ref.to_string(),
                spread_config: multi_spread_even,
                provider_contract_id: "prov:ider".to_string(),
                provider_link_name: DEFAULT_LINK_NAME.to_string(),
                model_name: MODEL_NAME.to_string(),
                provider_config: Some(CapabilityConfig::Opaque("foobar".to_string())),
            },
            "fake_component",
        );

        let mut commands = spreadscaler.reconcile().await?;
        assert_eq!(commands.len(), 2);
        // Sort to enable predictable test
        commands.sort_unstable_by(|a, b| match (a, b) {
            (Command::StartProvider(a), Command::StartProvider(b)) => a.host_id.cmp(&b.host_id),
            _ => panic!("Should have been start providers"),
        });

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
                        annotations: spreadscaler_annotations("SimpleOne", spreadscaler.id()),
                        config: Some(CapabilityConfig::Opaque("foobar".to_string())),
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
                        host_id: host_id_two.to_string(),
                        link_name: Some(DEFAULT_LINK_NAME.to_string()),
                        model_name: MODEL_NAME.to_string(),
                        annotations: spreadscaler_annotations("SimpleTwo", spreadscaler.id()),
                        config: Some(CapabilityConfig::Opaque("foobar".to_string())),
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
}
