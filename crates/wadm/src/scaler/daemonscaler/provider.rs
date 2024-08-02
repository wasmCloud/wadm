use std::collections::BTreeMap;

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::{instrument, trace};
use wadm_types::{api::StatusInfo, Spread, SpreadScalerProperty, TraitProperty};

use crate::commands::StopProvider;
use crate::events::{HostHeartbeat, ProviderInfo, ProviderStarted, ProviderStopped};
use crate::scaler::compute_id_sha256;
use crate::scaler::spreadscaler::{
    compute_ineligible_hosts, eligible_hosts, provider::ProviderSpreadConfig,
    spreadscaler_annotations,
};
use crate::SCALER_KEY;
use crate::{
    commands::{Command, StartProvider},
    events::{Event, HostStarted, HostStopped},
    scaler::Scaler,
    storage::{Host, ReadStore},
};

use super::DAEMON_SCALER_KIND;

/// The ProviderDaemonScaler ensures that a provider is running on every host, according to a
/// [SpreadScalerProperty](crate::model::SpreadScalerProperty)
///
/// If no [Spreads](crate::model::Spread) are specified, this Scaler simply maintains the number of instances
/// on every available host.
pub struct ProviderDaemonScaler<S> {
    config: ProviderSpreadConfig,
    store: S,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S: ReadStore + Send + Sync + Clone> Scaler for ProviderDaemonScaler<S> {
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        DAEMON_SCALER_KIND
    }

    fn name(&self) -> String {
        self.config.provider_id.to_string()
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

        let provider_id = &self.config.provider_id;
        let provider_ref = &self.config.provider_reference;

        let ineligible_hosts = compute_ineligible_hosts(
            &hosts,
            self.config
                .spread_config
                .spread
                .iter()
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
                                provider_id: provider_id.to_string(),
                                provider_ref: provider_ref.to_string(),
                                annotations: BTreeMap::default(),
                            });
                            match (provider_on_host, self.config.spread_config.instances) {
                                // Spread instances set to 0 means we're cleaning up and should stop
                                // running providers
                                (Some(_), 0) => Some(Command::StopProvider(StopProvider {
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
                                    model_name: self.config.model_name.to_owned(),
                                    annotations: spreadscaler_annotations(&spread.name, &self.id),
                                })),
                                // Whenever instances > 0, we should start a provider if it's not already running
                                (None, _n) => Some(Command::StartProvider(StartProvider {
                                    reference: provider_ref.to_owned(),
                                    provider_id: provider_id.to_owned(),
                                    host_id: host.id.to_string(),
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

        let cleanerupper = ProviderDaemonScaler {
            config: config_clone,
            store: self.store.clone(),
            id: self.id.clone(),
            status: RwLock::new(StatusInfo::reconciling("")),
        };

        cleanerupper.reconcile().await
    }
}

impl<S: ReadStore + Send + Sync> ProviderDaemonScaler<S> {
    /// Construct a new ProviderDaemonScaler with specified configuration values
    pub fn new(store: S, config: ProviderSpreadConfig, component_name: &str) -> Self {
        // Compute the id of this scaler based on all of the configuration values
        // that make it unique. This is used during upgrades to determine if a
        // scaler is the same as a previous one.
        let mut id_parts = vec![
            DAEMON_SCALER_KIND,
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

        // If no spreads are specified, an empty spread is sufficient to match _every_ host
        // in a lattice
        let spread_config = if config.spread_config.spread.is_empty() {
            SpreadScalerProperty {
                instances: config.spread_config.instances,
                spread: vec![Spread::default()],
            }
        } else {
            config.spread_config
        };
        Self {
            store,
            config: ProviderSpreadConfig {
                spread_config,
                ..config
            },
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
        commands::{Command, StartProvider},
        scaler::{spreadscaler::spreadscaler_annotations, Scaler},
        storage::{Host, Provider, Store},
        test_util::TestStore,
    };

    use super::*;

    const MODEL_NAME: &str = "test_provider_spreadscaler";

    #[test]
    fn test_id_generator() {
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
            ProviderDaemonScaler::new(Arc::new(TestStore::default()), config, "myprovider");

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
            ProviderDaemonScaler::new(Arc::new(TestStore::default()), config, "myprovider");
        assert_ne!(
            scaler1.id(),
            scaler2.id(),
            "ProviderDaemonScaler IDs should be different with different configuration"
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
                    components: HashMap::new(),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("inda".to_string(), "cloud".to_string()),
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
                        ("inda".to_string(), "cloud".to_string()),
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
            // instances are ignored so putting an absurd number
            instances: 12312,
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
                provider_id: provider_id.to_string(),
                provider_reference: provider_ref.to_string(),
                spread_config: multi_spread_even,
                model_name: MODEL_NAME.to_string(),
                provider_config: vec!["foobar".to_string()],
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
                    spreadscaler_annotations("SimpleOne", spreadscaler.id())
                )
            }
            Some(_other) => panic!("command should have been a start provider"),
        }

        Ok(())
    }
}
