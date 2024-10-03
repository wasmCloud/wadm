use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::instrument;
use wadm_types::{api::StatusInfo, TraitProperty};

use crate::{
    commands::{Command, DeleteLink, PutLink},
    events::{
        Event, LinkdefDeleted, LinkdefSet, ProviderHealthCheckInfo, ProviderHealthCheckPassed,
        ProviderHealthCheckStatus,
    },
    scaler::{compute_id_sha256, Scaler},
    storage::ReadStore,
    workers::LinkSource,
};

pub const LINK_SCALER_KIND: &str = "LinkScaler";

/// Config for a LinkSpreadConfig
pub struct LinkScalerConfig {
    /// Component identifier for the source of the link
    pub source_id: String,
    /// Target identifier or group for the link
    pub target: String,
    /// WIT Namespace for the link
    pub wit_namespace: String,
    /// WIT Package for the link
    pub wit_package: String,
    /// WIT Interfaces for the link
    pub wit_interfaces: Vec<String>,
    /// Name of the link
    pub name: String,
    /// Lattice ID the Link is configured for
    pub lattice_id: String,
    /// The name of the wadm model this SpreadScaler is under
    pub model_name: String,
    /// List of configurations for the source of this link
    pub source_config: Vec<String>,
    /// List of configurations for the target of this link
    pub target_config: Vec<String>,
}

/// The LinkSpreadScaler ensures that link configuration exists on a specified lattice.
pub struct LinkScaler<S, L> {
    pub config: LinkScalerConfig,
    // TODO(#253): Reenable once we figure out https://github.com/wasmCloud/wadm/issues/123
    #[allow(unused)]
    store: S,
    ctl_client: L,
    id: String,
    status: RwLock<StatusInfo>,
}

#[async_trait]
impl<S, L> Scaler for LinkScaler<S, L>
where
    S: ReadStore + Send + Sync,
    L: LinkSource + Send + Sync,
{
    fn id(&self) -> &str {
        &self.id
    }

    fn kind(&self) -> &str {
        LINK_SCALER_KIND
    }

    fn name(&self) -> String {
        format!(
            "{} -({}:{})-> {}",
            self.config.source_id,
            self.config.wit_namespace,
            self.config.wit_package,
            self.config.target
        )
    }

    async fn status(&self) -> StatusInfo {
        let _ = self.reconcile().await;
        self.status.read().await.to_owned()
    }

    async fn update_config(&mut self, _config: TraitProperty) -> Result<Vec<Command>> {
        // NOTE(brooksmtownsend): Updating a link scaler essentially means you're creating
        // a totally new scaler, so just do that instead.
        self.reconcile().await
    }

    #[instrument(level = "trace", skip_all, fields(scaler_id = %self.id))]
    async fn handle_event(&self, event: &Event) -> Result<Vec<Command>> {
        match event {
            // Trigger linkdef creation if this component starts and belongs to this model
            Event::ComponentScaled(evt) if evt.component_id == self.config.source_id || evt.component_id == self.config.target => {
                self.reconcile().await
            }
            Event::ProviderHealthCheckPassed(ProviderHealthCheckPassed {
                data: ProviderHealthCheckInfo { provider_id, .. },
                ..
            })
            | Event::ProviderHealthCheckStatus(ProviderHealthCheckStatus {
                data: ProviderHealthCheckInfo { provider_id, .. },
                ..
            // NOTE(brooksmtownsend): Ideally we shouldn't actually care about the target being healthy, but
            // I'm leaving this part in for now to avoid strange conditions in the future where we might want
            // to re-put a link or at least reconcile if the target changes health status.
            }) if provider_id == &self.config.source_id || provider_id == &self.config.target => {
                // Wait until we know the provider is healthy before we link. This also avoids the race condition
                // where a provider is started by the host
                self.reconcile().await
            }
            Event::LinkdefDeleted(LinkdefDeleted {
                source_id,
                wit_namespace,
                wit_package,
                name,
            }) if source_id == &self.config.source_id
                && name == &self.config.name
                && wit_namespace == &self.config.wit_namespace
                && wit_package == &self.config.wit_package =>
            {
                self.reconcile().await
            }
            Event::LinkdefSet(LinkdefSet { linkdef })
                if linkdef.source_id() == self.config.source_id
                    && linkdef.target() == self.config.target
                    && linkdef.name() == self.config.name =>
            {
                *self.status.write().await = StatusInfo::deployed("");
                Ok(Vec::new())
            }
            _ => Ok(Vec::new()),
        }
    }

    #[instrument(level = "trace", skip_all, fields(source_id = %self.config.source_id, target = %self.config.source_id, link_name = %self.config.name, scaler_id = %self.id))]
    async fn reconcile(&self) -> Result<Vec<Command>> {
        let source_id = &self.config.source_id;
        let target = &self.config.target;
        let linkdefs = self.ctl_client.get_links().await?;
        let (exists, _config_different) = linkdefs
            .into_iter()
            .find(|linkdef| {
                linkdef.source_id() == source_id
                    && linkdef.target() == target
                    && linkdef.name() == self.config.name
            })
            .map(|linkdef| {
                (
                    true,
                    // TODO(#88): reverse compare too
                    // Ensure all supplied configs (both source and target) are the same
                    linkdef
                        .source_config()
                        .iter()
                        .eq(self.config.source_config.iter())
                        && linkdef
                            .target_config()
                            .iter()
                            .eq(self.config.target_config.iter()),
                )
            })
            .unwrap_or((false, false));

        // TODO(#88)
        // If it already exists, but values are different, we need to have a delete event first
        // and recreate it with the correct values second
        // let mut commands = values_different
        //     .then(|| {
        //         trace!("Linkdef exists, but values are different, deleting and recreating");
        //         vec![Command::DeleteLinkdef(DeleteLinkdef {
        //             component_id: component_id.to_owned(),
        //             provider_id: provider_id.to_owned(),
        //             contract_id: self.config.provider_contract_id.to_owned(),
        //             link_name: self.config.provider_link_name.to_owned(),
        //             model_name: self.config.model_name.to_owned(),
        //         })]
        //     })
        //     .unwrap_or_default();

        // if exists && !values_different {
        //     trace!("Linkdef already exists, skipping");
        // } else if !exists || values_different {
        //     trace!("Linkdef does not exist or needs to be recreated");
        //     commands.push(Command::PutLinkdef(PutLinkdef {
        //         component_id: component_id.to_owned(),
        //         provider_id: provider_id.to_owned(),
        //         link_name: self.config.provider_link_name.to_owned(),
        //         contract_id: self.config.provider_contract_id.to_owned(),
        //         values: self.config.values.to_owned(),
        //         model_name: self.config.model_name.to_owned(),
        //     }))
        // };

        let commands = if !exists {
            *self.status.write().await = StatusInfo::reconciling(&format!(
                "Putting link definition between {source_id} and {target}"
            ));
            vec![Command::PutLink(PutLink {
                source_id: self.config.source_id.to_owned(),
                target: self.config.target.to_owned(),
                name: self.config.name.to_owned(),
                wit_namespace: self.config.wit_namespace.to_owned(),
                wit_package: self.config.wit_package.to_owned(),
                interfaces: self.config.wit_interfaces.to_owned(),
                source_config: self.config.source_config.clone(),
                target_config: self.config.target_config.clone(),
                model_name: self.config.model_name.to_owned(),
            })]
        } else {
            *self.status.write().await = StatusInfo::deployed("");
            Vec::with_capacity(0)
        };
        Ok(commands)
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        Ok(vec![Command::DeleteLink(DeleteLink {
            model_name: self.config.model_name.to_owned(),
            source_id: self.config.source_id.to_owned(),
            link_name: self.config.name.to_owned(),
            wit_namespace: self.config.wit_namespace.to_owned(),
            wit_package: self.config.wit_package.to_owned(),
        })])
    }
}

impl<S: ReadStore + Send + Sync, L: LinkSource> LinkScaler<S, L> {
    /// Construct a new LinkScaler with specified configuration values
    pub fn new(store: S, link_config: LinkScalerConfig, ctl_client: L) -> Self {
        // Compute the id of this scaler based on all of the configuration values
        // that make it unique. This is used during upgrades to determine if a
        // scaler is the same as a previous one.
        let mut id_parts = vec![
            LINK_SCALER_KIND,
            &link_config.model_name,
            &link_config.name,
            &link_config.source_id,
            &link_config.target,
            &link_config.wit_namespace,
            &link_config.wit_package,
        ];
        id_parts.extend(
            link_config
                .wit_interfaces
                .iter()
                .map(std::string::String::as_str),
        );
        id_parts.extend(
            link_config
                .source_config
                .iter()
                .map(std::string::String::as_str),
        );
        id_parts.extend(
            link_config
                .target_config
                .iter()
                .map(std::string::String::as_str),
        );
        let id = compute_id_sha256(&id_parts);

        Self {
            store,
            config: link_config,
            ctl_client,
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
        vec,
    };

    use wasmcloud_control_interface::Link;

    use chrono::Utc;

    use super::*;

    use crate::{
        events::{ComponentScaled, ProviderHealthCheckInfo, ProviderInfo},
        storage::{Component, Host, Provider, Store},
        test_util::{TestLatticeSource, TestStore},
        APP_SPEC_ANNOTATION,
    };

    async fn create_store(lattice_id: &str, component_ref: &str, provider_ref: &str) -> TestStore {
        let store = TestStore::default();
        store
            .store(
                lattice_id,
                "component".to_string(),
                Component {
                    id: "component".to_string(),
                    reference: component_ref.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("Couldn't store component");
        store
            .store(
                lattice_id,
                "provider".to_string(),
                Provider {
                    id: "provider".to_string(),
                    reference: provider_ref.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("Couldn't store component");
        store
    }

    #[tokio::test]
    async fn test_different_ids() {
        let lattice_id = "id_generator".to_string();
        let component_ref = "component_ref".to_string();
        let component_id = "component_id".to_string();
        let provider_ref = "provider_ref".to_string();
        let provider_id = "provider_id".to_string();

        let source_config = vec!["source_config".to_string()];
        let target_config = vec!["target_config".to_string()];

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &component_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: provider_id.clone(),
                target: component_id.clone(),
                wit_namespace: "wit_namespace".to_string(),
                wit_package: "wit_package".to_string(),
                wit_interfaces: vec!["wit_interface".to_string()],
                name: "default".to_string(),
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
                source_config: source_config.clone(),
                target_config: target_config.clone(),
            },
            TestLatticeSource::default(),
        );

        let other_same_scaler = LinkScaler::new(
            create_store(&lattice_id, &component_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: provider_id.clone(),
                target: component_id.clone(),
                wit_namespace: "wit_namespace".to_string(),
                wit_package: "wit_package".to_string(),
                wit_interfaces: vec!["wit_interface".to_string()],
                name: "default".to_string(),
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
                source_config: source_config.clone(),
                target_config: target_config.clone(),
            },
            TestLatticeSource::default(),
        );

        assert_eq!(scaler.id(), other_same_scaler.id(), "LinkScaler ID should be the same when scalers have the same type, model name, provider link name, component reference, provider reference, and values");

        let different_scaler = LinkScaler::new(
            create_store(&lattice_id, &component_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: provider_id.clone(),
                target: component_id.clone(),
                wit_namespace: "wit_namespace".to_string(),
                wit_package: "wit_package".to_string(),
                wit_interfaces: vec!["wit_interface".to_string()],
                name: "default".to_string(),
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
                source_config: vec!["foo".to_string()],
                target_config: vec!["bar".to_string()],
            },
            TestLatticeSource::default(),
        );

        assert_ne!(
            scaler.id(),
            different_scaler.id(),
            "LinkScaler ID should be different when scalers have different configured values"
        );
    }

    #[tokio::test]
    async fn test_no_linkdef() {
        let lattice_id = "no-linkdef".to_string();
        let component_ref = "component_ref".to_string();
        let component_id = "component".to_string();
        let provider_ref = "provider_ref".to_string();
        let provider_id = "provider".to_string();

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &component_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: component_id.clone(),
                target: provider_id.clone(),
                wit_namespace: "namespace".to_string(),
                wit_package: "package".to_string(),
                wit_interfaces: vec!["interface".to_string()],
                name: "default".to_string(),
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
                source_config: vec![],
                target_config: vec![],
            },
            TestLatticeSource::default(),
        );

        // Run a reconcile and make sure it returns a single put linkdef command
        let commands = scaler.reconcile().await.expect("Couldn't reconcile");
        assert_eq!(commands.len(), 1, "Expected 1 command, got {commands:?}");
        assert!(matches!(commands[0], Command::PutLink(_)));
    }

    #[tokio::test]
    async fn test_existing_linkdef() {
        let lattice_id = "existing-linkdef".to_string();
        let component_ref = "component_ref".to_string();
        let component_id = "component".to_string();
        let provider_ref = "provider_ref".to_string();
        let provider_id = "provider".to_string();

        let linkdef = Link::builder()
            .source_id(&component_id)
            .target(&provider_id)
            .wit_namespace("namespace")
            .wit_package("package")
            .interfaces(vec!["interface".to_string()])
            .name("default")
            .build()
            .unwrap();

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &component_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: linkdef.source_id().to_string(),
                target: linkdef.target().to_string(),
                wit_namespace: linkdef.wit_namespace().to_string(),
                wit_package: linkdef.wit_package().to_string(),
                wit_interfaces: linkdef.interfaces().clone(),
                name: linkdef.name().to_string(),
                source_config: vec![],
                target_config: vec![],
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
            },
            TestLatticeSource {
                links: vec![linkdef],
                ..Default::default()
            },
        );

        let commands = scaler.reconcile().await.expect("Couldn't reconcile");
        assert_eq!(
            commands.len(),
            0,
            "Scaler shouldn't have returned any commands"
        );
    }

    #[tokio::test]
    async fn can_put_linkdef_from_triggering_events() {
        let lattice_id = "can_put_linkdef_from_triggering_events";
        let echo_ref = "fakecloud.azurecr.io/echo:0.3.4".to_string();
        let echo_id = "MASDASDIAMAREALCOMPONENTECHO";
        let httpserver_ref = "fakecloud.azurecr.io/httpserver:0.5.2".to_string();

        let host_id_one = "NASDASDIMAREALHOSTONE";

        let store = Arc::new(TestStore::default());

        // STATE SETUP BEGIN

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    components: HashMap::from_iter([(echo_id.to_string(), 1)]),
                    friendly_name: "hey".to_string(),
                    labels: HashMap::from_iter([
                        ("cloud".to_string(), "fake".to_string()),
                        ("region".to_string(), "us-brooks-1".to_string()),
                    ]),

                    providers: HashSet::from_iter([ProviderInfo {
                        provider_id: "VASDASD".to_string(),
                        provider_ref: httpserver_ref.to_string(),
                        annotations: BTreeMap::from_iter([(
                            APP_SPEC_ANNOTATION.to_string(),
                            "foobar".to_string(),
                        )]),
                    }]),
                    uptime_seconds: 123,
                    version: None,
                    id: host_id_one.to_string(),
                    last_seen: Utc::now(),
                },
            )
            .await
            .expect("should be able to store a host");

        store
            .store(
                lattice_id,
                "VASDASD".to_string(),
                Provider {
                    id: "VASDASD".to_string(),
                    reference: httpserver_ref.to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("should be able to store provider");

        // STATE SETUP END

        let link_scaler = LinkScaler::new(
            store.clone(),
            LinkScalerConfig {
                source_id: echo_id.to_string(),
                target: "VASDASD".to_string(),
                wit_namespace: "wasmcloud".to_string(),
                wit_package: "httpserver".to_string(),
                wit_interfaces: vec![],
                name: "default".to_string(),
                source_config: vec![],
                target_config: vec![],
                lattice_id: lattice_id.to_string(),
                model_name: "foobar".to_string(),
            },
            TestLatticeSource::default(),
        );

        let commands = link_scaler
            .reconcile()
            .await
            .expect("link scaler to handle reconcile");
        // Since no link exists, we should expect a put link command
        assert_eq!(commands.len(), 1);

        // Component starts, put into state and then handle event
        store
            .store(
                lattice_id,
                echo_id.to_string(),
                Component {
                    id: echo_id.to_string(),
                    reference: echo_ref.to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("should be able to store component");

        let commands = link_scaler
            .handle_event(&Event::ComponentScaled(ComponentScaled {
                annotations: BTreeMap::from_iter([(
                    APP_SPEC_ANNOTATION.to_string(),
                    "foobar".to_string(),
                )]),
                claims: None,
                image_ref: echo_ref,
                component_id: echo_id.to_string(),
                max_instances: 1,
                host_id: host_id_one.to_string(),
            }))
            .await
            .expect("should be able to handle components started event");

        assert_eq!(commands.len(), 1);

        let commands = link_scaler
            .handle_event(&Event::LinkdefSet(LinkdefSet {
                linkdef: Link::builder()
                    // NOTE: contract, link, and provider id matches but the component is different
                    .source_id("nm0001772")
                    .target("VASDASD")
                    .wit_namespace("wasmcloud")
                    .wit_package("httpserver")
                    .name("default")
                    .build()
                    .unwrap(),
            }))
            .await
            .expect("");
        assert!(commands.is_empty());

        let commands = link_scaler
            .handle_event(&Event::ProviderHealthCheckPassed(
                ProviderHealthCheckPassed {
                    data: ProviderHealthCheckInfo {
                        provider_id: "VASDASD".to_string(),
                        host_id: host_id_one.to_string(),
                    },
                },
            ))
            .await
            .expect("should be able to handle provider health check");
        assert_eq!(commands.len(), 1);
    }
}
