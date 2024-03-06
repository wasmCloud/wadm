use std::hash::{Hash, Hasher};

use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::RwLock;
use tracing::instrument;

use crate::{
    commands::{Command, DeleteLinkdef, PutLinkdef},
    events::{
        Event, LinkdefDeleted, LinkdefSet, ProviderHealthCheckInfo, ProviderHealthCheckPassed,
        ProviderHealthCheckStatus,
    },
    model::TraitProperty,
    scaler::Scaler,
    server::StatusInfo,
    storage::ReadStore,
    workers::LinkSource,
};

pub const LINK_SCALER_TYPE: &str = "linkdefscaler";

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
    /// List of named configurations for the source of this link
    pub source_config: Vec<String>,
    /// List of named configurations for the target of this link
    pub target_config: Vec<String>,
}

/// The LinkSpreadScaler ensures that link configuration exists on a specified lattice.
pub struct LinkScaler<S, L> {
    pub config: LinkScalerConfig,
    // TODO: Reenable once we figure out https://github.com/wasmCloud/wadm/issues/123
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
            // Trigger linkdef creation if this actor starts and belongs to this model
            Event::ActorScaled(evt) if evt.actor_id == self.config.source_id => {
                self.reconcile().await
            }
            Event::ProviderHealthCheckPassed(ProviderHealthCheckPassed {
                data: ProviderHealthCheckInfo { provider_id, .. },
                ..
            })
            | Event::ProviderHealthCheckStatus(ProviderHealthCheckStatus {
                data: ProviderHealthCheckInfo { provider_id, .. },
                ..
            }) if provider_id == &self.config.source_id => {
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
                && wit_package == &self.config.wit_namespace =>
            {
                self.reconcile().await
            }
            Event::LinkdefSet(LinkdefSet { linkdef })
                if linkdef.source_id == self.config.source_id
                    && linkdef.target == self.config.target
                    && linkdef.name == self.config.name =>
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
                &linkdef.source_id == source_id
                    && &linkdef.target == target
                    && linkdef.name == self.config.name
            })
            .map(|linkdef| {
                (
                    true,
                    linkdef.source_config != self.config.source_config
                        || linkdef.target_config != self.config.target_config,
                )
            })
            .unwrap_or((false, false));

        // TODO(brooksmtownsend): Now that links are ID based not public key based, we should be able to reenable this
        // TODO: Reenable this functionality once we figure out https://github.com/wasmCloud/wadm/issues/123

        // If it already exists, but values are different, we need to have a delete event first
        // and recreate it with the correct values second
        // let mut commands = values_different
        //     .then(|| {
        //         trace!("Linkdef exists, but values are different, deleting and recreating");
        //         vec![Command::DeleteLinkdef(DeleteLinkdef {
        //             actor_id: actor_id.to_owned(),
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
        //         actor_id: actor_id.to_owned(),
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
            vec![Command::PutLinkdef(PutLinkdef {
                source_id: self.config.source_id.to_owned(),
                target: self.config.target.to_owned(),
                name: self.config.name.to_owned(),
                wit_namespace: self.config.wit_namespace.to_owned(),
                wit_package: self.config.wit_package.to_owned(),
                interfaces: self.config.wit_interfaces.to_owned(),
                source_config: self.config.source_config.to_owned(),
                target_config: self.config.target_config.to_owned(),
                model_name: self.config.model_name.to_owned(),
            })]
        } else {
            *self.status.write().await = StatusInfo::deployed("");
            Vec::with_capacity(0)
        };
        Ok(commands)
    }

    async fn cleanup(&self) -> Result<Vec<Command>> {
        Ok(vec![Command::DeleteLinkdef(DeleteLinkdef {
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(store: S, link_config: LinkScalerConfig, ctl_client: L) -> Self {
        // NOTE(thomastaylor312): Yep, this is gnarly, but it was all the information that would be
        // useful to have if uniquely identifying a link scaler
        let linkscaler_config_hash =
            compute_linkscaler_config_hash(&link_config.source_config, &link_config.target_config);
        let id = format!(
            "{LINK_SCALER_TYPE}-{}-{}-{}-{}-{linkscaler_config_hash}",
            link_config.model_name, link_config.name, link_config.source_id, link_config.target,
        );

        Self {
            store,
            config: link_config,
            ctl_client,
            id,
            status: RwLock::new(StatusInfo::reconciling("")),
        }
    }
}

fn compute_linkscaler_config_hash(source: &Vec<String>, target: &Vec<String>) -> u64 {
    let mut linkscaler_config_hasher = std::collections::hash_map::DefaultHasher::new();
    source
        .iter()
        .for_each(|s| s.hash(&mut linkscaler_config_hasher));
    target
        .iter()
        .for_each(|t| t.hash(&mut linkscaler_config_hasher));
    linkscaler_config_hasher.finish()
}

#[cfg(test)]
mod test {
    use std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::Arc,
        vec,
    };

    use wasmcloud_control_interface::InterfaceLinkDefinition;

    use chrono::Utc;

    use super::*;

    use crate::{
        events::{ActorClaims, ActorsStarted, ProviderHealthCheckInfo, ProviderInfo},
        storage::{Actor, Host, Provider, Store},
        test_util::{TestLatticeSource, TestStore},
        APP_SPEC_ANNOTATION,
    };

    async fn create_store(lattice_id: &str, actor_ref: &str, provider_ref: &str) -> TestStore {
        let store = TestStore::default();
        store
            .store(
                lattice_id,
                "actor".to_string(),
                Actor {
                    id: "actor".to_string(),
                    reference: actor_ref.to_owned(),
                    ..Default::default()
                },
            )
            .await
            .expect("Couldn't store actor");
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
            .expect("Couldn't store actor");
        store
    }

    #[tokio::test]
    async fn test_id_generator() {
        let lattice_id = "id_generator".to_string();
        let actor_ref = "actor_ref".to_string();
        let actor_id = "actor_id".to_string();
        let provider_ref = "provider_ref".to_string();
        let provider_id = "provider_id".to_string();

        let source_config = vec!["source_config".to_string()];
        let target_config = vec!["target_config".to_string()];

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: provider_id.clone(),
                target: actor_id.clone(),
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

        let id = format!(
            "{LINK_SCALER_TYPE}-{model_name}-{link_name}-{provider_id}-{actor_id}-{linkscaler_values_hash}",
            LINK_SCALER_TYPE = LINK_SCALER_TYPE,
            model_name = "model",
            link_name = "default",
            linkscaler_values_hash = compute_linkscaler_config_hash(&source_config, &target_config)
        );

        assert_eq!(scaler.id(), id, "LinkScaler ID should be the same when scalers have the same type, model name, provider link name, actor reference, provider reference, and values");

        let id = format!(
            "{LINK_SCALER_TYPE}-{model_name}-{link_name}-{actor_id}-{provider_id}-{linkscaler_values_hash}",
            LINK_SCALER_TYPE = LINK_SCALER_TYPE,
            model_name = "model",
            link_name = "default",
            linkscaler_values_hash = compute_linkscaler_config_hash(&vec!["foo".to_string()], &vec!["bar".to_string()])
        );

        assert_ne!(
            scaler.id(),
            id,
            "LinkScaler ID should be different when scalers have different configured values"
        );

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: actor_id.clone(),
                target: provider_id.clone(),
                wit_namespace: "contr".to_string(),
                wit_package: "act".to_string(),
                wit_interfaces: vec!["interface".to_string()],
                name: "default".to_string(),
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
                source_config: vec![],
                target_config: vec![],
            },
            TestLatticeSource::default(),
        );

        let id = format!(
            "{LINK_SCALER_TYPE}-{model_name}-{link_name}-{actor_id}-{provider_id}-{linkscaler_values_hash}",
            LINK_SCALER_TYPE = LINK_SCALER_TYPE,
            model_name = "model",
            link_name = "default",
            linkscaler_values_hash = compute_linkscaler_config_hash(&vec![], &vec![])
        );

        assert_eq!(scaler.id(), id, "LinkScaler ID should be the same when their type, model name, provider link name, actor reference, and provider reference are the same and they both have no values configured");

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: actor_id.clone(),
                target: provider_id.clone(),
                wit_namespace: "contr".to_string(),
                wit_package: "act".to_string(),
                wit_interfaces: vec!["interface".to_string()],
                name: "default".to_string(),
                lattice_id: lattice_id.clone(),
                model_name: "model".to_string(),
                source_config: vec!["default-http".to_string()],
                target_config: vec!["outbound-cert".to_string()],
            },
            TestLatticeSource::default(),
        );

        assert_ne!(scaler.id(), id, "Expected LinkScaler values hash to differiantiate scalers with the same type, model name, provider link name, actor reference, and provider reference");
    }

    #[tokio::test]
    async fn test_no_linkdef() {
        let lattice_id = "no-linkdef".to_string();
        let actor_ref = "actor_ref".to_string();
        let actor_id = "actor".to_string();
        let provider_ref = "provider_ref".to_string();
        let provider_id = "provider".to_string();

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: actor_id.clone(),
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
        assert!(matches!(commands[0], Command::PutLinkdef(_)));
    }

    // TODO: Uncomment once https://github.com/wasmCloud/wadm/issues/123 is fixed

    // #[tokio::test]
    // async fn test_different_values() {
    //     let lattice_id = "different-values".to_string();
    //     let actor_ref = "actor_ref".to_string();
    //     let provider_ref = "provider_ref".to_string();

    //     let values = HashMap::from([("foo".to_string(), "bar".to_string())]);

    //     let mut linkdef = LinkDefinition::default();
    //     linkdef.actor_id = "actor".to_string();
    //     linkdef.provider_id = "provider".to_string();
    //     linkdef.contract_id = "contract".to_string();
    //     linkdef.link_name = "default".to_string();
    //     linkdef.values = [("foo".to_string(), "nope".to_string())].into();

    //     let scaler = LinkScaler::new(
    //         create_store(&lattice_id, &actor_ref, &provider_ref).await,
    //         actor_ref,
    //         provider_ref,
    //         "contract".to_string(),
    //         None,
    //         lattice_id.clone(),
    //         "model".to_string(),
    //         Some(values),
    //         TestLatticeSource {
    //             links: vec![linkdef],
    //             ..Default::default()
    //         },
    //     );

    //     let commands = scaler.reconcile().await.expect("Couldn't reconcile");
    //     assert_eq!(commands.len(), 2);
    //     assert!(matches!(commands[0], Command::DeleteLinkdef(_)));
    //     assert!(matches!(commands[1], Command::PutLinkdef(_)));
    // }

    #[tokio::test]
    async fn test_existing_linkdef() {
        let lattice_id = "existing-linkdef".to_string();
        let actor_ref = "actor_ref".to_string();
        let actor_id = "actor".to_string();
        let provider_ref = "provider_ref".to_string();
        let provider_id = "provider".to_string();

        let linkdef = InterfaceLinkDefinition {
            source_id: actor_id.to_string(),
            target: provider_id.to_string(),
            wit_namespace: "namespace".to_string(),
            wit_package: "package".to_string(),
            interfaces: vec!["interface".to_string()],
            name: "default".to_string(),
            source_config: vec![],
            target_config: vec![],
        };

        let scaler = LinkScaler::new(
            create_store(&lattice_id, &actor_ref, &provider_ref).await,
            LinkScalerConfig {
                source_id: linkdef.source_id.clone(),
                target: linkdef.target.clone(),
                wit_namespace: linkdef.wit_namespace.clone(),
                wit_package: linkdef.wit_package.clone(),
                wit_interfaces: linkdef.interfaces.clone(),
                name: linkdef.name.clone(),
                source_config: linkdef.source_config.clone(),
                target_config: linkdef.target_config.clone(),
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
        let echo_id = "MASDASDIAMAREALACTORECHO";
        let httpserver_ref = "fakecloud.azurecr.io/httpserver:0.5.2".to_string();

        let host_id_one = "NASDASDIMAREALHOSTONE";

        let store = Arc::new(TestStore::default());

        // STATE SETUP BEGIN

        store
            .store(
                lattice_id,
                host_id_one.to_string(),
                Host {
                    actors: HashMap::from_iter([(echo_id.to_string(), 1)]),
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
        // TODO(brooksmtownsend): link scaler should? wait until the source is running before creating the link
        assert!(commands.is_empty());

        // Actor starts, put into state and then handle event
        store
            .store(
                lattice_id,
                echo_id.to_string(),
                Actor {
                    id: echo_id.to_string(),
                    reference: echo_ref.to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect("should be able to store actor");

        let commands = link_scaler
            .handle_event(&Event::ActorsStarted(ActorsStarted {
                annotations: BTreeMap::from_iter([(
                    APP_SPEC_ANNOTATION.to_string(),
                    "foobar".to_string(),
                )]),
                claims: ActorClaims::default(),
                image_ref: echo_ref,
                count: 1,
                public_key: echo_id.to_string(),
                host_id: host_id_one.to_string(),
            }))
            .await
            .expect("should be able to handle actors started event");

        assert_eq!(commands.len(), 1);

        let commands = link_scaler
            .handle_event(&Event::LinkdefSet(LinkdefSet {
                linkdef: InterfaceLinkDefinition {
                    // NOTE: contract, link, and provider id matches but the actor is different
                    source_id: "nm0001772".to_string(),
                    target: "VASDASD".to_string(),
                    wit_namespace: "wasmcloud".to_string(),
                    wit_package: "httpserver".to_string(),
                    interfaces: vec![],
                    name: "default".to_string(),
                    source_config: vec![],
                    target_config: vec![],
                },
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
