use std::collections::HashMap;

use crate::{
    commands::*,
    consumers::{
        manager::{WorkError, WorkResult, Worker},
        ScopedMessage,
    },
    model::CapabilityConfig,
    APP_SPEC_ANNOTATION,
};

lazy_static::lazy_static! {
    static ref MANAGED_BY_ANNOTATIONS: HashMap<String, String> =
        HashMap::from(
            [(crate::MANAGED_BY_ANNOTATION.to_owned(), crate::MANAGED_BY_IDENTIFIER.to_owned())]
        );
}

/// A worker implementation for handling incoming commands
#[derive(Clone)]
pub struct CommandWorker {
    client: wasmcloud_control_interface::Client,
}

impl CommandWorker {
    /// Creates a new command worker with the given connection pool.
    pub fn new(ctl_client: wasmcloud_control_interface::Client) -> CommandWorker {
        CommandWorker { client: ctl_client }
    }
}

#[async_trait::async_trait]
impl Worker for CommandWorker {
    type Message = Command;

    async fn do_work(&self, mut message: ScopedMessage<Self::Message>) -> WorkResult<()> {
        let res = match message.as_ref() {
            Command::StartActor(actor) => {
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = actor.annotations.clone();
                annotations.extend(MANAGED_BY_ANNOTATIONS.clone());
                annotations.insert(APP_SPEC_ANNOTATION.to_owned(), actor.model_name.clone());
                self.client
                    .start_actor(
                        &actor.host_id,
                        &actor.reference,
                        actor.count as u16,
                        Some(annotations),
                    )
                    .await
            }
            Command::StopActor(actor) => {
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = actor.annotations.clone();
                annotations.extend(MANAGED_BY_ANNOTATIONS.clone());
                annotations.insert(APP_SPEC_ANNOTATION.to_owned(), actor.model_name.clone());
                self.client
                    .stop_actor(
                        &actor.host_id,
                        &actor.actor_id,
                        actor.count as u16,
                        Some(annotations),
                    )
                    .await
            }
            Command::StartProvider(prov) => {
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = prov.annotations.clone();
                annotations.extend(MANAGED_BY_ANNOTATIONS.clone());
                annotations.insert(APP_SPEC_ANNOTATION.to_owned(), prov.model_name.clone());
                let config = prov.config.clone().map(|conf| match conf {
                    // NOTE: We validate the serialization when we store the model so this should be
                    // safe to unwrap
                    CapabilityConfig::Json(conf) => {
                        serde_json::to_string(&conf).unwrap_or_default()
                    }
                    CapabilityConfig::Opaque(conf) => conf,
                });
                self.client
                    .start_provider(
                        &prov.host_id,
                        &prov.reference,
                        prov.link_name.clone(),
                        Some(annotations),
                        config,
                    )
                    .await
            }
            Command::StopProvider(prov) => {
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = prov.annotations.clone();
                annotations.extend(MANAGED_BY_ANNOTATIONS.clone());
                annotations.insert(APP_SPEC_ANNOTATION.to_owned(), prov.model_name.clone());
                self.client
                    .stop_provider(
                        &prov.host_id,
                        &prov.provider_id,
                        prov.link_name
                            .as_deref()
                            .unwrap_or(crate::DEFAULT_LINK_NAME),
                        &prov.contract_id,
                        Some(annotations),
                    )
                    .await
            }
            Command::PutLinkdef(ld) => {
                // TODO(thomastaylor312): We should probably change ScopedMessage to allow us `pub`
                // access to the inner type so we don't have to clone, but no need to worry for now
                self.client
                    .advertise_link(
                        &ld.actor_id,
                        &ld.provider_id,
                        &ld.contract_id,
                        &ld.link_name,
                        ld.values.clone(),
                    )
                    .await
            }
            Command::DeleteLinkdef(ld) => {
                self.client
                    .remove_link(&ld.actor_id, &ld.contract_id, &ld.link_name)
                    .await
            }
        }
        .map_err(|e| anyhow::anyhow!("{e:?}"));

        if let Err(e) = res {
            message.nack().await;
            return Err(WorkError::Other(e.into()));
        }
        message.ack().await.map_err(WorkError::from)
    }
}
