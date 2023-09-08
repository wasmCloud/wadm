use tracing::{instrument, trace};

use crate::{
    commands::*,
    consumers::{
        manager::{WorkError, WorkResult, Worker},
        ScopedMessage,
    },
    model::CapabilityConfig,
};

use super::insert_managed_annotations;

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

    #[instrument(level = "trace", skip_all)]
    async fn do_work(&self, mut message: ScopedMessage<Self::Message>) -> WorkResult<()> {
        let res = match message.as_ref() {
            Command::ScaleActor(actor) => {
                trace!(command = ?actor, "Handling scale actor command");
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = actor.annotations.clone();
                insert_managed_annotations(&mut annotations, &actor.model_name);
                self.client
                    .scale_actor(
                        &actor.host_id,
                        &actor.reference,
                        actor
                            .actor_id
                            .as_ref()
                            .map(|s| s.as_str())
                            .unwrap_or_default(),
                        actor.count as u16,
                        Some(annotations),
                    )
                    .await
            }
            Command::StartProvider(prov) => {
                trace!(command = ?prov, "Handling start provider command");
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = prov.annotations.clone();
                insert_managed_annotations(&mut annotations, &prov.model_name);
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
                        Some(annotations.into_iter().collect()),
                        config,
                    )
                    .await
            }
            Command::StopProvider(prov) => {
                trace!(command = ?prov, "Handling stop provider command");
                // Order here is intentional to prevent scalers from overwriting managed annotations
                let mut annotations = prov.annotations.clone();
                insert_managed_annotations(&mut annotations, &prov.model_name);
                self.client
                    .stop_provider(
                        &prov.host_id,
                        &prov.provider_id,
                        prov.link_name
                            .as_deref()
                            .unwrap_or(crate::DEFAULT_LINK_NAME),
                        &prov.contract_id,
                        Some(annotations.into_iter().collect()),
                    )
                    .await
            }
            Command::PutLinkdef(ld) => {
                trace!(command = ?ld, "Handling put linkdef command");
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
                trace!(command = ?ld, "Handling delete linkdef command");
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
