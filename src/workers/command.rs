use tracing::{instrument, trace};
use wasmcloud_control_interface::{kv::KvStore, CtlOperationAck};

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
pub struct CommandWorker<T: Clone> {
    client: wasmcloud_control_interface::Client<T>,
}

impl<T: Clone> CommandWorker<T> {
    /// Creates a new command worker with the given connection pool.
    pub fn new(ctl_client: wasmcloud_control_interface::Client<T>) -> CommandWorker<T> {
        CommandWorker { client: ctl_client }
    }
}

#[async_trait::async_trait]
impl<T: KvStore + Clone + Send + Sync> Worker for CommandWorker<T> {
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
                        Some(actor.count as u16),
                        Some(annotations.into_iter().collect()),
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
                    .map(|_| CtlOperationAck {
                        accepted: true,
                        ..Default::default()
                    })
            }
            Command::DeleteLinkdef(ld) => {
                trace!(command = ?ld, "Handling delete linkdef command");
                self.client
                    .remove_link(&ld.actor_id, &ld.contract_id, &ld.link_name)
                    .await
                    .map(|_| CtlOperationAck {
                        accepted: true,
                        ..Default::default()
                    })
            }
        }
        .map_err(|e| anyhow::anyhow!("{e:?}"));

        match res {
            Ok(ack) if !ack.accepted => {
                message.nack().await;
                Err(WorkError::Other(anyhow::anyhow!("{}", ack.error).into()))
            }
            Ok(_) => message.ack().await.map_err(WorkError::from),
            Err(e) => {
                message.nack().await;
                Err(WorkError::Other(e.into()))
            }
        }
    }
}
