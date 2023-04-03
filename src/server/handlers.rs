use async_nats::{Client, Message};
use serde_json::json;
use tracing::{debug, error, instrument, trace};

use crate::{model::internal::StoredManifest, storage::Store};

use super::{
    parser::parse_manifest, DeleteModelRequest, DeleteModelResponse, DeleteResult, GetModelRequest,
    GetModelResponse, GetResult, ModelSummary, PutModelResponse, PutResult, StatusType,
    VersionInfo,
};

pub(crate) struct Handler<S> {
    pub(crate) store: S,
    pub(crate) client: Client,
}

impl<S: Store + Send + Sync> Handler<S> {
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn put_model(&self, msg: Message, lattice_id: &str, name: &str) {
        trace!("Parsing incoming manifest");
        let manifest = match parse_manifest(msg.payload.into(), msg.headers.as_ref()) {
            Ok(m) => m,
            Err(e) => {
                self.send_error(msg.reply, format!("Unable to parse manifest: {e:?}"))
                    .await;
                return;
            }
        };
        if manifest.metadata.name != name {
            self.send_error(
                msg.reply,
                "Manifest name doesn't match name from topic".to_string(),
            )
            .await;
            return;
        }

        trace!(
            ?manifest,
            "Manifest is valid. Fetching current manifests from store"
        );

        let mut current_manifests: StoredManifest = match self.store.get(lattice_id, name).await {
            Ok(d) => d.unwrap_or_default(),
            Err(e) => {
                error!(error = %e, "Unable to fetch data from store");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };

        let mut resp = PutModelResponse {
            // If we successfully insert, the given manifest version will be the new current version
            current_version: manifest.version().to_owned(),
            result: if current_manifests.is_empty() {
                PutResult::Created
            } else {
                PutResult::NewVersion
            },
            total_versions: 0,
            message: "Successfully put manifest".to_owned(),
        };

        // TODO: Trigger deploy of new manifest if the previous manifest was deployed

        if !current_manifests.add_version(manifest) {
            self.send_error(
                msg.reply,
                format!("Manifest version {} already exists", resp.current_version),
            )
            .await;
            return;
        }
        resp.total_versions = current_manifests.count();

        trace!(total_manifests = %resp.total_versions, "Storing manifests");
        if let Err(e) = self
            .store
            .store(lattice_id, name.to_owned(), current_manifests)
            .await
        {
            error!(error = %e, "Unable to store updated data");
            self.send_error(msg.reply, "Internal storage error".to_string())
                .await;
            return;
        }

        trace!("Storage complete, sending reply");
        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in case we
            // unwrap to nothing
            serde_json::to_vec(&resp).unwrap_or_default(),
        )
        .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn get_model(&self, msg: Message, lattice_id: &str, name: &str) {
        // For empty payloads, just fetch the latest version
        let req: GetModelRequest = if msg.payload.is_empty() {
            GetModelRequest { version: None }
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse get model request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };

        let manifests: StoredManifest = match self.store.get(lattice_id, name).await {
            Ok(Some(m)) => m,
            Ok(None) => {
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&GetModelResponse {
                        result: GetResult::NotFound,
                        message: format!("Model with the name {name} not found"),
                        manifest: None,
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
            Err(e) => {
                error!(error = %e, "Unable to store updated data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };
        let reply = match req.version {
            Some(version) => {
                if let Some(current) = manifests.get_version(&version) {
                    GetModelResponse {
                        manifest: Some(current.to_owned()),
                        result: GetResult::Success,
                        message: format!("Fetched model {name} with version {version}"),
                    }
                } else {
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just
                        // in case we unwrap to nothing
                        serde_json::to_vec(&GetModelResponse {
                            result: GetResult::NotFound,
                            message: format!("Model {name} with version {} doesn't exist", version),
                            manifest: None,
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
            }
            None => GetModelResponse {
                manifest: Some(manifests.get_current().to_owned()),
                result: GetResult::Success,
                message: format!("Fetched model {name}"),
            },
        };
        // NOTE: We _just_ deserialized this from the store above, so we should be just fine. but
        // just in case we unwrap to the default
        self.send_reply(msg.reply, serde_json::to_vec(&reply).unwrap_or_default())
            .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn list_models(&self, msg: Message, lattice_id: &str) {
        let data: Vec<ModelSummary> = match self.store.list::<StoredManifest>(lattice_id).await {
            Ok(manifests) => {
                manifests
                    .into_iter()
                    .map(|(name, manifest)| {
                        let current = manifest.get_current();
                        let version = current.version();
                        ModelSummary {
                            name,
                            version: version.to_owned(),
                            description: current.description().map(|s| s.to_owned()),
                            deployed: manifest.is_deployed(version),
                            // TODO: Actually fetch the status info from the stored manifest once we
                            // figure it out
                            status: StatusType::default(),
                        }
                    })
                    .collect()
            }
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };
        // NOTE: We _just_ deserialized this from the store above and then manually constructed it,
        // so we should be just fine. Just in case though, we unwrap to default
        self.send_reply(msg.reply, serde_json::to_vec(&data).unwrap_or_default())
            .await
    }

    // NOTE(thomastaylor312): This method differs from the wadm 0.3 docs as it doesn't include
    // timestamp (at least for now). However, this is guaranteed to return the list of versions
    // ordered by time of creation. When we document, we should change this to reflect that
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn list_versions(&self, msg: Message, lattice_id: &str, name: &str) {
        let data: Vec<VersionInfo> = match self.store.get::<StoredManifest>(lattice_id, name).await
        {
            Ok(Some(manifest)) => manifest
                .all_versions()
                .into_iter()
                .cloned()
                .map(|v| {
                    let deployed = manifest.is_deployed(&v);
                    VersionInfo {
                        version: v,
                        deployed,
                    }
                })
                .collect(),
            Ok(None) => Vec::with_capacity(0),
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };
        // NOTE: We _just_ deserialized this from the store above and then manually constructed it,
        // so we should be just fine. Just in case though, we unwrap to default
        self.send_reply(msg.reply, serde_json::to_vec(&data).unwrap_or_default())
            .await
    }

    // NOTE(thomastaylor312): This is different than wadm 0.3. I found it remarkably confusing that
    // you could delete something without undeploying it. So the new behavior is that if a manifest
    // that is deployed is deleted, it is automatically undeployed, and we indicate that to the
    // user. This should be documented when we get to our documentation tasks
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn delete_model(&self, msg: Message, lattice_id: &str, name: &str) {
        let req: DeleteModelRequest =
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse delete model request: {e:?}"),
                    )
                    .await;
                    return;
                }
            };
        let reply_data = if req.delete_all {
            match self.store.delete::<StoredManifest>(lattice_id, name).await {
                Ok(_) => {
                    DeleteModelResponse {
                        result: DeleteResult::Deleted,
                        message: "All models deleted".to_string(),
                        // By default if it is all gone, we definitely undeployed things
                        undeploy: true,
                    }
                }
                Err(e) => {
                    error!(error = %e, "Unable to delete data");
                    DeleteModelResponse {
                        result: DeleteResult::Error,
                        message: "Internal storage error".to_string(),
                        undeploy: false,
                    }
                }
            }
        } else {
            match self.store.get::<StoredManifest>(lattice_id, name).await {
                Ok(Some(mut current)) => {
                    let deleted = current.delete_version(&req.version);
                    if deleted && !current.is_empty() {
                        // If the version we deleted was the deployed one, undeploy it
                        let undeploy = if current.current_version() == req.version {
                            current.undeploy();
                            true
                        } else {
                            false
                        };
                        self.store
                            .store(lattice_id, name.to_owned(), current)
                            .await
                            .map(|_| DeleteModelResponse {
                                result: DeleteResult::Deleted,
                                message: format!("Model version {} deleted", req.version),
                                undeploy,
                            })
                            .unwrap_or_else(|e| {
                                error!(error = %e, "Unable to delete data");
                                DeleteModelResponse {
                                    result: DeleteResult::Deleted,
                                    message: "Internal storage error".to_string(),
                                    undeploy: false,
                                }
                            })
                    } else if deleted && current.is_empty() {
                        // If we deleted the last one, delete the model from the store
                        self.store
                            .delete::<StoredManifest>(lattice_id, name)
                            .await
                            .map(|_| DeleteModelResponse {
                                result: DeleteResult::Deleted,
                                message: "Last model version deleted".to_string(),
                                // By default if it is all gone, we definitely undeployed things
                                undeploy: true,
                            })
                            .unwrap_or_else(|e| {
                                error!(error = %e, "Unable to delete data");
                                DeleteModelResponse {
                                    result: DeleteResult::Deleted,
                                    message: "Internal storage error".to_string(),
                                    undeploy: false,
                                }
                            })
                    } else {
                        DeleteModelResponse {
                            result: DeleteResult::Noop,
                            message: format!("Model version {} doesn't exist", req.version),
                            undeploy: false,
                        }
                    }
                }
                Ok(None) => DeleteModelResponse {
                    result: DeleteResult::Noop,
                    message: format!("Model {name} doesn't exist"),
                    undeploy: false,
                },
                Err(e) => {
                    error!(error = %e, "Unable to fetch current data data");
                    DeleteModelResponse {
                        result: DeleteResult::Error,
                        message: "Internal storage error".to_string(),
                        undeploy: false,
                    }
                }
            }
        };

        // NOTE: We control all the data getting sent in here, but we unwrap to default just in case
        self.send_reply(
            msg.reply,
            serde_json::to_vec(&reply_data).unwrap_or_default(),
        )
        .await
    }

    /// Sends a reply to the topic with the given data, logging an error if one occurs when
    /// sending the reply
    #[instrument(level = "debug", skip(self, data))]
    pub async fn send_reply(&self, reply: Option<String>, data: Vec<u8>) {
        let reply_topic = match reply {
            Some(t) => t,
            None => {
                debug!("No reply topic was sent. Skipping reply");
                return;
            }
        };

        if let Err(e) = self.client.publish(reply_topic, data.into()).await {
            error!(error = %e, "Unable to send reply");
        }
    }

    /// Sends an error reply
    #[instrument(level = "error", skip(self, error_message))]
    pub async fn send_error(&self, reply: Option<String>, error_message: String) {
        // SAFETY: We control the construction of the JSON here and all data going in, so this
        // shouldn't fail except in some sort of really odd case. In those cases, we just unwrap to
        // a default
        let response = serde_json::to_vec(&json!({
            // NOTE: This is a cheating response. Basically all of our API methods have an error
            // variant in their result enum that serializes to this, so we just make it easy on
            // ourselves rather than taking concrete types
            "result": "error",
            "message": error_message,
        }))
        .unwrap_or_default();
        self.send_reply(reply, response).await;
    }
}
