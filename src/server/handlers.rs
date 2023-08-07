use async_nats::{jetstream::stream::Stream, Client, Message};
use base64::{engine::general_purpose::STANDARD as B64decoder, Engine};
use regex::Regex;
use serde_json::json;
use tokio::sync::OnceCell;
use tracing::{debug, error, instrument, log::warn, trace};

use crate::{
    model::{
        internal::StoredManifest, CapabilityConfig, CapabilityProperties, Properties,
        LATEST_VERSION,
    },
    publisher::Publisher,
    server::StatusType,
};

use super::{
    parser::parse_manifest, storage::ModelStorage, DeleteModelRequest, DeleteModelResponse,
    DeleteResult, DeployModelRequest, DeployModelResponse, DeployResult, GetModelRequest,
    GetModelResponse, GetResult, ManifestNotifier, PutModelResponse, PutResult, Status, StatusInfo,
    StatusResponse, StatusResult, UndeployModelRequest, VersionInfo, VersionResponse,
};

static MANIFEST_NAME_REGEX: OnceCell<Regex> = OnceCell::const_new();

pub(crate) struct Handler<P> {
    pub(crate) store: ModelStorage,
    pub(crate) client: Client,
    pub(crate) notifier: ManifestNotifier<P>,
    pub(crate) status_stream: Stream,
}

impl<P: Publisher> Handler<P> {
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn put_model(&self, msg: Message, account_id: Option<&str>, lattice_id: &str) {
        trace!("Parsing incoming manifest");
        let manifest = match parse_manifest(msg.payload.into(), msg.headers.as_ref()) {
            Ok(m) => m,
            Err(e) => {
                self.send_error(msg.reply, format!("Unable to parse manifest: {e:?}"))
                    .await;
                return;
            }
        };

        trace!(
            ?manifest,
            "Manifest is valid. Fetching current manifests from store"
        );

        if manifest.version() == LATEST_VERSION {
            self.send_error(
                msg.reply,
                format!("A manifest with a version {LATEST_VERSION} is not allowed in wadm"),
            )
            .await;
            return;
        }

        // For all components that have JSON config, validate that it can serialize. We need this so
        // it doesn't trigger an error when sending a command down the line
        for component in manifest.spec.components.iter() {
            if let Properties::Capability {
                properties:
                    CapabilityProperties {
                        config: Some(CapabilityConfig::Json(data)),
                        ..
                    },
            } = &component.properties
            {
                if let Err(e) = serde_json::to_string(data) {
                    self.send_error(
                        msg.reply,
                        format!(
                            "Unable to serialize JSON config data for component {}: {e:?}",
                            component.name,
                        ),
                    )
                    .await;
                    return;
                }
            }
        }

        let manifest_name = manifest.metadata.name.trim().to_string();
        if !MANIFEST_NAME_REGEX
            // SAFETY: We know this is valid Regex
            .get_or_init(|| async { Regex::new(r"^[-\w]+$").unwrap() })
            .await
            .is_match(&manifest_name)
        {
            self.send_error(
                msg.reply,
                format!(
                    "Manifest name {} contains invalid characters. Manifest names can only contain alphanumeric characters, dashes, and underscores.",
                    manifest_name
                ),
            )
            .await;
            return;
        }

        let (mut current_manifests, current_revision) =
            match self.store.get(account_id, lattice_id, &manifest_name).await {
                Ok(Some(data)) => data,
                Ok(None) => (StoredManifest::default(), 0),
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
            name: manifest_name.clone(),
            total_versions: 0,
            message: "Successfully put manifest".to_owned(),
        };

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
            .set(
                account_id,
                lattice_id,
                current_manifests,
                Some(current_revision),
            )
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
    pub async fn get_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
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

        let (manifests, _) = match self.store.get(account_id, lattice_id, name).await {
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
                error!(error = %e, "Unable to fetch data");
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
    pub async fn list_models(&self, msg: Message, account_id: Option<&str>, lattice_id: &str) {
        let mut data = match self.store.list(account_id, lattice_id).await {
            Ok(d) => d,
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };

        for model in &mut data {
            if let Some(status) = self.get_manifest_status(lattice_id, &model.name).await {
                model.status = status.status_type;
                model.status_message = Some(status.message);
            } else {
                warn!("Could not fetch status for model, assuming undeployed");
                model.status = StatusType::Undeployed;
                model.status_message = None;
            }
        }

        // NOTE: We _just_ deserialized this from the store above and then manually constructed it,
        // so we should be just fine. Just in case though, we unwrap to default
        self.send_reply(msg.reply, serde_json::to_vec(&data).unwrap_or_default())
            .await
    }

    // NOTE(thomastaylor312): This method differs from the wadm 0.3 docs as it doesn't include
    // timestamp (at least for now). However, this is guaranteed to return the list of versions
    // ordered by time of creation. When we document, we should change this to reflect that
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn list_versions(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let data: VersionResponse = match self.store.get(account_id, lattice_id, name).await {
            Ok(Some((manifest, _))) => VersionResponse {
                result: GetResult::Success,
                message: "Successfully fetched versions".to_string(),
                versions: manifest
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
            },
            Ok(None) => VersionResponse {
                result: GetResult::NotFound,
                message: format!("Model with the name {name} not found"),
                versions: Vec::with_capacity(0),
            },
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
    pub async fn delete_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
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
            match self.store.delete(account_id, lattice_id, name).await {
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
            match self.store.get(account_id, lattice_id, name).await {
                Ok(Some((mut current, current_revision))) => {
                    let deleted = current.delete_version(&req.version);
                    if deleted && !current.is_empty() {
                        // If the version we deleted was the deployed one, undeploy it
                        let deployed_version = current.deployed_version();
                        let undeploy = if deployed_version
                            .map(|v| v == req.version)
                            .unwrap_or(false)
                        {
                            trace!(?deployed_version, deleted_version = %req.version, "Deployed version matches deleted. Will undeploy");
                            current.undeploy();
                            true
                        } else {
                            trace!(?deployed_version, deleted_version = %req.version, "Deployed version does not match deleted version. Will not undeploy");
                            false
                        };
                        self.store
                            .set(account_id, lattice_id, current, Some(current_revision))
                            .await
                            .map(|_| DeleteModelResponse {
                                result: DeleteResult::Deleted,
                                message: format!("Model version {} deleted", req.version),
                                undeploy,
                            })
                            .unwrap_or_else(|e| {
                                error!(error = %e, "Unable to delete data");
                                DeleteModelResponse {
                                    result: DeleteResult::Error,
                                    message: "Internal storage error".to_string(),
                                    undeploy: false,
                                }
                            })
                    } else if deleted && current.is_empty() {
                        // If we deleted the last one, delete the model from the store
                        self.store
                            .delete(account_id, lattice_id, name)
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

        // On a noop, we should still send an undeploy in case of notification failure
        // TODO(thomastaylor312): We might want to come back and revisit how we handle a failure
        // like this in the delete case. If the data gets deleted, but we can't send it, we get into
        // an odd state. So I'd rather err on the side of caution and send a notification that gets
        // ignored
        if reply_data.undeploy || matches!(reply_data.result, DeleteResult::Noop) {
            trace!("Sending undeploy notification");
            if let Err(e) = self.notifier.undeployed(lattice_id, name).await {
                error!(error = ?e, "Error when attempting to send undeploy notification during delete");
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&DeleteModelResponse {
                        result: DeleteResult::Error,
                        message: "Error notifying processors of newly undeployed manifest on delete. This is likely a transient error, so please retry the request. Please note that the response will say it is a noop, but will notify the processors".to_string(),
                        undeploy: false,
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
        }

        // NOTE: We control all the data getting sent in here, but we unwrap to default just in case
        self.send_reply(
            msg.reply,
            serde_json::to_vec(&reply_data).unwrap_or_default(),
        )
        .await
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn deploy_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let req: DeployModelRequest = if msg.payload.is_empty() {
            DeployModelRequest { version: None }
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse deploy model request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };
        trace!(?req, "Got request");

        trace!("Fetching current data from store");
        let (mut manifests, current_revision) =
            match self.store.get(account_id, lattice_id, name).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                        // case we unwrap to nothing
                        serde_json::to_vec(&DeployModelResponse {
                            result: DeployResult::NotFound,
                            message: format!("Model with the name {name} not found"),
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
                Err(e) => {
                    error!(error = %e, "Unable to fetch data");
                    self.send_error(msg.reply, "Internal storage error".to_string())
                        .await;
                    return;
                }
            };

        if !manifests.deploy(req.version) {
            trace!("Requested version does not exist");
            self.send_reply(
                msg.reply,
                // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                // case we unwrap to nothing
                serde_json::to_vec(&DeployModelResponse {
                    result: DeployResult::Error,
                    message: format!(
                        "Model with the name {name} does not have the specified version to deploy"
                    ),
                })
                .unwrap_or_default(),
            )
            .await;
            return;
        }
        // SAFETY: We can unwrap here because we know we _just_ successfully deployed the manifest so they should all exist
        let manifest = manifests
            .get_version(manifests.deployed_version().unwrap())
            .unwrap()
            .to_owned();

        let reply = self
            .store
            .set(account_id, lattice_id, manifests, Some(current_revision))
            .await
            .map(|_| DeployModelResponse {
                result: DeployResult::Acknowledged,
                message: "Deployed model".to_string(),
            })
            .unwrap_or_else(|e| {
                error!(error = %e, "Unable to store updated data");
                DeployModelResponse {
                    result: DeployResult::Error,
                    message: "Internal storage error".to_string(),
                }
            });
        trace!("Manifest saved in store, sending notification");
        if let Err(e) = self.notifier.deployed(lattice_id, manifest).await {
            error!(error = ?e, "Error when attempting to send deployed notification");
            self.send_reply(
                msg.reply,
                // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                // case we unwrap to nothing
                serde_json::to_vec(&DeployModelResponse {
                    result: DeployResult::Error,
                    message: "Error notifying processors of newly deployed manifest. This is likely a transient error, so please retry the request".to_string(),
                })
                .unwrap_or_default(),
            )
            .await;
            return;
        }
        trace!(resp = ?reply, "Sending response");
        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in
            // case we unwrap to nothing
            serde_json::to_vec(&reply).unwrap_or_default(),
        )
        .await;
    }

    // NOTE(thomastaylor312): This is different than wadm 0.3. By default we destructively undeploy
    // unless specified in the request. We also have the exact same acknowledgement types as a
    // deploy request
    #[instrument(level = "debug", skip(self, msg))]
    pub async fn undeploy_model(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        let req: UndeployModelRequest = if msg.payload.is_empty() {
            UndeployModelRequest {
                non_destructive: false,
            }
        } else {
            match serde_json::from_reader(std::io::Cursor::new(msg.payload)) {
                Ok(r) => r,
                Err(e) => {
                    self.send_error(
                        msg.reply,
                        format!("Unable to parse deploy model request: {e:?}"),
                    )
                    .await;
                    return;
                }
            }
        };
        trace!(?req, "Got request");

        trace!("Fetching current data from store");
        let (mut manifests, current_revision) =
            match self.store.get(account_id, lattice_id, name).await {
                Ok(Some(m)) => m,
                Ok(None) => {
                    self.send_reply(
                        msg.reply,
                        // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                        // case we unwrap to nothing
                        serde_json::to_vec(&DeployModelResponse {
                            result: DeployResult::NotFound,
                            message: format!("Model with the name {name} not found"),
                        })
                        .unwrap_or_default(),
                    )
                    .await;
                    return;
                }
                Err(e) => {
                    error!(error = %e, "Unable to fetch data");
                    self.send_error(msg.reply, "Internal storage error".to_string())
                        .await;
                    return;
                }
            };

        let reply = if manifests.undeploy() {
            trace!("Manifest undeployed. Storing updated manifest");

            self.store
                .set(account_id, lattice_id, manifests, Some(current_revision))
                .await
                .map(|_| DeployModelResponse {
                    result: DeployResult::Acknowledged,
                    message: "Undeployed model".to_string(),
                })
                .unwrap_or_else(|e| {
                    error!(error = %e, "Unable to store updated data");
                    DeployModelResponse {
                        result: DeployResult::Error,
                        message: "Internal storage error".to_string(),
                    }
                })
        } else {
            trace!("Manifest was already undeployed");
            DeployModelResponse {
                result: DeployResult::Acknowledged,
                message: "Undeployed model".to_string(),
            }
        };
        // We always want to resend in an undeploy in case things failed last time
        if matches!(reply.result, DeployResult::Acknowledged) {
            trace!("Sending undeploy notification");
            if let Err(e) = self.notifier.undeployed(lattice_id, name).await {
                error!(error = ?e, "Error when attempting to send undeploy notification");
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&DeployModelResponse {
                        result: DeployResult::Error,
                        message: "Error notifying processors of undeployed manifest. This is likely a transient error, so please retry the request".to_string(),
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
        }
        trace!(resp = ?reply, "Sending response");
        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in
            // case we unwrap to nothing
            serde_json::to_vec(&reply).unwrap_or_default(),
        )
        .await;
    }

    #[instrument(level = "debug", skip(self, msg))]
    pub async fn model_status(
        &self,
        msg: Message,
        account_id: Option<&str>,
        lattice_id: &str,
        name: &str,
    ) {
        trace!("Fetching current manifest from store");
        let manifests: StoredManifest = match self.store.get(account_id, lattice_id, name).await {
            Ok(Some((m, _))) => m,
            Ok(None) => {
                self.send_reply(
                    msg.reply,
                    // NOTE: We are constructing all data here, so this shouldn't fail, but just in
                    // case we unwrap to nothing
                    serde_json::to_vec(&StatusResponse {
                        result: StatusResult::NotFound,
                        message: format!("Model with the name {name} not found"),
                        status: None,
                    })
                    .unwrap_or_default(),
                )
                .await;
                return;
            }
            Err(e) => {
                error!(error = %e, "Unable to fetch data");
                self.send_error(msg.reply, "Internal storage error".to_string())
                    .await;
                return;
            }
        };

        let current = manifests.get_current();

        let status = Status {
            version: current.version().to_owned(),
            info: self
                .get_manifest_status(lattice_id, name)
                .await
                .unwrap_or_default(),
            components: vec![],
        };

        self.send_reply(
            msg.reply,
            // NOTE: We are constructing all data here, so this shouldn't fail, but just in
            // case we unwrap to nothing
            serde_json::to_vec(&StatusResponse {
                result: StatusResult::Ok,
                message: "Successfully fetched status".to_string(),
                status: Some(status),
            })
            .unwrap_or_default(),
        )
        .await;
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

    async fn get_manifest_status(&self, lattice_id: &str, name: &str) -> Option<StatusInfo> {
        // NOTE(brooksmtownsend): We're getting the last raw message instead of direct get here
        // to ensure we fetch the latest message from the cluster leader.
        match self
            .status_stream
            .direct_get_last_for_subject(&format!("wadm.status.{lattice_id}.{name}",))
            .await
            .map(|msg| serde_json::from_slice::<StatusInfo>(&msg.payload))
        {
            Ok(Ok(status)) => Some(status),
            // Model status doesn't exist or is invalid, assuming undeployed
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    #[tokio::test]
    async fn manifest_name_regex_works() {
        let regex = super::MANIFEST_NAME_REGEX
            .get_or_init(|| async { regex::Regex::new(r"^[-\w]+$").unwrap() })
            .await;

        // Acceptable manifest names
        let word = "mymanifest";
        let word_with_dash = "my-manifest";
        let word_with_underscore = "my_manifest";
        let word_with_numbers = "mymanifest-v2-v3-final";

        assert!(regex.is_match(word));
        assert!(regex.is_match(word_with_dash));
        assert!(regex.is_match(word_with_underscore));
        assert!(regex.is_match(word_with_numbers));

        // Not acceptable manifest names
        let word_with_period = "my.manifest";
        let word_with_space = "my manifest";
        assert!(!regex.is_match(word_with_period));
        assert!(!regex.is_match(word_with_space));
    }
}
