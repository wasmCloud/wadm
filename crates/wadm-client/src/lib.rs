//! A client for interacting with Wadm.
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use async_nats::{HeaderMap, Message};
use error::{ClientError, SerializationError};
use futures::Stream;
use topics::TopicGenerator;
use wadm_types::{
    api::{
        DeleteModelRequest, DeleteModelResponse, DeleteResult, DeployModelRequest,
        DeployModelResponse, DeployResult, GetModelRequest, GetModelResponse, GetResult,
        ModelSummary, PutModelResponse, PutResult, Status, StatusResponse, StatusResult,
        VersionInfo, VersionResponse,
    },
    Manifest,
};

mod nats;

pub mod error;
pub use error::Result;
pub mod loader;
pub use loader::ManifestLoader;
pub mod topics;

/// Headers for `Content-Type: application/json`
static HEADERS_CONTENT_TYPE_JSON: OnceLock<HeaderMap> = OnceLock::new();
/// Retrieve static content type headers
fn get_headers_content_type_json() -> &'static HeaderMap {
    HEADERS_CONTENT_TYPE_JSON.get_or_init(|| {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json");
        headers
    })
}

#[derive(Clone)]
pub struct Client {
    topics: Arc<TopicGenerator>,
    client: async_nats::Client,
}

#[derive(Default, Clone)]
/// Options for connecting to a NATS server for a Wadm client. Setting none of these options will
/// default to anonymous authentication with a localhost NATS server running on port 4222
pub struct ClientConnectOptions {
    /// The URL of the NATS server to connect to. If not provided, the client will connect to the
    /// default NATS address of 127.0.0.1:4222
    pub url: Option<String>,
    /// An nkey seed to use for authenticating with the NATS server. This can either be the raw seed
    /// or a path to a file containing the seed. If used, the `jwt` option must be provided
    pub seed: Option<String>,
    /// A JWT to use for authenticating with the NATS server. This can either be the raw JWT or a
    /// path to a file containing the JWT. If used, the `seed` option must be provided
    pub jwt: Option<String>,
    /// A path to a file containing the credentials to use for authenticating with the NATS server.
    /// If used, the `seed` and `jwt` options must not be provided
    pub creds_path: Option<PathBuf>,
    /// An optional path to a file containing the root CA certificates to use for authenticating
    /// with the NATS server.
    pub ca_path: Option<PathBuf>,
}

impl Client {
    /// Creates a new client with the given lattice ID, optional API prefix, and connection options.
    /// Errors if it is unable to connect to the NATS server
    pub async fn new(
        lattice: &str,
        prefix: Option<&str>,
        opts: ClientConnectOptions,
    ) -> anyhow::Result<Self> {
        let topics = TopicGenerator::new(lattice, prefix);
        let nats_client =
            nats::get_client(opts.url, opts.seed, opts.jwt, opts.creds_path, opts.ca_path).await?;
        Ok(Client {
            topics: Arc::new(topics),
            client: nats_client,
        })
    }

    /// Creates a new client with the given lattice ID, optional API prefix, and NATS client. This
    /// is not recommended and is hidden because the async-nats crate is not 1.0 yet. That means it
    /// is a breaking API change every time we upgrade versions. DO NOT use this function unless you
    /// are willing to accept this breaking change. This function is explicitly excluded from our
    /// semver guarantees until async-nats is 1.0.
    #[doc(hidden)]
    pub fn from_nats_client(
        lattice: &str,
        prefix: Option<&str>,
        nats_client: async_nats::Client,
    ) -> Self {
        let topics = TopicGenerator::new(lattice, prefix);
        Client {
            topics: Arc::new(topics),
            client: nats_client,
        }
    }

    /// Puts the given manifest into the lattice. The lattice can be anything that implements the
    /// [`ManifestLoader`] trait (a path to a file, raw bytes, or an already parsed manifest).
    ///
    /// Returns the name and version of the manifest that was put into the lattice
    pub async fn put_manifest(&self, manifest: impl ManifestLoader) -> Result<(String, String)> {
        let manifest = manifest.load_manifest().await?;
        let manifest_bytes = serde_json::to_vec(&manifest).map_err(SerializationError::from)?;
        let topic = self.topics.model_put_topic();
        let resp = self
            .client
            .request_with_headers(
                topic,
                get_headers_content_type_json().clone(),
                manifest_bytes.into(),
            )
            .await?;
        let body: PutModelResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        if matches!(body.result, PutResult::Error) {
            return Err(ClientError::ApiError(body.message));
        }
        Ok((body.name, body.current_version))
    }

    /// Gets a list of all manifests in the lattice. This does not return the full manifest, just a
    /// summary of its metadata and status
    pub async fn list_manifests(&self) -> Result<Vec<ModelSummary>> {
        let topic = self.topics.model_list_topic();
        let resp = self
            .client
            .request(topic, Vec::with_capacity(0).into())
            .await?;
        let body: Vec<ModelSummary> =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        Ok(body)
    }

    /// Gets a manifest from the lattice by name and optionally its version. If no version is set,
    /// the latest version will be returned
    pub async fn get_manifest(&self, name: &str, version: Option<&str>) -> Result<Manifest> {
        let topic = self.topics.model_get_topic(name);
        let body = if let Some(version) = version {
            serde_json::to_vec(&GetModelRequest {
                version: Some(version.to_string()),
            })
            .map_err(SerializationError::from)?
        } else {
            Vec::with_capacity(0)
        };
        let resp = self.client.request(topic, body.into()).await?;
        let body: GetModelResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;

        match body.result {
            GetResult::Error => Err(ClientError::ApiError(body.message)),
            GetResult::NotFound => Err(ClientError::NotFound(name.to_string())),
            GetResult::Success => body.manifest.ok_or_else(|| {
                ClientError::ApiError("API returned success but didn't set a manifest".to_string())
            }),
        }
    }

    /// Deletes a manifest from the lattice by name and optionally its version. If no version is
    /// set, all versions will be deleted
    ///
    /// Returns true if the manifest was deleted, false if it was a noop (meaning it wasn't found or
    /// was already deleted)
    pub async fn delete_manifest(&self, name: &str, version: Option<&str>) -> Result<bool> {
        let topic = self.topics.model_delete_topic(name);
        let body = if let Some(version) = version {
            serde_json::to_vec(&DeleteModelRequest {
                version: Some(version.to_string()),
            })
            .map_err(SerializationError::from)?
        } else {
            Vec::with_capacity(0)
        };
        let resp = self.client.request(topic, body.into()).await?;
        let body: DeleteModelResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        match body.result {
            DeleteResult::Error => Err(ClientError::ApiError(body.message)),
            DeleteResult::Noop => Ok(false),
            DeleteResult::Deleted => Ok(true),
        }
    }

    /// Gets a list of all versions of a manifest in the lattice
    pub async fn list_versions(&self, name: &str) -> Result<Vec<VersionInfo>> {
        let topic = self.topics.model_versions_topic(name);
        let resp = self
            .client
            .request(topic, Vec::with_capacity(0).into())
            .await?;
        let body: VersionResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        match body.result {
            GetResult::Error => Err(ClientError::ApiError(body.message)),
            GetResult::NotFound => Err(ClientError::NotFound(name.to_string())),
            GetResult::Success => Ok(body.versions),
        }
    }

    /// Deploys a manifest to the lattice. The optional version parameter can be used to deploy a
    /// specific version of a manifest. If no version is set, the latest version will be deployed
    ///
    /// Please note that an OK response does not necessarily mean that the manifest was deployed
    /// successfully, just that the server accepted the deployment request.
    ///
    /// Returns a tuple of the name and version of the manifest that was deployed
    pub async fn deploy_manifest(
        &self,
        name: &str,
        version: Option<&str>,
    ) -> Result<(String, Option<String>)> {
        let topic = self.topics.model_deploy_topic(name);
        let body = if let Some(version) = version {
            serde_json::to_vec(&DeployModelRequest {
                version: Some(version.to_string()),
            })
            .map_err(SerializationError::from)?
        } else {
            Vec::with_capacity(0)
        };
        let resp = self.client.request(topic, body.into()).await?;
        let body: DeployModelResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        match body.result {
            DeployResult::Error => Err(ClientError::ApiError(body.message)),
            DeployResult::NotFound => Err(ClientError::NotFound(name.to_string())),
            DeployResult::Acknowledged => Ok((body.name, body.version)),
        }
    }

    /// A shorthand method that is the equivalent of calling [`put_manifest`](Self::put_manifest)
    /// and then [`deploy_manifest`](Self::deploy_manifest)
    ///
    /// Returns the name and version of the manifest that was deployed. Note that this will always
    /// deploy the latest version of the manifest (i.e. the one that was just put)
    pub async fn put_and_deploy_manifest(
        &self,
        manifest: impl ManifestLoader,
    ) -> Result<(String, String)> {
        let (name, version) = self.put_manifest(manifest).await?;
        // We don't technically need to put the version since we just deployed, but to make sure we
        // maintain that behvior we'll put it here just in case
        self.deploy_manifest(&name, Some(&version)).await?;
        Ok((name, version))
    }

    /// Undeploys the given manifest from the lattice
    ///
    /// Returns Ok(manifest_name) if the manifest undeploy request was acknowledged
    pub async fn undeploy_manifest(&self, name: &str) -> Result<String> {
        let topic = self.topics.model_undeploy_topic(name);
        let resp = self
            .client
            .request(topic, Vec::with_capacity(0).into())
            .await?;
        let body: DeployModelResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        match body.result {
            DeployResult::Error => Err(ClientError::ApiError(body.message)),
            DeployResult::NotFound => Err(ClientError::NotFound(name.to_string())),
            DeployResult::Acknowledged => Ok(body.name),
        }
    }

    /// Gets the status of the given manifest
    pub async fn get_manifest_status(&self, name: &str) -> Result<Status> {
        let topic = self.topics.model_status_topic(name);
        let resp = self
            .client
            .request(topic, Vec::with_capacity(0).into())
            .await?;
        let body: StatusResponse =
            serde_json::from_slice(&resp.payload).map_err(SerializationError::from)?;
        match body.result {
            StatusResult::Error => Err(ClientError::ApiError(body.message)),
            StatusResult::NotFound => Err(ClientError::NotFound(name.to_string())),
            StatusResult::Ok => body.status.ok_or_else(|| {
                ClientError::ApiError("API returned success but didn't set a status".to_string())
            }),
        }
    }

    /// Subscribes to the status of a given manifest
    pub async fn subscribe_to_status(&self, name: &str) -> Result<impl Stream<Item = Message>> {
        let subject = self.topics.wadm_status_topic(name);
        let subscriber = self
            .client
            .subscribe(subject)
            .await
            .map_err(|e| ClientError::ApiError(e.to_string()))?;

        Ok(subscriber)
    }
}
