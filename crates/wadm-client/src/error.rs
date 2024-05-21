use thiserror::Error;

pub type Result<T> = std::result::Result<T, ClientError>;

/// Errors that can occur when interacting with the wadm client.
#[derive(Error, Debug)]
pub enum ClientError {
    /// Unable to load the manifest from a given source. The underlying error is anyhow::Error to
    /// allow for flexibility in loading from different sources.
    #[error("Unable to load manifest: {0:?}")]
    ManifestLoad(anyhow::Error),
    /// An error occurred with the NATS transport
    #[error(transparent)]
    NatsError(#[from] async_nats::RequestError),
    /// An API error occurred with the request
    #[error("Invalid request: {0}")]
    ApiError(String),
    /// The named model was not found
    #[error("Model not found: {0}")]
    NotFound(String),
    /// Unable to serialize or deserialize YAML or JSON data.
    #[error("Unable to parse manifest: {0:?}")]
    Serialization(#[from] SerializationError),
    /// Any other errors that are not covered by the other error cases
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Errors that can occur when serializing or deserializing YAML or JSON data.
#[derive(Error, Debug)]
pub enum SerializationError {
    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}
