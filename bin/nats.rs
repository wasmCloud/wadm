use std::path::PathBuf;

use anyhow::Result;
use async_nats::{
    jetstream::{
        self,
        kv::{Config as KvConfig, Store},
        stream::{Config as StreamConfig, Stream},
        Context,
    },
    Client, ConnectOptions,
};

use wadm::DEFAULT_EXPIRY_TIME;

/// Creates a NATS client from the given options
pub async fn get_client_and_context(
    url: String,
    js_domain: Option<String>,
    seed: Option<String>,
    jwt_path: Option<PathBuf>,
    creds_path: Option<PathBuf>,
) -> Result<(Client, Context)> {
    let client = if seed.is_none() && jwt_path.is_none() && creds_path.is_none() {
        async_nats::connect(url).await?
    } else {
        let opts = build_nats_options(seed, jwt_path, creds_path).await?;
        async_nats::connect_with_options(url, opts).await?
    };

    let context = if let Some(domain) = js_domain {
        jetstream::with_domain(client.clone(), domain)
    } else {
        jetstream::new(client.clone())
    };

    Ok((client, context))
}

pub async fn get_alt_client(
    url: String,
    seed: Option<String>,
    jwt_path: Option<PathBuf>,
    creds_path: Option<PathBuf>,
) -> Result<alt_nats::Client> {
    let client = if seed.is_none() && jwt_path.is_none() && creds_path.is_none() {
        alt_nats::connect(url).await?
    } else {
        let opts = build_alt_nats_options(seed, jwt_path, creds_path).await?;
        alt_nats::connect_with_options(url, opts).await?
    };

    Ok(client)
}

async fn build_nats_options(
    seed: Option<String>,
    jwt_path: Option<PathBuf>,
    creds_path: Option<PathBuf>,
) -> Result<ConnectOptions> {
    match (seed, jwt_path, creds_path) {
        (Some(seed), Some(jwt), None) => {
            let jwt = tokio::fs::read_to_string(jwt).await?;
            let kp = std::sync::Arc::new(get_seed(seed).await?);

            Ok(async_nats::ConnectOptions::with_jwt(jwt, move |nonce| {
                let key_pair = kp.clone();
                async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
            }))
        }
        (None, None, Some(creds)) => async_nats::ConnectOptions::with_credentials_file(creds)
            .await
            .map_err(anyhow::Error::from),
        _ => {
            // We shouldn't ever get here due to the requirements on the flags, but return a helpful error just in case
            Err(anyhow::anyhow!(
                "Got too many options. Make sure to provide a seed and jwt or a creds path"
            ))
        }
    }
}

async fn build_alt_nats_options(
    seed: Option<String>,
    jwt_path: Option<PathBuf>,
    creds_path: Option<PathBuf>,
) -> Result<alt_nats::ConnectOptions> {
    match (seed, jwt_path, creds_path) {
        (Some(seed), Some(jwt), None) => {
            let jwt = tokio::fs::read_to_string(jwt).await?;
            let kp = std::sync::Arc::new(get_seed(seed).await?);

            Ok(alt_nats::ConnectOptions::with_jwt(jwt, move |nonce| {
                let key_pair = kp.clone();
                async move { key_pair.sign(&nonce).map_err(alt_nats::AuthError::new) }
            }))
        }
        (None, None, Some(creds)) => alt_nats::ConnectOptions::with_credentials_file(creds)
            .await
            .map_err(anyhow::Error::from),
        _ => {
            // We shouldn't ever get here due to the requirements on the flags, but return a helpful error just in case
            Err(anyhow::anyhow!(
                "Got too many options. Make sure to provide a seed and jwt or a creds path"
            ))
        }
    }
}

/// Takes a string that could be a raw seed, or a path and does all the necessary loading and parsing steps
async fn get_seed(seed: String) -> Result<nkeys::KeyPair> {
    // MAGIC NUMBER: Length of a seed key
    let raw_seed = if seed.len() == 58 && seed.starts_with('S') {
        seed
    } else {
        tokio::fs::read_to_string(seed).await?
    };

    nkeys::KeyPair::from_seed(&raw_seed).map_err(anyhow::Error::from)
}

/// A helper that ensures that the given stream name exists, using defaults to create if it does
/// not. Returns the handle to the stream
pub async fn ensure_stream(
    context: &Context,
    name: String,
    subject: String,
    description: Option<String>,
) -> Result<Stream> {
    context
        .get_or_create_stream(StreamConfig {
            name,
            description,
            num_replicas: 1,
            retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
            subjects: vec![subject],
            max_age: DEFAULT_EXPIRY_TIME,
            storage: async_nats::jetstream::stream::StorageType::File,
            allow_rollup: false,
            ..Default::default()
        })
        .await
        .map_err(|e| anyhow::anyhow!("{e:?}"))
}

/// A helper that ensures that the given KV bucket exists, using defaults to create if it does
/// not. Returns the handle to the stream
pub async fn ensure_kv_bucket(
    context: &Context,
    name: String,
    history_to_keep: i64,
) -> Result<Store> {
    if let Ok(kv) = context.get_key_value(&name).await {
        Ok(kv)
    } else {
        context
            .create_key_value(KvConfig {
                bucket: name,
                history: history_to_keep,
                num_replicas: 1,
                storage: jetstream::stream::StorageType::File,
                ..Default::default()
            })
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }
}
