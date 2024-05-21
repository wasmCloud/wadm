//! Helpers for creating a NATS client without exposing the NATS client in the API
use std::path::PathBuf;

use anyhow::{Context, Result};
use async_nats::{Client, ConnectOptions};

const DEFAULT_NATS_ADDR: &str = "nats://127.0.0.1:4222";

/// Creates a NATS client from the given options
pub async fn get_client(
    url: Option<String>,
    seed: Option<String>,
    jwt: Option<String>,
    creds_path: Option<PathBuf>,
    ca_path: Option<PathBuf>,
) -> Result<Client> {
    let mut opts = ConnectOptions::new();
    opts = match (seed, jwt, creds_path) {
        (Some(seed), Some(jwt), None) => {
            let jwt = resolve_jwt(jwt).await?;
            let kp = std::sync::Arc::new(get_seed(seed).await?);

            opts.jwt(jwt, move |nonce| {
                let key_pair = kp.clone();
                async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
            })
        }
        (None, None, Some(creds)) => opts.credentials_file(creds).await?,
        (None, None, None) => opts,
        _ => {
            // We shouldn't ever get here due to the requirements on the flags, but return a helpful error just in case
            return Err(anyhow::anyhow!(
                "Got incorrect combination of connection options. Should either have nothing set, a seed, a jwt, or a credentials file"
            ));
        }
    };
    if let Some(ca) = ca_path {
        opts = opts.add_root_certificates(ca).require_tls(true);
    }
    opts.connect(url.unwrap_or_else(|| DEFAULT_NATS_ADDR.to_string()))
        .await
        .map_err(Into::into)
}

/// Takes a string that could be a raw seed, or a path and does all the necessary loading and parsing steps
async fn get_seed(seed: String) -> Result<nkeys::KeyPair> {
    // MAGIC NUMBER: Length of a seed key
    let raw_seed = if seed.len() == 58 && seed.starts_with('S') {
        seed
    } else {
        tokio::fs::read_to_string(seed)
            .await
            .context("Unable to read seed file")?
    };

    nkeys::KeyPair::from_seed(&raw_seed).map_err(anyhow::Error::from)
}

/// Resolves a JWT value by either returning the string itself if it's a valid JWT
/// or by loading the contents of a file specified by the JWT value.
async fn resolve_jwt(jwt_or_file: String) -> Result<String> {
    if tokio::fs::metadata(&jwt_or_file)
        .await
        .map(|metadata| metadata.is_file())
        .unwrap_or(false)
    {
        tokio::fs::read_to_string(jwt_or_file)
            .await
            .map_err(|e| anyhow::anyhow!("Error loading JWT from file: {e}"))
    } else {
        // We could do more validation on the JWT here, but if the JWT is invalid then
        // connecting will fail anyways
        Ok(jwt_or_file)
    }
}
