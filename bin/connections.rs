//! A module for connection pools and generators. This is needed because control interface clients
//! (and possibly other things like nats connections in the future) are lattice scoped or need
//! different credentials

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;
use wasmcloud_control_interface::{Client, ClientBuilder};

#[derive(Debug, Default, Clone)]
pub struct ControlClientConfig {
    /// The jetstream domain to use for the clients
    pub js_domain: Option<String>,
    /// The topic prefix to use for operations
    pub topic_prefix: Option<String>,
}

/// A connection pool for wasmCloud control interface clients, identified by a lattice ID
#[derive(Clone)]
pub struct ControlClientPool {
    client: async_nats::Client,
    config: ControlClientConfig,
    // NOTE(thomastaylor312): At some point we should revisit this. If we are in a massive
    // multi-tenant environment with thousands of lattices, that means this could be thousands of
    // items big. I initially attempted to use the `lru` crate to have a limited number cached in
    // memory (basically all of the hottest ones), but that type doesn't work very well in async
    // contexts behind a RwLock due to the need for it to be `mut`. Having it always locked behind a
    // mutex would introduce its own set of issues. So we may need to come up with another form of
    // eviction here to let it scale a bit better at some point. We could also decide that just
    // creating the client every time is fine as well
    pool: Arc<RwLock<HashMap<String, Client>>>,
}

impl ControlClientPool {
    /// Creates a new client pool that is all backed using the same NATS client. The given NATS
    /// client should be using credentials that can access all desired lattices.
    pub fn new(client: async_nats::Client, config: ControlClientConfig) -> ControlClientPool {
        ControlClientPool {
            client,
            config,
            pool: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the client for the given lattice ID
    pub async fn get_connection(&self, id: &str) -> anyhow::Result<Client> {
        if let Some(client) = self.pool.read().await.get(id) {
            return Ok(client.clone());
        }

        let builder = ClientBuilder::new(self.client.clone()).lattice_prefix(id);
        let builder = if let Some(domain) = self.config.js_domain.as_deref() {
            builder.js_domain(domain)
        } else {
            builder
        };
        let builder = if let Some(prefix) = self.config.topic_prefix.as_deref() {
            builder.topic_prefix(prefix)
        } else {
            builder
        };
        let client = builder
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Error building client for {id}: {e:?}"))?;
        let mut pool = self.pool.write().await;
        pool.insert(id.to_owned(), client.clone());
        Ok(client)
    }
}
