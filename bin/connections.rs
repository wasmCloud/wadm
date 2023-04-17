//! A module for connection pools and generators. This is needed because control interface clients
//! (and possibly other things like nats connections in the future) are lattice scoped or need
//! different credentials
use wasmcloud_control_interface::{Client, ClientBuilder};

#[derive(Debug, Default, Clone)]
pub struct ControlClientConfig {
    /// The jetstream domain to use for the clients
    pub js_domain: Option<String>,
    /// The topic prefix to use for operations
    pub topic_prefix: Option<String>,
}

/// A client constructor for wasmCloud control interface clients, identified by a lattice ID
// NOTE: Yes, this sounds java-y. Deal with it.
#[derive(Clone)]
pub struct ControlClientConstructor {
    client: async_nats::Client,
    config: ControlClientConfig,
}

impl ControlClientConstructor {
    /// Creates a new client pool that is all backed using the same NATS client. The given NATS
    /// client should be using credentials that can access all desired lattices.
    pub fn new(
        client: async_nats::Client,
        config: ControlClientConfig,
    ) -> ControlClientConstructor {
        ControlClientConstructor { client, config }
    }

    /// Get the client for the given lattice ID
    pub async fn get_connection(&self, id: &str) -> anyhow::Result<Client> {
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
        builder
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Error building client for {id}: {e:?}"))
    }
}
