//! A module for connection pools and generators. This is needed because control interface clients
//! (and possibly other things like nats connections in the future) are lattice scoped or need
//! different credentials
use std::marker::PhantomData;

use wasmcloud_control_interface::{
    kv::{Build, KvStore},
    Client, ClientBuilder,
};

// Copied from https://github.com/wasmCloud/control-interface-client/blob/main/src/broker.rs#L1, not public
const DEFAULT_TOPIC_PREFIX: &str = "wasmbus.ctl";

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
pub struct ControlClientConstructor<T> {
    client: async_nats::Client,
    config: ControlClientConfig,
    marker: PhantomData<T>,
}

impl<T: KvStore + Build + Clone> ControlClientConstructor<T> {
    /// Creates a new client pool that is all backed using the same NATS client. The given NATS
    /// client should be using credentials that can access all desired lattices.
    pub fn new(
        client: async_nats::Client,
        config: ControlClientConfig,
    ) -> ControlClientConstructor<T> {
        ControlClientConstructor {
            client,
            config,
            marker: PhantomData,
        }
    }

    /// Get the client for the given lattice ID
    pub async fn get_connection(
        &self,
        id: &str,
        multitenant_prefix: Option<&str>,
    ) -> anyhow::Result<Client<T>> {
        let builder: ClientBuilder<T> =
            ClientBuilder::new_generic(self.client.clone()).lattice_prefix(id);
        let builder = if let Some(domain) = self.config.js_domain.as_deref() {
            builder.js_domain(domain)
        } else {
            builder
        };

        let builder = builder.topic_prefix(topic_prefix(
            multitenant_prefix,
            self.config.topic_prefix.as_deref(),
        ));

        builder
            .build()
            .await
            .map_err(|e| anyhow::anyhow!("Error building client for {id}: {e:?}"))
    }
}

/// Returns the topic prefix to use for the given multitenant prefix and topic prefix. The
/// default prefix is `wasmbus.ctl`.
///
/// If running in multitenant mode, we listen to events on *.wasmbus.evt and need to send commands
/// back to the '*' account. This match takes into account custom prefixes as well to support
/// advanced use cases.
///
/// This function does _not_ take into account whether or not wadm is running in multitenant mode, it's assumed
/// that passing a Some() value for multitenant_prefix means that wadm is running in multitenant mode.
fn topic_prefix(multitenant_prefix: Option<&str>, topic_prefix: Option<&str>) -> String {
    match (multitenant_prefix, topic_prefix) {
        (Some(mt), Some(prefix)) => format!("{}.{}", mt, prefix),
        (Some(mt), None) => format!("{}.{DEFAULT_TOPIC_PREFIX}", mt),
        (None, Some(prefix)) => prefix.to_string(),
        _ => DEFAULT_TOPIC_PREFIX.to_string(),
    }
}
