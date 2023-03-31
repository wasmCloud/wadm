use async_nats::{Client, Subscriber};
use futures::StreamExt;
use tracing::{info, instrument, warn};

use crate::storage::Store;

mod handlers;
mod parser;
mod types;

use handlers::Handler;
pub use parser::CONTENT_TYPE_HEADER;
pub use types::*;

/// The default topic prefix for the wadm API;
pub const DEFAULT_WADM_TOPIC_PREFIX: &str = "wadm.api";

const QUEUE_GROUP: &str = "wadm_server";

/// A server for the wadm API
pub struct Server<S> {
    handler: Handler<S>,
    subscriber: Subscriber,
    prefix: String,
}

impl<S: Store + Send + Sync> Server<S> {
    /// Returns a new server configured with the given store, NATS client, and optional topic
    /// prefix. Returns an error if it can't subscribe on the right topics
    ///
    /// In most cases, you shouldn't need a custom topic prefix, but it is exposed for the cases
    /// when you may need to set a custom prefix for security purposes or topic segregation
    #[instrument(level = "info", skip_all)]
    pub async fn new(
        store: S,
        client: Client,
        topic_prefix: Option<&str>,
    ) -> anyhow::Result<Server<S>> {
        // Trim off any spaces or trailing/preceding dots
        let prefix = topic_prefix
            .unwrap_or(DEFAULT_WADM_TOPIC_PREFIX)
            .trim()
            .trim_matches('.')
            .to_owned();
        if prefix.is_empty() {
            anyhow::bail!("Given prefix was empty")
        }

        let topic = format!("{prefix}.>");
        info!(%topic, "Creating API subscriber");
        let subscriber = client
            .queue_subscribe(topic, QUEUE_GROUP.to_owned())
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;

        Ok(Server {
            handler: Handler { store, client },
            subscriber,
            prefix,
        })
    }

    /// Starts the server, consuming it.
    ///
    /// This function will run until it either returns an error (which should always be fatal) or
    /// you stop polling the future
    #[instrument(level = "info", skip_all)]
    pub async fn serve(mut self) -> anyhow::Result<()> {
        while let Some(msg) = self.subscriber.next().await {
            if !msg.subject.starts_with(&self.prefix) {
                warn!(subject = %msg.subject, "Received message on an invalid subject");
                continue;
            }

            // Cloning here to avoid using owned string matching _everywhere_. If we don't use a
            // struct with borrowed strings, then the matches in the block below have to be owned
            // strings. But we need to pass the message to consume the data off of it in the
            // handlers
            let subject = msg.subject.clone();
            let parsed = match self.parse_subject(&subject) {
                Ok(p) => p,
                Err(e) => {
                    self.handler
                        .send_error(msg.reply, format!("Invalid subject: {e:?}"))
                        .await;
                    continue;
                }
            };

            match parsed {
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "list",
                    object_name: None,
                } => self.handler.list_models(msg, lattice_id).await,
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "get",
                    object_name: Some(name),
                } => self.handler.get_model(msg, lattice_id, name).await,
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "put",
                    object_name: Some(name),
                } => self.handler.put_model(msg, lattice_id, name).await,
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "del",
                    object_name: Some(name),
                } => self.handler.delete_model(msg, lattice_id, name).await,
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "versions",
                    object_name: Some(name),
                } => self.handler.list_versions(msg, lattice_id, name).await,
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "deploy",
                    object_name: Some(name),
                } => todo!(),
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "undeploy",
                    object_name: Some(name),
                } => todo!(),
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "status",
                    object_name: Some(name),
                } => todo!(),
                ParsedSubject {
                    lattice_id,
                    category: "model",
                    operation: "history",
                    object_name: Some(name),
                } => todo!(),
                _ => {
                    let err = format!("Unsupported subject: {}", msg.subject);
                    self.handler.send_error(msg.reply, err).await;
                }
            }
        }
        return Err(anyhow::anyhow!("Subscriber terminated"));
    }

    fn parse_subject<'a>(&self, subject: &'a str) -> anyhow::Result<ParsedSubject<'a>> {
        // Topic structure: wadm.api.{lattice-id}.{category}.{operation}.{object}
        // First, clean off the prefix and then split and iterate
        let mut trimmed = subject
            .trim_start_matches(&self.prefix)
            .trim_start_matches('.')
            .split('.')
            .fuse();
        let lattice_id = trimmed
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected to find lattice ID"))?;
        let category = trimmed
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected to find API category"))?;
        let operation = trimmed
            .next()
            .ok_or_else(|| anyhow::anyhow!("Expected to find operation"))?;
        // Some commands don't have names, so this is optional
        let object_name = trimmed.next();
        // Catch malformed long subjects
        if trimmed.next().is_some() {
            anyhow::bail!("Found extra components of subject")
        }
        Ok(ParsedSubject {
            lattice_id,
            category,
            operation,
            object_name,
        })
    }
}

struct ParsedSubject<'a> {
    lattice_id: &'a str,
    category: &'a str,
    operation: &'a str,
    object_name: Option<&'a str>,
}
