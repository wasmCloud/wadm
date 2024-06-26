use wadm_types::api::{DEFAULT_WADM_TOPIC_PREFIX, WADM_STATUS_API_PREFIX};

/// A generator that uses various config options to generate the proper topic names for the wadm API
pub struct TopicGenerator {
    topic_prefix: String,
    model_prefix: String,
}

impl TopicGenerator {
    /// Creates a new topic generator with a lattice ID and an optional API prefix
    pub fn new(lattice: &str, prefix: Option<&str>) -> TopicGenerator {
        let topic_prefix = format!(
            "{}.{}",
            prefix.unwrap_or(DEFAULT_WADM_TOPIC_PREFIX),
            lattice
        );
        let model_prefix = format!("{}.model", topic_prefix);
        TopicGenerator {
            topic_prefix,
            model_prefix,
        }
    }

    /// Returns the full prefix for the topic, including the API prefix and the lattice ID
    pub fn prefix(&self) -> &str {
        &self.topic_prefix
    }

    /// Returns the full prefix for model operations (currently the only operations supported in the
    /// API)
    pub fn model_prefix(&self) -> &str {
        &self.model_prefix
    }

    /// Returns the full topic for a model put operation
    pub fn model_put_topic(&self) -> String {
        format!("{}.put", self.model_prefix())
    }

    /// Returns the full topic for a model get operation
    pub fn model_get_topic(&self, model_name: &str) -> String {
        format!("{}.get.{model_name}", self.model_prefix())
    }

    /// Returns the full topic for a model delete operation
    pub fn model_delete_topic(&self, model_name: &str) -> String {
        format!("{}.del.{model_name}", self.model_prefix())
    }

    /// Returns the full topic for a model list operation
    pub fn model_list_topic(&self) -> String {
        format!("{}.list", self.model_prefix())
    }

    /// Returns the full topic for listing the versions of a model
    pub fn model_versions_topic(&self, model_name: &str) -> String {
        format!("{}.versions.{model_name}", self.model_prefix())
    }

    /// Returns the full topic for a model deploy operation
    pub fn model_deploy_topic(&self, model_name: &str) -> String {
        format!("{}.deploy.{model_name}", self.model_prefix())
    }

    /// Returns the full topic for a model undeploy operation
    pub fn model_undeploy_topic(&self, model_name: &str) -> String {
        format!("{}.undeploy.{model_name}", self.model_prefix())
    }

    /// Returns the full topic for getting a model status
    pub fn model_status_topic(&self, model_name: &str) -> String {
        format!("{}.status.{model_name}", self.model_prefix())
    }

    /// Returns the full topic for WADM status subscriptions
    pub fn wadm_status_topic(&self, app_name: &str) -> String {
        format!(
            "{}.{}.{}",
            WADM_STATUS_API_PREFIX, self.topic_prefix, app_name
        )
    }
}
