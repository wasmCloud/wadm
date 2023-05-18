//! Contains the internal storage definition of a manifest
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use crate::model::Manifest;
use crate::server::ComponentStatus;

use super::LATEST_VERSION;

/// This struct represents a single manfiest, with its version history. Internally these are stored
/// as an indexmap keyed by version name
#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct StoredManifest {
    // Ordering matters for how we store a manifest, so we need to use an index map to preserve
    // insertion order _and_ have quick access to specific versions
    manifests: IndexMap<String, Manifest>,
    // Set only if a version is deployed
    deployed_version: Option<String>,
    // TODO: Figure out which status to store (probably just the status for each component). We can
    // convert it to the external facing type from the server as needed
    status: Vec<ComponentStatus>,
    // An optional top level status message to help with debugging or status for the whole manifest
    status_message: Option<String>,
}

impl StoredManifest {
    /// Gets the current version of the manifest
    pub fn current_version(&self) -> &str {
        self.manifests
            .last()
            .map(|(v, _)| v.as_str())
            .unwrap_or_default()
    }

    /// Adds the given manifest, returning `false` if unable to add (e.g. the version already
    /// exists)
    pub fn add_version(&mut self, manifest: Manifest) -> bool {
        let version = manifest.version().to_owned();
        if self.manifests.contains_key(&version) {
            return false;
        }
        self.manifests.insert(version, manifest);
        true
    }

    /// Deletes the given version from the manifest. Returning true if it was deleted
    pub fn delete_version(&mut self, version: &str) -> bool {
        self.manifests.shift_remove(version).is_some()
    }

    /// Returns an iterator over all stored versions in creation order
    pub fn all_versions(&self) -> impl IntoIterator<Item = &String> {
        self.manifests.keys()
    }

    #[allow(unused)]
    /// Returns a reference to the deployed version (if it is set)
    pub fn deployed_version(&self) -> Option<&str> {
        self.deployed_version.as_deref()
    }

    /// Helper method that returns true if the given version number was deployed
    pub fn is_deployed(&self, version: &str) -> bool {
        self.deployed_version
            .as_deref()
            .map(|v| v == version)
            .unwrap_or(false)
    }

    /// Sets this manifest as undeployed. Returning true if it was currently deployed
    pub fn undeploy(&mut self) -> bool {
        self.deployed_version.take().is_some()
    }

    /// Attempts to deploy the given version. If none is passed or the version is "latest", it will
    /// deploy the latest version.
    ///
    /// Returns true if it was deployed, false otherwise
    pub fn deploy(&mut self, version: Option<String>) -> bool {
        match version {
            Some(v) if v == LATEST_VERSION => {
                self.deployed_version = Some(self.current_version().to_owned());
                true
            }
            None => {
                self.deployed_version = Some(self.current_version().to_owned());
                true
            }
            Some(v) => {
                if self.manifests.contains_key(&v) {
                    self.deployed_version = Some(v);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Returns a reference to the current manifest
    pub fn get_current(&self) -> &Manifest {
        // SAFETY: This is internal usage only so we will always have at least one thing in here.
        // Panicking here means programmer error
        self.manifests
            .last()
            .map(|(_, v)| v)
            .expect("A manifest should always exist. This is programmer error")
    }

    /// Returns the name of the manifest
    pub fn name(&self) -> &str {
        &self.get_current().metadata.name
    }

    /// Gets a reference to the specified version of the manifest
    pub fn get_version(&self, version: &str) -> Option<&Manifest> {
        self.manifests.get(version)
    }

    /// Returns the deployed version of the manifest (if it is deployed)
    pub fn get_deployed(&self) -> Option<&Manifest> {
        self.deployed_version
            .as_ref()
            .and_then(|v| self.manifests.get(v))
    }

    /// Returns whether or not this is a new (empty) manifest
    pub fn is_empty(&self) -> bool {
        self.manifests.is_empty()
    }

    /// Returns the total count of manifests
    pub fn count(&self) -> usize {
        self.manifests.len()
    }

    /// Returns a reference to the current status message, if any
    pub fn status_message(&self) -> Option<&str> {
        self.status_message.as_deref()
    }

    /// Returns a reference to the current status
    pub fn status(&self) -> &Vec<ComponentStatus> {
        &self.status
    }
}
