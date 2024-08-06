use std::collections::BTreeSet;

use anyhow::Result;
use async_nats::jetstream::kv::{Operation, Store};
use tracing::{debug, instrument, trace};

use crate::model::StoredManifest;

// TODO(thomastaylor312): Once async nats has concrete error types for KV, we should switch out
// anyhow for concrete error types so we can indicate whether a failure was due to something like a
// CAS failure or a network error

/// Storage for models, with some logic around updating a list of all models in a lattice to make
/// calls more efficient
#[derive(Clone)]
pub(crate) struct ModelStorage {
    store: Store,
}

impl ModelStorage {
    pub fn new(store: Store) -> ModelStorage {
        Self { store }
    }

    /// Gets the stored data and its current revision for the given model, returning None if it
    /// doesn't exist
    // NOTE(thomastaylor312): The model name is an AsRef purely so we don't have to clone a bunch of
    // things when fetching in the manager. If we expose this struct outside of the crate, we should
    // either revert this to be `&str` or make it `AsRef<str>` everywhere
    #[instrument(level = "debug", skip(self, model_name), fields(model_name = %model_name.as_ref()))]
    pub async fn get(
        &self,
        account_id: Option<&str>,
        lattice_id: &str,
        model_name: impl AsRef<str>,
    ) -> Result<Option<(StoredManifest, u64)>> {
        let key = model_key(account_id, lattice_id, model_name.as_ref());
        debug!(%key, "Fetching model from storage");
        self.store
            .entry(&key)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
            .and_then(|entry| {
                // Skip any delete or purge operations
                if matches!(entry.operation, Operation::Delete | Operation::Purge) {
                    return None;
                }

                Some(
                    serde_json::from_slice::<StoredManifest>(&entry.value)
                        .map_err(anyhow::Error::from)
                        .map(|m| (m, entry.revision)),
                )
            })
            .transpose()
    }

    /// Updates the stored data with the given model, overwriting any existing data. The optional
    /// `current_revision` parameter can be used to compare whether or not you're updating the model
    /// with the latest revision
    #[instrument(level = "debug", skip(self, model), fields(model_name = %model.name()))]
    pub async fn set(
        &self,
        account_id: Option<&str>,
        lattice_id: &str,
        model: StoredManifest,
        current_revision: Option<u64>,
    ) -> Result<()> {
        debug!("Storing model in storage");
        // We need to store the model, then update the set. This is because if we update the set
        // first and the model fails, it will look like the model exists when it actually doesn't
        let key = model_key(account_id, lattice_id, model.name());
        trace!(%key, "Storing manifest at key");
        let data = serde_json::to_vec(&model).map_err(anyhow::Error::from)?;
        if let Some(revision) = current_revision.filter(|r| r > &0) {
            self.store
                .update(&key, data.into(), revision)
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        } else {
            self.store
                .put(&key, data.into())
                .await
                .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        }

        trace!("Adding model to set");
        self.retry_model_update(
            account_id,
            lattice_id,
            ModelNameOperation::Add(model.name()),
        )
        .await
    }

    /// Fetches a summary of all manifests for the given lattice.
    #[instrument(level = "debug", skip(self))]
    pub async fn list(
        &self,
        account_id: Option<&str>,
        lattice_id: &str,
    ) -> Result<Vec<StoredManifest>> {
        debug!("Fetching list of models from storage");
        let futs = self
            .get_model_set(account_id, lattice_id)
            .await?
            .unwrap_or_default()
            .0
            .into_iter()
            // We can't use filter map with futures, but we can use map and then flatten it below
            .map(|model_name| async move {
                match self.get(account_id, lattice_id, &model_name).await {
                    Ok(Some((manifest, _))) => Some(Ok(manifest)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            });

        // Flatten, collect, and sort on name
        futures::future::join_all(futs)
            .await
            .into_iter()
            .flatten()
            .collect::<Result<Vec<StoredManifest>>>()
    }

    /// Deletes the given model from storage. This also removes the model from the list of all
    /// models in the lattice
    #[instrument(level = "debug", skip(self))]
    pub async fn delete(
        &self,
        account_id: Option<&str>,
        lattice_id: &str,
        model_name: &str,
    ) -> Result<()> {
        debug!("Deleting model from storage");
        // We need to delete from the set first, then delete the model itself. This is because if we
        // delete the model but then cannot delete the item from the set, then we end up in a
        // situation where we say it already exists when creating. If the model doesn't delete it is
        // fine, because a set operation will overwrite it
        self.retry_model_update(
            account_id,
            lattice_id,
            ModelNameOperation::Delete(model_name),
        )
        .await?;

        let key = model_key(account_id, lattice_id, model_name);
        trace!("Deleting model from storage");
        self.store
            .purge(&key)
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    /// Helper function that returns the list of models for the given lattice along with the current
    /// revision for use in updating
    async fn get_model_set(
        &self,
        account_id: Option<&str>,
        lattice_id: &str,
    ) -> Result<Option<(BTreeSet<String>, u64)>> {
        match self
            .store
            .entry(model_set_key(account_id, lattice_id))
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))?
        {
            Some(entry) if !matches!(entry.operation, Operation::Delete | Operation::Purge) => {
                let models: BTreeSet<String> =
                    serde_json::from_slice(&entry.value).map_err(anyhow::Error::from)?;
                Ok(Some((models, entry.revision)))
            }
            Some(_) | None => Ok(None),
        }
    }

    /// Convenience wrapper around retrying a key update
    #[instrument(level = "debug", skip(self))]
    async fn retry_model_update<'a>(
        &self,
        account_id: Option<&str>,
        lattice_id: &str,
        operation: ModelNameOperation<'a>,
    ) -> Result<()> {
        // Always retry 3 times for now. We can make this configurable later if we want
        for i in 0..3 {
            trace!("Fetching current models from storage");
            let (mut model_list, current_revision) =
                match self.get_model_set(account_id, lattice_id).await? {
                    Some((models, revision)) => (models, revision),
                    None if matches!(operation, ModelNameOperation::Delete(_)) => {
                        debug!("No models exist in storage for delete, returning early");
                        return Ok(());
                    }
                    None => (BTreeSet::new(), 0),
                };

            match operation {
                ModelNameOperation::Add(model_name) => {
                    if !model_list.insert(model_name.to_owned()) {
                        debug!("Model was already in list, returning early");
                        return Ok(());
                    }
                }
                ModelNameOperation::Delete(model_name) => {
                    if !model_list.remove(model_name) {
                        debug!("Model was not in list, returning early");
                        return Ok(());
                    }
                }
            }

            match self
                .store
                .update(
                    model_set_key(account_id, lattice_id),
                    serde_json::to_vec(&model_list)
                        .map_err(anyhow::Error::from)?
                        .into(),
                    current_revision,
                )
                .await
            {
                Ok(_) => return Ok(()),
                // NOTE(thomastaylor312): This is brittle but will be replaced once the NATS client
                // has a concrete error for KV stuff
                Err(e) if e.to_string().contains("wrong last sequence") => {
                    debug!(error = %e, attempt = i+1, "Model list update failed due to the underlying data changing, retrying");
                    continue;
                }
                Err(e) => {
                    // If it wasn't a wrong last sequence error, then we should bail
                    anyhow::bail!("{e:?}")
                }
            }
        }
        Err(anyhow::anyhow!(
            "Model list update failed due to conflicts after multiple retries"
        ))
    }
}

#[derive(Debug)]
enum ModelNameOperation<'a> {
    Add(&'a str),
    Delete(&'a str),
}

fn model_set_key(account_id: Option<&str>, lattice_id: &str) -> String {
    if let Some(account) = account_id {
        format!("{}-{}", account, lattice_id)
    } else {
        lattice_id.to_string()
    }
}

fn model_key(account_id: Option<&str>, lattice_id: &str, model_name: &str) -> String {
    if let Some(account) = account_id {
        format!("{}-{}-{}", account, lattice_id, model_name)
    } else {
        format!("{}-{}", lattice_id, model_name)
    }
}
