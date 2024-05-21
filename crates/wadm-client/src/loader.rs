//! Various helpers and traits for loading and parsing manifests

use std::{
    future::Future,
    path::{Path, PathBuf},
};

use wadm_types::Manifest;

use crate::{error::ClientError, Result};

/// A trait for loading a [`Manifest`] from a variety of sources. This is also used as a convenience
/// trait in the client for easily passing in any type of Manifest
pub trait ManifestLoader {
    fn load_manifest(self) -> impl Future<Output = Result<Manifest>>;
}

impl ManifestLoader for &Manifest {
    async fn load_manifest(self) -> Result<Manifest> {
        Ok(self.clone())
    }
}

impl ManifestLoader for Manifest {
    async fn load_manifest(self) -> Result<Manifest> {
        Ok(self)
    }
}

impl ManifestLoader for Vec<u8> {
    async fn load_manifest(self) -> Result<Manifest> {
        parse_yaml_or_json(self).map_err(Into::into)
    }
}

impl ManifestLoader for &[u8] {
    async fn load_manifest(self) -> Result<Manifest> {
        parse_yaml_or_json(self).map_err(Into::into)
    }
}

// Helper macro for implementing `ManifestLoader` for anything that implements `AsRef<Path>` (which
// results in a compiler error if we do it generically)
macro_rules! impl_manifest_loader_for_path {
    ($($ty:ty),*) => {
        $(
            impl ManifestLoader for $ty {
                async fn load_manifest(self) -> Result<Manifest> {
                    let raw = tokio::fs::read(self).await.map_err(|e| ClientError::ManifestLoad(e.into()))?;
                    parse_yaml_or_json(raw).map_err(Into::into)
                }
            }
        )*
    };
}

impl_manifest_loader_for_path!(&Path, &str, &String, String, PathBuf, &PathBuf);

/// A simple function that attempts to parse the given bytes as YAML or JSON. This is used in the
/// implementations of `ManifestLoader`
pub fn parse_yaml_or_json(
    raw: impl AsRef<[u8]>,
) -> std::result::Result<Manifest, crate::error::SerializationError> {
    // Attempt to parse as YAML first, then JSON
    serde_yaml::from_slice(raw.as_ref())
        .or_else(|_| serde_json::from_slice(raw.as_ref()))
        .map_err(Into::into)
}
