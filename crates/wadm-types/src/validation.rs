//! Logic for model ([`Manifest`]) validation
//!

use std::collections::HashMap;
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context as _, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::{LinkProperty, Manifest, TraitProperty, LATEST_VERSION};

/// A namespace -> package -> interface lookup
type KnownInterfaceLookup = HashMap<String, HashMap<String, HashMap<String, ()>>>;

/// Hard-coded list of known namespaces/packages and the interfaces they contain.
///
/// Using an interface that is *not* on this list is not an error --
/// custom interfaces are expected to not be on this list, but when using
/// a known namespace and package, interfaces should generally be well known.
static KNOWN_INTERFACE_LOOKUP: OnceLock<KnownInterfaceLookup> = OnceLock::new();

/// Get the static list of known interfaces
fn get_known_interface_lookup() -> &'static KnownInterfaceLookup {
    KNOWN_INTERFACE_LOOKUP.get_or_init(|| {
        HashMap::from([
            (
                "wrpc".into(),
                HashMap::from([
                    (
                        "blobstore".into(),
                        HashMap::from([("blobstore".into(), ())]),
                    ),
                    (
                        "keyvalue".into(),
                        HashMap::from([("atomics".into(), ()), ("store".into(), ())]),
                    ),
                    (
                        "http".into(),
                        HashMap::from([
                            ("incoming-handler".into(), ()),
                            ("outgoing-handler".into(), ()),
                        ]),
                    ),
                ]),
            ),
            (
                "wasi".into(),
                HashMap::from([
                    (
                        "blobstore".into(),
                        HashMap::from([("blobstore".into(), ())]),
                    ),
                    ("config".into(), HashMap::from([("runtime".into(), ())])),
                    (
                        "keyvalue".into(),
                        HashMap::from([("atomics".into(), ()), ("store".into(), ())]),
                    ),
                    (
                        "http".into(),
                        HashMap::from([
                            ("incoming-handler".into(), ()),
                            ("outgoing-handler".into(), ()),
                        ]),
                    ),
                    ("logging".into(), HashMap::from([("logging".into(), ())])),
                ]),
            ),
            (
                "wasmcloud".into(),
                HashMap::from([(
                    "messaging".into(),
                    HashMap::from([("consumer".into(), ()), ("handler".into(), ())]),
                )]),
            ),
        ])
    })
}

static MANIFEST_NAME_REGEX_STR: &str = r"^[-\w]+$";
static MANIFEST_NAME_REGEX: OnceLock<Regex> = OnceLock::new();

/// Retrieve regular expression which manifest names must match, compiled to a usable [`Regex`]
fn get_manifest_name_regex() -> &'static Regex {
    MANIFEST_NAME_REGEX.get_or_init(|| {
        Regex::new(MANIFEST_NAME_REGEX_STR)
            .context("failed to parse manifest name regex")
            .unwrap()
    })
}

/// Check whether a manifest name matches requirements, returning all validation errors
pub fn validate_manifest_name(name: &str) -> impl ValidationOutput {
    let mut errors = Vec::new();
    if !get_manifest_name_regex().is_match(name) {
        errors.push(ValidationFailure::new(
            ValidationFailureLevel::Error,
            format!("manifest name [{name}] is not allowed (should match regex [{MANIFEST_NAME_REGEX_STR}])"),
        ))
    }
    errors
}

/// Check whether a manifest name matches requirements
pub fn is_valid_manifest_name(name: &str) -> bool {
    validate_manifest_name(name).valid()
}

/// Check whether a manifest version is valid, returning all validation errors
pub fn validate_manifest_version(version: &str) -> impl ValidationOutput {
    let mut errors = Vec::new();
    if version == LATEST_VERSION {
        errors.push(ValidationFailure::new(
            ValidationFailureLevel::Error,
            format!("{LATEST_VERSION} is not allowed in wadm"),
        ))
    }
    errors
}

/// Check whether a manifest version is valid requirements
pub fn is_valid_manifest_version(version: &str) -> bool {
    validate_manifest_version(version).valid()
}

/// Check whether a known grouping of namespace, package and interface are valid.
/// A grouping must be both known/expected and invalid to fail this test (ex. a typo).
///
/// NOTE: what is considered a valid interface known to the host depends explicitly on
/// the wasmCloud host and wasmCloud project goals/implementation. This information is
/// subject to change.
fn is_invalid_known_interface(
    namespace: &str,
    package: &str,
    interface: &str,
) -> Vec<ValidationFailure> {
    let known_interfaces = get_known_interface_lookup();
    let Some(pkg_lookup) = known_interfaces.get(namespace) else {
        // This namespace isn't known, so it may be a custom interface
        return vec![];
    };
    let Some(iface_lookup) = pkg_lookup.get(package) else {
        // Unknown package inside a known interface we control is probably a bug
        return vec![ValidationFailure::new(
            ValidationFailureLevel::Warning,
            format!("unrecognized interface [{namespace}:{package}/{interface}]"),
        )];
    };
    // Unknown interface inside known namespace and package is probably a bug
    if !iface_lookup.contains_key(interface) {
        // Unknown package inside a known interface we control is probably a bug
        return vec![ValidationFailure::new(
            ValidationFailureLevel::Error,
            format!("unrecognized interface [{namespace}:{package}/{interface}]"),
        )];
    }

    Vec::new()
}

/// Level of a failure related to validation
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ValidationFailureLevel {
    #[default]
    Warning,
    Error,
}

impl core::fmt::Display for ValidationFailureLevel {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Warning => "warning",
                Self::Error => "error",
            }
        )
    }
}

/// Failure detailing a validation failure, normally indicating a failure
#[derive(Debug, Default, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct ValidationFailure {
    pub level: ValidationFailureLevel,
    pub msg: String,
}

impl ValidationFailure {
    fn new(level: ValidationFailureLevel, msg: String) -> Self {
        ValidationFailure { level, msg }
    }
}

impl core::fmt::Display for ValidationFailure {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "[{}] {}", self.level, self.msg)
    }
}

/// Things that support output validation
pub trait ValidationOutput {
    /// Whether the object is valid
    fn valid(&self) -> bool;
    /// Warnings returned (if any) during validation
    fn warnings(&self) -> Vec<&ValidationFailure>;
    /// The errors returned by the validation
    fn errors(&self) -> Vec<&ValidationFailure>;
}

/// Default implementation for a list of concrete [`ValidationFailure`]s
impl ValidationOutput for [ValidationFailure] {
    fn valid(&self) -> bool {
        self.errors().is_empty()
    }
    fn warnings(&self) -> Vec<&ValidationFailure> {
        self.iter()
            .filter(|m| m.level == ValidationFailureLevel::Warning)
            .collect()
    }
    fn errors(&self) -> Vec<&ValidationFailure> {
        self.iter()
            .filter(|m| m.level == ValidationFailureLevel::Error)
            .collect()
    }
}

/// Default implementation for a list of concrete [`ValidationFailure`]s
impl ValidationOutput for Vec<ValidationFailure> {
    fn valid(&self) -> bool {
        self.as_slice().valid()
    }
    fn warnings(&self) -> Vec<&ValidationFailure> {
        self.iter()
            .filter(|m| m.level == ValidationFailureLevel::Warning)
            .collect()
    }
    fn errors(&self) -> Vec<&ValidationFailure> {
        self.iter()
            .filter(|m| m.level == ValidationFailureLevel::Error)
            .collect()
    }
}

/// Validate a WADM application manifest, returning a list of validation failures
///
/// At present this can check for:
/// - unsupported interfaces (i.e. typos, etc)
/// - unknown packages under known namespaces
/// - "dangling" links (missing components)
///
/// Since `[ValidationFailure]` implements `ValidationOutput`, you can call `valid()` and other
/// trait methods on it:
///
/// ```rust,ignore
/// let messages = validate_manifest(some_path).await?;
/// let valid = messages.valid();
/// ```
///
/// # Arguments
///
/// * `path` - Path to the Manifest that will be read into memory and validated
pub async fn validate_manifest_file(
    path: impl AsRef<Path>,
) -> Result<(Manifest, Vec<ValidationFailure>)> {
    let content = tokio::fs::read_to_string(path.as_ref())
        .await
        .with_context(|| format!("failed to read manifest @ [{}]", path.as_ref().display()))?;

    validate_manifest_bytes(&content).await.with_context(|| {
        format!(
            "failed to parse YAML manifest [{}]",
            path.as_ref().display()
        )
    })
}

/// Validate a lsit of bytes that represents a  WADM application manifest
///
/// # Arguments
///
/// * `content` - YAML content  to the Manifest that will be read into memory and validated
pub async fn validate_manifest_bytes(
    content: impl AsRef<[u8]>,
) -> Result<(Manifest, Vec<ValidationFailure>)> {
    let manifest =
        serde_yaml::from_slice(content.as_ref()).context("failed to parse manifest content")?;
    let failures = validate_manifest(&manifest).await?;
    Ok((manifest, failures))
}

/// Validate a WADM application manifest, returning a list of validation failures
///
/// At present this can check for:
/// - unsupported interfaces (i.e. typos, etc)
/// - unknown packages under known namespaces
/// - "dangling" links (missing components)
///
/// Since `[ValidationFailure]` implements `ValidationOutput`, you can call `valid()` and other
/// trait methods on it:
///
/// ```rust,ignore
/// let messages = validate_manifest(some_path).await?;
/// let valid = messages.valid();
/// ```
///
/// # Arguments
///
/// * `manifest` - The [`Manifest`] that should be validated
pub async fn validate_manifest(manifest: &Manifest) -> Result<Vec<ValidationFailure>> {
    // Check for known failures with the manifest
    let mut failures = Vec::new();
    failures.extend(
        validate_manifest_name(&manifest.metadata.name)
            .errors()
            .into_iter()
            .cloned(),
    );
    failures.extend(
        validate_manifest_version(manifest.version())
            .errors()
            .into_iter()
            .cloned(),
    );
    failures.extend(check_misnamed_interfaces(manifest));
    failures.extend(check_dangling_links(manifest));
    Ok(failures)
}

/// Check for misnamed host-supported interfaces in the manifest
fn check_misnamed_interfaces(manifest: &Manifest) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();
    for link_trait in manifest.links() {
        if let TraitProperty::Link(LinkProperty {
            namespace,
            package,
            interfaces,
            ..
        }) = &link_trait.properties
        {
            for interface in interfaces {
                failures.extend(is_invalid_known_interface(namespace, package, interface))
            }
        }
    }

    failures
}

/// Check for "dangling" links, which contain targets that are not specified elsewhere in the
/// WADM manifest.
///
/// A problem of this type only constitutes a warning, because it is possible that the manifest
/// does not *completely* specify targets (they may be deployed/managed external to WADM or in a separte
/// manifest).
fn check_dangling_links(manifest: &Manifest) -> Vec<ValidationFailure> {
    let lookup = manifest.component_lookup();
    let mut failures = Vec::new();
    for link_trait in manifest.links() {
        match &link_trait.properties {
            TraitProperty::Custom(obj) => {
                // Ensure target property it present
                match obj["target"].as_str() {
                    // If target is present, ensure it's pointing to a known component
                    Some(target) if !lookup.contains_key(&String::from(target)) => {
                        failures.push(ValidationFailure::new(
                            ValidationFailureLevel::Warning,
                            format!("custom link target [{target}] is not a listed component"),
                        ))
                    }
                    // For all keys where the the component is in the lookup we can do nothing
                    Some(_) => {}
                    // if target property is not present, note that it is missing
                    None => failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Error,
                        "custom link is missing 'target' property".into(),
                    )),
                }
            }

            TraitProperty::Link(LinkProperty { name, target, .. }) => {
                let link_identifier = name
                    .as_ref()
                    .map(|n| format!("(name [{n}])"))
                    .unwrap_or_else(|| format!("(target [{target}])"));
                if !lookup.contains_key(target) {
                    failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Warning,
                        format!(
                            "link {link_identifier} target [{target}] is not a listed component"
                        ),
                    ))
                }
            }

            _ => unreachable!("manifest.links() should only return links"),
        }
    }

    failures
}

#[cfg(test)]
mod tests {
    use super::is_valid_manifest_name;

    const VALID_MANIFEST_NAMES: [&str; 4] = [
        "mymanifest",
        "my-manifest",
        "my_manifest",
        "mymanifest-v2-v3-final",
    ];

    const INVALID_MANIFEST_NAMES: [&str; 2] = ["my.manifest", "my manifest"];

    /// Ensure valid manifest names pass
    #[test]
    fn manifest_names_valid() {
        // Acceptable manifest names
        for valid in VALID_MANIFEST_NAMES {
            assert!(is_valid_manifest_name(valid));
        }
    }

    /// Ensure invalid manifest names fail
    #[test]
    fn manifest_names_invalid() {
        for invalid in INVALID_MANIFEST_NAMES {
            assert!(!is_valid_manifest_name(invalid))
        }
    }
}
