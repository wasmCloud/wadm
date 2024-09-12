//! Logic for model ([`Manifest`]) validation
//!

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::OnceLock;

use anyhow::{Context as _, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::{
    CapabilityProperties, ComponentProperties, LinkProperty, Manifest, Properties, Trait,
    TraitProperty, LATEST_VERSION,
};

/// A namespace -> package -> interface lookup
type KnownInterfaceLookup = HashMap<String, HashMap<String, HashMap<String, ()>>>;

/// Hard-coded list of known namespaces/packages and the interfaces they contain.
///
/// Using an interface that is *not* on this list is not an error --
/// custom interfaces are expected to not be on this list, but when using
/// a known namespace and package, interfaces should generally be well known.
static KNOWN_INTERFACE_LOOKUP: OnceLock<KnownInterfaceLookup> = OnceLock::new();

const SECRET_POLICY_TYPE: &str = "policy.secret.wasmcloud.dev/v1alpha1";

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
                        HashMap::from([
                            ("atomics".into(), ()),
                            ("store".into(), ()),
                            ("batch".into(), ()),
                            ("watch".into(), ()),
                        ]),
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
/// - secrets mapped to unknown policies
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
    failures.extend(core_validation(manifest));
    failures.extend(check_misnamed_interfaces(manifest));
    failures.extend(check_dangling_links(manifest));
    failures.extend(validate_policies(manifest));
    failures.extend(ensure_no_custom_traits(manifest));
    Ok(failures)
}

fn core_validation(manifest: &Manifest) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();
    let mut name_registry: HashSet<String> = HashSet::new();
    let mut id_registry: HashSet<String> = HashSet::new();
    let mut required_capability_components: HashSet<String> = HashSet::new();

    for label in manifest.metadata.labels.iter() {
        if !valid_oam_label(label) {
            failures.push(ValidationFailure::new(
                ValidationFailureLevel::Error,
                format!("Invalid OAM label: {:?}", label),
            ));
        }
    }

    for annotation in manifest.metadata.annotations.iter() {
        if !valid_oam_label(annotation) {
            failures.push(ValidationFailure::new(
                ValidationFailureLevel::Error,
                format!("Invalid OAM annotation: {:?}", annotation),
            ));
        }
    }

    for component in manifest.spec.components.iter() {
        // Component name validation : each component (components or providers) should have a unique name
        if !name_registry.insert(component.name.clone()) {
            failures.push(ValidationFailure::new(
                ValidationFailureLevel::Error,
                format!("Duplicate component name in manifest: {}", component.name),
            ));
        }
        // Provider validation :
        // Provider config should be serializable [For all components that have JSON config, validate that it can serialize.
        // We need this so it doesn't trigger an error when sending a command down the line]
        // Providers should have a unique image ref and link name
        if let Properties::Capability {
            properties:
                CapabilityProperties {
                    id: Some(component_id),
                    config: _capability_config,
                    ..
                },
        } = &component.properties
        {
            if !id_registry.insert(component_id.to_string()) {
                failures.push(ValidationFailure::new(
                    ValidationFailureLevel::Error,
                    format!(
                        "Duplicate component identifier in manifest: {}",
                        component_id
                    ),
                ));
            }
        }

        // Component validation : Components should have a unique identifier per manifest
        if let Properties::Component {
            properties: ComponentProperties { id: Some(id), .. },
        } = &component.properties
        {
            if !id_registry.insert(id.to_string()) {
                failures.push(ValidationFailure::new(
                    ValidationFailureLevel::Error,
                    format!("Duplicate component identifier in manifest: {}", id),
                ));
            }
        }

        // Linkdef validation : A linkdef from a component should have a unique target and reference
        if let Some(traits_vec) = &component.traits {
            for trait_item in traits_vec.iter() {
                if let Trait {
                    // TODO : add trait type validation after custom types are done. See TraitProperty enum.
                    properties: TraitProperty::Link(LinkProperty { target, .. }),
                    ..
                } = &trait_item
                {
                    // Multiple components{ with type != 'capability'} can declare the same target, so we don't need to check for duplicates on insert
                    required_capability_components.insert(target.name.to_string());
                }
            }
        }
    }

    let missing_capability_components = required_capability_components
        .difference(&name_registry)
        .collect::<Vec<&String>>();

    if !missing_capability_components.is_empty() {
        failures.push(ValidationFailure::new(
            ValidationFailureLevel::Error,
            format!(
                "The following capability component(s) are missing from the manifest: {:?}",
                missing_capability_components
            ),
        ));
    };
    failures
}

/// Check for misnamed host-supported interfaces in the manifest
fn check_misnamed_interfaces(manifest: &Manifest) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();
    for link_trait in manifest.links() {
        if let TraitProperty::Link(LinkProperty {
            namespace,
            package,
            interfaces,
            target: _target,
            source: _source,
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

/// This validation rule should eventually be removed, but at this time (as of wadm 0.14.0)
/// custom traits are not supported. We technically deserialize the custom trait, but 99%
/// of the time this is just a poorly formatted spread or link scaler which is incredibly
/// frustrating to debug.
fn ensure_no_custom_traits(manifest: &Manifest) -> Vec<ValidationFailure> {
    let mut failures = Vec::new();
    for component in manifest.components() {
        if let Some(traits) = &component.traits {
            for trait_item in traits {
                match &trait_item.properties {
                    TraitProperty::Custom(trt) if trait_item.is_link() => failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Error,
                        format!("Link trait deserialized as custom trait, ensure fields are correct: {}", trt),
                    )),
                    TraitProperty::Custom(trt) if trait_item.is_scaler() => failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Error,
                        format!("Scaler trait deserialized as custom trait, ensure fields are correct: {}", trt),
                    )),
                    _ => (),
                }
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
                if obj.get("target").is_none() {
                    failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Error,
                        "custom link is missing 'target' property".into(),
                    ));
                    continue;
                }

                // Ensure target property is present
                match obj["target"]["name"].as_str() {
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
                        "custom link is missing 'target' name property".into(),
                    )),
                }
            }
            TraitProperty::Link(LinkProperty { name, target, .. }) => {
                let link_identifier = name
                    .as_ref()
                    .map(|n| format!("(name [{n}])"))
                    .unwrap_or_else(|| format!("(target [{}])", target.name));
                if !lookup.contains_key(&target.name) {
                    failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Warning,
                        format!(
                            "link {link_identifier} target [{}] is not a listed component",
                            target.name
                        ),
                    ))
                }
            }

            _ => unreachable!("manifest.links() should only return links"),
        }
    }

    failures
}

/// Ensure that a manifest has secrets that are mapped to known policies
/// and that those policies have the expected type and properties.
fn validate_policies(manifest: &Manifest) -> Vec<ValidationFailure> {
    let policies = manifest.policy_lookup();
    let mut failures = Vec::new();
    for c in manifest.components() {
        // Ensure policies meant for secrets are valid
        for secret in c.secrets() {
            match policies.get(&secret.properties.policy) {
                Some(policy) if policy.policy_type != SECRET_POLICY_TYPE => {
                    failures.push(ValidationFailure::new(
                        ValidationFailureLevel::Error,
                        format!(
                            "secret '{}' is mapped to policy '{}' which is not a secret policy. Expected type '{SECRET_POLICY_TYPE}'",
                            secret.name, secret.properties.policy
                        ),
                    ))
                }
                Some(policy) => {
                    if !policy.properties.contains_key("backend") {
                        failures.push(ValidationFailure::new(
                            ValidationFailureLevel::Error,
                            format!(
                                "secret '{}' is mapped to policy '{}' which does not include a 'backend' property",
                                secret.name, secret.properties.policy
                            ),
                        ))
                    }
                }
                None => failures.push(ValidationFailure::new(
                    ValidationFailureLevel::Error,
                    format!(
                        "secret '{}' is mapped to unknown policy '{}'",
                        secret.name, secret.properties.policy
                    ),
                )),
            }
        }
    }
    failures
}

/// This function validates that a key/value pair is a valid OAM label. It's using fairly
/// basic validation rules to ensure that the manifest isn't doing anything horribly wrong. Keeping
/// this function free of regex is intentional to keep this code functional but simple.
///
/// See <https://github.com/oam-dev/spec/blob/master/metadata.md#metadata> for details
pub fn valid_oam_label(label: (&String, &String)) -> bool {
    let (key, _) = label;
    match key.split_once('/') {
        Some((prefix, name)) => is_valid_dns_subdomain(prefix) && is_valid_label_name(name),
        None => is_valid_label_name(key),
    }
}

pub fn is_valid_dns_subdomain(s: &str) -> bool {
    if s.is_empty() || s.len() > 253 {
        return false;
    }

    s.split('.').all(|part| {
        // Ensure each part is non-empty, <= 63 characters, starts with an alphabetic character,
        // ends with an alphanumeric character, and contains only alphanumeric characters or hyphens
        !part.is_empty()
            && part.len() <= 63
            && part.starts_with(|c: char| c.is_ascii_alphabetic())
            && part.ends_with(|c: char| c.is_ascii_alphanumeric())
            && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
    })
}

// Ensure each name is non-empty, <= 63 characters, starts with an alphanumeric character,
// ends with an alphanumeric character, and contains only alphanumeric characters, hyphens,
// underscores, or periods
pub fn is_valid_label_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 63 {
        return false;
    }

    name.starts_with(|c: char| c.is_ascii_alphanumeric())
        && name.ends_with(|c: char| c.is_ascii_alphanumeric())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
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
