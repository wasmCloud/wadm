//! Contains code for converting the list of [`Component`]s in an application into a list of [`Scaler`]s
//! that are responsible for monitoring and enforcing the desired state of a lattice

use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use tracing::{error, warn};
use wadm_types::{
    api::StatusInfo, CapabilityProperties, Component, ComponentProperties, ConfigProperty,
    LinkProperty, Policy, Properties, SecretProperty, SharedApplicationComponentProperties,
    SpreadScalerProperty, Trait, TraitProperty, DAEMONSCALER_TRAIT, LINK_TRAIT, SPREADSCALER_TRAIT,
};
use wasmcloud_secrets_types::SECRET_PREFIX;

use crate::{
    publisher::Publisher,
    scaler::{
        spreadscaler::{link::LINK_SCALER_KIND, ComponentSpreadScaler, SPREAD_SCALER_KIND},
        statusscaler::StatusScaler,
        Scaler,
    },
    storage::{snapshot::SnapshotStore, ReadStore},
    workers::{ConfigSource, LinkSource, SecretSource},
    DEFAULT_LINK_NAME,
};

use super::{
    configscaler::ConfigScaler,
    daemonscaler::{provider::ProviderDaemonScaler, ComponentDaemonScaler},
    secretscaler::SecretScaler,
    spreadscaler::{
        link::{LinkScaler, LinkScalerConfig},
        provider::{ProviderSpreadConfig, ProviderSpreadScaler},
    },
    BackoffWrapper,
};

pub(crate) type BoxedScaler = Box<dyn Scaler + Send + Sync + 'static>;
pub(crate) type ScalerList = Vec<BoxedScaler>;

const EMPTY_TRAIT_VEC: Vec<Trait> = Vec::new();

/// Converts a list of manifest [`Component`]s into a [`ScalerList`], resolving shared application
/// references, links, configuration and secrets as necessary.
///
/// # Arguments
/// * `components` - The list of components to convert
/// * `policies` - The policies to use when creating the scalers so they can access secrets
/// * `lattice_id` - The lattice id the scalers operate on
/// * `notifier` - The publisher to use when creating the scalers so they can report status
/// * `name` - The name of the manifest that the scalers are being created for
/// * `notifier_subject` - The subject to use when creating the scalers so they can report status
/// * `snapshot_data` - The store to use when creating the scalers so they can access lattice state
pub(crate) fn manifest_components_to_scalers<S, P, L>(
    components: &[Component],
    policies: &HashMap<&String, &Policy>,
    lattice_id: &str,
    manifest_name: &str,
    notifier_subject: &str,
    notifier: &P,
    snapshot_data: &SnapshotStore<S, L>,
) -> ScalerList
where
    S: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
    L: LinkSource + ConfigSource + SecretSource + Clone + Send + Sync + 'static,
{
    let mut scalers: ScalerList = Vec::new();
    components
        .iter()
        .for_each(|component| match &component.properties {
            Properties::Component { properties } => {
                // Determine if this component is contained in this manifest or a shared application
                let (application_name, component_name) = match resolve_manifest_component(
                    manifest_name,
                    &component.name,
                    properties.image.as_ref(),
                    properties.application.as_ref(),
                ) {
                    Ok(names) => names,
                    Err(err) => {
                        error!(err);
                        scalers.push(Box::new(StatusScaler::new(
                            uuid::Uuid::new_v4().to_string(),
                            SPREAD_SCALER_KIND,
                            &component.name,
                            StatusInfo::failed(err),
                        )) as BoxedScaler);
                        return;
                    }
                };

                component_scalers(
                    &mut scalers,
                    components,
                    properties,
                    component.traits.as_ref(),
                    manifest_name,
                    application_name,
                    component_name,
                    lattice_id,
                    policies,
                    notifier_subject,
                    notifier,
                    snapshot_data,
                )
            }
            Properties::Capability { properties } => {
                // Determine if this component is contained in this manifest or a shared application
                let (application_name, component_name) = match resolve_manifest_component(
                    manifest_name,
                    &component.name,
                    properties.image.as_ref(),
                    properties.application.as_ref(),
                ) {
                    Ok(names) => names,
                    Err(err) => {
                        error!(err);
                        scalers.push(Box::new(StatusScaler::new(
                            uuid::Uuid::new_v4().to_string(),
                            SPREAD_SCALER_KIND,
                            &component.name,
                            StatusInfo::failed(err),
                        )) as BoxedScaler);
                        return;
                    }
                };
                provider_scalers(
                    &mut scalers,
                    components,
                    properties,
                    component.traits.as_ref(),
                    manifest_name,
                    application_name,
                    component_name,
                    lattice_id,
                    policies,
                    notifier_subject,
                    notifier,
                    snapshot_data,
                )
            }
        });
    scalers
}

/// Helper function, primarily to remove nesting, that extends a [`ScalerList`] with all scalers
/// from a (Wasm) component [`Component`]
///
/// # Arguments
/// * `scalers` - The list of scalers to extend
/// * `components` - The list of components to convert
/// * `properties` - The properties of the component to convert
/// * `traits` - The traits of the component to convert
/// * `manifest_name` - The name of the manifest that the scalers are being created for
/// * `application_name` - The name of the application that the scalers are being created for
/// * `component_name` - The name of the component to convert
/// * **The following arguments are required to create scalers, passed directly through to the scaler
/// * `lattice_id` - The lattice id the scalers operate on
/// * `policies` - The policies to use when creating the scalers so they can access secrets
/// * `notifier_subject` - The subject to use when creating the scalers so they can report status
/// * `notifier` - The publisher to use when creating the scalers so they can report status
/// * `snapshot_data` - The store to use when creating the scalers so they can access lattice state
#[allow(clippy::too_many_arguments)]
fn component_scalers<S, P, L>(
    scalers: &mut ScalerList,
    components: &[Component],
    properties: &ComponentProperties,
    traits: Option<&Vec<Trait>>,
    manifest_name: &str,
    application_name: &str,
    component_name: &str,
    lattice_id: &str,
    policies: &HashMap<&String, &Policy>,
    notifier_subject: &str,
    notifier: &P,
    snapshot_data: &SnapshotStore<S, L>,
) where
    S: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
    L: LinkSource + ConfigSource + SecretSource + Clone + Send + Sync + 'static,
{
    scalers.extend(traits.unwrap_or(&EMPTY_TRAIT_VEC).iter().filter_map(|trt| {
        // If an image is specified, then it's a component in the same manifest. Otherwise, it's a shared component
        let component_id = if properties.image.is_some() {
            compute_component_id(manifest_name, properties.id.as_ref(), component_name)
        } else {
            compute_component_id(application_name, properties.id.as_ref(), component_name)
        };
        let (config_scalers, mut config_names) =
            config_to_scalers(snapshot_data, manifest_name, &properties.config);
        let (secret_scalers, secret_names) = secrets_to_scalers(
            snapshot_data,
            manifest_name,
            &properties.secrets,
            policies,
        );

        config_names.append(&mut secret_names.clone());
        // TODO(#451): Consider a way to report on status of a shared component
        match (trt.trait_type.as_str(), &trt.properties, &properties.image) {
            // Shared application components already have their own spread/daemon scalers, you
            // cannot modify them from another manifest
            (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(_), None) => {
                warn!(
                    "Unsupported SpreadScaler trait specified for a shared component {component_name}"
                );
                None
            }
            (DAEMONSCALER_TRAIT, TraitProperty::SpreadScaler(_), None) => {
                warn!(
                    "Unsupported DaemonScaler trait specified for a shared component {component_name}"
                );
                None
            }
            (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(p), Some(image_ref)) => {
                // If the image is not specified, then it's a reference to a shared provider
                // in a different manifest
                Some(Box::new(BackoffWrapper::new(
                    ComponentSpreadScaler::new(
                        snapshot_data.clone(),
                        image_ref.clone(),
                        component_id,
                        lattice_id.to_owned(),
                        application_name.to_owned(),
                        p.to_owned(),
                        component_name,
                        config_names,
                    ),
                    notifier.clone(),
                    config_scalers,
                    secret_scalers,
                    notifier_subject,
                    application_name,
                    Some(Duration::from_secs(5)),
                )) as BoxedScaler)
            }
            (DAEMONSCALER_TRAIT, TraitProperty::SpreadScaler(p), Some(image_ref)) => {
                Some(Box::new(BackoffWrapper::new(
                    ComponentDaemonScaler::new(
                        snapshot_data.clone(),
                        image_ref.to_owned(),
                        component_id,
                        lattice_id.to_owned(),
                        application_name.to_owned(),
                        p.to_owned(),
                        component_name,
                        config_names,
                    ),
                    notifier.clone(),
                    config_scalers,
                    secret_scalers,
                    notifier_subject,
                    application_name,
                    Some(Duration::from_secs(5)),
                )) as BoxedScaler)
            }
            (LINK_TRAIT, TraitProperty::Link(p), _) => {
                // Find the target component of the link and create a scaler for it
                components
                    .iter()
                    .find_map(|component| match &component.properties {
                        Properties::Capability {
                            properties:
                                CapabilityProperties {
                                    id,
                                    application,
                                    image,
                                    ..
                                },
                        }
                        | Properties::Component {
                            properties:
                                ComponentProperties {
                                    id,
                                    application,
                                    image,
                                    ..
                                },
                        } if component.name == p.target.name => Some(link_scaler(
                            p,
                            lattice_id,
                            manifest_name,
                            application_name,
                            &component.name,
                            component_id.to_string(),
                            id.as_ref(),
                            image.as_ref(),
                            application.as_ref(),
                            policies,
                            notifier_subject,
                            notifier,
                            snapshot_data,
                        )),
                        _ => None,
                    })
            }
            _ => None,
        }
    }));
}

/// Helper function, primarily to remove nesting, that extends a [`ScalerList`] with all scalers
/// from a capability provider [`Component`]
/// /// # Arguments
/// * `scalers` - The list of scalers to extend
/// * `components` - The list of components to convert
/// * `properties` - The properties of the capability provider to convert
/// * `traits` - The traits of the component to convert
/// * `manifest_name` - The name of the manifest that the scalers are being created for
/// * `application_name` - The name of the application that the scalers are being created for
/// * `component_name` - The name of the component to convert
/// * **The following arguments are required to create scalers, passed directly through to the scaler
/// * `lattice_id` - The lattice id the scalers operate on
/// * `policies` - The policies to use when creating the scalers so they can access secrets
/// * `notifier_subject` - The subject to use when creating the scalers so they can report status
/// * `notifier` - The publisher to use when creating the scalers so they can report status
/// * `snapshot_data` - The store to use when creating the scalers so they can access lattice state
#[allow(clippy::too_many_arguments)]
fn provider_scalers<S, P, L>(
    scalers: &mut ScalerList,
    components: &[Component],
    properties: &CapabilityProperties,
    traits: Option<&Vec<Trait>>,
    manifest_name: &str,
    application_name: &str,
    component_name: &str,
    lattice_id: &str,
    policies: &HashMap<&String, &Policy>,
    notifier_subject: &str,
    notifier: &P,
    snapshot_data: &SnapshotStore<S, L>,
) where
    S: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
    L: LinkSource + ConfigSource + SecretSource + Clone + Send + Sync + 'static,
{
    // If an image is specified, then it's a provider in the same manifest. Otherwise, it's a shared component
    let provider_id = if properties.image.is_some() {
        compute_component_id(manifest_name, properties.id.as_ref(), component_name)
    } else {
        compute_component_id(application_name, properties.id.as_ref(), component_name)
    };

    let mut scaler_specified = false;
    scalers.extend(traits.unwrap_or(&EMPTY_TRAIT_VEC).iter().filter_map(|trt| {
        match (trt.trait_type.as_str(), &trt.properties, &properties.image) {
            // Shared application components already have their own spread/daemon scalers, you
            // cannot modify them from another manifest
            (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(_), None) => {
                warn!(
                    "Unsupported SpreadScaler trait specified for a shared provider {component_name}"
                );
                None
            }
            (DAEMONSCALER_TRAIT, TraitProperty::SpreadScaler(_), None) => {
                warn!(
                    "Unsupported DaemonScaler trait specified for a shared provider {component_name}"
                );
                None
            }
            (SPREADSCALER_TRAIT, TraitProperty::SpreadScaler(p), Some(image)) => {
                scaler_specified = true;
                let (config_scalers, mut config_names) =
                    config_to_scalers(snapshot_data, application_name, &properties.config);
                let (secret_scalers, secret_names) = secrets_to_scalers(
                    snapshot_data,
                    application_name,
                    &properties.secrets,
                    policies,
                );
                config_names.append(&mut secret_names.clone());

                Some(Box::new(BackoffWrapper::new(
                    ProviderSpreadScaler::new(
                        snapshot_data.clone(),
                        ProviderSpreadConfig {
                            lattice_id: lattice_id.to_owned(),
                            provider_id: provider_id.to_owned(),
                            provider_reference: image.to_owned(),
                            spread_config: p.to_owned(),
                            model_name: application_name.to_owned(),
                            provider_config: config_names,
                        },
                        component_name,
                    ),
                    notifier.clone(),
                    config_scalers,
                    secret_scalers,
                    notifier_subject,
                    application_name,
                    // Providers are a bit longer because it can take a bit to download
                    Some(Duration::from_secs(60)),
                )) as BoxedScaler)
            }
            (DAEMONSCALER_TRAIT, TraitProperty::SpreadScaler(p), Some(image)) => {
                scaler_specified = true;
                let (config_scalers, mut config_names) =
                    config_to_scalers(snapshot_data, application_name, &properties.config);
                let (secret_scalers, secret_names) = secrets_to_scalers(
                    snapshot_data,
                    application_name,
                    &properties.secrets,
                    policies,
                );
                config_names.append(&mut secret_names.clone());
                Some(Box::new(BackoffWrapper::new(
                        ProviderDaemonScaler::new(
                            snapshot_data.clone(),
                            ProviderSpreadConfig {
                                lattice_id: lattice_id.to_owned(),
                                provider_id: provider_id.to_owned(),
                                provider_reference: image.to_owned(),
                                spread_config: p.to_owned(),
                                model_name: application_name.to_owned(),
                                provider_config: config_names,
                            },
                            component_name,
                        ),
                        notifier.clone(),
                        config_scalers,
                        secret_scalers,
                        notifier_subject,
                        application_name,
                        // Providers are a bit longer because it can take a bit to download
                        Some(Duration::from_secs(60)),
                    )) as BoxedScaler)
            }
            // Find the target component of the link and create a scaler for it.
            (LINK_TRAIT, TraitProperty::Link(p), _) => {
                components
                    .iter()
                    .find_map(|component| match &component.properties {
                        // Providers cannot link to other providers, only components
                        Properties::Capability { .. } if component.name == p.target.name => {
                            error!(
                                "Provider {} cannot link to provider {}, only components",
                                &component.name, p.target.name
                            );
                            None
                        }
                        Properties::Component {
                            properties:
                                ComponentProperties {
                                    image,
                                    application,
                                    id,
                                    ..
                                },
                        } if component.name == p.target.name => Some(link_scaler(
                            p,
                            lattice_id,
                            manifest_name,
                            application_name,
                            &component.name,
                            provider_id.to_owned(),
                            id.as_ref(),
                            image.as_ref(),
                            application.as_ref(),
                            policies,
                            notifier_subject,
                            notifier,
                            snapshot_data,
                        )),
                        _ => None,
                    })
            }
            _ => None,
        }
    }));
    // Allow providers to omit the spreadscaler entirely for simplicity
    if !scaler_specified {
        if let Some(image) = &properties.image {
            let (config_scalers, mut config_names) =
                config_to_scalers(snapshot_data, application_name, &properties.config);

            let (secret_scalers, mut secret_names) = secrets_to_scalers(
                snapshot_data,
                application_name,
                &properties.secrets,
                policies,
            );
            config_names.append(&mut secret_names);
            scalers.push(Box::new(BackoffWrapper::new(
                ProviderSpreadScaler::new(
                    snapshot_data.clone(),
                    ProviderSpreadConfig {
                        lattice_id: lattice_id.to_owned(),
                        provider_id,
                        provider_reference: image.to_owned(),
                        spread_config: SpreadScalerProperty {
                            instances: 1,
                            spread: vec![],
                        },
                        model_name: application_name.to_owned(),
                        provider_config: config_names,
                    },
                    component_name,
                ),
                notifier.clone(),
                config_scalers,
                secret_scalers,
                notifier_subject,
                application_name,
                // Providers are a bit longer because it can take a bit to download
                Some(Duration::from_secs(60)),
            )) as BoxedScaler)
        }
    }
}

/// Resolves configuration, secrets, and the target of a link to create a boxed [`LinkScaler`]
///
/// # Arguments
/// * `link_property` - The properties of the link to convert
/// * `lattice_id` - The lattice id the scalers operate on
/// * `manifest_name` - The name of the manifest that the scalers are being created for
/// * `application_name` - The name of the application that the scalers are being created for
/// * `component_name` - The name of the component to convert
/// * `source_id` - The ID of the source component
/// * `target_id` - The optional ID of the target component
/// * `image` - The optional image reference of the target component
/// * `shared` - The optional shared application reference of the target component
/// * `policies` - The policies to use when creating the scalers so they can access secrets
/// * `notifier_subject` - The subject to use when creating the scalers so they can report status
/// * `notifier` - The publisher to use when creating the scalers so they can report status
/// * `snapshot_data` - The store to use when creating the scalers so they can access lattice state
#[allow(clippy::too_many_arguments)]
fn link_scaler<S, P, L>(
    link_property: &LinkProperty,
    lattice_id: &str,
    manifest_name: &str,
    application_name: &str,
    component_name: &str,
    source_id: String,
    target_id: Option<&String>,
    image: Option<&String>,
    shared: Option<&SharedApplicationComponentProperties>,
    policies: &HashMap<&String, &Policy>,
    notifier_subject: &str,
    notifier: &P,
    snapshot_data: &SnapshotStore<S, L>,
) -> BoxedScaler
where
    S: ReadStore + Send + Sync + Clone + 'static,
    P: Publisher + Clone + Send + Sync + 'static,
    L: LinkSource + ConfigSource + SecretSource + Clone + Send + Sync + 'static,
{
    let (mut config_scalers, mut source_config) = config_to_scalers(
        snapshot_data,
        manifest_name,
        &link_property
            .source
            .as_ref()
            .unwrap_or(&Default::default())
            .config,
    );
    let (target_config_scalers, mut target_config) =
        config_to_scalers(snapshot_data, manifest_name, &link_property.target.config);
    let (target_secret_scalers, target_secrets) = secrets_to_scalers(
        snapshot_data,
        manifest_name,
        &link_property.target.secrets,
        policies,
    );
    let (mut source_secret_scalers, source_secrets) = secrets_to_scalers(
        snapshot_data,
        manifest_name,
        &link_property
            .source
            .as_ref()
            .unwrap_or(&Default::default())
            .secrets,
        policies,
    );
    config_scalers.extend(target_config_scalers);
    source_secret_scalers.extend(target_secret_scalers);
    target_config.extend(target_secrets);
    source_config.extend(source_secrets);

    let (target_manifest_name, target_component_name) =
        match resolve_manifest_component(manifest_name, component_name, image, shared) {
            Ok(name) => name,
            Err(err) => {
                error!(err);
                return Box::new(StatusScaler::new(
                    uuid::Uuid::new_v4().to_string(),
                    LINK_SCALER_KIND,
                    format!(
                        "{} -({}:{})-> {}",
                        component_name,
                        link_property.namespace,
                        link_property.package,
                        link_property.target.name
                    ),
                    StatusInfo::failed(err),
                )) as BoxedScaler;
            }
        };
    let target = compute_component_id(target_manifest_name, target_id, target_component_name);
    Box::new(BackoffWrapper::new(
        LinkScaler::new(
            snapshot_data.clone(),
            LinkScalerConfig {
                source_id,
                target,
                wit_namespace: link_property.namespace.to_owned(),
                wit_package: link_property.package.to_owned(),
                wit_interfaces: link_property.interfaces.to_owned(),
                name: link_property
                    .name
                    .to_owned()
                    .unwrap_or_else(|| DEFAULT_LINK_NAME.to_string()),
                lattice_id: lattice_id.to_owned(),
                model_name: application_name.to_owned(),
                source_config,
                target_config,
            },
            snapshot_data.clone(),
        ),
        notifier.clone(),
        config_scalers,
        source_secret_scalers,
        notifier_subject,
        application_name,
        Some(Duration::from_secs(5)),
    )) as BoxedScaler
}

/// Returns a tuple which is a list of scalers and a list of the names of the configs that the
/// scalers use.
///
/// Any input [ConfigProperty] that has a `properties` field will be converted into a [ConfigScaler], and
/// the name of the configuration will be modified to be unique to the model and component. If the properties
/// field is not present, the name will be used as-is and assumed that it's managed externally to wadm.
fn config_to_scalers<C: ConfigSource + Send + Sync + Clone>(
    config_source: &C,
    manifest_name: &str,
    configs: &[ConfigProperty],
) -> (Vec<ConfigScaler<C>>, Vec<String>) {
    configs
        .iter()
        .map(|config| {
            let name = if config.properties.is_some() {
                compute_component_id(manifest_name, None, &config.name)
            } else {
                config.name.clone()
            };
            (
                ConfigScaler::new(config_source.clone(), &name, config.properties.as_ref()),
                name,
            )
        })
        .unzip()
}

fn secrets_to_scalers<S: SecretSource + Send + Sync + Clone>(
    secret_source: &S,
    manifest_name: &str,
    secrets: &[SecretProperty],
    policies: &HashMap<&String, &Policy>,
) -> (Vec<SecretScaler<S>>, Vec<String>) {
    secrets
        .iter()
        .map(|s| {
            let name = compute_secret_id(manifest_name, None, &s.name);
            let policy = *policies.get(&s.properties.policy).unwrap();
            (
                SecretScaler::new(
                    name.clone(),
                    policy.clone(),
                    s.clone(),
                    secret_source.clone(),
                ),
                name,
            )
        })
        .unzip()
}

/// Based on the name of the model and the optionally provided ID, returns a unique ID for the
/// component that is a sanitized version of the component reference and model name, separated
/// by a dash.
pub(crate) fn compute_component_id(
    manifest_name: &str,
    component_id: Option<&String>,
    component_name: &str,
) -> String {
    if let Some(id) = component_id {
        id.to_owned()
    } else {
        format!(
            "{}-{}",
            manifest_name
                .to_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "_"),
            component_name
                .to_lowercase()
                .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        )
    }
}

pub(crate) fn compute_secret_id(
    manifest_name: &str,
    component_id: Option<&String>,
    component_name: &str,
) -> String {
    let name = compute_component_id(manifest_name, component_id, component_name);
    format!("{SECRET_PREFIX}_{name}")
}

/// Helper function to resolve a link to a manifest component, returning the name of the manifest
/// and the name of the component where the target resides.
///
/// If the component resides in the same manifest, then the name of the manifest & the name of the
/// component as specified will be returned. In the case that the component resides in a shared
/// application, the name of the shared application & the name of the component in that application
/// will be returned.
///
/// # Arguments
/// * `application_name` - The name of the manifest that the scalers are being created for
/// * `component_name` - The name of the component in the source manifest to target
/// * `component_image_ref` - The image reference for the component
/// * `shared_app_info` - The optional shared application reference for the component
fn resolve_manifest_component<'a>(
    application_name: &'a str,
    component_name: &'a str,
    component_image_ref: Option<&'a String>,
    shared_app_info: Option<&'a SharedApplicationComponentProperties>,
) -> Result<(&'a str, &'a str), &'a str> {
    match (component_image_ref, shared_app_info) {
        (Some(_), None) => Ok((application_name, component_name)),
        (None, Some(app)) => Ok((app.name.as_str(), app.component.as_str())),
        // These two cases should both be unreachable, since this is caught at manifest
        // validation before it's put. Just in case, we'll log an error and ensure the status is failed
        (None, None) => Err("Application did not specify an image or shared application reference"),
        (Some(_image), Some(_app)) => {
            Err("Application specified both an image and a shared application reference")
        }
    }
}

#[cfg(test)]
mod test {
    use super::compute_component_id;

    #[test]
    fn compute_proper_component_id() {
        // User supplied ID always takes precedence
        assert_eq!(
            compute_component_id("mymodel", Some(&"myid".to_string()), "echo"),
            "myid"
        );
        assert_eq!(
            compute_component_id(
                "some model name with spaces cause yaml",
                Some(&"myid".to_string()),
                " echo "
            ),
            "myid"
        );
        // Sanitize component reference
        assert_eq!(
            compute_component_id("mymodel", None, "echo-component"),
            "mymodel-echo_component"
        );
        // Ensure we can support spaces in the model name, because YAML strings
        assert_eq!(
            compute_component_id("some model name with spaces cause yaml", None, "echo"),
            "some_model_name_with_spaces_cause_yaml-echo"
        );
        // Ensure we can support spaces in the model name, because YAML strings
        // Ensure we can support lowercasing the reference as well, just in case
        assert_eq!(
            compute_component_id("My ThInG", None, "thing.wasm"),
            "my_thing-thing_wasm"
        );
    }
}
