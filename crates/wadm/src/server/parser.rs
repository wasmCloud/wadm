use async_nats::HeaderMap;

use wadm_types::Manifest;

/// The name of the header in the NATS request to use for content type inference. The header value
/// should be a valid MIME type
pub const CONTENT_TYPE_HEADER: &str = "Content-Type";

// NOTE(thomastaylor312): If we do _anything_ else with mime types in the server, we should just
// pull in the `mime` crate instead
const YAML_MIME: &str = "application/yaml";
const JSON_MIME: &str = "application/json";

/// Parse the incoming bytes to a manifest
///
/// This function takes the optional headers from a NATS request to use them as a type hint for
/// parsing
pub fn parse_manifest(data: Vec<u8>, headers: Option<&HeaderMap>) -> anyhow::Result<Manifest> {
    // There is far too much cloning here, but there is no way to just return a reference to a &str
    let content_type = headers
        .and_then(|map| map.get(CONTENT_TYPE_HEADER).cloned())
        .map(|value| value.as_str().to_owned());
    if let Some(content_type) = content_type {
        match content_type.as_str() {
            JSON_MIME => serde_json::from_slice(&data).map_err(anyhow::Error::from),
            YAML_MIME => serde_yaml::from_slice(&data).map_err(anyhow::Error::from),
            _ => {
                // If the user passed a non-supported mime type, we should let them know rather than
                // just falling back
                Err(anyhow::anyhow!(
                    "Unsupported content type {content_type} given. Wadm supports YAML and JSON"
                ))
            }
        }
    } else {
        parse_yaml_or_json(data)
    }
}

/// Parse the bytes as yaml or json (in that order)
fn parse_yaml_or_json(data: Vec<u8>) -> anyhow::Result<Manifest> {
    serde_yaml::from_slice(&data).or_else(|e| {
        serde_json::from_slice(&data).map_err(|err| {
            // Combine both errors in case one was a legit parsing failure due to invalid data
            anyhow::anyhow!("JSON parsing failed: {err:?}")
                .context(format!("YAML parsing failed: {e:?}"))
        })
    })
}
