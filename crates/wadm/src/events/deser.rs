//! Custom deserializers for event fields

use serde::{Deserialize, Deserializer};

pub(crate) fn tags<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Deserialize::deserialize(deserializer)?;
    // NOTE: We could probably do something to avoid extra allocations here, but not worth it at
    // this time
    Ok(s.map(|st| st.split(',').map(ToOwned::to_owned).collect()))
}
