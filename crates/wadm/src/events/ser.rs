//! Custom serializers for event fields

use serde::Serializer;

pub(crate) fn tags<S>(data: &Option<Vec<String>>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match data {
        Some(v) => {
            let joined = v.join(",");
            serializer.serialize_some(&joined)
        }
        None => serializer.serialize_none(),
    }
}
