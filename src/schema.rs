use schemars::schema_for;
use wadm_types::Manifest;

fn main() {
    let schema = schema_for!(Manifest);
    let sch = serde_json::to_string_pretty(&schema).unwrap();

    std::fs::write("oam.schema.json", sch).unwrap();
}
