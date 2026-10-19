//! Derive expansion under raw identifiers and generic schemas.
#![allow(non_camel_case_types)]
use rdkafka::{Record, WireBatch};

#[derive(Debug, PartialEq, Record)]
struct r#type<T> {
    r#type: String,
    value: T,
}
#[test]
fn integration_raw_identifiers_and_generic_fields() {
    let record = r#type {
        r#type: "raw".into(),
        value: 42u64,
    };
    let mut wire = WireBatch::new();
    record.encode_into(&mut wire).unwrap();
    let view = r#type::<u64>::view(wire.get(0).unwrap()).unwrap();
    assert_eq!(view.r#type, "raw");
    assert_eq!(view.value, 42);
    let mut batch = typeBatch::<u64>::default();
    batch.push_ref(view).unwrap();
    assert_eq!(batch.column_type(), &["raw"]);
    assert_eq!(batch.column_value(), &[42]);
}

#[test]
fn integration_scenario_invalid_metadata_has_compile_diagnostics() {
    let cases = trybuild::TestCases::new();
    cases.compile_fail("tests/fixtures/derive/conflicting_metadata.rs");
    cases.compile_fail("tests/fixtures/derive/duplicate_key.rs");
}
