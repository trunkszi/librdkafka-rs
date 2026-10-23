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
    let package = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = package.join("../..");
    let scratch = root.join("target/derive-diagnostics");
    std::fs::create_dir_all(&scratch).unwrap();
    for name in ["conflicting_metadata", "duplicate_key"] {
        let fixture = package.join(format!("tests/fixtures/derive/{name}.rs"));
        let manifest = format!(
            "[package]\nname = \"derive-diagnostics\"\nversion = \"0.0.0\"\nedition = \
             \"2024\"\n\n[workspace]\n\n[dependencies]\nrdkafka = {{ path = {package:?}, package \
             = \"librdkafka-rs\" }}\n\n[[bin]]\nname = \"diagnostic\"\npath = {fixture:?}\n"
        );
        std::fs::write(scratch.join("Cargo.toml"), manifest).unwrap();
        let output = std::process::Command::new(env!("CARGO"))
            .args(["check", "--offline", "--color", "never", "--manifest-path"])
            .arg(scratch.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(root.join("target"))
            .output()
            .unwrap();
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        let snapshot = std::fs::read_to_string(fixture.with_extension("stderr")).unwrap();
        let expected = snapshot.lines().next().unwrap();
        assert!(
            !output.status.success(),
            "invalid metadata compiled: {name}"
        );
        assert!(
            diagnostic.contains(expected),
            "missing {expected}: {diagnostic}"
        );
        assert!(
            diagnostic.contains(&format!("{name}.rs:")),
            "missing source location: {diagnostic}"
        );
    }
    std::fs::remove_dir_all(scratch).unwrap();
}
