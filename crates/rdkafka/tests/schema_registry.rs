#![cfg(feature = "http")]
#[path = "support/http_server.rs"]
mod http_server;
use rdkafka::{
    http::{HttpTransport, TlsSettings},
    schema_registry::{
        Schema, SchemaRegistryClient, SchemaType, decode_confluent, encode_confluent,
    },
};
use sonic_rs::{JsonValueMutTrait, JsonValueTrait};

#[test]
fn unit_scenario_confluent_frames_validate_prefix_and_preserve_payload() {
    let mut buffer = vec![9; 32];
    for id in [0, 1, u32::MAX] {
        encode_confluent(id, b"payload", &mut buffer).unwrap();
        let (actual, payload) = decode_confluent(&buffer).unwrap();
        assert_eq!(actual, id);
        assert_eq!(payload, b"payload");
        assert_eq!(payload.as_ptr(), buffer[5..].as_ptr());
    }
    assert!(decode_confluent(&[0; 4]).is_err());
    assert!(decode_confluent(&[1; 5]).is_err());
}
#[test]
fn integration_scenario_subject_encoding_and_cache_keys() {
    let (url, server) = http_server::serve(vec![
        (200, r#"{"id":0}"#.into()),
        (200, r#"{"id":2}"#.into()),
        (200, r#"{"id":3}"#.into()),
    ]);
    let mut client =
        SchemaRegistryClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), url);
    let avro = Schema::new(r#"{"type":"string"}"#, SchemaType::Avro);
    assert_eq!(client.register_schema("a/b ?", &avro).unwrap(), 0);
    assert_eq!(client.register_schema("a/b ?", &avro).unwrap(), 0);
    assert_eq!(client.get_schema_by_id(0).unwrap(), avro);
    assert_eq!(client.register_schema("other", &avro).unwrap(), 2);
    assert_eq!(
        client
            .register_schema("a/b ?", &Schema::new(&avro.schema, SchemaType::Json))
            .unwrap(),
        3
    );
    let requests = server.join().unwrap();
    assert!(requests[0].starts_with("POST /subjects/a%2Fb%20%3F/versions "));
    let json = requests[0].split("\r\n\r\n").nth(1).unwrap();
    let value: sonic_rs::Value = sonic_rs::from_str(json).unwrap();
    assert_eq!(value["schema"], avro.schema);
}
#[test]
fn integration_scenario_failed_and_mismatched_lookup_do_not_populate_cache() {
    let (url, server) = http_server::serve(vec![
        (404, "credentials must stay private".into()),
        (
            200,
            r#"{"id":7,"subject":"wrong","version":1,"schema":"s"}"#.into(),
        ),
        (200, r#"{"id":8}"#.into()),
        (200, r#"{"schema":"s","schemaType":"INVALID"}"#.into()),
        (200, r#"{"schema":"valid"}"#.into()),
    ]);
    let mut client =
        SchemaRegistryClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), url);
    let schema = Schema::new("s", SchemaType::Avro);
    assert!(
        !client
            .lookup_schema("wanted", &schema)
            .unwrap_err()
            .to_string()
            .contains("credentials")
    );
    assert!(client.lookup_schema("wanted", &schema).is_err());
    assert_eq!(client.register_schema("wanted", &schema).unwrap(), 8);
    assert!(client.get_schema_by_id(7).is_err());
    assert_eq!(client.get_schema_by_id(7).unwrap().schema, "valid");
    server.join().unwrap();
}
#[test]
fn integration_scenario_latest_is_fresh_and_successful_lookup_caches_identity() {
    let (url, server) = http_server::serve(vec![
        (
            200,
            r#"{"id":1,"subject":"s","version":1,"schema":"one"}"#.into(),
        ),
        (
            200,
            r#"{"id":2,"subject":"s","version":2,"schema":"two"}"#.into(),
        ),
        (
            200,
            r#"{"id":3,"subject":"other","version":1,"schema":"lookup","schemaType":"JSON"}"#
                .into(),
        ),
    ]);
    let mut client =
        SchemaRegistryClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), url);
    assert_eq!(client.get_latest_subject_schema("s").unwrap().version, 1);
    assert_eq!(client.get_latest_subject_schema("s").unwrap().version, 2);
    let schema = Schema::new("lookup", SchemaType::Json);
    assert_eq!(client.lookup_schema_id("other", &schema).unwrap(), 3);
    assert_eq!(client.lookup_schema_id("other", &schema).unwrap(), 3);
    assert_eq!(client.register_schema("other", &schema).unwrap(), 3);
    assert_eq!(client.get_schema_by_id(3).unwrap(), schema);
    assert_eq!(server.join().unwrap().len(), 3);
}

#[test]
fn integration_scenario_invalid_subjects_fail_before_http() {
    let (url, server) = http_server::serve(vec![(200, r#"{"id":9}"#.into())]);
    let mut client =
        SchemaRegistryClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), url);
    let schema = Schema::new("s", SchemaType::Avro);
    for subject in ["", " ", "\t\r\n", ".", ".."] {
        assert!(client.register_schema(subject, &schema).is_err());
        assert!(client.lookup_schema(subject, &schema).is_err());
        assert!(client.lookup_schema_id(subject, &schema).is_err());
        assert!(client.get_latest_subject_schema(subject).is_err());
    }
    assert_eq!(client.register_schema("valid", &schema).unwrap(), 9);
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 1);
    assert!(requests[0].starts_with("POST /subjects/valid/versions "));
}

#[test]
fn integration_scenario_invalid_registration_ids_retry_without_cache() {
    for bad in ["{", "{}", r#"{"id":-1}"#, r#"{"id":4294967296}"#] {
        let (url, server) =
            http_server::serve(vec![(200, bad.into()), (200, r#"{"id":17}"#.into())]);
        let mut client =
            SchemaRegistryClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), url);
        let schema = Schema::new("s", SchemaType::Avro);
        assert!(client.register_schema("s", &schema).is_err());
        assert_eq!(client.register_schema("s", &schema).unwrap(), 17);
        assert_eq!(client.lookup_schema_id("s", &schema).unwrap(), 17);
        assert_eq!(client.get_schema_by_id(17).unwrap(), schema);
        assert_eq!(server.join().unwrap().len(), 2);
    }
}

#[test]
fn integration_scenario_invalid_lookup_responses_retry_without_cache() {
    let valid = sonic_rs::json!({
        "id": 19, "subject": "s", "version": 1, "schema": "definition", "schemaType": "JSON"
    });
    let mut responses = vec!["{".to_owned()];
    for (field, value) in [
        ("id", sonic_rs::Value::new()),
        ("id", sonic_rs::json!(-1)),
        ("id", sonic_rs::json!(4294967296u64)),
        ("version", sonic_rs::json!(0)),
        ("version", sonic_rs::json!(4294967296u64)),
        ("schema", sonic_rs::Value::new()),
        ("schemaType", sonic_rs::json!("AVRO")),
    ] {
        let mut bad = valid.clone();
        if value.is_null() {
            bad.as_object_mut().unwrap().remove(&field);
        } else {
            bad[field] = value;
        }
        responses.push(bad.to_string());
    }
    for bad in responses {
        let (url, server) = http_server::serve(vec![
            (200, bad),
            (
                200,
                r#"{"schema":"global-definition","schemaType":"JSON"}"#.into(),
            ),
            (200, valid.to_string()),
        ]);
        let mut client =
            SchemaRegistryClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), url);
        let schema = Schema::new("definition", SchemaType::Json);
        assert!(client.lookup_schema("s", &schema).is_err());
        assert_eq!(
            client.get_schema_by_id(19).unwrap().schema,
            "global-definition"
        );
        assert_eq!(client.lookup_schema_id("s", &schema).unwrap(), 19);
        assert_eq!(client.register_schema("s", &schema).unwrap(), 19);
        assert_eq!(client.get_schema_by_id(19).unwrap(), schema);
        assert_eq!(server.join().unwrap().len(), 3);
    }
}

#[test]
fn integration_scenario_registry_endpoint_validation_precedes_network() {
    for endpoint in [
        "http://user:private@127.0.0.1:1",
        "http://127.0.0.1:1/#fragment",
        "http://127.0.0.1:1/?query=private",
        "ws://127.0.0.1:1",
        "not-a-uri",
    ] {
        let transport = HttpTransport::new(TlsSettings::default()).unwrap();
        let mut client = SchemaRegistryClient::new(transport, endpoint);
        let error = client
            .register_schema("subject", &Schema::new("definition", SchemaType::Json))
            .unwrap_err();
        assert!(error.to_string().contains("invalid registry URL"));
        assert!(!error.to_string().contains("private"));
    }
}
