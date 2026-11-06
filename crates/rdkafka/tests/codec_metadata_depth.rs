use rdkafka::{Record, Result, WireBatch};

#[derive(Debug, PartialEq, Record)]
struct NestedKey {
    #[record(key)]
    key: Option<Option<String>>,
    payload: u32,
}

#[derive(Debug, PartialEq, Record)]
struct NestedHeader {
    #[record(header = "nested")]
    value: Option<Option<Vec<u8>>>,
    payload: u32,
}

#[test]
fn nested_key_preserves_inner_null() -> Result<()> {
    for key in [
        None,
        Some(None),
        Some(Some(String::new())),
        Some(Some("value".into())),
    ] {
        let record = NestedKey { key, payload: 7 };
        let mut wire = WireBatch::new();
        record.encode_into(&mut wire)?;
        assert_eq!(
            NestedKey::decode(
                wire.get(0)
                    .ok_or_else(|| rdkafka::Error::Codec("frame".into()))?
            )?,
            record
        );
    }
    Ok(())
}

#[test]
fn nested_header_preserves_inner_null() -> Result<()> {
    for value in [
        None,
        Some(None),
        Some(Some(Vec::new())),
        Some(Some(vec![0, 255])),
    ] {
        let record = NestedHeader { value, payload: 7 };
        let mut wire = WireBatch::new();
        record.encode_into(&mut wire)?;
        assert_eq!(
            NestedHeader::decode(
                wire.get(0)
                    .ok_or_else(|| rdkafka::Error::Codec("frame".into()))?
            )?,
            record
        );
    }
    Ok(())
}
