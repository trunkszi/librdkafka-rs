use std::{borrow::Cow, collections::BTreeMap};

use rdkafka::{Error, SerdeBridge, WireBatch, WireRecord};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Borrowed<'a> {
    number: u16,
    #[serde(borrow)]
    text: &'a str,
    #[serde(borrow, with = "serde_bytes")]
    bytes: &'a [u8],
    #[serde(borrow)]
    cow: Cow<'a, str>,
    #[serde(borrow, with = "serde_bytes")]
    cow_bytes: Cow<'a, [u8]>,
}
#[test]
fn integration_borrowed_golden_bytes_and_pointer_provenance() {
    let input = Borrowed {
        number: 0x1234,
        text: "hi",
        bytes: &[0, 255],
        cow: Cow::Borrowed("z"),
        cow_bytes: Cow::Borrowed(&[9, 8]),
    };
    let golden = [0x34, 0x12, 2, b'h', b'i', 2, 0, 255, 1, b'z', 2, 9, 8];
    let mut encoded = Vec::with_capacity(128);
    let ptr = encoded.as_ptr();
    assert_eq!(
        SerdeBridge::encode_into(&input, &mut encoded).unwrap(),
        golden.len()
    );
    assert_eq!(encoded, golden);
    assert_eq!(ptr, encoded.as_ptr());
    let decoded: Borrowed<'_> = SerdeBridge::decode_borrowed(&encoded).unwrap();
    assert_eq!(decoded, input);
    assert_eq!(decoded.text.as_ptr(), encoded[3..].as_ptr());
    assert_eq!(decoded.bytes.as_ptr(), encoded[6..].as_ptr());
    assert!(matches!(decoded.cow, Cow::Borrowed(_)));
    assert_eq!(decoded.cow.as_ptr(), encoded[9..].as_ptr());
    assert!(matches!(decoded.cow_bytes, Cow::Borrowed(_)));
    assert_eq!(decoded.cow_bytes.as_ptr(), encoded[11..].as_ptr());
    let wire = WireRecord::new("topic", Some(&encoded));
    assert_eq!(
        SerdeBridge::decode_record_borrowed::<Borrowed<'_>>(wire).unwrap(),
        input
    );
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
enum Event {
    Empty,
    New(u64),
    Pair(bool, i16),
    Named { text: String },
}
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct Nested {
    optional: Option<(i32, bool)>,
    list: Vec<Event>,
    map: BTreeMap<String, Vec<u16>>,
    scalar: char,
    floats: (f32, f64),
    wide: (i128, u128),
}
#[test]
fn integration_nested_sequences_maps_options_and_enums() {
    let value = Nested {
        optional: Some((-12, true)),
        list: vec![
            Event::Empty,
            Event::New(99),
            Event::Pair(false, -5),
            Event::Named { text: "ok".into() },
        ],
        map: BTreeMap::from([("a".into(), vec![1, 65535])]),
        scalar: 'λ',
        floats: (1.25, -2.5),
        wide: (i128::MIN, u128::MAX),
    };
    let mut encoded = vec![];
    SerdeBridge::encode_into(&value, &mut encoded).unwrap();
    assert_eq!(
        SerdeBridge::decode_borrowed::<Nested>(&encoded).unwrap(),
        value
    );
    assert_eq!(&encoded[..6], &[1, 244, 255, 255, 255, 1]);
    assert_eq!(
        SerdeBridge::decode_borrowed::<Option<u8>>(&[0]).unwrap(),
        None
    );
}
#[test]
fn integration_scenario_rejects_malformed_truncated_and_excessive_lengths() {
    for invalid in [&[2][..], &[255][..], &[][..]] {
        assert!(SerdeBridge::decode_borrowed::<bool>(invalid).is_err());
    }
    assert!(SerdeBridge::decode_borrowed::<Option<u8>>(&[2]).is_err());
    assert!(SerdeBridge::decode_borrowed::<u16>(&[1]).is_err());
    assert!(SerdeBridge::decode_borrowed::<u8>(&[1, 2]).is_err());
    for invalid in [
        vec![0x80, 0],
        vec![0x81, 0, b'a'],
        vec![0xff; 11],
        vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
        vec![1, 255],
        vec![3, b'a'],
    ] {
        assert!(
            SerdeBridge::decode_borrowed::<&str>(&invalid).is_err(),
            "{invalid:?}"
        );
    }
    assert!(SerdeBridge::decode_borrowed::<Vec<()>>(&[0x81, 0x80, 0x40]).is_err());
    assert!(SerdeBridge::decode_borrowed::<char>(&[0, 0xd8, 0, 0]).is_err());
    let mut encoded = vec![];
    SerdeBridge::encode_into(&(1u64, "abc", Some(42i32)), &mut encoded).unwrap();
    for len in 0..encoded.len() {
        assert!(SerdeBridge::decode_borrowed::<(u64, &str, Option<i32>)>(&encoded[..len]).is_err());
    }
    let mut batch = WireBatch::default();
    batch.push(WireRecord::new("t", None)).unwrap();
    assert!(SerdeBridge::decode_batch_into::<u8>(&batch, &mut vec![]).is_err());
}
#[test]
fn integration_batch_caller_buffers_streaming_and_partial_failure() {
    let mut batch = WireBatch::with_capacity(4);
    let mut scratch = Vec::with_capacity(32);
    let scratch_ptr = scratch.as_ptr();
    assert_eq!(
        SerdeBridge::encode_batch_into("t", &[10u32, 20, 30], &mut batch, &mut scratch).unwrap(),
        3
    );
    assert_eq!(scratch.as_ptr(), scratch_ptr);
    let mut appended = vec![7];
    assert_eq!(
        SerdeBridge::decode_batch_into(&batch, &mut appended).unwrap(),
        3
    );
    assert_eq!(appended, [7, 10, 20, 30]);
    let mut slots = [99u32; 4];
    assert_eq!(
        SerdeBridge::decode_batch_into_slice(&batch, &mut slots).unwrap(),
        3
    );
    assert_eq!(slots, [10, 20, 30, 99]);
    let mut short = [88u32; 2];
    assert!(SerdeBridge::decode_batch_into_slice(&batch, &mut short).is_err());
    assert_eq!(short, [88, 88]);
    let mut sum = 0;
    assert_eq!(
        SerdeBridge::decode_batch_with::<u32, _>(&batch, |x| {
            sum += x;
            Ok(())
        })
        .unwrap(),
        3
    );
    assert_eq!(sum, 60);
    batch.push(WireRecord::new("t", Some(&[1]))).unwrap();
    let mut partial = vec![];
    assert!(SerdeBridge::decode_batch_into::<u32>(&batch, &mut partial).is_err());
    assert_eq!(partial, [10, 20, 30]);
}
#[test]
fn integration_batch_borrows_from_arena_without_intermediate_storage() {
    let mut batch = WireBatch::default();
    let mut scratch = vec![];
    SerdeBridge::encode_batch_into("t", &["alpha", "beta"], &mut batch, &mut scratch).unwrap();
    let mut texts = Vec::<&str>::new();
    SerdeBridge::decode_batch_into(&batch, &mut texts).unwrap();
    assert_eq!(texts, ["alpha", "beta"]);
    for (text, record) in texts.iter().zip(batch.iter()) {
        assert_eq!(text.as_ptr(), record.payload.unwrap()[1..].as_ptr());
    }
}
#[test]
fn integration_custom_formats_and_callback_errors_preserve_identity() {
    let sentinel = String::from("caller sentinel");
    let identity = sentinel.as_ptr();
    let mut sentinel = Some(sentinel);
    let mut batch = WireBatch::default();
    let mut scratch = vec![];
    SerdeBridge::encode_batch_into_using(
        "custom",
        &[4u8, 5],
        &mut batch,
        &mut scratch,
        |v, out| {
            out.extend_from_slice(&[b'X', *v]);
            Ok(())
        },
    )
    .unwrap();
    let mut output = vec![];
    SerdeBridge::decode_batch_into_using(&batch, &mut output, |bytes| Ok(bytes[1])).unwrap();
    assert_eq!(output, [4, 5]);
    let error = SerdeBridge::decode_batch_with::<u16, _>(&batch, |_| {
        Err(Error::Codec(sentinel.take().unwrap()))
    })
    .unwrap_err();
    let Error::Codec(error) = error else {
        panic!("wrong error variant")
    };
    assert_eq!(error.as_ptr(), identity);
    let mut bytes = vec![99];
    assert!(
        SerdeBridge::encode_into_using(&1, &mut bytes, |_, out| {
            out.push(2);
            Err(Error::Codec("caller failure".into()))
        })
        .is_err()
    );
    assert_eq!(bytes, [99]);
    let result =
        SerdeBridge::decode_borrowed_using(b"42", |bytes| Ok(std::str::from_utf8(bytes).unwrap()))
            .unwrap();
    assert_eq!(result, "42");
    let mut slots = [0u8; 3];
    SerdeBridge::decode_batch_into_slice_using(&batch, &mut slots, |bytes| Ok(bytes[1])).unwrap();
    assert_eq!(slots, [4, 5, 0]);
}

#[test]
fn integration_scenario_configurable_limits_bound_untrusted_collections() {
    use rdkafka::serde_bridge::DecodeLimits;
    let mut bytes = vec![];
    SerdeBridge::encode_into(&vec![Some(Some(1u8)); 3], &mut bytes).unwrap();
    let small = DecodeLimits {
        max_collection_elements: 2,
        max_depth: 10,
    };
    assert!(
        SerdeBridge::decode_borrowed_with_limits::<Vec<Option<Option<u8>>>>(&bytes, small).is_err()
    );
    let shallow = DecodeLimits {
        max_collection_elements: 3,
        max_depth: 2,
    };
    assert!(
        SerdeBridge::decode_borrowed_with_limits::<Vec<Option<Option<u8>>>>(&bytes, shallow)
            .is_err()
    );
    let sufficient = DecodeLimits {
        max_collection_elements: 3,
        max_depth: 3,
    };
    assert_eq!(
        SerdeBridge::decode_borrowed_with_limits::<Vec<Option<Option<u8>>>>(&bytes, sufficient)
            .unwrap(),
        vec![Some(Some(1)); 3]
    );
}

#[test]
fn integration_scenario_compound_count_failures_roll_back_builtin_writes() {
    struct WrongCount(bool);
    impl Serialize for WrongCount {
        fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            use serde::ser::SerializeSeq;
            let mut sequence = serializer.serialize_seq(Some(1))?;
            if self.0 {
                sequence.serialize_element(&1u8)?;
                sequence.serialize_element(&2u8)?;
            }
            sequence.end()
        }
    }
    for excessive in [false, true] {
        let mut output = vec![42, 43];
        assert!(SerdeBridge::encode_into(&WrongCount(excessive), &mut output).is_err());
        assert_eq!(output, [42, 43]);
    }
}

#[test]
fn integration_scenario_early_returning_visitors_cannot_accept_unread_elements() {
    #[derive(Debug)]
    struct Early<const MAP: bool>;
    struct EarlyVisitor<const MAP: bool>;
    impl<'de, const MAP: bool> serde::de::Visitor<'de> for EarlyVisitor<MAP> {
        type Value = Early<MAP>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a collection")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, _: A) -> Result<Self::Value, A::Error> {
            Ok(Early)
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, _: A) -> Result<Self::Value, A::Error> {
            Ok(Early)
        }
    }
    impl<'de, const MAP: bool> Deserialize<'de> for Early<MAP> {
        fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
            if MAP {
                deserializer.deserialize_map(EarlyVisitor)
            } else {
                deserializer.deserialize_seq(EarlyVisitor)
            }
        }
    }
    assert!(SerdeBridge::decode_borrowed::<Early<false>>(&[1]).is_err());
    assert!(SerdeBridge::decode_borrowed::<Early<true>>(&[1]).is_err());
    assert!(SerdeBridge::decode_borrowed::<Early<false>>(&[0]).is_ok());
    assert!(SerdeBridge::decode_borrowed::<Early<true>>(&[0]).is_ok());
}

#[test]
fn integration_empty_payload_and_canonical_multibyte_lengths() {
    let mut bytes = vec![];
    SerdeBridge::encode_into(&"x".repeat(128), &mut bytes).unwrap();
    assert_eq!(&bytes[..2], &[0x80, 1]);
    assert_eq!(
        SerdeBridge::decode_borrowed::<&str>(&bytes).unwrap().len(),
        128
    );
    let mut batch = WireBatch::default();
    batch.push(WireRecord::new("t", Some(&[]))).unwrap();
    let mut output = vec![];
    assert_eq!(
        SerdeBridge::decode_batch_into::<()>(&batch, &mut output).unwrap(),
        1
    );
    assert_eq!(output, [()]);
    batch.push(WireRecord::new("t", None)).unwrap();
    let mut output = vec![];
    assert!(SerdeBridge::decode_batch_into::<()>(&batch, &mut output).is_err());
    assert_eq!(output, [()]);
}

#[test]
fn integration_encoded_sequence_borrows_and_reencodes_nested_values() {
    use rdkafka::serde_bridge::{BorrowDecode, Deserializer, EncodedSequence};
    let input = vec![vec![Some("alpha"), None], vec![Some("beta")]];
    let mut bytes = vec![];
    SerdeBridge::encode_into(&(input, 77u16), &mut bytes).unwrap();
    let mut decoder = Deserializer::new(&bytes);
    type Inner<'a> = EncodedSequence<'a, Option<&'a str>>;
    let sequence = EncodedSequence::<Inner<'_>>::decode(&mut decoder).unwrap();
    assert_eq!(sequence.len(), 2);
    assert!(!sequence.is_empty());
    assert_eq!(sequence.encoded_bytes().as_ptr(), bytes[1..].as_ptr());
    assert_eq!(u16::borrow_decode(&mut decoder).unwrap(), 77);
    decoder.finish().unwrap();
    let mut outer = sequence.values();
    let first = outer.next().unwrap();
    let mut first_values = first.values();
    let text = first_values.next().unwrap().unwrap();
    assert_eq!(text, "alpha");
    assert_eq!(text.as_ptr(), bytes[4..].as_ptr());
    assert_eq!(first_values.next(), Some(None));
    assert_eq!(first_values.next(), None);
    assert_eq!(outer.next().unwrap().values().next(), Some(Some("beta")));
    assert!(outer.next().is_none());
    let mut reencoded = vec![];
    SerdeBridge::encode_into(&(sequence, 77u16), &mut reencoded).unwrap();
    assert_eq!(reencoded, bytes);
}

#[test]
fn integration_scenario_encoded_sequence_validates_before_advancing() {
    use rdkafka::serde_bridge::{BorrowDecode, DecodeLimits, Deserializer, EncodedSequence};
    for malformed in [&[1, 2][..], &[1, 1, 1, 255][..]] {
        let mut decoder = Deserializer::new(malformed);
        assert!(EncodedSequence::<Option<&str>>::decode(&mut decoder).is_err());
        assert_eq!(u8::borrow_decode(&mut decoder).unwrap(), 1);
    }
    let mut decoder = Deserializer::with_limits(
        &[2, 0, 0],
        DecodeLimits {
            max_collection_elements: 1,
            max_depth: 5,
        },
    );
    assert!(EncodedSequence::<Option<u8>>::decode(&mut decoder).is_err());
    let mut decoder = Deserializer::with_limits(
        &[1, 1, 1, 0],
        DecodeLimits {
            max_collection_elements: 10,
            max_depth: 2,
        },
    );
    assert!(EncodedSequence::<Option<Option<u8>>>::decode(&mut decoder).is_err());
    let mut decoder = Deserializer::new(&[0]);
    let empty = EncodedSequence::<u64>::decode(&mut decoder).unwrap();
    assert!(empty.is_empty());
    assert!(empty.iter().next().is_none());
    decoder.finish().unwrap();
}
