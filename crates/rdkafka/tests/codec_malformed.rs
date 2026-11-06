use std::collections::BTreeMap;

use rdkafka::{SerdeBridge, serde_bridge::DecodeLimits};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Frame {
    label: String,
    values: Vec<(u16, Option<Vec<u8>>)>,
    metadata: BTreeMap<String, bool>,
}

#[test]
fn malformed_payloads_never_panic() {
    let mut seed = 0x78a4_391b_7412_927eu64;
    for len in 0usize..=96 {
        for _ in 0..64 {
            let bytes: Vec<_> = (0..len)
                .map(|_| {
                    seed ^= seed << 13;
                    seed ^= seed >> 7;
                    seed ^= seed << 17;
                    seed as u8
                })
                .collect();
            let result = std::panic::catch_unwind(|| {
                SerdeBridge::decode_borrowed_with_limits::<Frame>(
                    &bytes,
                    DecodeLimits {
                        max_collection_elements: 16,
                        max_depth: 16,
                    },
                )
            });
            assert!(result.is_ok());
            if let Ok(Ok(frame)) = result {
                assert!(frame.values.len() <= 16 && frame.metadata.len() <= 16);
                assert!(frame.label.len() <= bytes.len());
            }
        }
    }
}

#[test]
fn length_prefixes_reject_overflow() {
    for bytes in [
        vec![0x80, 0],
        vec![0xff; 10],
        vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
    ] {
        assert!(SerdeBridge::decode_borrowed::<Vec<u8>>(&bytes).is_err());
        assert!(SerdeBridge::decode_borrowed::<String>(&bytes).is_err());
    }
}
