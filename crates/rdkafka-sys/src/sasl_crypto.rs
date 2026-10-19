//! Cryptographic primitives for the native SCRAM providers, independent of TLS.

use std::{
    ffi::{c_int, c_uint},
    num::NonZeroU32,
    ptr, slice,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ring::{
    digest, hmac, pbkdf2,
    rand::{SecureRandom, SystemRandom},
};

fn algorithm(
    kind: c_uint,
) -> Option<(
    &'static digest::Algorithm,
    hmac::Algorithm,
    pbkdf2::Algorithm,
)> {
    match kind {
        1 => Some((
            &digest::SHA1_FOR_LEGACY_USE_ONLY,
            hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY,
            pbkdf2::PBKDF2_HMAC_SHA1,
        )),
        256 => Some((
            &digest::SHA256,
            hmac::HMAC_SHA256,
            pbkdf2::PBKDF2_HMAC_SHA256,
        )),
        512 => Some((
            &digest::SHA512,
            hmac::HMAC_SHA512,
            pbkdf2::PBKDF2_HMAC_SHA512,
        )),
        _ => None,
    }
}

// All entry points are private to the patched native provider. C supplies valid
// input buffers and writable output buffers with the capacities in sasl_crypto.h.
/// # Safety
/// A nonempty input must point to `len` readable bytes valid for the returned borrow.
unsafe fn input<'a>(data: *const u8, len: usize) -> &'a [u8] {
    if len == 0 {
        return &[];
    }
    // SAFETY: The native provider owns a readable buffer of len bytes.
    unsafe { slice::from_raw_parts(data, len) }
}

/// # Safety
/// Input must contain `len` readable bytes; output must hold 64 writable bytes.
#[unsafe(no_mangle)]
unsafe extern "C" fn rdkafka_rs_digest(
    kind: c_uint,
    data: *const u8,
    len: usize,
    out: *mut u8,
) -> c_int {
    let Some((algorithm, _, _)) = algorithm(kind) else {
        return -1;
    };
    // SAFETY: C provides readable input and at least 64 writable output bytes.
    unsafe {
        let value = digest::digest(algorithm, input(data, len));
        ptr::copy_nonoverlapping(value.as_ref().as_ptr(), out, value.as_ref().len());
    }
    0
}

/// # Safety
/// Key and data must be readable for their lengths; output must hold 64 writable bytes.
#[unsafe(no_mangle)]
unsafe extern "C" fn rdkafka_rs_hmac(
    kind: c_uint,
    key: *const u8,
    key_len: usize,
    data: *const u8,
    len: usize,
    out: *mut u8,
) -> c_int {
    let Some((_, algorithm, _)) = algorithm(kind) else {
        return -1;
    };
    // SAFETY: C provides readable input/key and at least 64 writable output bytes.
    unsafe {
        let key = hmac::Key::new(algorithm, input(key, key_len));
        let value = hmac::sign(&key, input(data, len));
        ptr::copy_nonoverlapping(value.as_ref().as_ptr(), out, value.as_ref().len());
    }
    0
}

/// # Safety
/// Key and salt must be readable for their lengths; output must hold the selected digest.
#[unsafe(no_mangle)]
unsafe extern "C" fn rdkafka_rs_pbkdf2(
    kind: c_uint,
    key: *const u8,
    key_len: usize,
    salt: *const u8,
    salt_len: usize,
    iterations: c_int,
    out: *mut u8,
) -> c_int {
    let Some((digest, _, algorithm)) = algorithm(kind) else {
        return -1;
    };
    let Ok(iterations) = u32::try_from(iterations) else {
        return -1;
    };
    let Some(iterations) = NonZeroU32::new(iterations) else {
        return -1;
    };
    // SAFETY: C provides readable key/salt and an output sized for this digest.
    unsafe {
        pbkdf2::derive(
            algorithm,
            iterations,
            input(salt, salt_len),
            input(key, key_len),
            slice::from_raw_parts_mut(out, digest.output_len()),
        );
    }
    0
}

/// # Safety
/// Output must point to `len` writable bytes.
#[unsafe(no_mangle)]
unsafe extern "C" fn rdkafka_rs_random(out: *mut u8, len: usize) -> c_int {
    // SAFETY: C provides len writable bytes.
    let out = unsafe { slice::from_raw_parts_mut(out, len) };
    if SystemRandom::new().fill(out).is_ok() {
        0
    } else {
        -1
    }
}

/// # Safety
/// Input must be readable for `len` bytes; output must hold the encoded length plus a NUL.
#[unsafe(no_mangle)]
unsafe extern "C" fn rdkafka_rs_base64(out: *mut u8, data: *const u8, len: c_int) -> c_int {
    let Ok(len) = usize::try_from(len) else {
        return -1;
    };
    let Some(capacity) = base64::encoded_len(len, true) else {
        return -1;
    };
    // SAFETY: Native OAuth allocates 4 * ceil(len / 3) + 1 writable bytes.
    unsafe {
        let buffer = slice::from_raw_parts_mut(out, capacity + 1);
        match STANDARD.encode_slice(input(data, len), &mut buffer[..capacity]) {
            Ok(written) => {
                buffer[written] = 0;
                c_int::try_from(written).unwrap_or(-1)
            }
            Err(_) => -1,
        }
    }
}

/// # Safety
/// Input must be readable for `len` bytes; output must hold `len / 4 * 3 + 1` bytes.
#[unsafe(no_mangle)]
unsafe extern "C" fn rdkafka_rs_base64_decode(out: *mut u8, data: *const u8, len: usize) -> c_int {
    let capacity = len / 4 * 3;
    // SAFETY: Native decoding allocates len / 4 * 3 + 1 bytes and supplies valid input.
    unsafe {
        let buffer = slice::from_raw_parts_mut(out, capacity + 1);
        match STANDARD.decode_slice(input(data, len), &mut buffer[..capacity]) {
            Ok(written) => {
                buffer[written] = 0;
                c_int::try_from(written).unwrap_or(-1)
            }
            Err(_) => -1,
        }
    }
}

#[cfg(test)]
mod unit {
    use super::*;

    #[test]
    fn scenario_base64_salt_decoding_rejects_malformed_input() {
        let mut output = [0; 19];
        // SAFETY: Output is large enough for both valid and malformed inputs.
        unsafe {
            assert_eq!(
                rdkafka_rs_base64_decode(
                    output.as_mut_ptr(),
                    b"W22ZaJ0SNY7soEsUEjb6gQ==".as_ptr(),
                    24
                ),
                16
            );
            assert_eq!(
                output[..16],
                [
                    91, 109, 153, 104, 157, 18, 53, 142, 236, 160, 75, 20, 18, 54, 250, 129
                ]
            );
            assert_eq!(
                rdkafka_rs_base64_decode(output.as_mut_ptr(), b"!!!!".as_ptr(), 4),
                -1
            );
        }
    }

    #[test]
    fn scenario_sha_and_hmac_known_vectors() {
        for (kind, hash, mac) in [
            (
                256,
                "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=",
                "97yD9DBThCSxMpjmqm+xQ+9NWaFJRhdZl0edvC0aPNg=",
            ),
            (
                512,
                "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/\
                 uu9RU1EI2Q86A4qmslPpUyknw==",
                "tCrwkFe6weLUFwjkipAuCbX/\
                 fxKrQopP6GZTxz3SSPuC+UilSfe3kaW0GRXuTR7Dk1NX5OIxclDQNyr6Lr7rOg==",
            ),
        ] {
            let mut output = [0; 64];
            // SAFETY: Inputs are valid slices and output holds the largest digest.
            unsafe {
                assert_eq!(
                    rdkafka_rs_digest(kind, b"abc".as_ptr(), 3, output.as_mut_ptr()),
                    0
                );
            }
            let size = algorithm(kind).unwrap().0.output_len();
            assert_eq!(STANDARD.encode(&output[..size]), hash);
            // SAFETY: All buffers have the capacities required by the C ABI.
            unsafe {
                assert_eq!(
                    rdkafka_rs_hmac(
                        kind,
                        b"key".as_ptr(),
                        3,
                        b"The quick brown fox jumps over the lazy dog".as_ptr(),
                        43,
                        output.as_mut_ptr()
                    ),
                    0
                );
            }
            assert_eq!(STANDARD.encode(&output[..size]), mac);
        }
    }

    #[test]
    fn scenario_scram_sha256_rfc7677_salted_password() {
        let salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let mut output = [0; 32];
        // SAFETY: Output is SHA-256 sized and inputs are valid slices.
        unsafe {
            assert_eq!(
                rdkafka_rs_pbkdf2(
                    256,
                    b"pencil".as_ptr(),
                    6,
                    salt.as_ptr(),
                    salt.len(),
                    4096,
                    output.as_mut_ptr()
                ),
                0
            );
        }
        assert_eq!(
            STANDARD.encode(output),
            "xKSVEDI6tPlSysH6mUQZOeeOp01r6B3fcJbodRPcYV0="
        );
    }

    #[test]
    fn scenario_pbkdf2_sha512_known_vector_and_invalid_iterations() {
        let mut output = [0; 64];
        // SAFETY: The output is SHA-512 sized and all inputs are valid slices.
        unsafe {
            assert_eq!(
                rdkafka_rs_pbkdf2(
                    512,
                    b"password".as_ptr(),
                    8,
                    b"salt".as_ptr(),
                    4,
                    1,
                    output.as_mut_ptr()
                ),
                0
            );
            assert_eq!(
                rdkafka_rs_pbkdf2(
                    512,
                    b"password".as_ptr(),
                    8,
                    b"salt".as_ptr(),
                    4,
                    0,
                    output.as_mut_ptr()
                ),
                -1
            );
        }
        assert_eq!(
            STANDARD.encode(output),
            "hn9wzxreAs/zdSWZo6U9xK80x6ZpgVrl1RNVThyM8lLALUcKKFoFAbrZmb/pQ8CPBQI119aLHaVeY/c7YKV/\
             zg=="
        );
    }
}

#[cfg(test)]
mod integration {
    use crate::ffi;

    #[test]
    fn lifecycle_scenario_builtin_native_providers_initialize_without_ssl() {
        // This exercises provider selection and local initialization only.
        // Broker authentication is covered by the live broker integration tests.
        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "OAUTHBEARER"] {
            let mut conf = ffi::conf_new(false);
            for (key, value) in [
                ("security.protocol", "SASL_PLAINTEXT"),
                ("sasl.mechanism", mechanism),
                ("sasl.username", "user"),
                ("sasl.password", "password"),
            ] {
                ffi::conf_set(conf.pin_mut(), key, value).unwrap();
            }
            let features = ffi::conf_get(&conf, "builtin.features").unwrap();
            for feature in ["sasl_plain", "sasl_scram", "sasl_oauthbearer"] {
                assert!(
                    features.split(',').any(|value| value == feature),
                    "missing {feature}: {features}"
                );
            }
            assert!(!features.split(',').any(|value| value == "ssl"));
            let mut client =
                ffi::client_new(&conf, 0).unwrap_or_else(|error| panic!("{mechanism}: {error}"));
            if mechanism == "OAUTHBEARER" {
                ffi::oauth_set_token(
                    client.pin_mut(),
                    "opaque-token",
                    i64::MAX / 1000,
                    "user",
                    &[],
                )
                .unwrap();
            }
            drop(client);
        }
    }
}
