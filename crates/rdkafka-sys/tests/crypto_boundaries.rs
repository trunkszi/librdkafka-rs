use std::ptr;

use rdkafka_sys::ffi;

unsafe extern "C" {
    fn rdkafka_rs_base64(out: *mut u8, data: *const u8, len: i32) -> i32;
    fn rdkafka_rs_base64_decode(out: *mut u8, data: *const u8, len: usize) -> i32;
    fn rdkafka_rs_digest(kind: u32, data: *const u8, len: usize, out: *mut u8) -> i32;
}

#[test]
fn base64_respects_native_buffer_bounds() -> Result<(), Box<dyn std::error::Error>> {
    let conf = ffi::conf_new(false);
    assert!(!ffi::conf_dump(&conf).is_empty());
    for len in 0usize..=128 {
        let input: Vec<_> = (0..len).map(|n| (n * 137) as u8).collect();
        let encoded_len = len.div_ceil(3) * 4;
        let mut encoded = vec![0xa5; encoded_len + 3];
        // SAFETY: output includes the exact encoded extent plus NUL, bracketed by canaries.
        let written =
            unsafe { rdkafka_rs_base64(encoded.as_mut_ptr().add(1), input.as_ptr(), len as i32) };
        assert_eq!(written as usize, encoded_len);
        assert_eq!(
            (
                encoded[0],
                encoded[encoded_len + 1],
                encoded[encoded_len + 2]
            ),
            (0xa5, 0, 0xa5)
        );
        let mut decoded = vec![0xa5; encoded_len / 4 * 3 + 3];
        // SAFETY: encoded is readable for its extent; output has the documented decode size and
        // NUL.
        let read = unsafe {
            rdkafka_rs_base64_decode(
                decoded.as_mut_ptr().add(1),
                encoded.as_ptr().add(1),
                encoded_len,
            )
        };
        assert_eq!(read as usize, len);
        assert_eq!(&decoded[1..len + 1], &input);
        assert_eq!(decoded[0], 0xa5);
        assert_eq!(*decoded.last().ok_or("canary")?, 0xa5);
        assert_eq!(decoded[len + 1], 0);
    }
    Ok(())
}

#[test]
fn invalid_crypto_input_keeps_output_canaries() {
    let mut out = [0xa5; 66];
    // SAFETY: invalid pointer/length combinations are rejected; valid output has the largest digest
    // size.
    unsafe {
        assert_eq!(
            rdkafka_rs_base64(out.as_mut_ptr().add(1), ptr::null(), -1),
            -1
        );
        assert_eq!(
            rdkafka_rs_base64_decode(out.as_mut_ptr().add(1), ptr::null(), 1),
            -1
        );
        assert_eq!(
            rdkafka_rs_digest(256, ptr::null(), 1, out.as_mut_ptr().add(1)),
            -1
        );
        assert_eq!(
            rdkafka_rs_digest(999, ptr::null(), 0, out.as_mut_ptr().add(1)),
            -1
        );
    }
    assert!(out.iter().all(|b| *b == 0xa5));
    // SAFETY: empty input is permitted and the writable output has 64 bytes with canaries outside
    // it.
    let result = unsafe { rdkafka_rs_digest(256, ptr::null(), 0, out.as_mut_ptr().add(1)) };
    assert_eq!(result, 0);
    assert_eq!(out[0], 0xa5);
    assert!(out[33..].iter().all(|b| *b == 0xa5));
}
