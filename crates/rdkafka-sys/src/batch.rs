//! One bridge transition per producer or consumer batch.
#[cxx::bridge(namespace = "kafka_bridge")]
pub mod ffi {
    struct BatchRecord<'a> {
        topic: &'a str,
        partition: i32,
        key: &'a [u8],
        has_key: bool,
        payload: &'a [u8],
        has_payload: bool,
        timestamp: i64,
        headers: Vec<NativeHeader>,
    }
    unsafe extern "C++" {
        include!("rdkafka-sys/src/batch.h");
        type Client = crate::ffi::Client;
        type Message = crate::ffi::Message;
        type NativeHeader = crate::ffi::NativeHeader;
        type BatchMessages;
        /// Attempts every record and returns one native error code per input.
        fn produce_records(client: Pin<&mut Client>, records: &[BatchRecord<'_>]) -> Vec<i32>;
        /// # Errors
        /// Returns queue initialization or native batch receive errors.
        fn consume_records(
            client: Pin<&mut Client>,
            maximum: usize,
            timeout: i32,
        ) -> Result<UniquePtr<BatchMessages>>;
        fn batch_count(messages: &BatchMessages) -> usize;
        fn batch_take(messages: Pin<&mut BatchMessages>, index: usize) -> UniquePtr<Message>;
    }
}
