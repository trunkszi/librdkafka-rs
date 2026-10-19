//! Vectorized native transitions and native-owned borrowed receive batches.
use std::rc::Rc;

use rdkafka_sys::{batch::ffi as native, ffi};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
    Batch, BufferPolicy, Consumer, Error, Headers, Producer, Record, Result, SerdeBridge,
    WireBatch, WireHeaders, WireRecord,
    core::{Core, status, text},
};

/// Independent enqueue outcomes in the same order as the input records.
/// A successful enqueue does not imply broker delivery or atomic batch commit.
#[derive(Debug)]
pub struct BatchProduceResult {
    /// Native enqueue success or failure for every submitted record.
    pub results: Vec<Result<()>>,
}
impl BatchProduceResult {
    /// Number of records accepted into the native producer queue.
    pub fn accepted(&self) -> usize {
        self.results.iter().filter(|result| result.is_ok()).count()
    }
    /// Whether all submitted records were accepted.
    pub fn all_accepted(&self) -> bool {
        self.results.iter().all(Result::is_ok)
    }
}
impl Producer {
    /// Encodes serde values into reusable caller buffers and submits one native batch.
    /// Replaces previous wire records; scratch is emptied with the default policy.
    /// # Errors
    /// Returns serialization or pre-enqueue validation errors. Native per-record
    /// enqueue errors remain in the result; no records enqueue on encoding failure.
    pub fn produce_serde_batch<T: Serialize>(
        &self,
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
    ) -> Result<BatchProduceResult> {
        batch.clear_with_policy(BufferPolicy::default());
        SerdeBridge::encode_batch_into(topic, values, batch, scratch)?;
        self.produce_wire_records(batch, None)
    }
    /// Encodes with a caller codec and submits one native batch using default retention.
    /// # Errors
    /// Returns codec or pre-enqueue validation errors before submitting any record.
    pub fn produce_batch_using<T>(
        &self,
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
        encode: impl FnMut(&T, &mut Vec<u8>) -> Result<()>,
    ) -> Result<BatchProduceResult> {
        self.produce_batch_using_with_policy(
            topic,
            values,
            batch,
            scratch,
            BufferPolicy::default(),
            encode,
        )
    }
    /// Encodes with a caller codec, bounding retained scratch and prior wire capacity.
    /// Previous wire records are cleared with `policy` before encoding. Encoded
    /// records remain in `batch` for inspection, including a prefix on codec errors.
    /// Scratch is emptied and trimmed on success, errors, and codec unwinding.
    /// # Errors
    /// Returns codec or validation errors before enqueue; native per-record enqueue
    /// failures are retained in the result without rolling back accepted records.
    pub fn produce_batch_using_with_policy<T>(
        &self,
        topic: &str,
        values: &[T],
        batch: &mut WireBatch,
        scratch: &mut Vec<u8>,
        policy: BufferPolicy,
        encode: impl FnMut(&T, &mut Vec<u8>) -> Result<()>,
    ) -> Result<BatchProduceResult> {
        batch.clear_with_policy(policy);
        SerdeBridge::encode_batch_into_using_with_policy(
            topic, values, batch, scratch, policy, encode,
        )?;
        self.produce_wire_records(batch, None)
    }

    /// Encodes a typed column batch and submits it in one native bridge call.
    /// # Errors
    /// Returns codec, invalid metadata, or caller-thread partitioner errors before
    /// enqueue. Native per-record enqueue failures are retained in the result.
    pub fn produce_batch<B: Batch>(&self, topic: &str, batch: &B) -> Result<BatchProduceResult> {
        self.produce_wire_records(&batch.to_wire_batch()?, Some(topic))
    }
    /// Submits wire descriptors in one native bridge call using produceva.
    /// Each record is enqueued independently; accepted records are not rolled
    /// back when another record fails. Keys and payloads are copied by librdkafka.
    /// # Errors
    /// Returns invalid metadata or partitioner errors before any enqueue occurs.
    pub fn produce_wire_batch(&self, batch: &WireBatch) -> Result<BatchProduceResult> {
        self.produce_wire_records(batch, None)
    }
    fn produce_wire_records(
        &self,
        batch: &WireBatch,
        topic: Option<&str>,
    ) -> Result<BatchProduceResult> {
        let records = batch
            .iter()
            .map(|record| {
                let topic = topic.unwrap_or(record.topic);
                text(topic)?;
                for header in record.headers.iter() {
                    text(header.name)?;
                }
                Ok(native::BatchRecord {
                    topic,
                    partition: self.resolve_partition(topic, record.key, record.partition)?,
                    key: record.key.unwrap_or_default(),
                    has_key: record.key.is_some(),
                    payload: record.payload.unwrap_or_default(),
                    has_payload: record.payload.is_some(),
                    timestamp: record.timestamp.unwrap_or(0),
                    headers: record
                        .headers
                        .iter()
                        .map(|header| ffi::NativeHeader {
                            name: header.name.into(),
                            value: header.value.unwrap_or_default().to_vec(),
                            has_value: header.value.is_some(),
                        })
                        .collect(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let results = native::produce_records(self.core.native.borrow_mut().pin_mut(), &records)
            .into_iter()
            .map(status)
            .collect();
        Ok(BatchProduceResult { results })
    }
}
struct ReceivedEntry {
    native: cxx::UniquePtr<ffi::Message>,
    topic: String,
    headers: Headers,
}
/// Owns native receive buffers; projected key and payload slices borrow those
/// buffers directly. Header metadata is copied once when the batch is received.
/// Keeping this batch alive also keeps its originating consumer alive.
pub struct ReceivedBatch {
    entries: Vec<ReceivedEntry>,
    _owner: Rc<Core>,
}
impl ReceivedBatch {
    /// Decodes native payloads using a caller codec, borrowing from this batch.
    /// Earlier visitor effects remain on a later failure; native buffers stay owned.
    /// # Errors
    /// Returns native message, null payload, codec, or visitor errors unchanged.
    pub fn decode_with_using<'de, T>(
        &'de self,
        mut decode: impl FnMut(&'de [u8]) -> Result<T>,
        mut on_value: impl FnMut(T) -> Result<()>,
    ) -> Result<usize> {
        for record in self.iter() {
            let payload = record?
                .payload
                .ok_or_else(|| Error::Codec("cannot decode a null payload".into()))?;
            on_value(SerdeBridge::decode_borrowed_using(payload, &mut decode)?)?;
        }
        Ok(self.len())
    }
    /// Appends decoded values, permitting values borrowed from this native batch.
    /// # Errors
    /// Returns native, null payload, or codec errors; earlier appends remain.
    pub fn decode_into_using<'de, T>(
        &'de self,
        output: &mut Vec<T>,
        decode: impl FnMut(&'de [u8]) -> Result<T>,
    ) -> Result<usize> {
        self.decode_with_using(decode, |value| {
            output.push(value);
            Ok(())
        })
    }
    /// Fills existing slots, retaining later slots and rejecting short slices atomically.
    /// # Errors
    /// Returns length mismatch before decoding, or native/null/codec errors after
    /// retaining successfully overwritten earlier slots.
    pub fn decode_into_slice_using<'de, T>(
        &'de self,
        output: &mut [T],
        decode: impl FnMut(&'de [u8]) -> Result<T>,
    ) -> Result<usize> {
        if output.len() < self.len() {
            return Err(Error::BatchLengthMismatch {
                expected: self.len(),
                actual: output.len(),
            });
        }
        let mut index = 0;
        self.decode_with_using(decode, |value| {
            output[index] = value;
            index += 1;
            Ok(())
        })
    }

    /// Number of received records, including native error records.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    /// Whether the native receive returned no records.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Projects one native message, preserving nullable bytes and duplicate headers.
    /// # Errors
    /// Returns the native message error for an error record.
    pub fn get(&self, index: usize) -> Result<Option<WireRecord<'_>>> {
        let Some(entry) = self.entries.get(index) else {
            return Ok(None);
        };
        status(ffi::message_error(&entry.native))?;
        let mut record = WireRecord::new(
            &entry.topic,
            ffi::message_has_payload(&entry.native).then(|| ffi::message_payload(&entry.native)),
        )
        .with_key(ffi::message_has_key(&entry.native).then(|| ffi::message_key(&entry.native)))
        .with_partition(ffi::message_partition(&entry.native))
        .with_offset(ffi::message_offset(&entry.native));
        let timestamp = ffi::message_timestamp(&entry.native);
        record.timestamp = (timestamp >= 0).then_some(timestamp);
        record.headers = WireHeaders::from_headers(&entry.headers);
        Ok(Some(record))
    }
    /// Iterates native records without copying payloads.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = Result<WireRecord<'_>>> {
        self.entries.iter().enumerate().map(|(index, _)| {
            self.get(index)?
                .ok_or_else(|| Error::Layout("native batch index disappeared".into()))
        })
    }
    /// Visits records starting at `start` while retaining all native buffers.
    /// On failure this batch remains owned by the caller, allowing inspection or
    /// retry from the failed index. Successful visitor side effects are not undone.
    /// # Errors
    /// Returns invalid start, native message, or visitor errors.
    pub fn visit_from(
        &self,
        start: usize,
        mut visit: impl FnMut(usize, WireRecord<'_>) -> Result<()>,
    ) -> Result<usize> {
        if start > self.len() {
            return Err(Error::Layout("batch start exceeds record count".into()));
        }
        for index in start..self.len() {
            let record = self
                .get(index)?
                .ok_or_else(|| Error::Layout("native batch index disappeared".into()))?;
            visit(index, record)?;
        }
        Ok(self.len() - start)
    }
    /// Decodes native records into a typed SoA destination in receive order.
    /// Successfully decoded preceding records remain appended on a later failure.
    /// # Errors
    /// Returns native message or schema decoding errors.
    pub fn decode_into<B: Batch>(&self, destination: &mut B) -> Result<()> {
        for record in self.iter() {
            destination.push_ref(B::Record::view(record?)?)?;
        }
        Ok(())
    }
}
impl Consumer {
    /// Receives one native batch and visits serde values without a staging vector.
    /// # Errors
    /// Returns receive, native, null payload, codec, or visitor errors. The whole
    /// fetched batch is consumed even if decoding or visiting fails.
    pub fn consume_serde_with<T: DeserializeOwned>(
        &self,
        maximum: usize,
        timeout_ms: i32,
        on_value: impl FnMut(T) -> Result<()>,
    ) -> Result<usize> {
        self.consume_with_using(
            maximum,
            timeout_ms,
            |payload| SerdeBridge::decode_borrowed::<T>(payload),
            on_value,
        )
    }
    /// Receives and decodes a native batch with caller codecs and visitors.
    /// `T` cannot borrow from temporary receive storage; retain a `ReceivedBatch`
    /// and use its decoding methods when borrowed output must outlive this call.
    /// # Errors
    /// Returns receive, native, null payload, codec, or visitor errors; fetched
    /// records are consumed even on failure, including unvisited records.
    pub fn consume_with_using<T>(
        &self,
        maximum: usize,
        timeout_ms: i32,
        decode: impl for<'de> FnMut(&'de [u8]) -> Result<T>,
        on_value: impl FnMut(T) -> Result<()>,
    ) -> Result<usize> {
        self.consume_batch(maximum, timeout_ms)?
            .decode_with_using(decode, on_value)
    }
    /// Appends caller-decoded values from one native receive operation.
    /// # Errors
    /// Returns receive/native/null/codec errors, preserving earlier appends.
    /// The entire fetched batch is consumed on failure.
    pub fn consume_into_using<T>(
        &self,
        maximum: usize,
        timeout_ms: i32,
        output: &mut Vec<T>,
        decode: impl for<'de> FnMut(&'de [u8]) -> Result<T>,
    ) -> Result<usize> {
        self.consume_batch(maximum, timeout_ms)?
            .decode_into_using(output, decode)
    }
    /// Fills caller slots from one native batch, without intermediate decoded storage.
    /// # Errors
    /// Returns receive/native/null/codec or length errors. Too few output slots
    /// leaves the output unchanged but the fetched batch is already consumed.
    pub fn consume_into_slice_using<T>(
        &self,
        maximum: usize,
        timeout_ms: i32,
        output: &mut [T],
        decode: impl for<'de> FnMut(&'de [u8]) -> Result<T>,
    ) -> Result<usize> {
        self.consume_batch(maximum, timeout_ms)?
            .decode_into_slice_using(output, decode)
    }

    /// Receives at most `maximum` messages with one native batch queue operation.
    /// Local consume observers run on this thread after the native borrow ends.
    /// # Errors
    /// Returns reentrant drive, queue, callback or oversized receive errors.
    /// Native per-message errors remain inspectable through the returned batch.
    pub fn consume_batch(&self, maximum: usize, timeout_ms: i32) -> Result<ReceivedBatch> {
        if maximum > i32::MAX as usize {
            return Err(Error::Layout(
                "batch size exceeds native receive limit".into(),
            ));
        }
        let _guard = self.core.begin_drive()?;
        let outcome =
            native::consume_records(self.core.native.borrow_mut().pin_mut(), maximum, timeout_ms)
                .map_err(Error::from);
        let mut received = self.core.complete(outcome)?;
        let mut entries = Vec::with_capacity(native::batch_count(&received));
        for index in 0..native::batch_count(&received) {
            let message = native::batch_take(received.pin_mut(), index);
            let mut headers = Headers::new();
            for header in ffi::message_headers(&message) {
                headers.add(
                    header.name,
                    header.has_value.then_some(header.value.as_slice()),
                );
            }
            entries.push(ReceivedEntry {
                topic: ffi::message_topic(&message),
                headers,
                native: message,
            });
        }
        let batch = ReceivedBatch {
            entries,
            _owner: Rc::clone(&self.core),
        };
        if self.core.callbacks.borrow().consume.is_some() {
            for record in batch.iter() {
                // Error records remain available to the caller in the batch.
                let Ok(record) = record else { continue };
                let mut headers = Headers::new();
                for header in record.headers.iter() {
                    headers.add(header.name, header.value);
                }
                self.core.observe(&crate::Message {
                    topic: record.topic.into(),
                    key: record.key.map(<[u8]>::to_vec),
                    payload: record.payload.map(<[u8]>::to_vec),
                    headers,
                    partition: record.partition,
                    offset: record.offset,
                    timestamp: record.timestamp,
                    token: 0,
                });
            }
        }
        Ok(batch)
    }
    /// Visits borrowed typed views from one native receive operation.
    /// # Errors
    /// Returns receive, schema, or visitor errors. The entire fetched batch is consumed
    /// even on failure, including unvisited records. Use `consume_batch` and
    /// `ReceivedBatch::visit_from` to retain buffers for inspection or retry, and
    /// disable automatic offset storage when acknowledging only processed records.
    pub fn consume_views<R: Record>(
        &self,
        maximum: usize,
        timeout_ms: i32,
        mut visit: impl for<'a> FnMut(R::View<'a>) -> Result<()>,
    ) -> Result<usize> {
        self.consume_for_each(maximum, timeout_ms, |record| visit(R::view(record)?))
    }
    /// Visits borrowed wire records from one native receive operation.
    /// # Errors
    /// Returns receive or visitor errors. The entire fetched batch is consumed even
    /// on failure. Use `consume_batch` and `ReceivedBatch::visit_from` to retain
    /// unvisited records; disable automatic offset storage for manual processing.
    pub fn consume_for_each(
        &self,
        maximum: usize,
        timeout_ms: i32,
        mut visit: impl FnMut(WireRecord<'_>) -> Result<()>,
    ) -> Result<usize> {
        let batch = self.consume_batch(maximum, timeout_ms)?;
        for record in batch.iter() {
            visit(record?)?;
        }
        Ok(batch.len())
    }
}
