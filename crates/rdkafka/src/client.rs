//! Producer and consumer handles with caller-thread operations.
use std::rc::Rc;

use rdkafka_sys::ffi;

use crate::{
    core::{Core, from_parts, message, status},
    *,
};
/// An enqueue request using a topic name or an owned topic configuration.
pub enum ProduceRequest<'a> {
    Record(ProduceRecord<'a>),
    Topic(&'a Topic, ProduceRecord<'a>),
}
impl<'a> From<ProduceRecord<'a>> for ProduceRequest<'a> {
    fn from(record: ProduceRecord<'a>) -> Self {
        Self::Record(record)
    }
}
impl<'a> From<(&'a Topic, ProduceRecord<'a>)> for ProduceRequest<'a> {
    fn from((topic, record): (&'a Topic, ProduceRecord<'a>)) -> Self {
        Self::Topic(topic, record)
    }
}
/// A synchronous producer whose callbacks execute during caller polling.
pub struct Producer {
    pub(crate) core: Rc<Core>,
}
impl Producer {
    /// Creates a native producer.
    /// # Errors
    /// Returns invalid configuration or native initialization errors.
    pub fn new(conf: &Conf) -> Result<Self> {
        Ok(Self {
            core: Core::new(conf, 0)?,
        })
    }
    /// Enqueues a record, preserving null keys and payloads.
    /// # Errors
    /// Returns metadata, partitioner, or native enqueue failures.
    pub fn produce<'a>(&self, request: impl Into<ProduceRequest<'a>>) -> Result<()> {
        let record = match request.into() {
            ProduceRequest::Record(record) => record,
            ProduceRequest::Topic(topic, record) => return self.produce_to_topic(topic, record),
        };
        #[cfg(feature = "tracing")]
        tracing::trace!(
            topic = record.topic,
            partition = record.partition,
            "enqueue Kafka record"
        );
        crate::core::text(record.topic)?;
        if let Some(headers) = record.headers {
            for h in headers.iter() {
                crate::core::text(&h.name)?;
            }
        }
        let partition = self.resolve_partition(record.topic, record.key, record.partition)?;
        let headers: Vec<_> = record
            .headers
            .into_iter()
            .flat_map(Headers::iter)
            .map(|h| ffi::NativeHeader {
                name: h.name.clone(),
                value: h.value.clone().unwrap_or_default(),
                has_value: h.value.is_some(),
            })
            .collect();
        status(ffi::produce(
            self.core.native.borrow_mut().pin_mut(),
            record.topic,
            partition,
            record.key.unwrap_or_default(),
            record.key.is_some(),
            record.payload.unwrap_or_default(),
            record.payload.is_some(),
            &headers,
            record.timestamp.unwrap_or(0),
            record.token,
        ))
    }
    /// Enqueues through an owned topic's configuration and local partitioner.
    /// The topic handle determines the destination; `record.topic` is ignored.
    /// # Errors
    /// Returns ownership, metadata, partitioner, or native enqueue errors.
    pub fn produce_to_topic(&self, topic: &Topic, record: ProduceRecord<'_>) -> Result<()> {
        if !Rc::ptr_eq(&self.core, &topic.core) {
            return Err(Error::Config("topic belongs to another producer".into()));
        }
        let name = topic.name();
        let local = {
            let callbacks = topic.callbacks.borrow();
            callbacks.partitioner.is_some() || callbacks.key_partitioner.is_some()
        };
        let callbacks = if local {
            &topic.callbacks
        } else {
            &self.core.callbacks
        };
        let partition =
            self.resolve_partition_using(callbacks, &name, record.key, record.partition)?;
        let mut headers = Vec::new();
        for header in record.headers.into_iter().flat_map(Headers::iter) {
            crate::core::text(&header.name)?;
            headers.push(ffi::NativeHeader {
                name: header.name.clone(),
                value: header.value.clone().unwrap_or_default(),
                has_value: header.value.is_some(),
            });
        }
        status(ffi::produce_topic(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
            record.key.unwrap_or_default(),
            record.key.is_some(),
            record.payload.unwrap_or_default(),
            record.payload.is_some(),
            &headers,
            record.timestamp.unwrap_or(0),
            record.token,
        ))
    }
    /// Returns bootstrap addresses of an explicitly configured native mock cluster.
    pub fn mock_bootstraps(&self) -> String {
        ffi::mock_bootstraps(&self.core.native.borrow())
    }
    pub(crate) fn resolve_partition(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        partition: i32,
    ) -> Result<i32> {
        self.resolve_partition_using(&self.core.callbacks, topic, key, partition)
    }
    fn resolve_partition_using(
        &self,
        callbacks: &Rc<std::cell::RefCell<crate::callbacks::Callbacks>>,
        topic: &str,
        key: Option<&[u8]>,
        partition: i32,
    ) -> Result<i32> {
        crate::core::text(topic)?;
        let mut partition = partition;
        let has_partitioner = {
            let c = callbacks.borrow();
            c.partitioner.is_some() || c.key_partitioner.is_some()
        };
        if partition == PARTITION_UA && has_partitioner {
            let cached = self.core.partition_counts.borrow().get(topic).copied();
            let count = if let Some(count) = cached {
                count
            } else {
                self.metadata(Some(topic), 5000)?;
                self.core
                    .partition_counts
                    .borrow()
                    .get(topic)
                    .copied()
                    .unwrap_or(0)
            };
            if count <= 0 {
                return Err(Error::native(3, "topic has no available partitions"));
            }
            partition = crate::callbacks::with_callback(
                callbacks,
                |c| &mut c.partitioner,
                |cb| cb(topic, key, count),
            )
            .or_else(|| {
                crate::callbacks::with_callback(
                    callbacks,
                    |c| &mut c.key_partitioner,
                    |cb| cb(topic, key, count),
                )
            })
            .unwrap_or(partition);
            if partition < 0 || partition >= count {
                return Err(Error::native(
                    -186,
                    "partitioner returned an unavailable partition",
                ));
            }
        }
        Ok(partition)
    }
    /// Drains native callbacks on the calling thread.
    /// # Errors
    /// Returns callback failures.
    pub fn poll(&self, timeout_ms: i32) -> Result<usize> {
        self.core.poll(timeout_ms)
    }
    /// Waits for delivery completion and drains callbacks.
    /// # Errors
    /// Returns timeout or callback errors.
    pub fn flush(&self, timeout_ms: i32) -> Result<()> {
        let _guard = self.core.begin_drive()?;
        let code = ffi::client_flush(self.core.native.borrow_mut().pin_mut(), timeout_ms);
        self.core.complete(status(code))
    }
}
/// A native consumer-group client.
pub struct Consumer {
    pub(crate) core: Rc<Core>,
}
pub type KafkaConsumer = Consumer;
impl Consumer {
    /// Creates a group consumer.
    /// # Errors
    /// Returns configuration or initialization errors.
    pub fn new(conf: &Conf) -> Result<Self> {
        Ok(Self {
            core: Core::new(conf, 1)?,
        })
    }
    /// Subscribes to topics.
    /// # Errors
    /// Returns a native subscription error.
    pub fn subscribe(&self, topics: &[&str]) -> Result<()> {
        for topic in topics {
            crate::core::text(topic)?;
        }
        status(ffi::subscribe(
            self.core.native.borrow_mut().pin_mut(),
            &topics.iter().map(|t| (*t).into()).collect::<Vec<_>>(),
        ))
    }
    /// Removes the subscription.
    /// # Errors
    /// Returns a native unsubscribe error.
    pub fn unsubscribe(&self) -> Result<()> {
        status(ffi::unsubscribe(self.core.native.borrow_mut().pin_mut()))
    }
    /// Reads the current subscription.
    /// # Errors
    /// Returns a native query error.
    pub fn subscription(&self) -> Result<Vec<TopicPartition>> {
        Ok(from_parts(ffi::consumer_list(
            self.core.native.borrow_mut().pin_mut(),
            false,
        )?))
    }
    /// Reads assigned partitions.
    /// # Errors
    /// Returns a native query error.
    pub fn assignment(&self) -> Result<Vec<TopicPartition>> {
        Ok(from_parts(ffi::consumer_list(
            self.core.native.borrow_mut().pin_mut(),
            true,
        )?))
    }
    /// Consumes one message and invokes local observers.
    /// # Errors
    /// Returns native message errors. When a record was fetched, callback failures
    /// are deferred until the next poll or receive so that record is not discarded.
    pub fn consume(&self, timeout_ms: i32) -> Result<Option<Message>> {
        let _guard = self.core.begin_drive()?;
        self.core.take_pending_error()?;
        let m = message(ffi::consume(
            self.core.native.borrow_mut().pin_mut(),
            timeout_ms,
        ));
        if let Ok(Some(v)) = &m {
            self.core.observe(v);
        }
        self.core.complete_received(m, Option::is_some)
    }
    /// Consumes a native-owned message with borrowed key and payload slices.
    /// Local consume observers, when configured, receive an owned projection.
    /// # Errors
    /// Returns message errors; callback failures after fetching a record are
    /// deferred until the next poll or receive.
    pub fn consume_native(&self, timeout_ms: i32) -> Result<Option<ReceivedMessage>> {
        let _guard = self.core.begin_drive()?;
        self.core.take_pending_error()?;
        let native = ffi::consume(self.core.native.borrow_mut().pin_mut(), timeout_ms);
        let message = ReceivedMessage::wrap(native, &self.core);
        if let Ok(Some(m)) = &message
            && self.core.callbacks.borrow().consume.is_some()
        {
            self.core.observe(&m.to_owned());
        }
        self.core.complete_received(message, Option::is_some)
    }
    /// Closes group membership synchronously.
    /// # Errors
    /// Returns native close or callback errors.
    pub fn close(&self) -> Result<()> {
        let _guard = self.core.begin_drive()?;
        ffi::consumer_close_begin(self.core.native.borrow_mut().pin_mut())?;
        loop {
            let closed = ffi::consumer_close_poll(self.core.native.borrow_mut().pin_mut(), 100);
            self.core.drain(0)?;
            if closed {
                return Ok(());
            }
        }
    }
}
macro_rules! operations {
    ($($name:ident=$code:expr),*) => {impl Consumer {$ (
        /// Applies a native partition operation.
        /// # Errors
        /// Returns native operation errors, including per-partition failures.
        pub fn $name(&self,parts:&[TopicPartition])->Result<Vec<TopicPartition>>{self.core.operation($code,parts,5000)}
    )*}}
}
operations!(
    assign = 0,
    commit_sync = 2,
    commit_async = 3,
    offsets_store = 4,
    position = 6
);
impl Consumer {
    /// Queries committed offsets within the supplied timeout.
    /// # Errors
    /// Returns invalid timeout, native, or per-partition errors.
    pub fn committed(
        &self,
        parts: &[TopicPartition],
        timeout: std::time::Duration,
    ) -> Result<Vec<TopicPartition>> {
        self.core
            .operation(5, parts, crate::core::timeout(timeout)?)
    }
    /// Seeks assigned partitions within the supplied timeout.
    /// # Errors
    /// Returns invalid timeout, native, or per-partition errors.
    pub fn seek(
        &self,
        parts: &[TopicPartition],
        timeout: std::time::Duration,
    ) -> Result<Vec<TopicPartition>> {
        self.core
            .operation(10, parts, crate::core::timeout(timeout)?)
    }
    /// Clears explicit assignment.
    /// # Errors
    /// Returns a native assignment error.
    pub fn unassign(&self) -> Result<()> {
        self.core.operation(1, &[], 0)?;
        Ok(())
    }
}
macro_rules! common {
    ($($ty:ty),*)=>{$(impl $ty{
        /// Publishes a bearer token to the native authentication mechanism.
        /// # Errors
        /// Returns unsupported authentication, invalid argument, or native errors.
        pub fn oauthbearer_set_token(&self,token:&str,expiry_ms:i64,principal:&str,extensions:&[String])->Result<()> {
            for value in [token,principal] {crate::core::text(value)?;} for value in extensions{crate::core::text(value)?;}
            ffi::oauth_set_token(self.core.native.borrow_mut().pin_mut(),token,expiry_ms,principal,extensions)?; Ok(())
        }
        /// Records a failed token refresh.
        /// # Errors
        /// Returns invalid arguments or native authentication errors.
        pub fn oauthbearer_set_token_failure(&self,reason:&str)->Result<()> {
            crate::core::text(reason)?;
            status(ffi::oauth_set_failure(self.core.native.borrow_mut().pin_mut(),reason))
        }
        pub fn outq_len(&self)->usize {ffi::client_outq(&self.core.native.borrow()).max(0) as usize}
        pub fn name(&self)->String{ffi::client_name(&self.core.native.borrow())}
        /// Queries cluster metadata.
        /// # Errors
        /// Returns timeout or broker errors.
        pub fn metadata(&self,topic:Option<&str>,timeout_ms:i32)->Result<Metadata>{self.core.metadata(topic,timeout_ms)}
        /// Queries authoritative watermark offsets.
        /// # Errors
        /// Returns native query failures or malformed native responses.
        pub fn query_watermark_offsets(&self,topic:&str,partition:i32,timeout_ms:i32)->Result<(i64,i64)>{crate::core::text(topic)?;self.core.query(||{let v=ffi::client_watermarks(self.core.native.borrow_mut().pin_mut(),topic,partition,timeout_ms,false)?;match v.as_slice(){[lo,hi]=>Ok((*lo,*hi)),_=>Err(Error::Layout("watermark response must contain two offsets".into()))}})}
        /// Reads cached watermark offsets.
        /// # Errors
        /// Returns native query failures.
        pub fn get_watermark_offsets(&self,topic:&str,partition:i32)->Result<(i64,i64)>{crate::core::text(topic)?;let v=ffi::client_watermarks(self.core.native.borrow_mut().pin_mut(),topic,partition,0,true)?;match v.as_slice(){[lo,hi]=>Ok((*lo,*hi)),_=>Err(Error::Layout("watermark response must contain two offsets".into()))}}
        /// Finds offsets at timestamps supplied in each partition offset.
        /// # Errors
        /// Returns native lookup failures.
        pub fn offsets_for_times(&self,parts:&[TopicPartition],timeout_ms:i32)->Result<Vec<TopicPartition>>{self.core.operation(7,parts,timeout_ms)}
        /// Pauses partitions.
        /// # Errors
        /// Returns native pause errors.
        pub fn pause(&self,parts:&[TopicPartition])->Result<Vec<TopicPartition>>{self.core.operation(8,parts,0)}
        /// Resumes partitions.
        /// # Errors
        /// Returns native resume errors.
        pub fn resume(&self,parts:&[TopicPartition])->Result<Vec<TopicPartition>>{self.core.operation(9,parts,0)}
        pub fn cluster_id(&self,timeout_ms:i32)->String{ffi::client_id(self.core.native.borrow_mut().pin_mut(),0,timeout_ms)}
        pub fn member_id(&self)->String{ffi::client_id(self.core.native.borrow_mut().pin_mut(),1,0)}
        pub fn controller_id(&self,timeout_ms:i32)->i32{ffi::client_controller(self.core.native.borrow_mut().pin_mut(),timeout_ms)}
        pub fn fatal_error(&self)->Option<Error>{let e=ffi::client_fatal(self.core.native.borrow_mut().pin_mut());(e.code!=0).then(||Error::Native{code:ErrorCode(e.code),message:e.message,retriable:false,fatal:true,txn_requires_abort:false})}
    })*}
}
common!(Producer, Consumer, SimpleConsumer);
/// A low-level consumer without consumer-group membership.
pub struct SimpleConsumer {
    pub(crate) core: Rc<Core>,
}
impl SimpleConsumer {
    /// Creates a low-level consumer.
    /// # Errors
    /// Returns invalid configuration or initialization errors.
    pub fn new(conf: &Conf) -> Result<Self> {
        Ok(Self {
            core: Core::new(conf, 2)?,
        })
    }
    /// Starts an explicit partition.
    /// # Errors
    /// Returns a native start error.
    pub fn start(&self, topic: &Topic, partition: i32, offset: Offset) -> Result<()> {
        self.check_topic(topic)?;
        status(ffi::simple_start(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
            offset.value(),
        ))
    }
    /// Stops an explicit partition.
    /// # Errors
    /// Returns a native stop error.
    pub fn stop(&self, topic: &Topic, partition: i32) -> Result<()> {
        self.check_topic(topic)?;
        status(ffi::simple_stop(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
        ))
    }
    /// Seeks an active partition.
    /// # Errors
    /// Returns a native seek error.
    pub fn seek(
        &self,
        topic: &Topic,
        partition: i32,
        offset: Offset,
        timeout_ms: i32,
    ) -> Result<()> {
        self.check_topic(topic)?;
        self.core.query(|| {
            status(ffi::simple_seek(
                self.core.native.borrow_mut().pin_mut(),
                &topic.native,
                partition,
                offset.value(),
                timeout_ms,
            ))
        })
    }
    /// Consumes an explicit partition.
    /// # Errors
    /// Returns native message errors. When a record was fetched, callback failures
    /// are deferred until the next poll or receive so that record is not discarded.
    pub fn consume(
        &self,
        topic: &Topic,
        partition: i32,
        timeout_ms: i32,
    ) -> Result<Option<Message>> {
        let _guard = self.core.begin_drive()?;
        self.check_topic(topic)?;
        self.core.take_pending_error()?;
        let outcome = message(ffi::simple_consume(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
            timeout_ms,
        ));
        if let Ok(Some(v)) = &outcome {
            self.core.observe(v);
        }
        self.core.complete_received(outcome, Option::is_some)
    }
    fn check_topic(&self, topic: &Topic) -> Result<()> {
        if Rc::ptr_eq(&self.core, &topic.core) {
            Ok(())
        } else {
            Err(Error::Config("topic belongs to another client".into()))
        }
    }
    /// Starts a partition forwarded to a queue.
    /// # Errors
    /// Returns ownership mismatch or native start errors.
    pub fn start_queue(
        &self,
        topic: &Topic,
        partition: i32,
        offset: Offset,
        queue: &Queue,
    ) -> Result<()> {
        self.check_topic(topic)?;
        if !Rc::ptr_eq(&self.core, &queue.core) {
            return Err(Error::Config("queue belongs to another client".into()));
        }
        status(ffi::simple_start_queue(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
            offset.value(),
            &queue.native.borrow(),
        ))
    }
}
