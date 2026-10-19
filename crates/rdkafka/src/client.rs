//! Producer and consumer handles with caller-thread operations.
use std::{cell::RefCell, collections::HashMap, rc::Rc};

use rdkafka_sys::ffi;

use crate::{
    core::{Core, from_parts, message, status},
    *,
};
/// A synchronous producer whose callbacks execute during caller polling.
pub struct Producer {
    pub(crate) core: Rc<Core>,
    counts: RefCell<HashMap<String, i32>>,
}
impl Producer {
    /// Creates a native producer.
    /// # Errors
    /// Returns invalid configuration or native initialization errors.
    pub fn new(conf: &Conf) -> Result<Self> {
        Ok(Self {
            core: Core::new(conf, 0)?,
            counts: RefCell::default(),
        })
    }
    /// Enqueues a record, preserving null keys and payloads.
    /// # Errors
    /// Returns metadata, partitioner, or native enqueue failures.
    pub fn produce(&self, record: ProduceRecord<'_>) -> Result<()> {
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
        let mut partition = record.partition;
        let has_partitioner = {
            let c = self.core.callbacks.borrow();
            c.partitioner.is_some() || c.key_partitioner.is_some()
        };
        if partition == PARTITION_UA && has_partitioner {
            let cached = self.counts.borrow().get(record.topic).copied();
            let count = if let Some(v) = cached {
                v
            } else {
                let m = self.metadata(Some(record.topic), 5000)?;
                let n = m
                    .topics
                    .iter()
                    .find(|t| t.name == record.topic)
                    .map_or(0, |t| t.partitions.len() as i32);
                if n <= 0 {
                    return Err(Error::native(3, "topic has no available partitions"));
                }
                self.counts.borrow_mut().insert(record.topic.into(), n);
                n
            };
            let callback = self.core.callbacks.borrow_mut().partitioner.take();
            if let Some(mut cb) = callback {
                partition = cb(record.topic, record.key, count);
                let mut c = self.core.callbacks.borrow_mut();
                if c.partitioner.is_none() {
                    c.partitioner = Some(cb);
                }
            } else {
                let callback = self.core.callbacks.borrow_mut().key_partitioner.take();
                if let Some(mut cb) = callback {
                    partition = cb(record.topic, record.key, count);
                    let mut c = self.core.callbacks.borrow_mut();
                    if c.key_partitioner.is_none() {
                        c.key_partitioner = Some(cb);
                    }
                }
            }
            if partition < 0 || partition >= count {
                return Err(Error::native(
                    -186,
                    "partitioner returned an unavailable partition",
                ));
            }
        }
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
    /// Returns bootstrap addresses of an explicitly configured native mock cluster.
    pub fn mock_bootstraps(&self) -> String {
        ffi::mock_bootstraps(&self.core.native.borrow())
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
        self.core.drain(0)?;
        status(code)
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
    /// Returns native message or callback errors.
    pub fn consume(&self, timeout_ms: i32) -> Result<Option<Message>> {
        let _guard = self.core.begin_drive()?;
        let m = message(ffi::consume(
            self.core.native.borrow_mut().pin_mut(),
            timeout_ms,
        ));
        self.core.drain(0)?;
        let m = m?;
        if let Some(v) = &m {
            self.core.observe(v);
        }
        Ok(m)
    }
    /// Consumes a native-owned message with borrowed key and payload slices.
    /// Local consume observers, when configured, receive an owned projection.
    /// # Errors
    /// Returns message or callback errors.
    pub fn consume_native(&self, timeout_ms: i32) -> Result<Option<ReceivedMessage>> {
        let _guard = self.core.begin_drive()?;
        let native = ffi::consume(self.core.native.borrow_mut().pin_mut(), timeout_ms);
        let message = ReceivedMessage::wrap(native, &self.core)?;
        self.core.drain(0)?;
        if let Some(m) = &message
            && self.core.callbacks.borrow().consume.is_some()
        {
            self.core.observe(&m.to_owned());
        }
        Ok(message)
    }
    /// Closes group membership synchronously.
    /// # Errors
    /// Returns native close or callback errors.
    pub fn close(&self) -> Result<()> {
        let _guard = self.core.begin_drive()?;
        let code = ffi::consumer_close(self.core.native.borrow_mut().pin_mut());
        self.core.drain(0)?;
        status(code)
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
    committed = 5,
    position = 6,
    seek = 10
);
impl Consumer {
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
        pub fn query_watermark_offsets(&self,topic:&str,partition:i32,timeout_ms:i32)->Result<(i64,i64)>{crate::core::text(topic)?;let v=ffi::client_watermarks(self.core.native.borrow_mut().pin_mut(),topic,partition,timeout_ms,false)?;match v.as_slice(){[lo,hi]=>Ok((*lo,*hi)),_=>Err(Error::Layout("watermark response must contain two offsets".into()))}}
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
        status(ffi::simple_seek(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
            offset.value(),
            timeout_ms,
        ))
    }
    /// Consumes an explicit partition.
    /// # Errors
    /// Returns native message or callback errors.
    pub fn consume(
        &self,
        topic: &Topic,
        partition: i32,
        timeout_ms: i32,
    ) -> Result<Option<Message>> {
        let _guard = self.core.begin_drive()?;
        self.check_topic(topic)?;
        let m = message(ffi::simple_consume(
            self.core.native.borrow_mut().pin_mut(),
            &topic.native,
            partition,
            timeout_ms,
        ))?;
        self.core.drain(0)?;
        if let Some(v) = &m {
            self.core.observe(v);
        }
        Ok(m)
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
