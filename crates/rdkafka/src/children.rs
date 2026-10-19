//! Native children retain the owning client until their destruction.
use std::{cell::RefCell, rc::Rc};

use rdkafka_sys::ffi;

use crate::{
    core::{Core, message},
    *,
};
pub struct Topic {
    pub(crate) callbacks: Rc<RefCell<crate::callbacks::Callbacks>>,
    pub(crate) native: cxx::UniquePtr<ffi::Topic>,
    pub(crate) core: Rc<Core>,
}
impl Topic {
    /// Creates a producer topic with topic-specific configuration.
    /// # Errors
    /// Returns invalid topic or configuration errors.
    pub fn new(producer: &Producer, name: &str, conf: &Conf) -> Result<Self> {
        Self::create(&producer.core, name, conf)
    }
    /// Creates a low-level consumer topic.
    /// # Errors
    /// Returns invalid topic or configuration errors.
    pub fn for_consumer(consumer: &SimpleConsumer, name: &str, conf: &Conf) -> Result<Self> {
        Self::create(&consumer.core, name, conf)
    }
    fn create(core: &Rc<Core>, name: &str, conf: &Conf) -> Result<Self> {
        crate::core::text(name)?;
        Ok(Self {
            callbacks: Rc::clone(&conf.callbacks),
            native: ffi::topic_new(core.native.borrow_mut().pin_mut(), name, &conf.native)?,
            core: Rc::clone(core),
        })
    }
    pub fn name(&self) -> String {
        ffi::topic_name(&self.native)
    }
    pub fn partition_available(&self, partition: i32) -> bool {
        ffi::topic_available(&self.native, partition)
    }
}
pub struct Queue {
    pub(crate) native: RefCell<cxx::UniquePtr<ffi::Queue>>,
    pub(crate) core: Rc<Core>,
    forwarded: RefCell<Option<Rc<Queue>>>,
}
impl Queue {
    /// Creates a queue attached to a low-level consumer.
    /// # Errors
    /// Returns native queue creation errors.
    pub fn new(consumer: &SimpleConsumer) -> Result<Self> {
        Self::create(&consumer.core, "", -1)
    }
    /// Retrieves a consumer partition queue.
    /// # Errors
    /// Returns an error if the partition queue is unavailable.
    pub fn partition(consumer: &Consumer, topic: &str, partition: i32) -> Result<Self> {
        Self::create(&consumer.core, topic, partition)
    }
    fn create(core: &Rc<Core>, topic: &str, partition: i32) -> Result<Self> {
        crate::core::text(topic)?;
        Ok(Self {
            native: RefCell::new(ffi::queue_new(
                core.native.borrow_mut().pin_mut(),
                topic,
                partition,
            )?),
            core: Rc::clone(core),
            forwarded: RefCell::default(),
        })
    }
    /// Forwards to a retained destination queue.
    /// # Errors
    /// Returns an error if forwarding would create an ownership cycle.
    pub fn forward(&self, destination: Rc<Self>) -> Result<()> {
        let mut next = Some(Rc::clone(&destination));
        while let Some(q) = next {
            if std::ptr::eq(self, Rc::as_ptr(&q)) {
                return Err(Error::Config("queue forwarding cycle".into()));
            }
            next = q.forwarded.borrow().clone();
        }
        ffi::queue_forward(
            self.native.borrow_mut().pin_mut(),
            &destination.native.borrow(),
        );
        *self.forwarded.borrow_mut() = Some(destination);
        Ok(())
    }
    pub fn unforward(&self) {
        ffi::queue_unforward(self.native.borrow_mut().pin_mut());
        self.forwarded.borrow_mut().take();
    }
    /// Polls queue events and calling-thread observers.
    /// # Errors
    /// Returns callback errors.
    pub fn poll(&self, timeout_ms: i32) -> Result<i32> {
        let _guard = self.core.begin_drive()?;
        let n = ffi::queue_poll(self.native.borrow_mut().pin_mut(), timeout_ms);
        self.core.drain(0)?;
        Ok(n)
    }
    /// Consumes one message.
    /// # Errors
    /// Returns message or callback errors.
    pub fn consume(&self, timeout_ms: i32) -> Result<Option<Message>> {
        let _guard = self.core.begin_drive()?;
        let m = message(ffi::queue_consume(
            self.native.borrow_mut().pin_mut(),
            timeout_ms,
        ))?;
        self.core.drain(0)?;
        if let Some(v) = &m {
            self.core.observe(v);
        }
        Ok(m)
    }
    /// Enables native readiness notifications on an owned file descriptor.
    /// The descriptor owner must keep it open until notifications are disabled.
    pub fn io_event_enable(&self, fd: i32, payload: &[u8]) {
        ffi::queue_io_event(self.native.borrow_mut().pin_mut(), fd, payload);
    }
}
