//! Native global and topic configuration with local callback ownership.
use std::{cell::RefCell, rc::Rc};

use rdkafka_sys::ffi;

use crate::{Result, callbacks::*};
/// Configuration and callback registrations remain on their creating thread.
pub struct Conf {
    pub(crate) native: cxx::UniquePtr<ffi::Conf>,
    pub(crate) callbacks: Rc<RefCell<Callbacks>>,
    hooks: rdkafka_sys::NativeHooks,
}
impl Default for Conf {
    fn default() -> Self {
        Self::new()
    }
}
impl Conf {
    pub fn new() -> Self {
        Self {
            native: ffi::conf_new(false),
            callbacks: Rc::default(),
            hooks: Default::default(),
        }
    }
    pub fn topic() -> Self {
        Self {
            native: ffi::conf_new(true),
            callbacks: Rc::default(),
            hooks: Default::default(),
        }
    }
    /// Sets a native property.
    /// # Errors
    /// Returns an error for an unknown property or invalid value.
    pub fn set(&mut self, key: &str, value: &str) -> Result<&mut Self> {
        crate::core::text(key)?;
        crate::core::text(value)?;
        ffi::conf_set(self.native.pin_mut(), key, value)
            .map_err(|e| crate::Error::Config(e.to_string()))?;
        Ok(self)
    }
    /// Reads a native property.
    /// # Errors
    /// Returns an error if the property does not exist in this configuration.
    pub fn get(&self, key: &str) -> Result<String> {
        crate::core::text(key)?;
        Ok(ffi::conf_get(&self.native, key)?)
    }
    /// Installs a native broker-thread hook; state must be thread safe.
    pub fn socket_hook(
        &mut self,
        hook: impl Fn(i32, i32, i32) -> i32 + Send + Sync + 'static,
    ) -> &mut Self {
        self.hooks.socket = Some(std::sync::Arc::new(hook));
        ffi::conf_set_hooks(self.native.pin_mut(), Box::new(self.hooks.clone()));
        self
    }
    /// Installs a native broker-thread file hook; state must be thread safe.
    pub fn open_hook(
        &mut self,
        hook: impl Fn(&str, i32, i32) -> i32 + Send + Sync + 'static,
    ) -> &mut Self {
        self.hooks.open = Some(std::sync::Arc::new(hook));
        ffi::conf_set_hooks(self.native.pin_mut(), Box::new(self.hooks.clone()));
        self
    }
    pub fn dump(&self) -> Vec<(String, String)> {
        ffi::conf_dump(&self.native)
            .into_iter()
            .map(|p| (p.name, p.value))
            .collect()
    }
    pub fn delivery_report_cb(&mut self, cb: impl FnMut(&DeliveryReport) + 'static) -> &mut Self {
        self.callbacks.borrow_mut().delivery = Some(Box::new(cb));
        self
    }
    pub fn event_cb(&mut self, cb: impl FnMut(&Event) + 'static) -> &mut Self {
        self.callbacks.borrow_mut().event = Some(Box::new(cb));
        self
    }
    pub fn consume_cb(&mut self, cb: impl FnMut(&crate::Message) + 'static) -> &mut Self {
        self.callbacks.borrow_mut().consume = Some(Box::new(cb));
        ffi::conf_set_observers(
            self.native.pin_mut(),
            true,
            self.callbacks.borrow().rebalance.is_some(),
        );
        self
    }
    /// Registers a caller-thread rebalance decision callback.
    /// The callback must apply assignment or revocation explicitly; automatic
    /// native assignment is used only when no callback was registered.
    pub fn rebalance_cb(
        &mut self,
        cb: impl FnMut(i32, &[crate::TopicPartition]) + 'static,
    ) -> &mut Self {
        self.callbacks.borrow_mut().rebalance = Some(Box::new(cb));
        ffi::conf_set_observers(
            self.native.pin_mut(),
            self.callbacks.borrow().consume.is_some(),
            true,
        );
        self
    }
    pub fn offset_commit_cb(
        &mut self,
        cb: impl FnMut(i32, &[crate::TopicPartition]) + 'static,
    ) -> &mut Self {
        self.callbacks.borrow_mut().commit = Some(Box::new(cb));
        self
    }
    pub fn partitioner_cb(
        &mut self,
        cb: impl FnMut(&str, Option<&[u8]>, i32) -> i32 + 'static,
    ) -> &mut Self {
        self.callbacks.borrow_mut().partitioner = Some(Box::new(cb));
        self
    }
    pub fn partitioner_key_pointer_cb(
        &mut self,
        cb: impl FnMut(&str, Option<&[u8]>, i32) -> i32 + 'static,
    ) -> &mut Self {
        self.callbacks.borrow_mut().key_partitioner = Some(Box::new(cb));
        self
    }
    pub fn socket_cb(&mut self, cb: impl FnMut(i32, i32, i32) + 'static) -> &mut Self {
        self.callbacks.borrow_mut().socket = Some(Box::new(cb));
        self
    }
    pub fn open_cb(&mut self, cb: impl FnMut(&str, i32, i32) + 'static) -> &mut Self {
        self.callbacks.borrow_mut().open = Some(Box::new(cb));
        self
    }
    pub fn oauthbearer_token_refresh_cb(
        &mut self,
        cb: impl FnMut(&str) -> Result<()> + 'static,
    ) -> &mut Self {
        self.callbacks.borrow_mut().oauth = Some(Box::new(cb));
        self
    }
}
