//! Native global and topic configuration with local callback ownership.
use std::{cell::RefCell, rc::Rc};

use rdkafka_sys::ffi;

use crate::{Result, callbacks::*};
/// Configuration and callback registrations remain on their creating thread.
pub struct Conf {
    pub(crate) native: cxx::UniquePtr<ffi::Conf>,
    pub(crate) callbacks: Rc<RefCell<Callbacks>>,
    hooks: rdkafka_sys::NativeHooks,
    security_protocol: Option<String>,
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
            security_protocol: None,
        }
    }
    pub fn topic() -> Self {
        Self {
            native: ffi::conf_new(true),
            callbacks: Rc::default(),
            hooks: Default::default(),
            security_protocol: None,
        }
    }
    /// Sets a native property.
    /// # Errors
    /// Returns an error for an unknown property or invalid value.
    pub fn set(&mut self, key: &str, value: &str) -> Result<&mut Self> {
        crate::core::text(key)?;
        crate::core::text(value)?;
        if key == "security.protocol" {
            let protocol = value.to_ascii_uppercase();
            let native_value = if matches!(protocol.as_str(), "SSL" | "SASL_SSL") {
                #[cfg(not(feature = "tls"))]
                return Err(crate::Error::Tls(
                    "broker TLS requires the tls feature".into(),
                ));
                #[cfg(feature = "tls")]
                {
                    if protocol == "SSL" {
                        "plaintext"
                    } else {
                        "sasl_plaintext"
                    }
                }
            } else {
                if self.hooks.tls.is_some() {
                    return Err(crate::Error::Tls(
                        "TLS settings require SSL or SASL_SSL".into(),
                    ));
                }
                value
            };
            ffi::conf_set(self.native.pin_mut(), key, native_value).map_err(crate::Error::from)?;
            self.security_protocol = Some(protocol);
            return Ok(self);
        }
        ffi::conf_set(self.native.pin_mut(), key, value)
            .map_err(|e| crate::Error::Config(e.to_string()))?;
        Ok(self)
    }
    /// Configure verified broker TLS. Defaults to SSL when no protocol was set.
    /// # Errors
    /// Returns invalid trust, identity, or incompatible security protocol errors.
    #[cfg(feature = "tls")]
    pub fn tls_settings(&mut self, settings: crate::TlsSettings) -> Result<&mut Self> {
        let transport = crate::broker_tls::new(&settings)?;
        if let Some(protocol) = &self.security_protocol {
            if !matches!(protocol.as_str(), "SSL" | "SASL_SSL") {
                return Err(crate::Error::Tls(
                    "TLS settings require SSL or SASL_SSL".into(),
                ));
            }
        } else {
            self.set("security.protocol", "SSL")?;
        }
        self.hooks.tls = Some(transport);
        ffi::conf_set_hooks(self.native.pin_mut(), Box::new(self.hooks.clone()));
        Ok(self)
    }
    pub(crate) fn validate_transport(&self) -> Result<()> {
        if self
            .security_protocol
            .as_deref()
            .is_some_and(|p| matches!(p, "SSL" | "SASL_SSL"))
            && self.hooks.tls.is_none()
        {
            return Err(crate::Error::Tls(
                "SSL and SASL_SSL require verified TlsSettings".into(),
            ));
        }
        Ok(())
    }
    /// Reads a native property.
    /// # Errors
    /// Returns an error if the property does not exist in this configuration.
    pub fn get(&self, key: &str) -> Result<String> {
        crate::core::text(key)?;
        if key == "security.protocol"
            && let Some(protocol) = &self.security_protocol
        {
            return Ok(protocol.to_ascii_lowercase());
        }
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
            .map(|p| {
                let value = if p.name == "security.protocol" {
                    self.security_protocol
                        .as_ref()
                        .map_or(p.value.clone(), |v| v.to_ascii_lowercase())
                } else {
                    p.value
                };
                (p.name, value)
            })
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
