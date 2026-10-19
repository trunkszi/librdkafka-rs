//! Verified broker TLS behind native socket descriptors.

#[cfg(unix)]
mod pump;
#[cfg(unix)]
mod transport;

use std::sync::Arc;

use rdkafka_sys::tls::BrokerTlsHooks;

#[cfg(not(unix))]
use crate::Error;
use crate::{Result, TlsSettings};

/// Build an isolated native-thread transport without capturing local observers.
pub(crate) fn new(settings: &TlsSettings) -> Result<Arc<dyn BrokerTlsHooks>> {
    #[cfg(unix)]
    {
        Ok(Arc::new(transport::BrokerTls::new(settings)?))
    }
    #[cfg(not(unix))]
    {
        let _ = settings;
        Err(Error::Tls("broker TLS requires Unix socket pairs".into()))
    }
}

#[cfg(all(test, unix))]
mod tests;
