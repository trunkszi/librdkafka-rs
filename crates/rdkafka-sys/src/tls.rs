//! Thread-safe broker transport hooks, independent of caller-thread observers.

/// Internal transport callbacks invoked on native broker threads.
///
/// Implementations must never capture caller-local observers or client handles.
pub trait BrokerTlsHooks: Send + Sync {
    /// Create an independent connection registry sharing immutable TLS trust.
    fn fork(&self) -> std::sync::Arc<dyn BrokerTlsHooks>;
    /// Connect the supplied native descriptor to a verified TLS bridge.
    /// Return zero on success, otherwise an operating-system error number.
    fn connect(&self, fd: i32, address: &str, port: u16, name: &str) -> i32;
    /// Close the native descriptor and release its bridge resources.
    fn close(&self, fd: i32) -> i32;
    /// Drain transport failures for delivery on the client's calling thread.
    fn errors(&self) -> Vec<String>;
}
