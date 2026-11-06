use std::rc::Rc;

use rdkafka::{
    Result,
    threading::{
        ThreadLocalClient, ThreadLocalConsumer, ThreadLocalProducer, WorkerClient, WorkerConsumer,
        WorkerProducer, WorkerSlotLease,
    },
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

type LocalFactory = fn() -> Result<Rc<()>>;
type SlotFactory = fn(usize) -> Result<Rc<()>>;
assert_impl_all!(ThreadLocalClient<Rc<()>, LocalFactory>: Send, Sync);
assert_impl_all!(ThreadLocalProducer<Rc<()>, LocalFactory>: Send, Sync);
assert_impl_all!(ThreadLocalConsumer<Rc<()>, LocalFactory>: Send, Sync);
assert_impl_all!(WorkerClient<Rc<()>, SlotFactory>: Send, Sync);
assert_impl_all!(WorkerProducer<Rc<()>, SlotFactory>: Send, Sync);
assert_impl_all!(WorkerConsumer<Rc<()>, SlotFactory>: Send, Sync);

assert_not_impl_any!(WorkerSlotLease<()>: Send, Sync);

#[cfg(feature = "http")]
assert_not_impl_any!(rdkafka::HttpTransport: Send, Sync);
#[cfg(feature = "http")]
assert_not_impl_any!(rdkafka::OAuthBearerHttpClient: Send, Sync);
#[cfg(feature = "http")]
assert_not_impl_any!(rdkafka::SchemaRegistryClient: Send, Sync);
