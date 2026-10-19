use std::rc::Rc;

use rdkafka::{
    Result,
    threading::{
        ThreadLocalClient, ThreadLocalConsumer, ThreadLocalFactory, ThreadLocalProducer,
        WorkerClient, WorkerConsumer, WorkerFactory, WorkerProducer,
    },
};
use static_assertions::{assert_impl_all, assert_not_impl_any};

type LocalFactory = fn() -> Result<Rc<()>>;
type SlotFactory = fn(usize) -> Result<Rc<()>>;
assert_not_impl_any!(ThreadLocalClient<Rc<()>, LocalFactory>: Send, Sync);
assert_not_impl_any!(ThreadLocalProducer<(), fn() -> Result<()>>: Send, Sync);
assert_not_impl_any!(ThreadLocalConsumer<(), fn() -> Result<()>>: Send, Sync);
assert_not_impl_any!(WorkerClient<Rc<()>, SlotFactory>: Send, Sync);
assert_not_impl_any!(WorkerProducer<(), fn(usize) -> Result<()>>: Send, Sync);
assert_not_impl_any!(WorkerConsumer<(), fn(usize) -> Result<()>>: Send, Sync);
assert_impl_all!(ThreadLocalFactory<Rc<()>, LocalFactory>: Send, Sync);
assert_impl_all!(WorkerFactory<Rc<()>, SlotFactory>: Send, Sync);

#[cfg(feature = "http")]
assert_not_impl_any!(rdkafka::HttpTransport: Send, Sync);
#[cfg(feature = "http")]
assert_not_impl_any!(rdkafka::OAuthBearerHttpClient: Send, Sync);
#[cfg(feature = "http")]
assert_not_impl_any!(rdkafka::SchemaRegistryClient: Send, Sync);
