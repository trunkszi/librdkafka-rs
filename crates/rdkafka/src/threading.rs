//! Synchronous local client caching and in-thread worker slot selection.
//!
//! Factories may share immutable configuration across OS threads. Their `local`
//! methods create strictly thread-affine managers; clients never leave TLS.
use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::HashMap,
    marker::PhantomData,
    rc::{Rc, Weak},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use crate::{Error, Result};

/// Routing policy for slots numbered `0..workers` on the calling thread.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WorkerBalance {
    /// Rotate through available slots.
    #[default]
    RoundRobin,
    /// Select the fewest active calls, rotating ties.
    LeastInFlight,
    /// FNV-1a hash of key bytes modulo the slot count; unkeyed calls rotate.
    KeyHash,
}

/// Counts for one local slot. Completion includes errors and unwinding callbacks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkerMetrics {
    pub dispatched: u64,
    pub completed: u64,
    pub in_flight: usize,
}

static NEXT_ID: AtomicU64 = AtomicU64::new(1);
thread_local! {
    static LOCAL_MANAGERS: RefCell<HashMap<usize, Box<dyn Any>>> = RefCell::new(HashMap::new());
    static CLIENTS: RefCell<HashMap<u64, Option<Box<dyn Any>>>> = RefCell::new(HashMap::new());
}

fn worker_error(message: &str) -> Error {
    Error::Worker(message.into())
}

struct Slot<T> {
    client: RefCell<Option<T>>,
    dispatched: Cell<u64>,
    completed: Cell<u64>,
    active: Cell<usize>,
}
struct Pool<T> {
    slots: Vec<Slot<T>>,
    next: Cell<usize>,
}

struct LocalInner<T> {
    id: u64,
    factory_key: Cell<Option<usize>>,
    count: usize,
    balance: WorkerBalance,
    factory: Box<dyn Fn(usize) -> Result<T>>,
}
impl<T> Drop for LocalInner<T> {
    fn drop(&mut self) {
        if let Some(key) = self.factory_key.get() {
            let removed = LOCAL_MANAGERS.try_with(|cell| {
                cell.try_borrow_mut()
                    .ok()
                    .and_then(|mut map| map.remove(&key))
            });
            drop(removed);
        }
        // Destructors run after releasing the map borrow, allowing client cleanup callbacks.
        let removed = CLIENTS.try_with(|cell| {
            cell.try_borrow_mut()
                .ok()
                .and_then(|mut map| map.remove(&self.id))
        });
        drop(removed);
    }
}

/// Local worker manager for any synchronous client type, including `!Send` clients.
/// Clones share the same cache and routing counters on this OS thread.
pub struct WorkerClient<T, F = fn(usize) -> Result<T>> {
    inner: Rc<LocalInner<T>>,
    factory_type: PhantomData<fn() -> F>,
}
impl<T, F> Clone for WorkerClient<T, F> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            factory_type: PhantomData,
        }
    }
}
impl<T: 'static, F: Fn(usize) -> Result<T> + 'static> WorkerClient<T, F> {
    /// Configure local slots. No client is created until the first operation.
    ///
    /// # Errors
    /// Returns a worker error when `workers` is zero.
    pub fn new(workers: usize, balance: WorkerBalance, factory: F) -> Result<Self> {
        Self::from_factory(workers, balance, Box::new(factory))
    }
}
impl<T: 'static, F> WorkerClient<T, F> {
    fn from_factory(
        count: usize,
        balance: WorkerBalance,
        factory: Box<dyn Fn(usize) -> Result<T>>,
    ) -> Result<Self> {
        if count == 0 {
            return Err(worker_error("worker count must be positive"));
        }
        let id = NEXT_ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| worker_error("worker identity space exhausted"))?;
        Ok(Self {
            inner: Rc::new(LocalInner {
                id,
                factory_key: Cell::new(None),
                count,
                balance,
                factory,
            }),
            factory_type: PhantomData,
        })
    }
    fn for_shared_factory(
        key: usize,
        count: usize,
        balance: WorkerBalance,
        factory: Box<dyn Fn(usize) -> Result<T>>,
    ) -> Result<Self> {
        LOCAL_MANAGERS.with(|cell| {
            let mut managers = cell
                .try_borrow_mut()
                .map_err(|_| worker_error("local manager registry is borrowed"))?;
            if let Some(inner) = managers
                .get(&key)
                .and_then(|entry| entry.downcast_ref::<Weak<LocalInner<T>>>())
                .and_then(Weak::upgrade)
            {
                return Ok(Self {
                    inner,
                    factory_type: PhantomData,
                });
            }
            let manager = Self::from_factory(count, balance, factory)?;
            managers.insert(key, Box::new(Rc::downgrade(&manager.inner)));
            manager.inner.factory_key.set(Some(key));
            Ok(manager)
        })
    }
    pub fn workers(&self) -> usize {
        self.inner.count
    }
    pub fn balance(&self) -> WorkerBalance {
        self.inner.balance
    }

    fn pool(&self) -> Result<Rc<Pool<T>>> {
        CLIENTS.with(|cell| {
            {
                let mut entries = cell
                    .try_borrow_mut()
                    .map_err(|_| worker_error("local cache is borrowed"))?;
                if let Some(entry) = entries.get(&self.inner.id) {
                    return entry
                        .as_ref()
                        .ok_or_else(|| worker_error("recursive worker initialization"))?
                        .downcast_ref::<Rc<Pool<T>>>()
                        .cloned()
                        .ok_or_else(|| worker_error("local cache type mismatch"));
                }
                entries.insert(self.inner.id, None);
            }
            let mut pending = Initialization {
                id: self.inner.id,
                committed: false,
            };
            let slots = (0..self.inner.count)
                .map(|index| {
                    Ok(Slot {
                        client: RefCell::new(Some((self.inner.factory)(index)?)),
                        dispatched: Cell::new(0),
                        completed: Cell::new(0),
                        active: Cell::new(0),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let pool = Rc::new(Pool {
                slots,
                next: Cell::new(0),
            });
            cell.try_borrow_mut()
                .map_err(|_| worker_error("local cache is borrowed"))?
                .insert(self.inner.id, Some(Box::new(pool.clone())));
            pending.committed = true;
            Ok(pool)
        })
    }

    /// Run an operation on an available local slot.
    ///
    /// # Errors
    /// Returns factory or callback errors, or a worker error if every slot is busy.
    ///
    /// # Panics
    /// Propagates factory and callback panics after restoring cache and slot ownership.
    pub fn with<R>(&self, callback: impl FnOnce(&mut T) -> Result<R>) -> Result<R> {
        self.dispatch(None, callback)
    }
    /// Route using FNV-1a of `key` when the policy is `KeyHash`.
    /// A busy hashed slot is rejected rather than rerouted to another key owner.
    ///
    /// # Errors
    /// Returns factory or callback errors, or a worker error for a busy slot.
    ///
    /// # Panics
    /// Propagates factory and callback panics after restoring ownership.
    pub fn with_keyed<R>(
        &self,
        key: &[u8],
        callback: impl FnOnce(&mut T) -> Result<R>,
    ) -> Result<R> {
        self.dispatch(Some(key), callback)
    }
    fn dispatch<R>(
        &self,
        key: Option<&[u8]>,
        callback: impl FnOnce(&mut T) -> Result<R>,
    ) -> Result<R> {
        let pool = self.pool()?;
        let index = pool.select(self.inner.balance, key)?;
        let slot = &pool.slots[index];
        let client = slot
            .client
            .try_borrow_mut()
            .map_err(|_| worker_error("worker slot is borrowed"))?
            .take()
            .ok_or_else(|| worker_error("worker slot is busy"))?;
        slot.dispatched.set(slot.dispatched.get().saturating_add(1));
        slot.active.set(slot.active.get() + 1);
        let mut lease = Lease {
            slot,
            client: Some(client),
        };
        callback(
            lease
                .client
                .as_mut()
                .ok_or_else(|| worker_error("worker lease is empty"))?,
        )
    }
    /// Read local counters without initializing clients.
    ///
    /// # Errors
    /// Returns a worker error during recursive initialization or a cache borrow conflict.
    pub fn metrics(&self) -> Result<Vec<WorkerMetrics>> {
        CLIENTS.with(|cell| {
            let entries = cell
                .try_borrow()
                .map_err(|_| worker_error("local cache is borrowed"))?;
            let Some(entry) = entries.get(&self.inner.id) else {
                return Ok(vec![WorkerMetrics::default(); self.workers()]);
            };
            let pool = entry
                .as_ref()
                .ok_or_else(|| worker_error("worker initialization is active"))?
                .downcast_ref::<Rc<Pool<T>>>()
                .ok_or_else(|| worker_error("local cache type mismatch"))?;
            Ok(pool
                .slots
                .iter()
                .map(|slot| WorkerMetrics {
                    dispatched: slot.dispatched.get(),
                    completed: slot.completed.get(),
                    in_flight: slot.active.get(),
                })
                .collect())
        })
    }
    /// Drop this manager's cached clients; the next operation recreates all slots.
    /// All clones observe the reset. Active operations prevent reset.
    ///
    /// # Errors
    /// Returns a worker error while initialization or an operation is active.
    pub fn reset(&self) -> Result<()> {
        let removed = CLIENTS.with(|cell| {
            let mut entries = cell
                .try_borrow_mut()
                .map_err(|_| worker_error("local cache is borrowed"))?;
            if let Some(entry) = entries.get(&self.inner.id) {
                let pool = entry
                    .as_ref()
                    .ok_or_else(|| worker_error("worker initialization is active"))?
                    .downcast_ref::<Rc<Pool<T>>>()
                    .ok_or_else(|| worker_error("local cache type mismatch"))?;
                if pool.slots.iter().any(|slot| slot.active.get() != 0) {
                    return Err(worker_error("worker operation is active"));
                }
            }
            Ok(entries.remove(&self.inner.id))
        })?;
        drop(removed);
        Ok(())
    }
}
impl<T> Pool<T> {
    fn select(&self, balance: WorkerBalance, key: Option<&[u8]>) -> Result<usize> {
        let count = self.slots.len();
        if balance == WorkerBalance::KeyHash
            && let Some(key) = key
        {
            let hash = key.iter().fold(0xcbf2_9ce4_8422_2325u64, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(0x100_0000_01b3)
            });
            return Ok((hash % count as u64) as usize);
        }
        let start = self.next.get();
        for offset in 0..count {
            let index = (start + offset) % count;
            if self.slots[index].active.get() == 0 {
                self.next.set((index + 1) % count);
                return Ok(index);
            }
        }
        Err(worker_error("all local worker slots are busy"))
    }
}
struct Lease<'a, T> {
    slot: &'a Slot<T>,
    client: Option<T>,
}
impl<T> Drop for Lease<'_, T> {
    fn drop(&mut self) {
        // No RefCell borrow is retained across user code.
        if let Ok(mut destination) = self.slot.client.try_borrow_mut() {
            *destination = self.client.take();
        }
        self.slot.active.set(self.slot.active.get() - 1);
        self.slot
            .completed
            .set(self.slot.completed.get().saturating_add(1));
    }
}
struct Initialization {
    id: u64,
    committed: bool,
}
impl Drop for Initialization {
    fn drop(&mut self) {
        if !self.committed {
            let removed = CLIENTS.try_with(|cell| {
                cell.try_borrow_mut()
                    .ok()
                    .and_then(|mut map| map.remove(&self.id))
            });
            drop(removed);
        }
    }
}

/// One lazy client on the current OS thread, shared by clones of this manager.
pub struct ThreadLocalClient<T, F = fn() -> Result<T>> {
    worker: WorkerClient<T, F>,
}
impl<T, F> Clone for ThreadLocalClient<T, F> {
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
        }
    }
}
impl<T: 'static, F: Fn() -> Result<T> + 'static> ThreadLocalClient<T, F> {
    /// Configure a lazy local client.
    ///
    /// # Errors
    /// Returns a worker error if the manager identity space is exhausted.
    pub fn new(factory: F) -> Result<Self> {
        Ok(Self {
            worker: WorkerClient::from_factory(
                1,
                WorkerBalance::RoundRobin,
                Box::new(move |_| factory()),
            )?,
        })
    }
}
impl<T: 'static, F> ThreadLocalClient<T, F> {
    /// Access the current thread's client. Recursive access returns an error.
    ///
    /// # Errors
    /// Returns factory, callback, or reentrant-access errors.
    ///
    /// # Panics
    /// Propagates factory and callback panics after restoring ownership.
    pub fn with<R>(&self, callback: impl FnOnce(&mut T) -> Result<R>) -> Result<R> {
        self.worker.with(callback)
    }
    /// Drop the cached client, creating it again on the next operation.
    ///
    /// # Errors
    /// Returns a worker error if a callback or initialization is active.
    pub fn reset(&self) -> Result<()> {
        self.worker.reset()
    }
}

/// Shareable factory configuration; only `F` determines `Send` and `Sync`.
/// Calls to `local` on the same OS thread share one thread-affine manager.
/// Different OS threads own independent managers and clients.
/// If a client retains a clone of its manager, call `reset` before dropping the
/// last external manager handle to break that ownership cycle.
pub struct ThreadLocalFactory<T, F> {
    factory: Arc<F>,
    client_type: PhantomData<fn() -> T>,
}
impl<T, F> Clone for ThreadLocalFactory<T, F> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            client_type: PhantomData,
        }
    }
}
impl<T: 'static, F: Fn() -> Result<T> + 'static> ThreadLocalFactory<T, F> {
    pub fn new(factory: F) -> Self {
        Self {
            factory: Arc::new(factory),
            client_type: PhantomData,
        }
    }
    /// Obtain the lazy manager shared by this factory on the calling OS thread.
    ///
    /// # Errors
    /// Returns a worker error if the manager identity space is exhausted.
    pub fn local(&self) -> Result<ThreadLocalClient<T, F>> {
        let factory = self.factory.clone();
        Ok(ThreadLocalClient {
            worker: WorkerClient::for_shared_factory(
                Arc::as_ptr(&self.factory) as usize,
                1,
                WorkerBalance::RoundRobin,
                Box::new(move |_| factory()),
            )?,
        })
    }
}

/// Shareable worker configuration with a factory invoked once per local slot.
pub struct WorkerFactory<T, F> {
    factory: Arc<F>,
    workers: usize,
    balance: WorkerBalance,
    client_type: PhantomData<fn() -> T>,
}
impl<T, F> Clone for WorkerFactory<T, F> {
    fn clone(&self) -> Self {
        Self {
            factory: self.factory.clone(),
            workers: self.workers,
            balance: self.balance,
            client_type: PhantomData,
        }
    }
}
impl<T: 'static, F: Fn(usize) -> Result<T> + 'static> WorkerFactory<T, F> {
    /// Capture factory configuration without constructing clients.
    ///
    /// # Errors
    /// Returns a worker error when `workers` is zero.
    pub fn new(workers: usize, balance: WorkerBalance, factory: F) -> Result<Self> {
        if workers == 0 {
            return Err(worker_error("worker count must be positive"));
        }
        Ok(Self {
            factory: Arc::new(factory),
            workers,
            balance,
            client_type: PhantomData,
        })
    }
    /// Obtain this factory's shared local worker slots on the calling OS thread.
    ///
    /// # Errors
    /// Returns a worker error if the manager identity space is exhausted.
    pub fn local(&self) -> Result<WorkerClient<T, F>> {
        let factory = self.factory.clone();
        WorkerClient::for_shared_factory(
            Arc::as_ptr(&self.factory) as usize,
            self.workers,
            self.balance,
            Box::new(move |index| factory(index)),
        )
    }
}

/// Generic producer factory manager; `T` supplies the concrete producer API.
pub type ThreadLocalProducer<T, F = fn() -> Result<T>> = ThreadLocalClient<T, F>;
/// Generic consumer factory manager; `T` supplies the concrete consumer API.
pub type ThreadLocalConsumer<T, F = fn() -> Result<T>> = ThreadLocalClient<T, F>;
/// Generic producer slot manager, operated through `with` or `with_keyed`.
pub type WorkerProducer<T, F = fn(usize) -> Result<T>> = WorkerClient<T, F>;
/// Generic consumer slot manager, operated through `with` or `with_keyed`.
pub type WorkerConsumer<T, F = fn(usize) -> Result<T>> = WorkerClient<T, F>;
