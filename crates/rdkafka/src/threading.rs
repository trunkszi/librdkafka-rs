//! Shareable synchronous managers with client state confined to each calling OS thread.
use std::{
    any::Any,
    cell::{Cell, RefCell},
    collections::HashMap,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    rc::Rc,
    sync::{
        Arc, Weak,
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
    static CLIENTS: RefCell<HashMap<u64, Entry>> = RefCell::new(HashMap::new());
}
struct Entry {
    owner: Weak<()>,
    pool: Option<Box<dyn Any>>,
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
struct Shared<F> {
    id: u64,
    owner: Arc<()>,
    count: usize,
    balance: WorkerBalance,
    factory: F,
}
impl<F> Drop for Shared<F> {
    fn drop(&mut self) {
        let removed = CLIENTS.try_with(|cell| {
            cell.try_borrow_mut()
                .ok()
                .and_then(|mut entries| entries.remove(&self.id))
        });
        // Client destructors may invoke other managers after the registry borrow ends.
        drop(removed);
    }
}
struct Manager<T, F> {
    shared: Arc<Shared<F>>,
    client_type: PhantomData<fn() -> T>,
}
impl<T, F> Clone for Manager<T, F> {
    fn clone(&self) -> Self {
        Self {
            shared: self.shared.clone(),
            client_type: PhantomData,
        }
    }
}
impl<T: 'static, F> Manager<T, F> {
    fn new(count: usize, balance: WorkerBalance, factory: F) -> Result<Self> {
        if count == 0 {
            return Err(worker_error("worker count must be positive"));
        }
        let id = NEXT_ID
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| worker_error("worker identity space exhausted"))?;
        Ok(Self {
            shared: Arc::new(Shared {
                id,
                owner: Arc::new(()),
                count,
                balance,
                factory,
            }),
            client_type: PhantomData,
        })
    }
    fn pool(&self, factory: impl Fn(usize) -> Result<T>) -> Result<Rc<Pool<T>>> {
        CLIENTS.with(|cell| {
            let expired = {
                let mut entries = cell
                    .try_borrow_mut()
                    .map_err(|_| worker_error("local cache is borrowed"))?;
                entries
                    .extract_if(|_, entry| entry.owner.strong_count() == 0)
                    .map(|(_, entry)| entry)
                    .collect::<Vec<_>>()
            };
            drop(expired);
            {
                let mut entries = cell
                    .try_borrow_mut()
                    .map_err(|_| worker_error("local cache is borrowed"))?;
                if let Some(entry) = entries.get(&self.shared.id) {
                    return entry
                        .pool
                        .as_ref()
                        .ok_or_else(|| worker_error("recursive worker initialization"))?
                        .downcast_ref::<Rc<Pool<T>>>()
                        .cloned()
                        .ok_or_else(|| worker_error("local cache type mismatch"));
                }
                entries.insert(
                    self.shared.id,
                    Entry {
                        owner: Arc::downgrade(&self.shared.owner),
                        pool: None,
                    },
                );
            }
            let mut pending = Initialization {
                id: self.shared.id,
                committed: false,
            };
            let slots = (0..self.shared.count)
                .map(|index| {
                    Ok(Slot {
                        client: RefCell::new(Some(factory(index)?)),
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
                .insert(
                    self.shared.id,
                    Entry {
                        owner: Arc::downgrade(&self.shared.owner),
                        pool: Some(Box::new(pool.clone())),
                    },
                );
            pending.committed = true;
            Ok(pool)
        })
    }
    fn lease(
        &self,
        factory: impl Fn(usize) -> Result<T>,
        key: Option<&[u8]>,
    ) -> Result<WorkerSlotLease<T>> {
        let pool = self.pool(factory)?;
        let index = pool.select(self.shared.balance, key)?;
        let slot = &pool.slots[index];
        let client = slot
            .client
            .try_borrow_mut()
            .map_err(|_| worker_error("worker slot is borrowed"))?
            .take()
            .ok_or_else(|| worker_error("worker slot is busy"))?;
        slot.dispatched.set(slot.dispatched.get().saturating_add(1));
        slot.active.set(1);
        Ok(WorkerSlotLease {
            pool,
            index,
            client: Some(client),
        })
    }
    fn metrics(&self) -> Result<Vec<WorkerMetrics>> {
        CLIENTS.with(|cell| {
            let entries = cell
                .try_borrow()
                .map_err(|_| worker_error("local cache is borrowed"))?;
            let Some(entry) = entries.get(&self.shared.id) else {
                return Ok(vec![WorkerMetrics::default(); self.shared.count]);
            };
            let pool = entry
                .pool
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
    fn reset(&self) -> Result<()> {
        let removed = CLIENTS.with(|cell| {
            let mut entries = cell
                .try_borrow_mut()
                .map_err(|_| worker_error("local cache is borrowed"))?;
            if let Some(entry) = entries.get(&self.shared.id) {
                let pool = entry
                    .pool
                    .as_ref()
                    .ok_or_else(|| worker_error("worker initialization is active"))?
                    .downcast_ref::<Rc<Pool<T>>>()
                    .ok_or_else(|| worker_error("local cache type mismatch"))?;
                if pool.slots.iter().any(|slot| slot.active.get() != 0) {
                    return Err(worker_error("worker operation is active"));
                }
            }
            Ok(entries.remove(&self.shared.id))
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

/// Exclusive ownership of one client on the calling thread, released on drop.
/// The lease is `!Send + !Sync`, even when `T` is transferable. It can outlive
/// its manager; the local pool stays alive until the lease is released.
#[must_use = "dropping the lease releases the worker slot"]
pub struct WorkerSlotLease<T> {
    pool: Rc<Pool<T>>,
    index: usize,
    client: Option<T>,
}
impl<T> Deref for WorkerSlotLease<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.client.as_ref().expect("live lease owns its client")
    }
}
impl<T> DerefMut for WorkerSlotLease<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.client.as_mut().expect("live lease owns its client")
    }
}
impl<T> Drop for WorkerSlotLease<T> {
    fn drop(&mut self) {
        let slot = &self.pool.slots[self.index];
        *slot.client.borrow_mut() = self.client.take();
        slot.active.set(0);
        slot.completed.set(slot.completed.get().saturating_add(1));
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

/// Shareable manager of independent `0..N` client slots per calling OS thread.
/// `Send + Sync` depends only on the factory `F`; `T` may be `!Send + !Sync`.
/// Clones share identity and routing on each thread. No client crosses threads.
/// `reset` affects only the calling thread. On final manager drop, remote caches
/// are reclaimed on their next manager access or when their owning thread exits.
/// If a client retains a manager clone, use `reset` to break that ownership cycle.
pub struct WorkerClient<T, F = fn(usize) -> Result<T>> {
    manager: Manager<T, F>,
}
impl<T, F> Clone for WorkerClient<T, F> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
        }
    }
}
impl<T: 'static, F: Fn(usize) -> Result<T>> WorkerClient<T, F> {
    /// Configure lazy per-thread slots without creating clients.
    ///
    /// # Errors
    /// Returns a worker error for zero workers or exhausted manager identities.
    pub fn new(workers: usize, balance: WorkerBalance, factory: F) -> Result<Self> {
        Ok(Self {
            manager: Manager::new(workers, balance, factory)?,
        })
    }
    pub fn workers(&self) -> usize {
        self.manager.shared.count
    }
    pub fn balance(&self) -> WorkerBalance {
        self.manager.shared.balance
    }
    /// Acquire a local slot; only one lease can own a slot at a time.
    ///
    /// # Errors
    /// Returns factory errors or a worker error when every slot is busy.
    ///
    /// # Panics
    /// Propagates factory panics after restoring initialization state.
    pub fn lease(&self) -> Result<WorkerSlotLease<T>> {
        self.manager
            .lease(|index| (self.manager.shared.factory)(index), None)
    }
    /// Acquire the local key owner under `KeyHash`, without rerouting a busy key.
    ///
    /// # Errors
    /// Returns factory errors or a worker error for a busy slot.
    ///
    /// # Panics
    /// Propagates factory panics after restoring initialization state.
    pub fn lease_keyed(&self, key: &[u8]) -> Result<WorkerSlotLease<T>> {
        self.manager
            .lease(|index| (self.manager.shared.factory)(index), Some(key))
    }
    /// Execute on an available local slot, returning it on completion or unwind.
    ///
    /// # Errors
    /// Returns factory, callback, or busy-slot errors.
    ///
    /// # Panics
    /// Propagates factory and callback panics after restoring local ownership.
    pub fn with<R>(&self, callback: impl FnOnce(&mut T) -> Result<R>) -> Result<R> {
        let mut lease = self.lease()?;
        callback(&mut lease)
    }
    /// Execute on the local key owner under `KeyHash`.
    ///
    /// # Errors
    /// Returns factory, callback, or busy-slot errors.
    ///
    /// # Panics
    /// Propagates factory and callback panics after restoring local ownership.
    pub fn with_keyed<R>(
        &self,
        key: &[u8],
        callback: impl FnOnce(&mut T) -> Result<R>,
    ) -> Result<R> {
        let mut lease = self.lease_keyed(key)?;
        callback(&mut lease)
    }
    /// Read calling-thread counters without initializing clients.
    ///
    /// # Errors
    /// Returns a worker error during initialization or a cache borrow conflict.
    pub fn metrics(&self) -> Result<Vec<WorkerMetrics>> {
        self.manager.metrics()
    }
    /// Release calling-thread clients; subsequent use recreates them.
    ///
    /// # Errors
    /// Returns a worker error during initialization or while a lease is active.
    pub fn reset(&self) -> Result<()> {
        self.manager.reset()
    }
}

/// Shareable manager of one lazy, thread-affine client per calling OS thread.
/// `Send + Sync` depends only on `F`, independently of the client's auto traits.
/// Reset and remote reclamation follow the same rules as [`WorkerClient`].
pub struct ThreadLocalClient<T, F = fn() -> Result<T>> {
    manager: Manager<T, F>,
}
impl<T, F> Clone for ThreadLocalClient<T, F> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager.clone(),
        }
    }
}
impl<T: 'static, F: Fn() -> Result<T>> ThreadLocalClient<T, F> {
    /// Configure one lazy client per calling OS thread.
    ///
    /// # Errors
    /// Returns a worker error if manager identities are exhausted.
    pub fn new(factory: F) -> Result<Self> {
        Ok(Self {
            manager: Manager::new(1, WorkerBalance::RoundRobin, factory)?,
        })
    }
    /// Access the calling thread's client. Recursive access returns an error.
    ///
    /// # Errors
    /// Returns factory, callback, or reentrant-access errors.
    ///
    /// # Panics
    /// Propagates factory and callback panics after restoring ownership.
    pub fn with<R>(&self, callback: impl FnOnce(&mut T) -> Result<R>) -> Result<R> {
        let mut lease = self
            .manager
            .lease(|_| (self.manager.shared.factory)(), None)?;
        callback(&mut lease)
    }
    /// Release the calling thread's cached client.
    ///
    /// # Errors
    /// Returns a worker error if initialization or a lease is active.
    pub fn reset(&self) -> Result<()> {
        self.manager.reset()
    }
}
/// Shareable producer manager with one client per calling OS thread.
pub type ThreadLocalProducer<T, F = fn() -> Result<T>> = ThreadLocalClient<T, F>;
/// Shareable consumer manager with one client per calling OS thread.
pub type ThreadLocalConsumer<T, F = fn() -> Result<T>> = ThreadLocalClient<T, F>;
/// Shareable producer slot manager.
pub type WorkerProducer<T, F = fn(usize) -> Result<T>> = WorkerClient<T, F>;
/// Shareable consumer slot manager.
pub type WorkerConsumer<T, F = fn(usize) -> Result<T>> = WorkerClient<T, F>;
