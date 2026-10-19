use std::{
    cell::Cell,
    panic::{AssertUnwindSafe, catch_unwind},
    rc::Rc,
    sync::{Arc, Mutex},
    thread::{self, ThreadId},
};

use rdkafka::{
    Error, Result,
    threading::{ThreadLocalClient, WorkerBalance, WorkerClient},
};

struct Client {
    index: usize,
    owner: ThreadId,
    drops: Rc<Cell<usize>>,
}
impl Drop for Client {
    fn drop(&mut self) {
        assert_eq!(self.owner, thread::current().id());
        self.drops.set(self.drops.get() + 1);
    }
}

#[test]
fn integration_lifecycle_thread_local_clones_reset_and_drop() -> Result<()> {
    let created = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let manager = ThreadLocalClient::new({
        let created = created.clone();
        let drops = drops.clone();
        move || {
            created.set(created.get() + 1);
            Ok(Client {
                index: created.get(),
                owner: thread::current().id(),
                drops: drops.clone(),
            })
        }
    })?;
    assert_eq!(created.get(), 0);
    assert_eq!(manager.with(|client| Ok(client.index))?, 1);
    let clone = manager.clone();
    drop(manager);
    assert_eq!(clone.with(|client| Ok(client.index))?, 1);
    assert_eq!(drops.get(), 0);
    clone.reset()?;
    assert_eq!(drops.get(), 1);
    assert_eq!(clone.with(|client| Ok(client.index))?, 2);
    drop(clone);
    assert_eq!(drops.get(), 2);
    Ok(())
}

#[test]
fn integration_scenario_worker_factory_failure_rolls_back_and_retries() -> Result<()> {
    let fail = Rc::new(Cell::new(true));
    let drops = Rc::new(Cell::new(0));
    let manager = WorkerClient::new(3, WorkerBalance::RoundRobin, {
        let drops = drops.clone();
        move |index| {
            if index == 1 && fail.replace(false) {
                return Err(Error::Worker("factory rejected slot".into()));
            }
            Ok(Client {
                index,
                owner: thread::current().id(),
                drops: drops.clone(),
            })
        }
    })?;
    assert!(manager.with(|_| Ok(())).is_err());
    assert_eq!(drops.get(), 1);
    assert_eq!(
        manager.metrics()?.iter().map(|m| m.dispatched).sum::<u64>(),
        0
    );
    for expected in [0, 1, 2, 0] {
        assert_eq!(manager.with(|client| Ok(client.index))?, expected);
    }
    manager.reset()?;
    assert_eq!(drops.get(), 4);
    Ok(())
}

#[test]
fn integration_scenario_nested_calls_error_and_panic_restore_slots() -> Result<()> {
    let manager = WorkerClient::new(2, WorkerBalance::LeastInFlight, Ok)?;
    manager.with(|index| {
        assert_eq!(*index, 0);
        assert!(manager.reset().is_err());
        let metrics = manager.metrics()?;
        assert_eq!(metrics[0].in_flight, 1);
        manager.with(|index| {
            assert_eq!(*index, 1);
            assert!(manager.with(|_| Ok(())).is_err());
            Ok(())
        })
    })?;
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        manager.with::<()>(|_| panic!("callback panic"))
    }));
    assert!(outcome.is_err());
    assert!(
        manager
            .with::<()>(|_| Err(Error::Worker("callback rejected".into())))
            .is_err()
    );
    let metrics = manager.metrics()?;
    assert!(
        metrics
            .iter()
            .all(|m| m.in_flight == 0 && m.dispatched == m.completed)
    );
    assert_eq!(metrics.iter().map(|m| m.completed).sum::<u64>(), 4);
    manager.with(|_| Ok(()))?;
    Ok(())
}

#[test]
fn integration_scenario_thread_local_reentrancy_and_initialization_panic() -> Result<()> {
    let panic_once = Rc::new(Cell::new(true));
    let manager = ThreadLocalClient::new(move || {
        if panic_once.replace(false) {
            panic!("factory panic");
        }
        Ok(17)
    })?;
    assert!(catch_unwind(AssertUnwindSafe(|| manager.with(|_| Ok(())))).is_err());
    manager.with(|value| {
        assert_eq!(*value, 17);
        assert!(manager.with(|_| Ok(())).is_err());
        Ok(())
    })?;
    manager.with(|value| {
        *value += 1;
        Ok(())
    })?;
    assert_eq!(manager.with(|value| Ok(*value))?, 18);
    Ok(())
}

#[test]
fn unit_scenario_fnv1a_key_routing_and_busy_key_rejection() -> Result<()> {
    let manager = WorkerClient::new(7, WorkerBalance::KeyHash, Ok)?;
    // Published FNV-1a 64-bit test vector for the ASCII bytes "hello".
    let expected = (0xa430_d846_80aa_bd0bu64 % 7) as usize;
    manager.with_keyed(b"hello", |index| {
        assert_eq!(*index, expected);
        assert!(manager.with_keyed(b"hello", |_| Ok(())).is_err());
        Ok(())
    })?;
    assert_eq!(manager.with_keyed(b"hello", |index| Ok(*index))?, expected);
    assert_eq!(manager.with(|index| Ok(*index))?, 0);
    assert_eq!(manager.with(|index| Ok(*index))?, 1);
    assert!(WorkerClient::new(0, WorkerBalance::RoundRobin, |_: usize| Ok(())).is_err());
    Ok(())
}

#[test]
fn integration_lifecycle_managers_create_and_destroy_on_two_calling_os_threads() {
    struct LocalClient {
        owner: ThreadId,
        drops: Arc<Mutex<Vec<ThreadId>>>,
        _local: Rc<()>,
    }
    impl Drop for LocalClient {
        fn drop(&mut self) {
            assert_eq!(self.owner, thread::current().id());
            self.drops.lock().unwrap().push(thread::current().id());
        }
    }
    let created = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(Mutex::new(Vec::new()));
    let factory = ThreadLocalClient::new({
        let created = created.clone();
        let drops = dropped.clone();
        move || {
            let owner = thread::current().id();
            created.lock().unwrap().push(owner);
            Ok(LocalClient {
                owner,
                drops: drops.clone(),
                _local: Rc::new(()),
            })
        }
    })
    .unwrap();
    let workers = WorkerClient::new(2, WorkerBalance::RoundRobin, |index| {
        Ok((index, thread::current().id(), Rc::new(())))
    })
    .unwrap();
    let threads = (0..2)
        .map(|_| {
            let factory = factory.clone();
            let workers = workers.clone();
            thread::spawn(move || {
                let manager = factory.clone();
                let worker = workers;
                for _ in 0..2 {
                    manager
                        .with(|client| {
                            assert_eq!(client.owner, thread::current().id());
                            Ok(())
                        })
                        .unwrap();
                }
                for expected in [0, 1, 0] {
                    worker
                        .with(|client| {
                            assert_eq!(client.0, expected);
                            assert_eq!(client.1, thread::current().id());
                            Ok(())
                        })
                        .unwrap();
                }
                thread::current().id()
            })
        })
        .collect::<Vec<_>>();
    let owners = threads
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect::<Vec<_>>();
    assert_ne!(owners[0], owners[1]);
    let created = created.lock().unwrap().clone();
    let dropped = dropped.lock().unwrap().clone();
    assert_eq!(created.len(), 2);
    assert_eq!(dropped.len(), 2);
    for owner in owners {
        assert!(created.contains(&owner));
        assert!(dropped.contains(&owner));
    }
}

#[test]
fn integration_lifecycle_reset_releases_client_retained_manager_clone() -> Result<()> {
    struct Client {
        retained_manager: Option<Box<dyn std::any::Any>>,
        drops: Rc<Cell<usize>>,
    }
    impl Drop for Client {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }
    let drops = Rc::new(Cell::new(0));
    let manager = ThreadLocalClient::new({
        let drops = drops.clone();
        move || {
            Ok(Client {
                retained_manager: None,
                drops: drops.clone(),
            })
        }
    })?;
    manager.with(|client| {
        client.retained_manager = Some(Box::new(manager.clone()));
        Ok(())
    })?;
    manager.reset()?;
    assert_eq!(drops.get(), 1);
    manager.with(|client| {
        assert!(client.retained_manager.is_none());
        Ok(())
    })?;
    drop(manager);
    assert_eq!(drops.get(), 2);
    Ok(())
}

#[test]
fn integration_lifecycle_manager_clones_share_one_client() -> Result<()> {
    let created = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let factory = ThreadLocalClient::new({
        let created = created.clone();
        let drops = drops.clone();
        move || {
            created.set(created.get() + 1);
            Ok(Client {
                index: created.get(),
                owner: thread::current().id(),
                drops: drops.clone(),
            })
        }
    })?;
    let first = factory.clone();
    let second = factory.clone();
    assert_eq!(first.with(|client| Ok(client.index))?, 1);
    assert_eq!(second.with(|client| Ok(client.index))?, 1);
    drop(first);
    assert_eq!(drops.get(), 0);
    assert_eq!(second.with(|client| Ok(client.index))?, 1);
    assert_eq!(created.get(), 1);
    second.reset()?;
    assert_eq!(drops.get(), 1);
    let third = factory.clone();
    assert_eq!(third.with(|client| Ok(client.index))?, 2);
    assert_eq!(second.with(|client| Ok(client.index))?, 2);
    drop(second);
    assert_eq!(drops.get(), 1);
    drop(third);
    factory.reset()?;
    assert_eq!(drops.get(), 2);
    assert_eq!(factory.with(|client| Ok(client.index))?, 3);
    factory.reset()?;
    assert_eq!(drops.get(), 3);
    Ok(())
}

#[test]
fn integration_lifecycle_worker_clones_share_slots() -> Result<()> {
    let created = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let factory = WorkerClient::new(2, WorkerBalance::RoundRobin, {
        let created = created.clone();
        let drops = drops.clone();
        move |index| {
            created.set(created.get() + 1);
            Ok(Client {
                index,
                owner: thread::current().id(),
                drops: drops.clone(),
            })
        }
    })?;
    let first = factory.clone();
    let second = factory.clone();
    assert_eq!(first.with(|client| Ok(client.index))?, 0);
    assert_eq!(second.with(|client| Ok(client.index))?, 1);
    assert_eq!(created.get(), 2);
    drop(first);
    assert_eq!(drops.get(), 0);
    assert_eq!(second.with(|client| Ok(client.index))?, 0);
    let third = factory.clone();
    second.reset()?;
    assert_eq!(drops.get(), 2);
    assert_eq!(third.with(|client| Ok(client.index))?, 0);
    assert_eq!(second.with(|client| Ok(client.index))?, 1);
    assert_eq!(created.get(), 4);
    drop(second);
    assert_eq!(drops.get(), 2);
    drop(third);
    factory.reset()?;
    assert_eq!(drops.get(), 4);
    Ok(())
}

#[test]
fn integration_lifecycle_shared_reference_and_arc_keep_clients_thread_local() -> Result<()> {
    let manager =
        rdkafka::ThreadLocalProducer::new(|| Ok((thread::current().id(), Rc::new(Cell::new(0)))))?;
    let workers = Arc::new(rdkafka::WorkerProducer::new(
        2,
        WorkerBalance::RoundRobin,
        |index| Ok((thread::current().id(), index, Rc::new(Cell::new(0)))),
    )?);
    let barrier = std::sync::Barrier::new(2);
    thread::scope(|scope| {
        let handles = (0..2)
            .map(|_| {
                let manager = &manager;
                let workers = workers.clone();
                let barrier = &barrier;
                scope.spawn(move || -> Result<()> {
                    barrier.wait();
                    for expected in 0..3 {
                        manager.with(|client| {
                            assert_eq!(client.0, thread::current().id());
                            assert_eq!(client.1.get(), expected);
                            client.1.set(expected + 1);
                            Ok(())
                        })?;
                    }
                    let mut first = workers.lease()?;
                    let mut second = workers.lease()?;
                    assert_eq!((first.1, second.1), (0, 1));
                    assert_eq!(first.0, thread::current().id());
                    assert_eq!(second.0, thread::current().id());
                    assert!(!Rc::ptr_eq(&first.2, &second.2));
                    first.1 = 11;
                    second.1 = 22;
                    assert!(workers.lease().is_err());
                    assert!(workers.reset().is_err());
                    drop(first);
                    drop(second);
                    assert_eq!(workers.lease()?.1, 11);
                    workers.reset()?;
                    manager.reset()?;
                    Ok(())
                })
            })
            .collect::<Vec<_>>();
        for handle in handles {
            handle.join().unwrap()?;
        }
        Ok(())
    })
}

#[test]
fn integration_lifecycle_public_lease_survives_manager_and_releases_on_unwind() -> Result<()> {
    let drops = Rc::new(Cell::new(0));
    let manager = WorkerClient::new(1, WorkerBalance::KeyHash, {
        let drops = drops.clone();
        move |index| {
            Ok(Client {
                index,
                owner: thread::current().id(),
                drops: drops.clone(),
            })
        }
    })?;
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let mut lease = manager.lease_keyed(b"key").unwrap();
        lease.index = 23;
        assert!(manager.lease_keyed(b"key").is_err());
        panic!("lease owner panicked");
    }));
    assert!(outcome.is_err());
    let mut lease = manager.lease_keyed(b"key")?;
    assert_eq!(lease.index, 23);
    lease.index = 24;
    let metrics = manager.metrics()?;
    assert_eq!(
        (
            metrics[0].dispatched,
            metrics[0].completed,
            metrics[0].in_flight
        ),
        (2, 1, 1)
    );
    drop(manager);
    assert_eq!(drops.get(), 0);
    assert_eq!(lease.index, 24);
    drop(lease);
    assert_eq!(drops.get(), 1);
    Ok(())
}

#[test]
fn integration_lifecycle_remote_cache_reclaimed_on_owning_thread() -> Result<()> {
    struct Owned {
        owner: ThreadId,
        drops: Arc<Mutex<Vec<ThreadId>>>,
        _local: Rc<()>,
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            assert_eq!(self.owner, thread::current().id());
            self.drops.lock().unwrap().push(self.owner);
        }
    }
    let drops = Arc::new(Mutex::new(Vec::new()));
    let manager = Arc::new(ThreadLocalClient::new({
        let drops = drops.clone();
        move || {
            Ok(Owned {
                owner: thread::current().id(),
                drops: drops.clone(),
                _local: Rc::new(()),
            })
        }
    })?);
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (resume_tx, resume_rx) = std::sync::mpsc::channel();
    let worker = {
        let manager = manager.clone();
        thread::spawn(move || -> Result<()> {
            manager.with(|_| Ok(()))?;
            drop(manager);
            ready_tx.send(thread::current().id()).unwrap();
            resume_rx.recv().unwrap();
            let other = ThreadLocalClient::new(|| Ok(Rc::new(())))?;
            other.with(|_| Ok(()))?;
            Ok(())
        })
    };
    let owner = ready_rx.recv().unwrap();
    drop(manager);
    assert!(drops.lock().unwrap().is_empty());
    resume_tx.send(()).unwrap();
    worker.join().unwrap()?;
    assert_eq!(*drops.lock().unwrap(), vec![owner]);
    Ok(())
}
