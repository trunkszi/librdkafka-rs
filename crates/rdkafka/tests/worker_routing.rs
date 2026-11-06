//! Routing remains exact across the scalar bitmap boundary and lease teardown.
use rdkafka::{Result, WorkerBalance, WorkerClient};

#[test]
fn integration_sparse_worker_routes() -> Result<()> {
    for count in [1, 3, 63, 64, 65, 129] {
        for balance in [WorkerBalance::RoundRobin, WorkerBalance::LeastInFlight] {
            let manager = WorkerClient::new(count, balance, Ok::<_, rdkafka::Error>)?;
            let mut held = (0..count)
                .map(|_| manager.lease().map(Some))
                .collect::<Result<Vec<_>>>()?;
            for (index, lease) in held.iter().enumerate() {
                assert_eq!(**lease.as_ref().expect("held slot"), index);
            }
            assert!(manager.lease().is_err());
            let mut holes = vec![0, count / 2, count - 1];
            holes.sort_unstable();
            holes.dedup();
            for &index in &holes {
                drop(held[index].take());
            }
            let replacements = holes
                .iter()
                .map(|_| manager.lease())
                .collect::<Result<Vec<_>>>()?;
            assert_eq!(replacements.iter().map(|v| **v).collect::<Vec<_>>(), holes);
            assert!(manager.lease().is_err());
            assert!(manager.reset().is_err());
            drop(replacements);
            drop(held);
            let metrics = manager.metrics()?;
            assert!(metrics.iter().all(|slot| slot.in_flight == 0));
            assert_eq!(
                metrics.iter().map(|slot| slot.completed).sum::<u64>(),
                (count + holes.len()) as u64
            );
            manager.reset()?;
        }
    }
    Ok(())
}

#[test]
fn integration_key_owner_matches_modulo() -> Result<()> {
    // Fixed FNV-1a vectors exercise full-width remainder without copying the routing hash.
    let vectors: [(&[u8], u64); 2] = [
        (b"", 0xcbf2_9ce4_8422_2325),
        (b"abc", 16_654_208_175_385_433_931),
    ];
    for count in [1, 3, 63, 64, 65, 129] {
        let manager = WorkerClient::new(count, WorkerBalance::KeyHash, Ok::<_, rdkafka::Error>)?;
        for (key, hash) in vectors {
            let lease = manager.lease_keyed(key)?;
            assert_eq!(*lease, (hash % count as u64) as usize);
            assert!(manager.lease_keyed(key).is_err());
            drop(lease);
            assert_eq!(*manager.lease_keyed(key)?, (hash % count as u64) as usize);
        }
    }
    Ok(())
}

#[test]
fn integration_worker_init_boundary() -> Result<()> {
    for count in [64, 65] {
        let fail = std::cell::Cell::new(true);
        let workers = WorkerClient::new(count, WorkerBalance::RoundRobin, |index| {
            if index + 1 == count && fail.replace(false) {
                return Err(rdkafka::Error::Worker("last slot failed".into()));
            }
            Ok(index)
        })?;
        assert!(workers.lease().is_err());
        assert!(workers.metrics()?.iter().all(|slot| slot.in_flight == 0));
        let mut leases = (0..count)
            .map(|_| workers.lease())
            .collect::<Result<Vec<_>>>()?;
        assert_eq!(
            leases.iter().map(|slot| **slot).collect::<Vec<_>>(),
            (0..count).collect::<Vec<_>>()
        );
        assert!(workers.lease().is_err());
        // The final slot straddles the bitmap and per-slot fallback boundary.
        drop(leases.pop());
        assert_eq!(*workers.lease()?, count - 1);
        drop(leases);
        workers.reset()?;
    }
    Ok(())
}
