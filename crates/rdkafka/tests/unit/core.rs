//! Caller-thread lifecycle and error ordering regressions.

use super::*;
#[test]
fn dispatch_guard_resets_during_unwind() {
    let active = Rc::new(Cell::new(DriveState::polling()));
    let flag = Rc::clone(&active);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let _guard = DriveGuard {
            state: flag,
            restore: DriveState::default(),
        };
        panic!("user callback panic");
    }));
    assert!(result.is_err());
    assert_eq!(active.get(), DriveState::default());
}

#[test]
fn fractional_timeout_rounds_up() {
    use std::time::Duration;
    assert_eq!(timeout(Duration::ZERO).unwrap(), 0);
    assert_eq!(timeout(Duration::from_nanos(1)).unwrap(), 1);
    assert_eq!(timeout(Duration::from_micros(999)).unwrap(), 1);
    assert_eq!(timeout(Duration::from_micros(1001)).unwrap(), 2);
    assert_eq!(
        timeout(Duration::from_millis(i32::MAX as u64)).unwrap(),
        i32::MAX
    );
    assert!(timeout(Duration::from_millis(i32::MAX as u64) + Duration::from_nanos(1)).is_err());
}
#[test]
fn shared_callbacks_preserve_native_transactions() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?
        .set("transactional.id", "native-ownership")?;
    let first = Producer::new(&conf)?;
    let second = Producer::new(&conf)?;
    first.init_transactions(10000)?;
    first.begin_transaction()?;
    assert!(second.begin_transaction().is_err());
    let guard = first.core.begin_drive()?;
    assert!(matches!(second.poll(0), Err(Error::ReentrantDrive)));
    drop(guard);
    first.abort_transaction(10000)?;
    Ok(())
}

#[test]
fn tls_priority_retains_operation_error() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    let core = Core::new(&conf, 0)?;
    *core.pending_tls_error.borrow_mut() = Some(Error::Tls("certificate failure".into()));
    let result = core.complete::<()>(Err(Error::native(-185, "operation timed out")));
    assert!(matches!(result, Err(Error::Tls(_))));
    assert!(
        matches!(core.take_pending_error(), Err(error) if error.code().is_some_and(|c| c.value()==-185)),
        "TLS priority must defer rather than erase the operation error"
    );
    Ok(())
}

#[test]
fn error_payload_trims_counted_nul() {
    let code = ErrorCode::PARTITION_EOF.value();
    assert_eq!(
        consumer_error_text(Some(b"broker failure\0"), code),
        "broker failure"
    );
    assert_eq!(
        consumer_error_text(Some(b"inner\0text\0\0"), code),
        "inner\0text"
    );
}
