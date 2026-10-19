use super::*;

fn event(kind: i32, message: &str) -> ffi::NativeEvent {
    ffi::NativeEvent {
        kind,
        code: 0,
        message: message.into(),
        topic: "orders".into(),
        partition: 2,
        offset: 42,
        token: 7,
        partitions: Vec::new(),
        status: 2,
        latency_us: -1,
        broker_name: String::new(),
        broker_id: -1,
        throttle_ms: 0,
        log_level: 0,
        log_facility: String::new(),
        fatal: false,
        timestamp: -1,
        key: Vec::new(),
        has_key: false,
        payload: Vec::new(),
        has_payload: false,
        headers: Vec::new(),
        assignment_lost: false,
        rebalance_protocol: String::new(),
    }
}

fn core() -> Rc<Core> {
    let callbacks = Rc::new(RefCell::new(Callbacks::default()));
    Rc::new(Core {
        native: RefCell::new(cxx::UniquePtr::null()),
        active: Rc::clone(&callbacks.borrow().active),
        callbacks: Rc::clone(&callbacks),
        rebalance_snapshot: RefCell::default(),
    })
}

#[test]
fn unit_scenario_tls_failure_preserves_remaining_callback_order() {
    let core = core();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let events = Rc::clone(&seen);
    core.callbacks.borrow_mut().event = Some(Box::new(move |event| {
        if let Event::Error { message, .. } = event {
            events.borrow_mut().push(message.clone());
        }
    }));
    let deliveries = Rc::clone(&seen);
    core.callbacks.borrow_mut().delivery = Some(Box::new(move |report| {
        assert_eq!((report.token, report.offset), (7, 42));
        deliveries.borrow_mut().push("delivery".into());
    }));
    let rebalances = Rc::clone(&seen);
    let owner = Rc::downgrade(&core);
    core.callbacks.borrow_mut().rebalance = Some(Box::new(move |code, parts| {
        assert_eq!(code, 0);
        assert_eq!(parts[0].topic, "orders");
        assert_eq!(parts[0].partition, 2);
        assert_eq!(
            owner
                .upgrade()
                .unwrap()
                .rebalance_snapshot
                .borrow()
                .as_ref(),
            Some(&(true, "COOPERATIVE".into()))
        );
        rebalances.borrow_mut().push("rebalance".into());
    }));
    let oauth = Rc::clone(&seen);
    core.callbacks.borrow_mut().oauth = Some(Box::new(move |_| {
        oauth.borrow_mut().push("oauth".into());
        Err(Error::Config("token refresh failed".into()))
    }));
    let consumed = Rc::clone(&seen);
    core.callbacks.borrow_mut().consume = Some(Box::new(move |message| {
        assert_eq!(message.offset, 42);
        consumed.borrow_mut().push("consume".into());
    }));
    let mut rebalance = event(5, "");
    rebalance.assignment_lost = true;
    rebalance.rebalance_protocol = "COOPERATIVE".into();
    rebalance.partitions = partitions(&[TopicPartition::new("orders", 2, 42)]);
    let mut invalid_message = event(10, "");
    invalid_message.code = -1;
    let result = core.dispatch(vec![
        event(13, "first TLS failure"),
        event(0, ""),
        event(1, "broker error"),
        rebalance,
        event(13, "second TLS failure"),
        event(7, "refresh"),
        invalid_message,
        event(12, "callback queue exhausted"),
        event(10, ""),
        event(0, ""),
    ]);
    assert!(matches!(result, Err(Error::Tls(message)) if message == "first TLS failure"));
    assert_eq!(
        *seen.borrow(),
        [
            "first TLS failure",
            "delivery",
            "broker error",
            "rebalance",
            "second TLS failure",
            "oauth",
            "consume",
            "delivery",
        ]
    );
    assert!(core.rebalance_snapshot.borrow().is_none());
    assert!(core.callbacks.borrow().oauth.is_some());
}

#[test]
fn unit_scenario_tls_supersedes_prior_callback_errors_without_dropping_events() {
    for kind in [7, 10, 12] {
        let core = core();
        core.callbacks.borrow_mut().oauth =
            Some(Box::new(|_| Err(Error::Config("refresh failed".into()))));
        let deliveries = Rc::new(Cell::new(0));
        let observed = Rc::clone(&deliveries);
        core.callbacks.borrow_mut().delivery = Some(Box::new(move |_| {
            observed.set(observed.get() + 1);
        }));
        let mut failure = event(kind, "prior native failure");
        failure.code = -1;
        let result = core.dispatch(vec![
            failure,
            event(0, ""),
            event(13, "TLS failure"),
            event(0, ""),
        ]);
        assert!(matches!(result, Err(Error::Tls(message)) if message == "TLS failure"));
        assert_eq!(deliveries.get(), 2);
    }
}

#[test]
fn unit_scenario_callback_query_reuses_drive_without_dispatching() {
    let core = core();
    let guard = core.begin_drive().unwrap();
    // The native handle is absent: dispatching here would dereference it.
    let result = core.query(|| {
        assert!(core.active.get());
        assert!(matches!(core.begin_drive(), Err(Error::ReentrantDrive)));
        Err::<(), _>(Error::Config("query failed".into()))
    });
    assert!(matches!(result, Err(Error::Config(message)) if message == "query failed"));
    assert!(core.active.get());
    drop(guard);
    assert!(!core.active.get());
}
