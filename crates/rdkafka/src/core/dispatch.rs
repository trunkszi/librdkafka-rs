//! Ordered callback delivery and deferred native or transport failures.
use super::*;

struct RebalanceScope<'a>(&'a RefCell<Option<(bool, String)>>);
impl Drop for RebalanceScope<'_> {
    fn drop(&mut self) {
        self.0.borrow_mut().take();
    }
}

impl Core {
    pub(super) fn dispatch(&self, events: Vec<ffi::NativeEvent>) -> Result<usize> {
        macro_rules! record {
            ($error:expr) => {{
                let error = $error;
                self.pending_error.borrow_mut().get_or_insert(error);
            }};
        }
        macro_rules! invoke {
            ($slot:ident, $($arg:expr),*) => {{
                with_callback(&self.callbacks, |state| &mut state.$slot, |cb| cb($($arg),*));
            }}
        }
        self.pending_events.borrow_mut().extend(events);
        let n = self.pending_events.borrow().len();
        loop {
            let Some(e) = self.pending_events.borrow_mut().pop_front() else {
                break;
            };
            if e.kind == 12 {
                record!(Error::Native {
                    code: ErrorCode(e.code),
                    message: e.message,
                    retriable: true,
                    fatal: false,
                    txn_requires_abort: false,
                });
                continue;
            }
            match e.kind {
                13 => {
                    self.pending_tls_error
                        .borrow_mut()
                        .get_or_insert_with(|| Error::Tls(e.message.clone()));
                    invoke!(
                        event,
                        &Event::Error {
                            code: e.code,
                            message: e.message.clone(),
                            is_fatal: true
                        }
                    );
                }
                0 => invoke!(delivery, &DeliveryReport::from_native(&e)),
                1..=4 => invoke!(event, &Event::from_native(&e)),
                5 => {
                    *self.rebalance_snapshot.borrow_mut() =
                        Some((e.assignment_lost, e.rebalance_protocol));
                    let _scope = RebalanceScope(&self.rebalance_snapshot);
                    invoke!(rebalance, e.code, &from_parts(e.partitions));
                }
                6 => invoke!(commit, e.code, &from_parts(e.partitions)),
                7 => {
                    if let Some(Err(error)) = with_callback(
                        &self.callbacks,
                        |state| &mut state.oauth,
                        |cb| cb(&e.message),
                    ) {
                        record!(error);
                    }
                }
                8 => invoke!(socket, e.code, e.partition, e.offset as i32),
                9 => invoke!(open, &e.message, e.code, e.partition),
                10 => {
                    if let Err(error) = status(e.code) {
                        record!(error);
                        continue;
                    }
                    let mut headers = Headers::new();
                    for header in e.headers {
                        headers.add(
                            header.name,
                            header.has_value.then_some(header.value.as_slice()),
                        );
                    }
                    self.observe(&Message {
                        topic: e.topic,
                        partition: e.partition,
                        offset: e.offset,
                        timestamp: (e.timestamp >= 0).then_some(e.timestamp),
                        key: e.has_key.then_some(e.key),
                        payload: e.has_payload.then_some(e.payload),
                        headers,
                        token: e.token,
                    });
                }
                _ => {}
            }
        }
        self.take_pending_error().map(|()| n)
    }
    /// Reports a deferred callback failure before another receive advances offsets.
    pub fn take_pending_error(&self) -> Result<()> {
        self.pending_tls_error
            .borrow_mut()
            .take()
            .or_else(|| self.pending_error.borrow_mut().take())
            .map_or(Ok(()), Err)
    }
    pub(super) fn finish_received<T>(
        &self,
        outcome: Result<T>,
        callbacks: Result<usize>,
        nonempty: impl FnOnce(&T) -> bool,
    ) -> Result<T> {
        if let Ok(value) = &outcome
            && nonempty(value)
        {
            if let Err(error) = callbacks {
                let pending = if matches!(error, Error::Tls(_)) {
                    &self.pending_tls_error
                } else {
                    &self.pending_error
                };
                pending.borrow_mut().get_or_insert(error);
            }
            return outcome;
        }
        match callbacks {
            Err(error @ Error::Tls(_)) => Err(error),
            callbacks => outcome.and_then(|value| callbacks.map(|_| value)),
        }
    }
    pub fn observe(&self, m: &Message) {
        with_callback(&self.callbacks, |state| &mut state.consume, |cb| cb(m));
    }
}

#[cfg(test)]
mod tests {
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
            pending_error: RefCell::default(),
            pending_tls_error: RefCell::default(),
            partition_counts: RefCell::default(),
            pending_events: RefCell::default(),
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
        assert!(
            matches!(core.take_pending_error(), Err(Error::Config(message)) if message == "token refresh failed")
        );
        assert!(core.take_pending_error().is_ok());
    }

    #[test]
    fn unit_scenario_first_callback_error_survives_later_tls_without_dropping_events() {
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
            let retained = core.take_pending_error();
            match kind {
                7 => assert!(
                    matches!(retained, Err(Error::Config(message)) if message == "refresh failed")
                ),
                10 | 12 => assert!(matches!(retained, Err(Error::Native { .. }))),
                _ => unreachable!(),
            }
            assert_eq!(deliveries.get(), 2);
            assert!(core.take_pending_error().is_ok());
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

    #[test]
    fn unit_scenario_received_record_defers_callback_failure_without_loss() {
        let core = core();
        for error in [
            Error::Tls("bad certificate".into()),
            Error::Config("oauth refresh".into()),
        ] {
            let fetched = core
                .finish_received(Ok(Some(42)), Err(error), Option::is_some)
                .unwrap();
            assert_eq!(fetched, Some(42));
            assert!(core.take_pending_error().is_err());
            assert!(core.take_pending_error().is_ok());
        }
        let empty = core.finish_received(
            Ok(None::<i32>),
            Err(Error::Tls("offline".into())),
            Option::is_some,
        );
        assert!(matches!(empty, Err(Error::Tls(_))));
    }

    #[test]
    fn unit_scenario_callback_slots_restore_after_unwind_and_resume_queued_events() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        for kind in [0, 1, 5, 6, 7, 8, 9, 10] {
            let core = core();
            {
                let mut callbacks = core.callbacks.borrow_mut();
                callbacks.delivery = Some(Box::new(|_| panic!("delivery")));
                callbacks.event = Some(Box::new(|_| panic!("event")));
                callbacks.rebalance = Some(Box::new(|_, _| panic!("rebalance")));
                callbacks.commit = Some(Box::new(|_, _| panic!("commit")));
                callbacks.oauth = Some(Box::new(|_| panic!("oauth")));
                callbacks.socket = Some(Box::new(|_, _, _| panic!("socket")));
                callbacks.open = Some(Box::new(|_, _, _| panic!("open")));
                callbacks.consume = Some(Box::new(|_| panic!("consume")));
            }
            assert!(
                catch_unwind(AssertUnwindSafe(
                    || core.dispatch(vec![event(kind, ""), event(0, "")])
                ))
                .is_err()
            );
            let state = core.callbacks.borrow();
            assert!(state.delivery.is_some() && state.event.is_some() && state.rebalance.is_some());
            assert!(state.commit.is_some() && state.oauth.is_some() && state.consume.is_some());
            assert!(state.socket.is_some() && state.open.is_some());
            drop(state);
            assert!(core.rebalance_snapshot.borrow().is_none());
            let delivered = Rc::new(Cell::new(0));
            let seen = Rc::clone(&delivered);
            core.callbacks.borrow_mut().delivery =
                Some(Box::new(move |_| seen.set(seen.get() + 1)));
            core.dispatch(Vec::new()).unwrap();
            assert_eq!(delivered.get(), 1);
        }
    }

    #[test]
    fn unit_scenario_nonempty_receive_batch_retains_every_record_before_deferred_error() {
        let core = core();
        for error in [
            Error::Tls("bad certificate".into()),
            Error::Config("oauth refresh".into()),
        ] {
            let delivered = core
                .finish_received(Ok(vec![7, 11, 13]), Err(error), |batch| !batch.is_empty())
                .unwrap();
            assert_eq!(delivered, [7, 11, 13]);
            assert!(core.take_pending_error().is_err());
            assert!(core.take_pending_error().is_ok());
        }
        let empty = core.finish_received(
            Ok(Vec::<i32>::new()),
            Err(Error::Config("oauth refresh".into())),
            |batch| !batch.is_empty(),
        );
        assert!(matches!(empty, Err(Error::Config(message)) if message == "oauth refresh"));
    }

    #[test]
    fn unit_scenario_fetched_message_survives_oauth_and_tls_dispatch_failures() {
        let core = core();
        core.callbacks.borrow_mut().oauth =
            Some(Box::new(|_| Err(Error::Config("refresh failed".into()))));
        for kind in [7, 13] {
            let message = Message {
                topic: "orders".into(),
                partition: 2,
                offset: 42,
                timestamp: Some(123),
                key: Some(b"key".to_vec()),
                payload: Some(b"unique-record".to_vec()),
                headers: Headers::new(),
                token: 0,
            };
            let callbacks = core.dispatch(vec![event(kind, "certificate rejected")]);
            let fetched = core
                .finish_received(Ok(Some(message)), callbacks, Option::is_some)
                .unwrap()
                .unwrap();
            assert_eq!(fetched.offset, 42);
            assert_eq!(fetched.payload.as_deref(), Some(&b"unique-record"[..]));
            let deferred = core.poll(0);
            if kind == 7 {
                assert!(
                    matches!(deferred, Err(Error::Config(message)) if message == "refresh failed")
                );
            } else {
                assert!(
                    matches!(deferred, Err(Error::Tls(message)) if message == "certificate rejected")
                );
            }
            assert!(!core.active.get());
        }
    }

    #[test]
    fn integration_scenario_receive_observer_failures_are_drained_and_deferred_same_call()
    -> Result<()> {
        let mut host = Conf::new();
        host.set("test.mock.num.brokers", "1")?;
        let producer = Producer::new(&host)?;
        let topic_name = format!("observer-drain-{}", std::process::id());
        let mut record = ProduceRecord::new(&topic_name, Some(b"observer-record"));
        record.partition = 0;
        producer.produce(record)?;
        producer.flush(10000)?;
        for mode in 0..5 {
            let mut config = Conf::new();
            config.set("bootstrap.servers", &producer.mock_bootstraps())?;
            if mode < 2 || mode == 4 {
                config.set("group.id", &format!("observer-drain-{mode}"))?;
            }
            let (consumer, simple) = if mode < 2 || mode == 4 {
                (Some(Consumer::new(&config)?), None)
            } else {
                (None, Some(SimpleConsumer::new(&config)?))
            };
            let owner = consumer
                .as_ref()
                .map_or_else(|| &simple.as_ref().unwrap().core, |c| &c.core);
            let weak = Rc::downgrade(owner);
            owner.callbacks.borrow_mut().consume = Some(Box::new(move |_| {
                weak.upgrade()
                    .unwrap()
                    .pending_events
                    .borrow_mut()
                    .push_back(event(13, "observer TLS failure"));
            }));
            let native_topic = if let Some(simple) = &simple {
                Some(Topic::for_consumer(simple, &topic_name, &Conf::topic())?)
            } else {
                None
            };
            let queue = if mode == 3 {
                Some(Queue::new(simple.as_ref().unwrap())?)
            } else {
                None
            };
            if let Some(consumer) = &consumer {
                consumer.assign(&[TopicPartition::new(&topic_name, 0, 0)])?;
            } else if let Some(queue) = &queue {
                simple.as_ref().unwrap().start_queue(
                    native_topic.as_ref().unwrap(),
                    0,
                    Offset::Beginning,
                    queue,
                )?;
            } else {
                simple.as_ref().unwrap().start(
                    native_topic.as_ref().unwrap(),
                    0,
                    Offset::Beginning,
                )?;
            }
            let receive = || -> Result<Option<Vec<u8>>> {
                match mode {
                    0 => consumer
                        .as_ref()
                        .unwrap()
                        .consume(100)
                        .map(|m| m.and_then(|m| m.payload)),
                    1 => consumer
                        .as_ref()
                        .unwrap()
                        .consume_native(100)
                        .map(|m| m.and_then(|m| m.to_owned().payload)),
                    2 => simple
                        .as_ref()
                        .unwrap()
                        .consume(native_topic.as_ref().unwrap(), 0, 100)
                        .map(|m| m.and_then(|m| m.payload)),
                    4 => consumer
                        .as_ref()
                        .unwrap()
                        .consume_batch(1, 100)
                        .and_then(|batch| {
                            batch.get(0).map(|record| {
                                record.and_then(|record| record.payload.map(<[u8]>::to_vec))
                            })
                        }),
                    _ => queue
                        .as_ref()
                        .unwrap()
                        .consume(100)
                        .map(|m| m.and_then(|m| m.payload)),
                }
            };
            let until = std::time::Instant::now() + std::time::Duration::from_secs(15);
            loop {
                if let Some(payload) = receive()? {
                    assert_eq!(payload, b"observer-record");
                    break;
                }
                assert!(
                    std::time::Instant::now() < until,
                    "mode {mode} did not receive record"
                );
            }
            assert!(
                matches!(receive(), Err(Error::Tls(message)) if message == "observer TLS failure")
            );
            if let Some(simple) = &simple {
                simple.stop(native_topic.as_ref().unwrap(), 0)?;
            }
            if let Some(consumer) = &consumer {
                consumer.close()?;
            }
        }
        Ok(())
    }

    #[test]
    fn unit_scenario_tls_error_survives_panicking_error_observer() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let core = core();
        core.callbacks.borrow_mut().event = Some(Box::new(|_| panic!("error observer")));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                core.dispatch(vec![event(13, "certificate rejected"), event(0, "")])
            }))
            .is_err()
        );
        assert!(
            matches!(core.poll(0), Err(Error::Tls(message)) if message == "certificate rejected")
        );
        assert!(!core.active.get());
        let delivered = Rc::new(Cell::new(0));
        let seen = Rc::clone(&delivered);
        core.callbacks.borrow_mut().delivery = Some(Box::new(move |_| seen.set(seen.get() + 1)));
        core.dispatch(Vec::new()).unwrap();
        assert_eq!(delivered.get(), 1);
        assert!(core.take_pending_error().is_ok());
    }

    #[test]
    fn unit_scenario_callback_failure_survives_later_observer_unwind() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let core = core();
        core.callbacks.borrow_mut().oauth = Some(Box::new(|_| {
            Err(Error::Config("first refresh failed".into()))
        }));
        core.callbacks.borrow_mut().delivery = Some(Box::new(|_| panic!("delivery observer")));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                core.dispatch(vec![event(7, "refresh"), event(0, "")])
            }))
            .is_err()
        );
        assert!(
            matches!(core.poll(0), Err(Error::Config(message)) if message == "first refresh failed")
        );
        assert!(core.take_pending_error().is_ok());
    }

    #[test]
    fn unit_scenario_pending_error_survives_receive_observer_and_later_tls_unwind() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let core = core();
        *core.pending_error.borrow_mut() = Some(Error::Config("first pending failure".into()));
        core.callbacks.borrow_mut().consume = Some(Box::new(|_| panic!("consume observer")));
        let message = Message {
            topic: "orders".into(),
            partition: 0,
            offset: 42,
            timestamp: None,
            key: None,
            payload: None,
            headers: Headers::new(),
            token: 0,
        };
        assert!(catch_unwind(AssertUnwindSafe(|| core.observe(&message))).is_err());
        core.callbacks.borrow_mut().event = Some(Box::new(|_| panic!("TLS observer")));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                core.dispatch(vec![event(13, "later TLS failure")])
            }))
            .is_err()
        );
        assert!(
            matches!(core.take_pending_error(), Err(Error::Tls(message)) if message == "later TLS failure")
        );
        assert!(
            matches!(core.dispatch(Vec::new()), Err(Error::Config(message)) if message == "first pending failure")
        );
        assert!(core.take_pending_error().is_ok());
    }

    #[test]
    fn unit_scenario_first_oauth_failure_survives_later_tls_observer_panic() {
        use std::panic::{AssertUnwindSafe, catch_unwind};
        let core = core();
        core.callbacks.borrow_mut().oauth = Some(Box::new(|_| {
            Err(Error::Config("first refresh failed".into()))
        }));
        core.callbacks.borrow_mut().event = Some(Box::new(|_| panic!("TLS observer")));
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                core.dispatch(vec![event(7, "refresh"), event(13, "later TLS failure")])
            }))
            .is_err()
        );
        assert!(
            matches!(core.take_pending_error(), Err(Error::Tls(message)) if message == "later TLS failure")
        );
        assert!(
            matches!(core.dispatch(Vec::new()), Err(Error::Config(message)) if message == "first refresh failed")
        );
        assert!(core.take_pending_error().is_ok());
    }

    #[test]
    fn integration_scenario_successful_receive_defers_tls_before_retained_oauth_failure()
    -> Result<()> {
        let mut conf = Conf::new();
        conf.set("test.mock.num.brokers", "1")?;
        let core = Core::new(&conf, 0)?;
        core.callbacks.borrow_mut().oauth =
            Some(Box::new(|_| Err(Error::Config("refresh failed".into()))));
        core.pending_events
            .borrow_mut()
            .extend([event(7, "refresh"), event(13, "certificate rejected")]);
        assert_eq!(
            core.complete_received(Ok(vec![7, 11]), |batch| !batch.is_empty())?,
            [7, 11]
        );
        assert!(
            matches!(core.complete(Ok(())), Err(Error::Tls(message)) if message == "certificate rejected")
        );
        assert!(
            matches!(core.complete(Ok(())), Err(Error::Config(message)) if message == "refresh failed")
        );
        core.take_pending_error()?;
        Ok(())
    }
}
