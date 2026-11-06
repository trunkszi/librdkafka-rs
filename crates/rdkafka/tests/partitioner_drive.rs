use std::{
    cell::{Cell, RefCell},
    rc::{Rc, Weak},
};

use rdkafka::{Conf, Error, ProduceRecord, Producer, Result};

#[test]
fn partitioner_rejects_recursive_poll() -> Result<()> {
    let target = Rc::new(RefCell::new(Weak::<Producer>::new()));
    let captured = Rc::clone(&target);
    let saw_guard = Rc::new(Cell::new(false));
    let observed = Rc::clone(&saw_guard);
    let mut conf = Conf::new();
    conf.set("test.mock.num.brokers", "1")?;
    conf.partitioner_cb(move |_, _, _| {
        if let Some(producer) = captured.borrow().upgrade() {
            observed.set(matches!(producer.poll(0), Err(Error::ReentrantDrive)));
        }
        0
    });
    let producer = Rc::new(Producer::new(&conf)?);
    *target.borrow_mut() = Rc::downgrade(&producer);
    let mut prime = ProduceRecord::new("recursive-partitioner", Some(b"prime"));
    prime.partition = 0;
    producer.produce(prime)?;
    producer.flush(10000)?;
    producer.metadata(Some("recursive-partitioner"), 10000)?;
    producer.produce(ProduceRecord::new("recursive-partitioner", Some(b"next")))?;
    assert!(
        saw_guard.get(),
        "partitioner callbacks must not recursively drive their producer"
    );
    producer.flush(10000)?;
    Ok(())
}
