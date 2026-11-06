use std::{cell::RefCell, rc::Rc};

use rdkafka::{Conf, Headers, ProduceRecord, Producer, PurgeFlags, Result};

#[test]
fn queue_full_keeps_retry_token_distinct() -> Result<()> {
    let delivered = Rc::new(RefCell::new(Vec::new()));
    let observed = Rc::clone(&delivered);
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "127.0.0.1:1")?
        .set("queue.buffering.max.messages", "1")?
        .set("message.timeout.ms", "30000")?;
    conf.delivery_report_cb(move |report| observed.borrow_mut().push(report.token));
    let producer = Producer::new(&conf)?;
    let mut headers = Headers::new();
    headers
        .add("duplicate", Some(b"first"))
        .add("duplicate", None);
    let send = |token| {
        producer.produce(ProduceRecord {
            topic: "queue-limit",
            partition: 0,
            key: Some(b"key"),
            payload: Some(b"payload"),
            timestamp: None,
            headers: Some(&headers),
            token,
        })
    };
    send(11)?;
    assert!(matches!(send(22), Err(error) if error.is_queue_full()));
    producer.purge(PurgeFlags::QUEUE)?;
    producer.poll(0)?;
    assert_eq!(&*delivered.borrow(), &[11]);
    send(22)?;
    producer.purge(PurgeFlags::QUEUE)?;
    producer.poll(0)?;
    assert_eq!(&*delivered.borrow(), &[11, 22]);
    assert_eq!(producer.outq_len(), 0);
    Ok(())
}
