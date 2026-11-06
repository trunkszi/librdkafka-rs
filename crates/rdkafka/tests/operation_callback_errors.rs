use std::{cell::Cell, rc::Rc};

use rdkafka::{Conf, Error, ProduceRecord, Producer, Result};

#[test]
fn flush_error_preserves_oauth_failure() -> Result<()> {
    let called = Rc::new(Cell::new(0));
    let observed = Rc::clone(&called);
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "127.0.0.1:1")?
        .set("security.protocol", "SASL_PLAINTEXT")?
        .set("sasl.mechanism", "OAUTHBEARER")?;
    conf.oauthbearer_token_refresh_cb(move |_| {
        observed.set(observed.get() + 1);
        Err(Error::OAuth("refresh rejected".into()))
    });
    let producer = Producer::new(&conf)?;
    producer.produce(ProduceRecord::new("no-broker", Some(b"pending")))?;
    let flushed = producer.flush(0);
    assert!(matches!(flushed, Err(error) if error.code().is_some_and(|c| c.value()==-185)));
    assert_eq!(
        called.get(),
        1,
        "the refresh callback ran during completion"
    );
    assert!(
        matches!(producer.poll(0), Err(Error::OAuth(message)) if message == "refresh rejected"),
        "native timeout must not erase the callback failure"
    );
    Ok(())
}
