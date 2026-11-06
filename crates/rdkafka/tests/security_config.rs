//! Security configuration rejects missing trust before any broker connection.
use rdkafka::{Conf, Error, Producer, Result};

#[cfg(feature = "tls")]
#[test]
fn integration_scenario_tls_protocol_never_falls_back_without_settings() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("security.protocol", "SASL_SSL")?;
    conf.set("sasl.mechanism", "PLAIN")?
        .set("sasl.username", "user")?
        .set("sasl.password", "secret")?;
    assert!(matches!(Producer::new(&conf), Err(Error::Tls(_))));
    assert_eq!(conf.get("security.protocol")?, "sasl_ssl");
    assert!(
        conf.dump()
            .iter()
            .any(|(key, value)| key == "security.protocol" && value == "sasl_ssl")
    );
    Ok(())
}

#[cfg(feature = "tls")]
#[test]
fn integration_scenario_invalid_tls_settings_do_not_enable_plaintext() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("security.protocol", "SSL")?;
    let invalid = rdkafka::TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(b"invalid certificate".to_vec());
    assert!(matches!(conf.tls_settings(invalid), Err(Error::Tls(_))));
    assert!(matches!(Producer::new(&conf), Err(Error::Tls(_))));
    Ok(())
}

#[cfg(not(feature = "tls"))]
#[test]
fn integration_scenario_tls_protocol_requires_feature() -> Result<()> {
    let mut conf = Conf::new();
    assert!(matches!(
        conf.set("security.protocol", "SSL"),
        Err(Error::Tls(_))
    ));
    conf.set("test.mock.num.brokers", "1")?;
    let producer = Producer::new(&conf)?;
    producer.flush(1000)?;
    Ok(())
}
