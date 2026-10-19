//! Native OAuth installation preserves expiry validation and failure recovery.
#![cfg(feature = "http")]
#[path = "support/h2_server.rs"]
mod server;

use std::{
    cell::Cell,
    collections::BTreeMap,
    rc::{Rc, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use rdkafka::{
    Conf, HttpTransport, OAuthBearerHttpClient, OAuthBearerToken, OAuthBearerTokenSetter, Producer,
    Result, TlsSettings, oauth::OAuthBearerConfig,
};

#[test]
fn integration_lifecycle_scenario_native_refresh_installs_http2_token_during_poll() -> Result<()> {
    let mut server = server::Server::new(
        br#"{"access_token":"native-http-token","expires_in":120,"scope":"kafka read","token_type":"Bearer","extensions":{"tenant":"fixture"}}"#,
        true,
    )
    .expect("start HTTP/2 TLS token endpoint");
    let tls = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(server.ca_pem.clone())
        .with_server_name("fixture.test")
        .with_client_identity(server.client_cert.clone(), server.client_key.clone());
    let mut config = OAuthBearerConfig::new(server.url(), "native-client", "secret&value");
    config.principal = "native-user".into();
    config.scope = vec!["kafka".into()];
    let mut http = OAuthBearerHttpClient::new(HttpTransport::new(tls)?, config);

    struct NativeTarget {
        handle: Weak<Producer>,
        installations: Rc<Cell<usize>>,
    }
    impl OAuthBearerTokenSetter for NativeTarget {
        fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> Result<()> {
            assert_eq!(token.access_token, "native-http-token");
            assert_eq!(token.principal, "native-user");
            assert_eq!(token.scope, ["kafka", "read"]);
            assert_eq!(
                token.extensions.get("tenant").map(String::as_str),
                Some("fixture")
            );
            let extensions = token
                .extensions
                .iter()
                .flat_map(|(name, value)| [name.clone(), value.clone()])
                .collect::<Vec<_>>();
            self.handle
                .upgrade()
                .expect("polling producer is alive")
                .oauthbearer_set_token(
                    &token.access_token,
                    token.expires_at_ms,
                    &token.principal,
                    &extensions,
                )?;
            self.installations.set(self.installations.get() + 1);
            Ok(())
        }
        fn oauthbearer_set_token_failure(&mut self, reason: &str) -> Result<()> {
            self.handle
                .upgrade()
                .expect("polling producer is alive")
                .oauthbearer_set_token_failure(reason)?;
            panic!("trusted HTTP/2 token refresh failed: {reason}")
        }
    }

    let owner = std::thread::current().id();
    let installations = Rc::new(Cell::new(0));
    let callback_calls = Rc::new(Cell::new(0));
    let callback_count = Rc::clone(&callback_calls);
    let target = Rc::new(std::cell::RefCell::new(NativeTarget {
        handle: Weak::new(),
        installations: Rc::clone(&installations),
    }));
    let callback_target = Rc::clone(&target);
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "127.0.0.1:1")?
        .set("security.protocol", "SASL_PLAINTEXT")?
        .set("sasl.mechanisms", "OAUTHBEARER")?;
    conf.oauthbearer_token_refresh_cb(move |_| {
        assert_eq!(owner, std::thread::current().id());
        callback_count.set(callback_count.get() + 1);
        http.refresh(&mut *callback_target.borrow_mut())
    });
    let producer = Rc::new(Producer::new(&conf)?);
    target.borrow_mut().handle = Rc::downgrade(&producer);
    assert_eq!(callback_calls.get(), 0);
    let deadline = Instant::now() + Duration::from_secs(3);
    while installations.get() == 0 && Instant::now() < deadline {
        producer.poll(20)?;
    }
    assert_eq!(
        callback_calls.get(),
        1,
        "native refresh delivered during poll"
    );
    assert_eq!(
        installations.get(),
        1,
        "native handle accepted fetched token"
    );
    let producer_lifetime = Rc::downgrade(&producer);
    drop(producer);
    assert!(
        producer_lifetime.upgrade().is_none(),
        "callback does not retain producer"
    );
    drop(conf);
    let observed = server.finish().expect("join HTTP/2 TLS token endpoint");
    assert!(observed.client_authenticated);
    let body = String::from_utf8(observed.body).expect("form request is UTF-8");
    assert!(body.contains("grant_type=client_credentials"));
    assert!(body.contains("client_id=native-client"));
    assert!(body.contains("client_secret=secret%26value"));
    assert!(body.contains("scope=kafka"));
    Ok(())
}

#[test]
fn integration_lifecycle_scenario_native_oauth_token_target() -> Result<()> {
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "127.0.0.1:1")?
        .set("security.protocol", "SASL_PLAINTEXT")?
        .set("sasl.mechanisms", "OAUTHBEARER")?;
    conf.oauthbearer_token_refresh_cb(|_| Ok(()));
    let mut producer = Producer::new(&conf)?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as i64;
    let mut token = OAuthBearerToken {
        access_token: "test-native-bearer".into(),
        expires_at_ms: now + 60000,
        principal: "native-user".into(),
        scope: vec!["kafka".into()],
        extensions: BTreeMap::from([("tenant".into(), "test".into())]),
    };
    OAuthBearerTokenSetter::oauthbearer_set_token(&mut producer, &token)?;
    token.expires_at_ms = now - 1;
    assert!(OAuthBearerTokenSetter::oauthbearer_set_token(&mut producer, &token).is_err());
    OAuthBearerTokenSetter::oauthbearer_set_token_failure(&mut producer, "refresh unavailable")?;
    token.expires_at_ms = now + 60000;
    OAuthBearerTokenSetter::oauthbearer_set_token(&mut producer, &token)?;
    Ok(())
}

#[path = "support/http_server.rs"]
mod http_server;

#[test]
fn integration_lifecycle_scenario_native_refresh_http_rejection_then_retry_recovers() -> Result<()>
{
    let (url, server) = http_server::serve(vec![
        (401, "private-server-body client-secret access-token".into()),
        (
            200,
            r#"{"access_token":"recovered-token","expires_in":120}"#.into(),
        ),
    ]);
    let mut config = OAuthBearerConfig::new(url, "client", "client-secret");
    config.allow_insecure_loopback_http = true;
    config.principal = "native-user".into();
    let mut http = OAuthBearerHttpClient::new(HttpTransport::new(TlsSettings::default())?, config);
    struct NativeTarget {
        handle: Weak<Producer>,
        failures: Rc<Cell<usize>>,
        installed: Rc<Cell<usize>>,
    }
    impl OAuthBearerTokenSetter for NativeTarget {
        fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> Result<()> {
            assert_eq!(token.access_token, "recovered-token");
            self.handle
                .upgrade()
                .expect("producer alive")
                .oauthbearer_set_token(
                    &token.access_token,
                    token.expires_at_ms,
                    &token.principal,
                    &[],
                )?;
            self.installed.set(self.installed.get() + 1);
            Ok(())
        }
        fn oauthbearer_set_token_failure(&mut self, reason: &str) -> Result<()> {
            assert_eq!(reason, "OAuth bearer refresh failed");
            self.handle
                .upgrade()
                .expect("producer alive")
                .oauthbearer_set_token_failure(reason)?;
            self.failures.set(self.failures.get() + 1);
            Ok(())
        }
    }
    let failures = Rc::new(Cell::new(0));
    let installed = Rc::new(Cell::new(0));
    let callbacks = Rc::new(Cell::new(0));
    let target = Rc::new(std::cell::RefCell::new(NativeTarget {
        handle: Weak::new(),
        failures: Rc::clone(&failures),
        installed: Rc::clone(&installed),
    }));
    let callback_target = Rc::clone(&target);
    let callback_count = Rc::clone(&callbacks);
    let owner = std::thread::current().id();
    let mut conf = Conf::new();
    conf.set("bootstrap.servers", "127.0.0.1:1")?
        .set("security.protocol", "SASL_PLAINTEXT")?
        .set("sasl.mechanisms", "OAUTHBEARER")?;
    conf.oauthbearer_token_refresh_cb(move |_| {
        assert_eq!(owner, std::thread::current().id());
        callback_count.set(callback_count.get() + 1);
        let result = http.refresh(&mut *callback_target.borrow_mut());
        if callback_count.get() == 1 {
            let error = result
                .expect_err("HTTP rejection fails refresh")
                .to_string();
            assert!(error.contains("token endpoint rejected request"));
            for secret in ["private-server-body", "client-secret", "access-token"] {
                assert!(!error.contains(secret));
            }
            Ok(())
        } else {
            result
        }
    });
    let producer = Rc::new(Producer::new(&conf)?);
    target.borrow_mut().handle = Rc::downgrade(&producer);
    assert_eq!(callbacks.get(), 0);
    // Native token failure schedules a refresh after its ten-second retry interval.
    let deadline = Instant::now() + Duration::from_secs(15);
    while installed.get() == 0 && Instant::now() < deadline {
        producer.poll(20)?;
    }
    assert_eq!(failures.get(), 1, "native failure notification accepted");
    assert_eq!(callbacks.get(), 2, "native retry delivered through polling");
    assert_eq!(
        installed.get(),
        1,
        "retry installed the recovered HTTP token"
    );
    let lifetime = Rc::downgrade(&producer);
    drop(producer);
    assert!(lifetime.upgrade().is_none());
    drop(conf);
    let requests = server.join().expect("join OAuth HTTP endpoint");
    assert_eq!(requests.len(), 2);
    assert!(
        requests
            .iter()
            .all(|request| request.starts_with("POST / "))
    );
    Ok(())
}
