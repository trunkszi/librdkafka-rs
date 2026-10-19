#![cfg(feature = "http")]
#[path = "support/http_server.rs"]
mod http_server;
use rdkafka::{
    Result,
    http::{HttpTransport, TlsSettings},
    oauth::{OAuthBearerConfig, OAuthBearerHttpClient, OAuthBearerToken, OAuthBearerTokenSetter},
};

#[derive(Default)]
struct Target {
    token: Option<String>,
    failed: bool,
}
impl OAuthBearerTokenSetter for Target {
    fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> Result<()> {
        assert!(token.expires_at_ms > 0);
        assert_eq!(token.scope, ["read", "write"]);
        assert_eq!(
            token.extensions.get("tenant").map(String::as_str),
            Some("a")
        );
        self.token = Some(token.access_token.clone());
        Ok(())
    }
    fn oauthbearer_set_token_failure(&mut self, _: &str) -> Result<()> {
        self.token = None;
        self.failed = true;
        Ok(())
    }
}
#[test]
fn integration_scenario_refresh_encodes_credentials_and_installs_token() {
    let (url, server) = http_server::serve(vec![(200, r#"{"access_token":"secret-token","expires_in":3600,"scope":"read write","extensions":{"tenant":"a"}}"#.into())]);
    let mut config = OAuthBearerConfig::new(url, "id +", "password&");
    config.allow_insecure_loopback_http = true;
    config.scope = vec!["read".into()];
    let mut client =
        OAuthBearerHttpClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), config);
    let mut target = Target::default();
    client.refresh(&mut target).unwrap();
    assert_eq!(target.token.as_deref(), Some("secret-token"));
    assert!(!target.failed);
    let requests = server.join().unwrap();
    assert!(requests[0].contains("client_id=id%20%2B"));
    assert!(requests[0].contains("client_secret=password%26"));
}
#[test]
fn integration_scenario_refresh_rejects_expiry_and_sanitizes_endpoint_errors() {
    for (status, body) in [
        (200, r#"{"access_token":"hidden-token","expires_in":0}"#),
        (
            200,
            r#"{"access_token":"hidden-token","expires_in":18446744073709551615}"#,
        ),
        (401, "password hidden-token"),
    ] {
        let (url, server) = http_server::serve(vec![(status, body.into())]);
        let mut config = OAuthBearerConfig::new(url, "id", "password");
        config.allow_insecure_loopback_http = true;
        let mut client =
            OAuthBearerHttpClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), config);
        let mut target = Target {
            token: Some("old".into()),
            failed: false,
        };
        let error = client.refresh(&mut target).unwrap_err().to_string();
        assert!(target.failed && target.token.is_none());
        assert!(!error.contains("password") && !error.contains("hidden-token"));
        server.join().unwrap();
    }
}
#[test]
fn integration_scenario_insecure_endpoint_fails_closed_before_network() {
    let mut client = OAuthBearerHttpClient::new(
        HttpTransport::new(TlsSettings::default()).unwrap(),
        OAuthBearerConfig::new("http://127.0.0.1:1/token", "id", "password"),
    );
    let mut target = Target::default();
    assert!(
        client
            .refresh(&mut target)
            .unwrap_err()
            .to_string()
            .contains("HTTPS")
    );
    assert!(target.failed);
}
#[test]
fn integration_scenario_native_installation_rejection_notifies_failure() {
    struct RejectingTarget(bool);
    impl OAuthBearerTokenSetter for RejectingTarget {
        fn oauthbearer_set_token(&mut self, _: &OAuthBearerToken) -> Result<()> {
            Err(rdkafka::Error::OAuth("hidden-token rejected".into()))
        }
        fn oauthbearer_set_token_failure(&mut self, _: &str) -> Result<()> {
            self.0 = true;
            Ok(())
        }
    }
    let (url, server) = http_server::serve(vec![(
        200,
        r#"{"access_token":"hidden-token","expires_in":3600}"#.into(),
    )]);
    let mut config = OAuthBearerConfig::new(url, "id", "password");
    config.allow_insecure_loopback_http = true;
    let mut client =
        OAuthBearerHttpClient::new(HttpTransport::new(TlsSettings::default()).unwrap(), config);
    let mut target = RejectingTarget(false);
    assert!(
        !client
            .refresh(&mut target)
            .unwrap_err()
            .to_string()
            .contains("hidden-token")
    );
    assert!(target.0);
    server.join().unwrap();
}
