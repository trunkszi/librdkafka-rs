//! Real curl/rustls HTTP/2 requests against a synchronous controlled TLS peer.
#![cfg(feature = "http")]
#[path = "support/h2_server.rs"]
mod server;
use rdkafka::{
    HttpTransport, TlsSettings,
    http::{HttpRequest, HttpVersionChoice},
    oauth::{OAuthBearerConfig, OAuthBearerHttpClient, OAuthBearerToken, OAuthBearerTokenSetter},
};

#[cfg(target_os = "linux")]
#[test]
fn integration_scenario_http2_custom_ca_sni_and_mutual_tls() {
    let mut server = server::Server::new(b"verified h2", true).unwrap();
    let settings = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(server.ca_pem.clone())
        .with_server_name("fixture.test")
        .with_client_identity(server.client_cert.clone(), server.client_key.clone());
    let mut transport = HttpTransport::new(settings).unwrap();
    let response = transport
        .execute(HttpRequest::new("GET", server.url()))
        .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.version, Some(HttpVersionChoice::Http2Tls));
    assert_eq!(response.body, b"verified h2");
    drop(transport);
    assert!(server.finish().unwrap().client_authenticated);
}

#[test]
fn integration_scenario_http2_oidc_installs_on_calling_thread() {
    let mut server = server::Server::new(br#"{"access_token":"opaque-token","expires_in":120,"scope":"read write","token_type":"Bearer"}"#, false).unwrap();
    let tls = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(server.ca_pem.clone())
        .with_server_name("fixture.test");
    let transport = HttpTransport::new(tls).unwrap();
    let config = OAuthBearerConfig::new(server.url(), "client", "secret&value");
    let mut client = OAuthBearerHttpClient::new(transport, config);
    struct Setter {
        owner: std::thread::ThreadId,
        token: Option<String>,
    }
    impl OAuthBearerTokenSetter for Setter {
        fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> rdkafka::Result<()> {
            assert_eq!(self.owner, std::thread::current().id());
            self.token = Some(token.access_token.clone());
            Ok(())
        }
        fn oauthbearer_set_token_failure(&mut self, _: &str) -> rdkafka::Result<()> {
            panic!("valid TLS token request failed")
        }
    }
    let mut setter = Setter {
        owner: std::thread::current().id(),
        token: None,
    };
    client.refresh(&mut setter).unwrap();
    assert_eq!(setter.token.as_deref(), Some("opaque-token"));
    drop(client);
    let observed = server.finish().unwrap();
    let body = String::from_utf8(observed.body).unwrap();
    assert!(body.contains("client_secret=secret%26value"));
}

#[test]
fn integration_scenario_tls_rejects_untrusted_and_wrong_peer_name() {
    for trusted in [false, true] {
        let server = server::Server::new(b"must not be accepted", false).unwrap();
        let settings = if trusted {
            TlsSettings::new()
                .with_native_roots(false)
                .with_ca_pem(server.ca_pem.clone())
        } else {
            TlsSettings::new()
        };
        let mut transport = HttpTransport::new(settings).unwrap();
        assert!(
            transport
                .execute(HttpRequest::new("GET", server.url()))
                .is_err()
        );
    }
}

#[test]
fn integration_scenario_tls_requires_client_identity_and_rejects_mismatched_keys() {
    let server = server::Server::new(b"must require identity", true).unwrap();
    let settings = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(server.ca_pem.clone())
        .with_server_name("fixture.test");
    let mut transport = HttpTransport::new(settings).unwrap();
    assert!(
        transport
            .execute(HttpRequest::new("GET", server.url()))
            .is_err()
    );
    drop(transport);
    let other = server::Server::new(b"unused peer", false).unwrap();
    let settings = TlsSettings::new()
        .with_native_roots(false)
        .with_ca_pem(server.ca_pem.clone())
        .with_client_identity(server.client_cert.clone(), other.client_key.clone());
    assert!(settings.build_client_config().is_err());
}
