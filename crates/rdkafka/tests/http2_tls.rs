//! Real Nexora/rustls HTTP/2 requests against a synchronous controlled TLS peer.
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

#[test]
fn integration_scenario_tls_identity_override_preserves_http2_authority_on_the_wire() {
    for version in [
        HttpVersionChoice::Negotiate,
        HttpVersionChoice::Http2Tls,
        HttpVersionChoice::Http2PriorKnowledge,
    ] {
        let mut peer = server::Server::new(b"routed", false).unwrap();
        let settings = TlsSettings::new()
            .with_native_roots(false)
            .with_ca_pem(peer.ca_pem.clone())
            .with_server_name("fixture.test");
        let mut transport = HttpTransport::with_preferences(
            settings,
            rdkafka::http::HttpPreferences {
                version,
                ..rdkafka::http::HttpPreferences::default()
            },
        )
        .unwrap();
        let response = transport
            .execute(HttpRequest::new("GET", peer.url()))
            .unwrap();
        assert_eq!(response.body, b"routed");
        drop(transport);
        let observed = peer.finish().unwrap();
        assert_eq!(observed.server_name.as_deref(), Some("fixture.test"));
        let authority = observed
            .headers
            .iter()
            .find(|(name, _)| name == ":authority")
            .map(|(_, value)| value.as_str());
        assert_eq!(authority, Some(peer.address.to_string().as_str()));
        assert!(!observed.headers.iter().any(|(name, _)| name == "host"));
    }
}

#[test]
fn integration_scenario_http2_upload_and_receive_limits_are_independent() {
    for reply in [b"".as_slice(), b"x".as_slice()] {
        let mut peer = server::Server::new(reply, false).unwrap();
        let settings = TlsSettings::new()
            .with_native_roots(false)
            .with_ca_pem(peer.ca_pem.clone())
            .with_server_name("fixture.test");
        let mut transport = HttpTransport::new(settings).unwrap();
        let mut request = HttpRequest::new("POST", peer.url());
        request.body = b"12345".to_vec();
        request.max_response_bytes = 0;
        let response = transport.execute(request);
        drop(transport);
        let observed = peer.finish();
        if reply.is_empty() {
            assert!(response.unwrap().body.is_empty());
        } else {
            assert!(
                response
                    .unwrap_err()
                    .to_string()
                    .contains("HTTP response body exceeds configured size limit")
            );
        }
        assert_eq!(observed.unwrap().body, b"12345");
    }
}

#[test]
fn integration_scenario_decoded_hpack_response_headers_keep_their_limit_diagnostic() {
    for count in [1_000, 5_000] {
        let mut block = vec![0x88];
        block.extend(std::iter::repeat_n(0x90, count));
        assert!(block.len() < 65_536);
        let mut peer = server::Server::with_response_header_block(b"", false, Some(block)).unwrap();
        let settings = TlsSettings::new()
            .with_native_roots(false)
            .with_ca_pem(peer.ca_pem.clone())
            .with_server_name("fixture.test");
        let mut transport = HttpTransport::new(settings).unwrap();
        let response = transport.execute(HttpRequest::new("GET", peer.url()));
        drop(transport);
        peer.finish().unwrap();
        if count == 1_000 {
            assert_eq!(response.unwrap().status, 200);
        } else {
            assert!(
                response
                    .unwrap_err()
                    .to_string()
                    .contains("HTTP response headers exceed configured size limit")
            );
        }
    }
}
