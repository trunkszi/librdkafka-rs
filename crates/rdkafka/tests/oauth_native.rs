//! Native OAuth installation preserves expiry validation and failure recovery.
#![cfg(feature = "http")]
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use rdkafka::{Conf, OAuthBearerToken, OAuthBearerTokenSetter, Producer, Result};

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
