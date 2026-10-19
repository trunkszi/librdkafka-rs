//! Native token installation for caller-thread HTTP refresh clients.
use crate::{Consumer, OAuthBearerToken, OAuthBearerTokenSetter, Producer, Result, SimpleConsumer};

macro_rules! token_target {
    ($($client:ty),*) => {$(
        impl OAuthBearerTokenSetter for $client {
            fn oauthbearer_set_token(&mut self, token: &OAuthBearerToken) -> Result<()> {
                let extensions = token.extensions.iter()
                    .flat_map(|(key, value)| [key.clone(), value.clone()])
                    .collect::<Vec<_>>();
                <$client>::oauthbearer_set_token(
                    self, &token.access_token, token.expires_at_ms,
                    &token.principal, &extensions,
                )
            }
            fn oauthbearer_set_token_failure(&mut self, reason: &str) -> Result<()> {
                <$client>::oauthbearer_set_token_failure(self, reason)
            }
        }
    )*};
}
token_target!(Producer, Consumer, SimpleConsumer);
