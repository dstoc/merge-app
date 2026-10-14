use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt,
    sync::Mutex,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use reqwest::{Client, Response, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::json;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const API_ROOT: &str = "https://api.github.com";
const API_VERSION: &str = "2022-11-28";
const USER_AGENT: &str = "merge-app";
const APP_JWT_LIFETIME: u64 = 9 * 60;
const APP_JWT_IAT_BACKDATE: u64 = 60;
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
pub enum AuthenticationError {
    AppAuthentication(String),
    Installation(String),
    Repository(String),
    Client(String),
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppAuthentication(message) => {
                write!(formatter, "GitHub App authentication failed: {message}")
            }
            Self::Installation(message) => {
                write!(
                    formatter,
                    "GitHub installation authentication failed: {message}"
                )
            }
            Self::Repository(message) => {
                write!(
                    formatter,
                    "GitHub repository authentication failed: {message}"
                )
            }
            Self::Client(message) => write!(formatter, "GitHub API client failed: {message}"),
        }
    }
}

impl Error for AuthenticationError {}

#[derive(Debug, Deserialize)]
pub struct AppInfo {
    pub id: u64,
    pub name: String,
    #[serde(default)]
    pub permissions: BTreeMap<String, String>,
}

pub struct InstallationCredentials {
    pub installation_id: u64,
    pub token: String,
    pub expires_at: String,
    pub permissions: BTreeMap<String, String>,
}

impl Clone for InstallationCredentials {
    fn clone(&self) -> Self {
        Self {
            installation_id: self.installation_id,
            token: self.token.clone(),
            expires_at: self.expires_at.clone(),
            permissions: self.permissions.clone(),
        }
    }
}

impl fmt::Debug for InstallationCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InstallationCredentials")
            .field("installation_id", &self.installation_id)
            .field("token", &"[REDACTED]")
            .field("expires_at", &self.expires_at)
            .field("permissions", &self.permissions)
            .finish()
    }
}

#[derive(Serialize, Deserialize)]
struct AppJwtClaims {
    iat: u64,
    exp: u64,
    iss: String,
}

#[derive(Deserialize)]
struct InstallationInfo {
    id: u64,
}

#[derive(Deserialize)]
struct InstallationTokenResponse {
    token: String,
    expires_at: String,
    #[serde(default)]
    permissions: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TokenCacheKey {
    installation_id: u64,
    repository: String,
}

struct CachedToken {
    credentials: InstallationCredentials,
    expires_at: SystemTime,
}

#[derive(Default)]
struct TokenCache {
    entries: HashMap<TokenCacheKey, CachedToken>,
}

impl TokenCache {
    fn get(&mut self, key: &TokenCacheKey, now: SystemTime) -> Option<InstallationCredentials> {
        self.entries.retain(|_, cached| {
            cached
                .expires_at
                .duration_since(now)
                .is_ok_and(|remaining| remaining > TOKEN_REFRESH_MARGIN)
        });
        self.entries
            .get(key)
            .map(|cached| cached.credentials.clone())
    }

    fn insert(
        &mut self,
        key: TokenCacheKey,
        credentials: InstallationCredentials,
        expires_at: SystemTime,
    ) {
        self.entries.insert(
            key,
            CachedToken {
                credentials,
                expires_at,
            },
        );
    }
}

pub struct GitHubAppAuthenticator {
    app_id: u64,
    issuer: String,
    signing_key: EncodingKey,
    client: Client,
    token_cache: Mutex<TokenCache>,
}

impl GitHubAppAuthenticator {
    pub fn new(
        app_id: u64,
        issuer: impl Into<String>,
        private_key_pem: &[u8],
    ) -> Result<Self, AuthenticationError> {
        if app_id == 0 {
            return Err(AuthenticationError::AppAuthentication(
                "App ID must be greater than zero".to_owned(),
            ));
        }
        let signing_key = EncodingKey::from_rsa_pem(private_key_pem).map_err(|_| {
            AuthenticationError::AppAuthentication(
                "private key must contain a valid RSA PEM".to_owned(),
            )
        })?;
        let client = Client::builder()
            .user_agent(USER_AGENT)
            .build()
            .map_err(|error| AuthenticationError::Client(error.to_string()))?;

        Ok(Self {
            app_id,
            issuer: issuer.into(),
            signing_key,
            client,
            token_cache: Mutex::new(TokenCache::default()),
        })
    }

    pub fn app_id(&self) -> u64 {
        self.app_id
    }

    pub fn http_client(&self) -> Client {
        self.client.clone()
    }

    /// Confirm that the configured signing key authenticates as the expected GitHub App.
    pub async fn verify_app(&self) -> Result<AppInfo, AuthenticationError> {
        let jwt = self.app_jwt()?;
        let response = self
            .client
            .get(format!("{API_ROOT}/app"))
            .bearer_auth(&jwt)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", API_VERSION)
            .send()
            .await
            .map_err(|error| {
                AuthenticationError::AppAuthentication(format!("GET /app request failed: {error}"))
            })?;
        if !response.status().is_success() {
            let message = response_error(response, &jwt).await;
            return Err(AuthenticationError::AppAuthentication(message));
        }
        let app: AppInfo = response.json().await.map_err(|error| {
            AuthenticationError::AppAuthentication(format!(
                "could not decode GET /app response: {error}"
            ))
        })?;
        if app.id != self.app_id {
            return Err(AuthenticationError::AppAuthentication(format!(
                "private key authenticated as App {}, expected App {}",
                app.id, self.app_id
            )));
        }
        Ok(app)
    }

    /// Find the installation that grants this App access to a repository.
    pub async fn repository_installation(
        &self,
        owner: &str,
        repository: &str,
    ) -> Result<u64, AuthenticationError> {
        let jwt = self.app_jwt()?;
        let response = self
            .client
            .get(format!(
                "{API_ROOT}/repos/{owner}/{repository}/installation"
            ))
            .bearer_auth(&jwt)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", API_VERSION)
            .send()
            .await
            .map_err(|error| {
                AuthenticationError::Repository(format!(
                    "installation lookup request failed: {error}"
                ))
            })?;
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(AuthenticationError::AppAuthentication(
                response_error(response, &jwt).await,
            ));
        }
        if !response.status().is_success() {
            return Err(AuthenticationError::Repository(
                response_error(response, &jwt).await,
            ));
        }
        let installation: InstallationInfo = response.json().await.map_err(|error| {
            AuthenticationError::Repository(format!(
                "could not decode repository installation response: {error}"
            ))
        })?;
        if installation.id == 0 {
            return Err(AuthenticationError::Repository(
                "GitHub returned an invalid installation ID".to_owned(),
            ));
        }
        Ok(installation.id)
    }

    /// Return a cached repository-scoped token or mint one with Pull requests: write.
    pub async fn installation_token(
        &self,
        installation_id: u64,
        owner: &str,
        repository: &str,
    ) -> Result<InstallationCredentials, AuthenticationError> {
        if installation_id == 0 {
            return Err(AuthenticationError::Installation(
                "installation ID must be greater than zero".to_owned(),
            ));
        }
        if owner.is_empty() || repository.is_empty() {
            return Err(AuthenticationError::Repository(
                "owner and repository must not be empty".to_owned(),
            ));
        }

        let cache_key = TokenCacheKey {
            installation_id,
            repository: format!("{owner}/{repository}"),
        };
        if let Some(credentials) = self
            .token_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&cache_key, SystemTime::now())
        {
            return Ok(credentials);
        }

        let jwt = self.app_jwt()?;
        let response = self
            .client
            .post(format!(
                "{API_ROOT}/app/installations/{installation_id}/access_tokens"
            ))
            .bearer_auth(&jwt)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", API_VERSION)
            .json(&json!({
                "repositories": [repository],
                "permissions": {"pull_requests": "write"}
            }))
            .send()
            .await
            .map_err(|error| {
                AuthenticationError::Installation(format!(
                    "installation token request failed: {error}"
                ))
            })?;
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(AuthenticationError::AppAuthentication(
                response_error(response, &jwt).await,
            ));
        }
        if response.status() == StatusCode::UNPROCESSABLE_ENTITY {
            return Err(AuthenticationError::Repository(
                response_error(response, &jwt).await,
            ));
        }
        if !response.status().is_success() {
            return Err(AuthenticationError::Installation(
                response_error(response, &jwt).await,
            ));
        }
        let token: InstallationTokenResponse = response.json().await.map_err(|error| {
            AuthenticationError::Installation(format!(
                "could not decode installation token response: {error}"
            ))
        })?;
        let expires_at = parse_expiration(&token.expires_at)?;
        let credentials = InstallationCredentials {
            installation_id,
            token: token.token,
            expires_at: token.expires_at,
            permissions: token.permissions,
        };
        self.token_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(cache_key, credentials.clone(), expires_at);
        Ok(credentials)
    }

    fn app_jwt(&self) -> Result<String, AuthenticationError> {
        create_app_jwt(&self.issuer, &self.signing_key, SystemTime::now())
    }
}

fn create_app_jwt(
    issuer: &str,
    signing_key: &EncodingKey,
    now: SystemTime,
) -> Result<String, AuthenticationError> {
    let now = now
        .duration_since(UNIX_EPOCH)
        .map_err(|_| {
            AuthenticationError::AppAuthentication(
                "system clock is before the Unix epoch".to_owned(),
            )
        })?
        .as_secs();
    let claims = AppJwtClaims {
        iat: now.saturating_sub(APP_JWT_IAT_BACKDATE),
        exp: now.saturating_add(APP_JWT_LIFETIME),
        iss: issuer.to_owned(),
    };
    encode(&Header::new(Algorithm::RS256), &claims, signing_key)
        .map_err(|_| AuthenticationError::AppAuthentication("could not sign App JWT".to_owned()))
}

fn parse_expiration(value: &str) -> Result<SystemTime, AuthenticationError> {
    let parsed = OffsetDateTime::parse(value, &Rfc3339).map_err(|error| {
        AuthenticationError::Installation(format!(
            "GitHub returned an invalid installation token expiry: {error}"
        ))
    })?;
    let timestamp = parsed.unix_timestamp_nanos();
    if timestamp < 0 {
        return Err(AuthenticationError::Installation(
            "GitHub returned an installation token expiry before the Unix epoch".to_owned(),
        ));
    }
    let seconds = u64::try_from(timestamp / 1_000_000_000).map_err(|_| {
        AuthenticationError::Installation(
            "GitHub returned an installation token expiry outside the supported range".to_owned(),
        )
    })?;
    let nanos = (timestamp % 1_000_000_000) as u32;
    UNIX_EPOCH
        .checked_add(Duration::new(seconds, nanos))
        .ok_or_else(|| {
            AuthenticationError::Installation(
                "GitHub returned an installation token expiry outside the supported range"
                    .to_owned(),
            )
        })
}

async fn response_error(response: Response, credential: &str) -> String {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let body = body.replace(credential, "[REDACTED]");
    if body.is_empty() {
        format!("GitHub returned HTTP {status}")
    } else {
        format!("GitHub returned HTTP {status}: {body}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{DecodingKey, Validation, decode};

    const TEST_PRIVATE_KEY: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIEogIBAAKCAQEA0/nJpwFEKIwohNqQMNtpt9vRxtSwAJuGPz+wY8b7b5ZdVTLt\n\
lvoVyvG2WtH0m/wmeVohQyqOZDBFkAES4BRwloq916s9ddNcjvC5GfS7nfvHqGxy\n\
A3miaprrDrC+RxqgLX1sVdMrYfHiTvihdB5E5KFElOPDvPk9qLXKH8mLCZ+8cy9F\n\
QQGU739UMkJO4yGyMkkJL+b4OioITqzuzYWObpVcn4dthioa0PqFD/ynWzWZHFkl\n\
nHXGHfiMwyKiZN6DqRCLgDyi/Sx6B8kLA8AIUXfcQ1H0+avSTug9QdasfnzEsDXm\n\
SasjENipeb+UvmtsMMve+RaDwVc0P8jOZyhzxwIDAQABAoIBAA9f+0XfPCPawqec\n\
OMAqcfkRex46WAe7QBn/UxpxmNQnql5fIseEHSeKr6jXuK/Fsr6zkF0QTaKrs6f1\n\
w/o30VDCAHIPrBYLPhfqEur0itUzKXCoaP6iiGrGEM2QeUSsBDk4cy6ocemZQ0pq\n\
Ez/aYDHbFp/cA1sGEvhVMzNqCWkpqIaWCSbvvnxB4zyvuk/VpTCJqrcRv/KMNBjJ\n\
mCmapdWwOQQhb5Mnbls8FpopyRM70Fb+vYfnM4kp2acLja088z6DOSp33DOHdQDv\n\
RjA13NMseuHNptgaOc8aGFKnMoiccrQtHampTXyKKgfYtoTIop9tu4QpWZR6XfSt\n\
7MLHlskCgYEA9s/s79sHRyGfWA6WCmCV8UnxUCax/W0tiZCeYH3pKX/OybpmcDE7\n\
qkEOCyIevQMD6cYRrpeMN/+2Bj2+eCncaKRGFvcKw3xqomel0cb52u1BbQsCJm2T\n\
oqNczFQXCeHC36b5rQ6/SsoRrQDMCeCljgi9FokiBMj7KDpaNdx5+ikCgYEA293g\n\
hR2MXO/41t4e+p4RconjfW7ApuDnkIq/5z3uA9BfFI8ZFrg2f0hadQPKuuySG8Hu\n\
XD7ZDUb6/StciFOWEsEEGPHxBb76ak48a87hUX/S6EVXxPlqnbtv5fgkOfSJrWyN\n\
veoBg3aXq7dbMZoKuf1HF2UAtUPryidrxY20nG8CgYBDcQWZdQxED5fvA+1T4JiY\n\
WlAqSqiQS2kKEg5053Ly+A2Vae3b8lgPab9go24qz8wd8RWb06VpF+KxSeXmB+pG\n\
Xb0ZQWAbkr/AqPnSnMBhZjtehIeN4Jta8bDF1KSG+r25R3MqKsFyukyiLXmTqWMT\n\
sKeapNY64IBgubE0zo42IQKBgCERmRBMZm0gBrxCgkRNe/BPPvEBtGYlE48Q31Yn\n\
YWIfd/1+nFZndILmXfyVimtc44jbPT+QkFLH5z4mxG8Yq0y8zSH3Wbn0hHNyuvG4\n\
FvmMESuwKkhM/zUkGOtsy/4Pgpmb5yG2TW35a0x/cm7ebViV+JhTL60TvyYTkckP\n\
zCdvAoGAXJjdZZHH/e+ZBxbLOI1MVUq6SALClfmxFseskNSpDQZmUa5CC6+hrl6n\n\
Nn7LuBHmYjqK7FT+1Nu9dva+gc+ypqu5659AAfSRz2CxJ48O3/omEjYqD5vXjJ6k\n\
7k/BoufYDdMW0lhYKk3x1O6+YY8HaRO85BCMphf5hZf1aUF2zLc=\n\
-----END RSA PRIVATE KEY-----\n";

    const TEST_PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----\n\
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0/nJpwFEKIwohNqQMNtp\n\
t9vRxtSwAJuGPz+wY8b7b5ZdVTLtlvoVyvG2WtH0m/wmeVohQyqOZDBFkAES4BRw\n\
loq916s9ddNcjvC5GfS7nfvHqGxyA3miaprrDrC+RxqgLX1sVdMrYfHiTvihdB5E\n\
5KFElOPDvPk9qLXKH8mLCZ+8cy9FQQGU739UMkJO4yGyMkkJL+b4OioITqzuzYWO\n\
bpVcn4dthioa0PqFD/ynWzWZHFklnHXGHfiMwyKiZN6DqRCLgDyi/Sx6B8kLA8AI\n\
UXfcQ1H0+avSTug9QdasfnzEsDXmSasjENipeb+UvmtsMMve+RaDwVc0P8jOZyhz\n\
xwIDAQAB\n\
-----END PUBLIC KEY-----\n";

    #[test]
    fn app_jwt_uses_rs256_and_short_lived_claims() {
        let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY.as_bytes()).unwrap();
        let jwt = create_app_jwt("app-client-id", &key, SystemTime::now()).unwrap();
        let public_key = DecodingKey::from_rsa_pem(TEST_PUBLIC_KEY.as_bytes()).unwrap();
        let claims =
            decode::<AppJwtClaims>(&jwt, &public_key, &Validation::new(Algorithm::RS256)).unwrap();

        assert_eq!(claims.header.alg, Algorithm::RS256);
        assert_eq!(claims.claims.iss, "app-client-id");
        assert_eq!(
            claims.claims.exp - claims.claims.iat,
            APP_JWT_LIFETIME + APP_JWT_IAT_BACKDATE
        );
    }

    #[test]
    fn token_cache_reuses_tokens_until_the_refresh_margin() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let expires_at = now + Duration::from_secs(3600);
        let key = TokenCacheKey {
            installation_id: 12,
            repository: "owner/repo".to_owned(),
        };
        let mut cache = TokenCache::default();
        cache.insert(
            key.clone(),
            InstallationCredentials {
                installation_id: 12,
                token: "secret-token".to_owned(),
                expires_at: "2030-01-01T00:00:00Z".to_owned(),
                permissions: BTreeMap::new(),
            },
            expires_at,
        );

        assert_eq!(cache.get(&key, now).unwrap().token, "secret-token");
        assert!(cache.get(&key, expires_at - TOKEN_REFRESH_MARGIN).is_none());
        assert!(
            cache
                .get(
                    &TokenCacheKey {
                        installation_id: 12,
                        repository: "owner/other".to_owned(),
                    },
                    now,
                )
                .is_none()
        );
    }

    #[test]
    fn parses_rfc3339_installation_token_expiry() {
        let expires_at = parse_expiration("2030-01-01T00:00:00Z").unwrap();
        assert_eq!(
            expires_at.duration_since(UNIX_EPOCH).unwrap(),
            Duration::from_secs(1_893_456_000)
        );
    }
}
