//! Authentication support for the Max client.

pub mod captcha;
pub mod operator_channels;

use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::client::InnerClient;
use crate::error::{Error, Result};
use crate::models::{LoginData, LoginSession};
use crate::protocol::opcode;

use self::captcha::http::{HttpServer, HttpServerConfig};
use self::captcha::solver::{CaptchaSolver, CaptchaSolverConfig};
use self::operator_channels::OperatorChannel;

pub const SESSION_TOKEN_FILE: &str = ".max_session_token";
pub const ENV_PASSWORD: &str = "MAX_PASSWORD";
pub const ENV_PHONE: &str = "MAX_PHONE";
pub const ENV_SOLVER_URL: &str = "MAX_SOLVER_URL";
pub const ENV_CALLBACK_BIND: &str = "MAX_CALLBACK_BIND";
pub const ENV_CALLBACK_URL_BASE: &str = "MAX_CALLBACK_URL_BASE";

pub const DEFAULT_SOLVER_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_CALLBACK_BIND: &str = "127.0.0.1:3002";
pub const DEFAULT_CAPTCHA_CALLBACK_PATH: &str = "/captcha-callback";

fn session_token_path() -> PathBuf {
    PathBuf::from(SESSION_TOKEN_FILE)
}

pub fn session_token_from_file() -> Option<String> {
    std::fs::read_to_string(session_token_path())
        .ok()
        .and_then(non_empty_trimmed)
}

fn non_empty_trimmed(value: String) -> Option<String> {
    let value = value.trim().to_string();
    (!value.is_empty()).then_some(value)
}

async fn store_session_token_file(token: &str) -> std::io::Result<()> {
    tokio::fs::write(session_token_path(), format!("{}\n", token.trim())).await
}

#[derive(Debug, Clone)]
pub struct LoginConfig {
    pub phone: Option<String>,
    pub password: Option<String>,
    pub session_token: Option<String>,
    pub captcha: AuthCaptchaConfig,
    pub operator: OperatorChannel,
}

impl LoginConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            phone: env_string(ENV_PHONE),
            password: env_password(),
            session_token: session_token_from_file(),
            captcha: AuthCaptchaConfig::from_env(),
            operator: OperatorChannel::from_env()?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AuthCaptchaConfig {
    pub solver_url: Option<String>,
    pub callback_bind: String,
    pub callback_url_base: Option<String>,
}

impl AuthCaptchaConfig {
    pub fn from_env() -> Self {
        Self {
            solver_url: Some(
                env_string(ENV_SOLVER_URL).unwrap_or_else(|| DEFAULT_SOLVER_URL.into()),
            ),
            callback_bind: env_string(ENV_CALLBACK_BIND)
                .unwrap_or_else(|| DEFAULT_CALLBACK_BIND.into()),
            callback_url_base: env_string(ENV_CALLBACK_URL_BASE),
        }
    }

    pub fn disabled() -> Self {
        Self {
            solver_url: None,
            callback_bind: DEFAULT_CALLBACK_BIND.into(),
            callback_url_base: None,
        }
    }

    pub fn callback_url(&self, callback_addr: SocketAddr) -> String {
        match &self.callback_url_base {
            Some(base) => format!(
                "{}{}",
                base.replace("{port}", &callback_addr.port().to_string())
                    .trim_end_matches('/'),
                DEFAULT_CAPTCHA_CALLBACK_PATH
            ),
            None => format!(
                "http://{}{}",
                normalize_callback_addr(callback_addr),
                DEFAULT_CAPTCHA_CALLBACK_PATH
            ),
        }
    }
}

impl Default for AuthCaptchaConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

impl InnerClient {
    pub(crate) async fn login(inner: Arc<Self>, config: LoginConfig) -> Result<LoginSession> {
        AuthFlow::new(inner, config).login().await
    }
}

struct AuthFlow {
    inner: Arc<InnerClient>,
    config: LoginConfig,
}

impl AuthFlow {
    fn new(inner: Arc<InnerClient>, config: LoginConfig) -> Self {
        Self { inner, config }
    }

    async fn login(&self) -> Result<LoginSession> {
        if let Some(token) = self.config.session_token.as_deref() {
            match self.perform_login(token).await {
                Ok(session) => return Ok(session),
                Err(err) if should_fallback_to_sms_login(&err) => {
                    tracing::info!(%err, "saved Max session token was rejected; starting SMS auth")
                }
                Err(err) => return Err(err),
            }
        }

        let phone = self
            .config
            .phone
            .as_deref()
            .ok_or_else(|| Error::UnexpectedResponse("missing phone for SMS login".into()))?;
        let sms_token = self.request_sms_code(phone).await?;
        let code = self.config.operator.request_sms_code(phone).await?;
        self.verify_sms_code(&sms_token, code.trim()).await
    }

    async fn request_sms_code(&self, phone: &str) -> Result<String> {
        let retry_err = match self.request_sms_code_with_captcha_token(phone, None).await {
            Ok(token) => return Ok(token),
            Err(err) if should_retry_sms_with_captcha(&err) => err,
            Err(err) => return Err(err),
        };
        tracing::info!(%retry_err, "Max rejected SMS auth request; retrying with captcha");
        let captcha_token = self.solve_auth_captcha(phone).await?;
        self.request_sms_code_with_captcha_token(phone, Some(&captcha_token))
            .await
    }

    async fn request_sms_code_with_captcha_token(
        &self,
        phone: &str,
        captcha_token: Option<&str>,
    ) -> Result<String> {
        let payload = sms_auth_request_payload(phone, captcha_token);
        let response = self.inner.invoke(opcode::AUTH_REQUEST, payload).await?;
        response.payload["token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::UnexpectedResponse("missing auth token".into()))
    }

    async fn solve_auth_captcha(&self, phone: &str) -> Result<String> {
        // request captcha link from the server, which will be solved by the captcha solver
        let payload = json!({
            "source": "auth",
            "identifier": phone,
        });
        let response = self
            .inner
            .invoke(opcode::AUTH_CAPTCHA_REQUEST, payload)
            .await?;
        let captcha_link = response.payload["link"].as_str().unwrap_or_default();
        if captcha_link.is_empty() {
            return Err(Error::UnexpectedResponse("missing captcha link".into()));
        }

        let solver_url = self
            .config
            .captcha
            .solver_url
            .as_ref()
            .ok_or(Error::CaptchaSolverDisabled)?;
        let server =
            HttpServer::bind(HttpServerConfig::new(&self.config.captcha.callback_bind)).await?;
        let callback_addr = server.local_addr()?;
        let callback_url = self.config.captcha.callback_url(callback_addr);
        let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::new(
            solver_url.clone(),
            callback_url,
        ))?);
        let server_task = server.with_captcha_solver(Arc::clone(&solver)).spawn();
        let result = solver.solve(captcha_link).await;
        server_task.shutdown().await;
        result
    }

    async fn verify_sms_code(&self, sms_token: &str, code: &str) -> Result<LoginSession> {
        let payload = json!({
            "token": sms_token,
            "verifyCode": code,
            "authTokenType": "CHECK_CODE",
        });
        let response = self.inner.invoke(opcode::AUTH, payload).await?;
        if response.payload["passwordChallenge"].is_object() {
            let password = self
                .config
                .password
                .as_deref()
                .ok_or(Error::PasswordRequired)?;
            return self
                .verify_password_challenge(&response.payload["passwordChallenge"], password)
                .await;
        }

        self.login_with_auth_payload(&response.payload).await
    }

    async fn verify_password_challenge(
        &self,
        challenge: &Value,
        password: &str,
    ) -> Result<LoginSession> {
        let track_id = challenge["trackId"].as_str().ok_or_else(|| {
            Error::UnexpectedResponse("missing password challenge trackId".into())
        })?;
        let response = self
            .inner
            .invoke(
                opcode::AUTH_PASSWORD,
                json!({
                    "trackId": track_id,
                    "password": password,
                }),
            )
            .await?;

        self.login_with_auth_payload(&response.payload).await
    }

    async fn login_with_auth_payload(&self, payload: &Value) -> Result<LoginSession> {
        let token = login_token_from_auth_payload(payload)?;
        let session = self.perform_login(&token).await?;
        if let Err(err) = store_session_token_file(&session.token).await {
            tracing::warn!(%err, path = %session_token_path().display(), "failed to store Max session token; continuing with in-memory session");
        }
        Ok(session)
    }

    async fn perform_login(&self, token: &str) -> Result<LoginSession> {
        let payload = json!({
            "interactive": true,
            "token": token,
            "chatsSync": 0,
            "contactsSync": 0,
            "presenceSync": 0,
            "draftsSync": 0,
            "chatsCount": 40,
        });
        let response = self.inner.invoke(opcode::LOGIN, payload).await?;
        let login_data = login_data_from_login_payload(&response.payload)?;
        if let Some(user_id) = login_data.own_user_id {
            self.inner.set_own_user_id(user_id).await;
        }
        Ok(LoginSession {
            token: token.to_string(),
            login_data,
        })
    }
}

fn should_fallback_to_sms_login(err: &Error) -> bool {
    matches!(err, Error::Server { opcode, .. } if *opcode == opcode::LOGIN)
}

fn should_retry_sms_with_captcha(err: &Error) -> bool {
    matches!(err, Error::Server { opcode, .. } if *opcode == opcode::AUTH_REQUEST)
}

fn sms_auth_request_payload(phone: &str, captcha_token: Option<&str>) -> Value {
    let mut payload = json!({
        "phone": phone,
        "type": "START_AUTH",
        "language": "ru",
    });
    if let Some(captcha_token) = captcha_token {
        payload["captchaToken"] = Value::String(captcha_token.to_string());
    }
    payload
}

fn login_token_from_auth_payload(payload: &Value) -> Result<String> {
    payload["tokenAttrs"]["LOGIN"]["token"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| Error::UnexpectedResponse("missing session token".into()))
}

#[derive(Debug, Deserialize)]
struct LoginPayload {
    #[serde(default)]
    profile: Option<LoginProfile>,
    #[serde(default, rename = "userId")]
    user_id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct LoginProfile {
    #[serde(default, alias = "userId", alias = "uid")]
    id: Option<i64>,
}

fn login_data_from_login_payload(payload: &Value) -> Result<LoginData> {
    let payload = serde_json::from_value::<LoginPayload>(payload.clone())?;
    Ok(LoginData {
        own_user_id: payload
            .profile
            .and_then(|profile| profile.id)
            .or(payload.user_id),
    })
}

fn env_string(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_password() -> Option<String> {
    env_string(ENV_PASSWORD)
}

fn normalize_callback_addr(callback_addr: SocketAddr) -> SocketAddr {
    let port = callback_addr.port();
    match callback_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        IpAddr::V6(ip) if ip.is_unspecified() => SocketAddr::from((Ipv6Addr::LOCALHOST, port)),
        _ => callback_addr,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn trims_session_token_file_contents() {
        assert_eq!(
            non_empty_trimmed("  token-value\n".into()),
            Some("token-value".into())
        );
        assert_eq!(non_empty_trimmed("  \n".into()), None);
    }

    #[test]
    fn sms_login_fallback_only_handles_login_server_errors() {
        assert!(should_fallback_to_sms_login(&Error::Server {
            opcode: opcode::LOGIN,
            message: "invalid session".into(),
        }));
        assert!(!should_fallback_to_sms_login(&Error::Server {
            opcode: opcode::AUTH,
            message: "invalid code".into(),
        }));
        assert!(!should_fallback_to_sms_login(&Error::Timeout(
            opcode::LOGIN
        )));
    }

    #[test]
    fn captcha_retry_only_handles_sms_auth_request_server_errors() {
        assert!(should_retry_sms_with_captcha(&Error::Server {
            opcode: opcode::AUTH_REQUEST,
            message: "captcha required".into(),
        }));
        assert!(!should_retry_sms_with_captcha(&Error::Server {
            opcode: opcode::AUTH,
            message: "invalid code".into(),
        }));
        assert!(!should_retry_sms_with_captcha(&Error::Timeout(
            opcode::AUTH_REQUEST
        )));
    }

    #[test]
    fn sms_auth_request_payload_omits_captcha_by_default() {
        let payload = sms_auth_request_payload("+79990000000", None);

        assert_eq!(
            payload,
            json!({
                "phone": "+79990000000",
                "type": "START_AUTH",
                "language": "ru",
            })
        );
        assert!(payload.get("captchaToken").is_none());
    }

    #[test]
    fn sms_auth_request_payload_includes_captcha_on_retry() {
        assert_eq!(
            sms_auth_request_payload("+79990000000", Some("captcha-token")),
            json!({
                "phone": "+79990000000",
                "type": "START_AUTH",
                "language": "ru",
                "captchaToken": "captcha-token",
            })
        );
    }

    #[test]
    fn extracts_login_token_from_auth_payload() {
        let payload = json!({
            "tokenAttrs": {
                "LOGIN": {
                    "token": "session-token"
                }
            }
        });

        assert_eq!(
            login_token_from_auth_payload(&payload).unwrap(),
            "session-token"
        );
    }

    #[test]
    fn parses_login_data_from_profile_id() {
        assert_eq!(
            login_data_from_login_payload(&json!({ "profile": { "id": 777 } }))
                .unwrap()
                .own_user_id,
            Some(777)
        );
        assert_eq!(
            login_data_from_login_payload(&json!({ "profile": { "userId": 778 } }))
                .unwrap()
                .own_user_id,
            Some(778)
        );
        assert_eq!(
            login_data_from_login_payload(&json!({ "profile": { "uid": 779 } }))
                .unwrap()
                .own_user_id,
            Some(779)
        );
        assert_eq!(
            login_data_from_login_payload(&json!({ "profile": {}, "userId": 780 }))
                .unwrap()
                .own_user_id,
            Some(780)
        );
        assert_eq!(
            login_data_from_login_payload(&json!({ "profile": {} }))
                .unwrap()
                .own_user_id,
            None
        );
    }

    #[test]
    fn login_data_ignores_unused_login_response_fields() {
        let data = login_data_from_login_payload(&json!({
            "profile": { "id": 777, "name": "Unused" },
            "chats": [{ "id": 1 }],
            "contacts": [{ "id": 2 }],
            "sync": { "timestamp": 3 }
        }))
        .unwrap();

        assert_eq!(
            data,
            LoginData {
                own_user_id: Some(777)
            }
        );
    }
}
