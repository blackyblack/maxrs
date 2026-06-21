//! Internal authentication support for the Max client.

use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use serde_json::{json, Value};

use crate::captcha::http::{HttpServer, HttpServerConfig};
use crate::captcha::solver::{CaptchaSolver, CaptchaSolverConfig};
use crate::client::InnerClient;
use crate::error::{Error, Result};
use crate::models::Session;
use crate::operator_channels::OperatorChannel;
use crate::protocol::opcode;

pub const ENV_SESSION_TOKEN: &str = "MAX_SESSION_TOKEN";
pub const ENV_PASSWORD: &str = "MAX_PASSWORD";
pub const ENV_PHONE: &str = "MAX_PHONE";
pub const ENV_SOLVER_URL: &str = "MAX_SOLVER_URL";
pub const ENV_CALLBACK_BIND: &str = "MAX_CALLBACK_BIND";
pub const ENV_CALLBACK_URL_BASE: &str = "MAX_CALLBACK_URL_BASE";

pub const DEFAULT_SOLVER_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_CALLBACK_BIND: &str = "127.0.0.1:3002";
pub const DEFAULT_CAPTCHA_CALLBACK_PATH: &str = "/captcha-callback";

pub fn session_token_from_env() -> Option<String> {
    env_string(ENV_SESSION_TOKEN)
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
            session_token: session_token_from_env(),
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
    pub(crate) async fn login(&self, config: LoginConfig) -> Result<Session> {
        if let Some(token) = config.session_token.as_deref() {
            match self.login_with_token(token).await {
                Ok(session) => return Ok(session),
                Err(err) if should_fallback_to_sms_login(&err) => {
                    tracing::info!(%err, "saved Max session token was rejected; starting SMS auth")
                }
                Err(err) => return Err(err),
            }
        }

        let phone = config
            .phone
            .as_deref()
            .ok_or_else(|| Error::UnexpectedResponse("missing phone for SMS login".into()))?;
        let sms_token = self
            .request_sms_code_with_auth_captcha(phone, &config.captcha)
            .await?;
        let code = config.operator.request_sms_code(phone).await?;
        self.verify_sms_code(&sms_token, code.trim(), config.password.as_deref())
            .await
    }

    async fn request_sms_code_with_auth_captcha(
        &self,
        phone: &str,
        config: &AuthCaptchaConfig,
    ) -> Result<String> {
        match self.request_sms_code(phone).await {
            Ok(token) => Ok(token),
            Err(Error::CaptchaRequired { link }) => {
                let solver_url = config
                    .solver_url
                    .as_ref()
                    .ok_or(Error::CaptchaSolverDisabled)?;
                let server = HttpServer::bind(HttpServerConfig::new(&config.callback_bind)).await?;
                let callback_addr = server.local_addr()?;
                let callback_url = config.callback_url(callback_addr);
                let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::new(
                    solver_url.clone(),
                    callback_url,
                ))?);
                tokio::spawn(server.with_captcha_solver(Arc::clone(&solver)).serve());
                let captcha_token = solver.solve(&link).await?;
                self.request_sms_code_with_captcha_token(phone, &captcha_token)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn request_auth_captcha(&self, phone: &str) -> Result<Option<String>> {
        self.session_init().await?;
        let payload = json!({
            "source": "auth",
            "identifier": phone,
        });
        match self.invoke(opcode::AUTH_CAPTCHA_REQUEST, payload).await {
            Ok(response) => Ok(response.payload["link"].as_str().map(str::to_string)),
            Err(Error::Server { opcode, message })
                if message == "captcha.create-session-failed" =>
            {
                tracing::debug!(
                    opcode,
                    "captcha session creation failed; continuing without token"
                );
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    async fn request_sms_code(&self, phone: &str) -> Result<String> {
        if let Some(link) = self.request_auth_captcha(phone).await? {
            return Err(Error::CaptchaRequired { link });
        }

        self.request_sms_code_with_captcha_token(phone, "").await
    }

    async fn request_sms_code_with_captcha_token(
        &self,
        phone: &str,
        captcha_token: &str,
    ) -> Result<String> {
        self.session_init().await?;
        let payload = json!({
            "phone": phone,
            "type": "START_AUTH",
            "language": "ru",
            "captchaToken": captcha_token,
        });
        let response = self.invoke(opcode::AUTH_REQUEST, payload).await?;
        response.payload["token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::UnexpectedResponse("missing auth token".into()))
    }

    async fn verify_sms_code(
        &self,
        sms_token: &str,
        code: &str,
        password: Option<&str>,
    ) -> Result<Session> {
        let payload = json!({
            "token": sms_token,
            "verifyCode": code,
            "authTokenType": "CHECK_CODE",
        });
        let response = self.invoke(opcode::AUTH, payload).await?;
        if response.payload["passwordChallenge"].is_object() {
            return self
                .verify_password_challenge(&response.payload["passwordChallenge"], password)
                .await;
        }

        self.login_with_auth_payload(&response.payload).await
    }

    async fn verify_password_challenge(
        &self,
        challenge: &Value,
        password: Option<&str>,
    ) -> Result<Session> {
        let track_id = challenge["trackId"].as_str().ok_or_else(|| {
            Error::UnexpectedResponse("missing password challenge trackId".into())
        })?;
        let password = password.ok_or(Error::PasswordRequired)?;
        let response = self
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

    async fn login_with_auth_payload(&self, payload: &Value) -> Result<Session> {
        let token = login_token_from_auth_payload(payload)?;
        self.login_with_token(&token).await
    }

    async fn login_with_token(&self, token: &str) -> Result<Session> {
        self.session_init().await?;
        self.perform_login(token).await
    }

    async fn perform_login(&self, token: &str) -> Result<Session> {
        let payload = json!({
            "interactive": true,
            "token": token,
            "chatsSync": 0,
            "contactsSync": 0,
            "presenceSync": 0,
            "draftsSync": 0,
            "chatsCount": 40,
        });
        let response = self.invoke(opcode::LOGIN, payload).await?;
        Ok(Session {
            token: token.to_string(),
            login_payload: response.payload,
        })
    }
}

fn should_fallback_to_sms_login(err: &Error) -> bool {
    matches!(err, Error::Server { opcode, .. } if *opcode == opcode::LOGIN)
}

fn login_token_from_auth_payload(payload: &Value) -> Result<String> {
    payload["tokenAttrs"]["LOGIN"]["token"]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| Error::UnexpectedResponse("missing session token".into()))
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
}
