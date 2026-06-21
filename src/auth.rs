//! Authentication helpers shared by examples and applications.

use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::captcha::http::{HttpServer, HttpServerConfig};
use crate::captcha::solver::{CaptchaSolver, CaptchaSolverConfig};
use crate::client::MaxClient;
use crate::error::{Error, Result};
use crate::models::Session;

pub const ENV_SESSION_TOKEN: &str = "MAX_SESSION_TOKEN";
pub const ENV_PHONE: &str = "MAX_PHONE";
pub const ENV_SOLVER_URL: &str = "MAX_SOLVER_URL";
pub const ENV_CALLBACK_BIND: &str = "MAX_CALLBACK_BIND";
pub const ENV_CALLBACK_URL_BASE: &str = "MAX_CALLBACK_URL_BASE";
pub const ENV_OPERATOR_CHANNEL: &str = "MAX_OPERATOR_CHANNEL";
pub const ENV_TELEGRAM_BOT_TOKEN: &str = "MAX_TELEGRAM_BOT_TOKEN";
pub const ENV_TELEGRAM_CHAT_ID: &str = "MAX_TELEGRAM_CHAT_ID";
pub const ENV_TELEGRAM_POLL_TIMEOUT_SECS: &str = "MAX_TELEGRAM_POLL_TIMEOUT_SECS";

pub const DEFAULT_SOLVER_URL: &str = "http://127.0.0.1:3000";
pub const DEFAULT_CALLBACK_BIND: &str = "127.0.0.1:3002";
pub const DEFAULT_CAPTCHA_CALLBACK_PATH: &str = "/captcha-callback";

pub fn session_token_from_env() -> Option<String> {
    env_string(ENV_SESSION_TOKEN)
}

#[derive(Debug, Clone)]
pub struct LoginConfig {
    pub phone: Option<String>,
    pub session_token: Option<String>,
    pub captcha: AuthCaptchaConfig,
    pub operator: OperatorChannel,
}

impl LoginConfig {
    pub fn from_env() -> Self {
        Self {
            phone: env_string(ENV_PHONE),
            session_token: session_token_from_env(),
            captcha: AuthCaptchaConfig::from_env(),
            operator: OperatorChannel::from_env(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum OperatorChannel {
    None,
    Cli,
    Telegram(TelegramOperatorConfig),
}

impl OperatorChannel {
    pub fn from_env() -> Self {
        match env_string(ENV_OPERATOR_CHANNEL)
            .unwrap_or_else(|| "cli".into())
            .to_ascii_lowercase()
            .as_str()
        {
            "none" | "off" | "disabled" => Self::None,
            "telegram" | "tg" => match TelegramOperatorConfig::from_env() {
                Some(config) => Self::Telegram(config),
                None => Self::None,
            },
            _ => Self::Cli,
        }
    }

    pub(crate) async fn request_sms_code(&self, phone: &str) -> Result<String> {
        match self {
            OperatorChannel::None => Err(Error::NoOperatorChannel),
            OperatorChannel::Cli => prompt(&format!("Enter the SMS code for {phone}: ")).await,
            OperatorChannel::Telegram(config) => request_telegram_code(config, phone).await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TelegramOperatorConfig {
    pub bot_token: String,
    pub chat_id: i64,
    pub poll_timeout: Duration,
}

impl TelegramOperatorConfig {
    pub fn from_env() -> Option<Self> {
        Some(Self {
            bot_token: env_string(ENV_TELEGRAM_BOT_TOKEN)?,
            chat_id: env_string(ENV_TELEGRAM_CHAT_ID)?.parse().ok()?,
            poll_timeout: env_string(ENV_TELEGRAM_POLL_TIMEOUT_SECS)
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(300)),
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

impl MaxClient {
    /// Logs in using a saved session token when valid, otherwise starts the SMS auth flow.
    pub async fn login(&self, config: LoginConfig) -> Result<Session> {
        if let Some(token) = config.session_token.as_deref() {
            match self.login_with_token_internal(token).await {
                Ok(session) => return Ok(session),
                Err(err) => {
                    tracing::info!(%err, "saved Max session token was rejected; starting SMS auth")
                }
            }
        }

        let phone = config
            .phone
            .as_deref()
            .ok_or_else(|| Error::UnexpectedResponse("missing phone for SMS login".into()))?;
        let sms_token = self
            .request_sms_code_with_auth_captcha_internal(phone, &config.captcha)
            .await?;
        let code = config.operator.request_sms_code(phone).await?;
        self.verify_sms_code_internal(&sms_token, code.trim()).await
    }

    pub(crate) async fn request_sms_code_with_auth_captcha_internal(
        &self,
        phone: &str,
        config: &AuthCaptchaConfig,
    ) -> Result<String> {
        match self.request_sms_code_internal(phone).await {
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
                self.request_sms_code_with_captcha_token_internal(phone, &captcha_token)
                    .await
            }
            Err(err) => Err(err),
        }
    }
}

async fn prompt(label: &str) -> Result<String> {
    let mut stdout = tokio::io::stdout();
    stdout.write_all(label.as_bytes()).await?;
    stdout.flush().await?;
    let mut line = String::new();
    let mut reader = BufReader::new(tokio::io::stdin());
    reader.read_line(&mut line).await?;
    Ok(line)
}

async fn request_telegram_code(config: &TelegramOperatorConfig, phone: &str) -> Result<String> {
    let http = reqwest::Client::new();
    let base = format!("https://api.telegram.org/bot{}", config.bot_token);
    let prompt = format!("Max login requested for {phone}. Reply to this chat with the SMS code.");
    let send: Value = http
        .post(format!("{base}/sendMessage"))
        .json(&serde_json::json!({ "chat_id": config.chat_id, "text": prompt }))
        .send()
        .await?
        .json()
        .await?;
    if !send["ok"].as_bool().unwrap_or(false) {
        return Err(Error::Telegram(send.to_string()));
    }

    let deadline = tokio::time::Instant::now() + config.poll_timeout;
    let mut offset = 0_i64;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Timeout(0));
        }
        let resp: Value = http
            .get(format!("{base}/getUpdates"))
            .query(&[("timeout", "20"), ("offset", &offset.to_string())])
            .send()
            .await?
            .json()
            .await?;
        if !resp["ok"].as_bool().unwrap_or(false) {
            return Err(Error::Telegram(resp.to_string()));
        }
        if let Some(updates) = resp["result"].as_array() {
            for update in updates {
                if let Some(id) = update["update_id"].as_i64() {
                    offset = id + 1;
                }
                let message = &update["message"];
                if message["chat"]["id"].as_i64() == Some(config.chat_id) {
                    if let Some(text) = message["text"].as_str() {
                        let code = text.trim();
                        if !code.is_empty() {
                            return Ok(code.to_string());
                        }
                    }
                }
            }
        }
    }
}

fn env_string(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn normalize_callback_addr(callback_addr: SocketAddr) -> SocketAddr {
    let port = callback_addr.port();
    match callback_addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        IpAddr::V6(ip) if ip.is_unspecified() => SocketAddr::from((Ipv6Addr::LOCALHOST, port)),
        _ => callback_addr,
    }
}
