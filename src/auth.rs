//! Authentication helpers shared by examples and applications.

use std::env;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use crate::error::{Error, Result};

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
            OperatorChannel::Cli => crate::operator_channels::cli::request_sms_code(phone).await,
            OperatorChannel::Telegram(config) => {
                crate::operator_channels::telegram::request_sms_code(config, phone).await
            }
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
