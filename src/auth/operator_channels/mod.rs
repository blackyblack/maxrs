use std::time::Duration;

use crate::{
    auth::env_string,
    error::{Error, Result},
};

pub(crate) mod cli;
pub(crate) mod telegram;

pub const ENV_OPERATOR_CHANNEL: &str = "MAX_OPERATOR_CHANNEL";
pub const ENV_TELEGRAM_BOT_TOKEN: &str = "MAX_TELEGRAM_BOT_TOKEN";
pub const ENV_TELEGRAM_CHAT_ID: &str = "MAX_TELEGRAM_CHAT_ID";
pub const ENV_TELEGRAM_POLL_TIMEOUT_SECS: &str = "MAX_TELEGRAM_POLL_TIMEOUT_SECS";

#[derive(Debug, Clone)]
pub enum OperatorChannel {
    None,
    Cli,
    Telegram(TelegramOperatorConfig),
}

impl OperatorChannel {
    pub fn from_env() -> Result<Self> {
        Self::from_name(env_string(ENV_OPERATOR_CHANNEL).as_deref())
    }

    fn from_name(channel: Option<&str>) -> Result<Self> {
        let channel = channel.unwrap_or("none").to_ascii_lowercase();

        Ok(match channel.as_str() {
            "cli" => Self::Cli,
            "telegram" | "tg" => Self::Telegram(TelegramOperatorConfig::from_env()?),
            _ => Self::None,
        })
    }

    pub(crate) async fn request_sms_code(&self, phone: &str) -> Result<String> {
        match self {
            OperatorChannel::None => Err(Error::NoOperatorChannel),
            OperatorChannel::Cli => cli::request_sms_code(phone).await,
            OperatorChannel::Telegram(config) => telegram::request_sms_code(config, phone).await,
        }
    }
}

#[derive(Debug, Clone)]
pub struct TelegramOperatorConfig {
    pub bot_token: String,
    pub bot_user_id: i64,
    pub chat_id: i64,
    pub poll_timeout: Duration,
}

impl TelegramOperatorConfig {
    pub fn from_env() -> Result<Self> {
        let bot_token = env_string(ENV_TELEGRAM_BOT_TOKEN)
            .ok_or_else(|| Error::TelegramConfigMissing(format!("set {ENV_TELEGRAM_BOT_TOKEN}")))?;
        let bot_user_id = telegram_bot_id_from_token(&bot_token).ok_or_else(|| {
            Error::TelegramConfigMissing(format!(
                "{ENV_TELEGRAM_BOT_TOKEN} must start with the numeric Telegram bot id prefix"
            ))
        })?;
        let chat_id: i64 = env_string(ENV_TELEGRAM_CHAT_ID)
            .ok_or_else(|| Error::TelegramConfigMissing(format!("set {ENV_TELEGRAM_CHAT_ID}")))?
            .parse()
            .map_err(|_| {
                Error::TelegramConfigMissing(format!(
                    "{ENV_TELEGRAM_CHAT_ID} must be an integer chat id"
                ))
            })?;

        Ok(Self {
            bot_token,
            bot_user_id,
            chat_id,
            poll_timeout: env_string(ENV_TELEGRAM_POLL_TIMEOUT_SECS)
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or_else(|| Duration::from_secs(300)),
        })
    }
}

fn telegram_bot_id_from_token(token: &str) -> Option<i64> {
    token.split_once(':')?.0.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::{telegram_bot_id_from_token, OperatorChannel};

    #[test]
    fn parses_telegram_bot_id_from_token_prefix() {
        assert_eq!(telegram_bot_id_from_token("123456:secret"), Some(123456));
        assert_eq!(telegram_bot_id_from_token("not-a-token"), None);
    }

    #[test]
    fn defaults_to_no_operator_channel() {
        assert!(matches!(
            OperatorChannel::from_name(None).unwrap(),
            OperatorChannel::None
        ));
    }
}
