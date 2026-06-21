use serde_json::Value;

use crate::auth::TelegramOperatorConfig;
use crate::error::{Error, Result};

pub(crate) async fn request_sms_code(
    config: &TelegramOperatorConfig,
    phone: &str,
) -> Result<String> {
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
