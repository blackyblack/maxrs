use serde_json::Value;

use crate::error::{Error, Result};
use crate::operator_channels::TelegramOperatorConfig;

const TELEGRAM_API_HOST: &str = "api.telegram.org";

pub async fn request_sms_code(config: &TelegramOperatorConfig, phone: &str) -> Result<String> {
    let prompt = format!("Max login requested for {phone}. Reply to this chat with the SMS code.");
    request_text(config, &prompt).await
}

async fn request_text(config: &TelegramOperatorConfig, prompt: &str) -> Result<String> {
    let http = telegram_http_client()?;
    let base = format!("https://api.telegram.org/bot{}", config.bot_token);
    let mut offset = next_update_offset(&fetch_updates(&http, &base, "0", None).await?)?;

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
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Telegram(
                "timed out waiting for an SMS code reply from the configured Telegram chat".into(),
            ));
        }
        let resp = fetch_updates(&http, &base, "20", Some(offset)).await?;
        if !resp["ok"].as_bool().unwrap_or(false) {
            return Err(Error::Telegram(resp.to_string()));
        }
        if let Some(updates) = resp["result"].as_array() {
            for update in updates {
                if let Some(id) = update["update_id"].as_i64() {
                    offset = id + 1;
                }
                if let Some(text) =
                    operator_text_from_update(update, config.chat_id, config.bot_user_id)
                {
                    return Ok(text);
                }
            }
        }
    }
}

fn telegram_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .retry(
            reqwest::retry::for_host(TELEGRAM_API_HOST)
                .max_retries_per_request(2)
                .classify_fn(|req_rep| {
                    let should_retry = req_rep.method() == reqwest::Method::GET
                        && req_rep.uri().path().ends_with("/getUpdates")
                        && (req_rep.error().is_some()
                            || matches!(
                                req_rep.status(),
                                Some(status)
                                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                                        || status.is_server_error()
                            ));

                    if should_retry {
                        req_rep.retryable()
                    } else {
                        req_rep.success()
                    }
                }),
        )
        .build()
        .map_err(Into::into)
}

async fn fetch_updates(
    http: &reqwest::Client,
    base: &str,
    timeout: &str,
    offset: Option<i64>,
) -> Result<Value> {
    let mut request = http
        .get(format!("{base}/getUpdates"))
        .query(&[("timeout", timeout)]);
    let offset_value;
    if let Some(offset) = offset {
        offset_value = offset.to_string();
        request = request.query(&[("offset", offset_value.as_str())]);
    }

    let resp = request.send().await?.json().await?;
    Ok(resp)
}

fn next_update_offset(resp: &Value) -> Result<i64> {
    if !resp["ok"].as_bool().unwrap_or(false) {
        return Err(Error::Telegram(resp.to_string()));
    }

    Ok(resp["result"]
        .as_array()
        .and_then(|updates| {
            updates
                .iter()
                .filter_map(|update| update["update_id"].as_i64())
                .max()
        })
        .map(|id| id + 1)
        .unwrap_or_default())
}

fn operator_text_from_update(update: &Value, chat_id: i64, bot_user_id: i64) -> Option<String> {
    let message = &update["message"];
    if message["chat"]["id"].as_i64() != Some(chat_id) {
        return None;
    }
    if is_own_message(message, bot_user_id) {
        return None;
    }

    let text = message["text"].as_str()?.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

fn is_own_message(message: &Value, bot_user_id: i64) -> bool {
    message["from"]["id"].as_i64() == Some(bot_user_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn ignores_messages_sent_by_the_telegram_bot() {
        let update = json!({
            "message": {
                "chat": { "id": 42 },
                "from": { "id": 1001, "is_bot": true },
                "text": "Max login requested for +100. Reply to this chat with the SMS code."
            }
        });

        assert_eq!(operator_text_from_update(&update, 42, 1001,), None);
    }

    #[test]
    fn accepts_user_sms_code_from_configured_chat() {
        let update = json!({
            "message": {
                "chat": { "id": 42 },
                "from": { "id": 2002, "is_bot": false },
                "text": " 12345 "
            }
        });

        assert_eq!(
            operator_text_from_update(&update, 42, 1001),
            Some("12345".to_string())
        );
    }

    #[test]
    fn accepts_prompt_text_when_it_does_not_come_from_the_bot() {
        let update = json!({
            "message": {
                "chat": { "id": 42 },
                "from": { "id": 2002, "is_bot": false },
                "text": "prompt"
            }
        });

        assert_eq!(
            operator_text_from_update(&update, 42, 1001),
            Some("prompt".to_string())
        );
    }

    #[test]
    fn starts_polling_after_latest_pending_update() {
        let resp = json!({
            "ok": true,
            "result": [
                { "update_id": 10 },
                { "update_id": 14 },
                { "message": { "text": "missing id" } }
            ]
        });

        assert_eq!(next_update_offset(&resp).unwrap(), 15);
        assert_eq!(
            next_update_offset(&json!({ "ok": true, "result": [] })).unwrap(),
            0
        );
    }
}
