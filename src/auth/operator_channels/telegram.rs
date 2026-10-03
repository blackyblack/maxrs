use serde_json::Value;

use crate::error::{Error, Result};

use super::TelegramOperatorConfig;

const TELEGRAM_API_HOST: &str = "api.telegram.org";

pub async fn request_sms_code(config: &TelegramOperatorConfig, phone: &str) -> Result<String> {
    let text = format!("Max login requested for {phone}. Reply to this chat with the SMS code.");
    request_text(config, &text).await
}

async fn request_text(config: &TelegramOperatorConfig, text: &str) -> Result<String> {
    let http = telegram_http_client()?;
    let base = format!("https://{TELEGRAM_API_HOST}/bot{}", config.bot_token);
    let mut offset = next_update_offset(&fetch_updates(&http, &base, 0, None).await?)?;

    let send_response = http
        .post(format!("{base}/sendMessage"))
        .json(&serde_json::json!({ "chat_id": config.chat_id, "text": text }))
        .send()
        .await?;
    let send_status = send_response.status();
    ensure_telegram_http_status(send_status)?;
    let send: Value = send_response.json().await?;
    ensure_telegram_success(send_status, &send)?;

    let deadline = tokio::time::Instant::now() + config.poll_timeout;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::TelegramUnavailable(
                "timed out waiting for an SMS code reply from the configured Telegram chat".into(),
            ));
        }
        let resp = fetch_updates(&http, &base, 20, Some(offset)).await?;
        let updates = if let Some(updates) = resp["result"].as_array() {
            updates
        } else {
            continue;
        };
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

fn telegram_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .retry(
            reqwest::retry::for_host(TELEGRAM_API_HOST)
                .max_retries_per_request(2)
                .classify_fn(|request_result| {
                    let is_retryable_endpoint = (request_result.method() == reqwest::Method::GET
                        && request_result.uri().path().ends_with("/getUpdates"))
                        || (request_result.method() == reqwest::Method::POST
                            && request_result.uri().path().ends_with("/sendMessage"));
                    let should_retry = is_retryable_endpoint
                        && (request_result.error().is_some()
                            || matches!(
                                request_result.status(),
                                Some(status)
                                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS
                                        || status.is_server_error()
                            ));

                    if should_retry {
                        request_result.retryable()
                    } else {
                        request_result.success()
                    }
                }),
        )
        .build()
        .map_err(Into::into)
}

async fn fetch_updates(
    http: &reqwest::Client,
    base: &str,
    timeout: u64,
    offset: Option<i64>,
) -> Result<Value> {
    let mut request = http
        .get(format!("{base}/getUpdates"))
        .query(&[("timeout", timeout)]);
    if let Some(offset) = offset {
        request = request.query(&[("offset", offset)]);
    }

    let response = request.send().await?;
    let status = response.status();
    ensure_telegram_http_status(status)?;
    let payload = response.json().await?;
    ensure_telegram_success(status, &payload)?;
    Ok(payload)
}

fn next_update_offset(resp: &Value) -> Result<i64> {
    ensure_telegram_success(reqwest::StatusCode::OK, resp)?;

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

fn ensure_telegram_http_status(status: reqwest::StatusCode) -> Result<()> {
    if status == reqwest::StatusCode::REQUEST_TIMEOUT
        || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
    {
        Err(Error::TelegramUnavailable(format!("HTTP {status}")))
    } else {
        Ok(())
    }
}

fn ensure_telegram_success(status: reqwest::StatusCode, response: &Value) -> Result<()> {
    ensure_telegram_http_status(status)?;
    if response["ok"].as_bool().unwrap_or(false) {
        return Ok(());
    }

    let error_code = response["error_code"].as_u64();
    if error_code == Some(429) || error_code.is_some_and(|code| (500..600).contains(&code)) {
        Err(Error::TelegramUnavailable(response.to_string()))
    } else {
        Err(Error::Telegram(response.to_string()))
    }
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

    #[test]
    fn distinguishes_permanent_and_temporary_bot_api_errors() {
        assert!(matches!(
            ensure_telegram_success(
                reqwest::StatusCode::UNAUTHORIZED,
                &json!({"ok": false, "error_code": 401, "description": "Unauthorized"}),
            ),
            Err(Error::Telegram(_))
        ));
        assert!(matches!(
            ensure_telegram_success(
                reqwest::StatusCode::TOO_MANY_REQUESTS,
                &json!({"ok": false, "error_code": 429, "description": "Too Many Requests"}),
            ),
            Err(Error::TelegramUnavailable(_))
        ));
        assert!(matches!(
            ensure_telegram_success(
                reqwest::StatusCode::BAD_GATEWAY,
                &json!({"ok": false, "error_code": 502, "description": "Bad Gateway"}),
            ),
            Err(Error::TelegramUnavailable(_))
        ));
    }
}
