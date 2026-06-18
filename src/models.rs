//! Typed views over the JSON payloads this client cares about.

use serde::{Deserialize, Serialize};

/// Client/device descriptor sent during `SESSION_INIT` and `LOGIN`.
///
/// The defaults mimic the official web client closely enough for the server to
/// accept the handshake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserAgent {
    #[serde(rename = "deviceType")]
    pub device_type: String,
    pub locale: String,
    #[serde(rename = "deviceLocale")]
    pub device_locale: String,
    #[serde(rename = "osVersion")]
    pub os_version: String,
    #[serde(rename = "deviceName")]
    pub device_name: String,
    #[serde(rename = "headerUserAgent")]
    pub header_user_agent: String,
    #[serde(rename = "appVersion")]
    pub app_version: String,
    pub screen: String,
    pub timezone: String,
}

/// User-agent string used both as the WS `User-Agent` header and inside the
/// `userAgent` payload.
pub const BROWSER_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) \
AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36";

impl Default for UserAgent {
    fn default() -> Self {
        Self {
            device_type: "WEB".to_string(),
            locale: "ru_RU".to_string(),
            device_locale: "ru_RU".to_string(),
            os_version: "Windows".to_string(),
            device_name: "maxrs".to_string(),
            header_user_agent: BROWSER_USER_AGENT.to_string(),
            app_version: "25.9.15".to_string(),
            screen: "1080x1920 1.0x".to_string(),
            timezone: "Europe/Moscow".to_string(),
        }
    }
}

/// A message pushed by the server (`NOTIF_MESSAGE`, opcode 128).
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    /// Chat the message belongs to.
    pub chat_id: i64,
    /// Server-assigned message id.
    pub message_id: i64,
    /// Sender user id.
    pub sender: i64,
    /// Message text (may be empty for attachment-only messages).
    pub text: String,
    /// Send time in milliseconds since the Unix epoch.
    pub time: i64,
}

/// The result of a successful login: the in-memory session token plus the raw
/// login payload (profile, chats, contacts, ...) for callers that need more.
#[derive(Debug, Clone)]
pub struct Session {
    /// Long-lived session token. Keep it to re-login without SMS.
    pub token: String,
    /// Raw `LOGIN` response payload.
    pub login_payload: serde_json::Value,
}
