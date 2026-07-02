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

/// The data that client needs from a successful `LOGIN`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoginData {
    /// Logged-in user id, when the server includes it in the login response.
    pub own_user_id: Option<i64>,
}

/// The result of a successful login.
#[derive(Debug, Clone)]
pub struct LoginSession {
    /// Long-lived session token. Keep it to re-login without SMS.
    pub token: String,
    /// Parsed `LOGIN` response data used by the client.
    pub login_data: LoginData,
}

/// Outgoing text message with optional Max formatter elements.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaxMessage {
    /// Message text.
    pub text: String,
    /// Formatter elements applied to spans in `text`.
    #[serde(default)]
    pub elements: Vec<MessageElement>,
}

impl MaxMessage {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            elements: Vec::new(),
        }
    }

    pub fn with_elements(text: impl Into<String>, elements: Vec<MessageElement>) -> Self {
        Self {
            text: text.into(),
            elements,
        }
    }
}

/// A formatter annotation over a `[from, from + length)` span of `text`.
///
/// `LINK` carries its target as `attributes.url`; other kinds have no
/// attributes. `from`/`length` units (Unicode scalars vs UTF-16 code units) are
/// caller-defined and unverified against the server.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageElement {
    #[serde(rename = "type")]
    pub kind: MessageElementKind,
    #[serde(default)]
    pub from: usize,
    pub length: usize,
    #[serde(default, skip_serializing_if = "ElementAttributes::is_empty")]
    pub attributes: ElementAttributes,
}

/// Type-specific attributes of a [`MessageElement`]; skipped when empty.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ElementAttributes {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl ElementAttributes {
    fn is_empty(&self) -> bool {
        self.url.is_none()
    }
}

impl MessageElement {
    pub fn strong(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Strong, from, length)
    }
    pub fn emphasized(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Emphasized, from, length)
    }
    pub fn underline(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Underline, from, length)
    }
    pub fn strikethrough(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Strikethrough, from, length)
    }
    pub fn monospaced(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Monospaced, from, length)
    }
    pub fn code(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Code, from, length)
    }
    pub fn heading(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Heading, from, length)
    }
    pub fn quote(from: usize, length: usize) -> Self {
        Self::new(MessageElementKind::Quote, from, length)
    }

    pub fn link(from: usize, length: usize, url: impl Into<String>) -> Self {
        Self {
            kind: MessageElementKind::Link,
            from,
            length,
            attributes: ElementAttributes {
                url: Some(url.into()),
            },
        }
    }

    pub fn new(kind: MessageElementKind, from: usize, length: usize) -> Self {
        Self {
            kind,
            from,
            length,
            attributes: ElementAttributes::default(),
        }
    }

    pub fn url(&self) -> Option<&str> {
        self.attributes.url.as_deref()
    }
}

/// Max-supported text formatter element kinds.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageElementKind {
    Strong,
    Emphasized,
    Underline,
    Strikethrough,
    Monospaced,
    Code,
    Link,
    Heading,
    Quote,
}
