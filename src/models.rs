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

impl Session {
    /// Chats returned in the `LOGIN` response (the same shape `GET_CHATS`
    /// returns). Useful for discovering a `chatId` to send to.
    pub fn chats(&self) -> Vec<Chat> {
        self.login_payload["chats"]
            .as_array()
            .map(|chats| chats.iter().map(Chat::from_value).collect())
            .unwrap_or_default()
    }
}

/// A chat as described by the server in `LOGIN`/`GET_CHATS` responses.
///
/// Only the fields useful for identifying a chat are modelled; the raw object
/// carries more (last message, participants, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chat {
    /// Chat id, used as `chatId` when sending messages.
    pub id: i64,
    /// `DIALOG`, `CHAT`, or `CHANNEL` (empty if absent).
    pub chat_type: String,
    /// Display title / interlocutor name (may be empty for some dialogs).
    pub title: String,
}

impl Chat {
    fn from_value(value: &serde_json::Value) -> Self {
        Self {
            id: value["id"].as_i64().unwrap_or_default(),
            chat_type: value["type"].as_str().unwrap_or_default().to_string(),
            title: value["title"].as_str().unwrap_or_default().to_string(),
        }
    }
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

/// A single formatter annotation over a span of a message's `text`.
///
/// On the wire each element is `{ "type", "from", "length", "attributes"? }`,
/// where `attributes` is type-specific and omitted for kinds that take no
/// parameters. A `LINK` carries its target there as `attributes.url`:
///
/// ```json
/// { "type": "LINK", "from": 258, "length": 50,
///   "attributes": { "url": "https://example.com/page" } }
/// ```
///
/// (see pr0bel1230/max-api-docs `protocol/elements.md`).
///
/// `from`/`length` are span offsets into `text`. They are supplied by the
/// caller; this type does not interpret them. The reverse-engineered reference
/// describes them "в символах" (in characters); callers should confirm whether
/// the server treats them as Unicode scalar values or UTF-16 code units (the two
/// agree for BMP text but diverge for astral characters such as emoji).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MessageElement {
    #[serde(rename = "type")]
    pub kind: MessageElementKind,
    /// Span start offset into `text`. May be absent on the wire (treated as 0).
    #[serde(default)]
    pub from: usize,
    pub length: usize,
    /// Type-specific attributes (e.g. `url` for `LINK`); omitted when empty.
    #[serde(default, skip_serializing_if = "ElementAttributes::is_empty")]
    pub attributes: ElementAttributes,
}

/// Type-specific attributes carried by a [`MessageElement`].
///
/// Serializes to an `attributes` object and is omitted entirely when it holds
/// no values, matching the kinds (STRONG, EMPHASIZED, ...) that take no
/// parameters.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct ElementAttributes {
    /// Target URL for a `LINK` element.
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

    /// Target URL when this is a `LINK` element.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn session_lists_chats_from_login_payload() {
        let session = Session {
            token: "t".into(),
            login_payload: json!({
                "chats": [
                    { "id": 7268926, "type": "DIALOG", "title": "Alice" },
                    { "id": -100, "type": "CHANNEL" },
                ]
            }),
        };

        assert_eq!(
            session.chats(),
            vec![
                Chat {
                    id: 7268926,
                    chat_type: "DIALOG".into(),
                    title: "Alice".into(),
                },
                Chat {
                    id: -100,
                    chat_type: "CHANNEL".into(),
                    title: String::new(),
                },
            ]
        );
    }

    #[test]
    fn session_without_chats_is_empty() {
        let session = Session {
            token: "t".into(),
            login_payload: json!({}),
        };
        assert!(session.chats().is_empty());
    }
}
