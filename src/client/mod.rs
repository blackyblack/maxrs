//! The asynchronous Max client.

mod read_loop;
mod transport;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::auth::LoginConfig;
use crate::error::{Error, Result};
use crate::models::{IncomingMessage, LoginSession, MaxMessage, UserAgent};
use crate::protocol::{opcode, Packet};

use self::transport::Transport;

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const FILE_PROCESS_TIMEOUT: Duration = Duration::from_secs(60);

struct ClientState {
    seq: u32,
    cid: i64,
    own_user_id: Option<i64>,
    keepalive_task: Option<tokio::task::JoinHandle<()>>,
}

pub(crate) struct InnerClient {
    transport: Transport,
    file_waiters: Mutex<HashMap<i64, oneshot::Sender<()>>>,
    login_config: Mutex<LoginConfig>,
    connect_lock: Mutex<()>,
    msg_tx: Mutex<Option<mpsc::UnboundedSender<IncomingMessage>>>,
    state: Mutex<ClientState>,
    device_id: String,
    user_agent: UserAgent,
    http: reqwest::Client,
}

impl InnerClient {
    async fn next_seq(&self) -> u32 {
        let mut state = self.state.lock().await;
        let seq = state.seq;
        state.seq = state.seq.wrapping_add(1);
        seq
    }

    async fn next_cid(&self) -> i64 {
        let now = -chrono_millis();
        // Client-generated message ids are negative to avoid colliding with
        // server-assigned ids. Keep them unique even within the same millisecond.
        let mut state = self.state.lock().await;
        let next = now.min(state.cid - 1);
        state.cid = next;
        next
    }

    pub(crate) async fn set_own_user_id(&self, user_id: i64) {
        self.state.lock().await.own_user_id = Some(user_id);
    }

    async fn own_user_id(&self) -> Option<i64> {
        self.state.lock().await.own_user_id
    }

    pub(crate) async fn invoke(&self, opcode: u16, payload: Value) -> Result<Packet> {
        self.transport
            .invoke(self.next_seq().await, opcode, payload)
            .await
    }

    async fn session_init(&self) -> Result<()> {
        let payload = session_init_payload(&self.user_agent, &self.device_id);
        self.invoke(opcode::SESSION_INIT, payload).await.map(|_| ())
    }

    pub(crate) async fn disconnect(&self) {
        if let Some(task) = self.state.lock().await.keepalive_task.take() {
            task.abort();
        }
        self.file_waiters.lock().await.clear();
        self.transport.close().await;
    }

    pub(crate) async fn fail(&self) {
        self.msg_tx.lock().await.take();
        self.disconnect().await;
    }

    async fn store_keepalive(&self, task: tokio::task::JoinHandle<()>) {
        if let Some(previous) = self.state.lock().await.keepalive_task.replace(task) {
            previous.abort();
        }
    }
}

/// An asynchronous client for the Max (OneMe) WebSocket API.
///
/// The client is cheap to clone (`Arc` inside); clones share the same
/// connection, so you can move one clone into a background task to listen for
/// messages while sending from another.
#[derive(Clone)]
pub struct MaxClient {
    inner: Arc<InnerClient>,
}

impl MaxClient {
    /// Creates a disconnected client handle.
    ///
    /// Call [`MaxClient::connect`] to open the WebSocket connection, log in, and
    /// obtain the incoming-message receiver for that connection. If the receiver
    /// yields `None`, the connection died unexpectedly; the same client can be
    /// connected again with [`MaxClient::connect`].
    pub fn new(config: LoginConfig) -> Result<Self> {
        Self::new_with_user_agent(config, UserAgent::default())
    }

    /// Like [`MaxClient::new`] but with a custom [`UserAgent`].
    pub fn new_with_user_agent(config: LoginConfig, user_agent: UserAgent) -> Result<Self> {
        let header_user_agent = user_agent.header_user_agent.clone();
        let http = reqwest::Client::builder()
            .user_agent(header_user_agent)
            .build()?;
        let inner = Arc::new(InnerClient {
            transport: Transport::new(),
            file_waiters: Mutex::new(HashMap::new()),
            login_config: Mutex::new(config.clone()),
            connect_lock: Mutex::new(()),
            msg_tx: Mutex::new(None),
            state: Mutex::new(ClientState {
                seq: 1,
                cid: -chrono_millis(),
                own_user_id: None,
                keepalive_task: None,
            }),
            device_id: uuid::Uuid::new_v4().to_string(),
            user_agent,
            http,
        });

        Ok(MaxClient { inner })
    }

    /// Opens or reopens the WebSocket connection, logs in, and starts the
    /// background read and keepalive tasks.
    ///
    /// This is the path that connects or reconnects a client. If the saved
    /// session token is missing or rejected, the configured login flow may
    /// request SMS/password/captcha input. Each successful call returns the
    /// incoming-message receiver for that connection.
    pub async fn connect(
        &self,
    ) -> Result<(LoginSession, mpsc::UnboundedReceiver<IncomingMessage>)> {
        let _guard = self.inner.connect_lock.lock().await;
        self.inner.disconnect().await;
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        *self.inner.msg_tx.lock().await = Some(msg_tx);

        if let Err(err) = self
            .inner
            .transport
            .connect(&self.inner, &self.inner.user_agent.header_user_agent)
            .await
        {
            self.inner.fail().await;
            return Err(err);
        }

        if let Err(err) = self.inner.session_init().await {
            self.inner.fail().await;
            return Err(err);
        }

        self.spawn_keepalive().await;

        let config = self.inner.login_config.lock().await.clone();
        let session = match InnerClient::login(Arc::clone(&self.inner), config.clone()).await {
            Ok(session) => session,
            Err(err) => {
                self.inner.fail().await;
                return Err(err);
            }
        };
        let mut stored_config = config;
        stored_config.session_token = Some(session.token.clone());
        *self.inner.login_config.lock().await = stored_config;

        Ok((session, msg_rx))
    }

    async fn spawn_keepalive(&self) {
        let inner = Arc::clone(&self.inner);
        let task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(KEEPALIVE_INTERVAL).await;
                if let Err(err) = inner
                    .invoke(opcode::PING, json!({ "interactive": false }))
                    .await
                {
                    tracing::warn!(%err, "Max keepalive failed");
                    inner.fail().await;
                    break;
                }
            }
        });
        self.inner.store_keepalive(task).await;
    }

    /// Sends a text message to `chat_id`.
    pub async fn send_text(&self, chat_id: i64, message: MaxMessage) -> Result<()> {
        let payload = text_message_payload(chat_id, &message, self.inner.next_cid().await);
        self.invoke(opcode::MSG_SEND, payload).await?;
        Ok(())
    }

    /// Sends a "typing..." notification to `chat_id`.
    pub async fn send_typing(&self, chat_id: i64) -> Result<()> {
        let payload = json!({
            "chatId": chat_id,
            "type": "TEXT",
        });
        self.invoke(opcode::MSG_TYPING, payload).await?;
        Ok(())
    }

    /// Uploads a local file and sends it to `chat_id` with an optional caption.
    ///
    /// Implements the three-step flow: request an upload URL (`FILE_UPLOAD`),
    /// HTTP `POST` the bytes, wait for the server's `NOTIF_ATTACH`
    /// confirmation, then send a message referencing the uploaded file.
    pub async fn send_file(
        &self,
        chat_id: i64,
        path: impl AsRef<std::path::Path>,
        caption: &str,
    ) -> Result<()> {
        let path = path.as_ref();
        let bytes = tokio::fs::read(path).await?;
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "file".to_string());

        self.send_uploaded_file(chat_id, file_name, bytes, caption)
            .await
    }

    /// Uploads a file from an in-memory byte buffer and sends it to `chat_id`.
    ///
    /// This follows the same Max upload flow as [`MaxClient::send_file`], but
    /// uses the supplied bytes instead of reading from the filesystem. The
    /// `file_name` is sent in the HTTP `Content-Disposition` header.
    pub async fn send_file_bytes<'a>(
        &self,
        chat_id: i64,
        file_name: impl Into<String>,
        bytes: impl Into<Cow<'a, [u8]>>,
        caption: &str,
    ) -> Result<()> {
        self.send_uploaded_file(
            chat_id,
            file_name.into(),
            bytes.into().into_owned(),
            caption,
        )
        .await
    }

    async fn send_uploaded_file(
        &self,
        chat_id: i64,
        file_name: String,
        bytes: Vec<u8>,
        caption: &str,
    ) -> Result<()> {
        let file_name = normalized_file_name(file_name);
        let response = self
            .invoke(opcode::FILE_UPLOAD, json!({ "count": 1 }))
            .await?;
        let info = response.payload["info"]
            .get(0)
            .ok_or_else(|| Error::UnexpectedResponse("empty file upload info".into()))?;
        let url = info["url"]
            .as_str()
            .ok_or_else(|| Error::UnexpectedResponse("missing upload url".into()))?
            .to_string();
        let file_id = info["fileId"]
            .as_i64()
            .ok_or_else(|| Error::UnexpectedResponse("missing fileId".into()))?;

        // Register a waiter for the NOTIF_ATTACH confirmation before uploading.
        let (tx, rx) = oneshot::channel();
        self.inner.file_waiters.lock().await.insert(file_id, tx);

        let size = bytes.len();
        let result = self
            .inner
            .http
            .post(&url)
            .header(
                "Content-Disposition",
                format!(
                    "attachment; filename={}",
                    percent_encode_file_name(&file_name)
                ),
            )
            .header("Content-Length", size.to_string())
            .header(
                "Content-Range",
                format!("0-{}/{}", size.saturating_sub(1), size),
            )
            .body(bytes)
            .send()
            .await;

        if let Err(err) = result {
            self.inner.file_waiters.lock().await.remove(&file_id);
            return Err(err.into());
        }

        match tokio::time::timeout(FILE_PROCESS_TIMEOUT, rx).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                self.inner.file_waiters.lock().await.remove(&file_id);
                return Err(Error::ConnectionClosed);
            }
            Err(_) => {
                self.inner.file_waiters.lock().await.remove(&file_id);
                return Err(Error::FileProcessingTimeout(file_id));
            }
        }
        self.inner.file_waiters.lock().await.remove(&file_id);

        let payload = file_message_payload(chat_id, caption, file_id, self.inner.next_cid().await);
        self.invoke(opcode::MSG_SEND, payload).await?;
        Ok(())
    }

    /// Returns whether the WebSocket sink is present and the read task is still running.
    pub async fn is_connected(&self) -> bool {
        self.inner.transport.is_connected().await
    }

    /// Sends a single keepalive ping. Mostly useful for tests; the background
    /// task pings automatically.
    pub async fn ping(&self) -> Result<()> {
        self.invoke(opcode::PING, json!({ "interactive": false }))
            .await?;
        Ok(())
    }

    async fn invoke(&self, opcode: u16, payload: Value) -> Result<Packet> {
        match self.inner.invoke(opcode, payload).await {
            Ok(response) => Ok(response),
            Err(err) => {
                // A server rejection doesn't mean the socket is dead; keep it
                // open and only disconnect on transport failures.
                if !matches!(&err, Error::Server { .. }) {
                    self.inner.fail().await;
                }
                Err(err)
            }
        }
    }
}

fn session_init_payload(user_agent: &UserAgent, device_id: &str) -> Value {
    json!({
        "userAgent": user_agent,
        "deviceId": device_id,
    })
}

fn text_message_payload(chat_id: i64, message: &MaxMessage, cid: i64) -> Value {
    let text = &message.text;
    let elements = &message.elements;
    json!({
        "chatId": chat_id,
        "message": {
            "text": text,
            "cid": cid,
            "type": "USER",
            "elements": elements,
            "attaches": [],
        },
        "notify": true,
    })
}

fn file_message_payload(chat_id: i64, caption: &str, file_id: i64, cid: i64) -> Value {
    json!({
        "chatId": chat_id,
        "message": {
            "text": caption,
            "cid": cid,
            "type": "USER",
            "elements": [],
            "attaches": [{ "_type": "FILE", "fileId": file_id }],
        },
        "notify": true,
    })
}

fn normalized_file_name(file_name: String) -> String {
    if file_name.is_empty() {
        "file".to_string()
    } else {
        file_name
    }
}

fn percent_encode_file_name(file_name: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(file_name.len());
    for byte in file_name.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'/') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(HEX[(byte >> 4) as usize] as char);
            encoded.push(HEX[(byte & 0x0F) as usize] as char);
        }
    }
    encoded
}

fn chrono_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_config() -> LoginConfig {
        LoginConfig {
            phone: None,
            password: None,
            session_token: None,
            captcha: crate::auth::AuthCaptchaConfig {
                solver_url: None,
                callback_bind: "127.0.0.1:0".into(),
                callback_url_base: None,
            },
            operator: crate::auth::operator_channels::OperatorChannel::None,
        }
    }

    #[tokio::test]
    async fn fail_closes_message_channel_but_disconnect_keeps_it_open() {
        let client = MaxClient::new(test_config()).expect("client");
        let (tx, mut messages) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(tx);

        client.inner.disconnect().await;
        assert!(matches!(
            messages.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        client.inner.fail().await;
        assert!(messages.recv().await.is_none());
    }

    #[tokio::test]
    async fn message_channel_can_be_recreated_after_failure() {
        let client = MaxClient::new(test_config()).expect("client");
        let (old_tx, mut old_messages) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(old_tx);

        client.inner.fail().await;
        assert!(old_messages.recv().await.is_none());

        let (new_tx, mut new_messages) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(new_tx);
        let message = IncomingMessage {
            chat_id: 1,
            message_id: 2,
            sender: 3,
            text: "after reconnect".into(),
            time: 4,
        };
        client
            .inner
            .msg_tx
            .lock()
            .await
            .as_ref()
            .expect("recreated sender")
            .send(message.clone())
            .expect("send message");

        let received = new_messages.recv().await.expect("message");
        assert_eq!(received.chat_id, message.chat_id);
        assert_eq!(received.message_id, message.message_id);
        assert_eq!(received.sender, message.sender);
        assert_eq!(received.text, message.text);
        assert_eq!(received.time, message.time);
    }

    #[test]
    fn text_message_payload_matches_web_schema() {
        let payload =
            text_message_payload(295438091, &MaxMessage::new("hello"), -1_700_000_000_001);

        assert_eq!(payload["chatId"], 295438091);
        assert_eq!(payload["message"]["text"], "hello");
        assert_eq!(payload["message"]["cid"], -1_700_000_000_001i64);
        assert_eq!(payload["message"]["type"], "USER");
        assert_eq!(payload["message"]["elements"], json!([]));
        assert_eq!(payload["message"]["attaches"], json!([]));
        assert_eq!(payload["notify"], true);
    }

    #[test]
    fn text_message_payload_serializes_typed_formatter_elements() {
        let message = MaxMessage::with_elements(
            "hello docs",
            vec![
                crate::models::MessageElement::strong(0, 5),
                crate::models::MessageElement::link(6, 4, "https://example.test"),
            ],
        );
        let payload = text_message_payload(295438091, &message, -1_700_000_000_002);

        assert_eq!(
            payload["message"]["elements"],
            json!([
                { "type": "STRONG", "from": 0, "length": 5 },
                {
                    "type": "LINK",
                    "from": 6,
                    "length": 4,
                    "attributes": { "url": "https://example.test" }
                }
            ])
        );
    }

    #[test]
    fn link_element_nests_url_under_attributes() {
        let element = crate::models::MessageElement::link(6, 4, "https://example.test");
        let value = serde_json::to_value(&element).unwrap();

        assert_eq!(value["type"], "LINK");
        assert_eq!(value["attributes"]["url"], "https://example.test");
        assert!(
            value.get("url").is_none(),
            "url must not be serialized at the top level"
        );
    }

    #[test]
    fn formatting_element_omits_attributes() {
        let value = serde_json::to_value(crate::models::MessageElement::strong(0, 5)).unwrap();

        assert_eq!(value, json!({ "type": "STRONG", "from": 0, "length": 5 }));
        assert!(value.get("attributes").is_none());
    }

    #[test]
    fn link_element_round_trips_through_attributes() {
        let element = crate::models::MessageElement::link(1, 2, "https://round.trip");
        let json = serde_json::to_string(&element).unwrap();
        let parsed: crate::models::MessageElement = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed, element);
        assert_eq!(parsed.url(), Some("https://round.trip"));
    }

    #[test]
    fn file_message_payload_matches_web_schema() {
        let payload = file_message_payload(295438091, "caption", 987654, -1_700_000_000_003);

        assert_eq!(payload["chatId"], 295438091);
        assert_eq!(payload["message"]["text"], "caption");
        assert_eq!(payload["message"]["cid"], -1_700_000_000_003i64);
        assert_eq!(payload["message"]["type"], "USER");
        assert_eq!(payload["message"]["elements"], json!([]));
        assert_eq!(
            payload["message"]["attaches"],
            json!([{ "_type": "FILE", "fileId": 987654 }])
        );
        assert_eq!(payload["notify"], true);
    }

    #[test]
    fn empty_buffer_file_name_falls_back_to_file() {
        assert_eq!(normalized_file_name(String::new()), "file");
        assert_eq!(normalized_file_name("report.txt".to_string()), "report.txt");
    }

    #[test]
    fn percent_encode_file_name_leaves_ascii_filename_characters_unchanged() {
        assert_eq!(
            percent_encode_file_name("reports/report-2026_07.02~final.txt"),
            "reports/report-2026_07.02~final.txt"
        );
    }

    #[test]
    fn percent_encode_file_name_encodes_utf8_and_spaces() {
        assert_eq!(
            percent_encode_file_name("привет мир.txt"),
            "%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82%20%D0%BC%D0%B8%D1%80.txt"
        );
    }

    #[test]
    fn session_init_payload_uses_supplied_device_id() {
        let user_agent = UserAgent::default();
        let payload = session_init_payload(&user_agent, "stable-device-id");

        assert_eq!(payload["deviceId"], "stable-device-id");
        assert_eq!(payload["userAgent"]["deviceType"], user_agent.device_type);
        assert_eq!(
            payload["userAgent"]["headerUserAgent"],
            user_agent.header_user_agent
        );
    }
}
