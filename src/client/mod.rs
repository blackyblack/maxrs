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
use crate::models::{IncomingMessage, MaxMessage, Session, UserAgent};
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
    msg_tx: mpsc::UnboundedSender<IncomingMessage>,
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

    async fn disconnect(&self) {
        if let Some(task) = self.state.lock().await.keepalive_task.take() {
            task.abort();
        }
        self.file_waiters.lock().await.clear();
        self.transport.close().await;
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
    /// Creates a disconnected client handle and message receiver.
    ///
    /// Call [`MaxClient::connect`] to open the WebSocket connection and log in.
    pub fn new(config: LoginConfig) -> Result<(Self, mpsc::UnboundedReceiver<IncomingMessage>)> {
        Self::new_with_user_agent(config, UserAgent::default())
    }

    /// Like [`MaxClient::new`] but with a custom [`UserAgent`].
    pub fn new_with_user_agent(
        config: LoginConfig,
        user_agent: UserAgent,
    ) -> Result<(Self, mpsc::UnboundedReceiver<IncomingMessage>)> {
        let header_user_agent = user_agent.header_user_agent.clone();
        let http = reqwest::Client::builder()
            .user_agent(header_user_agent)
            .build()?;
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();

        let inner = Arc::new(InnerClient {
            transport: Transport::new(),
            file_waiters: Mutex::new(HashMap::new()),
            login_config: Mutex::new(config.clone()),
            connect_lock: Mutex::new(()),
            msg_tx,
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

        Ok((MaxClient { inner }, msg_rx))
    }

    /// Opens or reopens the WebSocket connection, logs in, and starts the
    /// background read and keepalive tasks.
    ///
    /// This is the only path that reconnects a disconnected client. If the
    /// saved session token is missing or rejected, the configured login flow may
    /// request SMS/password/captcha input.
    pub async fn connect(&self) -> Result<Session> {
        let _guard = self.inner.connect_lock.lock().await;
        self.inner.disconnect().await;

        if let Err(err) = self
            .inner
            .transport
            .connect(&self.inner, &self.inner.user_agent.header_user_agent)
            .await
        {
            self.inner.disconnect().await;
            return Err(err);
        }

        if let Err(err) = self.inner.session_init().await {
            self.inner.disconnect().await;
            return Err(err);
        }

        self.spawn_keepalive().await;

        let config = self.inner.login_config.lock().await.clone();
        let session = match InnerClient::login(Arc::clone(&self.inner), config.clone()).await {
            Ok(session) => session,
            Err(err) => {
                self.inner.disconnect().await;
                return Err(err);
            }
        };
        let mut stored_config = config;
        stored_config.session_token = Some(session.token.clone());
        *self.inner.login_config.lock().await = stored_config;

        Ok(session)
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
                    tracing::debug!(%err, "Max keepalive failed");
                    inner.disconnect().await;
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
                format!("attachment; filename={file_name}"),
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

        // Best effort: wait for processing confirmation, but proceed on timeout.
        let _ = tokio::time::timeout(FILE_PROCESS_TIMEOUT, rx).await;
        self.inner.file_waiters.lock().await.remove(&file_id);

        let payload = file_message_payload(chat_id, caption, file_id, self.inner.next_cid().await);
        self.invoke(opcode::MSG_SEND, payload).await?;
        Ok(())
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
                    self.inner.disconnect().await;
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
            "attaches": [{ "type": "FILE", "fileId": file_id }],
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
            json!([{ "type": "FILE", "fileId": 987654 }])
        );
        assert_eq!(payload["notify"], true);
    }

    #[test]
    fn empty_buffer_file_name_falls_back_to_file() {
        assert_eq!(normalized_file_name(String::new()), "file");
        assert_eq!(normalized_file_name("report.txt".to_string()), "report.txt");
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
