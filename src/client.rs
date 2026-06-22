//! The asynchronous Max client.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

pub use crate::auth::{
    session_token_from_file, session_token_path, AuthCaptchaConfig, LoginConfig,
    DEFAULT_CALLBACK_BIND, DEFAULT_CAPTCHA_CALLBACK_PATH, DEFAULT_SOLVER_URL, ENV_CALLBACK_BIND,
    ENV_CALLBACK_URL_BASE, ENV_PASSWORD, ENV_PHONE, ENV_SOLVER_URL, SESSION_TOKEN_FILE,
};
use crate::error::{Error, Result};
use crate::models::{IncomingMessage, MaxMessage, Session, UserAgent, BROWSER_USER_AGENT};
pub use crate::operator_channels::{
    OperatorChannel, TelegramOperatorConfig, ENV_OPERATOR_CHANNEL, ENV_TELEGRAM_BOT_TOKEN,
    ENV_TELEGRAM_CHAT_ID, ENV_TELEGRAM_POLL_TIMEOUT_SECS,
};
use crate::protocol::{opcode, Packet, CMD_ERROR};

const WS_URL: &str = "wss://ws-api.oneme.ru/websocket";
const ORIGIN: &str = "https://web.max.ru";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const FILE_PROCESS_TIMEOUT: Duration = Duration::from_secs(60);
const UNKNOWN_USER_ID: i64 = i64::MIN;
const SECURITY_SERVICE_USER_ID: i64 = 543_835;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;

pub(crate) struct InnerClient {
    sink: Mutex<WsSink>,
    pending: Mutex<HashMap<u32, oneshot::Sender<Packet>>>,
    file_waiters: Mutex<HashMap<i64, oneshot::Sender<()>>>,
    login_config: Mutex<Option<LoginConfig>>,
    msg_tx: mpsc::UnboundedSender<IncomingMessage>,
    reconnect_tx: mpsc::UnboundedSender<()>,
    seq: AtomicU32,
    cid: AtomicI64,
    own_user_id: AtomicI64,
    session_initialized: AtomicBool,
    reconnecting: AtomicBool,
    user_agent: UserAgent,
    http: reqwest::Client,
}

impl InnerClient {
    fn next_seq(&self) -> u32 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    fn next_cid(&self) -> i64 {
        let now = -chrono_millis();
        // Client-generated message ids are negative to avoid colliding with
        // server-assigned ids. Keep them unique even within the same millisecond.
        let mut prev = self.cid.load(Ordering::Relaxed);
        loop {
            let next = now.min(prev - 1);
            match self
                .cid
                .compare_exchange_weak(prev, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return next,
                Err(actual) => prev = actual,
            }
        }
    }

    pub(crate) fn set_own_user_id(&self, user_id: i64) {
        self.own_user_id.store(user_id, Ordering::Release);
    }

    fn own_user_id(&self) -> Option<i64> {
        match self.own_user_id.load(Ordering::Acquire) {
            UNKNOWN_USER_ID => None,
            user_id => Some(user_id),
        }
    }

    async fn send_packet(&self, packet: &Packet) -> Result<()> {
        let text = serde_json::to_string(packet)?;
        let mut sink = self.sink.lock().await;
        sink.send(Message::text(text)).await?;
        Ok(())
    }

    fn notify_disconnect(&self) {
        if self
            .reconnecting
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            let _ = self.reconnect_tx.send(());
        }
    }

    pub(crate) async fn invoke(&self, opcode: u16, payload: Value) -> Result<Packet> {
        let seq = self.next_seq();
        let (tx, rx) = oneshot::channel();
        {
            let mut pending = self.pending.lock().await;
            pending.insert(seq, tx);
        }

        let packet = Packet::request(seq, opcode, payload);
        if let Err(err) = self.send_packet(&packet).await {
            self.pending.lock().await.remove(&seq);
            return Err(err);
        }

        match tokio::time::timeout(DEFAULT_TIMEOUT, rx).await {
            Ok(Ok(response)) => {
                if response.cmd == CMD_ERROR {
                    return Err(Error::Server {
                        opcode,
                        message: error_message(&response.payload),
                    });
                }
                Ok(response)
            }
            // Sender dropped -> connection closed.
            Ok(Err(_)) => Err(Error::ConnectionClosed),
            Err(_) => {
                self.pending.lock().await.remove(&seq);
                Err(Error::Timeout(opcode))
            }
        }
    }

    pub(crate) async fn session_init(&self) -> Result<()> {
        if self
            .session_initialized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let payload = json!({
            "userAgent": self.user_agent,
            "deviceId": uuid::Uuid::new_v4().to_string(),
        });
        match self.invoke(opcode::SESSION_INIT, payload).await {
            Ok(_) => Ok(()),
            Err(err) => {
                self.session_initialized.store(false, Ordering::Release);
                Err(err)
            }
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
    /// Opens a WebSocket connection and starts the background read and
    /// keepalive tasks.
    ///
    /// Returns the client together with a receiver of [`IncomingMessage`]s that
    /// the server pushes (`NOTIF_MESSAGE`). Authenticate next with
    /// [`MaxClient::login`].
    pub async fn connect() -> Result<(Self, mpsc::UnboundedReceiver<IncomingMessage>)> {
        Self::connect_with(UserAgent::default()).await
    }

    /// Like [`MaxClient::connect`] but with a custom [`UserAgent`].
    pub async fn connect_with(
        user_agent: UserAgent,
    ) -> Result<(Self, mpsc::UnboundedReceiver<IncomingMessage>)> {
        let (sink, read) = open_connection().await?;
        let http = reqwest::Client::builder()
            .user_agent(BROWSER_USER_AGENT)
            .build()?;
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        let (reconnect_tx, reconnect_rx) = mpsc::unbounded_channel();

        let inner = Arc::new(InnerClient {
            sink: Mutex::new(sink),
            pending: Mutex::new(HashMap::new()),
            file_waiters: Mutex::new(HashMap::new()),
            login_config: Mutex::new(None),
            msg_tx,
            reconnect_tx,
            seq: AtomicU32::new(1),
            cid: AtomicI64::new(-chrono_millis()),
            own_user_id: AtomicI64::new(UNKNOWN_USER_ID),
            session_initialized: AtomicBool::new(false),
            reconnecting: AtomicBool::new(false),
            user_agent,
            http,
        });

        tokio::spawn(read_loop(read, Arc::clone(&inner)));

        let client = MaxClient { inner };
        client.spawn_keepalive();
        client.spawn_reconnect_loop(reconnect_rx);

        Ok((client, msg_rx))
    }

    fn spawn_keepalive(&self) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(KEEPALIVE_INTERVAL).await;
                if inner
                    .invoke(opcode::PING, json!({ "interactive": false }))
                    .await
                    .is_err()
                {
                    inner.notify_disconnect();
                }
            }
        });
    }

    fn spawn_reconnect_loop(&self, mut reconnect_rx: mpsc::UnboundedReceiver<()>) {
        let inner = Arc::clone(&self.inner);
        tokio::spawn(async move {
            while reconnect_rx.recv().await.is_some() {
                reconnect_inner(Arc::clone(&inner)).await;
            }
        });
    }

    /// Logs in using a saved session token when valid, otherwise starts the SMS auth flow.
    pub async fn login(&self, config: LoginConfig) -> Result<Session> {
        let session = self.inner.login(config.clone()).await?;
        *self.inner.login_config.lock().await = Some(config);
        Ok(session)
    }

    /// Sends a text message to `chat_id`.
    pub async fn send_text(&self, chat_id: i64, message: MaxMessage) -> Result<()> {
        let payload = text_message_payload(chat_id, &message, self.inner.next_cid());
        self.inner.invoke(opcode::MSG_SEND, payload).await?;
        Ok(())
    }

    /// Sends a "typing..." notification to `chat_id`.
    pub async fn send_typing(&self, chat_id: i64) -> Result<()> {
        let payload = json!({
            "chatId": chat_id,
            "type": "TEXT",
        });
        self.inner.invoke(opcode::MSG_TYPING, payload).await?;
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

        let response = self
            .inner
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

        let payload = json!({
            "chatId": chat_id,
            "message": {
                "text": caption,
                "cid": self.inner.next_cid(),
                "type": "USER",
                "elements": [],
                "attaches": [{ "_type": "FILE", "fileId": file_id }],
            },
            "notify": true,
        });
        self.inner.invoke(opcode::MSG_SEND, payload).await?;
        Ok(())
    }

    /// Sends a single keepalive ping. Mostly useful for tests; the background
    /// task pings automatically.
    pub async fn ping(&self) -> Result<()> {
        self.inner
            .invoke(opcode::PING, json!({ "interactive": false }))
            .await?;
        Ok(())
    }
}

async fn open_connection() -> Result<(WsSink, SplitStream<WsStream>)> {
    let mut request = WS_URL.into_client_request()?;
    {
        let headers = request.headers_mut();
        headers.insert("Origin", HeaderValue::from_static(ORIGIN));
        headers.insert("User-Agent", HeaderValue::from_static(BROWSER_USER_AGENT));
    }

    let (stream, _response) = connect_async(request).await?;
    Ok(stream.split())
}

async fn reconnect_inner(inner: Arc<InnerClient>) {
    let mut config = match inner.login_config.lock().await.clone() {
        Some(config) => config,
        None => {
            inner.reconnecting.store(false, Ordering::Release);
            return;
        }
    };

    loop {
        tracing::warn!(
            delay_secs = RECONNECT_DELAY.as_secs(),
            "Max connection was lost; reconnecting through the main login flow"
        );
        tokio::time::sleep(RECONNECT_DELAY).await;

        config.session_token = session_token_from_file();

        match open_connection().await {
            Ok((sink, read)) => {
                {
                    let mut current_sink = inner.sink.lock().await;
                    *current_sink = sink;
                }
                inner.session_initialized.store(false, Ordering::Release);
                inner.own_user_id.store(UNKNOWN_USER_ID, Ordering::Release);
                tokio::spawn(read_loop(read, Arc::clone(&inner)));

                match inner.login(config.clone()).await {
                    Ok(session) => {
                        config.session_token = Some(session.token);
                        *inner.login_config.lock().await = Some(config);
                        inner.reconnecting.store(false, Ordering::Release);
                        tracing::info!("Max reconnection completed");
                        break;
                    }
                    Err(err) => {
                        tracing::warn!(%err, "Max login after reconnect failed; retrying");
                        inner.notify_disconnect();
                    }
                }
            }
            Err(err) => {
                tracing::warn!(%err, "Max WebSocket reconnect failed; retrying");
            }
        }
    }
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

async fn read_loop(mut read: SplitStream<WsStream>, inner: Arc<InnerClient>) {
    while let Some(frame) = read.next().await {
        let text = match frame {
            Ok(Message::Text(text)) => text.to_string(),
            Ok(Message::Binary(bin)) => match String::from_utf8(bin.to_vec()) {
                Ok(text) => text,
                Err(_) => continue,
            },
            Ok(Message::Close(_)) | Err(_) => break,
            Ok(_) => continue,
        };

        let packet: Packet = match serde_json::from_str(&text) {
            Ok(packet) => packet,
            Err(err) => {
                tracing::warn!(%err, "failed to parse frame");
                continue;
            }
        };

        if packet.is_request() {
            handle_server_request(&inner, &inner.msg_tx, packet).await;
        } else if let Some(tx) = inner.pending.lock().await.remove(&packet.seq) {
            let _ = tx.send(packet);
        }
    }

    // Connection ended: drop all pending senders so callers get ConnectionClosed.
    inner.pending.lock().await.clear();
    inner.file_waiters.lock().await.clear();
    inner.session_initialized.store(false, Ordering::Release);
    inner.notify_disconnect();
}

async fn handle_server_request(
    inner: &Arc<InnerClient>,
    msg_tx: &mpsc::UnboundedSender<IncomingMessage>,
    packet: Packet,
) {
    match packet.opcode {
        opcode::NOTIF_MESSAGE => {
            if let Some(message) = parse_incoming(&packet.payload) {
                // Acknowledge the push, mirroring the official web client.
                let ack = Packet::response(
                    packet.seq,
                    packet.opcode,
                    json!({ "chatId": message.chat_id, "messageId": message.message_id }),
                );
                let _ = inner.send_packet(&ack).await;
                if !is_filtered_incoming_message(&message, inner.own_user_id()) {
                    let _ = msg_tx.send(message);
                }
            }
        }
        opcode::NOTIF_ATTACH => {
            if let Some(file_id) = packet.payload["fileId"].as_i64() {
                if let Some(tx) = inner.file_waiters.lock().await.remove(&file_id) {
                    let _ = tx.send(());
                }
            }
        }
        _ => {}
    }
}

fn parse_incoming(payload: &Value) -> Option<IncomingMessage> {
    let chat_id = payload["chatId"].as_i64()?;
    let message = &payload["message"];
    Some(IncomingMessage {
        chat_id,
        message_id: message["id"].as_i64().unwrap_or_default(),
        sender: message["sender"].as_i64().unwrap_or_default(),
        text: message["text"].as_str().unwrap_or_default().to_string(),
        time: message["time"].as_i64().unwrap_or_default(),
    })
}

fn is_filtered_incoming_message(message: &IncomingMessage, own_user_id: Option<i64>) -> bool {
    is_own_message(message, own_user_id) || is_security_service_message(message)
}

fn is_own_message(message: &IncomingMessage, own_user_id: Option<i64>) -> bool {
    own_user_id == Some(message.sender)
}

fn is_security_service_message(message: &IncomingMessage) -> bool {
    message.sender == SECURITY_SERVICE_USER_ID
}

fn error_message(payload: &Value) -> String {
    payload["error"]
        .as_str()
        .or_else(|| payload["message"].as_str())
        .or_else(|| payload["localizedMessage"].as_str())
        .unwrap_or("unknown error")
        .to_string()
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
    fn parses_incoming_notif_message() {
        let payload = json!({
            "chatId": -100,
            "message": {
                "id": 555,
                "sender": 777,
                "text": "hi there",
                "time": 1_700_000_000_000i64,
            }
        });
        let msg = parse_incoming(&payload).expect("should parse");
        assert_eq!(msg.chat_id, -100);
        assert_eq!(msg.message_id, 555);
        assert_eq!(msg.sender, 777);
        assert_eq!(msg.text, "hi there");
        assert_eq!(msg.time, 1_700_000_000_000);
    }

    #[test]
    fn missing_chat_id_is_not_a_message() {
        assert!(parse_incoming(&json!({ "message": { "text": "x" } })).is_none());
    }

    #[test]
    fn identifies_own_incoming_message() {
        let msg = IncomingMessage {
            chat_id: 1,
            message_id: 2,
            sender: 777,
            text: "echo".into(),
            time: 3,
        };

        assert!(is_own_message(&msg, Some(777)));
        assert!(!is_own_message(&msg, Some(778)));
        assert!(!is_own_message(&msg, None));
    }

    #[test]
    fn filters_security_service_message() {
        let msg = IncomingMessage {
            chat_id: 300_880_330,
            message_id: 0,
            sender: SECURITY_SERVICE_USER_ID,
            text: "New MAX login".into(),
            time: 0,
        };

        assert!(is_filtered_incoming_message(&msg, None));
    }

    #[test]
    fn extracts_error_message() {
        assert_eq!(error_message(&json!({ "error": "boom" })), "boom");
        assert_eq!(error_message(&json!({ "message": "m" })), "m");
        assert_eq!(error_message(&json!({})), "unknown error");
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
                { "_type": "STRONG", "from": 0, "length": 5 },
                {
                    "_type": "LINK",
                    "from": 6,
                    "length": 4,
                    "url": "https://example.test"
                }
            ])
        );
    }
}
