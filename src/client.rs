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

use crate::auth::{AuthCaptchaConfig, LoginConfig};
use crate::captcha::http::{HttpServer, HttpServerConfig};
use crate::captcha::solver::{CaptchaSolver, CaptchaSolverConfig};
use crate::error::{Error, Result};
use crate::models::{IncomingMessage, MaxMessage, Session, UserAgent, BROWSER_USER_AGENT};
use crate::protocol::{opcode, Packet, CMD_ERROR};

const WS_URL: &str = "wss://ws-api.oneme.ru/websocket";
const ORIGIN: &str = "https://web.max.ru";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const FILE_PROCESS_TIMEOUT: Duration = Duration::from_secs(60);

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;

struct Inner {
    sink: Mutex<WsSink>,
    pending: Mutex<HashMap<u32, oneshot::Sender<Packet>>>,
    file_waiters: Mutex<HashMap<i64, oneshot::Sender<()>>>,
    seq: AtomicU32,
    cid: AtomicI64,
    session_initialized: AtomicBool,
    user_agent: UserAgent,
    http: reqwest::Client,
}

impl Inner {
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

    async fn send_packet(&self, packet: &Packet) -> Result<()> {
        let text = serde_json::to_string(packet)?;
        let mut sink = self.sink.lock().await;
        sink.send(Message::text(text)).await?;
        Ok(())
    }

    async fn invoke(&self, opcode: u16, payload: Value) -> Result<Packet> {
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
}

/// An asynchronous client for the Max (OneMe) WebSocket API.
///
/// The client is cheap to clone (`Arc` inside); clones share the same
/// connection, so you can move one clone into a background task to listen for
/// messages while sending from another.
#[derive(Clone)]
pub struct MaxClient {
    inner: Arc<Inner>,
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
        let mut request = WS_URL.into_client_request()?;
        {
            let headers = request.headers_mut();
            headers.insert("Origin", HeaderValue::from_static(ORIGIN));
            headers.insert("User-Agent", HeaderValue::from_static(BROWSER_USER_AGENT));
        }

        let (stream, _response) = connect_async(request).await?;
        let (sink, read) = stream.split();

        let http = reqwest::Client::builder()
            .user_agent(BROWSER_USER_AGENT)
            .build()?;

        let inner = Arc::new(Inner {
            sink: Mutex::new(sink),
            pending: Mutex::new(HashMap::new()),
            file_waiters: Mutex::new(HashMap::new()),
            seq: AtomicU32::new(1),
            cid: AtomicI64::new(-chrono_millis()),
            session_initialized: AtomicBool::new(false),
            user_agent,
            http,
        });

        let (msg_tx, msg_rx) = mpsc::unbounded_channel();

        tokio::spawn(read_loop(read, Arc::clone(&inner), msg_tx));

        let client = MaxClient { inner };
        client.spawn_keepalive();

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
                    break;
                }
            }
        });
    }

    /// Logs in using a saved session token when valid, otherwise starts the SMS auth flow.
    pub async fn login(&self, config: LoginConfig) -> Result<Session> {
        if let Some(token) = config.session_token.as_deref() {
            match self.login_with_token(token).await {
                Ok(session) => return Ok(session),
                Err(err) => {
                    tracing::info!(%err, "saved Max session token was rejected; starting SMS auth")
                }
            }
        }

        let phone = config
            .phone
            .as_deref()
            .ok_or_else(|| Error::UnexpectedResponse("missing phone for SMS login".into()))?;
        let sms_token = self
            .request_sms_code_with_auth_captcha(phone, &config.captcha)
            .await?;
        let code = config.operator.request_sms_code(phone).await?;
        self.verify_sms_code(&sms_token, code.trim()).await
    }

    async fn request_sms_code_with_auth_captcha(
        &self,
        phone: &str,
        config: &AuthCaptchaConfig,
    ) -> Result<String> {
        match self.request_sms_code(phone).await {
            Ok(token) => Ok(token),
            Err(Error::CaptchaRequired { link }) => {
                let solver_url = config
                    .solver_url
                    .as_ref()
                    .ok_or(Error::CaptchaSolverDisabled)?;
                let server = HttpServer::bind(HttpServerConfig::new(&config.callback_bind)).await?;
                let callback_addr = server.local_addr()?;
                let callback_url = config.callback_url(callback_addr);
                let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::new(
                    solver_url.clone(),
                    callback_url,
                ))?);
                tokio::spawn(server.with_captcha_solver(Arc::clone(&solver)).serve());
                let captcha_token = solver.solve(&link).await?;
                self.request_sms_code_with_captcha_token(phone, &captcha_token)
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn session_init(&self) -> Result<()> {
        if self
            .inner
            .session_initialized
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Ok(());
        }

        let payload = json!({
            "userAgent": self.inner.user_agent,
            "deviceId": uuid::Uuid::new_v4().to_string(),
        });
        match self.inner.invoke(opcode::SESSION_INIT, payload).await {
            Ok(_) => Ok(()),
            Err(err) => {
                self.inner
                    .session_initialized
                    .store(false, Ordering::Release);
                Err(err)
            }
        }
    }

    /// Requests a captcha challenge URL for SMS authentication.
    async fn request_auth_captcha(&self, phone: &str) -> Result<Option<String>> {
        self.session_init().await?;
        let payload = json!({
            "source": "auth",
            "identifier": phone,
        });
        match self
            .inner
            .invoke(opcode::AUTH_CAPTCHA_REQUEST, payload)
            .await
        {
            Ok(response) => Ok(response.payload["link"].as_str().map(str::to_string)),
            Err(Error::Server { opcode, message })
                if message == "captcha.create-session-failed" =>
            {
                tracing::debug!(
                    opcode,
                    "captcha session creation failed; continuing without token"
                );
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    /// Step 1 of SMS auth: requests that the server send an SMS code to `phone`.
    ///
    /// Returns a short-lived token that must be passed to
    /// [`MaxClient::verify_sms_code`] together with the received code.
    async fn request_sms_code(&self, phone: &str) -> Result<String> {
        if let Some(link) = self.request_auth_captcha(phone).await? {
            return Err(Error::CaptchaRequired { link });
        }

        self.request_sms_code_with_captcha_token(phone, "").await
    }

    /// Step 1 of SMS auth with a captcha token obtained from captcha solving.
    async fn request_sms_code_with_captcha_token(
        &self,
        phone: &str,
        captcha_token: &str,
    ) -> Result<String> {
        self.session_init().await?;
        let payload = json!({
            "phone": phone,
            "type": "START_AUTH",
            "language": "ru",
            "captchaToken": captcha_token,
        });
        let response = self.inner.invoke(opcode::AUTH_REQUEST, payload).await?;
        response.payload["token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::UnexpectedResponse("missing auth token".into()))
    }

    /// Step 2 of SMS auth: verifies the code and logs in.
    ///
    /// On success the long-lived session token is returned in [`Session`].
    async fn verify_sms_code(&self, sms_token: &str, code: &str) -> Result<Session> {
        let payload = json!({
            "token": sms_token,
            "verifyCode": code,
            "authTokenType": "CHECK_CODE",
        });
        let response = self.inner.invoke(opcode::AUTH, payload).await?;
        let token = response.payload["tokenAttrs"]["LOGIN"]["token"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| Error::UnexpectedResponse("missing session token".into()))?;

        self.login_with_token(&token).await
    }

    async fn login_with_token(&self, token: &str) -> Result<Session> {
        self.session_init().await?;
        self.perform_login(token).await
    }

    async fn perform_login(&self, token: &str) -> Result<Session> {
        let payload = json!({
            "interactive": true,
            "token": token,
            "chatsSync": 0,
            "contactsSync": 0,
            "presenceSync": 0,
            "draftsSync": 0,
            "chatsCount": 40,
        });
        let response = self.inner.invoke(opcode::LOGIN, payload).await?;
        Ok(Session {
            token: token.to_string(),
            login_payload: response.payload,
        })
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

fn text_message_payload(chat_id: i64, message: &MaxMessage, cid: i64) -> Value {
    json!({
        "chatId": chat_id,
        "message": {
            "text": message.text,
            "cid": cid,
            "type": "USER",
            "elements": message.elements,
            "attaches": [],
        },
        "notify": true,
    })
}

async fn read_loop(
    mut read: SplitStream<WsStream>,
    inner: Arc<Inner>,
    msg_tx: mpsc::UnboundedSender<IncomingMessage>,
) {
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
            handle_server_request(&inner, &msg_tx, packet).await;
        } else if let Some(tx) = inner.pending.lock().await.remove(&packet.seq) {
            let _ = tx.send(packet);
        }
    }

    // Connection ended: drop all pending senders so callers get ConnectionClosed.
    inner.pending.lock().await.clear();
    inner.file_waiters.lock().await.clear();
}

async fn handle_server_request(
    inner: &Arc<Inner>,
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
                let _ = msg_tx.send(message);
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
