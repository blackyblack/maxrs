//! The asynchronous Max client.

mod attachment_registry;
mod dispatcher;
mod read_loop;
mod recovery;
mod transport;

#[cfg(test)]
mod recovery_tests;

use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
#[cfg(test)]
use tokio::sync::oneshot;
use tokio::sync::{mpsc, Mutex};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::auth::LoginConfig;
use crate::error::{Error, Result};
use crate::models::{IncomingMessage, MaxMessage, UserAgent};
use crate::protocol::opcode;
#[cfg(test)]
use crate::protocol::Packet;

pub(crate) use self::recovery::Connection;

/// Handles incoming messages dispatched by [`MaxClient`].
pub trait ChatHandler: Send + Sync + 'static {
    /// Called promptly for each admitted incoming message.
    ///
    /// The client deliberately does not limit concurrent handler futures: a
    /// handler may need to notify the user before waiting on expensive work.
    /// Implementations should apply their own concurrency bound around that
    /// expensive phase after sending the initial response.
    fn on_message(&self, msg: IncomingMessage) -> impl Future<Output = Result<()>> + Send;
}

const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);
const FILE_PROCESS_TIMEOUT: Duration = Duration::from_secs(60);
struct InnerClient {
    login_config: LoginConfig,
    run_lock: Arc<Mutex<()>>,
    recovery: recovery::Recovery,
    handler_shutdown: CancellationToken,
    cid: Mutex<i64>,
    device_id: String,
    user_agent: UserAgent,
    http: reqwest::Client,
    #[cfg(test)]
    ws_url: std::sync::Mutex<Option<String>>,
}

impl InnerClient {
    async fn next_cid(&self) -> i64 {
        let now = -chrono_millis();
        let mut cid = self.cid.lock().await;
        *cid = now.min(*cid - 1);
        *cid
    }
}

/// Only the runner owns task handles and tears down an attempt.
struct Attempt {
    connection: Arc<Connection>,
    tasks: JoinSet<()>,
    connected_at: Option<tokio::time::Instant>,
}

impl Attempt {
    fn new(connection: Arc<Connection>) -> Self {
        Self {
            connection,
            tasks: JoinSet::new(),
            connected_at: None,
        }
    }

    async fn close(&mut self) {
        self.connection.cancel();
        self.tasks.abort_all();
        while self.tasks.join_next().await.is_some() {}
        self.connection.transport.close().await;
    }
}

impl Drop for Attempt {
    fn drop(&mut self) {
        self.connection.cancel();
        // JoinSet aborts the workers if the runtime drops the runner.
    }
}

/// An asynchronous client for the Max (OneMe) WebSocket API.
///
/// Clones are cheap and share the same connection.
#[derive(Clone)]
pub struct MaxClient {
    inner: Arc<InnerClient>,
}

// Wake senders immediately when either the caller or supervisor exits.
struct RunGuard(MaxClient);

impl Drop for RunGuard {
    fn drop(&mut self) {
        self.0.inner.recovery.stop();
    }
}

impl MaxClient {
    /// Creates a disconnected client handle.
    ///
    /// Call [`MaxClient::run`] to connect and dispatch messages with automatic recovery.
    pub fn new(config: LoginConfig) -> Result<Self> {
        config.validate()?;
        let user_agent = UserAgent::default();
        let header_user_agent = user_agent.header_user_agent.clone();
        let http = reqwest::Client::builder()
            .user_agent(header_user_agent)
            .build()?;
        let inner = Arc::new(InnerClient {
            login_config: config,
            run_lock: Arc::new(Mutex::new(())),
            recovery: recovery::Recovery::new(),
            handler_shutdown: CancellationToken::new(),
            cid: Mutex::new(-chrono_millis()),
            device_id: uuid::Uuid::new_v4().to_string(),
            user_agent,
            http,
            #[cfg(test)]
            ws_url: std::sync::Mutex::new(None),
        });

        Ok(MaxClient { inner })
    }

    async fn run_attempt<H: ChatHandler>(
        &self,
        attempt: &mut Attempt,
        config: &mut LoginConfig,
        handler: Arc<H>,
    ) -> Result<()> {
        let connection = Arc::clone(&attempt.connection);
        let url = transport::WS_URL.to_string();
        #[cfg(test)]
        let url = self.inner.ws_url.lock().unwrap().clone().unwrap_or(url);
        let read = connection
            .transport
            .connect(&url, &self.inner.user_agent.header_user_agent)
            .await?;
        let (messages, incoming) = mpsc::unbounded_channel();
        attempt.tasks.spawn(read_loop::read_loop(
            read,
            messages,
            Arc::clone(&connection),
        ));
        connection
            .invoke(
                opcode::SESSION_INIT,
                session_init_payload(&self.inner.user_agent, &self.inner.device_id),
            )
            .await?;
        attempt.tasks.spawn(keepalive(Arc::clone(&connection)));
        let token = Connection::login(Arc::clone(&connection), config.clone()).await?;
        config.session_token = Some(token);
        self.inner.recovery.connected(&connection)?;
        tracing::info!(device_id = %self.inner.device_id, "Max connection established");
        attempt.connected_at = Some(tokio::time::Instant::now());
        dispatcher::run(self.inner.handler_shutdown.clone(), handler, incoming).await;
        Ok(())
    }

    /// Connects and dispatches, recovering transient failures until disconnected.
    ///
    /// Only one runner may use a client. Cancelling this future requests shutdown;
    /// its supervisor finishes teardown and wakes pending sends. Message handlers
    /// may overlap, including for the same chat.
    pub async fn run<H: ChatHandler>(&self, handler: H) -> Result<()> {
        let lock = Arc::clone(&self.inner.run_lock)
            .try_lock_owned()
            .map_err(|_| Error::ClientAlreadyRunning)?;
        if self.inner.recovery.shutdown.is_cancelled() {
            return Err(Error::ConnectionClosed);
        }
        let _cancel = RunGuard(self.clone());
        let client = self.clone();
        tokio::spawn(async move {
            let _lock = lock;
            let _handlers = client.inner.handler_shutdown.clone().drop_guard();
            let _guard = RunGuard(client.clone());
            client.run_loop(Arc::new(handler)).await
        })
        .await
        .map_err(|err| Error::UnexpectedResponse(format!("Max runner failed: {err}")))?
    }

    async fn run_loop<H: ChatHandler>(&self, handler: Arc<H>) -> Result<()> {
        let mut config = self.inner.login_config.clone();
        let mut backoff = recovery::Backoff::default();
        loop {
            if self.inner.recovery.shutdown.is_cancelled() {
                return Ok(());
            }
            let Ok(connection) = self.inner.recovery.begin_attempt() else {
                return Ok(());
            };
            let mut attempt = Attempt::new(connection);
            let connection = Arc::clone(&attempt.connection);
            // Covers socket setup, SMS/captcha waits, and established dispatch.
            let result = tokio::select! {
                biased;
                _ = connection.cancelled() => Err(Error::ConnectionClosed),
                result = self.run_attempt(&mut attempt, &mut config, Arc::clone(&handler)) => result,
            };
            self.inner.recovery.offline();
            if let Some(connected_at) = attempt.connected_at {
                backoff.reset_if_healthy(connected_at.elapsed());
            }
            attempt.close().await;
            if self.inner.recovery.shutdown.is_cancelled() {
                return Ok(());
            }
            match result {
                Ok(()) => {}
                Err(error) if recovery::should_retry_connection(&error) => {
                    tracing::warn!(%error, "Max connection attempt failed");
                }
                Err(error) => return Err(error),
            }
            let delay = backoff.next_delay();
            tracing::warn!(?delay, "Max reconnect backoff");
            tokio::select! {
                biased;
                _ = self.inner.recovery.shutdown.cancelled() => return Ok(()),
                () = tokio::time::sleep(delay) => {}
            }
        }
    }

    #[cfg(test)]
    fn set_test_url(&self, url: String) {
        *self.inner.ws_url.lock().unwrap() = Some(url);
    }

    /// Sends a text message to `chat_id`.
    ///
    /// Waits for a connection and replays after reconnection until acknowledged
    /// or the returned future is cancelled. A lost acknowledgement may still
    /// cause duplicate delivery.
    pub async fn send_text(&self, chat_id: i64, message: MaxMessage) -> Result<()> {
        let payload = text_message_payload(chat_id, &message, self.inner.next_cid().await);
        loop {
            let connection = self.inner.recovery.current().await?;
            match connection.invoke(opcode::MSG_SEND, payload.clone()).await {
                Ok(_) => return Ok(()),
                Err(error) if recovery::is_transport_failure(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }

    /// Uploads a file from an in-memory byte buffer and sends it to `chat_id`.
    ///
    /// The `file_name` is sent in the HTTP `Content-Disposition` header.
    /// Reconnection releases the old attachment waiter and replays the upload
    /// while preserving the message client ID used for deduplication.
    /// Cancelling the returned future prevents further replay attempts.
    pub async fn send_file_bytes<'a>(
        &self,
        chat_id: i64,
        file_name: impl Into<String>,
        bytes: impl Into<Cow<'a, [u8]>>,
        caption: &str,
    ) -> Result<()> {
        let file_name = normalized_file_name(file_name.into());
        let bytes = bytes.into().into_owned();
        let cid = self.inner.next_cid().await;
        loop {
            let connection = self.inner.recovery.current().await?;
            match self
                .send_uploaded_file(&connection, chat_id, &file_name, &bytes, caption, cid)
                .await
            {
                Ok(()) => return Ok(()),
                Err(error) if recovery::is_transport_failure(&error) => {}
                Err(error) => return Err(error),
            }
        }
    }

    async fn send_uploaded_file(
        &self,
        connection: &Arc<recovery::Connection>,
        chat_id: i64,
        file_name: &str,
        bytes: &[u8],
        caption: &str,
        cid: i64,
    ) -> Result<()> {
        let response = connection
            .invoke(opcode::FILE_UPLOAD, file_upload_payload())
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

        // Register before uploading so an immediate NOTIF_ATTACH is not missed.
        let attachment_waiter = connection
            .attachment_registry
            .register(file_id)
            .ok_or_else(|| {
                Error::UnexpectedResponse(format!(
                    "duplicate active fileId in upload response: {file_id}"
                ))
            })?;

        let size = bytes.len();
        let upload = self
            .inner
            .http
            .post(&url)
            .header(
                "Content-Disposition",
                format!(
                    "attachment; filename={}",
                    percent_encode_file_name(file_name)
                ),
            )
            .header("Content-Length", size.to_string())
            .header(
                "Content-Range",
                format!("0-{}/{}", size.saturating_sub(1), size),
            )
            .body(bytes.to_vec())
            .send();
        let result = tokio::select! {
            biased;
            _ = connection.cancelled() => return Err(Error::ConnectionClosed),
            result = upload => result,
        };

        if let Err(err) = result {
            return Err(err.into());
        }

        let processed = tokio::select! {
            biased;
            _ = connection.cancelled() => return Err(Error::ConnectionClosed),
            result = tokio::time::timeout(FILE_PROCESS_TIMEOUT, attachment_waiter.wait()) => result,
        };
        match processed {
            Ok(()) => {}
            Err(_) => {
                return Err(Error::FileProcessingTimeout(file_id));
            }
        }

        let payload = file_message_payload(chat_id, caption, file_id, cid);
        connection.invoke(opcode::MSG_SEND, payload).await?;
        Ok(())
    }

    /// Stops recovery, wakes pending sends, and aborts dispatch and handlers.
    /// Shutdown is terminal for this handle and all its clones.
    pub async fn disconnect(&self) {
        self.inner.recovery.stop();
        // The supervisor owns teardown; callers only request it and wait.
        let _lock = self.inner.run_lock.lock().await;
        self.inner.handler_shutdown.cancel();
    }
}

async fn keepalive(connection: Arc<Connection>) {
    let _failure = connection.cancellation.clone().drop_guard();
    loop {
        tokio::time::sleep(KEEPALIVE_INTERVAL).await;
        if let Err(err) = connection
            .invoke(opcode::PING, json!({ "interactive": false }))
            .await
        {
            tracing::warn!(%err, "Max keepalive failed");
            return;
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
            "elements": [],
            "attaches": [{ "_type": "FILE", "fileId": file_id }],
        },
        "notify": true,
    })
}

fn file_upload_payload() -> Value {
    json!({
        "count": 1,
        "type": 0,
        "uploaderType": 0,
        "profile": false,
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
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
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
            session_token: Some("test-token".into()),
            captcha: crate::auth::AuthCaptchaConfig {
                solver_url: None,
                callback_bind: "127.0.0.1:0".into(),
                callback_url_base: None,
            },
            operator: crate::auth::operator_channels::OperatorChannel::None,
        }
    }

    #[test]
    fn client_creation_validates_login_configuration() {
        let mut config = test_config();
        config.session_token = None;
        assert!(matches!(
            MaxClient::new(config.clone()),
            Err(Error::MissingCredentials)
        ));

        config.phone = Some("+79990000000".into());
        assert!(matches!(
            MaxClient::new(config.clone()),
            Err(Error::NoOperatorChannel)
        ));

        config.operator = crate::auth::operator_channels::OperatorChannel::Cli;
        assert!(MaxClient::new(config).is_ok());
    }

    #[test]
    fn dropping_run_guard_stops_recovery_without_a_runtime() {
        let client = MaxClient::new(test_config()).expect("client");
        let connection = client.inner.recovery.begin_attempt().unwrap();
        client.inner.recovery.connected(&connection).unwrap();

        drop(RunGuard(client.clone()));

        assert!(!client.inner.recovery.is_connected());
        assert!(client.inner.recovery.shutdown.is_cancelled());
        assert!(connection.is_cancelled());
    }

    #[test]
    fn text_message_payload_matches_web_schema() {
        let payload =
            text_message_payload(295438091, &MaxMessage::new("hello"), -1_700_000_000_001);

        assert_eq!(payload["chatId"], 295438091);
        assert_eq!(payload["message"]["text"], "hello");
        assert_eq!(payload["message"]["cid"], -1_700_000_000_001i64);
        assert!(payload["message"].get("type").is_none());
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
        assert!(payload["message"].get("type").is_none());
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
    fn percent_encodes_file_names() {
        for (name, expected) in [
            ("report-2026_07.02~final.txt", "report-2026_07.02~final.txt"),
            ("reports/report.txt", "reports%2Freport.txt"),
            (
                "привет мир.txt",
                "%D0%BF%D1%80%D0%B8%D0%B2%D0%B5%D1%82%20%D0%BC%D0%B8%D1%80.txt",
            ),
        ] {
            assert_eq!(percent_encode_file_name(name), expected);
        }
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
        assert_eq!(payload["userAgent"]["appVersion"], "26.8.4");
        assert_eq!(payload["userAgent"]["locale"], "ru");
        assert_eq!(payload["userAgent"]["deviceLocale"], "ru");
        assert_eq!(payload["userAgent"]["osVersion"], "Linux");
        assert_eq!(payload["userAgent"]["deviceName"], "Chrome");
    }

    #[test]
    fn file_upload_payload_matches_current_web_schema() {
        assert_eq!(
            file_upload_payload(),
            json!({
                "count": 1,
                "type": 0,
                "uploaderType": 0,
                "profile": false,
            })
        );
    }
}
