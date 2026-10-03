//! The asynchronous Max client.

mod dispatcher;
mod read_loop;
mod recovery;
mod transport;

#[cfg(test)]
mod recovery_tests;

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio_util::sync::CancellationToken;

use crate::auth::LoginConfig;
use crate::error::{Error, Result};
use crate::models::{IncomingMessage, MaxMessage, UserAgent};
use crate::protocol::{opcode, Packet};

use self::transport::Transport;

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

struct AttachmentWaiter {
    owner: Arc<()>,
    sender: oneshot::Sender<()>,
}

struct ClientState {
    cid: i64,
    own_user_id: Option<i64>,
    keepalive_task: Option<tokio::task::JoinHandle<()>>,
}

pub(crate) struct InnerClient {
    transport: Transport,
    file_waiters: Mutex<HashMap<i64, AttachmentWaiter>>,
    login_config: Mutex<LoginConfig>,
    connect_lock: Mutex<()>,
    run_lock: Mutex<()>,
    recovery: recovery::Recovery,
    handler_shutdown: CancellationToken,
    msg_tx: Mutex<Option<mpsc::UnboundedSender<IncomingMessage>>>,
    state: Mutex<ClientState>,
    device_id: String,
    user_agent: UserAgent,
    http: reqwest::Client,
}

impl InnerClient {
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
        self.transport.invoke(opcode, payload).await
    }

    async fn session_init(&self) -> Result<()> {
        let payload = session_init_payload(&self.user_agent, &self.device_id);
        self.invoke(opcode::SESSION_INIT, payload).await.map(|_| ())
    }

    async fn close_connection(&self, current: Option<&Arc<recovery::Connection>>) {
        if !self.recovery.disconnect(current) {
            return;
        }

        self.msg_tx.lock().await.take();
        self.file_waiters.lock().await.clear();
        self.transport.close().await;
        if let Some(task) = self.state.lock().await.keepalive_task.take() {
            task.abort();
        }
    }

    async fn store_keepalive(&self, task: tokio::task::JoinHandle<()>) {
        if let Some(previous) = self.state.lock().await.keepalive_task.replace(task) {
            previous.abort();
        }
    }
}

/// An asynchronous client for the Max (OneMe) WebSocket API.
///
/// Clones are cheap and share the same connection.
#[derive(Clone)]
pub struct MaxClient {
    inner: Arc<InnerClient>,
}

// Cancelling the recovery future must wake pending sends and clean up the socket.
struct RunGuard(Option<MaxClient>);

// A cancelled upload must release its attachment-notification subscription.
struct FileWaiter {
    inner: Arc<InnerClient>,
    owner: Arc<()>,
    file_id: i64,
}

impl Drop for FileWaiter {
    fn drop(&mut self) {
        if let Ok(mut waiters) = self.inner.file_waiters.try_lock() {
            remove_owned_waiter(&mut waiters, self.file_id, &self.owner);
            return;
        }
        let inner = Arc::clone(&self.inner);
        let owner = Arc::clone(&self.owner);
        let file_id = self.file_id;
        tokio::spawn(async move {
            let mut waiters = inner.file_waiters.lock().await;
            remove_owned_waiter(&mut waiters, file_id, &owner);
        });
    }
}

fn remove_owned_waiter(
    waiters: &mut HashMap<i64, AttachmentWaiter>,
    file_id: i64,
    owner: &Arc<()>,
) {
    if waiters
        .get(&file_id)
        .is_some_and(|waiter| Arc::ptr_eq(&waiter.owner, owner))
    {
        waiters.remove(&file_id);
    }
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        let client = if let Some(client) = self.0.take() {
            client
        } else {
            return;
        };
        // Make cancellation observable immediately. Socket cleanup remains
        // asynchronous, but must not panic if the future is dropped after
        // its runtime has already shut down.
        client.inner.recovery.stop();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                client.disconnect().await;
            });
        }
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
            transport: Transport::new(),
            file_waiters: Mutex::new(HashMap::new()),
            login_config: Mutex::new(config.clone()),
            connect_lock: Mutex::new(()),
            run_lock: Mutex::new(()),
            recovery: recovery::Recovery::new(),
            handler_shutdown: CancellationToken::new(),
            msg_tx: Mutex::new(None),
            state: Mutex::new(ClientState {
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

    async fn connect_once(&self) -> Result<mpsc::UnboundedReceiver<IncomingMessage>> {
        let _guard = self.inner.connect_lock.lock().await;
        if self.inner.recovery.shutdown.is_cancelled() {
            return Err(Error::ConnectionClosed);
        }
        let has_session_token = self.inner.login_config.lock().await.session_token.is_some();
        tracing::info!(
            device_id = %self.inner.device_id,
            has_session_token,
            "Starting Max connection"
        );
        self.inner.close_connection(None).await;
        let connection = self.inner.recovery.begin_attempt()?;
        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        *self.inner.msg_tx.lock().await = Some(msg_tx);

        if let Err(err) = self
            .inner
            .transport
            .connect(
                &self.inner,
                Arc::clone(&connection),
                &self.inner.user_agent.header_user_agent,
            )
            .await
        {
            tracing::warn!(
                device_id = %self.inner.device_id,
                stage = "websocket",
                %err,
                "Max connection failed"
            );
            self.inner.close_connection(Some(&connection)).await;
            return Err(err);
        }

        if let Err(err) = self.inner.session_init().await {
            tracing::warn!(
                device_id = %self.inner.device_id,
                stage = "session_init",
                %err,
                "Max connection failed"
            );
            self.inner.close_connection(Some(&connection)).await;
            return Err(err);
        }

        self.spawn_keepalive(Arc::clone(&connection)).await;

        let login_config = self.inner.login_config.lock().await.clone();
        let session = match InnerClient::login(Arc::clone(&self.inner), login_config.clone()).await
        {
            Ok(session) => session,
            Err(err) => {
                tracing::warn!(
                    device_id = %self.inner.device_id,
                    stage = "login",
                    %err,
                    "Max connection failed"
                );
                self.inner.close_connection(Some(&connection)).await;
                return Err(err);
            }
        };
        let mut stored_config = login_config;
        stored_config.session_token = Some(session.token.clone());
        *self.inner.login_config.lock().await = stored_config;
        if !self.inner.transport.is_connected().await {
            return Err(Error::ConnectionClosed);
        }
        self.inner.recovery.connected(&connection)?;

        tracing::info!(device_id = %self.inner.device_id, "Max connection established");
        Ok(msg_rx)
    }

    /// Connects and dispatches, recovering transient failures until disconnected.
    ///
    /// Only one runner may use a client. Cancelling this future stops recovery and
    /// wakes pending sends. Message handlers may overlap, including for the same chat.
    pub async fn run<H: ChatHandler>(&self, handler: H) -> Result<()> {
        let _lock = self
            .inner
            .run_lock
            .try_lock()
            .map_err(|_| Error::ClientAlreadyRunning)?;
        let mut guard = RunGuard(Some(self.clone()));
        let handler = Arc::new(handler);
        let mut backoff = recovery::Backoff::default();
        loop {
            let attempt = tokio::select! {
                biased;
                _ = self.inner.recovery.shutdown.cancelled() => break,
                attempt = self.connect_once() => attempt,
            };
            let delay = match attempt {
                Ok(incoming) => {
                    let connected_at = tokio::time::Instant::now();
                    dispatcher::run(
                        self.inner.handler_shutdown.clone(),
                        Arc::clone(&handler),
                        incoming,
                    )
                    .await;
                    if self.inner.recovery.shutdown.is_cancelled() {
                        break;
                    }
                    backoff.reset_if_healthy(connected_at.elapsed());
                    Some(backoff.next_delay())
                }
                Err(error) => {
                    if !recovery::should_retry_connection(&error) {
                        return Err(error);
                    }
                    tracing::warn!(%error, "Max connection attempt failed");
                    Some(backoff.next_delay())
                }
            };
            if let Some(delay) = delay {
                tracing::warn!(?delay, "Max reconnect backoff");
                tokio::select! {
                    biased;
                    _ = self.inner.recovery.shutdown.cancelled() => break,
                    () = tokio::time::sleep(delay) => {}
                }
            }
        }
        self.disconnect().await;
        guard.0.take();
        Ok(())
    }

    #[cfg(test)]
    fn set_test_url(&self, url: String) {
        self.inner.transport.set_test_url(url);
    }

    async fn spawn_keepalive(&self, connection: Arc<recovery::Connection>) {
        let inner = Arc::clone(&self.inner);
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = connection.cancelled() => break,
                    () = tokio::time::sleep(KEEPALIVE_INTERVAL) => {}
                }
                let result = tokio::select! {
                    biased;
                    _ = connection.cancelled() => break,
                    result = inner.invoke(opcode::PING, json!({ "interactive": false })) => result,
                };
                if let Err(err) = result {
                    tracing::warn!(%err, "Max keepalive failed");
                    inner.close_connection(Some(&connection)).await;
                    break;
                }
            }
        });
        self.inner.store_keepalive(task).await;
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
            match self
                .invoke(&connection, opcode::MSG_SEND, payload.clone())
                .await
            {
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
        let response = self
            .invoke(connection, opcode::FILE_UPLOAD, file_upload_payload())
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
        let owner = Arc::new(());
        self.inner.file_waiters.lock().await.insert(
            file_id,
            AttachmentWaiter {
                owner: Arc::clone(&owner),
                sender: tx,
            },
        );
        let _waiter = FileWaiter {
            inner: Arc::clone(&self.inner),
            owner,
            file_id,
        };

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
            result = tokio::time::timeout(FILE_PROCESS_TIMEOUT, rx) => result,
        };
        match processed {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return Err(Error::ConnectionClosed);
            }
            Err(_) => {
                return Err(Error::FileProcessingTimeout(file_id));
            }
        }

        let payload = file_message_payload(chat_id, caption, file_id, cid);
        self.invoke(connection, opcode::MSG_SEND, payload).await?;
        Ok(())
    }

    /// Stops recovery, wakes pending sends, and aborts dispatch and handlers.
    /// Shutdown is terminal for this handle and all its clones.
    pub async fn disconnect(&self) {
        self.inner.recovery.stop();
        let _lock = self.inner.connect_lock.lock().await;
        self.inner.close_connection(None).await;
        self.inner.handler_shutdown.cancel();
    }

    async fn invoke(
        &self,
        connection: &Arc<recovery::Connection>,
        opcode: u16,
        payload: Value,
    ) -> Result<Packet> {
        let result = tokio::select! {
            biased;
            _ = connection.cancelled() => return Err(Error::ConnectionClosed),
            result = self.inner.invoke(opcode, payload) => result,
        };
        match result {
            Ok(response) => Ok(response),
            Err(err) => {
                // A server rejection doesn't mean the socket is dead; keep it
                // open and only disconnect on transport failures.
                if recovery::is_transport_failure(&err) {
                    self.inner.close_connection(Some(connection)).await;
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

    #[tokio::test]
    async fn disconnect_closes_internal_message_feed() {
        let client = MaxClient::new(test_config()).expect("client");
        let (tx, mut messages) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(tx);

        client.inner.close_connection(None).await;
        assert!(messages.recv().await.is_none());
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

        drop(RunGuard(Some(client.clone())));

        assert!(!client.inner.recovery.is_connected());
        assert!(client.inner.recovery.shutdown.is_cancelled());
        assert!(connection.is_cancelled());
    }

    #[tokio::test]
    async fn keepalive_failure_finishes_cleanup_before_self_abort() {
        let client = MaxClient::new(test_config()).expect("client");
        let (waiter_tx, _waiter_rx) = oneshot::channel();
        let mut waiters = client.inner.file_waiters.lock().await;
        waiters.insert(
            1,
            AttachmentWaiter {
                owner: Arc::new(()),
                sender: waiter_tx,
            },
        );
        let connection = client.inner.recovery.begin_attempt().unwrap();
        client.inner.recovery.connected(&connection).unwrap();

        let inner = Arc::clone(&client.inner);
        let (started_tx, started_rx) = oneshot::channel();
        let (run_tx, run_rx) = oneshot::channel();
        let (finished_tx, finished_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            started_tx.send(()).unwrap();
            run_rx.await.unwrap();
            inner.close_connection(Some(&connection)).await;
            finished_tx.send(()).unwrap();
        });
        client.inner.state.lock().await.keepalive_task = Some(task);

        started_rx.await.expect("keepalive task must start");
        run_tx.send(()).unwrap();
        tokio::task::yield_now().await;
        drop(waiters);

        finished_rx
            .await
            .expect("self-abort must happen only after cleanup completes");
        assert!(client.inner.file_waiters.lock().await.is_empty());
        assert!(client.inner.state.lock().await.keepalive_task.is_none());
    }

    #[tokio::test]
    async fn message_channel_can_be_recreated_after_failure() {
        let client = MaxClient::new(test_config()).expect("client");
        let connection = client.inner.recovery.begin_attempt().unwrap();
        client.inner.recovery.connected(&connection).unwrap();
        let (old_tx, mut old_messages) = mpsc::unbounded_channel();
        *client.inner.msg_tx.lock().await = Some(old_tx);

        client.inner.close_connection(Some(&connection)).await;
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
