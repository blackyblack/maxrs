use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{oneshot, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use crate::error::{Error, Result};
use crate::protocol::{Packet, CMD_ERROR};

use super::read_loop::read_loop;
use super::InnerClient;

const WS_URL: &str = "wss://ws-api.oneme.ru/websocket";
const ORIGIN: &str = "https://web.max.ru";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(20);

pub(super) type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;

pub(super) struct Transport {
    request_lock: Mutex<()>,
    state: Mutex<TransportState>,
}

struct TransportState {
    sink: Option<WsSink>,
    pending_response: Option<oneshot::Sender<Packet>>,
    read_task: Option<tokio::task::JoinHandle<()>>,
}

impl Transport {
    pub(super) fn new() -> Self {
        Self {
            request_lock: Mutex::new(()),
            state: Mutex::new(TransportState {
                sink: None,
                pending_response: None,
                read_task: None,
            }),
        }
    }

    pub(super) async fn connect(
        &self,
        owner: &Arc<InnerClient>,
        header_user_agent: &str,
    ) -> Result<()> {
        let _guard = self.request_lock.lock().await;
        self.close().await;
        let mut request = WS_URL.into_client_request()?;
        {
            let headers = request.headers_mut();
            headers.insert("Origin", HeaderValue::from_static(ORIGIN));
            headers.insert("User-Agent", HeaderValue::from_str(header_user_agent)?);
        }

        let (stream, _response) = connect_async(request).await?;
        let (sink, read) = stream.split();

        {
            let mut state = self.state.lock().await;
            state.sink = Some(sink);
        }
        let task = tokio::spawn(read_loop(read, Arc::clone(owner)));
        self.state.lock().await.read_task = Some(task);
        Ok(())
    }

    pub(super) async fn send(&self, packet: &Packet) -> Result<()> {
        let text = serde_json::to_string(packet)?;
        let mut state = self.state.lock().await;
        let sink = state.sink.as_mut().ok_or(Error::ConnectionClosed)?;
        match tokio::time::timeout(DEFAULT_TIMEOUT, sink.send(Message::text(text))).await {
            Ok(result) => result?,
            Err(_) => return Err(Error::Timeout(packet.opcode)),
        }
        Ok(())
    }

    pub(super) async fn invoke(&self, seq: u32, opcode: u16, payload: Value) -> Result<Packet> {
        let _guard = self.request_lock.lock().await;
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            state.pending_response = Some(tx);
        }

        let packet = Packet::request(seq, opcode, payload);
        if let Err(err) = self.send(&packet).await {
            self.close().await;
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
            Ok(Err(_)) => {
                self.close().await;
                Err(Error::ConnectionClosed)
            }
            Err(_) => {
                self.close().await;
                Err(Error::Timeout(opcode))
            }
        }
    }

    pub(super) async fn receive_response(&self, packet: Packet) {
        if let Some(tx) = self.state.lock().await.pending_response.take() {
            let _ = tx.send(packet);
        }
    }

    pub(super) async fn is_connected(&self) -> bool {
        let state = self.state.lock().await;
        state.sink.is_some()
            && state
                .read_task
                .as_ref()
                .is_some_and(|task| !task.is_finished())
    }

    pub(super) async fn close(&self) {
        let mut state = self.state.lock().await;
        if let Some(task) = state.read_task.take() {
            task.abort();
        }
        state.sink = None;
        state.pending_response = None;
    }
}

fn error_message(payload: &Value) -> String {
    payload["error"]
        .as_str()
        .or_else(|| payload["message"].as_str())
        .or_else(|| payload["localizedMessage"].as_str())
        .unwrap_or("unknown error")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extracts_error_message() {
        assert_eq!(error_message(&json!({ "error": "boom" })), "boom");
        assert_eq!(error_message(&json!({ "message": "m" })), "m");
        assert_eq!(error_message(&json!({})), "unknown error");
    }
}
