use std::collections::HashMap;
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
    send_order: Mutex<()>,
    sink: Mutex<Option<WsSink>>,
    state: Mutex<TransportState>,
}

struct TransportState {
    next_seq: u32,
    pending: HashMap<u32, oneshot::Sender<Packet>>,
    read_task: Option<tokio::task::JoinHandle<()>>,
}

impl Transport {
    pub(super) fn new() -> Self {
        Self {
            send_order: Mutex::new(()),
            sink: Mutex::new(None),
            state: Mutex::new(TransportState {
                next_seq: 1,
                pending: HashMap::new(),
                read_task: None,
            }),
        }
    }

    pub(super) async fn connect(
        &self,
        owner: &Arc<InnerClient>,
        header_user_agent: &str,
    ) -> Result<()> {
        self.close().await;
        let mut request = WS_URL.into_client_request()?;
        {
            let headers = request.headers_mut();
            headers.insert("Origin", HeaderValue::from_static(ORIGIN));
            headers.insert("User-Agent", HeaderValue::from_str(header_user_agent)?);
        }

        let (stream, _response) = connect_async(request).await?;
        let (sink, read) = stream.split();

        *self.sink.lock().await = Some(sink);
        let task = tokio::spawn(read_loop(read, Arc::clone(owner)));
        self.state.lock().await.read_task = Some(task);
        Ok(())
    }

    pub(super) async fn send(&self, packet: &Packet) -> Result<()> {
        let text = serde_json::to_string(packet)?;
        let mut sink = self.sink.lock().await;
        let sink = sink.as_mut().ok_or(Error::ConnectionClosed)?;
        match tokio::time::timeout(DEFAULT_TIMEOUT, sink.send(Message::text(text))).await {
            Ok(result) => result?,
            Err(_) => return Err(Error::Timeout(packet.opcode)),
        }
        Ok(())
    }

    pub(super) async fn invoke(&self, opcode: u16, payload: Value) -> Result<Packet> {
        let send_guard = self.send_order.lock().await;
        let seq = {
            let mut state = self.state.lock().await;
            let seq = state.next_seq;
            state.next_seq = state.next_seq.wrapping_add(1);
            seq
        };
        let rx = self.register_and_send(seq, opcode, payload).await?;
        drop(send_guard);
        self.await_response(seq, opcode, rx, DEFAULT_TIMEOUT).await
    }

    #[cfg(test)]
    async fn invoke_with_seq_and_timeout(
        &self,
        seq: u32,
        opcode: u16,
        payload: Value,
        timeout: Duration,
    ) -> Result<Packet> {
        let send_guard = self.send_order.lock().await;
        let rx = self.register_and_send(seq, opcode, payload).await?;
        drop(send_guard);
        self.await_response(seq, opcode, rx, timeout).await
    }

    async fn register_and_send(
        &self,
        seq: u32,
        opcode: u16,
        payload: Value,
    ) -> Result<oneshot::Receiver<Packet>> {
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.state.lock().await;
            state.pending.retain(|_, waiter| !waiter.is_closed());
            if state.pending.contains_key(&seq) {
                return Err(Error::DuplicateSequence(seq));
            }
            state.pending.insert(seq, tx);
        }

        let packet = Packet::request(seq, opcode, payload);
        if let Err(err) = self.send(&packet).await {
            self.state.lock().await.pending.remove(&seq);
            return Err(err);
        }
        Ok(rx)
    }

    async fn await_response(
        &self,
        seq: u32,
        opcode: u16,
        rx: oneshot::Receiver<Packet>,
        timeout: Duration,
    ) -> Result<Packet> {
        match tokio::time::timeout(timeout, rx).await {
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
                self.state.lock().await.pending.remove(&seq);
                Err(Error::ConnectionClosed)
            }
            Err(_) => {
                self.state.lock().await.pending.remove(&seq);
                Err(Error::Timeout(opcode))
            }
        }
    }

    pub(super) async fn receive_response(&self, packet: Packet) {
        let seq = packet.seq;
        if let Some(tx) = self.state.lock().await.pending.remove(&seq) {
            let _ = tx.send(packet);
        } else {
            tracing::warn!(seq, "dropping response with unknown sequence number");
        }
    }

    pub(super) async fn is_connected(&self) -> bool {
        let has_sink = self.sink.lock().await.is_some();
        let state = self.state.lock().await;
        has_sink
            && state
                .read_task
                .as_ref()
                .is_some_and(|task| !task.is_finished())
    }

    pub(super) async fn close(&self) {
        {
            let mut state = self.state.lock().await;
            if let Some(task) = state.read_task.take() {
                task.abort();
            }
            state.pending.clear();
        }
        self.sink.lock().await.take();
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
    use futures_util::StreamExt;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio_tungstenite::tungstenite::protocol::Role;

    type ServerStream = WebSocketStream<TcpStream>;

    async fn connected_transport() -> (Arc<Transport>, ServerStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (client, accepted) = tokio::join!(TcpStream::connect(address), listener.accept());
        let client = client.unwrap();
        let (server, _) = accepted.unwrap();

        let client =
            WebSocketStream::from_raw_socket(MaybeTlsStream::Plain(client), Role::Client, None)
                .await;
        let server = WebSocketStream::from_raw_socket(server, Role::Server, None).await;
        let (sink, _) = client.split();
        let transport = Arc::new(Transport::new());
        *transport.sink.lock().await = Some(sink);
        (transport, server)
    }

    async fn next_request(server: &mut ServerStream) -> Packet {
        let message = tokio::time::timeout(Duration::from_secs(1), server.next())
            .await
            .expect("request timed out")
            .expect("stream closed")
            .expect("websocket error");
        serde_json::from_str(message.to_text().unwrap()).unwrap()
    }

    #[test]
    fn extracts_error_message() {
        assert_eq!(error_message(&json!({ "error": "boom" })), "boom");
        assert_eq!(error_message(&json!({ "message": "m" })), "m");
        assert_eq!(error_message(&json!({})), "unknown error");
    }

    #[tokio::test]
    async fn concurrent_invokes_match_out_of_order_responses_by_sequence() {
        let (transport, mut server) = connected_transport().await;
        let first = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(
                        10,
                        100,
                        json!({ "request": "first" }),
                        DEFAULT_TIMEOUT,
                    )
                    .await
            }
        });
        let second = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(
                        20,
                        200,
                        json!({ "request": "second" }),
                        DEFAULT_TIMEOUT,
                    )
                    .await
            }
        });

        let sent_a = next_request(&mut server).await;
        let sent_b = next_request(&mut server).await;
        assert_eq!([sent_a.seq, sent_b.seq].into_iter().sum::<u32>(), 30);

        transport
            .receive_response(Packet::response(20, 200, json!({ "reply": "second" })))
            .await;
        transport
            .receive_response(Packet::response(10, 100, json!({ "reply": "first" })))
            .await;

        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
        assert_eq!(first.seq, 10);
        assert_eq!(first.payload["reply"], "first");
        assert_eq!(second.seq, 20);
        assert_eq!(second.payload["reply"], "second");
    }

    #[tokio::test]
    async fn concurrent_invokes_are_emitted_in_sequence_order() {
        let (transport, mut server) = connected_transport().await;
        let mut invokes = Vec::new();
        for request in 0..4 {
            invokes.push(tokio::spawn({
                let transport = Arc::clone(&transport);
                async move { transport.invoke(100, json!({ "request": request })).await }
            }));
        }

        let mut requests = Vec::new();
        for _ in 0..4 {
            requests.push(next_request(&mut server).await);
        }
        assert_eq!(
            requests.iter().map(|packet| packet.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );

        for packet in requests.into_iter().rev() {
            transport
                .receive_response(Packet::response(packet.seq, packet.opcode, packet.payload))
                .await;
        }
        for invoke in invokes {
            invoke.await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn duplicate_in_flight_sequence_is_rejected_without_replacing_waiter() {
        let (transport, mut server) = connected_transport().await;
        let original = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(7, 70, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        next_request(&mut server).await;

        let duplicate = transport
            .invoke_with_seq_and_timeout(7, 71, Value::Null, DEFAULT_TIMEOUT)
            .await;
        assert!(matches!(duplicate, Err(Error::DuplicateSequence(7))));
        assert_eq!(transport.state.lock().await.pending.len(), 1);

        transport
            .receive_response(Packet::response(7, 70, json!({ "original": true })))
            .await;
        assert_eq!(original.await.unwrap().unwrap().payload["original"], true);
    }

    #[tokio::test]
    async fn registering_request_prunes_cancelled_waiters() {
        let (transport, mut server) = connected_transport().await;
        let cancelled = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(7, 70, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        next_request(&mut server).await;
        cancelled.abort();
        assert!(cancelled.await.unwrap_err().is_cancelled());
        assert!(transport.state.lock().await.pending.contains_key(&7));

        let replacement = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(7, 71, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        let request = next_request(&mut server).await;
        assert_eq!(request.seq, 7);
        assert_eq!(request.opcode, 71);
        assert_eq!(transport.state.lock().await.pending.len(), 1);

        transport
            .receive_response(Packet::response(7, 71, json!({ "replacement": true })))
            .await;
        assert_eq!(
            replacement.await.unwrap().unwrap().payload["replacement"],
            true
        );
    }

    #[tokio::test]
    async fn unknown_sequence_response_is_ignored() {
        let (transport, mut server) = connected_transport().await;
        let invoke = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(7, 70, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        next_request(&mut server).await;

        transport
            .receive_response(Packet::response(999, 70, json!({ "wrong": true })))
            .await;
        assert!(!invoke.is_finished());

        transport
            .receive_response(Packet::response(7, 70, json!({ "right": true })))
            .await;
        assert_eq!(invoke.await.unwrap().unwrap().payload["right"], true);
    }

    #[tokio::test]
    async fn close_resolves_all_pending_invokes_as_connection_closed() {
        let (transport, mut server) = connected_transport().await;
        let first = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(1, 10, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        let second = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(2, 20, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        next_request(&mut server).await;
        next_request(&mut server).await;

        transport.close().await;

        assert!(matches!(first.await.unwrap(), Err(Error::ConnectionClosed)));
        assert!(matches!(
            second.await.unwrap(),
            Err(Error::ConnectionClosed)
        ));
    }

    #[tokio::test]
    async fn timeout_removes_only_its_waiter_and_late_response_is_dropped() {
        let (transport, mut server) = connected_transport().await;
        let timed_out = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(1, 10, Value::Null, Duration::from_millis(20))
                    .await
            }
        });
        let waiting = tokio::spawn({
            let transport = Arc::clone(&transport);
            async move {
                transport
                    .invoke_with_seq_and_timeout(2, 20, Value::Null, DEFAULT_TIMEOUT)
                    .await
            }
        });
        next_request(&mut server).await;
        next_request(&mut server).await;

        assert!(matches!(timed_out.await.unwrap(), Err(Error::Timeout(10))));
        assert_eq!(
            transport
                .state
                .lock()
                .await
                .pending
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![2]
        );

        transport
            .receive_response(Packet::response(1, 10, json!({ "late": true })))
            .await;
        assert!(!waiting.is_finished());
        transport
            .receive_response(Packet::response(2, 20, json!({ "ok": true })))
            .await;
        assert_eq!(waiting.await.unwrap().unwrap().payload["ok"], true);
    }
}
