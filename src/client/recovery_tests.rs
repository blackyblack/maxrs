use super::*;
use crate::auth::{operator_channels::OperatorChannel, AuthCaptchaConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

struct Handler;

impl ChatHandler for Handler {
    async fn on_message(&self, _message: IncomingMessage) -> Result<()> {
        Ok(())
    }
}

fn config() -> LoginConfig {
    LoginConfig {
        phone: None,
        password: None,
        session_token: Some("saved-token".into()),
        captcha: AuthCaptchaConfig::disabled(),
        operator: OperatorChannel::None,
    }
}

async fn local_client() -> (MaxClient, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = MaxClient::new(config()).unwrap();
    client.set_test_url(format!("ws://{}/", listener.local_addr().unwrap()));
    (client, listener)
}

async fn wait_for_connection(client: &MaxClient, expected: bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while client.inner.recovery.is_connected() != expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection state change");
}

async fn respond(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    packet: &Packet,
    payload: Value,
) {
    socket
        .send(Message::text(
            serde_json::to_string(&Packet::response(packet.seq, packet.opcode, payload)).unwrap(),
        ))
        .await
        .unwrap();
}

async fn authenticated(
    listener: &TcpListener,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let (socket, _) = listener.accept().await.unwrap();
    let mut socket = accept_async(socket).await.unwrap();
    loop {
        let packet = next_packet(&mut socket).await;
        if packet.opcode == opcode::LOGIN {
            assert_eq!(packet.payload["token"], "saved-token");
            respond(
                &mut socket,
                &packet,
                json!({"profile": {"contact": {"id": 42}}}),
            )
            .await;
            return socket;
        }
        respond(&mut socket, &packet, json!({})).await;
    }
}

async fn next_packet(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> Packet {
    loop {
        let message = socket.next().await.unwrap().unwrap();
        if message.is_text() {
            let packet: Packet = serde_json::from_str(message.to_text().unwrap()).unwrap();
            if packet.opcode == opcode::PING {
                respond(socket, &packet, json!({})).await;
                continue;
            }
            return packet;
        }
    }
}

#[tokio::test]
async fn run_reconnects_and_pending_send_uses_same_handle_and_saved_token() {
    let (client, listener) = local_client().await;
    assert!(!client.inner.recovery.is_connected());
    let (first_ready, first_started) = oneshot::channel();
    let (close_first, close_requested) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = authenticated(&listener).await;
        first_ready.send(()).unwrap();
        close_requested.await.unwrap();
        first.close(None).await.unwrap();
        drop(first);
        let mut second = authenticated(&listener).await;
        let packet = next_packet(&mut second).await;
        assert_eq!(packet.opcode, opcode::MSG_SEND);
        assert_eq!(packet.payload["message"]["text"], "after reconnect");
        respond(&mut second, &packet, json!({})).await;
        std::future::pending::<()>().await;
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    first_started.await.unwrap();
    wait_for_connection(&client, true).await;
    close_first.send(()).unwrap();
    wait_for_connection(&client, false).await;
    tokio::time::timeout(
        Duration::from_secs(5),
        client.send_text(7, MaxMessage::new("after reconnect")),
    )
    .await
    .unwrap()
    .unwrap();
    client.disconnect().await;
    runner.await.unwrap().unwrap();
    assert!(!client.inner.recovery.is_connected());
    server.abort();
}

#[tokio::test]
async fn reconnect_replays_an_in_flight_send() {
    let (client, listener) = local_client().await;
    let (release_server, server_released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = authenticated(&listener).await;
        let first_packet = next_packet(&mut first).await;
        assert_eq!(first_packet.opcode, opcode::MSG_SEND);
        drop(first);

        let mut second = authenticated(&listener).await;
        let replayed = tokio::time::timeout(Duration::from_secs(2), next_packet(&mut second)).await;
        if let Ok(packet) = &replayed {
            assert_eq!(packet.payload["message"]["text"], "replay me");
            assert_eq!(
                packet.payload["message"]["cid"],
                first_packet.payload["message"]["cid"]
            );
            respond(&mut second, packet, json!({})).await;
            let _ = server_released.await;
        }
        replayed.is_ok()
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });

    let result = client.send_text(7, MaxMessage::new("replay me")).await;

    assert!(result.is_ok());
    let _ = release_server.send(());
    assert!(server.await.unwrap(), "the pending send was not replayed");
    client.disconnect().await;
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelling_a_pending_send_prevents_replay() {
    let (client, listener) = local_client().await;
    let (received, first_received) = oneshot::channel();
    let (close_first, close_requested) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = authenticated(&listener).await;
        assert_eq!(next_packet(&mut first).await.opcode, opcode::MSG_SEND);
        received.send(()).unwrap();
        close_requested.await.unwrap();
        drop(first);

        let mut second = authenticated(&listener).await;
        tokio::time::timeout(Duration::from_secs(1), next_packet(&mut second))
            .await
            .is_ok()
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let sending = tokio::spawn({
        let client = client.clone();
        async move { client.send_text(7, MaxMessage::new("cancel me")).await }
    });

    first_received.await.unwrap();
    sending.abort();
    assert!(sending.await.unwrap_err().is_cancelled());
    close_first.send(()).unwrap();

    assert!(!server.await.unwrap(), "the cancelled send was replayed");
    client.disconnect().await;
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn permanent_send_error_does_not_retry_or_disconnect() {
    let (client, listener) = local_client().await;
    let server = tokio::spawn(async move {
        let mut socket = authenticated(&listener).await;
        let mut packet = next_packet(&mut socket).await;
        packet.cmd = crate::protocol::CMD_ERROR;
        packet.payload = json!({"error": "chat not found"});
        socket
            .send(Message::text(serde_json::to_string(&packet).unwrap()))
            .await
            .unwrap();
        let next = next_packet(&mut socket).await;
        assert_eq!(next.payload["message"]["text"], "second");
        respond(&mut socket, &next, json!({})).await;
        std::future::pending::<()>().await;
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let result = client.send_text(7, MaxMessage::new("invalid")).await;
    assert!(matches!(result, Err(Error::Server { .. })));
    assert!(client.inner.recovery.is_connected());
    client
        .send_text(7, MaxMessage::new("second"))
        .await
        .unwrap();
    client.disconnect().await;
    runner.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn disconnect_wakes_waiting_sends_and_stops_recovery() {
    let (client, listener) = local_client().await;
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let (socket, _) = listener.accept().await.unwrap();
    let sending = tokio::spawn({
        let client = client.clone();
        async move { client.send_text(7, MaxMessage::new("waiting")).await }
    });
    tokio::task::yield_now().await;
    assert!(!sending.is_finished());
    client.disconnect().await;
    assert!(matches!(
        sending.await.unwrap(),
        Err(Error::ConnectionClosed)
    ));
    runner.await.unwrap().unwrap();
    drop(socket);
    assert!(!client.inner.recovery.is_connected());
}

#[tokio::test]
async fn cancelling_runner_wakes_senders() {
    let (client, listener) = local_client().await;
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let (socket, _) = listener.accept().await.unwrap();
    let sending = tokio::spawn({
        let client = client.clone();
        async move { client.send_text(7, MaxMessage::new("waiting")).await }
    });
    runner.abort();
    assert!(runner.await.unwrap_err().is_cancelled());
    assert!(matches!(
        sending.await.unwrap(),
        Err(Error::ConnectionClosed)
    ));
    drop(socket);
}

#[test]
fn connection_backoff_doubles_caps_and_resets_after_success() {
    let mut backoff = recovery::Backoff::default();
    for seconds in [1, 2, 4, 8, 16, 32, 64, 120, 120] {
        assert_eq!(backoff.next_delay(), Duration::from_secs(seconds));
    }
    backoff.reset();
    assert_eq!(backoff.next_delay(), Duration::from_secs(1));
}

#[test]
fn connection_backoff_only_resets_after_a_healthy_session() {
    let mut backoff = recovery::Backoff::default();
    assert_eq!(backoff.next_delay(), Duration::from_secs(1));
    assert_eq!(backoff.next_delay(), Duration::from_secs(2));

    backoff.reset_if_healthy(recovery::HEALTHY_CONNECTION_INTERVAL - Duration::from_millis(1));
    assert_eq!(backoff.next_delay(), Duration::from_secs(4));

    backoff.reset_if_healthy(recovery::HEALTHY_CONNECTION_INTERVAL);
    assert_eq!(backoff.next_delay(), Duration::from_secs(1));
}

#[test]
fn only_temporary_authentication_service_failures_are_retried() {
    assert!(recovery::should_retry_connection(&Error::CaptchaTimeout {
        challenge_id: "challenge".into(),
    }));
    assert!(recovery::should_retry_connection(
        &Error::TelegramUnavailable("timed out waiting for SMS".into())
    ));
    assert!(!recovery::should_retry_connection(&Error::Telegram(
        "invalid bot token".into(),
    )));
    assert!(!recovery::should_retry_connection(&Error::CaptchaFailed(
        "invalid callback".into(),
    )));
    assert!(!recovery::should_retry_connection(
        &Error::UnknownCaptchaChallenge {
            challenge_id: "stale".into(),
        }
    ));
    assert!(!recovery::should_retry_connection(&Error::Io(
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "bad callback bind")
    )));
    assert!(recovery::should_retry_connection(&Error::Io(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "temporary failure")
    )));
    assert!(!recovery::should_retry_connection(&Error::Telegram(
        "timed out waiting for SMS".into(),
    )));
    assert!(!recovery::should_retry_connection(
        &Error::MissingCredentials
    ));
}

#[tokio::test]
async fn cancelled_upload_removes_attachment_waiter_without_stopping_client() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (client, listener) = local_client().await;
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upload_url = format!("http://{}/", http.local_addr().unwrap());
    let (uploaded, uploaded_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = authenticated(&listener).await;
        let packet = next_packet(&mut socket).await;
        assert_eq!(packet.opcode, opcode::FILE_UPLOAD);
        respond(
            &mut socket,
            &packet,
            json!({"info": [{"url": upload_url, "fileId": 100}]}),
        )
        .await;
        let (mut upload, _) = http.accept().await.unwrap();
        let mut buffer = [0; 4096];
        assert!(upload.read(&mut buffer).await.unwrap() > 0);
        upload
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        uploaded.send(()).unwrap();
        let packet = next_packet(&mut socket).await;
        assert_eq!(packet.opcode, opcode::MSG_SEND);
        respond(&mut socket, &packet, json!({})).await;
        std::future::pending::<()>().await;
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let sending = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .send_file_bytes(7, "book.fb2", b"contents".as_slice(), "")
                .await
        }
    });
    uploaded_rx.await.unwrap();
    assert_eq!(client.inner.file_waiters.lock().await.len(), 1);
    sending.abort();
    assert!(sending.await.unwrap_err().is_cancelled());
    assert!(
        client.inner.file_waiters.lock().await.is_empty(),
        "cancelled upload leaked its attachment waiter"
    );
    client
        .send_text(7, MaxMessage::new("ordinary request"))
        .await
        .unwrap();
    client.disconnect().await;
    runner.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn reconnect_replays_an_in_flight_document_upload() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (client, listener) = local_client().await;
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upload_url = format!("http://{}/", http.local_addr().unwrap());
    let (release_server, server_released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = authenticated(&listener).await;
        assert_eq!(next_packet(&mut first).await.opcode, opcode::FILE_UPLOAD);
        drop(first);
        let mut second = authenticated(&listener).await;
        let Ok(packet) =
            tokio::time::timeout(Duration::from_secs(2), next_packet(&mut second)).await
        else {
            return false;
        };
        assert_eq!(packet.opcode, opcode::FILE_UPLOAD);
        respond(
            &mut second,
            &packet,
            json!({"info": [{"url": upload_url, "fileId": 101}]}),
        )
        .await;

        let (mut upload, _) = http.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut buffer = [0; 4096];
            let size = upload.read(&mut buffer).await.unwrap();
            assert!(size > 0);
            request.extend_from_slice(&buffer[..size]);
            if request.ends_with(b"book contents") {
                break;
            }
        }
        upload
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let attached = Packet::request(999, opcode::NOTIF_ATTACH, json!({"fileId": 101}));
        second
            .send(Message::text(serde_json::to_string(&attached).unwrap()))
            .await
            .unwrap();
        let packet = next_packet(&mut second).await;
        assert_eq!(packet.opcode, opcode::MSG_SEND);
        assert_eq!(packet.payload["message"]["attaches"][0]["fileId"], 101);
        respond(&mut second, &packet, json!({})).await;
        let _ = server_released.await;
        true
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        client.send_file_bytes(7, "book.fb2", b"book contents".as_slice(), "caption"),
    )
    .await
    .unwrap();
    assert!(result.is_ok());
    let _ = release_server.send(());
    assert!(server.await.unwrap(), "the pending upload was not replayed");
    client.disconnect().await;
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_replays_a_file_message_with_the_same_cid() {
    let (client, listener) = local_client().await;
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upload_url = format!("http://{}/", http.local_addr().unwrap());
    let (release_server, server_released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = authenticated(&listener).await;
        let upload = next_packet(&mut first).await;
        assert_eq!(upload.opcode, opcode::FILE_UPLOAD);
        respond(
            &mut first,
            &upload,
            json!({"info": [{"url": upload_url, "fileId": 100}]}),
        )
        .await;
        receive_upload(&http).await;
        first
            .send(Message::text(
                serde_json::to_string(&Packet::request(
                    998,
                    opcode::NOTIF_ATTACH,
                    json!({"fileId": 100}),
                ))
                .unwrap(),
            ))
            .await
            .unwrap();
        let first_message = next_packet(&mut first).await;
        assert_eq!(first_message.opcode, opcode::MSG_SEND);
        drop(first);

        let mut second = authenticated(&listener).await;
        let upload = next_packet(&mut second).await;
        assert_eq!(upload.opcode, opcode::FILE_UPLOAD);
        respond(
            &mut second,
            &upload,
            json!({"info": [{"url": upload_url, "fileId": 101}]}),
        )
        .await;
        receive_upload(&http).await;
        second
            .send(Message::text(
                serde_json::to_string(&Packet::request(
                    999,
                    opcode::NOTIF_ATTACH,
                    json!({"fileId": 101}),
                ))
                .unwrap(),
            ))
            .await
            .unwrap();
        let replayed = next_packet(&mut second).await;
        assert_eq!(replayed.opcode, opcode::MSG_SEND);
        assert_eq!(replayed.payload["message"]["attaches"][0]["fileId"], 101);
        assert_eq!(
            replayed.payload["message"]["cid"],
            first_message.payload["message"]["cid"]
        );
        respond(&mut second, &replayed, json!({})).await;
        let _ = server_released.await;
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        client.send_file_bytes(7, "book.fb2", b"book contents".as_slice(), "caption"),
    )
    .await
    .unwrap()
    .unwrap();
    let _ = release_server.send(());
    server.await.unwrap();
    client.disconnect().await;
    runner.await.unwrap().unwrap();
}

async fn receive_upload(listener: &TcpListener) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (mut upload, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    loop {
        let mut buffer = [0; 4096];
        let size = upload.read(&mut buffer).await.unwrap();
        assert!(size > 0);
        request.extend_from_slice(&buffer[..size]);
        if request.ends_with(b"book contents") {
            break;
        }
    }
    upload
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
}

#[tokio::test]
async fn runner_rejects_competing_run_without_stopping_owner() {
    let (client, listener) = local_client().await;
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let (socket, _) = listener.accept().await.unwrap();
    assert!(matches!(
        client.run(Handler).await,
        Err(Error::ClientAlreadyRunning)
    ));
    assert!(!client.inner.recovery.is_connected());
    client.disconnect().await;
    runner.await.unwrap().unwrap();
    drop(socket);
}
