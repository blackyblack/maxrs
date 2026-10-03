use super::*;
use crate::auth::{operator_channels::OperatorChannel, AuthCaptchaConfig};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

pub(super) struct Handler;

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

pub(super) async fn local_client() -> (MaxClient, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = MaxClient::new(config()).unwrap();
    client.set_test_url(format!("ws://{}/", listener.local_addr().unwrap()));
    (client, listener)
}

pub(super) async fn wait_for_connection(client: &MaxClient, expected: bool) {
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

pub(super) async fn authenticated(
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
async fn cancelled_upload_unregisters_attachment_waiter_without_stopping_client() {
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
        receive_upload(&http, b"contents").await;
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
    let connection = client.inner.recovery.current().await.unwrap();
    assert_eq!(connection.attachment_registry.waiter_count(), 1);
    sending.abort();
    assert!(sending.await.unwrap_err().is_cancelled());
    assert_eq!(connection.attachment_registry.waiter_count(), 0);
    client
        .send_text(7, MaxMessage::new("ordinary request"))
        .await
        .unwrap();
    client.disconnect().await;
    runner.await.unwrap().unwrap();
    server.abort();
}

#[tokio::test]
async fn attachment_completion_survives_an_unrelated_notification_burst() {
    use tokio::io::AsyncWriteExt;

    let (client, listener) = local_client().await;
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upload_url = format!("http://{}/", http.local_addr().unwrap());
    let (release_server, server_released) = oneshot::channel();
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

        let mut upload = receive_upload_request(&http, b"contents").await;

        let attached = Packet::request(900, opcode::NOTIF_ATTACH, json!({"fileId": 100}));
        socket
            .send(Message::text(serde_json::to_string(&attached).unwrap()))
            .await
            .unwrap();
        for file_id in 1_000..1_064 {
            let unrelated =
                Packet::request(file_id, opcode::NOTIF_ATTACH, json!({"fileId": file_id}));
            socket
                .send(Message::text(serde_json::to_string(&unrelated).unwrap()))
                .await
                .unwrap();
        }

        upload
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let message = next_packet(&mut socket).await;
        assert_eq!(message.opcode, opcode::MSG_SEND);
        assert_eq!(message.payload["message"]["attaches"][0]["fileId"], 100);
        respond(&mut socket, &message, json!({})).await;
        let _ = server_released.await;
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });

    tokio::time::timeout(
        Duration::from_secs(5),
        client.send_file_bytes(7, "book.fb2", b"contents".as_slice(), ""),
    )
    .await
    .expect("the registered completion must not be lost")
    .unwrap();

    let _ = release_server.send(());
    server.await.unwrap();
    client.disconnect().await;
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_replays_an_in_flight_document_upload() {
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

        receive_upload(&http, b"book contents").await;
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
        receive_upload(&http, b"book contents").await;
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
        receive_upload(&http, b"book contents").await;
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

async fn receive_upload_request(
    listener: &TcpListener,
    expected_body: &[u8],
) -> tokio::net::TcpStream {
    use tokio::io::AsyncReadExt;

    let (mut upload, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    loop {
        let mut buffer = [0; 4096];
        let size = upload.read(&mut buffer).await.unwrap();
        assert!(size > 0);
        request.extend_from_slice(&buffer[..size]);
        if let Some(header_end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
            let body = &request[header_end + 4..];
            if body.len() >= expected_body.len() {
                assert_eq!(body, expected_body);
                return upload;
            }
        }
    }
}

async fn receive_upload(listener: &TcpListener, expected_body: &[u8]) {
    use tokio::io::AsyncWriteExt;

    let mut upload = receive_upload_request(listener, expected_body).await;
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

#[tokio::test]
async fn old_session_cannot_complete_requests_or_attachments_on_its_replacement() {
    let (client, listener) = local_client().await;
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let first = authenticated(&listener).await;
    wait_for_connection(&client, true).await;
    let old = client.inner.recovery.current().await.unwrap();
    drop(first);
    let mut second = tokio::time::timeout(Duration::from_secs(3), authenticated(&listener))
        .await
        .unwrap();
    wait_for_connection(&client, true).await;
    let current = client.inner.recovery.current().await.unwrap();
    assert!(!Arc::ptr_eq(&old, &current));

    let waiter = current.attachment_registry.register(42).unwrap();
    old.attachment_registry.complete(42);
    assert_eq!(current.attachment_registry.waiter_count(), 1);
    let mut sending = tokio::spawn({
        let client = client.clone();
        async move { client.send_text(7, MaxMessage::new("new session")).await }
    });
    let packet = next_packet(&mut second).await;
    old.transport
        .receive_response(Packet::response(packet.seq, packet.opcode, json!({})))
        .await;
    old.cancel();
    old.transport.close().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut sending)
            .await
            .is_err()
    );
    assert!(!current.is_cancelled());
    assert!(matches!(
        old.invoke(opcode::PING, json!({})).await,
        Err(Error::ConnectionClosed)
    ));
    respond(&mut second, &packet, json!({})).await;
    sending.await.unwrap().unwrap();
    current.attachment_registry.complete(42);
    waiter.wait().await;
    client.disconnect().await;
    runner.await.unwrap().unwrap();
}

#[tokio::test]
async fn lost_socket_cancels_captcha_authentication_and_reconnects() {
    use http_body_util::{BodyExt, Full};
    use hyper::{body::Bytes, service::service_fn, Response};
    use hyper_util::rt::TokioIo;
    use std::convert::Infallible;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let solver = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config();
    config.session_token = None;
    config.phone = Some("+79990000000".into());
    config.operator = OperatorChannel::Cli;
    config.captcha.solver_url = Some(format!("http://{}", solver.local_addr().unwrap()));
    config.captcha.callback_bind = "127.0.0.1:0".into();
    let client = MaxClient::new(config).unwrap();
    client.set_test_url(format!("ws://{}/", listener.local_addr().unwrap()));
    let requested = Arc::new(tokio::sync::Notify::new());
    let solver_task = tokio::spawn({
        let requested = Arc::clone(&requested);
        async move {
            let (stream, _) = solver.accept().await.unwrap();
            hyper::server::conn::http1::Builder::new()
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
                        let requested = Arc::clone(&requested);
                        async move {
                            assert_eq!(request.uri().path(), "/solve");
                            let body = request.into_body().collect().await.unwrap().to_bytes();
                            let payload: Value = serde_json::from_slice(&body).unwrap();
                            assert!(payload["callbackUrl"]
                                .as_str()
                                .unwrap()
                                .ends_with("/captcha-callback"));
                            requested.notify_one();
                            // Accept, but never deliver a captcha callback.
                            Ok::<_, Infallible>(
                                Response::builder()
                                    .status(202)
                                    .body(Full::new(Bytes::new()))
                                    .unwrap(),
                            )
                        }
                    }),
                )
                .await
                .unwrap();
        }
    });
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(Handler).await }
    });
    let (stream, _) = listener.accept().await.unwrap();
    let mut first = accept_async(stream).await.unwrap();
    let init = next_packet(&mut first).await;
    assert_eq!(init.opcode, opcode::SESSION_INIT);
    respond(&mut first, &init, json!({})).await;
    let mut sms = next_packet(&mut first).await;
    assert_eq!(sms.opcode, opcode::AUTH_REQUEST);
    sms.cmd = crate::protocol::CMD_ERROR;
    sms.payload = json!({"error": "captcha required"});
    first
        .send(Message::text(serde_json::to_string(&sms).unwrap()))
        .await
        .unwrap();
    let captcha = next_packet(&mut first).await;
    assert_eq!(captcha.opcode, opcode::AUTH_CAPTCHA_REQUEST);
    respond(
        &mut first,
        &captcha,
        json!({"link": "https://captcha.example/challenge"}),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), requested.notified())
        .await
        .unwrap();
    first.close(None).await.unwrap();
    drop(first);
    let (stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("reconnect must not wait for the one-hour captcha timeout")
        .unwrap();
    let mut second = accept_async(stream).await.unwrap();
    assert_eq!(next_packet(&mut second).await.opcode, opcode::SESSION_INIT);
    client.disconnect().await;
    runner.await.unwrap().unwrap();
    solver_task.abort();
}

#[tokio::test]
async fn accepted_handler_survives_reconnect_and_can_request_shutdown() {
    struct ReplyHandler {
        client: MaxClient,
        release: Arc<tokio::sync::Semaphore>,
        events: mpsc::UnboundedSender<&'static str>,
    }
    struct Finished(mpsc::UnboundedSender<&'static str>);
    impl Drop for Finished {
        fn drop(&mut self) {
            let _ = self.0.send("finished");
        }
    }
    impl ChatHandler for ReplyHandler {
        async fn on_message(&self, message: IncomingMessage) -> Result<()> {
            let _finished = Finished(self.events.clone());
            self.events.send("started").unwrap();
            self.release.acquire().await.unwrap().forget();
            self.client
                .send_text(message.chat_id, MaxMessage::new("reply after reconnect"))
                .await?;
            self.events.send("replied").unwrap();
            self.client.disconnect().await;
            Ok(())
        }
    }
    let (client, listener) = local_client().await;
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (events, mut received) = mpsc::unbounded_channel();
    let handler = ReplyHandler {
        client: client.clone(),
        release: Arc::clone(&release),
        events,
    };
    let runner = tokio::spawn({
        let client = client.clone();
        async move { client.run(handler).await }
    });
    let mut first = authenticated(&listener).await;
    first
        .send(Message::text(
            serde_json::to_string(&Packet::request(
                100,
                opcode::NOTIF_MESSAGE,
                json!({"chatId": 7, "message": {"id": 10, "sender": 99, "text": "request"}}),
            ))
            .unwrap(),
        ))
        .await
        .unwrap();
    assert_eq!(
        next_packet(&mut first).await.cmd,
        crate::protocol::CMD_RESPONSE
    );
    assert_eq!(received.recv().await, Some("started"));
    drop(first);
    let mut second = tokio::time::timeout(Duration::from_secs(3), authenticated(&listener))
        .await
        .unwrap();
    release.add_permits(1);
    let reply = tokio::time::timeout(Duration::from_secs(1), next_packet(&mut second))
        .await
        .unwrap();
    assert_eq!(reply.payload["message"]["text"], "reply after reconnect");
    respond(&mut second, &reply, json!({})).await;
    assert_eq!(received.recv().await, Some("replied"));
    tokio::time::timeout(Duration::from_secs(1), runner)
        .await
        .expect("a handler must be able to stop the runner without deadlock")
        .unwrap()
        .unwrap();
    assert_eq!(received.recv().await, Some("finished"));
    // Shutdown has released the socket even though the client handle remains alive.
    let closed = tokio::time::timeout(Duration::from_secs(1), second.next())
        .await
        .unwrap();
    assert!(matches!(
        closed,
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}
