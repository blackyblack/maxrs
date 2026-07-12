use std::sync::Arc;

use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio_tungstenite::tungstenite::Message;

use crate::models::IncomingMessage;
use crate::protocol::{opcode, Packet};

use super::transport::WsStream;
use super::InnerClient;

const SECURITY_SERVICE_USER_ID: i64 = 543_835;

pub(super) async fn read_loop(
    mut read: futures_util::stream::SplitStream<WsStream>,
    inner: Arc<InnerClient>,
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
            handle_server_request(&inner, packet).await;
        } else {
            inner.transport.receive_response(packet).await;
        }
    }

    inner.fail().await;
}

async fn handle_server_request(inner: &Arc<InnerClient>, packet: Packet) {
    match packet.opcode {
        opcode::RECONNECT => {
            tracing::warn!("Max server requested reconnect");
            inner.fail().await;
        }
        opcode::NOTIF_MESSAGE => {
            if let Some(message) = parse_incoming(&packet.payload) {
                // Acknowledge the push, mirroring the official web client.
                let ack = Packet::response(
                    packet.seq,
                    packet.opcode,
                    json!({ "chatId": message.chat_id, "messageId": message.message_id }),
                );
                let _ = inner.transport.send(&ack).await;
                if !is_filtered_incoming_message(&message, inner.own_user_id().await) {
                    if let Some(tx) = inner
                        .msg_tx
                        .lock()
                        .await
                        .as_ref()
                        .and_then(|dispatcher| dispatcher.tx.as_ref())
                    {
                        let _ = tx.send(message);
                    }
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
}
