//! Low-level wire protocol: the JSON envelope and opcode constants.
//!
//! Every message exchanged with `wss://ws-api.oneme.ru/websocket` is a JSON
//! object of the form:
//!
//! ```json
//! {"ver": 11, "cmd": 0, "seq": 1, "opcode": 6, "payload": { ... }}
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Protocol version. The web client currently pins this to `11`.
pub const PROTOCOL_VERSION: u8 = 11;

/// `cmd` field: a request (originated by either side).
pub const CMD_REQUEST: u8 = 0;
/// `cmd` field: a successful response to a request with the same `seq`.
pub const CMD_RESPONSE: u8 = 1;
/// `cmd` field: an error response.
pub const CMD_ERROR: u8 = 3;

/// Operation codes used by this client.
///
/// The full list lives in PronikFire's Max-API-Guide; only the handful we need
/// are mirrored here.
pub mod opcode {
    pub const PING: u16 = 1;
    pub const SESSION_INIT: u16 = 6;
    pub const AUTH_REQUEST: u16 = 17;
    pub const AUTH: u16 = 18;
    pub const LOGIN: u16 = 19;
    pub const AUTH_CAPTCHA_REQUEST: u16 = 224;
    pub const MSG_SEND: u16 = 64;
    pub const MSG_TYPING: u16 = 65;
    pub const FILE_UPLOAD: u16 = 87;
    pub const NOTIF_MESSAGE: u16 = 128;
    pub const NOTIF_ATTACH: u16 = 136;
}

/// A single protocol frame.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet {
    #[serde(rename = "ver")]
    pub ver: u8,
    pub cmd: u8,
    pub seq: u32,
    pub opcode: u16,
    pub payload: Value,
}

impl Packet {
    /// Builds an outgoing request frame.
    pub fn request(seq: u32, opcode: u16, payload: Value) -> Self {
        Self {
            ver: PROTOCOL_VERSION,
            cmd: CMD_REQUEST,
            seq,
            opcode,
            payload,
        }
    }

    /// Builds a response frame echoing a server request's `seq`/`opcode`.
    pub fn response(seq: u32, opcode: u16, payload: Value) -> Self {
        Self {
            ver: PROTOCOL_VERSION,
            cmd: CMD_RESPONSE,
            seq,
            opcode,
            payload,
        }
    }

    pub fn is_error(&self) -> bool {
        self.cmd == CMD_ERROR
    }

    pub fn is_request(&self) -> bool {
        self.cmd == CMD_REQUEST
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn request_frame_serializes_to_expected_shape() {
        let packet = Packet::request(7, opcode::MSG_SEND, json!({ "chatId": 42 }));
        let value: Value = serde_json::from_str(&serde_json::to_string(&packet).unwrap()).unwrap();
        assert_eq!(value["ver"], PROTOCOL_VERSION);
        assert_eq!(value["cmd"], CMD_REQUEST);
        assert_eq!(value["seq"], 7);
        assert_eq!(value["opcode"], opcode::MSG_SEND);
        assert_eq!(value["payload"]["chatId"], 42);
    }

    #[test]
    fn classifies_cmd_field() {
        let req = Packet::request(1, opcode::PING, Value::Null);
        assert!(req.is_request());
        assert!(!req.is_error());

        let err = Packet {
            ver: PROTOCOL_VERSION,
            cmd: CMD_ERROR,
            seq: 1,
            opcode: opcode::PING,
            payload: Value::Null,
        };
        assert!(err.is_error());
        assert!(!err.is_request());
    }
}
