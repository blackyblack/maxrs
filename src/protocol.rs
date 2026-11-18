//! Binary frame types and opcodes for the Max wire protocol.

use crate::error::{Error, Result};
use serde_json::Value;
use std::io::Cursor;

/// Protocol version. The web client currently uses binary version `10`.
pub const PROTOCOL_VERSION: u8 = 10;

/// `cmd` field: a request (originated by either side).
pub const CMD_REQUEST: u8 = 0;
/// `cmd` field: a successful response to a request with the same `seq`.
pub const CMD_RESPONSE: u8 = 1;
/// `cmd` field: an error response.
pub const CMD_ERROR: u8 = 3;

/// Operation codes used by this client.
pub mod opcode {
    pub const PING: u16 = 1;
    pub const RECONNECT: u16 = 3;
    pub const SESSION_INIT: u16 = 6;
    pub const AUTH_REQUEST: u16 = 17;
    pub const AUTH: u16 = 18;
    pub const AUTH_PASSWORD: u16 = 115;
    pub const LOGIN: u16 = 19;
    pub const AUTH_CAPTCHA_REQUEST: u16 = 224;
    pub const MSG_SEND: u16 = 64;
    pub const FILE_UPLOAD: u16 = 87;
    pub const NOTIF_MESSAGE: u16 = 128;
    pub const NOTIF_ATTACH: u16 = 136;
}

/// A single protocol frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Packet {
    pub cmd: u8,
    pub seq: u32,
    pub opcode: u16,
    pub payload: Value,
}

impl Packet {
    pub(crate) fn encode(&self) -> Result<Vec<u8>> {
        let seq = u16::try_from(self.seq).map_err(|_| invalid("sequence overflow"))?;
        let mut payload = if self.payload.is_null() {
            Vec::new()
        } else {
            rmp_serde::to_vec(&self.payload).map_err(|e| invalid(e.to_string()))?
        };
        let mut ratio = 0;
        if payload.len() > 32 {
            let compressed = lz4_flex::block::compress(&payload);
            if let Ok(multiplier) = u8::try_from(payload.len().div_ceil(compressed.len())) {
                ratio = multiplier;
                payload = compressed;
            }
        }
        if payload.len() > 0xff_ffff {
            return Err(invalid("payload exceeds 24-bit length"));
        }
        let mut bytes = Vec::with_capacity(10 + payload.len());
        bytes.extend([PROTOCOL_VERSION, self.cmd]);
        bytes.extend(seq.to_be_bytes());
        bytes.extend(self.opcode.to_be_bytes());
        bytes.push(ratio);
        bytes.extend(&(payload.len() as u32).to_be_bytes()[1..]);
        bytes.extend(payload);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let [PROTOCOL_VERSION, cmd, seq_hi, seq_lo, op_hi, op_lo, ratio, hi, mid, lo, body @ ..] =
            bytes
        else {
            return Err(invalid("invalid header or protocol version"));
        };
        let len = u32::from_be_bytes([0, *hi, *mid, *lo]) as usize;
        if len != body.len() {
            return Err(invalid("payload length mismatch"));
        }
        let expanded;
        let payload = if body.is_empty() {
            Value::Null
        } else {
            let data = if *ratio == 0 {
                body
            } else {
                // The multiplier is a rounded-up size estimate, not an exact length.
                let capacity = (len * usize::from(*ratio)).min(64 * 1024 * 1024);
                expanded = lz4_flex::block::decompress(body, capacity)
                    .map_err(|e| invalid(e.to_string()))?;
                &expanded
            };
            unpack(data)?
        };
        Ok(Self {
            cmd: *cmd,
            seq: u16::from_be_bytes([*seq_hi, *seq_lo]).into(),
            opcode: u16::from_be_bytes([*op_hi, *op_lo]),
            payload,
        })
    }

    /// Builds an outgoing request frame.
    pub fn request(seq: u32, opcode: u16, payload: Value) -> Self {
        Self {
            cmd: CMD_REQUEST,
            seq,
            opcode,
            payload,
        }
    }

    /// Builds a response frame echoing a server request's `seq`/`opcode`.
    pub fn response(seq: u32, opcode: u16, payload: Value) -> Self {
        Self {
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

fn invalid(reason: impl Into<String>) -> Error {
    Error::UnexpectedResponse(reason.into())
}

fn unpack(bytes: &[u8]) -> Result<Value> {
    let mut input = Cursor::new(bytes);
    let value = rmpv::decode::read_value(&mut input).map_err(|e| invalid(e.to_string()))?;
    if input.position() != bytes.len() as u64 {
        return Err(invalid("trailing MessagePack data"));
    }
    to_json(value)
}

fn to_json(value: rmpv::Value) -> Result<Value> {
    use rmpv::Value as Msg;
    Ok(match value {
        Msg::Nil => Value::Null,
        Msg::Boolean(v) => v.into(),
        Msg::Integer(v) => match v.as_i64() {
            Some(i) => i.into(),
            None => v.as_u64().ok_or_else(|| invalid("invalid integer"))?.into(),
        },
        Msg::F32(v) => v.into(),
        Msg::F64(v) => v.into(),
        Msg::String(v) => v.into_str().ok_or_else(|| invalid("invalid UTF-8"))?.into(),
        Msg::Array(v) => Value::Array(v.into_iter().map(to_json).collect::<Result<_>>()?),
        Msg::Map(v) => {
            let mut map = serde_json::Map::new();
            for (key, item) in v {
                let key = match to_json(key)? {
                    Value::String(s) => s,
                    Value::Number(n) => n.to_string(),
                    _ => return Err(invalid("invalid map key")),
                };
                map.insert(key, to_json(item)?);
            }
            Value::Object(map)
        }
        Msg::Binary(v) => Value::from(v),
        Msg::Ext(1, v) => {
            // MAX wraps some int64 IDs in extension 1 containing a packed integer.
            let mut input = Cursor::new(&v);
            match rmpv::decode::read_value(&mut input) {
                Ok(n @ Msg::Integer(_)) if input.position() == v.len() as u64 => to_json(n)?,
                _ => return Err(invalid("invalid int64 extension")),
            }
        }
        Msg::Ext(_, _) => return Err(invalid("unknown MessagePack extension")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_compressed_map_with_int64_extension() {
        // A literal-only LZ4 block containing {"id": ext1(uint64(2^53 + 1))}.
        let frame = [
            10, 1, 0x12, 0x34, 0, 19, 1, 0, 0, 18, 0xf0, 1, 0x81, 0xa2, b'i', b'd', 0xc7, 9, 1,
            0xcf, 0, 0x20, 0, 0, 0, 0, 0, 1,
        ];
        let packet = Packet::decode(&frame).unwrap();
        assert_eq!(packet.seq, 0x1234);
        assert_eq!(packet.payload["id"], 9_007_199_254_740_993_u64);
        assert!(Packet::decode(&frame[..frame.len() - 1]).is_err());
    }

    #[test]
    fn classifies_cmd_field() {
        let req = Packet::request(1, opcode::PING, Value::Null);
        assert!(req.is_request());
        assert!(!req.is_error());

        let err = Packet {
            cmd: CMD_ERROR,
            seq: 1,
            opcode: opcode::PING,
            payload: Value::Null,
        };
        assert!(err.is_error());
        assert!(!err.is_request());
    }
}
