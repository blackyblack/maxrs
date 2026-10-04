use std::sync::Mutex;

use serde_json::Value;
use tokio_util::sync::CancellationToken;

use super::attachment_registry::AttachmentRegistry;
use super::transport::Transport;
use crate::error::{Error, Result};
use crate::protocol::Packet;

/// Resources belonging to one socket generation, including authentication.
pub struct Connection {
    pub transport: Transport,
    pub attachment_registry: AttachmentRegistry,
    pub cancellation: CancellationToken,
    own_user_id: Mutex<Option<i64>>,
}

pub fn is_transport_failure(error: &Error) -> bool {
    matches!(
        error,
        Error::WebSocket(_)
            | Error::WebSocketConnectTimeout
            | Error::Timeout(_)
            | Error::DuplicateSequence(_)
            | Error::ConnectionClosed
    )
}

impl Connection {
    pub fn new(cancellation: CancellationToken) -> Self {
        Self {
            transport: Transport::new(),
            attachment_registry: AttachmentRegistry::default(),
            cancellation,
            own_user_id: Mutex::new(None),
        }
    }

    pub async fn cancelled(&self) {
        self.cancellation.cancelled().await;
    }

    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub fn set_own_user_id(&self, id: i64) {
        *self.own_user_id.lock().expect("session user id") = Some(id);
    }

    pub fn own_user_id(&self) -> Option<i64> {
        *self.own_user_id.lock().expect("session user id")
    }

    pub async fn invoke(&self, opcode: u16, payload: Value) -> Result<Packet> {
        let result = tokio::select! {
            biased;
            _ = self.cancelled() => return Err(Error::ConnectionClosed),
            result = self.transport.invoke(opcode, payload) => result,
        };
        if result.as_ref().is_err_and(is_transport_failure) {
            self.cancel();
        }
        result
    }
}
