use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(120);
pub(super) const HEALTHY_CONNECTION_INTERVAL: Duration = Duration::from_secs(60);

pub(super) struct Connection {
    cancelled: CancellationToken,
}

impl Connection {
    fn new() -> Self {
        Self {
            cancelled: CancellationToken::new(),
        }
    }

    pub(super) async fn cancelled(&self) {
        self.cancelled.cancelled().await;
    }

    fn cancel(&self) {
        self.cancelled.cancel();
    }

    pub(super) fn is_cancelled(&self) -> bool {
        self.cancelled.is_cancelled()
    }
}

enum State {
    Disconnected(Option<Arc<Connection>>),
    Connected(Arc<Connection>),
    Stopped,
}

impl State {
    fn connection(&self) -> Option<&Arc<Connection>> {
        match self {
            Self::Disconnected(connection) => connection.as_ref(),
            Self::Connected(connection) => Some(connection),
            Self::Stopped => None,
        }
    }
}

pub(super) struct Recovery {
    state: Mutex<State>,
    changed: Notify,
    pub(super) shutdown: CancellationToken,
}

impl Recovery {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State::Disconnected(None)),
            changed: Notify::new(),
            shutdown: CancellationToken::new(),
        }
    }

    pub(super) fn begin_attempt(&self) -> Result<Arc<Connection>> {
        let connection = Arc::new(Connection::new());
        let mut state = self.state.lock().expect("Max recovery state");
        if matches!(*state, State::Stopped) {
            connection.cancel();
            return Err(Error::ConnectionClosed);
        }
        if let Some(previous) = state.connection() {
            previous.cancel();
        }
        *state = State::Disconnected(Some(Arc::clone(&connection)));
        self.changed.notify_waiters();
        Ok(connection)
    }

    pub(super) fn connected(&self, connection: &Arc<Connection>) -> Result<()> {
        let mut state = self.state.lock().expect("Max recovery state");
        let is_current = state
            .connection()
            .is_some_and(|current| Arc::ptr_eq(current, connection));
        if self.shutdown.is_cancelled() || connection.is_cancelled() || !is_current {
            return Err(Error::ConnectionClosed);
        }
        *state = State::Connected(Arc::clone(connection));
        self.changed.notify_waiters();
        Ok(())
    }

    pub(super) fn disconnect(&self, current: Option<&Arc<Connection>>) -> bool {
        let mut state = self.state.lock().expect("Max recovery state");
        if let Some(current) = current {
            let is_current = state
                .connection()
                .is_some_and(|connection| Arc::ptr_eq(connection, current));
            if !is_current {
                return false;
            }
        }
        if matches!(*state, State::Stopped) {
            return current.is_none();
        }
        if let Some(connection) = state.connection() {
            connection.cancel();
        }
        *state = State::Disconnected(None);
        self.changed.notify_waiters();
        true
    }

    pub(super) async fn current(&self) -> Result<Arc<Connection>> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let state = self.state.lock().expect("Max recovery state");
                match &*state {
                    State::Connected(connection) => return Ok(Arc::clone(connection)),
                    State::Stopped => return Err(Error::ConnectionClosed),
                    State::Disconnected(_) => {}
                }
            }
            notified.await;
        }
    }

    #[cfg(test)]
    pub(super) fn is_connected(&self) -> bool {
        matches!(
            *self.state.lock().expect("Max recovery state"),
            State::Connected(_)
        )
    }

    pub(super) fn stop(&self) {
        let mut state = self.state.lock().expect("Max recovery state");
        if let Some(connection) = state.connection() {
            connection.cancel();
        }
        *state = State::Stopped;
        self.shutdown.cancel();
        self.changed.notify_waiters();
    }
}

pub(super) struct Backoff {
    delay: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self {
            delay: INITIAL_RECONNECT_DELAY,
        }
    }
}

impl Backoff {
    pub(super) fn next_delay(&mut self) -> Duration {
        let delay = self.delay;
        self.delay = self.delay.saturating_mul(2).min(MAX_RECONNECT_DELAY);
        delay
    }

    pub(super) fn reset(&mut self) {
        self.delay = INITIAL_RECONNECT_DELAY;
    }

    pub(super) fn reset_if_healthy(&mut self, connected_for: Duration) {
        if connected_for >= HEALTHY_CONNECTION_INTERVAL {
            self.reset();
        }
    }
}

pub(super) fn is_transport_failure(error: &Error) -> bool {
    matches!(
        error,
        Error::WebSocket(_)
            | Error::WebSocketConnectTimeout
            | Error::Timeout(_)
            | Error::DuplicateSequence(_)
            | Error::ConnectionClosed
    )
}

pub(super) fn should_retry_connection(error: &Error) -> bool {
    if is_transport_failure(error) {
        return true;
    }

    match error {
        Error::Http(source) | Error::CaptchaSolverUnavailable { source, .. } => {
            crate::error::is_transient_http_error(source)
        }
        Error::Io(source) => is_transient_io_error(source),
        Error::CaptchaTimeout { .. } | Error::TelegramUnavailable(_) => true,
        _ => false,
    }
}

fn is_transient_io_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::ConnectionRefused
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::NotConnected
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::Interrupted
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::UnexpectedEof
    )
}
