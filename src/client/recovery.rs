use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use super::connection::{is_transport_failure, Connection};
use crate::error::{Error, Result};

const INITIAL_RECONNECT_DELAY: Duration = Duration::from_secs(1);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(120);
pub(super) const HEALTHY_CONNECTION_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
enum State {
    Offline,
    Ready(Arc<Connection>),
    Stopped,
}

pub(super) struct Recovery {
    state: watch::Sender<State>,
    pub(super) shutdown: CancellationToken,
}

impl Recovery {
    pub(super) fn new() -> Self {
        Self {
            state: watch::channel(State::Offline).0,
            shutdown: CancellationToken::new(),
        }
    }

    pub(super) fn begin_attempt(&self) -> Result<Arc<Connection>> {
        if self.shutdown.is_cancelled() {
            return Err(Error::ConnectionClosed);
        }
        Ok(Arc::new(Connection::new(self.shutdown.child_token())))
    }

    // Only the runner publishes readiness and takes a session offline.
    pub(super) fn connected(&self, connection: &Arc<Connection>) -> Result<()> {
        let published = self.state.send_if_modified(|state| {
            if matches!(state, State::Stopped) || connection.is_cancelled() {
                return false;
            }
            *state = State::Ready(Arc::clone(connection));
            true
        });
        if published {
            Ok(())
        } else {
            Err(Error::ConnectionClosed)
        }
    }

    pub(super) fn offline(&self) {
        self.state.send_if_modified(|state| {
            if matches!(state, State::Stopped) {
                return false;
            }
            *state = State::Offline;
            true
        });
    }

    pub(super) async fn current(&self) -> Result<Arc<Connection>> {
        let mut state = self.state.subscribe();
        loop {
            match state.borrow_and_update().clone() {
                State::Ready(connection) if !connection.is_cancelled() => return Ok(connection),
                State::Stopped => return Err(Error::ConnectionClosed),
                _ => {}
            }
            state.changed().await.map_err(|_| Error::ConnectionClosed)?;
        }
    }

    #[cfg(test)]
    pub(super) fn is_connected(&self) -> bool {
        matches!(&*self.state.borrow(), State::Ready(connection) if !connection.is_cancelled())
    }

    pub(super) fn stop(&self) {
        self.shutdown.cancel();
        self.state.send_replace(State::Stopped);
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
