use thiserror::Error;
use tokio_tungstenite::tungstenite;

/// Errors returned by the Max client.
#[derive(Debug, Error)]
pub enum Error {
    #[error("websocket error: {0}")]
    WebSocket(#[source] Box<tungstenite::Error>),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The server replied with an error frame (`cmd == 3`).
    #[error("server error (opcode {opcode}): {message}")]
    Server { opcode: u16, message: String },

    /// Captcha solving was requested, but no solver service URL is configured.
    #[error("captcha solver is not configured; set MAX_SOLVER_URL to a running max_captcha_solver service or disable captcha solving explicitly")]
    CaptchaSolverDisabled,

    /// Captcha solver service could not be reached or rejected the solve request.
    #[error("captcha solver is not available at {solver_url}; start max_captcha_solver or set MAX_SOLVER_URL to a reachable solver service: {source}")]
    CaptchaSolverUnavailable {
        solver_url: String,
        #[source]
        source: reqwest::Error,
    },

    /// Login needs an SMS code but no operator channel is configured.
    #[error("no operator channel is configured for SMS code entry")]
    NoOperatorChannel,

    /// Telegram was selected as operator channel but required env vars are missing or invalid.
    #[error("telegram operator channel is requested but configuration is missing or invalid: {0}")]
    TelegramConfigMissing(String),

    /// Login needs the sign-in password but no password is configured.
    #[error("max login requires a password but MAX_PASSWORD is not configured")]
    PasswordRequired,

    /// Captcha solver did not return before the challenge timeout.
    #[error("timed out waiting for captcha challenge {challenge_id}")]
    CaptchaTimeout { challenge_id: String },

    /// Captcha solver reported failure or an invalid callback.
    #[error("captcha solving failed: {0}")]
    CaptchaFailed(String),

    /// Callback was received for a challenge that is not pending in memory.
    #[error("unknown captcha challenge: {challenge_id}")]
    UnknownCaptchaChallenge { challenge_id: String },

    /// A response did not arrive within the configured timeout.
    #[error("timed out waiting for response to opcode {0}")]
    Timeout(u16),

    /// The connection was closed while a request was in flight.
    #[error("connection closed")]
    ConnectionClosed,

    /// A response payload was missing an expected field.
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),

    /// The client is used before a successful login.
    #[error("not authenticated")]
    NotAuthenticated,

    /// Telegram Bot API returned an error response.
    #[error("telegram operator channel failed: {0}")]
    Telegram(String),
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<tungstenite::Error> for Error {
    fn from(err: tungstenite::Error) -> Self {
        Self::WebSocket(Box::new(err))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as StdError;

    #[test]
    fn websocket_error_preserves_source() {
        let err = Error::from(tungstenite::Error::Io(std::io::Error::other("boom")));

        assert!(StdError::source(&err).is_some());
    }
}
