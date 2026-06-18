use thiserror::Error;

/// Errors returned by the Max client.
#[derive(Debug, Error)]
pub enum Error {
    #[error("websocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// The server replied with an error frame (`cmd == 3`).
    #[error("server error (opcode {opcode}): {message}")]
    Server { opcode: u16, message: String },

    /// SMS authentication requires completing a captcha challenge in a browser.
    #[error("captcha required before requesting SMS code: {link}")]
    CaptchaRequired { link: String },

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
}

pub type Result<T> = std::result::Result<T, Error>;
