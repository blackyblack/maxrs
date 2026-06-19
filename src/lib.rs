//! `maxrs` — a small asynchronous Rust client for the **Max** messenger,
//! talking to the web WebSocket API at `wss://ws-api.oneme.ru/websocket`.
//!
//! # Example
//!
//! ```no_run
//! use maxrs::MaxClient;
//!
//! # async fn run() -> maxrs::Result<()> {
//! let (client, mut messages) = MaxClient::connect().await?;
//!
//! // Listen for incoming messages in the background.
//! tokio::spawn(async move {
//!     while let Some(msg) = messages.recv().await {
//!         println!("[{}] {}", msg.chat_id, msg.text);
//!     }
//! });
//!
//! let sms_token = client.request_sms_code("+79990000000").await?;
//! // ... read the SMS code from the user ...
//! let session = client.verify_sms_code(&sms_token, "12345").await?;
//!
//! client.send_text(123, "Hello from Rust!").await?;
//! # let _ = session;
//! # Ok(())
//! # }
//! ```

pub mod captcha;
mod client;
mod error;
pub mod http;
pub mod models;
pub mod protocol;

pub use captcha::{CaptchaCallback, CaptchaChallenge, CaptchaSolver, CaptchaSolverConfig};
pub use client::MaxClient;
pub use error::{Error, Result};
pub use http::{HttpServer, HttpServerConfig};
pub use models::{IncomingMessage, Session, UserAgent};
