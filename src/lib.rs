//! `maxrs` — a small asynchronous Rust client for the **Max** messenger,
//! talking to the web WebSocket API at `wss://ws-api.oneme.ru/websocket`.
//!
//! # Example
//!
//! ```no_run
//! use maxrs::auth::LoginConfig;
//! use maxrs::client::{ChatHandler, MaxClient};
//! use maxrs::models::{IncomingMessage, MaxMessage};
//!
//! struct Handler {
//!     client: MaxClient,
//! }
//!
//! impl ChatHandler for Handler {
//!     async fn on_message(&self, msg: IncomingMessage) -> Result<(), maxrs::error::Error> {
//!         println!("[{}] {}", msg.chat_id, msg.text);
//!         self.client
//!             .send_text(msg.chat_id, MaxMessage::new("Received"))
//!             .await
//!     }
//! }
//!
//! # async fn run() -> maxrs::error::Result<()> {
//! let client = MaxClient::new(LoginConfig::from_env()?)?;
//! // This runner reconnects the same client after connection failures.
//! client
//!     .run(Handler {
//!         client: client.clone(),
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod client;
pub mod error;
pub mod models;
pub mod protocol;
