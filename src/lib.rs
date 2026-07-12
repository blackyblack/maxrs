//! `maxrs` — a small asynchronous Rust client for the **Max** messenger,
//! talking to the web WebSocket API at `wss://ws-api.oneme.ru/websocket`.
//!
//! # Example
//!
//! ```no_run
//! use maxrs::auth::LoginConfig;
//! use maxrs::client::{ChatHandler, MaxClient, ServeConfig};
//! use maxrs::models::{IncomingMessage, MaxMessage};
//!
//! struct Handler;
//!
//! impl ChatHandler for Handler {
//!     async fn on_message(
//!         &self,
//!         client: &MaxClient,
//!         msg: IncomingMessage,
//!     ) -> Result<(), maxrs::error::Error> {
//!         println!("[{}] {}", msg.chat_id, msg.text);
//!         client.send_text(msg.chat_id, MaxMessage::new("Received")).await
//!     }
//! }
//!
//! # async fn run() -> maxrs::error::Result<()> {
//! let client = MaxClient::new(LoginConfig::from_env()?)?;
//! let (session, connected) = client.connect(Handler, ServeConfig::default()).await?;
//!
//! client.send_text(123, MaxMessage::new("Hello from Rust!")).await?;
//! connected.run().await;
//! # let _ = session;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod client;
pub mod error;
pub mod models;
pub mod protocol;
