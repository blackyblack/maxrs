//! `maxrs` — a small asynchronous Rust client for the **Max** messenger,
//! talking to the web WebSocket API at `wss://ws-api.oneme.ru/websocket`.
//!
//! # Example
//!
//! ```no_run
//! use maxrs::client::{LoginConfig, MaxClient};
//! use maxrs::models::MaxMessage;
//!
//! # async fn run() -> maxrs::error::Result<()> {
//! let (client, mut messages) = MaxClient::connect().await?;
//!
//! tokio::spawn(async move {
//!     while let Some(msg) = messages.recv().await {
//!         println!("[{}] {}", msg.chat_id, msg.text);
//!     }
//! });
//!
//! let session = client.login(LoginConfig::from_env()?).await?;
//! client.send_text(123, MaxMessage::new("Hello from Rust!")).await?;
//! # let _ = session;
//! # Ok(())
//! # }
//! ```

mod auth;
pub mod captcha;
pub mod client;
pub mod error;
pub mod models;
mod operator_channels;
pub mod protocol;
