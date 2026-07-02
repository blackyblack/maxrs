//! `maxrs` — a small asynchronous Rust client for the **Max** messenger,
//! talking to the web WebSocket API at `wss://ws-api.oneme.ru/websocket`.
//!
//! # Example
//!
//! ```no_run
//! use maxrs::auth::LoginConfig;
//! use maxrs::client::MaxClient;
//! use maxrs::models::MaxMessage;
//!
//! # async fn run() -> maxrs::error::Result<()> {
//! let client = MaxClient::new(LoginConfig::from_env()?)?;
//! let (session, mut messages) = client.connect().await?;
//!
//! tokio::spawn(async move {
//!     while let Some(msg) = messages.recv().await {
//!         println!("[{}] {}", msg.chat_id, msg.text);
//!     }
//! });
//!
//! client.send_text(123, MaxMessage::new("Hello from Rust!")).await?;
//! # let _ = session;
//! # Ok(())
//! # }
//! ```

pub mod auth;
pub mod client;
pub mod error;
pub mod models;
pub mod protocol;
