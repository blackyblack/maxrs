//! Minimal interactive demo of the `maxrs` client.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cli
//! ```
//!
//! Configuration:
//! - `MAX_SESSION_TOKEN`: optional saved session token.
//! - `MAX_PHONE`: phone number used when the saved token is missing or expired.
//! - `MAX_OPERATOR_CHANNEL`: `cli`, `telegram`, or `none` for SMS code entry.
//! - `MAX_TELEGRAM_BOT_TOKEN` and `MAX_TELEGRAM_CHAT_ID`: required for Telegram.

use maxrs::auth::LoginConfig;
use maxrs::client::MaxClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "maxrs=info".into()),
        )
        .init();

    let (client, mut messages) = MaxClient::connect().await?;
    println!("Connected to Max.");

    tokio::spawn(async move {
        while let Some(msg) = messages.recv().await {
            let text = if msg.text.trim().is_empty() {
                "[non-text message]"
            } else {
                msg.text.trim()
            };
            println!("\n<< {text}");
        }
    });

    let session = client.login(LoginConfig::from_env()).await?;
    println!("Logged in. Session token (keep it safe): {}", session.token);
    println!("Listening for incoming messages (Ctrl-C to quit)...");

    tokio::signal::ctrl_c().await?;
    Ok(())
}
