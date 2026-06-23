//! Minimal interactive demo of the `maxrs` client.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cli
//! ```
//!
//! Configuration:
//! - `.max_session_token`: optional saved session token file.
//! - `MAX_PHONE`: phone number used when the saved token is missing or expired.
//! - `MAX_PASSWORD`: optional sign-in password used when Max requires it after SMS.
//! - `MAX_OPERATOR_CHANNEL`: `cli`, `telegram`, or `none` for SMS code entry.
//!   Defaults to `none`; set `cli` for terminal prompts.
//! - `MAX_TELEGRAM_BOT_TOKEN` and `MAX_TELEGRAM_CHAT_ID`: required for Telegram.

use maxrs::auth::{LoginConfig, SESSION_TOKEN_FILE};
use maxrs::client::MaxClient;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "maxrs=info".into()),
        )
        .init();

    let login_config = LoginConfig::from_env()?;
    let (client, mut messages) = MaxClient::new(login_config)?;
    let session = client.connect().await?;
    println!("Logged in. Session token is stored in {SESSION_TOKEN_FILE} when refreshed.");
    tracing::debug!(token = %session.token, "logged in to Max");
    println!("Listening for incoming messages (Ctrl-C to quit)...");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            msg = messages.recv() => {
                let Some(msg) = msg else {
                    tracing::warn!("incoming message stream closed");
                    break;
                };
                let text = if msg.text.trim().is_empty() {
                    "[non-text message]"
                } else {
                    msg.text.trim()
                };
                println!("\n<< {text}");
            }
        }
    }

    Ok(())
}
