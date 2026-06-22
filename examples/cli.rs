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
//! - `MAX_TELEGRAM_BOT_TOKEN` and `MAX_TELEGRAM_CHAT_ID`: required for Telegram.

use std::time::Duration;

use maxrs::client::{LoginConfig, MaxClient, SESSION_TOKEN_FILE};

const RECONNECT_DELAY: Duration = Duration::from_secs(5);

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

    loop {
        let login_config = LoginConfig::from_env()?;
        let (client, messages) = MaxClient::connect().await?;
        println!("Connected to Max.");

        let session = client.login(login_config).await?;
        println!("Logged in. Session token is stored in {SESSION_TOKEN_FILE} when refreshed.");
        tracing::debug!(token = %session.token, "logged in to Max");
        println!("Listening for incoming messages (Ctrl-C to quit)...");

        if listen_until_disconnect_or_shutdown(messages).await? {
            break;
        }

        tracing::warn!(
            delay_secs = RECONNECT_DELAY.as_secs(),
            "Max connection was lost; reconnecting through the main login flow"
        );
        tokio::time::sleep(RECONNECT_DELAY).await;
    }

    Ok(())
}

async fn listen_until_disconnect_or_shutdown(
    mut messages: tokio::sync::mpsc::UnboundedReceiver<maxrs::models::IncomingMessage>,
) -> Result<bool, std::io::Error> {
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => return Ok(true),
            msg = messages.recv() => {
                let Some(msg) = msg else {
                    return Ok(false);
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
}
