//! Minimal interactive demo of the `maxrs` client.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cli
//! ```
//!
//! The demo reuses `MAX_SESSION_TOKEN` when it is present. Otherwise it
//! performs SMS login and can solve auth captcha challenges through
//! `max_captcha_solver`.
//!
//! NOTE: this talks to the real Max servers, so SMS login needs a valid phone
//! number that can receive the SMS code.

use std::io::Write;

use maxrs::auth::{session_token_from_env, AuthCaptchaConfig};
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
            print!("> ");
            let _ = std::io::stdout().flush();
        }
    });

    let _session = match session_token_from_env() {
        Some(token) => {
            let session = client.login_with_token(&token).await?;
            println!("Logged in with MAX_SESSION_TOKEN.");
            session
        }
        None => {
            let phone = prompt("Phone number (e.g. +79990000000): ").await?;
            let captcha_config = AuthCaptchaConfig::from_env();
            let sms_token = client
                .request_sms_code_with_auth_captcha(phone.trim(), &captcha_config)
                .await?;
            println!("SMS code requested.");

            let code = prompt("Enter the SMS code: ").await?;
            let session = client.verify_sms_code(&sms_token, code.trim()).await?;
            println!("Logged in. Session token (keep it safe): {}", session.token);
            session
        }
    };

    println!("Listening for incoming messages (Ctrl-C to quit)...");
    print!("> ");
    std::io::stdout().flush()?;

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn prompt(label: &str) -> std::io::Result<String> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let mut stdout = tokio::io::stdout();
    stdout.write_all(label.as_bytes()).await?;
    stdout.flush().await?;

    let mut line = String::new();
    let mut reader = BufReader::new(tokio::io::stdin());
    reader.read_line(&mut line).await?;
    Ok(line)
}
