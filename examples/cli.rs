//! Interactive demo of the `maxrs` client.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cli
//! ```
//!
//! It performs an SMS login, prints incoming messages as they arrive, and lets
//! you send a text message (and a typing notification) from the terminal.
//!
//! NOTE: this talks to the real Max servers, so it needs a valid phone number
//! that can receive the SMS code.

use std::io::Write;

use maxrs::{Error, MaxClient};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "maxrs=info".into()),
        )
        .init();

    let (client, mut messages) = MaxClient::connect().await?;
    println!("Connected to Max.");

    // Print incoming messages in the background.
    tokio::spawn(async move {
        while let Some(msg) = messages.recv().await {
            println!(
                "\n<< [chat {}] from {}: {}",
                msg.chat_id, msg.sender, msg.text
            );
            print!("> ");
            let _ = std::io::stdout().flush();
        }
    });

    let phone = prompt("Phone number (e.g. +79990000000): ").await?;
    let sms_token = match client.request_sms_code(phone.trim()).await {
        Ok(token) => token,
        Err(Error::CaptchaRequired { link }) => {
            eprintln!("SMS login requires a captcha challenge first.");
            eprintln!("Captcha URL: {link}");
            eprintln!(
                "This terminal example cannot render the VK captcha widget. \
                 Use a saved session token with login_with_token, or complete \
                 this captcha in a browser-capable integration and call \
                 request_sms_code_with_captcha_token."
            );
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    println!("SMS code requested.");

    let code = prompt("Enter the SMS code: ").await?;
    let session = client.verify_sms_code(&sms_token, code.trim()).await?;
    println!("Logged in. Session token (keep it safe): {}", session.token);

    let chat_id: i64 = prompt("Chat id to message: ").await?.trim().parse()?;
    let text = prompt("Message text: ").await?;

    client.send_typing(chat_id).await?;
    client.send_text(chat_id, text.trim()).await?;
    println!("Sent. Listening for incoming messages (Ctrl-C to quit)...");
    print!("> ");
    std::io::stdout().flush()?;

    // Keep the process alive so the listener task keeps receiving.
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
