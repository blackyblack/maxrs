//! Interactive demo of the `maxrs` client.
//!
//! Run with:
//!
//! ```text
//! cargo run --example cli
//! ```
//!
//! It performs an SMS login, solves auth captcha challenges through
//! `max_captcha_solver`, prints incoming messages as they arrive, and lets you
//! send a text message (and a typing notification) from the terminal.
//!
//! NOTE: this talks to the real Max servers, so it needs a valid phone number
//! that can receive the SMS code.

use std::env;
use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use maxrs::captcha::{CaptchaSolver, CaptchaSolverConfig, HttpServer, HttpServerConfig};
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
    let sms_token = request_sms_code(&client, phone.trim()).await?;
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

async fn request_sms_code(
    client: &MaxClient,
    phone: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    match client.request_sms_code(phone).await {
        Ok(token) => Ok(token),
        Err(Error::CaptchaRequired { link }) => {
            println!("SMS login requires captcha; sending it to the solver.");
            println!("Captcha URL: {link}");

            let solver_url =
                env::var("MAX_SOLVER_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".into());
            let callback_bind =
                env::var("MAX_CALLBACK_BIND").unwrap_or_else(|_| "127.0.0.1:3002".into());

            let server = HttpServer::bind(HttpServerConfig::new(callback_bind)).await?;
            let callback_addr = server.local_addr()?;
            let callback_url = build_callback_url(callback_addr);
            let solver = Arc::new(CaptchaSolver::new(CaptchaSolverConfig::new(
                solver_url.clone(),
                callback_url.clone(),
            ))?);

            tokio::spawn(server.with_captcha_solver(Arc::clone(&solver)).serve());

            println!("Captcha solver: {solver_url}");
            println!("Captcha callback: {callback_url}");
            println!("Waiting for captcha callback...");

            let captcha_token = solver.solve(&link).await?;
            println!("Captcha solved; requesting SMS code.");

            Ok(client
                .request_sms_code_with_captcha_token(phone, &captcha_token)
                .await?)
        }
        Err(err) => Err(err.into()),
    }
}

fn build_callback_url(callback_addr: SocketAddr) -> String {
    match env::var("MAX_CALLBACK_URL_BASE") {
        Ok(base) => {
            let base = base.replace("{port}", &callback_addr.port().to_string());
            format!("{}/captcha-callback", base.trim_end_matches('/'))
        }
        Err(_) => format!("http://{callback_addr}/captcha-callback"),
    }
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
