//! Element / formatting probe for diagnosing MSG_SEND (opcode 64) failures.
//!
//! The bot built on top of `maxrs` sends a plain-text welcome fine, but a
//! search-results message — which carries formatter `elements` (bold spans and
//! links) — was rejected by the server. This example sends a battery of
//! messages that isolate each element kind so you can see, against a real chat,
//! exactly which payload shapes the server accepts and which it rejects.
//!
//! Run with:
//!
//! ```text
//! # list your chats (so you can pick a chat id), then exit:
//! cargo run --example element_probe
//!
//! # send the probes to a chat:
//! MAX_PROBE_CHAT_ID=<chat id> cargo run --example element_probe
//! ```
//!
//! Configuration (same as the `cli` example, plus the chat id):
//! - `.max_session_token`: optional saved session token file.
//! - `MAX_PHONE` / `MAX_PASSWORD` / `MAX_OPERATOR_CHANNEL`: login fallbacks.
//! - `MAX_PROBE_CHAT_ID`: numeric id of an existing chat to send the probes to.
//!   When unset, the example just prints the chats from your login payload.

use std::time::Duration;

use maxrs::auth::LoginConfig;
use maxrs::client::MaxClient;
use maxrs::models::{MaxMessage, MessageElement};

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

    let probe_chat_id: Option<i64> = match std::env::var("MAX_PROBE_CHAT_ID") {
        Ok(value) => Some(
            value
                .trim()
                .parse()
                .map_err(|_| "MAX_PROBE_CHAT_ID must be a number")?,
        ),
        Err(_) => None,
    };

    let config = LoginConfig::from_env()?;
    let (client, _messages) = MaxClient::new(config)?;
    let session = client.connect().await?;

    // Show the chats from the login payload so a chat id is easy to find.
    let chats = session.chats();
    if chats.is_empty() {
        println!("No chats found in the login payload.");
    } else {
        println!("Available chats ({}):", chats.len());
        for chat in &chats {
            let title = if chat.title.is_empty() {
                "(no title)"
            } else {
                chat.title.as_str()
            };
            println!("  {:>16}  [{}] {}", chat.id, chat.chat_type, title);
        }
    }

    let Some(chat_id) = probe_chat_id else {
        println!("\nSet MAX_PROBE_CHAT_ID=<chat id> to send the formatting probes to a chat.");
        return Ok(());
    };

    // Each probe is a labelled message, ordered from the simplest payload
    // (plain text) to the shapes that were suspected of triggering opcode 64.
    let probes: Vec<(&str, MaxMessage)> = vec![
        ("plain text (control)", MaxMessage::new("probe: plain text")),
        (
            "STRONG span",
            MaxMessage::with_elements("probe: bold word", vec![MessageElement::strong(7, 4)]),
        ),
        (
            "EMPHASIZED span",
            MaxMessage::with_elements("probe: italic word", vec![MessageElement::emphasized(7, 6)]),
        ),
        (
            "MONOSPACED span",
            MaxMessage::with_elements("probe: mono word", vec![MessageElement::monospaced(7, 4)]),
        ),
        (
            "LINK, full https url",
            MaxMessage::with_elements(
                "probe: https://example.com",
                vec![MessageElement::link(7, 19, "https://example.com")],
            ),
        ),
        (
            "LINK, relative slash-command url (bot uses these)",
            MaxMessage::with_elements("probe: /b_42", vec![MessageElement::link(7, 5, "/b_42")]),
        ),
        (
            "combined: STRONG + LINK (shape of a search result line)",
            MaxMessage::with_elements(
                "1. Title\nDetails: https://example.com/b/42",
                vec![
                    MessageElement::strong(3, 5),
                    MessageElement::link(18, 24, "https://example.com/b/42"),
                ],
            ),
        ),
        (
            "emoji + STRONG (checks from/length unit: utf-16 vs chars)",
            MaxMessage::with_elements("📚 bold", vec![MessageElement::strong(3, 4)]),
        ),
    ];

    // A server rejection no longer tears down the connection, so all probes run
    // on the same session and each result is independent.
    for (label, message) in probes {
        match client.send_text(chat_id, message).await {
            Ok(()) => println!("[ OK  ] {label}"),
            Err(err) => println!("[FAIL ] {label}: {err}"),
        }
        // Be gentle with the server between probes.
        tokio::time::sleep(Duration::from_millis(750)).await;
    }

    println!(
        "\nDone. Compare which probes the server accepted vs rejected to pin \
         down the element shape that causes any opcode-64 error."
    );
    Ok(())
}
