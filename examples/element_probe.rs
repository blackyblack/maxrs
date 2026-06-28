//! Element / formatting probe for diagnosing MSG_SEND (opcode 64) failures.
//!
//! The bot built on top of `maxrs` sends a plain-text welcome fine, but a
//! search-results message — which carries formatter `elements` (bold spans and
//! links) — is rejected by the server with an `Error::Server { opcode: 64 }`.
//! Because we cannot test against a live account from CI, this example sends a
//! battery of messages that isolate each element kind so you can see, against a
//! real chat, exactly which payload shapes the server accepts and which it
//! rejects.
//!
//! Run with:
//!
//! ```text
//! MAX_PROBE_CHAT_ID=<chat id> cargo run --example element_probe
//! ```
//!
//! Configuration (same as the `cli` example, plus the chat id):
//! - `.max_session_token`: optional saved session token file.
//! - `MAX_PHONE` / `MAX_PASSWORD` / `MAX_OPERATOR_CHANNEL`: login fallbacks.
//! - `MAX_PROBE_CHAT_ID`: numeric id of an existing chat to send the probes to.
//!   You can grab it by logging incoming messages with the `cli` example.
//!
//! Each probe is sent on a freshly (re)connected client. This is deliberate:
//! `MaxClient::invoke` currently disconnects the whole client on any error
//! (see the TODO in `src/client/mod.rs`), so without reconnecting, the first
//! rejected probe would make every later probe fail with `ConnectionClosed`
//! and hide the real per-element result.

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

    let chat_id: i64 = std::env::var("MAX_PROBE_CHAT_ID")
        .map_err(|_| "set MAX_PROBE_CHAT_ID to the numeric id of a chat to probe")?
        .trim()
        .parse()
        .map_err(|_| "MAX_PROBE_CHAT_ID must be a number")?;

    // Each probe is a labelled message. Ordering goes from the simplest payload
    // (plain text, known-good) to the ones suspected of triggering opcode 64.
    let probes: Vec<(&str, MaxMessage)> = vec![
        (
            "plain text (control, expected OK)",
            MaxMessage::new("probe: plain text"),
        ),
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
                "1. Title\nDetails: /b_42",
                vec![
                    MessageElement::strong(3, 5),
                    MessageElement::link(18, 5, "/b_42"),
                ],
            ),
        ),
        (
            "emoji + STRONG (checks from/length unit: utf-16 vs chars)",
            MaxMessage::with_elements("📚 bold", vec![MessageElement::strong(3, 4)]),
        ),
    ];

    let config = LoginConfig::from_env()?;
    let (client, _messages) = MaxClient::new(config)?;

    for (label, message) in probes {
        // Reconnect before every probe so a previous rejection (which currently
        // tears down the connection) does not contaminate this result.
        if let Err(err) = client.connect().await {
            eprintln!("[SKIP ] {label}: could not (re)connect: {err}");
            continue;
        }

        match client.send_text(chat_id, message).await {
            Ok(()) => println!("[ OK  ] {label}"),
            Err(err) => println!("[FAIL ] {label}: {err}"),
        }

        // Be gentle with the server between probes.
        tokio::time::sleep(Duration::from_millis(750)).await;
    }

    println!(
        "\nDone. Compare which probes the server accepted vs rejected to pin \
         down the element shape that causes the opcode-64 error."
    );
    Ok(())
}
