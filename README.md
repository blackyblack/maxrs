# maxrs

A small **proof-of-concept** async Rust client for the [Max](https://max.ru)
messenger (internal code name *OneMe*), talking to the web WebSocket API at
`wss://ws-api.oneme.ru/websocket`.

All existing community clients are written in Python (PyMax, vkmax, MadMax,
maxbridge-client, ...). This is a focused Rust port of the slice of the protocol
needed to build a minimal custom client.

## Features

- SMS authentication (request code → verify code → login)
- Re-login with an in-memory session token (no SMS)
- Send text messages
- Send files (`FILE_UPLOAD` → HTTP upload → attach)
- Typing notifications
- Asynchronous receiving of incoming messages
- Background keepalive

The session token is kept **in memory only** (no SQLite/persistence).

This is a proof of concept, not a production-ready library: only a handful of
opcodes are implemented and error handling is intentionally simple.

## Protocol

Every frame is JSON over a single WebSocket connection:

```json
{ "ver": 11, "cmd": 0, "seq": 1, "opcode": 6, "payload": { } }
```

- `cmd`: `0` request, `1` response, `3` error
- `seq`: request/response correlation id
- `opcode`: operation code (see [`protocol::opcode`](src/protocol.rs))

The handshake/auth sequence is:

```
SESSION_INIT (6)
  → AUTH_REQUEST (17, phone)   → sms token
  → AUTH (18, sms token, code) → session token
  → LOGIN (19, session token)  → profile + chats
```

Incoming messages arrive as server-initiated `NOTIF_MESSAGE` (128) frames and are
forwarded to an async channel.

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for the full opcode/flow reference and
the upstream sources it was derived from.

## Usage

```rust
use maxrs::MaxClient;

#[tokio::main]
async fn main() -> maxrs::Result<()> {
    let (client, mut messages) = MaxClient::connect().await?;

    // Receive incoming messages asynchronously.
    tokio::spawn(async move {
        while let Some(msg) = messages.recv().await {
            println!("[chat {}] {}: {}", msg.chat_id, msg.sender, msg.text);
        }
    });

    // SMS login.
    let sms_token = client.request_sms_code("+79990000000").await?;
    // ...read the code the user received...
    let session = client.verify_sms_code(&sms_token, "12345").await?;
    println!("session token: {}", session.token);

    // Send things.
    client.send_typing(123456).await?;
    client.send_text(123456, "Hello from Rust!").await?;
    client.send_file(123456, "report.pdf", "Here is the report").await?;

    Ok(())
}
```

Later, skip SMS by reusing the token:

```rust
let session = client.login_with_token(&saved_token).await?;
```

## Running the demo

```bash
cargo run --example cli
```

It performs an interactive SMS login, prints incoming messages, and sends a
message you type. It talks to the **real** Max servers, so you need a phone
number that can receive the SMS code.

## Disclaimer

This project is unofficial and not affiliated with Max/VK. It relies on a
reverse-engineered internal API that may change at any time. Use responsibly and
at your own risk.

## License

MIT
