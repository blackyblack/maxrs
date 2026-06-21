# maxrs

`maxrs` is an unofficial asynchronous Rust client for the Max messenger web
WebSocket API at `wss://ws-api.oneme.ru/websocket`.

The crate is a proof of concept for the parts of the reverse-engineered web
protocol that are useful for a minimal custom client: SMS authentication,
session-token login, incoming message notifications, typing notifications, text
messages, file upload, and optional captcha solving for the current web auth
flow.

This project is not affiliated with Max or VK. The internal API can change
without notice.

## Features

- WebSocket connection and protocol request/response correlation
- SMS authentication and re-login with a saved session token
- Auth captcha preflight and optional `max_captcha_solver` integration
- Incoming message channel for server-pushed `NOTIF_MESSAGE` frames
- Text messages, file messages, typing notifications, and keepalive pings

The session token is kept in memory by the library. Store it yourself if your
application needs to log in again without requesting another SMS code.

## Prerequisites

Install a current stable Rust toolchain from <https://rustup.rs/>.

SMS login talks to the real Max service, so you need a phone number that can
receive the verification SMS.

Captcha solving is optional, but Max may require it before sending an SMS. For
the ready-to-use helper, run
[`blackyblack/max_captcha_solver`](https://github.com/blackyblack/max_captcha_solver)
and configure the env variables below.

## Installation

Clone this repository and run the included example:

```bash
cargo run --example cli
```

## Configuration

The example CLI loads `.env` from the current directory before reading the
process environment. Copy `.env.template` to `.env` and fill only the values you
need. Empty values in `.env.template` mean "use the code default" unless noted.

`MAX_SESSION_TOKEN`

Default: unset.

Saved Max session token. When set, the CLI logs in with this token and skips SMS
auth.

`MAX_PHONE`

Default: unset.

Phone number used for SMS auth when `MAX_SESSION_TOKEN` is unset or rejected.

`MAX_PASSWORD`

Default: unset.

Sign-in password used only when Max requires a password challenge after the SMS
code.

`MAX_OPERATOR_CHANNEL`

Default: `cli`.

SMS code entry channel. Use `cli`, `telegram`, or `none`. If set to
`telegram`, both `MAX_TELEGRAM_BOT_TOKEN` and `MAX_TELEGRAM_CHAT_ID` must be
configured.

`MAX_TELEGRAM_BOT_TOKEN`

Default: unset.

Telegram bot token used when `MAX_OPERATOR_CHANNEL=telegram`.

`MAX_TELEGRAM_CHAT_ID`

Default: unset.

Telegram chat id where SMS prompts are sent when `MAX_OPERATOR_CHANNEL=telegram`.

`MAX_TELEGRAM_POLL_TIMEOUT_SECS`

Default: `300`.

Maximum time to wait for a Telegram SMS-code reply.

`MAX_SOLVER_URL`

Default: `http://127.0.0.1:3000`.

Base URL of the `max_captcha_solver` solve API. The helper posts captcha
challenges to `POST /solve` on this service when Max requires auth captcha. If
Max asks for captcha and this service is not running or not reachable, login
fails with a captcha solver configuration error.

`MAX_CALLBACK_BIND`

Default: `127.0.0.1:3002`.

Local address used by the built-in callback receiver. The receiver serves
`POST /captcha-callback` and forwards solver callbacks to the pending
captcha challenge.

`MAX_CALLBACK_URL_BASE`

Default: unset, which becomes `http://<bound callback address>/captcha-callback`.

Public base URL sent to `max_captcha_solver` as the callback target. Use this
when the solver cannot reach the callback receiver through the bind address.
The value may contain `{port}`, which is replaced with the actual callback
server port.

`RUST_LOG`

Default in the CLI: `maxrs=info`.

Tracing filter used by `tracing_subscriber`. Examples: `debug`,
`maxrs=debug`, or `maxrs=info,hyper=warn`.

## Captcha Solver

`maxrs` can work without a solver if Max does not require captcha. If captcha is
required, use
[`blackyblack/max_captcha_solver`](https://github.com/blackyblack/max_captcha_solver).
Its solve API listens on `127.0.0.1:3000` by default and its operator UI listens
on `0.0.0.0:3001` by default.

For a local solver process running on the host, the defaults are enough:

```env
MAX_SOLVER_URL=
MAX_CALLBACK_BIND=
MAX_CALLBACK_URL_BASE=
```

For a containerized solver, publish the solver ports and make the callback URL
point from the container back to the host:

```env
MAX_SOLVER_URL=http://127.0.0.1:3000
MAX_CALLBACK_BIND=0.0.0.0:3002
MAX_CALLBACK_URL_BASE=http://host.docker.internal:3002
```

On Linux, Docker may not define `host.docker.internal` automatically. Add a
host-gateway mapping to the solver container or use an address that the
container can route to on the host. The solve API should be reachable from
`maxrs`, and the callback URL should be reachable from the solver container.

## Usage

```rust
use maxrs::client::{LoginConfig, MaxClient};
use maxrs::models::MaxMessage;

#[tokio::main]
async fn main() -> maxrs::error::Result<()> {
    let login_config = LoginConfig::from_env()?;

    let (client, mut messages) = MaxClient::connect().await?;

    tokio::spawn(async move {
        while let Some(msg) = messages.recv().await {
            println!("[chat {}] {}: {}", msg.chat_id, msg.sender, msg.text);
        }
    });

    let session = client.login(login_config).await?;
    println!("session token: {}", session.token);

    client.send_typing(123456).await?;
    client.send_text(123456, MaxMessage::new("Hello from Rust!")).await?;
    client.send_file(123456, "report.pdf", "Here is the report").await?;

    Ok(())
}
```

`MaxClient::login` first tries `MAX_SESSION_TOKEN` when configured. If that
token is missing or rejected by Max, it requests a fresh SMS code for
`MAX_PHONE`. If Max requires a sign-in password after the SMS code, it uses
`MAX_PASSWORD`. Configure SMS code entry with `MAX_OPERATOR_CHANNEL=cli`,
`MAX_OPERATOR_CHANNEL=telegram`, or `MAX_OPERATOR_CHANNEL=none`. Telegram mode
uses `MAX_TELEGRAM_BOT_TOKEN` and `MAX_TELEGRAM_CHAT_ID`.

`MaxMessage` supports typed formatter elements for bold, italic, underline,
strikethrough, inline code, code blocks, links, headings, and quotes through
`MessageElement`.

## Protocol Notes

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for protocol details.

## Example CLI

```bash
cargo run --example cli
```

The CLI connects, logs in with `MAX_SESSION_TOKEN` or interactive auth, then
listens for incoming messages until Ctrl-C. It does not print login payload
details such as chats or contacts, and it does not send messages.

## License

MIT
