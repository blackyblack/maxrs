# maxrs

`maxrs` is an unofficial asynchronous Rust client for the Max messenger.

It supports SMS/session-token login, incoming messages, typing notifications,
text messages, file upload, automatic reconnects, and optional captcha solving.

This project is not affiliated with Max or VK. The internal API can change
without notice.

## Features

- WebSocket login and automatic reconnects
- SMS authentication and saved session-token login
- Auth captcha preflight and optional `max_captcha_solver` integration
- Incoming message channel
- Text messages, file messages, typing notifications, and keepalive pings

## Prerequisites

- Current stable Rust toolchain: <https://rustup.rs/>
- Phone number that can receive a Max verification SMS
- Optional captcha solver: [`blackyblack/max_captcha_solver`](https://github.com/blackyblack/max_captcha_solver)

## Installation

```bash
cargo run --example cli
```

## Configuration

The example CLI loads `.env` before reading the process environment. Copy
`.env.template` to `.env` and fill only the values you need.

- `.max_session_token`: optional saved token file. One token line; refreshed
  after SMS/password login.
- `MAX_PHONE`: required when no valid saved token exists.
- `MAX_PASSWORD`: used only if Max asks for a password challenge after SMS.
- `MAX_OPERATOR_CHANNEL`: `none` by default. Use `cli` for local SMS entry or
  `telegram` for Telegram prompts.
- `MAX_TELEGRAM_BOT_TOKEN`, `MAX_TELEGRAM_CHAT_ID`: required with
  `MAX_OPERATOR_CHANNEL=telegram`.
- `MAX_TELEGRAM_POLL_TIMEOUT_SECS`: Telegram SMS reply timeout. Default: `300`.
- `MAX_SOLVER_URL`: captcha solver API URL. Default: `http://127.0.0.1:3000`.
  Empty disables the solver.
- `MAX_CALLBACK_BIND`: captcha callback bind address. Default: `127.0.0.1:3002`.
- `MAX_CALLBACK_URL_BASE`: public callback base for the solver. May contain
  `{port}`.
- `RUST_LOG`: tracing filter. CLI default: `maxrs=info`.

## Captcha Solver

Max may require captcha before SMS auth. For a local solver running on the host,
the defaults are enough:

```env
MAX_SOLVER_URL=
MAX_CALLBACK_BIND=
MAX_CALLBACK_URL_BASE=
```

For a containerized solver, publish the solver ports and point callbacks back to
the host:

```env
MAX_SOLVER_URL=http://127.0.0.1:3000
MAX_CALLBACK_BIND=0.0.0.0:3002
MAX_CALLBACK_URL_BASE=http://host.docker.internal:3002
```

On Linux, Docker may need a `host-gateway` mapping for `host.docker.internal`.
The solve API must be reachable from `maxrs`; the callback URL must be reachable
from the solver.

## Usage

Run the CLI example after configuring the needed environment variables:

```bash
cargo run --example cli
```

The CLI logs in to Max and listens for incoming messages until Ctrl-C.
Reconnects reuse the stored login flow, so expired saved tokens can fall back to captcha/SMS/password auth.

## Protocol Notes

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for protocol details.

## License

MIT
