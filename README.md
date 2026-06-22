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
- SMS authentication and re-login with a saved session token file
- Auth captcha preflight and optional `max_captcha_solver` integration
- Incoming message channel for server-pushed `NOTIF_MESSAGE` frames
- Text messages, file messages, typing notifications, and keepalive pings

The library reads a saved session token from `.max_session_token` in the current
working directory and refreshes that file after successful SMS/password login.
If the file cannot be written, the active session keeps running and a warning is
written to the log.

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

Session token file: `.max_session_token`

Default: absent.

Saved Max session token used to skip SMS auth when valid. Create this file with a
single token line for easier local debugging, or let `maxrs` create/update it
after a successful SMS/password login. The file is ignored by git. If the token
is invalid or expired, login falls back to the normal auth flow, including
captcha, SMS code entry, and password challenge when Max requires them.

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

Run the CLI example after configuring the needed environment variables:

```bash
cargo run --example cli
```

The CLI logs in with `.max_session_token` when available. Otherwise it uses
`MAX_PHONE`, requests an SMS code through the configured operator channel, saves
the refreshed session token back to `.max_session_token`, and then listens for
incoming messages until Ctrl-C. If the WebSocket connection is lost, the CLI
reconnects and runs the same main login flow again so expired tokens fall back to
captcha/SMS/password auth automatically.

## Protocol Notes

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for protocol details.

## License

MIT
