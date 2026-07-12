# maxrs

`maxrs` is an unofficial asynchronous Rust client for the Max messenger. It
supports SMS/session-token login, concurrent chat handlers, text and file
messages, typing notifications, reconnects, and optional captcha solving.

This project is not affiliated with Max or VK. The internal API can change
without notice.

## Quick start

Install the current stable Rust toolchain, copy `.env.template` to `.env`, then
run:

```bash
cargo run --example cli
```

## Configuration

The CLI loads `.env` before reading the process environment.

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
  Empty disables the solver. Only used if Max rejects the captcha-free SMS
  request and requires a captcha retry.
- `MAX_CALLBACK_BIND`: captcha callback bind address. Default: `127.0.0.1:3002`.
- `MAX_CALLBACK_URL_BASE`: public callback base for the solver. May contain
  `{port}`.
- `RUST_LOG`: tracing filter. CLI default: `maxrs=info`.

## Captcha solver

SMS auth starts without captcha and falls back to the optional
[`max_captcha_solver`](https://github.com/blackyblack/max_captcha_solver) when
required. Empty values disable it:

```env
MAX_SOLVER_URL=
MAX_CALLBACK_BIND=
MAX_CALLBACK_URL_BASE=
```

For a containerized solver, publish its ports and point callbacks to the host:

```env
MAX_SOLVER_URL=http://127.0.0.1:3000
MAX_CALLBACK_BIND=0.0.0.0:3002
MAX_CALLBACK_URL_BASE=http://host.docker.internal:3002
```

On Linux, Docker may need a `host-gateway` mapping for `host.docker.internal`.
The solve API must be reachable from `maxrs`; the callback URL must be reachable
from the solver.

## Protocol notes

See [`docs/PROTOCOL.md`](docs/PROTOCOL.md) for protocol details.

## License

MIT
