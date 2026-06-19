# API

The service exposes two HTTP listeners:

- Solve API: `SOLVE_HOST:SOLVE_PORT`, default `127.0.0.1:3000`.
- Operator API: `OPERATOR_HOST:OPERATOR_PORT`, default `0.0.0.0:3001`.

The solve API is intended for trusted local callers and is not authenticated. Operator routes require an active `challengeId` in the URL and are short lived.

## `GET /healthz`

Available on both listeners.

Response:

```json
{
  "ok": true,
  "challenges": 0
}
```

## `POST /solve`

Starts a challenge on the solve API. `captchaUrl` must be fresh and unused.

Request:

```json
{
  "challengeId": "id-1",
  "captchaUrl": "https://id.vk.ru/not_robot_captcha?...",
  "callbackUrl": "https://max-login.example/captcha-callback"
}
```

Response, `202 Accepted`:

```json
{
  "challengeId": "id-1",
  "status": "accepted",
  "operatorUrl": "https://solver.example/operator/id-1"
}
```

Errors:

- `400` when required fields are missing or URLs are invalid.
- `409` when `challengeId` is already running.

## Callback

When solved, the service posts this JSON to `callbackUrl`:

```json
{
  "challengeId": "id-1",
  "status": "ok",
  "token": "success_token"
}
```

On failure:

```json
{
  "challengeId": "id-1",
  "status": "failed",
  "error": "reason"
}
```

Callback delivery uses `CALLBACK_TIMEOUT_MS`. Delivery failures are logged, but the challenge browser page and in-memory state are still cleaned up.

## Operator Routes

`GET /operator/:challengeId` opens the manual solve page.

`GET /operator/:challengeId/screenshot` returns the latest captcha screenshot as JPEG.

`POST /operator/:challengeId/tap` clicks the browser page at a relative coordinate.

Request:

```json
{
  "x": 0.5,
  "y": 0.5
}
```

`x` and `y` must be finite numbers from `0` to `1`.
