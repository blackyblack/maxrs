# Captcha solver integration

`maxrs` uses the external
[`max_captcha_solver`](https://github.com/blackyblack/max_captcha_solver) service.
See that repository for service configuration and operator routes.

## Solve request

The client sends `POST /solve` to `MAX_SOLVER_URL` with a fresh captcha URL and
its callback address:

```json
{
  "challengeId": "<client-generated UUID>",
  "captchaUrl": "<URL returned by Max>",
  "callbackUrl": "http://127.0.0.1:3002/captcha-callback"
}
```

The client checks the HTTP status; it does not consume the response body.
The callback URL must be reachable from the solver. The request and callback
share a one-hour timeout.

## Callback

The solver posts JSON to the supplied callback URL with the same `challengeId`:

```json
{ "challengeId": "<UUID>", "status": "ok", "token": "<captcha token>" }
```

On failure:

```json
{ "challengeId": "<UUID>", "status": "failed", "error": "<reason>" }
```

The client returns `200` for a delivered callback, `400` for malformed JSON or
an unknown challenge, and `413` for a body larger than 16 KiB. A failure callback
is delivered successfully but causes authentication to fail.
