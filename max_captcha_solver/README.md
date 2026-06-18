# MAX captcha solver

Standalone service that solves VK ID `not_robot_captcha` challenges for MAX auth flows and returns the resulting session token through a callback.

## Run

```sh
npm install
cp .env.template .env
npm start
```

## API

### `POST /solve`

Starts a challenge. `captchaUrl` must be fresh and unused.

```json
{
  "challengeId": "id-1",
  "captchaUrl": "https://id.vk.ru/not_robot_captcha?...",
  "callbackUrl": "https://max-login.example/captcha-callback"
}
```

Returns `202 Accepted` immediately:

```json
{
  "challengeId": "id-1",
  "status": "accepted",
  "operatorUrl": "https://solver.example/operator/id-1"
}
```

When solved, the service posts to `callbackUrl`:

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

### `GET /healthz`

Returns service health and active challenge count:

```json
{
  "ok": true,
  "challenges": 0
}
```

## Operator Handoff

The service first tries to solve automatically. If autosolve times out, it exposes `/operator/:challengeId` for manual solving and sends an operator notification.

Telegram notifications are sent when `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are configured.

## Telegram Setup

1. Open Telegram and start a chat with `@BotFather`.
2. Send `/newbot`, choose a name and username, and copy the bot token into `TELEGRAM_BOT_TOKEN`.
3. Start a direct chat with the new bot, or add it to the operator group.
4. Send any message to the bot or group.
5. Open `https://api.telegram.org/bot<TELEGRAM_BOT_TOKEN>/getUpdates` in a browser.
6. Copy the `chat.id` value into `TELEGRAM_CHAT_ID`. Group chat IDs are usually negative numbers.

After changing `.env`, restart the service.

## Configuration

Copy `.env.template` to `.env` and adjust values there. The service loads `.env` with `dotenv` on startup.

## Docker

```sh
docker build -t max-captcha-solver .
docker run --rm -p 3000:3000 max-captcha-solver
```
