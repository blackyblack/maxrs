# Max (OneMe) web protocol reference

This document captures the subset of the Max web API used by `maxrs`. It was
derived by cross-referencing several existing Python/C# clients and a community
guide (see *Sources* below).

## Transport

- Endpoint: `wss://ws-api.oneme.ru/websocket`
- Required header: `Origin: https://web.max.ru`
- A browser-like `User-Agent` header is also sent.

## Frame format

Every message is a JSON object:

```json
{ "ver": 11, "cmd": 0, "seq": 1, "opcode": 6, "payload": { } }
```

| field   | type | meaning |
| ------- | ---- | ------- |
| `ver`   | int  | protocol version, currently `11` |
| `cmd`   | int  | `0` request, `1` response, `3` error |
| `seq`   | int  | incrementing id; responses echo the request's `seq` |
| `opcode`| int  | operation code |
| `payload`| obj | operation-specific body |

Keepalive: send `PING` (opcode 1) `{"interactive": false}` roughly every 30s so
the server does not drop the connection.

## Opcodes used here

| opcode | name          | direction       |
| ------ | ------------- | --------------- |
| 1      | PING          | client → server |
| 6      | SESSION_INIT  | client → server |
| 224    | AUTH_CAPTCHA_REQUEST | client -> server |
| 17     | AUTH_REQUEST  | client → server |
| 18     | AUTH          | client → server |
| 19     | LOGIN         | client → server |
| 64     | MSG_SEND      | client → server |
| 65     | MSG_TYPING    | client → server |
| 87     | FILE_UPLOAD   | client → server |
| 128    | NOTIF_MESSAGE | server → client |
| 136    | NOTIF_ATTACH  | server → client |

## Authentication (SMS)

1. **SESSION_INIT (6)**

   ```json
   { "userAgent": { "deviceType": "WEB", "...": "..." }, "deviceId": "<uuid>" }
   ```

   Current web auth then performs **AUTH_CAPTCHA_REQUEST (224)** before
   requesting the SMS code:

   ```json
   { "source": "auth", "identifier": "+79990000000" }
   ```

   Response payload may contain `link`. The official web client renders that
   link in a VK captcha widget and passes the resulting `captchaToken` to
   AUTH_REQUEST.

2. **AUTH_REQUEST (17)** — asks the server to send an SMS code.

   ```json
   {
     "phone": "+79990000000",
     "type": "START_AUTH",
     "language": "ru",
     "captchaToken": "<captcha token or empty string>"
   }
   ```

   Response payload contains a short-lived `token` (the "SMS token").

3. **AUTH (18)** — verifies the code.

   ```json
   { "token": "<sms token>", "verifyCode": "12345", "authTokenType": "CHECK_CODE" }
   ```

   Response payload contains the long-lived session token at
   `tokenAttrs.LOGIN.token`.

4. **LOGIN (19)** — authenticates the socket and syncs state.

   ```json
   {
     "interactive": true,
     "token": "<session token>",
     "chatsSync": 0,
     "contactsSync": 0,
     "presenceSync": 0,
     "draftsSync": 0,
     "chatsCount": 40
   }
   ```

   Response payload contains `profile`, `chats`, `contacts`, etc.

Re-login on subsequent runs is just SESSION_INIT + LOGIN with the saved session
token (no SMS).

## Sending a text message — MSG_SEND (64)

```json
{
  "chatId": 123456,
  "message": { "text": "hello", "cid": -1700000000001, "type": "USER",
               "elements": [], "attaches": [] },
  "notify": true
}
```

`cid` is a client-generated negative id used to de-duplicate without colliding
with server-assigned positive message ids.

## Typing notification — MSG_TYPING (65)

```json
{ "chatId": 123456, "type": "TEXT" }
```

## Sending a file — FILE_UPLOAD (87)

1. Request an upload slot:

   ```json
   { "count": 1 }
   ```

   Response:

   ```json
   { "info": [ { "url": "https://...", "fileId": 987654, "token": "..." } ] }
   ```

2. HTTP `POST` the raw bytes to `url` with headers:

   ```
   Content-Disposition: attachment; filename=<name>
   Content-Length: <size>
   Content-Range: 0-<size-1>/<size>
   ```

3. Wait for the server's **NOTIF_ATTACH (136)** push, whose payload contains the
   matching `fileId`, signalling that processing finished.

4. Send the message with the file attached via MSG_SEND (64):

   ```json
   {
     "chatId": 123456,
     "message": { "text": "caption", "cid": -1700000000002, "type": "USER", "elements": [],
                  "attaches": [ { "_type": "FILE", "fileId": 987654 } ] },
     "notify": true
   }
   ```

Photos use PHOTO_UPLOAD (80) with `{"_type":"PHOTO","photoToken":...}` instead;
only generic files are implemented here.

## Receiving messages — NOTIF_MESSAGE (128)

The server pushes a request frame (`cmd = 0`) with:

```json
{ "chatId": 123456, "message": { "id": 1, "sender": 7, "text": "hi", "time": 1700000000000 } }
```

The official web client acknowledges it with a response frame echoing the same
`seq`/`opcode` and `{ "chatId": ..., "messageId": ... }`; `maxrs` does the same.

## Sources

The flows above were cross-checked against:

- PronikFire's **Max-API-Guide** (opcode table, framing) and the companion
  C# **Client-Max-Api** (WebSocket transport, SESSION_INIT/AUTH/LOGIN/MSG_SEND).
- **vkmax** / **python-max-client** (SMS auth flow, MSG_SEND, photo upload).
- **PyMax** (FILE_UPLOAD flow + NOTIF_ATTACH confirmation, opcode enum).
- **openmax-server** (MSG_TYPING payload shape).
