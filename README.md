# File Sync Service

A content-addressed file storage and sync system: a **FastAPI server** that stores
files as hash-addressed blocks, a **sync client** that watches a folder and
pushes/pulls changes (like the Dropbox desktop app), and a **React web app** that
runs the same delta engine in the browser.

- **Delta uploads** — files are split into 4 MiB blocks keyed by SHA-256; only
  changed blocks move, and a duplicate file uploads nothing.
- **File bytes never touch the app server** — clients PUT/GET blocks straight to
  Backblaze B2 via presigned URLs; the API moves only hashes and recipes.
- **Realtime multi-device sync** — a commit on one device reaches the others over
  WebSockets, fanned out across servers by Redis pub/sub.
- **No lost edits** — commits are an etag compare-and-swap; on a conflict the sync
  client keeps both versions, one as a `(conflicted copy)`.
- **Sharing** — read-only user-to-user grants and expiring, revocable public links.

**Stack:** FastAPI (Python 3.12) · MongoDB Atlas (metadata) · Backblaze B2 (blocks) ·
Redis (optional, realtime fan-out) · JWT + bcrypt · watchdog + httpx · React + Vite.

**Read [DESIGN.md](DESIGN.md)** for the architecture, core idea, and tradeoffs. This
README covers running it and the API.

---

## Quick start

### 1. Setup

```bash
uv venv --prompt file-sync-service --python /opt/homebrew/bin/python3.12
source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example .env            # fill in real values (see Configuration)
```

### 2. Server

```bash
uvicorn app.main:app --reload --log-config log_config.yaml   # http://127.0.0.1:8000
```

Swagger UI is at **http://127.0.0.1:8000/docs**. Create an account before running a client:

```bash
curl -X POST http://127.0.0.1:8000/auth/register \
     -H "Content-Type: application/json" \
     -d '{"username": "alice", "password": "secret123"}'
```

### 3. Sync client

```bash
python -m client --username alice --password secret123 --folder ~/FileSync
```

It logs in, does a two-way sync, then watches the folder and pushes changes, while a
WebSocket pulls remote changes as soon as another device commits. Run a second client
against another folder with the same account to watch files sync between them.

| Flag                        | Default                 | Meaning                          |
|-----------------------------|-------------------------|----------------------------------|
| `--server`                  | `http://127.0.0.1:8000` | server base URL                  |
| `--username` / `--password` | *(required)*            | existing account to log in as    |
| `--folder`                  | *(required)*            | local folder to sync             |
| `--once`                    | off                     | sync once and exit (no watching) |

### 4. Web UI (optional)

```bash
cd frontend
npm install
npm run dev                     # http://localhost:5173
```

Vite proxies the API routes to `:8000`, so local dev needs no CORS setup on the
server. The **B2 bucket** does need a CORS rule allowing your origin
(`http://localhost:5173`, plus production) for `GET`, `PUT`, and `HEAD`, because the
browser transfers blocks to B2 directly.

The web UI covers register/login, upload/download/delete, a "Shared with you" tab,
a per-file sharing panel (grants and public links), and a no-auth `/?token=…`
public download page. Lists refresh live over `/ws`.

### Multiple servers (optional)

Realtime works on one server out of the box (an in-process bus). To run several,
start Redis and set `REDIS_URL=redis://localhost:6379`.

---

## Configuration

All config comes from `.env` (see [`.env.example`](.env.example)). The app refuses to
start without the required values.

| Variable                                                                         | Default    | Notes                                                                      |
|----------------------------------------------------------------------------------|------------|----------------------------------------------------------------------------|
| `MONGODB_URI`, `MONGODB_DB`                                                      | *required* | Atlas TLS uses `certifi`, already wired in `dependencies.py`               |
| `S3_ENDPOINT_URL`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, `S3_REGION`, `S3_BUCKET` | *required* | B2's S3-compatible API; the endpoint needs the `https://` scheme          |
| `JWT_SECRET`                                                                     | *required* | `python -c "import secrets; print(secrets.token_urlsafe(48))"`             |
| `S3_URL_TTL_SECONDS`                                                             | `300`      | presigned-URL lifetime, which is also how long a revoked share stays readable |
| `JWT_ALGORITHM`                                                                  | `HS256`    |                                                                            |
| `ACCESS_TOKEN_EXPIRE_MINUTES`                                                    | `60`       |                                                                            |
| `REFRESH_TOKEN_EXPIRE_MINUTES`                                                   | `10080`    | 7 days                                                                     |
| `SHARE_LINK_EXPIRE_MINUTES`                                                      | `10080`    | 7 days                                                                     |
| `REDIS_URL`                                                                      | unset      | unset means a single-process in-memory bus                                 |

---

## Testing

```bash
uv pip install -r requirements-dev.txt
pytest                          # backend + sync client; fails under 90% coverage

cd frontend && npm test         # web client (Vitest)
```

Tests mirror the `app/` and `client/` trees, with B2 and HTTP mocked or faked. The
suite needs a `.env`; some router and dependency tests connect to its MongoDB.

---

## Project layout

```
app/                  FastAPI server, hexagonal (see DESIGN.md §4)
├── routers/          HTTP + WebSocket routes: auth, files, blocks, shares, link, ws
├── application/      FileService, AuthService, ShareService
├── domain/, models/  JWT/bcrypt helpers, Pydantic models
├── ports/            abstract interfaces (BlockStore, FileRepository, ChangeBus, …)
├── adapters/         B2, Mongo, Redis, and in-memory implementations
├── dependencies.py   composition root: wires adapters into services
└── realtime.py       WebSocket registry + ChangeBus subscriber

client/               sync client; imports nothing from app/
├── sync.py           SyncEngine: push (local → server) and pull (server → local)
├── watcher.py        folder watcher → push
├── ws_listener.py    /ws listener → pull
└── state.py          LocalIndex: last-synced hash + etag per file

frontend/src/         React web client; lib/ holds the API, chunking, and download code
scripts/              one-off maintenance (below)
tests/                mirrors app/ and client/
```

---

## API

Protected routes need `Authorization: Bearer <access_token>`. `owner` on any write is
always taken from the token, never the request. Denials return `404`, never `403`.

**Auth**

| Method | Path             | Body                        | Returns                                                 |
|--------|------------------|-----------------------------|---------------------------------------------------------|
| `POST` | `/auth/register` | JSON `{username, password}` | `201` `{username, created_at}`                          |
| `POST` | `/auth/login`    | form `username, password`   | `{access_token, refresh_token, token_type}`             |
| `POST` | `/auth/refresh`  | JSON `{refresh_token}`      | `{access_token, token_type}` · `401` if revoked/expired |
| `POST` | `/auth/logout`   | JSON `{refresh_token}`      | `204`, revokes that refresh token                       |

**Files and blocks**

| Method   | Path                                   | Body                          | Returns                                                                     |
|----------|----------------------------------------|-------------------------------|-----------------------------------------------------------------------------|
| `POST`   | `/blocks/missing`                      | `{hashes: [...]}`             | `{missing: [...]}`                                                          |
| `POST`   | `/blocks/upload-urls`                  | `{hashes: [...]}`             | `{urls: {hash: presigned PUT}}`                                             |
| `POST`   | `/blocks/download-urls?path=…&owner=…` | `{hashes: [...]}`             | `{urls: {hash: presigned GET}}`; `404` for a hash outside the file's recipe |
| `POST`   | `/files/commit`                        | `{path, size, block_hashes}`  | `201` create / `200` update + `ETag`; see preconditions below               |
| `GET`    | `/files/recipe?path=…&owner=…`         | —                             | `{path, size, block_hashes}` + `ETag`                                       |
| `GET`    | `/files`                               | —                             | `{files: [FileSummary]}`, each with its `etag`                              |
| `DELETE` | `/files?path=…`                        | —                             | `204`; also removes the file's grants and links                             |

On reads, `owner` defaults to the caller. Passing another user's `owner` reads a file
shared with you.

**Commit preconditions.** Every file has `etag = sha256(block_hashes)`. `commit` is a
conditional write on the etag the client last read:

- `If-Match: "<etag>"` updates only if the file is still at that etag, otherwise `412`.
- `If-None-Match: *` creates only if the path is free, otherwise `412`.
- Neither header gives `428`.
- A block in the recipe that isn't in B2 gives `409`.
- Re-committing identical content is a no-op, not a conflict.

**Sharing** (read-only)

| Method   | Path                 | Body                  | Returns                                   |
|----------|----------------------|-----------------------|-------------------------------------------|
| `POST`   | `/shares`            | `{path, shared_with}` | the `Share`; grants read on your file     |
| `DELETE` | `/shares`            | `{path, shared_with}` | `204`, revokes the grant                  |
| `GET`    | `/shares/incoming`   | —                     | `[Share]` shared with you                 |
| `GET`    | `/shares/outgoing`   | —                     | `[Share]` you've granted                  |
| `POST`   | `/shares/link`       | `{path}`              | `{token}` for a public link to your file  |
| `GET`    | `/shares/link`       | —                     | `[ShareLink]`, your live links            |
| `DELETE` | `/shares/link/{jti}` | —                     | `204`, revokes the link                   |

**Public links** (no auth; the token is the credential)

| Method | Path                          | Body              | Returns                                               |
|--------|-------------------------------|-------------------|-------------------------------------------------------|
| `GET`  | `/link/recipe?token=…`        | —                 | `{path, size, block_hashes}`; `404` if bad, expired, or revoked |
| `POST` | `/link/download-urls?token=…` | `{hashes: [...]}` | `{urls: {hash: presigned GET}}`                       |

**WebSocket:** `/ws?token=<access_token>` pushes `{"type":"changed"}` when something
you can see changes. Clients refetch on each message.

### Upload and download flows

```
upload:   chunk + hash locally → POST /blocks/missing → POST /blocks/upload-urls
          → PUT missing blocks to B2 → POST /files/commit (If-Match / If-None-Match)

download: GET /files/recipe → POST /blocks/download-urls
          → GET blocks from B2 → verify sha256(block) == hash → reassemble
```

Revoking a share stops future reads, but download URLs already issued stay valid
until they expire (`S3_URL_TTL_SECONDS`).

---

## Sync client behavior

- **Push** uploads only missing blocks, then commits with the etag stored in
  `LocalIndex`. On a `412` it saves the server's version as
  `name (conflicted copy).ext` and retries once.
- **Pull** downloads files that differ from the server and verifies every block. It
  never overwrites an unpushed local edit; if both sides changed, the server's version
  is saved beside it as a conflicted copy.
- **Deletes sync both ways.** A file missing locally is deleted on the server, and a
  file missing on the server is deleted locally (unless it has an unpushed edit).
- **Reconnects and token refresh are automatic.** The WebSocket retries every 3s and
  pulls on reconnect. A `401` refreshes the access token and retries once. When the
  7-day refresh token expires, restart the client to log in again.

---

## Demo via `curl`

With the server running and `jq` installed: upload, share, read as another user,
public link, revoke. Uses a single-block file, so the block hash is just `sha256(file)`.

```bash
BASE=http://127.0.0.1:8000

# 1. Two accounts, then log in (form-encoded, per OAuth2)
for u in alice bob; do
  curl -s -X POST $BASE/auth/register -H 'Content-Type: application/json' \
       -d "{\"username\":\"$u\",\"password\":\"secret123\"}"
done
ALICE=$(curl -s -X POST $BASE/auth/login -d 'username=alice&password=secret123' | jq -r .access_token)
BOB=$(curl -s -X POST $BASE/auth/login -d 'username=bob&password=secret123' | jq -r .access_token)

# 2. Bob uploads a file: presigned PUT to B2, then commit (If-None-Match: * = create)
echo "hello shared world" > /tmp/demo.txt
HASH=$(shasum -a 256 /tmp/demo.txt | cut -d' ' -f1)
SIZE=$(wc -c < /tmp/demo.txt | tr -d ' ')
PUT=$(curl -s -X POST $BASE/blocks/upload-urls -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -d "{\"hashes\":[\"$HASH\"]}" | jq -r ".urls[\"$HASH\"]")
curl -s -X PUT "$PUT" --data-binary @/tmp/demo.txt
curl -s -X POST $BASE/files/commit -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -H 'If-None-Match: *' \
     -d "{\"path\":\"/demo.txt\",\"size\":$SIZE,\"block_hashes\":[\"$HASH\"]}"

# 3. Bob shares with Alice; Alice reads it
curl -s -X POST $BASE/shares -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -d '{"path":"/demo.txt","shared_with":"alice"}'
GET=$(curl -s -X POST "$BASE/blocks/download-urls?owner=bob&path=/demo.txt" \
     -H "Authorization: Bearer $ALICE" -H 'Content-Type: application/json' \
     -d "{\"hashes\":[\"$HASH\"]}" | jq -r ".urls[\"$HASH\"]")
curl -s "$GET"                                                     # -> hello shared world

# 4. Public link: works with no auth, then 404 once revoked
TOKEN=$(curl -s -X POST $BASE/shares/link -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -d '{"path":"/demo.txt"}' | jq -r .token)
curl -s "$BASE/link/recipe?token=$TOKEN"                           # -> the recipe
JTI=$(curl -s "$BASE/shares/link" -H "Authorization: Bearer $BOB" | jq -r '.[0].jti')
curl -s -X DELETE "$BASE/shares/link/$JTI" -H "Authorization: Bearer $BOB"
curl -s -o /dev/null -w '%{http_code}\n' "$BASE/link/recipe?token=$TOKEN"   # -> 404
```

Files over 4 MiB span multiple blocks; `python -m client` handles that for you.

---

## Maintenance scripts

Both are dry-run by default; run from the repo root with the venv active.

- `python -m scripts.audit_storage` finds files with missing blocks, orphaned blocks,
  and orphaned grants. `--delete` removes them. Don't delete orphaned blocks during
  active uploads.
- `python -m scripts.backfill_etag` adds `etag` to files that predate optimistic
  concurrency. `--apply` writes; `--blocks` also seeds the block index.

---

## Roadmap

See [DESIGN.md §7](DESIGN.md#7-known-limitations--tradeoffs) for current limitations
and [§10](DESIGN.md#10-hardening-backlog) for the hardening backlog. Next up:

- Content-defined chunking, so a one-byte insert doesn't rewrite every block hash.
- Pull-side delta (reuse local blocks on download).
- Streaming chunking and download, so large files don't load fully into memory.
- Scheduled garbage collection of orphaned blocks.
