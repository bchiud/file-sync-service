# File Sync Service

A content-addressed file storage and sync system: a **FastAPI server** that stores
files as hash-addressed blocks, a **sync client** that watches a folder and
pushes/pulls changes (like the Dropbox desktop app), and a **React web app** that
runs the same delta engine in the browser.

**Highlights**

- **Delta uploads** — only changed 4 MiB blocks move: a 1 MB in-place edit to a
  100 MB file uploads one block (4 MB, not 100 MB), and a duplicate file uploads nothing.
- **File bytes never touch the app server** — clients PUT/GET blocks straight to
  Backblaze B2 via presigned URLs; the API moves only hashes and recipes.
- **Realtime multi-device sync** — a commit on one device reaches the others over
  WebSockets, fanned out across servers by Redis pub/sub.
- **No lost edits** — commits are an etag compare-and-swap; on a conflict the sync
  client keeps both versions, one as a `(conflicted copy)`.
- **Sharing** — read-only user-to-user grants and expiring, revocable public links.

**Start here:** [DESIGN.md](DESIGN.md) for the big picture ·
[Running](#running) to try it · [Demo via `curl`](#demo-the-whole-system-via-curl)

**Contents:** [Core idea](#core-idea-content-addressed-storage) ·
[Architecture](#architecture) · [Data models](#data-models) · [API](#api) ·
[Authentication](#authentication) · [Sharing](#sharing--permissions) ·
[Sync client](#sync-client) · [Configuration](#configuration) · [Running](#running) ·
[Testing](#testing) · [Scaling](#scaling) · [Status & roadmap](#status--roadmap)

- **Server API:** FastAPI (Python 3.12)
- **Blocks:** Backblaze B2 (S3-compatible)
- **Metadata:** MongoDB Atlas
- **Auth:** JWT (HS256) + bcrypt
- **Client:** watchdog folder watcher + httpx

---

## Core idea: content-addressed storage

Every file is split into fixed-size blocks (4 MiB). Each block is stored under
the **SHA-256 hash of its contents** — the key *is* the hash. A file is then just
an ordered list of block hashes (its "recipe"), stored as metadata.

```
file bytes ──chunk──▶ [block, block, block]
                         │      │      │
                       sha256 sha256 sha256
                         ▼      ▼      ▼
   recipe:  ["3af…", "9c1…", "70b…"]     ← stored in MongoDB
   blocks:  key=<owner>/<hash> → bytes   ← stored in B2
```

This buys three things:

- **Deduplication** — identical blocks are stored once **per user** (see scoping below).
- **Delta sync** — editing part of a file only transfers the changed blocks.
- **Integrity** — a block's key is a checksum of its contents; readers re-verify it.

**Per-user block scoping.** Block keys are namespaced by owner (`<owner>/<hash>`),
derived from the authenticated token — never from client input. It's what makes
client-driven direct uploads safe, trading cross-user dedup for two guarantees:

- **No cross-user side channel** — `/blocks/missing` can only answer "do *you*
  have this block?", so your upload speed can't reveal whether *another* user
  already holds one.
- **No cross-user poisoning** — the server mints a presigned PUT and never sees
  the bytes, so it can't check `sha256(data) == hash` on write. Namespacing the
  key means a bad block can only ever land in your own namespace.

> Chunking is **fixed-size** (like Dropbox). The `chunker.split()` seam is isolated
> so it can be swapped for content-defined chunking (rolling hash) without
> touching any other layer.

---

## Architecture

Hexagonal (ports & adapters) with dependency injection. Dependencies point
**inward**: the outer layers depend on the inner ones, never the reverse.

```
   client / browser
          │
          │ HTTP / WebSocket — JSON only: hashes, recipes, presigned urls
          ▼
   ┌──────────────────┐
   │  routers/        │  presentation
   └────────┬─────────┘
            │ Depends(...)
   ┌────────▼─────────┐     ┌───────────────────────┐
   │  application/    │ ──▶ │  domain/ + models/    │  pure core (no I/O)
   └────────┬─────────┘     └───────────────────────┘
            │ depends on ports
   ┌────────▼─────────┐
   │  ports/          │  abstract interfaces (ABCs)
   └────────▲─────────┘
            │ implemented by
   ┌────────┴─────────┐
   │  adapters/       │  infrastructure  ·  Mongo · Redis · B2
   └────────┬─────────┘
            │ has_block / presign  (metadata — never the bytes)
            ▼
   ┌──────────────────────────────┐
   │  B2   key = <owner>/<hash>   │ ◀╌╌╌╌ block bytes, presigned PUT / GET,
   └──────────────────────────────┘        client → B2, bypassing every layer above

   composition root: dependencies.py wires adapters → services
   cross-cutting:    auth_dependencies.py (bearer → user) · realtime.py (Notifier + ConnectionManager)
```

**Why this shape:**

- **No framework leakage** — the domain and application layers never import
  FastAPI, boto3, or pymongo.
- **Swappable infrastructure** — trade B2 for local disk, or Mongo for Postgres,
  by writing one new adapter and changing one line in `dependencies.py`. Nothing
  else moves.
- **Testable core** — services are unit-tested with in-memory fakes, no cloud
  required.

**Realtime path (pub/sub).** A commit/delete publishes onto a `ChangeBus`; every
server runs a subscribe-loop that fans out to *its own* WebSockets — so a change on
one server reaches a device connected to any other. The bus is a port: Redis in
production, in-process when `REDIS_URL` is unset.

```
   any change ─▶ Notifier.notify(user) ─▶ ChangeBus.publish(user)
                                                    │  Redis pub/sub
                             ┌──────────────────────┴──────────────────────┐
                             ▼                                              ▼
                    Server A: listen-loop                         Server B: listen-loop
                             │                                              │
                    ConnectionManager.notify                     ConnectionManager.notify
                             │                                              │
                    Alice's sockets ✔                            (holds none) no-op
```

#### Why WebSockets — and why SSE was the closer call

The traffic is one-way and single-event: the server pushes `{"type":"changed"}`,
the client refetches. Nothing ever flows client→server on that socket. That is
**exactly** Server-Sent Events' shape, so the two were weighed:

|                            | WebSocket (chosen)                                        | SSE (`EventSource`)                                                                    |
|----------------------------|-----------------------------------------------------------|----------------------------------------------------------------------------------------|
| **Fits one-way traffic**   | over-spec'd — bidirectional + binary framing unused       | purpose-built                                                                          |
| **Reconnect on drop**      | hand-rolled (exponential backoff, jitter, disposal guard) | free, built into the browser                                                           |
| **Missed-event replay**    | none — reconnect refetches instead                        | `Last-Event-ID`, if the server retains events                                          |
| **Auth**                   | no custom headers → token in query string                 | *same* — `EventSource` can't set headers either                                        |
| **Server (FastAPI)**       | first-class: `@app.websocket`, `WebSocketDisconnect`      | `StreamingResponse` + manual `text/event-stream` framing + `is_disconnected()` polling |
| **Connections per origin** | not subject to the HTTP/1.1 limit                         | consumes one of ~6 (moot here: ≤2 streams, and block transfers hit B2's origin)        |

- **Reconnect had to be hand-rolled** — `EventSource` ships with it. The strongest
  argument that SSE was the better fit.
- **Replay isn't wanted anyway.** `Last-Event-ID` needs retained events (Redis
  **Streams**, not pub/sub). Refetching on reconnect reconciles against current
  state — strictly stronger, and the payload is empty regardless.
- **Why the server side is simpler.** The traffic is one-way (SSE's shape), but the
  *plumbing* — upgrade, framing, liveness, teardown — is what FastAPI automates for
  WebSockets and leaves manual for SSE. The handler is ~20 lines of logic
  (`accept`, then `await receive_text()` whose only job is to raise
  `WebSocketDisconnect` on drop, then `finally` teardown); the SSE equivalent is
  that plus a hand-rolled `text/event-stream` generator with its own
  `is_disconnected()` polling.

**Verdict:** close call, and SSE is the better-matched primitive. WebSockets stay
because the server side is genuinely simpler in FastAPI and the reconnect is now
written and tested.

### Layers

| Layer        | Directory                    | Knows about      | Example                                                                                                                                                         |
|--------------|------------------------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Presentation | `app/routers/`               | HTTP, FastAPI    | `files.py`, `auth.py`, `shares.py`, `link.py`                                                                                                                   |
| Application  | `app/application/`           | ports, domain    | `FileService`, `AuthService`, `ShareService`                                                                                                                    |
| Domain       | `app/domain/`, `app/models/` | nothing external | `security`, Pydantic models                                                                                                                                     |
| Ports        | `app/ports/`                 | — (abstract)     | `BlockStore`, `BlockIndex`, `FileRepository`, `UserRepository`, `ShareRepository`, `ShareLinkRepository`, `RefreshTokenRepository`, `ChangeBus`                 |
| Adapters     | `app/adapters/`              | B2, Mongo, Redis | `B2BlockStore`, `MongoBlockIndex`, `MongoFileRepository`, `MongoShareRepository`, `MongoShareLinkRepository`, `MongoRefreshTokenRepository`, `RedisChangeBus`, `InMemoryChangeBus` |

---

## Directory layout

```
app/
├── main.py                 # thin assembler: app + include_router + lifespan (Notifier)
├── config.py               # Settings (pydantic-settings, loads .env)
├── dependencies.py         # composition root; @lru_cache lazy singletons
├── auth_dependencies.py    # get_current_user (OAuth2 bearer → username)
├── realtime.py             # ConnectionManager (local WS registry) + Notifier (ChangeBus ↔ sockets)
│
├── routers/                # presentation — HTTP routes
│   ├── files.py            #   /files   list · commit · recipe · delete (share-aware)
│   ├── blocks.py           #   /blocks  missing · upload-urls · download-urls (share-aware)
│   ├── auth.py             #   /auth    register · login · refresh · logout
│   ├── shares.py           #   /shares  grant · revoke · incoming · outgoing · link (mint · revoke · list)
│   ├── link.py             #   /link    public token-only recipe · download-urls (no auth)
│   └── ws.py               #   /ws      WebSocket: push "changed" to a user's devices
│
├── application/            # use-case orchestrators (no framework deps)
│   ├── file_service.py     #   FileService: missing_blocks · upload_urls · commit_file · get_recipe · delete_file
│   ├── auth_service.py     #   AuthService: register · authenticate · refresh · logout
│   └── share_service.py    #   ShareService: share · revoke · list · can_read · create/revoke/list/resolve_link
│
├── domain/                 # pure logic, no I/O
│   └── security.py         #   bcrypt + JWT (access · refresh · share) encode/decode · new_jti
│
├── models/                 # Pydantic schemas (the "NoSQL schema")
│   ├── file.py             #   FileMeta → FileSummary / FileRecord
│   ├── user.py             #   User, RefreshToken, Token/AccessToken/Refresh req+resp
│   └── share.py            #   Share, ShareRequest, ShareLink, ShareLinkRequest
│
├── ports/                  # abstract interfaces (ABCs)
│   ├── block_store.py              #   BlockStore
│   ├── block_index.py              #   BlockIndex (which blocks an owner has, in Mongo)
│   ├── file_repository.py          #   FileRepository
│   ├── user_repository.py          #   UserRepository
│   ├── share_repository.py         #   ShareRepository
│   ├── share_link_repository.py    #   ShareLinkRepository
│   ├── refresh_token_repository.py #   RefreshTokenRepository (jti allowlist)
│   └── change_bus.py               #   ChangeBus (realtime pub/sub)
│
└── adapters/               # concrete implementations
    ├── b2_block_store.py                 # BlockStore             → Backblaze B2 (boto3)
    ├── mongo_block_index.py              # BlockIndex             → MongoDB (owner,hash)
    ├── mongo_file_repository.py          # FileRepository         → MongoDB
    ├── mongo_user_repository.py          # UserRepository         → MongoDB
    ├── mongo_share_repository.py         # ShareRepository        → MongoDB
    ├── mongo_share_link_repository.py    # ShareLinkRepository    → MongoDB
    ├── mongo_refresh_token_repository.py # RefreshTokenRepository → MongoDB
    ├── in_memory_change_bus.py           # ChangeBus              → in-process (default)
    └── redis_change_bus.py               # ChangeBus              → Redis pub/sub

client/                     # the sync client — imports nothing from app/
├── chunker.py              #   client-side chunking (block hash = shared contract)
├── api_client.py           #   httpx wrapper: negotiation calls + direct-to-B2 PUT/GET
├── state.py                #   LocalIndex: last-synced content hash per file
├── sync.py                 #   SyncEngine: delta push (local→server) / pull (server→local)
├── watcher.py              #   watchdog → auto-push on file change
├── ws_listener.py          #   WebSocket listener → pull on server "changed" push
└── __main__.py             #   CLI entry point (python -m client)

frontend/src/               # the web client — same delta engine, in the browser
├── main.tsx               #   theme (light/dark) + StrictMode root
├── App.tsx                #   session restore; public-link page vs logged-in app
├── auth.tsx               #   Login: register / log in
├── home.tsx               #   shell: header + "Your files" / "Shared with you" tabs
├── files.tsx              #   FileList: upload · download · delete · share panel
├── incoming.tsx           #   SharedWithMe: grants received, download by owner
├── shares.tsx             #   SharePanel: grant/revoke a user · mint/revoke links
├── public.tsx             #   PublicDownload: no-auth /?token=… page
│
└── lib/                   #   no JSX — everything that talks to the server
    ├── api.ts             #     typed fetch wrapper + token refresh on 401
    ├── changes.ts         #     connectChanges (/ws socket, reconnect + backoff)
    ├── crypto.ts          #     chunkFile + sha256Hex (Web Crypto)
    ├── download.ts        #     assembleBlocks (fetch · verify · reassemble) + saveBlob
    └── format.ts          #     displayPath · middleTruncate · formatSize · userColor

tests/                      # mirrors app/ + client/ ; coverage gate 90% (pytest.ini)
```

---

## Data models

Documents are schemaless in Mongo, so the schema lives in code as Pydantic models.
Adapters translate model ⇄ dict at the storage boundary (`.model_dump()` /
`Model(**doc)`); raw dicts never leak past the adapter.

A `├──` below means *inherits from*. The sharing models have no common base — each
is a standalone `BaseModel`, so they're listed flat, request body under the thing
it creates.

```
FileBase(owner, path, size, updated_at)
 ├── FileSummary(+ etag)                             # listing view (no recipe)
 └── FileRecord(+ block_hashes, etag)                # full stored document

UserBase(username)
 ├── User(+ password_hash, created_at)               # internal — never returned by a route
 ├── UserRegisterRequest(+ password)                 # request body
 └── UserRegisterResponse(+ created_at)              # safe response (no hash)

Share(owner, path, shared_with, created_at)          # one user-to-user grant (Mongo `shares`)
ShareRequest(path, shared_with)                      #   → its grant/revoke request body

ShareLink(jti, owner, path, created_at, expires_at)  # one live public link (Mongo `share_links`)
ShareLinkRequest(path)                               #   → its mint request body
```

- **`FileSummary` vs `FileRecord`** — a deliberate read/write split: listings
  project away the (potentially huge) `block_hashes` array but keep the `etag`, so a
  client can commit an update straight off the list without a recipe round-trip.
- **`Share`** — keyed on the `(owner, path, shared_with)` triple, one document
  per grant.
- **`ShareLink`** — keyed on `jti` (the token's unique id). Its *existence* is
  what makes a link live, so revoking is just deleting the row; `expires_at`
  mirrors the token's `exp` claim (both set once, at mint).

### Why NoSQL (MongoDB) over SQL

The access pattern drove this, not the data volume:

- **No joins to give up.** Every query in `app/adapters/` is a single-collection
  point lookup — `files` by `(owner, path)`, `share_links` by `jti`, `users` by
  `username`. Not one join, aggregation, or transaction in the codebase.
- **Recipes embed naturally.** `block_hashes` is an ordered array (~12,000 entries
  for a 50 GB file) stored inline in `FileRecord`, so a recipe read is one lookup.
  SQL wants a child `file_blocks` table, joined and re-sorted on every read.
- **Pydantic is already the schema.** `.model_dump()` / `Model(**doc)` at the adapter
  boundary *is* the whole ORM — no migrations, no second schema to keep in sync.
- **TTL indexes reap expiring rows for free** (`expires_at, expireAfterSeconds=0` on
  `refresh_tokens` and `share_links`) — no cron job. Postgres needs `pg_cron` or a
  sweeper.
- **Shard on `owner`** to scale out — a user's files and grants colocate (see
  [Scaling](#scaling)).

**The cost — referential integrity.** Nothing here *requires* NoSQL; Postgres would
serve this data fine.

- **What we gave up.** The cascade on delete is hand-rolled (`purge_for_file`) and
  non-atomic — an invariant the application layer must remember, not one the store
  enforces. Grant a share on a path that doesn't exist and it's silently accepted.
- **But `ON DELETE CASCADE` is only half a fix.** A foreign key on the *natural* key
  `(owner, path)` clears the rows, then happily resurrects them: delete
  `/report.pdf`, commit a new one at the same path, and the old grants reattach.
- **Identity is the bug; the database is a side issue.** The durable fix is an
  immutable surrogate `file_id` minted at first commit — which SQL gives you by
  convention, not by nature. See [Sharing tradeoffs](#sharing--permissions).

---

## API

All `/files` and `/blocks` routes require `Authorization: Bearer <token>`.
`owner` is always taken from the token, never from the request.

**Auth**

| Method | Path             | Body                        | Returns                        |
|--------|------------------|-----------------------------|--------------------------------|
| `POST` | `/auth/register` | JSON `{username, password}` | `201` `{username, created_at}` |
| `POST` | `/auth/login`    | form `username, password`   | `{access_token, refresh_token, token_type}` |
| `POST` | `/auth/refresh`  | JSON `{refresh_token}`      | `{access_token, token_type}` · `401` if revoked/expired |
| `POST` | `/auth/logout`   | JSON `{refresh_token}`      | `204` — revokes that refresh token |

**Delta flow** (used by the sync client — file bytes go directly to/from B2)

| Method | Path                                 | Body                         | Returns                                                |
|--------|--------------------------------------|------------------------------|--------------------------------------------------------|
| `POST` | `/blocks/missing`                    | `{hashes: [...]}`            | `{missing: [...]}` — which blocks the server needs     |
| `POST` | `/blocks/upload-urls`                | `{hashes: [...]}`            | `{urls: {hash: presigned PUT url}}`                    |
| `POST` | `/blocks/download-urls?path=…&owner=` | `{hashes: [...]}`           | `{urls: {hash: presigned GET url}}` · `404` for a hash outside the file's recipe |
| `POST` | `/files/commit`                      | `{path, size, block_hashes}` | `201`/`200` `{path, size, blocks}` + `ETag` · `409` blocks missing · `412` stale precondition · `428` no precondition |
| `GET`  | `/files/recipe?path=…&owner=…`       | —                            | `{path, size, block_hashes}` + `ETag` header          |
| `GET`  | `/files`                             | —                            | `{files: [FileSummary]}` (each carries its `etag`)    |

On reads, `owner` is **optional** and defaults to the caller. Passing another
user's `owner` reads a file **shared with you** — the server gates it on a grant
and returns `404` (never `403`) if you have none. `path` is **required**:
`download-urls` presigns only hashes that belong to that file's recipe, so a
read authorized for one `(owner, path)` can't be used to pull other blocks in the
owner's namespace. A requested hash outside the recipe returns `404` — same
opaque answer as any other denial.

**Concurrent writes — optimistic concurrency on `commit`.** Every file carries an
`etag = sha256(ordered block_hashes)`, returned as an HTTP `ETag` on `recipe`,
`commit`, and each `/files` summary. `commit` is a **conditional write** keyed on
the etag the client last read:

- **`If-Match: "<etag>"`** — update this file *only if* it's still at that etag.
  A racing writer that moved it on gets **`412 Precondition Failed`**.
- **`If-None-Match: *`** — create only if the path is free; **`412`** if taken.
- **neither** — **`428 Precondition Required`**; the client must declare intent.
- A create returns **`201`**, an update **`200`**; both echo the new `ETag`.

The write is a single-document compare-and-swap in Mongo (`replace_one` filtered on
the etag), so it's atomic with no transaction. Re-committing identical content — a
retry or two clients making the *same* edit — folds into a no-op rather than a
false conflict. On a real `412` the policy differs by client: the sync client saves
the server's version as a `(conflicted copy)` and retries once off the new etag; the
browser shows an error and reloads the list so the user can retry.

**Sharing** (read-only grants; `owner` is always the caller's token identity)

| Method   | Path                 | Body                  | Returns                                                    |
|----------|----------------------|-----------------------|------------------------------------------------------------|
| `POST`   | `/shares`            | `{path, shared_with}` | the `Share` — grant read on *your* file to a user          |
| `DELETE` | `/shares`            | `{path, shared_with}` | `204` — revoke a grant                                     |
| `GET`    | `/shares/incoming`   | —                     | `[Share]` — files shared **with me**                       |
| `GET`    | `/shares/outgoing`   | —                     | `[Share]` — grants **I've made**                           |
| `POST`   | `/shares/link`       | `{path}`              | `{token}` — mint a public share-link token for *your* file |
| `DELETE` | `/shares/link/{jti}` | —                     | `204` — revoke a link by its `jti` (owner-scoped)          |
| `GET`    | `/shares/link`       | —                     | `[ShareLink]` — links **I've minted** that are still live  |

**Public links** (no auth — the signed token *is* the identity)

| Method | Path                          | Body              | Returns                                                                        |
|--------|-------------------------------|-------------------|--------------------------------------------------------------------------------|
| `GET`  | `/link/recipe?token=…`        | —                 | `{path, size, block_hashes}` — `404` on a bad/forged/**revoked/expired** token |
| `POST` | `/link/download-urls?token=…` | `{hashes: [...]}` | `{urls: {hash: presigned GET url}}` — `404` for a hash outside the linked file's recipe |

Interactive docs at `/docs` (Swagger UI, with the **Authorize** button).

### Request flow — delta upload (bytes never touch the app server)

```
1. client: blocks = chunker.split(data)            # hashes computed client-side
2. POST /blocks/missing {hashes}      → server: BlockIndex.present_subset(owner) → missing[]   (one query)
3. POST /blocks/upload-urls {missing} → server: presigned PUT url per owner/hash
4. client → PUT block bytes → B2 directly           (only the missing blocks)
5. POST /files/commit {path,size,hashes} + If-Match/If-None-Match
                                          → server HEADs only the not-yet-indexed blocks in B2,
                                            indexes them, CAS on etag, saves FileRecord, returns new
                                            ETag (409 if a new block is absent, 412 on a stale precondition)
```

**The block index — a Mongo cache of "which blocks an owner has."** Negotiation
(`/blocks/missing`) is a single indexed query, not one B2 `HEAD` per hash — a 12k-block
file was 12k serial round-trips. The index is a strict **subset of B2**: a block is
recorded only *after* `commit` confirms it's really in B2 (bytes go client→B2 directly,
so the server can't trust the upload landed). That direction is the safety property —
the index can only ever *over*-report "missing" (→ a harmless idempotent re-upload),
never claim a block is present when it isn't (→ a committable-but-unreadable file). So
`commit` `HEAD`s only genuinely-new blocks (paid once, per distinct block), and the hot
negotiation path touches Mongo alone.

### Request flow — delta download

```
1. GET /files/recipe?path=…          → server: FileRecord.block_hashes
2. POST /blocks/download-urls {h}    → server: presigned GET url per owner/hash
3. client → GET each block → B2 directly, re-verify sha256(block)==hash, reassemble
```

---

## Authentication

- **Passwords:** bcrypt with a per-password random salt. Only the hash is stored.
- **Tokens:** JWT (HS256), signed with `JWT_SECRET`, with an `exp` claim. The
  `sub` claim carries the username.
- **`get_current_user`** decodes the bearer token on each protected request and
  returns the username, or raises `401` for a missing / invalid / expired token.
- **Refresh tokens:** login also returns a 7-day refresh token. Its `jti` is
  allowlisted in Mongo (`refresh_tokens`, TTL-reaped), so `/auth/logout` revokes it
  by deleting the row; `/auth/refresh` mints a new 60-minute access token while the
  row exists. A `typ` claim stops refresh, access, and share tokens being used as
  one another.
- **No user enumeration:** unknown-username and wrong-password both return the
  same generic `401` on login.
- **No hash leakage:** routes return `UserRegisterResponse`, never the `User` model.

---

## Sharing & permissions

Read-only sharing comes in **two independent flavors**, but both rest on one
insight: **sharing never touches `FileService`.** It's a thin *authorization*
layer that only decides **which owner's namespace** a read resolves to. The
per-user block scoping does the rest — a recipient reads the owner's
`<owner>/<hash>` blocks but can never write into them, so "read-only" falls out
as *less code* (there is no `can_write`).

|                 | User-to-user                                                      | Public link                                                                                                          |
|-----------------|-------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| **Identity**    | JWT auth token → `current_user`                                   | signed share token → `(owner, path, jti)`                                                                            |
| **Grant store** | Mongo `shares` collection                                         | Mongo `share_links` — one row per **live** link, keyed on `jti`                                                      |
| **Authz check** | `ShareService.can_read` (you are the owner **or** a grant exists) | `ShareService.resolve_link` (valid signature, `typ == "share"`, **not expired**, **and its `jti` row still exists**) |
| **Mint**        | `POST /shares`                                                    | `POST /shares/link`                                                                                                  |
| **Read**        | `GET /files/recipe?owner=`, `POST /blocks/download-urls?owner=`   | `GET /link/recipe`, `POST /link/download-urls` (no login)                                                            |
| **Revoke**      | `DELETE /shares`                                                  | `DELETE /shares/link/{jti}` (deletes the row)                                                                        |
| **Expiry**      | — (grants are durable)                                            | `exp` claim, default 7 days (`SHARE_LINK_EXPIRE_MINUTES`)                                                            |

**Security invariants:**

- **`owner` on any mint/write comes from the token, never the body** — you can
  only share or link files you actually own.
- **`404`, never `403`, on denial** — refusing access never reveals that the
  file exists.
- **A read is scoped to one file's blocks, not the owner's whole namespace.**
  `can_read` / `resolve_link` authorize a `(owner, path)`, and `download-urls`
  presigns only hashes that appear in *that* file's recipe. Authz is on the path;
  the block hash alone is never a bearer capability — a grant on one file can't be
  turned into GET URLs for unrelated blocks in the owner's namespace. Out-of-recipe
  hashes get the same opaque `404`.
- **Auth and share tokens are non-interchangeable.** Both are HS256-signed with
  the same `JWT_SECRET`, so a `typ: "share"` claim (checked on decode) stops an
  auth token from being redeemed as a link, or vice-versa.
- **A share link carries no user.** The token *is* the capability: whoever holds
  `?token=…` may read exactly one `(owner, path)`, read-only, with no account.
- **Links expire and are revocable.** Two independent kill-switches, both checked
  on every read by `resolve_link`:
    - **Expiry** is free and offline — the `exp` claim is set once at mint (default
      7 days, `SHARE_LINK_EXPIRE_MINUTES`) and enforced by JWT decode, so an expired
      token fails signature-check before any DB lookup.
    - **Revocation** is an *allowlist*: minting writes a `share_links` row keyed on
      the token's `jti`, and a read only resolves while that row exists. `DELETE
    /shares/link/{jti}` deletes it — killing the link even though the signed token
      itself is still cryptographically valid. The same row powers `GET /shares/link`
      (list your live links). Revoke is **owner-scoped** (`{owner, jti}` filter), so
      no one can revoke a link they didn't mint.

**Revocation stops future reads, not bytes already in flight.**

- **What it kills.** `DELETE /shares` and `DELETE /shares/link/{jti}` take effect on
  the next `can_read` / `resolve_link`.
- **What it can't.** A presigned URL already handed out. B2 has never heard of the
  `shares` collection, so a recipient who fetched download URLs just before you
  revoked keeps them until those URLs expire.
- **How long.** `s3_url_ttl_seconds` (default **300**) bounds the window. It used to
  be an hour.
- **Why a window exists at all.** The same presigned direct-to-B2 transfer that keeps
  file bytes off the app server. The floor is set by the slowest legitimate download:
  `download_urls` signs a whole recipe at once and the client walks the blocks
  sequentially, so the TTL must outlast the entire transfer — at 4 MiB blocks and
  1 MB/s that's ~300 MB before the last URL goes stale. Minting URLs in batches as
  the client walks the recipe would decouple the two.

The `exp` claim and the row's `expires_at` are a **single source of truth** —
both computed once in `create_link`, so the JWT and the DB can never disagree.
A **TTL index** on `share_links.expires_at` reaps dead rows within about a minute
of expiry. It reclaims storage, not authority: `resolve_link` already rejects on
the `exp` claim, so the lagging row was inert before Mongo swept it.

**Tradeoffs (v1):**

- **Granting a nonexistent path** is silently accepted, creating a dangling grant
  that resolves to `404` on access.
- **Deleting a file purges its grants — but not atomically.** `DELETE /files` runs
  `purge_for_file` **before** `delete_file`. Purge-first fails safe: with no
  cross-collection transaction, a crash between the writes leaves a live file with
  no grants (recoverable), never a dead path with live grants (which would reattach
  on re-upload). `scripts/audit_storage.py --orphan-grants` reaps whatever the
  window leaves; closing it for good needs an immutable `file_id`.

---

## Sync client

A separate program (`client/`) that keeps a local folder in sync with the server —
the desktop-Dropbox piece. It talks **only** to the REST API and imports nothing
from `app/`, so a clean API is its entire contract. It does **delta sync**: it
chunks files itself and transfers only changed blocks, straight to/from B2.

```
file change      ──▶ watcher     ──▶ SyncEngine.push()   chunk → /blocks/missing → PUT missing → /files/commit
server "changed" ──▶ ws_listener ──▶ SyncEngine.pull()   /files/recipe → GET blocks (verify) → reassemble
```

- **`chunker`** splits files into hash-addressed blocks (its own copy of the 4 MiB
  scheme — the block hash is the shared contract with the server).
- **`LocalIndex`** remembers, per file, its last-synced content hash **and the
  server's etag** (persisted **outside** the synced folder), so `push` skips
  unchanged files without touching the network and knows the base etag for its next
  commit.
- **`push`** uploads only the blocks the server reports `missing`, then commits the
  recipe with `If-Match: <stored etag>` (or `If-None-Match: *` for a new file). On a
  `412`, `_reconcile` saves the server's version as `name (conflicted copy).ext`, then
  retries once off the current etag, so our bytes take the path and theirs survive in the copy.
- **`pull`** downloads a differing file's blocks from B2, **re-verifies** each
  (`sha256(block) == hash`) before reassembling — so a corrupt/poisoned block can
  never silently corrupt a file. It **never overwrites a local edit that `push` hasn't
  sent yet** (local content differs from what `LocalIndex` last synced). If the server
  changed that file too, it saves the server's version beside it as
  `name (conflicted copy).ext`, which the next `push` uploads, so both edits survive.
- **Deletes sync both ways**, using `LocalIndex` as the last-synced state. A path in
  the index but missing on disk was deleted locally, so `push` calls `DELETE /files`
  (a `404` counts as done). A path in the index but missing from the server was
  deleted remotely, so `pull` removes the local copy, unless it has an unpushed edit,
  which survives and is re-created on the next `push`. An edit made on another device
  wins over a local delete that `push` hasn't sent yet.
- **`watcher`** (watchdog) fires `push()` on any file event.
- **`ws_listener`** holds a WebSocket to `/ws` and fires `pull()` on every server
  `"changed"` push — plus once on (re)connect, to reconcile anything missed while
  it was disconnected. It reconnects on its own every 3s if the socket drops, reading
  the current access token on each attempt; if the handshake is rejected (`403`, an
  expired token), it refreshes the token first.
- **Token refresh** — access tokens live 60 minutes, so `ApiClient` keeps the refresh
  token from `login()`. Any protected call that gets a `401` refreshes the access token
  and retries once, keeping its own headers (e.g. `If-Match`). If the refresh token has
  expired too (7 days), the client stops and needs a fresh login.

**v1 limitations** (documented in `sync.py`):

- **No pull-side delta** — pull re-downloads a differing file's blocks in full,
  with no local block reuse. (It *does* skip files whose local blocks already
  match the recipe.)
- **Conflicts resolve to copies, not merges** — when two devices edit the same file,
  both versions survive (one at the path, one as a `(conflicted copy)`), whether the
  conflict surfaces in `pull` or as a `412` in `push`. Merging them is left to the user.
- **A sent delete beats a concurrent edit** — once `push` has sent a delete, an edit
  another device commits around the same time is lost: `DELETE /files` takes no
  `If-Match`, unlike `commit`. Adding one (and a 412 → re-download path in the client)
  would make deletes as safe as edits.

---

## Configuration

All config comes from a `.env` file (see `.env.example`). Secrets are **required**
(no defaults) so the app refuses to start without them.

```
MONGODB_URI, MONGODB_DB
S3_ENDPOINT_URL, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_REGION, S3_BUCKET
S3_URL_TTL_SECONDS=300            # optional — presigned-URL lifetime; also the
                                  #   revocation window. Raise for slow, large downloads.
JWT_SECRET                        # required; generate: python -c "import secrets; print(secrets.token_urlsafe(48))"
JWT_ALGORITHM=HS256               # optional
ACCESS_TOKEN_EXPIRE_MINUTES=60    # optional
REFRESH_TOKEN_EXPIRE_MINUTES=10080 # optional (default 7 days) — refresh-token lifetime
SHARE_LINK_EXPIRE_MINUTES=10080   # optional (default 7 days) — public share-link lifetime
REDIS_URL                         # optional; set to enable multi-server realtime
                                  #   (Redis pub/sub). Unset = single-process in-memory bus.
```

> **macOS note:** MongoDB Atlas TLS requires `certifi` (`tlsCAFile=certifi.where()`),
> already wired in `dependencies.py`. The single `MongoClient` is shared by both
> repositories via a cached `_get_database()`.

---

## Running

### 1. Setup (once)

```bash
uv venv --prompt file-sync-service --python /opt/homebrew/bin/python3.12
source .venv/bin/activate
uv pip install -r requirements.txt
cp .env.example .env            # then fill in real values (see Configuration)
```

### 2. Launch the server

```bash
uvicorn app.main:app --reload --log-config log_config.yaml   # serves on http://127.0.0.1:8000
```

> **Multi-server (optional):** realtime works out of the box on one server via an
> in-process bus. To run multiple app servers, start Redis (`redis-server`) and
> set `REDIS_URL=redis://localhost:6379` — `notify` then fans out across servers
> via Redis pub/sub instead of staying local.

Open **http://127.0.0.1:8000/docs** for the interactive Swagger UI. Create an
account there (or via `curl`) before running the client:

```bash
curl -X POST http://127.0.0.1:8000/auth/register \
     -H "Content-Type: application/json" \
     -d '{"username": "alice", "password": "secret123"}'
```

### 3. Launch the sync client

With the server running and the venv active, point the client at a local folder:

```bash
python -m client \
    --server   http://127.0.0.1:8000 \
    --username alice --password secret123 \
    --folder   ~/FileSync
```

The client logs in, does an initial two-way sync, then **watches the folder** and
pushes changes as they happen — while a WebSocket to `/ws` pulls remote updates
the moment another device commits. Drop a file into `~/FileSync` and it uploads;
run a second client against a different folder (or machine) with the same account
and it appears there, without polling.

```bash
# one-shot sync instead of continuous watching:
python -m client --server http://127.0.0.1:8000 \
    --username alice --password secret123 --folder ~/FileSync --once
```

| Flag                        | Default                 | Meaning                                   |
|-----------------------------|-------------------------|-------------------------------------------|
| `--server`                  | `http://127.0.0.1:8000` | server base URL                           |
| `--username` / `--password` | *(required)*            | account to log in as (must already exist) |
| `--folder`                  | *(required)*            | local folder to sync                      |
| `--once`                    | off                     | sync once and exit (no watching)          |

### 4. Launch the web UI (optional)

A React + Vite single-page app in `frontend/` gives you a browser client with
the same delta engine as the Python client — chunking, dedup, and hash
verification all run in the browser, and block bytes go **browser → B2 directly**
via presigned URLs. It covers:

- **Auth** — register / login / logout. The access token is held in memory; the
  refresh token is in `localStorage`, so the session survives a reload.
- **Your files** — list, upload (block-level dedup), download (on-read hash
  verify), delete.
- **Shared with you** — a second tab: incoming grants (`/shares/incoming`), with
  hash-verified download from the owner's `<owner>/<hash>` namespace.
- **Sharing** — an expandable per-file panel: grant read-only access to another
  user, see **who the file is currently shared with** (`/shares/outgoing`) and
  revoke any of them, plus mint / copy / revoke public share-links.
- **Public links** — a no-auth `/?token=…` page that resolves a share-link and
  downloads the file (hash-verified) with no account.
- **Real-time** — both tabs hold a WebSocket (`/ws`), as does an open sharing panel.
  "Your files" refreshes when the same account commits or deletes elsewhere;
  "Shared with you" refreshes when someone grants, revokes, or deletes a file shared
  with you; the sharing panel refreshes when you grant, revoke, or mint a link from
  another device. A **commit** notifies only the owner — a recipient's list shows
  grants, not file contents, so a new version changes nothing they can see.

> **Dropped sockets** — sleep, proxy idle-timeout, redeploy — reconnect with
> exponential backoff (1s → 30s, jittered so a redeploy doesn't stampede every
> client at once). Nothing replays the notifications missed while away, so a
> successful *re*connect refetches. The socket stays an accelerator, not a
> dependency: every action also refreshes explicitly, so a wedged socket degrades
> the UI rather than corrupting it. (Why a socket and not SSE:
> [above](#why-websockets--and-why-sse-was-the-closer-call).)

> The download path — fetch each block, re-verify `sha256(block) == hash`,
> reassemble — lives once in `frontend/src/lib/download.ts` (`assembleBlocks`), shared
> by all three of the owned-file, shared-file, and public-link flows.

```bash
cd frontend
npm install                     # once
npm run dev                     # serves on http://localhost:5173
```

Keep the API server (step 2) running in another terminal. Vite proxies the API
routes (`/auth`, `/files`, `/blocks`, `/shares`, `/link`, `/ws`) to
`http://127.0.0.1:8000`, so there's no CORS setup for local dev. Open
**http://localhost:5173** and register or log in.

> Because the browser PUTs/GETs block bytes straight to B2, the bucket needs a
> CORS rule allowing your dev origin (`http://localhost:5173`) for `GET`, `PUT`,
> and `HEAD`. Add your production origin to that rule when you deploy.

| Script            | What it does                                       |
|-------------------|----------------------------------------------------|
| `npm run dev`     | dev server with hot-reload on `:5173`              |
| `npm run build`   | type-check (`tsc`) then bundle to `frontend/dist/` |
| `npm run preview` | serve the production build locally                 |

## Demo: the whole system via `curl`

With the server running (and `jq` installed), this walks through upload →
share → read-as-another-user → public link → revoke-link → revoke-grant. It
uses a small single-block file, so the block hash is just `sha256(file)`.

```bash
BASE=http://127.0.0.1:8000

# 1. Two accounts
curl -s -X POST $BASE/auth/register -H 'Content-Type: application/json' \
     -d '{"username":"alice","password":"secret123"}'
curl -s -X POST $BASE/auth/register -H 'Content-Type: application/json' \
     -d '{"username":"bob","password":"secret123"}'

# 2. Log in (form-encoded, per OAuth2) and capture bearer tokens
ALICE=$(curl -s -X POST $BASE/auth/login \
     -d 'username=alice&password=secret123' | jq -r .access_token)
BOB=$(curl -s -X POST $BASE/auth/login \
     -d 'username=bob&password=secret123'   | jq -r .access_token)

# 3. Bob uploads a small (single-block) file via the delta flow
echo "hello shared world" > /tmp/demo.txt
HASH=$(shasum -a 256 /tmp/demo.txt | cut -d' ' -f1)
SIZE=$(wc -c < /tmp/demo.txt | tr -d ' ')

#   a) which blocks does the server still need?
curl -s -X POST $BASE/blocks/missing -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -d "{\"hashes\":[\"$HASH\"]}"
#   b) get a presigned PUT url and push the bytes straight to B2
PUT=$(curl -s -X POST $BASE/blocks/upload-urls -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -d "{\"hashes\":[\"$HASH\"]}" \
     | jq -r ".urls[\"$HASH\"]")
curl -s -X PUT "$PUT" --data-binary @/tmp/demo.txt
#   c) commit the recipe — If-None-Match: * says "create; fail if it exists"
#      (an update would send If-Match: "<etag>" from a prior recipe/list read)
curl -s -X POST $BASE/files/commit -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -H 'If-None-Match: *' \
     -d "{\"path\":\"/demo.txt\",\"size\":$SIZE,\"block_hashes\":[\"$HASH\"]}"

# 4. Bob shares /demo.txt with Alice
curl -s -X POST $BASE/shares -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' \
     -d '{"path":"/demo.txt","shared_with":"alice"}'

# 5. Alice reads Bob's file: recipe -> presigned GET -> bytes
curl -s "$BASE/files/recipe?owner=bob&path=/demo.txt" -H "Authorization: Bearer $ALICE"
GET=$(curl -s -X POST "$BASE/blocks/download-urls?owner=bob&path=/demo.txt" \
     -H "Authorization: Bearer $ALICE" -H 'Content-Type: application/json' \
     -d "{\"hashes\":[\"$HASH\"]}" | jq -r ".urls[\"$HASH\"]")
curl -s "$GET"                                   # -> hello shared world

# 6. Public share link — redeemable with NO auth header
TOKEN=$(curl -s -X POST $BASE/shares/link -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' -d '{"path":"/demo.txt"}' | jq -r .token)
curl -s "$BASE/link/recipe?token=$TOKEN"          # -> the recipe, no login

# 7. Bob lists his live links, then revokes this one by its jti
JTI=$(curl -s "$BASE/shares/link" -H "Authorization: Bearer $BOB" | jq -r '.[0].jti')
curl -s -X DELETE "$BASE/shares/link/$JTI" -H "Authorization: Bearer $BOB"
#   the SAME token is still cryptographically valid, yet its row is gone -> revoked
curl -s -o /dev/null -w '%{http_code}\n' "$BASE/link/recipe?token=$TOKEN"   # -> 404

# 8. Revoke Alice's grant — she now gets 404 on the shared file too
curl -s -X DELETE $BASE/shares -H "Authorization: Bearer $BOB" \
     -H 'Content-Type: application/json' \
     -d '{"path":"/demo.txt","shared_with":"alice"}'
curl -s -o /dev/null -w '%{http_code}\n' \
     "$BASE/files/recipe?owner=bob&path=/demo.txt" -H "Authorization: Bearer $ALICE"  # -> 404
```

> Files over 4 MiB span multiple blocks — the real client (`python -m client`)
> handles chunking, per-block negotiation, and reassembly for you.

## Testing

```bash
uv pip install -r requirements-dev.txt
pytest                          # backend + sync client, with coverage; fails under 90%

cd frontend && npm test         # web client unit tests (Vitest)
```

Tests mirror the `app/` and `client/` trees. B2 and HTTP are mocked or faked. The
suite needs a `.env`, and some router and dependency tests connect to the MongoDB it
points at.

---

## Scaling

A note on what it would take to run this for **large files** and **many users**.
The short version: the *data plane* already scales, because content addressing +
presigned direct-to-B2 transfer means **file bytes never touch the app server**.
The work is almost all in the *control plane*.

### Large files

Two things are already right:

- **Bytes bypass the app server** — block data goes browser/client → B2 directly;
  the API only moves tiny JSON.
- **Identical blocks transfer once** — dedup, within a user's namespace.

What starts to hurt:

- **Unbounded fan-out.** A 50 GB file is ~12,000 blocks — today that's one giant
  `upload-urls` response and (in the web client) 12,000 concurrent `PUT`s. Bound
  the in-flight concurrency to a small pool (~6–10) and mint presigned URLs in
  **batches** as the pool drains, rather than all at once.
- **No resumability.** A dropped connection restarts the transfer. Content
  addressing softens this (already-committed blocks are skipped by
  `/blocks/missing`), but large blocks want **S3 multipart upload**, and the
  client wants to **checkpoint** progress.
- **Fixed-size chunking defeats dedup on edits.** Inserting one byte at the front
  shifts every 4 MiB boundary, so every block hash changes. **Content-defined
  chunking** (rolling hash, à la restic/borg) is the fix — the `chunker.split()`
  seam is already isolated for exactly this swap.
- **Whole-file reassembly in memory.** The download path collects every block into
  one `Blob`/buffer — a huge file OOMs the tab or process. **Stream** to disk as
  blocks arrive (File System Access API / a `ReadableStream`) instead.

### Many users

Here the bottlenecks move to the app tier, Mongo, and the real-time layer:

- **Cross-server realtime fan-out — done.** This *was* the first thing to break
  on horizontal scale: `realtime.ConnectionManager` holds sockets in-process, so a
  commit on instance A couldn't notify a socket on instance B. It's now solved by
  the **`ChangeBus`** — `notify` publishes to Redis pub/sub and every server fans
  out `{"type":"changed"}` to its own sockets (see the Architecture "Realtime
  path"). Sticky WebSocket sessions are no longer needed. Falls back to an
  in-process bus when `REDIS_URL` is unset. What's left here is *tuning*, not
  architecture: per-user channels instead of one global channel if publish volume
  ever gets hot.
- **The app tier is nearly stateless already.** Auth is a stateless JWT and bytes
  bypass the app, so FastAPI instances scale horizontally behind a load balancer
  with autoscaling — and with the pub/sub bus shipped, that's now fully true.
- **Mongo indexing & sharding.** Every hot query is indexed — `files` on
  `(owner, path)` unique; `shares` on `(owner, path, shared_with)` unique (whose
  leftmost prefix also serves the `(owner, path)` purge) plus `shared_with` alone
  for the recipient's list; `share_links` on `jti` unique, `(owner, path)`, and a
  TTL on `expires_at`; `blocks` on `(owner, hash)` unique (the block index — a single
  `$in` lookup answers negotiation, replacing one B2 `HEAD` per hash). At very large
  scale, **shard on `owner`** so a user's files and blocks colocate.
- **CDN in front of B2.** Cache public-link downloads at the edge instead of
  re-fetching per request; presigned GETs work behind a CDN.
- **Garbage collection that scales.** Now that file delete ships, orphaned blocks
  are real. The one-shot full-scan audit (`scripts/audit_storage.py` — phantom
  files, orphaned blocks, orphaned grants) is fine at this size but won't scale —
  the endgame is **per-block reference counting** or an incremental mark-and-sweep,
  not a full bucket + collection scan.

The highest-leverage step — the **Redis pub/sub bus** behind `ConnectionManager`,
which unlocks running more than one server at all — is **done**. With the app tier
now horizontally scalable, the next control-plane investments are **Mongo indexing
& sharding** and **reference-counted GC**, both of which matter only as data
volume grows.

---

## Status & roadmap

**Done:**

- **Core storage** — content-addressed blocks, chunking/dedup, REST API, JWT auth,
  ports & adapters throughout.
- **Delta sync** — client-side chunking, have/need negotiation, presigned
  direct-to-B2 transfer, per-user block scoping, re-verify-on-read.
- **Sync client** — folder watcher, push/pull, WebSocket-driven pull.
- **Real-time** — `ChangeBus` over Redis pub/sub, so a commit on one server reaches
  a device on another. Each route notifies exactly the users whose view changed.
  The client socket reconnects with jittered exponential backoff and refetches on reopen.
- **Sharing** — user-to-user grants and public share-links, both revocable, links
  also expiring. Authorization never touches storage. Revocation closes the read
  window in 5 minutes (`s3_url_ttl_seconds`), bounded below by download speed.
- **File deletion** — owner-scoped, cascades to grants and links before the recipe.
  The sync client propagates deletes in both directions.
- **Optimistic concurrency** — every file carries an `etag = sha256(recipe)`; `commit`
  is an `If-Match`/`If-None-Match` conditional write (single-doc CAS), so concurrent
  writers get `412` instead of silently clobbering. Both clients send the precondition
  and reconcile on conflict; identical re-commits fold into a no-op. The sync client
  keeps the other side's version as a `(conflicted copy)`, so neither edit is lost.
- **Block index** — a Mongo `(owner, hash)` collection (`BlockIndex` port) makes
  `/blocks/missing` a single `$in` query instead of one B2 `HEAD` per hash; `commit`
  verifies only not-yet-indexed blocks in B2, keeping the index a strict subset of B2.
- **Refresh tokens** — `jti` allowlist with TTL reap; `typ`-guarded against
  cross-use; the web session survives reload and auto-refreshes on a 401, and so does
  the sync client (HTTP calls and WebSocket reconnects).
- **Web UI** — React + Vite: tabbed "Your files" / "Shared with you", upload,
  hash-verified download, delete, full sharing, no-auth public download page.
- **Indexes** — unique on `username`, `jti`, `(owner, path)`, the grant triple, and
  `(owner, hash)` (block index); TTL reap on expiring `refresh_tokens` and `share_links`.
- **100% backend coverage**, plus frontend unit tests.

**Next:**

- Content-defined chunking (so delta survives insertions). Fixed 4 MiB offsets mean
  a one-byte insertion at the front rewrites every block hash.
- Pull-side delta (reuse local blocks instead of re-downloading a changed file) —
  worth little until chunking is content-defined, since today an insert invalidates
  every block anyway.
- Streaming chunking for very large files (avoid reading whole file into memory).
- Orphaned-block garbage collection — now unlocked, since delete is the first
  operation that creates real orphans. `scripts/audit_storage.py` is the cleanup
  path (it already reaps orphaned grants too); a scheduled mark-and-sweep GC
  finally has a use case.

**Hardening backlog:**

- File versioning (keeping prior recipes), the natural extension of conflict copies:
  any overwritten version stays recoverable, not just the ones a conflict caught.
- **Auth hardening.** Deferred as operational/feature security, not architecture —
  the auth *model* (stateless JWT + bcrypt + a `jti`-allowlisted refresh token) is in
  place; these are policy knobs layered on top:
    - **Access-token revocation** — the access JWT is stateless, so logout kills only
      the refresh `jti`; a stolen access token stays valid to its `exp` (60 min). The
      fix trades a little statelessness for a check — a short-TTL denylist of revoked
      `jti`s (Redis), or shrinking the access-token lifetime toward the refresh cadence.
    - **Refresh-token rotation** — refresh tokens are non-rotating, so a stolen one is
      reusable for its whole 7-day life with no theft signal. Rotation mints a fresh
      `jti` per refresh and retires the old; a *reused* retired `jti` flags a compromise.
    - **httpOnly-cookie storage** — the web client keeps tokens in `localStorage`
      (XSS-readable); `httpOnly` cookies remove that surface, at the cost of adding CSRF
      defenses.
    - **Login rate-limiting** — `/auth/login` has no throttle or lockout; bcrypt slows a
      single guess but nothing caps attempt *volume* (credential stuffing). The fix is a
      per-IP / per-account limiter — a Redis token bucket (the pub/sub Redis is already
      wired) plus optional backoff/lockout.
- **Resource limits & quotas.** There's no cap on what a client can ask for or
  store — deliberately deferred as abuse-hardening / product policy, not core
  architecture (the data plane already scales; bytes bypass the app server). Three
  independent, well-understood knobs, none in place:
    - **Request shape** — `BlockHashesRequest.hashes` and `CommitFileRequest.block_hashes`
      have no length bound, so a giant `hashes` list is an amplification vector (one
      request → *N* B2 HEADs, or an unbounded Mongo recipe doc). Closable at the
      validation boundary with a Pydantic `Field(max_length=…)` → automatic `422`.
    - **Bytes per upload** — the presigned **PUT** carries no size limit, so a client
      can PUT arbitrary bytes (and never commit, orphaning them). The real fix is a
      presigned **POST** with a `content-length-range` policy S3 enforces — pending
      confirmation that B2's S3 API honors POST Object; otherwise a sweep in
      `scripts/audit_storage.py` reaps over-size/orphaned blocks after the fact.
    - **Per-user storage quota** — no running per-owner byte total, so nothing rejects
      the *N*-th GB. Needs a counter keyed on `owner` (logical sum of file sizes is the
      cheap first cut; physical/deduped needs per-block reference counting).
