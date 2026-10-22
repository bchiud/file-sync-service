# File Sync Service — High-Level Design

A content-addressed file storage and sync system: a **FastAPI server** that stores
files as hash-addressed blocks, plus two clients — a **folder-watching sync daemon**
and a **React web app** — that share one delta-sync engine.

This doc covers *what* the system is and *why* it's shaped the way it is. The
[README](./README.md) covers running it and the API reference.

---

## 1. Goals

- **Efficient sync** — transfer only what changed, and store identical data once.
- **Integrity** — a reader can always prove the bytes it got are the bytes that were
  stored, end to end.
- **Keep bulk data off the app server** — the API moves metadata; file bytes go
  client ↔ object store directly.
- **Swappable infrastructure** — the core logic must not know it's talking to B2,
  Mongo, or Redis.
- **Multi-client, real-time** — a change on one device shows up on the others without
  polling.

**Non-goals (v1):** merging conflicting edits and file versioning (a conflict keeps both
versions, one as a `(conflicted copy)`), content-defined chunking, resumable transfers,
write-sharing (all sharing is read-only). See
[§7](#7-known-limitations--tradeoffs).

---

## 2. The one core idea: content-addressed storage

Every file is split into fixed-size **4 MiB blocks**. Each block is stored under the
**SHA-256 hash of its contents** — the key *is* the hash. A file is then just an ordered
list of block hashes (its "recipe"), stored as metadata.

```
file bytes ──chunk──▶ [block, block, block]
                         │      │      │
                       sha256 sha256 sha256
                         ▼      ▼      ▼
   recipe:  ["3af…", "9c1…", "70b…"]     ← metadata in MongoDB
   blocks:  key = <owner>/<hash> → bytes ← object store (B2)
```

This single mechanism buys the three goals at once:

- **Deduplication** — an identical block is stored once (per user).
- **Delta sync** — editing part of a file only moves the changed blocks.
- **Integrity** — the key is a checksum of the contents; readers re-verify
  `sha256(block) == hash` on every download.

**Per-user block scoping** — keys are namespaced by owner (`<owner>/<hash>`), taken
from the authenticated token, never from client input. This is what makes
client-driven *direct-to-store* uploads safe: it removes the cross-user dedup side
channel and confines a bad block to its uploader's namespace. The cost is losing
cross-user dedup — an accepted trade.

---

## 3. System overview

```
   ┌────────────┐     ┌────────────┐          Two clients, one delta engine.
   │ sync client│     │  web app   │          Bytes go client → B2 directly.
   │ (watchdog) │     │(React+Vite)│
   └─────┬──────┘     └─────┬──────┘
         │  HTTP/WS: JSON only (hashes, recipes, presigned URLs)
         └────────┬─────────┘
                  ▼
         ┌──────────────────┐
         │  FastAPI server  │   control plane: auth, negotiation, metadata, authz
         │  (stateless)     │
         └───┬─────────┬────┘
             │         │
        ┌────▼───┐ ┌───▼────┐ ┌───────────┐
        │ Mongo  │ │  B2    │ │   Redis   │   metadata · block bytes · realtime pub/sub
        │ (meta) │ │(blocks)│ │(ChangeBus)│
        └────────┘ └───▲────┘ └───────────┘
                       │
     client ──presigned PUT/GET──┘   block bytes bypass every server layer
```

**Data plane vs. control plane** is the organizing split. The app server is pure
*control plane*: it negotiates *which* blocks to move and mints presigned URLs, but the
*data plane* (the actual bytes) flows client ↔ B2 directly. This is why the design
scales for large files almost for free — the server never touches a payload.

---

## 4. Architecture: hexagonal (ports & adapters)

Dependencies point **inward**. Outer layers depend on inner ones, never the reverse.

| Layer | Directory | Knows about | Examples |
|-------|-----------|-------------|----------|
| **Presentation** | `app/routers/` | HTTP, FastAPI | `files`, `blocks`, `auth`, `shares`, `link`, `ws` |
| **Application** | `app/application/` | ports, domain | `FileService`, `AuthService`, `ShareService` |
| **Domain / Models** | `app/domain/`, `app/models/` | nothing external | `security` (JWT/bcrypt), Pydantic models |
| **Ports** | `app/ports/` | — (abstract ABCs) | `BlockStore`, `FileRepository`, `ChangeBus`, … |
| **Adapters** | `app/adapters/` | B2, Mongo, Redis | `B2BlockStore`, `MongoFileRepository`, `RedisChangeBus` |

- **Composition root** — `dependencies.py` wires adapters → services (lazy `@lru_cache`
  singletons). Swapping B2 for local disk, or Mongo for Postgres, means writing one
  adapter and changing one line here.
- **No framework leakage** — domain and application layers never import FastAPI, boto3,
  or pymongo. The core is unit-tested against in-memory fakes; no cloud required.
- **Two thin cross-cutting modules** — `auth_dependencies.py` (bearer token → user) and
  `realtime.py` (`ConnectionManager` + `Notifier`).

The clients follow the same discipline: the sync client and web app import **nothing**
from `app/`. The block-hash contract and the REST API are their entire coupling.

---

## 5. Key flows

**Delta upload** (bytes never touch the app server):

```
1. client chunks file → block hashes (computed client-side)
2. POST /blocks/missing        → server: which of these do you not have?
3. POST /blocks/upload-urls    → server: a presigned PUT per missing hash
4. client → PUT bytes → B2 directly   (only the missing blocks)
5. POST /files/commit          → server verifies all present, saves the recipe
```

**Delta download** — get the recipe, get presigned GETs, fetch each block from B2,
**re-verify the hash**, reassemble. This one path lives once
(`frontend/src/lib/download.ts`, and its Python twin) and serves owned, shared, and
public-link reads alike.

**Real-time** — a commit/delete calls `Notifier.notify(user)`, which publishes onto the
`ChangeBus`. Every server runs a subscribe-loop that fans `{"type":"changed"}` out to
*its own* WebSockets, so a change on server A reaches a device on server B. The payload
is empty on purpose — the client just refetches. Redis pub/sub in production;
in-process bus when `REDIS_URL` is unset.

---

## 6. Data & auth model

**Metadata in MongoDB**, schema-as-Pydantic (`.model_dump()` / `Model(**doc)` at the
adapter boundary — no ORM, no migrations). NoSQL fits because every query is a
single-collection point lookup (no joins anywhere), recipes embed naturally as an
inline array, and TTL indexes reap expiring rows for free. The accepted cost is
hand-rolled referential integrity (the delete cascade is application-enforced, not
DB-enforced).

Collections: `files` (recipe by `(owner, path)`), `users`, `shares` (user-to-user
grants), `share_links` (one row per live public link, keyed on `jti`), `refresh_tokens`
(a `jti` allowlist).

**Auth** — bcrypt passwords, stateless JWT (HS256) access tokens; `owner` on every write
is taken from the token, never the body. Denials return **404, never 403**, so access
checks never reveal a file exists.

**Sharing is an authorization layer, not a storage feature.** It only decides *which
owner's namespace* a read resolves to — sharing never touches `FileService`, and
per-user block scoping makes "read-only" fall out as *less code* (there is no
`can_write`). Two flavors: user-to-user grants (durable, revocable) and public
share-links (the signed token *is* the capability; expiring + revocable via a
`jti` allowlist row).

- **A read is scoped to one file, not the owner's namespace** — `download-urls`
  presigns only hashes in the authorized file's recipe, so a block hash alone is never
  a bearer capability.
- **Token types don't cross** — access, refresh, and share tokens share `JWT_SECRET`,
  so a `typ` claim checked on decode stops one being redeemed as another.
- **Concurrent writes** — `commit` is an `If-Match` / `If-None-Match` compare-and-swap
  on `etag = sha256(recipe)`, a single-document `replace_one` filtered on the etag, so
  it's atomic without a transaction.

---

## 7. Known limitations & tradeoffs

| Area | v1 behavior | The real fix |
|------|-------------|--------------|
| **Chunking** | Fixed 4 MiB offsets — a 1-byte insert rewrites every hash | Content-defined chunking (rolling hash); the `split()` seam is isolated for it |
| **Conflicts** | Both versions kept (one as a `(conflicted copy)`); no merge, no history | File versioning (keep prior recipes) |
| **Large downloads** | Whole file reassembled in memory | Stream to disk as blocks arrive |
| **Transfers** | Not resumable; fan-out unbounded | S3 multipart + checkpointing; bounded concurrency, batched URL minting |
| **Revocation** | Stops *future* reads; already-issued presigned URLs live out their TTL (default 300s) | Bounded by design; batch-mint URLs to shrink the window |
| **Grant identity** | Keyed on `(owner, path)` — a re-uploaded path can resurrect old grants | Immutable surrogate `file_id` minted at first commit |
| **Orphaned blocks** | Full-scan audit script (`scripts/audit_storage.py`) | Reference-counted or incremental mark-and-sweep GC |

---

## 8. Scaling posture

The **data plane already scales** — content addressing + presigned direct-to-B2 transfer
means file bytes never hit the app server, and identical blocks move once. The remaining
work is all *control plane*:

- **App tier is effectively stateless** (JWT auth, bytes bypass it) → horizontal
  autoscaling behind a load balancer.
- **Cross-server realtime is done** — the `ChangeBus` over Redis pub/sub was the one
  thing blocking >1 server; sticky sessions are no longer needed.
- **Next investments, as volume grows:** Mongo sharding on `owner` (colocates a user's
  files and grants), a CDN in front of B2 for public-link reads, and reference-counted
  GC to replace the full-scan audit.

---

## 9. Technology choices at a glance

| Concern | Choice | Why |
|---------|--------|-----|
| API server | FastAPI (Python 3.12) | First-class async + WebSockets, Pydantic-native |
| Block store | Backblaze B2 (S3-compatible) | Presigned direct transfer; swappable via one adapter |
| Metadata | MongoDB Atlas | Point-lookup access pattern, embedded recipes, TTL reaping |
| Realtime | WebSockets + Redis pub/sub | Simpler server side in FastAPI than SSE; Redis fans out across servers |
| Auth | JWT (HS256) + bcrypt | Stateless verification; no session store |
| Sync client | watchdog + httpx | Folder watch → push; WebSocket → pull |
| Web app | React + Vite + TypeScript | Same delta engine in the browser (Web Crypto for hashing) |

**WebSocket vs. SSE** was a close call. The traffic is one-way (server pushes
`{"type":"changed"}`, client refetches), which is exactly SSE's shape.

- **For SSE:** `EventSource` reconnects for free; the WebSocket reconnect (backoff,
  jitter) had to be hand-rolled.
- **A wash:** neither can set auth headers, so both put the token in the query string.
  SSE's `Last-Event-ID` replay isn't wanted either, since refetching on reconnect
  reconciles against current state.
- **For WebSockets:** FastAPI automates the upgrade, framing, and disconnect detection
  (`WebSocketDisconnect`). SSE needs a hand-rolled `text/event-stream` generator with
  `is_disconnected()` polling.

WebSockets stayed because the server side is simpler and the reconnect is already
written and tested.

---

## 10. Hardening backlog

Deferred as policy layered on the existing model, not architecture:

- **Access-token revocation** — logout kills only the refresh `jti`; a stolen access
  token is valid to its `exp` (60 min). Fix: a short-TTL Redis denylist, or shorter
  access tokens.
- **Refresh-token rotation** — refresh tokens don't rotate, so a stolen one works for
  7 days with no theft signal. Fix: mint a new `jti` per refresh; reuse of a retired
  one flags compromise.
- **httpOnly cookies** — the web client keeps tokens in `localStorage` (XSS-readable).
  Cookies remove that, at the cost of CSRF defenses.
- **Login rate-limiting** — no throttle on `/auth/login`. Fix: a per-IP/per-account
  Redis token bucket.
- **Request size bounds** — `hashes` / `block_hashes` lists are unbounded (one request
  can trigger *N* B2 `HEAD`s). Fix: Pydantic `Field(max_length=…)`.
- **Upload size limits** — a presigned PUT carries no size limit. Fix: presigned POST
  with `content-length-range` (if B2 supports it), else an audit sweep.
- **Per-user quotas** — no per-owner byte total. Fix: a counter keyed on `owner`.
- **File versioning** — keep prior recipes, so any overwritten version is
  recoverable, not just conflict copies.
</content>
</invoke>
