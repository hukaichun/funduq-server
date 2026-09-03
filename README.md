# Agent Souk Server

**The reference [funduq](https://github.com/hukaichun/funduq) gateway — every network decision, in one place.** One HTTP surface serving humans (AG-UI SSE), agents (A2A v1.0 JSON-RPC), and the outbound relay that lets providers behind NAT serve agents without public IPs, open ports, or tunnels.

---

## Two projects, one boundary

This repo is the *serving* half. Upstream — [funduq](https://github.com/hukaichun/funduq) — is the domain, and it arrives here as ordinary PyPI packages; the split is a hard line, recorded in [funduq#27](https://github.com/hukaichun/funduq/issues/27):

| | **[funduq](https://github.com/hukaichun/funduq)** (upstream, from PyPI) | **AgentSoukServer** (this repo) |
|---|---|---|
| **Owns** | The domain: agents, threads, runs, identity, persistence, protocol *translation* | The network: ports, transports, TLS, CORS, endpoints, wire framing, admin surface — **both ends of every wire** |
| **Ships** | `funduq` (network-free core), `funduq-provider-sdk`, `funduq-contract` | The gateway process assembled from core, plus the client SDKs that speak its wire ([`souk-agent-sdk/`](souk-agent-sdk), [`souk-client-sdk/`](souk-client-sdk)) |
| **May it bind a socket?** | Never — enforced by packaging and test | Yes — that is its entire job |

Two consequences worth knowing before touching anything:

- **The wire contract is authored here, and so are both sides of it.** [`docs/server-mode.md`](docs/server-mode.md) is the spec of record — single HTTP port, WebSocket relays for providers and KYOK bridges, the ticket handshake. The SDKs that implement that spec live in this repo too ([`souk-agent-sdk/`](souk-agent-sdk) for providers, [`souk-client-sdk/`](souk-client-sdk) for callers and their KYOK bridges): upstream keeps no network code at all, client side included. The signed payloads inside the frames are upstream's, pinned at **contract revision 17** and vendored as [`docs/upstream-contract-vectors.json`](docs/upstream-contract-vectors.json). Upstream withdrew its own frame vocabulary and protocol machines at revision 11 — over a wire the provider-initiated calls are plain request/response — so the framing on this wire is stated and tested here; what it publishes instead is every crossing shape as a pydantic model. The one ordering that turned out to live inside core rather than on the wire went back there at revision 17 ([funduq#249](https://github.com/hukaichun/funduq/issues/249)).
- **funduq core arrives from PyPI** (`funduq`, version-pinned in `pyproject.toml`). This repo contains no domain logic — it lifts headers, frames responses, binds sockets, and hands everything else to core.

---

## Quick start

```bash
git clone git@github.com:hukaichun/AgentSoukServer.git
cd AgentSoukServer
```

Then, in three commands:

```bash
uv sync --group dev
uv run python -m funduq.migrate     # one-time DDL step; the chain ships inside the funduq wheel
FUNDUQ_TOKEN_SIGNING_SECRET=dev \
FUNDUQ_IDENTITY_PRIVATE_KEY=$(uv run python -c "from funduq.identity import FunduqIdentity; print(FunduqIdentity.generate_hex())") \
  uv run souk-server                # everything on :8000
```

(The identity key is required, and in a real deployment it is the *same* value across restarts and replicas — providers pin it. Generate once, keep it.)

Verify it's alive:

```bash
curl http://localhost:8000/healthz && curl http://localhost:8000/readyz
```

---

## What this process is

```mermaid
graph TD
    User([Human / Web Directory]) -->|"POST /agui/{agent} (SSE)"| HTTP["FastAPI surface — one listener<br/>:8000"]
    CallerAgent([External Agent]) -->|"POST /a2a/{agent}/rpc"| HTTP
    Bridge([Caller's KYOK bridge]) ==>|"WS /ws/kyok"| HTTP

    subgraph Process ["souk-server (single process)"]
        HTTP --> Core["funduq core (from PyPI)<br/>broker · handlers · protocol adapters"]
    end

    Process --> DB[(SQLite / Postgres)]
    HTTP <== "WS /ws/provider (outbound)" ==> Providers["Providers behind NAT<br/>(souk-agent-sdk)"]
```

`create_app(souk, serving)` returns a plain ASGI app that binds nothing — mount it inside a larger app, wrap it in your own middleware (pure ASGI, not `BaseHTTPMiddleware`: that class buffers streams and never sees WebSocket scopes), or let the `souk-server` console script serve it. Every I/O decision — which framework, which port, which TLS story — is made here so that core never has to.

**Server mode is live** ([`docs/server-mode.md`](docs/server-mode.md)): providers and KYOK bridges each hold a WebSocket on the one HTTP port (`/ws/provider`, `/ws/kyok` — JSON frames, the v4 ticket handshake). One port, one TLS certificate, any reverse proxy (`wss` is a plain HTTP/1.1 upgrade), and a browser can be a provider. The MCP docent rides the same listener at `/mcp`.

---

## Configuration

Two env families now — [.env.example](.env.example) documents both (nothing auto-loads it; it's for `export` / compose). The prefixes mirror the project boundary — and since contract revision 14 core reads no environment at all (`CoreSettings.from_env` is gone), so **the gateway reads both families**: the names and defaults are unchanged, the reading moved to the half of the system that was always allowed to know about deployments.

| Layer | Variables | Examples |
|---|---|---|
| **Core** (`CoreSettings`, upstream — read **here**, by `souk_server.config.core_settings_from_env()`) | database, domain policy, keys | `FUNDUQ_DATABASE_URL` (unset = zero-config SQLite `./funduq.db`), `FUNDUQ_DB_SCHEMA`, `FUNDUQ_TOKEN_SIGNING_SECRET` (**required**), `FUNDUQ_IDENTITY_PRIVATE_KEY` (**required** — the funduq's own Ed25519 identity; providers pin it, so it must be stable across restarts and replicas), plus the optional broker waits `FUNDUQ_UNSERVED_TIMEOUT_SECONDS` / `FUNDUQ_DELIVER_TIMEOUT_SECONDS` / `FUNDUQ_UNDELIVERED_WINDOW_SECONDS` |
| **Serving** (`ServingSettings`, here) | everything that only means something once there's a socket | `SOUK_HTTP_PORT`, `SOUK_PUBLIC_HTTP_URL`, `SOUK_CORS_ALLOW_ORIGINS`, `SOUK_HTTP_TLS_CERT_PATH`/`_KEY_PATH` |

---

## TLS is required off localhost

Not hardening advice — specific threats: session and KYOK tokens are **bearer credentials**, and `POST /tickets` is where a provider learns the funduq public key its connect proof will name — on a plaintext path, anyone in the middle reads a token outright or substitutes the key at the one moment it is taken on faith. TLS is what makes the ticket channel the trustworthy out-of-band step the handshake assumes. The server logs a warning when it binds HTTP without it.

Two supported terminations — pick one, but off-localhost you need one:

- **At the gateway**: `SOUK_HTTP_TLS_CERT_PATH` / `SOUK_HTTP_TLS_KEY_PATH` with a real CA-issued cert (dev pair: `uv run python scripts/gen_dev_tls_cert.py`).
- **At a reverse proxy** (nginx / caddy / cloud LB), gateway plaintext on an internal network. `wss` is a plain HTTP/1.1 upgrade — no HTTP/2 support required of the proxy.

---

## Docker

```bash
docker compose up --build
```

brings up the stack: **paradedb** (Postgres), **souk-migrate** (one-shot `python -m funduq.migrate` — the alembic chain ships inside the funduq wheel, no `alembic.ini` anywhere — then exits), **souk** (the gateway, after migration completes), and **docent** (the guide at the gate — needs `.env` with LLM credentials; see `.env.example`).

For a market with something in it, add the demo profile:

```bash
docker compose --profile demo up --build
```

which opens three more stalls beside the docent — **Zahra's Tongues** (a plain translator and a haggler), **Yusuf's Workshop** (a poetry translator and a scribe) and **The Midnight Tea House** (a storyteller). Six agents, four stalls, two of them with more than one agent and one with only one; Zahra's and Yusuf's both call their translator `translator`, so the ambiguous-name path is live rather than theoretical; and the haggler delegates across stalls to Yusuf's scribe, which shows up as real lineage under `GET /threads/{id}/tree`. The migration is deliberately its own service — DDL runs with different credentials than the DML-only role the server needs; the gateway never creates tables at startup.

The images install upstream (`funduq` and friends) from PyPI during the build — a plain `git clone` is all the checkout the build needs.

---

## Tests

SQLite by default; the same suite runs against Postgres, and both must pass — dialect bugs only ever appear on one side:

```bash
uv run pytest
docker compose up paradedb -d
SOUK_DATABASE_URL=postgresql+psycopg://souk:souk@localhost:5433/souk uv run pytest
```

A green suite does **not** import `souk_server/server.py` — after broad edits, also prove the app assembles:

```bash
uv run python -c "from funduq.config import CoreSettings; from funduq.core import Funduq; from souk_server.server import create_app; create_app(Funduq(CoreSettings(token_signing_secret='x', identity_private_key='11'*32))); print('app builds')"
```

---

## Roadmap

From [`docs/server-mode.md`](docs/server-mode.md); the transport work is done:

1. **`WS /ws/provider`** — landed, now on wire **v4**: the ticket handshake (`POST /tickets` + two frames) and registration on the open link ([tests/test_ws_provider.py](tests/test_ws_provider.py)).
2. **`WS /ws/kyok`** — landed. Replaced the poll/respond pair; answers are only accepted on the connection each request was delivered to (a security fix, not just a transport swap — see the design note).
3. **gRPC stripped** — landed. Listener, stubs, deps, `:50051` all gone; the wire semantics live on in the ws frames and in upstream's contract vectors (vendored as [docs/upstream-contract-vectors.json](docs/upstream-contract-vectors.json)).
4. **MCP docent** (`/mcp`) — landed. Discovery, not invocation: who is in the souk, what each stall offers, and the A2A endpoint to go talk to them ([souk_server/mcp_docent.py](souk_server/mcp_docent.py)). Read-only; calling an agent stays A2A's job.
5. **Examples** — the `demo` compose profile and the SDK-free Go probe ([providers/pod-probe-agent](providers/pod-probe-agent)) landed; a browser provider and a managed-gateway embedding sample (edge auth + admin router over the `Funduq` facade) remain.

## License

**The gateway is [AGPL-3.0](LICENSE); the SDKs, the template and the
reference providers are [Apache-2.0](souk-agent-sdk/LICENSE).** What you
*run* is copyleft, what you *build against* is not — so a hosted, modified
souk stays open, while your own agent stays yours. funduq core is
Apache-2.0 upstream and unaffected.

See [LICENSING.md](LICENSING.md) for the per-directory map and the
reasoning. Nobody has to use the SDKs at all: the wire is documented in
[docs/server-mode.md](docs/server-mode.md), and a provider written from
that owes this repository nothing.
