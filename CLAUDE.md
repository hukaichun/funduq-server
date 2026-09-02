# Working on AgentSoukServer

Notes that were expensive to learn. Everything here comes from a mistake
actually made in this repo, not from general principle.

## How things run here

Three rules, and they are not stylistic:

- **The gateway and every provider run in `docker compose`.** `souk`,
  `souk-migrate`, `paradedb` and `docent` are all services in
  `docker-compose.yml`. Bring the stack up with `docker compose up
  --build`; that is the thing being developed, so it is the thing to run.
  Hand-starting a uvicorn on a spare port and a provider subprocess beside
  it proves the pieces work in an arrangement nobody deploys — it will
  miss service names (`http://souk:8000` resolves in compose and nowhere
  else), volume-persisted identity keys, and startup ordering.
- **Anything Python goes through `uv`.** `uv sync --group dev`, `uv run
  pytest`, `uv run souk-server`, `uv run python -m funduq.migrate`.
  Never a bare `python`,
  `pip` or a manually activated venv: each subproject
  (`souk-agent-sdk/`, `souk-client-sdk/`, `agent-template/`,
  `providers/*`) has its own environment, and `uv run` from that
  directory is what picks the right one.
- **Environment variables come from a file, via `uv run --env-file`.**
  ```bash
  uv run --env-file ../../.env pydantic-ai-agent
  ```
  Not `export`, and not hand-parsing the file: `.env` values here are
  quoted (`LLM_BASE_URL = "https://..."`), and a naive `split("=")`
  hands the URL to httpx with its quotes still attached, which surfaces
  three layers away as "connection error" from the model. `--env-file`
  gets this right; a probe that re-implements it gets it wrong.

## Verify by running something

Inherited from upstream and re-earned here several times over. Reading
produced confident wrong answers; a throwaway probe found the defect.

- The MCP docent's `browse_souk` took no arguments, and a live model
  called it with `{}""` — invalid JSON, retried once, identical, run
  dead. Every tool taking a parameter was called cleanly. No unit test
  could find this: the tool worked perfectly against `mcp.Client` *and*
  against `fastmcp` directly. Only a real model failed it.
- Config-driven agents could not declare skills at all — `AgentHandle`
  had the field, `AgentConfig` did not, `main.py` never passed one — so
  every agent registered through the runner was findable only by someone
  who already knew its name. Found by asking the docent to find itself.
- `search_agents` matched whole phrases only, so "who can help with
  poetry" found nothing while the stall it wanted sat there tagged
  `poetry`. The caller is a model relaying a person; it passes sentences.

When you catch yourself about to write "this should work", write the
probe instead — and run it through compose, per the rules above.

**And then do not call the probe verification.** A passing probe proves
one ordering, the one you happened to write. The gateway once sent a
provider its first `run` frame *before* the `welcome`, because the broker
starts offering inside `attach_provider`'s own awaits — and any client
reading exactly one frame there raises and reconnects into the same race
forever. The probe missed it by starting its run after connecting, which
is the single ordering a real provider cannot rely on. The test suite
found it in one line. The two methods catch different sets and neither
catches all of it; "verified end to end" is a claim about one path.

## Docker

- Every `[tool.uv.sources]` path entry needs its own `COPY` in the
  Dockerfile. A missing one fails at `uv sync` with "Distribution not
  found at: file:///app/…", during build, long before any import — which
  reads as a broken image rather than as a missing line. The path
  sources are all in-repo now (`souk-agent-sdk/`, `souk-client-sdk/`;
  upstream funduq comes from PyPI and needs no COPY), but the lesson
  stands: both images here were missing the provider-SDK COPY the day
  that SDK arrived as a path dependency.
- `docker compose run -v "$PWD/x:/app/y"` resolves `$PWD` in *this
  shell*, whose directory persists across commands. Pointed at a path
  that does not exist, Docker creates it — root-owned — so a stale `cd`
  leaves a directory named after your config file somewhere unrelated.
  Removing it needs a container (`docker run --rm -v "$PWD/dir:/w" alpine
  rm -rf /w/thing`), not `rm`.

## Testing

- Run the suite on **both** backends. SQLite is the default;
  `SOUK_DATABASE_URL=postgresql+psycopg://…` for the other (`docker
  compose up paradedb -d`). Dialect bugs only appear on one side.
- **A green suite does not mean the app starts.** Nothing under `tests/`
  imports `souk_server/server.py`. After any broad edit, build the app:
  ```bash
  uv run python -c "from funduq.config import CoreSettings; from funduq.core import Funduq; from souk_server.server import create_app; create_app(Funduq(CoreSettings(token_signing_secret='x', identity_private_key='11'*32))); print('app builds')"
  ```
- WebSocket tests drive the real ASGI app over `httpx-ws` in the same
  event loop as the `souk` fixture. A threaded test client would be
  driving the broker's loop-bound queues cross-loop.
- The MCP client holds an anyio task group, so it is entered *inside*
  each test rather than supplied as a fixture — pytest-asyncio can
  finalise a fixture from a different task than it set up in, which a
  cancel scope cannot survive.

## Design invariants

Breaking one has caused a real bug here or upstream.

- **This repo owns both ends of every wire it defines.** Gateway, both
  SDKs, the reference providers and the directory UI live here; upstream
  is [hukaichun/funduq](https://github.com/hukaichun/funduq) — core, the
  contract and their docs, nothing network-facing (funduq#27) — and it
  arrives as PyPI packages (`funduq`, `funduq-provider-sdk`,
  `funduq-contract`), not as a submodule. `docs/server-mode.md` is the
  spec of record for the frame protocol.
- **Core is network-free, and this is where every I/O decision lives.**
  Ports, TLS, CORS, framing, edge auth. `create_app` binds nothing.
- **Serving state stays out of core's database.** No gateway table in
  core's schema (the migration chain ships inside the funduq wheel — no
  revision of ours could reach it anyway; the rule is about the schema,
  not the directory), and no code path putting core state and serving
  state in one transaction — see `docs/server-mode.md`.
- **The docent gives directions and stops.** No MCP tool may run,
  resume or cancel anything; invocation is A2A's, which souk already
  serves without deviation. A test asserts the tool list.
- **Skills only reach souk through `agent_card_extra`.**
  `repo.register_agents` builds the agent card from name + description +
  `agent_card_extra` and silently drops everything else.

## Where the design lives

`docs/server-mode.md` — the wire, the MCP docent, the DB boundary, and
the decisions that were tried and rejected. Read it before changing the
frame protocol, the docent's surface, or anything about persistence; if
the code contradicts it, one of them needs fixing, deliberately.
