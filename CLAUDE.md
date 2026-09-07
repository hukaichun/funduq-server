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
  else), volume-persisted identity keys, and startup ordering. One warning
  about that command as it stands: `souk-migrate` runs `python -m
  funduq.migrate`, and the current chain (alembic `a1f4c9d27e3b`)
  **deletes every row in `runs`, `run_events` and `thread_messages`** —
  upstream reshaped `runs` at contract revision 19 and deliberately did
  not migrate rows. Agents, providers and threads survive; conversations
  do not.
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

## Upstream's contract (currently revision 21)

The pin is `funduq` 0.0.8, `funduq-provider-sdk[llm]` 0.0.9,
`funduq-contract` 0.0.11. Read
[upstream's `docs/contract-changelog.md`](https://github.com/hukaichun/funduq/blob/main/docs/contract-changelog.md)
before moving it: it says what an implementation must change, which
commit subjects cannot. These bite in ways a green suite does not always
catch first:

- **Two dump rules that pull opposite ways.** A frame envelope is dumped
  `by_alias=True` and **never** `exclude_none` (`RunAgentInput`'s
  `forwardedProps` is legitimately null, and stripping it
  makes a good run come back as a *permanent refusal*); a typed AG-UI
  event is dumped **with** `exclude_none=True` (or `timestamp: null` and
  `rawEvent: null` land in the caller's stream). Upstream's codec used to
  enforce both and was withdrawn at revision 11 — nothing does now, so
  `docs/server-mode.md` is where the rules live.
- **Envelopes are flat and the models forbid extras.** A frame is
  `{"type": "run", **DeliveredRun}`, so strip `type` (and `requestId` on
  a `completionRequest`) before `model_validate`, or the frame fails
  validation on the field that routed it.
- **`takes_interjections` is not on the `ConnectedProvider` protocol**,
  and core calls it inside `register_agents`. A connection missing it
  type-checks, attaches, and raises `AttributeError` three layers deep at
  the first registration — which is why `SocketProvider` asserts its own
  surface at construction. On the SDK side it is a **method**, not a
  property: read as an attribute it is a truthy bound method, i.e. every
  agent silently declared interjection-capable.
- **Core reads no environment** (`CoreSettings.from_env` gone at revision
  14). `souk_server/config.py:core_settings_from_env()` is the only
  reader of `FUNDUQ_*` now; an empty string there means unset.
- **A chain with no presenter is refused** (revision 21,
  `PresenterRequired` -> 401). This gateway authenticates one:
  `Funduq-Presenter`, signed over
  `funduq-server-presenter:{public_key}:{timestamp}:{sha256hex(body)}`,
  60s window, in `souk_server/presenter.py`. Scope is small on purpose —
  **a caller with no chain sends no header and is unaffected**. A bad
  header yields `None`, never an error: core owns the refusal, and
  raising here would tell an unauthorized reader there was something
  behind the id. `X-Funduq-View` is deleted; do not reintroduce a second
  header.
- **`input-required` is not a run status** (revision 19). A run that
  finishes asking is `completed`; the answer is a **new** run with
  `parentRunId`; an A2A task is that lineage (id = root run, state = tail
  run). Read "is this thread waiting" from the latest run's *events*
  (`funduq.pause.open_asks`) — never a status, never run metadata, which
  `RunRecord` no longer has (nor `head_key` — use `doors.head_key_of` —
  nor `protocol`, nor `input_json`). A resolution proof signs the
  **task/root** id plus the ask ids, while the ids come from the **tail**:
  a first-turn pause makes the two coincide, so only a two-pause test
  catches getting it wrong.
- **One key, request level** (18 and 20).
  `forwardedProps.funduq.{kyok, actorChain, addressedRunId}`; A2A
  `metadata.funduq.{agui_event, agui_events, interrupts, cancelRequested}`,
  and this gateway's `outstandingAsks` beside them. The caller's
  declarations are **request-level on both doors** (`forwardedProps` on
  AG-UI, the *request's* `metadata` on A2A) — funduq reads nothing from a
  message's own metadata. **Merge into `metadata.funduq`, never assign**:
  a protobuf `Struct` is replaced wholesale, so assigning drops core's
  own keys silently.
- **Reads go through `Funduq.as_reader(key)`** (revision 21);
  `FunduqLink.thread_messages` left the ABC. This repo keeps its
  `query`/`queryResult` frames — the wire is ours, core only stopped
  requiring the verb — backed by `as_reader(public_key)` **behind** the
  existing "a provider may only read threads for agents it serves" check.
  Core's circle is the weaker one (an unbound thread is readable by
  anyone holding its id), and the two compose because both answer "not
  yours" and "no such thread" identically. The old subset-of-
  `__abstractmethods__` assertion is deleted, not repaired.
- **Migrating deletes history.** `python -m funduq.migrate` to revision
  `a1f4c9d27e3b` **drops every row in `runs`, `run_events` and
  `thread_messages`** by design. `docker compose up` runs it.
- **`enqueue_run` lost its `protocol` positional.** An old call passes
  `"ag-ui"` into `seq: int` and corrupts event ordering *without raising*.

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
