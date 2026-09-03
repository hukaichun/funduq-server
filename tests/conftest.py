"""Test fixtures for the gateway's test suite.

Deliberately parallel to upstream funduq's own conftest rather than an
import of it: these are two independent distributions, and a test-only
dependency from the gateway back into upstream's test package would be
the first thread of exactly the coupling the split exists to remove.
What is shared is a database, a schema and a wire, not fixtures.

Two things are the gateway's own. `client` is an ASGI client over
`create_app`, which is the whole reason these tests live here. And
`serve`/`register` hand back the `AgentRef` *and* the fingerprint,
because an agent is `(provider_key, name)` and this layer puts the short
form of that pair in a URL — a test that talks to a route needs both
halves.

`Identity` subclasses `funduq_provider_sdk.ProviderIdentity` rather than
reimplementing the signing against `cryptography`: this gateway depends
on the SDK, so reimplementing what a provider signs would mean the tests
could agree with themselves while disagreeing with every real provider.

Runs against SQLite by default — zero configuration, no database to
stand up first. The same suite runs against Postgres by exporting
SOUK_DATABASE_URL (a `postgresql+psycopg://…` DSN) before invoking
pytest; funduq's schema and queries are dialect-neutral, so both
backends exercise the same semantics.

Settings are constructed explicitly here, which is now the only way:
`CoreSettings.from_env` was removed at contract revision 14 and core
reads no environment at all. The gateway does that reading instead
(`souk_server.config.core_settings_from_env`), and these tests
deliberately do not go through it — a test that read the environment
would answer differently on somebody's laptop. `identity_private_key` is
required (providers pin it), and it is a fixed key rather than a
generated one: a test that asserts what a provider pinned needs the same
funduq to be the same funduq across runs.

Tests aren't wrapped in a rolled-back transaction: funduq's repo
functions commit internally throughout, so a single outer transaction
can't cleanly contain a whole test. The schema is applied once per
session via `funduq.migrate.migrate()` (the same packaged chain a real
deployment runs), and rows are cleared between tests.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from httpx import ASGITransport, AsyncClient
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from funduq.config import CoreSettings
from funduq.core import Funduq
from funduq.identity import provider_fingerprint
from funduq.migrate import migrate as funduq_migrate
from funduq.models import AgentRef
from funduq_contract import Registration
from funduq_provider_sdk import InProcessLink, ProviderIdentity, ProviderRuntime
from souk_server.handshake import WIRE_VERSION, new_nonce
from souk_server.server import create_app

TEST_SIGNING_SECRET = "test-signing-secret"

# Postgres when a DSN is exported, a throwaway SQLite file otherwise.
DATABASE_URL = os.environ.get(
    "SOUK_DATABASE_URL", f"sqlite+aiosqlite:///{Path(tempfile.gettempdir()) / 'souk_pytest.db'}"
)

# FK-safe teardown order (children before parents) for the SQLite path,
# where there's no TRUNCATE ... CASCADE. Postgres uses TRUNCATE directly.
_TABLES_CHILD_FIRST = (
    "run_events",
    "thread_messages",
    "runs",
    "threads",
    "agents",
    "llm_providers",
    "providers",
)


# A fixed key rather than a generated one: a test that asserts what a
# provider pinned needs the same funduq to be the same funduq across runs,
# and generating one per session would make "is this the funduq I meant" a
# question with no stable answer to write down.
TEST_FUNDUQ_IDENTITY = "11" * 32


@pytest.fixture(scope="session")
def settings() -> CoreSettings:
    return CoreSettings(
        database_url=DATABASE_URL,
        token_signing_secret=TEST_SIGNING_SECRET,
        identity_private_key=TEST_FUNDUQ_IDENTITY,
    )


@pytest.fixture(scope="session")
def souk(settings: CoreSettings) -> Funduq:
    return Funduq(settings)


@pytest.fixture(scope="session", autouse=True)
def _schema(settings: CoreSettings) -> None:
    # Start each SQLite run from a clean file so a schema change between
    # runs can't leave a stale table lying around (Postgres relies on the
    # migration + per-test cleanup instead — its DB isn't disposable here).
    url = make_url(settings.database_url)
    if url.get_backend_name() == "sqlite" and url.database:
        for suffix in ("", "-wal", "-shm"):
            Path(url.database + suffix).unlink(missing_ok=True)
    # Through the chain packaged inside the funduq wheel — the one way a
    # funduq database gets built; no alembic.ini anywhere in this repo.
    funduq_migrate(settings.database_url)


@pytest.fixture(autouse=True)
async def _dispatching(souk: Funduq) -> AsyncIterator[None]:
    """The broker's dispatch loop, for every test.

    `create_app`'s lifespan is what starts it in a real process, and
    `ASGITransport` does not run lifespans — so without this a run reaches
    the broker and simply sits there, which reads as a hang rather than as
    a missing fixture.
    """
    souk.broker.start()
    try:
        yield
    finally:
        souk.broker.stop()


@pytest.fixture(autouse=True)
async def _clean_db(souk: Funduq) -> AsyncIterator[None]:
    is_postgres = souk.engine.sync_engine.dialect.name == "postgresql"
    async with souk.engine.begin() as conn:
        if is_postgres:
            await conn.exec_driver_sql(
                "TRUNCATE providers, agents, llm_providers, threads, runs, thread_messages, "
                "run_events RESTART IDENTITY CASCADE"
            )
        else:
            for table in _TABLES_CHILD_FIRST:
                await conn.exec_driver_sql(f"DELETE FROM {table}")
    yield


@pytest.fixture(autouse=True)
async def _fresh_pool(souk: Funduq) -> AsyncIterator[None]:
    """Dispose the engine's pool after every test.

    The Funduq is session-scoped and pytest-asyncio gives each test its
    own event loop, so a pooled aiosqlite connection checked out under
    one test's loop can resurface under the next — and answers with
    "no active connection" from a loop that no longer exists. Observed
    as a once-in-ten flake; disposing between tests makes every
    connection loop-local by construction."""
    yield
    await souk.engine.dispose()


@pytest.fixture
async def session(souk: Funduq) -> AsyncIterator[AsyncSession]:
    async with souk.session() as s:
        yield s


@pytest.fixture
async def client(souk: Funduq) -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=create_app(souk))
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class Identity(ProviderIdentity):
    """A throwaway keypair that signs the way a real provider does.

    Everything funduq verifies is `ProviderIdentity`'s: `sign_connect`
    builds the connect proof over `provider_connect_payload(funduq_key,
    ticket, nonce)` (the SDK's signature names the second seat
    `funduq_nonce`; the ticket rides in it — funduq chose the ticket, so
    it *is* the verifier-chosen freshness).
    """

    def __init__(self) -> None:
        self._key = Ed25519PrivateKey.generate()
        super().__init__(self._key)

    @property
    def fingerprint(self) -> str:
        return provider_fingerprint(self.public_key)

    def hello(self, souk: Funduq, **extra) -> dict:
        """A complete, honest v4 hello: ticket from funduq (the in-process
        stand-in for `POST /tickets`), proof computed before connecting —
        the two-frame handshake's whole provider half."""
        ticket = souk.issue_ticket(self.public_key)
        nonce = new_nonce()
        return {
            "type": "hello",
            "version": WIRE_VERSION,
            "publicKey": self.public_key,
            "ticket": ticket,
            "nonce": nonce,
            "proof": self.sign_connect(souk.identity_public_key, ticket, nonce),
            **extra,
        }


@pytest.fixture
def new_identity() -> type[Identity]:
    return Identity


@dataclass
class Served:
    """What `serve`/`register` hand back: everything a test needs to talk
    about the provider it just stood up, including how to address it.

    `ref` is what funduq takes, `fingerprint` is what goes in a URL — the
    same identity in two forms, and a test that has to derive one from the
    other is a test that has taken a position on which is authoritative.
    """

    identity: Identity
    provider: Any
    runtime: ProviderRuntime | None
    names: tuple[str, ...]

    @property
    def public_key(self) -> str:
        return self.identity.public_key

    @property
    def fingerprint(self) -> str:
        return self.identity.fingerprint

    def ref(self, name: str | None = None) -> AgentRef:
        return AgentRef(provider_key=self.public_key, name=name or self.names[0])

    def path(self, name: str | None = None) -> str:
        """The `{provider}/{name}` half of every route this gateway serves."""
        return f"{self.fingerprint}/{name or self.names[0]}"


@pytest.fixture
async def attach(souk: Funduq):
    """Attach a provider the way a real one arrives: the SDK's runtime,
    with an in-process link funduq can hand a run to — opened first,
    published on second, the order the handshake now has.

    Every runtime is stopped when the test ends. The `souk` fixture is
    session-scoped, so one left running stays registered with the broker
    and takes the next test's runs.
    """
    started: list[ProviderRuntime] = []

    async def _attach(identity: ProviderIdentity, provider, names, **kwargs) -> ProviderRuntime:
        runtime = ProviderRuntime(identity, provider, **kwargs)
        started.append(runtime)
        runtime.start()
        # Constructing the link is what joins it to the runtime, so it has
        # to happen before any work arrives — a runtime with no link drops
        # its output silently.
        link = InProcessLink(souk, runtime)
        await souk.attach_provider(link)
        # `Registration` models end to end since revision 11: core reads
        # `.name` off each entry, so a dict raises AttributeError rather
        # than being coerced.
        await souk.register_agents(link, [Registration(name=n) for n in names])
        return runtime

    yield _attach
    for runtime in started:
        await runtime.aclose(cancel_in_flight=True)


class EchoAgent:
    """A provider that answers with one short message and remembers the
    chain it was handed."""

    def __init__(self) -> None:
        self.seen_chain: list | None = None

    async def run_stream(self, agent_name: str, run_input):
        self.seen_chain = (run_input.forwarded_props or {}).get("actorChain")
        ids = {"threadId": run_input.thread_id, "runId": run_input.run_id}
        yield {"type": "RUN_STARTED", **ids}
        yield {"type": "TEXT_MESSAGE_START", "messageId": "m1", "role": "assistant"}
        yield {"type": "TEXT_MESSAGE_CONTENT", "messageId": "m1", "delta": "done"}
        yield {"type": "TEXT_MESSAGE_END", "messageId": "m1"}
        yield {"type": "RUN_FINISHED", **ids}


@pytest.fixture
async def register(souk: Funduq):
    """Registered and then offline — reached the only way it can be now:
    publish on an open link, then close the link. Which is most of this
    suite: an offline agent is a state the gateway has routes for.
    """

    async def _register(*names: str, provider_name: str | None = None, **agent_extra) -> Served:
        names = names or ("agent",)
        identity = Identity()
        runtime = ProviderRuntime(identity, EchoAgent())
        runtime.start()
        link = InProcessLink(souk, runtime)
        await souk.attach_provider(link)
        await souk.register_agents(
            link,
            [Registration.model_validate({"name": n, **agent_extra}) for n in names],
            provider_name=provider_name,
        )
        souk.detach_provider(identity.public_key, link)
        await runtime.aclose()
        return Served(identity, None, None, tuple(names))

    return _register


@pytest.fixture
async def serve(souk: Funduq, attach):
    """Open a link and publish agents on it, in one step — registration is
    what makes the names funduq's to serve, and it happens on the link
    that will serve them."""

    async def _serve(provider=None, *names: str, **kwargs) -> Served:
        provider = EchoAgent() if provider is None else provider
        names = names or ("agent",)
        identity = Identity()
        runtime = await attach(identity, provider, names, **kwargs)
        return Served(identity, provider, runtime, tuple(names))

    return _serve
