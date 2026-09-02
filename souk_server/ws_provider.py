"""WS /ws/provider: the socket a provider connects out on.

This file is transport and nothing else. funduq states the provider
contract itself (`funduq_provider_sdk`), and `broker.ConnectedProvider`
is the whole of what funduq needs to know about anybody: who you are,
how to hand you a run, how to ask you to stop one. `SocketProvider`
below is that, with a WebSocket underneath — the gateway's half of a
contract whose other half is `funduq_provider_sdk.ProviderRuntime`,
running behind the same socket in souk-agent-sdk.

**funduq hands work over; it does not ask for it.** There is no claim
loop here on either side: the broker finds whoever serves an agent and
offers each run, `deliver` writes it to the wire, and the ack frame that
comes back is the return value. Declining is how a full provider says
so, and funduq keeps the run.

Opening the socket is the v4 ticket handshake — two frames, the proof
computed before connecting against a ticket from `POST /tickets`. See
handshake.py for the payloads and for what changed from the in-band
challenge-response it replaced.

**Attach is nameless; registration happens on the open link.** A fresh
socket serves nothing until its first `register` frame, and the names it
serves are exactly the ones it last registered — a smaller roster takes
the omitted names offline, `deleteAgent` removes a record outright (and
is refused for an agent with a conversation behind it). Registration
frames carry no signature: the link is the credential, proved once when
it opened, and what that asks of this transport is the ordinary thing —
an open link stays the party that opened it (one read loop, one
identity, established here and never rebound).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket

from funduq.errors import FunduqError, InvalidRegistration
from funduq.models import AgentRef
from pydantic import ValidationError
from funduq_provider_sdk import CONNECTED_PROVIDER_ATTRS, DeliveredRun, Refusal
from funduq_provider_sdk.contract import LINK_QUERY_METHODS, REGISTRATION_FIELDS
from souk_server.handshake import WIRE_VERSION
from souk_server.models import AgentRegistration
from souk_server.ws_common import (
    POLICY_VIOLATION,
    close_frame,
    parse_frame,
    receive_hello,
    write_loop,
)

if TYPE_CHECKING:
    from funduq.core import Funduq

logger = logging.getLogger("souk.ws_provider")

router = APIRouter()

# A backstop, and deliberately longer than the deadline that actually
# governs. funduq wraps every offer in `RunBroker.deliver_timeout_seconds`
# (5s) because it has one delivery loop and an offer that never returns
# stops dispatch for everybody — so funduq gives up first, and this never
# fires in a funduq that sets a deadline. Shortening it to "win" would put
# the same policy in two places and let them drift; deleting it would
# leave a wait with no bound at all if funduq ever offers without one.
ACK_TIMEOUT_SECONDS = 30.0

# What a provider may ask funduq, and it is deliberately short. Upstream's
# `contract.LINK_QUERY_METHODS` states the rule: this is not a mirror of
# funduq's API, because every method admitted here is one more frame type
# every transport has to carry. Read from upstream rather than retyped, so
# a method added there without a frame here fails a test instead of a
# provider.
QUERY_METHODS = frozenset(LINK_QUERY_METHODS)

# How often a live socket re-asks whether funduq still lists the agents it
# registered. The condition it catches is rare and permanent, so noticing
# it a minute late costs nothing — see `_watch_registration`.
OWNERSHIP_RECHECK_SECONDS = 120.0

# What this socket accepts after the handshake. The dispatch below reads
# it, and docs/wire-vectors.json publishes it — one set, asserted equal in
# tests, so a frame type added in code without a vectors row goes red.
INBOUND_FRAME_TYPES = frozenset(
    {"register", "deleteAgent", "ack", "event", "finish", "query"}
)


class SocketProvider:
    """`broker.ConnectedProvider` with a socket underneath.

    **Not a `FunduqLink`,** and upstream's own docstring says so. A link
    is one provider joined to one funduq, both directions in one object;
    this lives on funduq's side, holds an outbound queue and no runtime,
    and only ever carries work *outward*. The object opposite it — the
    socket client in souk-agent-sdk — is the one that subclasses
    `FunduqLink`, because it really does own both halves of the connection.

    So the members below are duck-typed against funduq's own
    `ConnectedProvider` protocol rather than inherited. That loses the
    fails-at-construction property a base class gave, which is why the
    constructor asserts against `CONNECTED_PROVIDER_ATTRS` instead: funduq
    sizes a capacity bucket from `max_concurrent_runs`, and a connection
    that forgets it attaches perfectly well and then fails inside the
    broker, three layers from the cause.

    Deliberately exposes no `sign_connect`: core would otherwise mint a
    ticket and sign on this object's behalf, and this object cannot sign —
    the real provider on the far side of the socket is the only holder of
    the key, which is the whole point of the handshake. The ticket, nonce
    and proof arrive in the hello and go to `attach_provider` explicitly.

    Holds no per-run state beyond the acks it is waiting on — every frame
    names its run, and funduq keeps the only routing table — plus
    `registered_names`, the roster this link last published, which the
    registration watcher reads.
    """

    def __init__(
        self, public_key: str, outbound: asyncio.Queue, max_concurrent_runs: int | None
    ) -> None:
        # Against the class, not the instance: `public_key` and
        # `max_concurrent_runs` are properties, so asking `self` would run
        # their getters against fields this constructor has not assigned
        # yet and report every one of them missing.
        missing = sorted(a for a in CONNECTED_PROVIDER_ATTRS if not hasattr(type(self), a))
        if missing:
            raise TypeError(f"{type(self).__name__} is not a ConnectedProvider: missing {missing}")
        self._public_key = public_key
        self._max_concurrent_runs = max_concurrent_runs
        self._outbound = outbound
        self._acks: dict[str, asyncio.Future[bool | Refusal]] = {}
        self.registered_names: set[str] = set()

    @property
    def public_key(self) -> str:
        return self._public_key

    @property
    def max_concurrent_runs(self) -> int | None:
        return self._max_concurrent_runs

    async def deliver(self, run: Any) -> bool | Refusal:
        """Write this run to the wire and wait for the answer.

        The frame is `{"type": "run"}` plus the wire form upstream
        declares — `DeliveredRun.from_claimed(...).model_dump(by_alias=
        True)` — so this gateway does not hand-write the mapping and the
        far side rebuilds with `model_validate` instead of picking fields.
        `from_claimed` also owns the validation rule: input that does not
        parse as `RunAgentInput` is a permanent `Refusal`, answered here
        without ever touching the wire (funduq built the input, so this
        firing means a core bug or a version skew — either way permanent).

        Answering late is the same as declining, whichever deadline ran
        out: the ack arrives for a run nobody is waiting on any more, and
        `ack` drops it. funduq keeps the run either way.
        """
        try:
            delivered = DeliveredRun.from_claimed(run)
        except ValidationError as e:
            return Refusal(f"input does not validate as RunAgentInput: {e}")
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[bool | Refusal] = loop.create_future()
        self._acks[run.run_id] = waiter
        self._outbound.put_nowait(
            {"type": "run", **delivered.model_dump(by_alias=True, mode="json")}
        )
        try:
            return await asyncio.wait_for(waiter, timeout=ACK_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            logger.warning(
                "provider %s did not answer the offer of run %s in %.0fs — treating as declined",
                self.public_key,
                run.run_id,
                ACK_TIMEOUT_SECONDS,
            )
            return False
        finally:
            self._acks.pop(run.run_id, None)

    def ack(self, run_id: str, accepted: bool, reason: str | None = None) -> None:
        """A declined ack carrying a `reason` is a *permanent* refusal — the
        provider saying re-offering can never succeed (an input that does
        not parse, most importantly). funduq fails the run with the reason
        recorded verbatim and stops re-offering; a bare decline stays what
        it always was, \"full right now\". The wire says so with one
        optional field because the port says so with one optional type
        (`funduq_provider_sdk.Refusal`, read duck-typed by the broker)."""
        waiter = self._acks.get(run_id)
        if waiter is not None and not waiter.done():
            if not accepted and isinstance(reason, str) and reason:
                waiter.set_result(Refusal(reason))
            else:
                waiter.set_result(accepted)

    def cancel(self, run_id: str) -> None:
        """Ask, and do not wait. funduq decides the outcome from what the
        run's stream does next, not from anything this returns."""
        self._outbound.put_nowait({"type": "cancel", "runId": run_id})

    def fail_pending(self) -> None:
        """The socket is gone: nothing can answer these offers."""
        for waiter in self._acks.values():
            if not waiter.done():
                waiter.set_result(False)
        self._acks.clear()


async def _answer_query(
    funduq: "Funduq", public_key: str, parsed: dict[str, Any], outbound: asyncio.Queue
) -> None:
    """One `query` frame, answered on the same socket by `queryId`.

    The one frame on this wire that is not fire-and-forget, and the
    reason it exists is a real gap rather than convenience: a provider
    sees exactly what the *caller* sent for its run and nothing more. An
    AG-UI client resends its whole history every turn by convention;
    A2A's `message/send` carries one message. The same agent, unchanged,
    cannot tell a tenth turn from a first — and funduq has held the thread
    the whole time.

    **`limit` is applied here, not by the caller.** The parameter exists
    to keep the response frame bounded; trimming after receiving would
    bound nothing and put a months-old thread on the wire to do it.

    **A provider may only read threads for agents it serves.** Thread ids
    are not guessable, but "not guessable" is not an authorization rule:
    a provider that served one run knows that thread id permanently, and
    would otherwise keep reading the conversation after being de-listed,
    or after the agent moved to somebody else's stall. The thread names
    its agent, and an agent is `(provider_key, name)`, so the check is a
    comparison funduq can already make.
    """
    query_id = parsed.get("queryId")
    method = parsed.get("method")
    params = parsed.get("params") or {}

    def answer(**fields: Any) -> None:
        outbound.put_nowait({"type": "queryResult", "queryId": query_id, **fields})

    if not isinstance(query_id, str) or not query_id:
        outbound.put_nowait({"type": "error", "message": "query needs a queryId"})
        return
    if method not in QUERY_METHODS:
        answer(error=f"unknown query method {method!r}")
        return

    thread_id = params.get("threadId")
    limit = params.get("limit")
    if not isinstance(thread_id, str) or not thread_id:
        answer(error="thread_messages needs a threadId")
        return
    if limit is not None and not (isinstance(limit, int) and limit > 0):
        answer(error="limit must be a positive integer")
        return

    thread = await funduq.get_thread(thread_id)
    if thread is None or thread["provider_key"] != public_key:
        # One answer for "no such thread" and "not yours", deliberately:
        # telling them apart would confirm a thread exists to somebody who
        # may not read it, which is the whole of what the check is for.
        answer(error="no such thread for this provider")
        return

    messages = await funduq.get_thread_messages(thread_id)
    answer(result=messages[-limit:] if limit is not None else messages)


async def _watch_registration(
    funduq: "Funduq", provider: SocketProvider, outbound: asyncio.Queue
) -> None:
    """Close this socket if funduq stops listing every agent it published.

    Registration writes the records and the broker then holds the live
    mapping in memory. So a registration that disappears *underneath* a
    live socket — a restored database, a de-listing, a funduq redeployed
    against a fresh one — leaves the broker serving an agent funduq's own
    roster no longer has. Nothing can route to it, because addressing it
    needs a row that is gone; nothing complains, because the socket is
    fine. A healthy container and an invisible agent, indefinitely —
    observed once already, for half an hour, with one server-side warning
    per cycle and nothing at all on the provider's side.

    Closing is the repair, not a punishment. Registration is what puts
    the name back and the SDK re-registers on every reconnect, so a
    provider told goodbye here comes back listed. Only when *every* name
    has gone: losing one of several is a de-listing somebody meant, and
    the socket still has work to do for the rest. A link that has
    published nothing yet is left alone — there is nothing to have lost.
    """
    key = provider.public_key
    while True:
        await asyncio.sleep(OWNERSHIP_RECHECK_SECONDS)
        names = sorted(provider.registered_names)
        if not names:
            continue
        refs = [AgentRef(provider_key=key, name=name) for name in names]
        if any([await funduq.get_agent(ref) is not None for ref in refs]):
            continue
        logger.warning(
            "provider %s is attached for agent(s) funduq no longer lists (%s) — "
            "closing so its reconnect re-registers",
            key[:16],
            names,
        )
        outbound.put_nowait(
            close_frame(POLICY_VIOLATION, "funduq no longer lists these agents; re-register")
        )
        return


def _hello_error(hello: dict[str, Any]) -> str | None:
    """What a hello must carry to be worth handing to attach.

    The version is first because a provider on an older handshake says so
    most clearly by its absence or its number, and refusing it by name is
    far more use than the bad-ticket error it would otherwise get — which
    is what an attack looks like too.
    """
    version = hello.get("version")
    if version != WIRE_VERSION:
        if version is None:
            return (
                "hello has no version: this souk speaks wire "
                f"v{WIRE_VERSION}, the ticket handshake. Upgrade souk-agent-sdk."
            )
        return f"unsupported wire version {version!r}; this souk speaks v{WIRE_VERSION}"
    if not isinstance(hello.get("publicKey"), str) or not hello["publicKey"]:
        return "hello needs a publicKey"
    if not isinstance(hello.get("ticket"), str) or not hello["ticket"]:
        return "hello needs a ticket — POST /tickets issues one"
    if not isinstance(hello.get("nonce"), str) or not hello["nonce"]:
        return "hello needs a nonce"
    if not isinstance(hello.get("proof"), str) or not hello["proof"]:
        return "hello needs a proof signed over the ticket"
    max_runs = hello.get("maxConcurrentRuns")
    if max_runs is not None and not isinstance(max_runs, int):
        return "maxConcurrentRuns must be an integer"
    return None


def _parse_registration(frame: dict[str, Any]) -> list[dict[str, Any]]:
    """The `register` frame's agents, in the snake_case shape core takes.

    Validated here with the same model the old HTTP body used, so a
    malformed entry is answered with a message naming the field rather
    than a stack trace from inside core. The field list is upstream's
    `REGISTRATION_FIELDS`; the model and it are compared in a test.
    """
    agents = frame.get("agents")
    if not isinstance(agents, list) or not agents:
        raise ValueError("register needs a non-empty 'agents' list")
    parsed = []
    for entry in agents:
        if not isinstance(entry, dict):
            raise ValueError("each agent must be an object")
        try:
            registration = AgentRegistration.model_validate(entry)
        except ValidationError as e:
            raise ValueError(f"invalid agent registration: {e}") from None
        parsed.append(registration.model_dump())
    return parsed


@router.websocket("/ws/provider")
async def provider_socket(websocket: WebSocket) -> None:
    funduq: "Funduq" = websocket.app.state.souk

    await websocket.accept()
    hello = await receive_hello(websocket)
    if hello is None:
        return

    problem = _hello_error(hello)
    if problem:
        await websocket.close(code=POLICY_VIOLATION, reason=problem)
        return

    public_key = hello["publicKey"]
    outbound: asyncio.Queue = asyncio.Queue()
    provider = SocketProvider(public_key, outbound, hello.get("maxConcurrentRuns"))
    try:
        # Core is the verifier: the ticket must be live and issued to this
        # key, and the proof must answer it. A failure closes 1008 with
        # core's own words; 1011 stays reserved for faults the client did
        # not cause.
        answer = await funduq.attach_provider(
            provider,
            ticket=hello["ticket"],
            provider_nonce=hello["nonce"],
            proof=hello["proof"],
        )
    except (InvalidRegistration, ValueError) as e:
        await websocket.close(code=POLICY_VIOLATION, reason=str(e))
        return

    # The welcome relays funduq's half of the handshake: its signature over
    # `funduq_connect_payload(ticket, nonce)`, which the provider checks
    # against the key it pinned at ticket time before treating the link as
    # open. Queued before the read loop below can process a `register`, so
    # it is the first frame on the wire — and since a nameless attach
    # serves nothing until registration, no run can precede it either.
    outbound.put_nowait(
        {
            "type": "welcome",
            "funduqPublicKey": funduq.identity_public_key,
            "answer": answer,
        }
    )

    writer = asyncio.create_task(write_loop(websocket, outbound))
    watcher = asyncio.create_task(_watch_registration(funduq, provider, outbound))
    try:
        while True:
            frame = await websocket.receive()
            if frame["type"] == "websocket.disconnect":
                break
            parsed = parse_frame(frame)
            if parsed is None:
                outbound.put_nowait({"type": "error", "message": "unparseable frame"})
                continue
            kind = parsed.get("type")
            run_id = parsed.get("runId")
            if kind not in INBOUND_FRAME_TYPES:
                outbound.put_nowait({"type": "error", "message": f"unexpected frame {kind!r}"})
            elif kind == "register":
                # Answered, and the socket stays: a registration mistake is
                # a caller error on an authenticated link, not a breach.
                try:
                    agents = _parse_registration(parsed)
                    registration = await funduq.register_agents(
                        provider, agents, provider_name=parsed.get("providerName")
                    )
                except (FunduqError, ValueError) as e:
                    outbound.put_nowait({"type": "error", "message": str(e)})
                else:
                    names = sorted(registration.agents)
                    provider.registered_names = set(names)
                    outbound.put_nowait({"type": "registered", "names": names})
            elif kind == "deleteAgent":
                name = parsed.get("name")
                if not isinstance(name, str) or not name:
                    outbound.put_nowait({"type": "error", "message": "deleteAgent needs a name"})
                    continue
                try:
                    await funduq.delete_agent(provider, name)
                except FunduqError as e:
                    # Core's refusal reaches the provider verbatim — most
                    # importantly "has a conversation behind it", which is
                    # the one guard a deletion still has.
                    outbound.put_nowait({"type": "error", "name": name, "message": str(e)})
                else:
                    provider.registered_names.discard(name)
                    outbound.put_nowait({"type": "deleted", "name": name})
            elif kind == "ack":
                provider.ack(
                    run_id,
                    bool(parsed.get("accepted", True)),
                    parsed.get("reason"),
                )
            elif kind == "event":
                if not funduq.report_event(run_id, parsed.get("event"), claimed_by=public_key):
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "event refused"}
                    )
            elif kind == "finish":
                if not funduq.finish_run(run_id, claimed_by=public_key):
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "finish refused"}
                    )
            elif kind == "query":
                # Spawned rather than awaited: a query hits the database,
                # and awaiting it here would stop this socket reading —
                # including the acks and events of every run in flight on
                # it — for the length of that read.
                asyncio.create_task(_answer_query(funduq, public_key, parsed, outbound))
    finally:
        provider.fail_pending()
        # Connection-scoped on purpose: a re-attach replaces the old link,
        # and a whole-key detach fired by this (replaced) socket's teardown
        # would take the live replacement offline — exactly the case a
        # reconnect produces. Scoped to this connection it is a no-op then.
        funduq.detach_provider(public_key, provider)
        for task in (watcher, writer):
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
