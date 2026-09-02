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

**One field joined the register frame** at contract revision 12:
`takesInterjections`, per agent. It is the only thing this side can
answer core's `takes_interjections(agent_name)` from — a required call
that is *not* on the `ConnectedProvider` protocol, so a connection
without it opens perfectly well and raises three layers deep at the
first registration. See `SocketProvider.takes_interjections`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket

import re

import funduq_contract
from funduq.errors import FunduqError, InvalidRegistration
from funduq.models import AgentRef
from pydantic import ValidationError
from funduq_provider_sdk import DeliveredRun, FunduqLink, Refusal
from souk_server.handshake import WIRE_VERSION
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

# What a provider may ask funduq, and it is deliberately short: this is
# not a mirror of funduq's API, because every method admitted here is one
# more frame type every transport has to carry. Upstream withdrew the
# `LINK_QUERY_METHODS` constant at revision 11 — the models are the single
# definition now and there is no list left to read — so the one query is
# named here and checked against the link ABC that declares it, which is
# the surface that would actually grow a second one.
QUERY_METHODS = frozenset({"thread_messages"})
assert QUERY_METHODS <= FunduqLink.__abstractmethods__, (
    "a query this wire carries is no longer a FunduqLink verb"
)

# Agent names go in URLs on every road this gateway serves, so the shape is
# this layer's to police. Core validates none of it (upstream's
# `Registration` has no pattern), and a name with a slash or a space in it
# would register happily and then be unaddressable.
_AGENT_NAME = re.compile(r"^[A-Za-z0-9_-]{1,128}$")

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
    constructor asserts against `_PROTOCOL_SURFACE` instead: funduq sizes a
    capacity bucket from `max_concurrent_runs`, and a connection that
    forgets it attaches perfectly well and then fails inside the broker,
    three layers from the cause. Upstream withdrew the
    `CONNECTED_PROVIDER_ATTRS` list this used to read; the lesson it
    encoded outlived it, so the surface is named here — and it is wider
    than `broker.ConnectedProvider` declares, because
    **`takes_interjections` is not on the protocol at all** and core calls
    it inside `register_agents`. A connection without it type-checks, opens
    and then raises `AttributeError` at the first registration.

    Deliberately exposes no `sign_connect`: core would otherwise mint a
    ticket and sign on this object's behalf, and this object cannot sign —
    the real provider on the far side of the socket is the only holder of
    the key, which is the whole point of the handshake. The ticket, nonce
    and proof arrive in the hello and go to `attach_provider` explicitly.

    Holds no per-run state beyond the acks it is waiting on — every frame
    names its run, and funduq keeps the only routing table — plus
    `registered_names`, the roster this link last published, which the
    registration watcher reads, and `_interjections`, the per-agent
    capability that roster declared.
    """

    # The whole of what funduq reaches for on this object. Not read off
    # `broker.ConnectedProvider`: a Protocol's members are not enumerable
    # in a way worth trusting, and `takes_interjections` is not on it.
    _PROTOCOL_SURFACE = (
        "public_key",
        "max_concurrent_runs",
        "deliver",
        "cancel",
        "takes_interjections",
    )

    def __init__(
        self, public_key: str, outbound: asyncio.Queue, max_concurrent_runs: int | None
    ) -> None:
        # Against the class, not the instance: `public_key` and
        # `max_concurrent_runs` are properties, so asking `self` would run
        # their getters against fields this constructor has not assigned
        # yet and report every one of them missing.
        missing = sorted(a for a in self._PROTOCOL_SURFACE if not hasattr(type(self), a))
        if missing:
            raise TypeError(f"{type(self).__name__} is not a ConnectedProvider: missing {missing}")
        self._public_key = public_key
        self._max_concurrent_runs = max_concurrent_runs
        self._outbound = outbound
        self._acks: dict[str, asyncio.Future[bool | Refusal]] = {}
        self.registered_names: set[str] = set()
        # Runs this connection has accepted whose first report has not
        # landed yet. See `claim_settling` — this is the whole state the
        # ordering guard needs, and it empties itself.
        self.accepted_unreported: set[str] = set()
        self._interjections: dict[str, bool] = {}

    @property
    def public_key(self) -> str:
        return self._public_key

    @property
    def max_concurrent_runs(self) -> int | None:
        return self._max_concurrent_runs

    def declare_interjections(self, registrations: list[funduq_contract.Registration]) -> None:
        """Record what this roster said about interjections, per agent.

        Called before `register_agents`, because core asks during it. The
        table is replaced rather than merged: `register` carries the FULL
        roster, so an agent dropped from it has no declaration any more,
        exactly as it has no live name.
        """
        self._interjections = {r.name: bool(r.takes_interjections) for r in registrations}

    def takes_interjections(self, agent_name: str) -> bool:
        """Whether the remote link declared this agent able to take an
        interjection — a run arriving on a thread whose active run it
        names.

        Core calls this inside `register_agents` and **overwrites**
        whatever the incoming `Registration` said, so that the agent card's
        declaration is derived from the serving party rather than typed by
        an author. Over a wire the serving party is on the other end of the
        socket, and the `register` frame's `takesInterjections` is the only
        thing this side can honestly answer from: the in-process link reads
        the runtime's hook, and this reads what the runtime put on the
        wire. An agent this link never published gets `False` — declaring a
        capability for a name nobody registered would be a guess.
        """
        return self._interjections.get(agent_name, False)

    async def deliver(self, run: DeliveredRun) -> bool | Refusal:
        """Write this run to the wire and wait for the answer.

        The frame is `{"type": "run"}` plus the published envelope: since
        contract revision 11 `deliver` *receives* the `DeliveredRun`, so
        there is no translation left to do and no claimed-run shape to
        validate — funduq built and validated the model before offering it.

        **Dumped `by_alias=True` and never `exclude_none`.** `RunAgentInput`
        has required fields that are legitimately null (`state`,
        `forwardedProps`); stripping them yields a `runInput` the far side
        cannot rebuild, and a perfectly good run comes back as a permanent
        refusal. (The opposite rule holds for AG-UI *events* — see
        api_agui.encode_event — and the two pull against each other, which
        is why each says so where it is written.)

        Answering late is the same as declining, whichever deadline ran
        out: the ack arrives for a run nobody is waiting on any more, and
        `ack` drops it. funduq keeps the run either way.
        """
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future[bool | Refusal] = loop.create_future()
        self._acks[run.run_id] = waiter
        self._outbound.put_nowait(
            {"type": "run", **run.model_dump(by_alias=True, mode="json")}
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

    def ack(self, verdict: funduq_contract.Verdict) -> bool:
        """Answer one outstanding offer, and say whether anyone was waiting.

        A `refused` verdict is *permanent* — the provider saying re-offering
        can never succeed (an input that does not parse, most importantly).
        funduq fails the run with the reason recorded verbatim and stops
        re-offering; `declined` stays what it always was, "full right now".
        The port says so with one optional type
        (`funduq_provider_sdk.Refusal`, read duck-typed by the broker), and
        `funduq_contract.Verdict` is where the three names are written down.

        **A verdict for nothing is normal, not an error.** Upstream
        withdrew `accept_late_ack` at revision 11: an answer arriving after
        the delivery window matches nothing and is answered false. funduq
        has already taken the run back and will offer it again.
        """
        waiter = self._acks.get(verdict.id)
        if waiter is None or waiter.done():
            return False
        if verdict.verdict == "refused":
            # Keyword, not positional: `Refusal` is a pydantic model in
            # funduq_contract now (it was a one-argument dataclass), and a
            # positional call raises inside this socket's read loop —
            # which reads as a provider that went quiet, not as a bad
            # constructor.
            waiter.set_result(Refusal(reason=verdict.reason or "refused"))
        else:
            accepted = verdict.verdict == "accepted"
            if accepted:
                self.accepted_unreported.add(verdict.id)
            waiter.set_result(accepted)
        return True

    async def cancel(self, run_id: str) -> bool:
        """Ask, and do not wait for an outcome.

        The return is a **receipt** — "the ask is on the wire" — never a
        result: funduq decides the outcome from what the run's stream does
        next. Returning `None` (which this used to do) logs a warning on
        every cancel since revision 11, and would be a lie in the other
        direction if it tried to mean more.
        """
        self._outbound.put_nowait({"type": "cancel", "runId": run_id})
        return True

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
    # Presence and emptiness first, by name, because those are the
    # mistakes worth a sentence. What survives that goes through
    # `funduq_contract.Connect` — the published shape of this exchange —
    # so the types are policed by the model both ends already share
    # rather than by an isinstance ladder that can drift from it. The one
    # difference is spelling: our v4 hello has carried `nonce` since
    # before the model named the field `providerNonce`, and the wire
    # vocabulary does not change in this round.
    for field, message in (
        ("publicKey", "hello needs a publicKey"),
        ("ticket", "hello needs a ticket — POST /tickets issues one"),
        ("nonce", "hello needs a nonce"),
        ("proof", "hello needs a proof signed over the ticket"),
    ):
        value = hello.get(field)
        if not isinstance(value, str) or not value:
            return message
    try:
        funduq_contract.Connect(
            publicKey=hello["publicKey"],
            ticket=hello["ticket"],
            providerNonce=hello["nonce"],
            proof=hello["proof"],
            maxConcurrentRuns=hello.get("maxConcurrentRuns"),
        )
    except ValidationError as e:
        return f"hello is not a valid connect: {e}"
    return None


def _parse_registration(frame: dict[str, Any]) -> list[funduq_contract.Registration]:
    """The `register` frame's agents, as the models core now takes.

    `funduq_contract.Registration` is the single definition of this shape
    — both ends import it — and it is `extra="forbid"`, so a misspelt key
    is caught at the door with a message naming the field instead of
    travelling intact and being dropped in silence by a reader that cannot
    tell a typo from an omission. The local `AgentRegistration` that used
    to stand here was a second model of the same thing; it is gone.

    Two things the model does not police and this layer must: `agents`
    being a non-empty list at all, and the name being URL-shaped — every
    road this gateway serves puts an agent name in a path segment.
    """
    agents = frame.get("agents")
    if not isinstance(agents, list) or not agents:
        raise ValueError("register needs a non-empty 'agents' list")
    parsed = []
    for entry in agents:
        if not isinstance(entry, dict):
            raise ValueError("each agent must be an object")
        try:
            registration = funduq_contract.Registration.model_validate(entry)
        except ValidationError as e:
            raise ValueError(f"invalid agent registration: {e}") from None
        if not _AGENT_NAME.match(registration.name):
            raise ValueError(
                f"invalid agent name {registration.name!r}: "
                "names are 1-128 characters of [A-Za-z0-9_-]"
            )
        parsed.append(registration)
    return parsed


CLAIM_SETTLE_SECONDS = 0.5


async def claim_settling(
    report: "Callable[[], bool]", provider: "SocketProvider", run_id: str
) -> bool:
    """Report upward, allowing for a claim that has not landed yet.

    **The window this closes is real and was found by running the stack, not
    by reading.** Since contract revision 11 funduq offers a `DeliveredRun`
    and records the claim only *after* `deliver` returns — before then, the
    run is held by nobody. Our `deliver` returns by having this socket's read
    loop resolve its future, and that read loop is then free to handle the
    very next frame. A provider that answers `accepted` and starts streaming
    in the same breath — which an SDK-less one does, because nothing tells it
    not to — therefore lands its first events inside funduq's own claim
    window, and every one of them is refused as "reported for a run nobody
    holds". Measured: exactly one event-loop turn, but only on this machine,
    on this day, against SQLite; one turn is not a number to build on.

    So the first report of an accepted run waits for the claim rather than
    assuming it. Bounded by `CLAIM_SETTLE_SECONDS`, and only for a run *this*
    connection accepted and has not successfully reported yet — a report for
    anything else is refused immediately, as it always was, because that is
    a provider talking about work it does not hold.

    The retry costs one "nobody holds" line in funduq's log per run, which is
    the honest trace of a wait that really happened.
    """
    if report():
        provider.accepted_unreported.discard(run_id)
        return True
    if run_id not in provider.accepted_unreported:
        return False
    loop = asyncio.get_running_loop()
    deadline = loop.time() + CLAIM_SETTLE_SECONDS
    while loop.time() < deadline:
        await asyncio.sleep(0.005)
        if report():
            provider.accepted_unreported.discard(run_id)
            return True
    provider.accepted_unreported.discard(run_id)
    return False


def _parse_verdict(frame: dict[str, Any]) -> funduq_contract.Verdict:
    """One inbound `ack` frame as the contract's own three-valued verdict.

    The v4 wire spells an offer's answer as `accepted` plus an optional
    `reason` and correlates by `runId`; upstream's `Verdict` spells it as
    one of three names correlated by the request's `id`. Translating here
    rather than carrying two vocabularies means the *meaning* — that a
    reasoned no is a different answer from a bare no — has one definition,
    the model's, and the frame stays what every shipped provider sends.
    """
    reason = frame.get("reason")
    accepted = bool(frame.get("accepted", True))
    return funduq_contract.Verdict(
        id=frame.get("runId"),
        verdict="accepted"
        if accepted
        else ("refused" if isinstance(reason, str) and reason else "declined"),
        reason=reason if isinstance(reason, str) and reason else None,
    )


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
                    # Before the call, not after: core asks
                    # `takes_interjections` *inside* register_agents, and
                    # this is the only place the answer exists.
                    provider.declare_interjections(agents)
                    registered = await funduq.register_agents(
                        provider, agents, provider_name=parsed.get("providerName")
                    )
                except (FunduqError, ValueError) as e:
                    outbound.put_nowait({"type": "error", "message": str(e)})
                else:
                    # `register_agents` answers a plain {name: AgentRef}
                    # mapping since revision 11 (it was a dataclass with an
                    # `.agents` field).
                    names = sorted(registered)
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
                try:
                    verdict = _parse_verdict(parsed)
                except ValidationError as e:
                    outbound.put_nowait(
                        {"type": "error", "message": f"invalid ack: {e}"}
                    )
                    continue
                # The answer to "did anyone hear that" is deliberately
                # dropped: a verdict that matches nothing is normal — the
                # window ran out and funduq took the run back — and
                # answering an error frame for it would teach every
                # provider to log a scare on its own slow morning.
                provider.ack(verdict)
            elif kind == "event":
                event = parsed.get("event")
                landed = await claim_settling(
                    lambda: funduq.report_event(run_id, event, claimed_by=public_key),
                    provider,
                    run_id,
                )
                if not landed:
                    outbound.put_nowait(
                        {"type": "error", "runId": run_id, "message": "event refused"}
                    )
            elif kind == "finish":
                landed = await claim_settling(
                    lambda: funduq.finish_run(run_id, claimed_by=public_key),
                    provider,
                    run_id,
                )
                if not landed:
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
