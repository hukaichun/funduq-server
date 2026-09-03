"""WS /ws/provider — the socket a provider connects out on.

The v4 handshake replaced the in-band challenge-response: the proof is
computed *before* connecting, over a single-use ticket funduq minted for
this key (`POST /tickets`), so the exchange is two frames and the
freshness is the verifier's. What that deleted from this file: the
challenge-ordering tests (there is no challenge frame) and the
names-bound-into-the-proof test (attach is nameless — a ticket issued to
one key cannot be replayed at all, which is the stronger property the
name binding approximated).

What replaced them: registration lives on the open link. `register`
publishes a full roster (a smaller batch withdraws the omitted names),
`deleteAgent` removes a record and is refused — socket intact — for an
agent with a conversation behind it. Runs are only offered after
registration.

What survives is the catalogue server-mode.md orders carried over,
because it is about the transport rather than the handshake's shape: no
per-run state here, a cancel reaching whichever socket the identity has
open, a dropped socket ending nothing with a reconnect reporting the
rest, and the three-valued ack.

Driven through the real ASGI app over httpx-ws, same event loop as the
souk fixture — the broker's queues are loop-bound, so a threaded test
client would be driving them cross-loop.
"""

from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest
from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

import funduq_contract
from funduq_provider_sdk import verify_signature
from souk_server import ws_provider
from souk_server.handshake import WIRE_VERSION, funduq_connect_payload, new_nonce
from souk_server.server import create_app

from tests.conftest import Identity

RECEIVE_TIMEOUT = 2.0


class _Socket:
    """One /ws/provider connection speaking the frame table directly."""

    def __init__(self, ws) -> None:
        self._ws = ws

    async def recv(self) -> dict:
        return json.loads(await self._ws.receive_text(timeout=RECEIVE_TIMEOUT))

    async def send(self, frame: dict) -> None:
        await self._ws.send_text(json.dumps(frame))

    async def register(self, *names: str, **extra) -> dict:
        await self.send(
            {"type": "register", "agents": [{"name": n} for n in names], **extra}
        )
        frame = await self.recv()
        assert frame == {"type": "registered", "names": sorted(names)}, frame
        return frame

    async def take(self, run_id: str | None = None) -> dict:
        """Receive a run offer and accept it — the two halves are never
        apart in a real provider, and an offer left unacked stalls the
        broker for ACK_TIMEOUT_SECONDS."""
        frame = await self.recv()
        assert frame["type"] == "run", frame
        if run_id is not None:
            assert frame["runId"] == run_id
        await self.send({"type": "ack", "runId": frame["runId"], "accepted": True})
        return frame

    async def expect_nothing(self, seconds: float = 0.3) -> None:
        with pytest.raises(TimeoutError):
            await self._ws.receive_text(timeout=seconds)


def _provider_client(souk) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=ASGIWebSocketTransport(app=create_app(souk)), base_url="http://test"
    )


def _connect(client: httpx.AsyncClient, **kwargs):
    return aconnect_ws("http://test/ws/provider", client, **kwargs)


async def _handshake(ws, souk, identity, *names: str, **hello_extra) -> _Socket:
    """Both frames, then — when names are given — the register/registered
    exchange, the way a real provider arrives."""
    socket = _Socket(ws)
    hello = identity.hello(souk, **hello_extra)
    await socket.send(hello)

    welcome = await socket.recv()
    assert welcome["type"] == "welcome", welcome
    assert welcome["funduqPublicKey"] == souk.identity_public_key
    # The provider's half of mutual identity: funduq's answer over the
    # ticket and nonce, verified against the key pinned at ticket time —
    # the SDK's confirm_connect does exactly this before opening the link.
    assert verify_signature(
        welcome["funduqPublicKey"],
        welcome["answer"],
        funduq_connect_payload(hello["ticket"], hello["nonce"]),
    )
    if names:
        await socket.register(*names)
    return socket


async def _drain(souk, *run_ids: str) -> None:
    async with asyncio.timeout(2):
        while any(souk.broker.get(r) is not None for r in run_ids):
            await asyncio.sleep(0.01)


async def _claimed(souk, run_id: str) -> None:
    """Wait until funduq has recorded the provider as holding this run.

    Sending the ack frame is not the same moment as funduq processing it —
    the frame has a socket, a read loop and a future to cross first — and a
    test that treats them as one is testing its own timing. It matters for
    cancel specifically: a cancel arriving while `claimed_by` is still None
    is, correctly, a cancel of a run nobody has, and funduq answers it
    without telling anybody.
    """
    async with asyncio.timeout(2):
        while (snapshot := souk.broker.get(run_id)) is None or snapshot.claimed_by is None:
            await asyncio.sleep(0.01)


async def _closed_1008(ws) -> str:
    with pytest.raises(WebSocketDisconnect) as excinfo:
        await ws.receive_text(timeout=RECEIVE_TIMEOUT)
    assert excinfo.value.code == 1008
    return excinfo.value.reason or ""


# --- the ticket handshake ----------------------------------------------------


async def test_a_ticketed_hello_opens_the_socket_and_the_welcome_proves_funduq(souk):
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _handshake(ws, souk, Identity())


async def test_the_ticket_endpoint_serves_the_whole_out_of_band_half(souk):
    """The road a real provider takes: POST /tickets for the ticket and
    funduq's key, proof computed before connecting, hello over the same
    listener. The response's funduqPublicKey is the pin the welcome's
    answer is then checked against."""
    identity = Identity()
    async with _provider_client(souk) as client:
        resp = await client.post("/tickets", json={"publicKey": identity.public_key})
        assert resp.status_code == 201, resp.text
        issued = resp.json()
        assert issued["funduqPublicKey"] == souk.identity_public_key

        nonce = new_nonce()
        async with _connect(client) as ws:
            socket = _Socket(ws)
            await socket.send(
                {
                    "type": "hello",
                    "version": WIRE_VERSION,
                    "publicKey": identity.public_key,
                    "ticket": issued["ticket"],
                    "nonce": nonce,
                    "proof": identity.sign_connect(
                        issued["funduqPublicKey"], issued["ticket"], nonce
                    ),
                }
            )
            welcome = await socket.recv()
            assert welcome["type"] == "welcome"
            assert verify_signature(
                issued["funduqPublicKey"],
                welcome["answer"],
                funduq_connect_payload(issued["ticket"], nonce),
            )


async def test_a_captured_handshake_does_not_open_a_second_socket(souk):
    """The replay property, against exactly what an observer holds: every
    frame of a complete, successful handshake, replayed verbatim onto a
    fresh connection. The ticket was destroyed by the handshake that
    answered it, so the recording answers a question nobody is asking."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = _Socket(ws)
            hello = identity.hello(souk)
            await socket.send(hello)
            assert (await socket.recv())["type"] == "welcome"

        async with _connect(client) as ws:
            await _Socket(ws).send(hello)
            reason = await _closed_1008(ws)
            assert "ticket" in reason


async def test_a_ticket_issued_to_another_key_does_not_admit_this_one(souk):
    """A leaked ticket is worthless: it names the key it admits, and the
    match happens before the ticket is destroyed — so a stranger can
    neither use it nor burn it (the named provider's own hello here still
    succeeds afterwards)."""
    victim, thief = Identity(), Identity()
    ticket = souk.issue_ticket(victim.public_key)
    nonce = new_nonce()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _Socket(ws).send(
                {
                    "type": "hello",
                    "version": WIRE_VERSION,
                    "publicKey": thief.public_key,
                    "ticket": ticket,
                    "nonce": nonce,
                    "proof": thief.sign_connect(souk.identity_public_key, ticket, nonce),
                }
            )
            await _closed_1008(ws)

        # Not burned: the key it names still gets in with it.
        async with _connect(client) as ws:
            socket = _Socket(ws)
            await socket.send(
                {
                    "type": "hello",
                    "version": WIRE_VERSION,
                    "publicKey": victim.public_key,
                    "ticket": ticket,
                    "nonce": nonce,
                    "proof": victim.sign_connect(souk.identity_public_key, ticket, nonce),
                }
            )
            assert (await socket.recv())["type"] == "welcome"


async def test_a_proof_naming_a_different_funduq_is_refused(souk):
    """The proof binds the recipient: `provider_connect_payload` opens
    with the funduq key the provider means to reach, and the verifying
    funduq builds the payload with its own — so a proof one funduq coaxes
    out cannot be relayed to attach at another. Here the provider signs
    for a funduq that is not this one."""
    identity = Identity()
    ticket = souk.issue_ticket(identity.public_key)
    nonce = new_nonce()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _Socket(ws).send(
                {
                    "type": "hello",
                    "version": WIRE_VERSION,
                    "publicKey": identity.public_key,
                    "ticket": ticket,
                    "nonce": nonce,
                    "proof": identity.sign_connect("22" * 32, ticket, nonce),
                }
            )
            await _closed_1008(ws)


async def test_a_garbage_proof_is_refused(souk):
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await _Socket(ws).send({**identity.hello(souk), "proof": "00" * 64})
            await _closed_1008(ws)


@pytest.mark.parametrize(
    "mangle",
    [
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "version"}, id="no-version"),
        pytest.param(lambda h: {**h, "version": 2}, id="old-version"),
        pytest.param(lambda h: {**h, "version": 99}, id="unsupported-version"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "publicKey"}, id="no-public-key"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "ticket"}, id="no-ticket"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "nonce"}, id="no-nonce"),
        pytest.param(lambda h: {k: v for k, v in h.items() if k != "proof"}, id="no-proof"),
        pytest.param(lambda h: {**h, "maxConcurrentRuns": "three"}, id="bad-max-runs"),
        pytest.param(lambda h: {"type": "event", "runId": "x"}, id="anything-before-hello"),
    ],
)
async def test_a_bad_hello_closes_the_socket_by_name(souk, mangle):
    """Refused at the hello — the version first and by its name, because a
    provider on the old handshake is best told which side is behind, and a
    bare bad-ticket error is what an attack looks like too."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            await ws.send_text(json.dumps(mangle(identity.hello(souk))))
            reason = await _closed_1008(ws)
            if "version" in str(mangle):
                assert "version" in reason


# --- registration on the open link -------------------------------------------


async def test_registering_on_the_link_is_what_puts_an_agent_on_the_roster(souk, client):
    """Attach is nameless: the welcome arrives with nothing served, and
    the roster row — online, this key's — appears with the registered
    echo. The dropped socket then takes it offline at once, record
    intact."""
    identity = Identity()
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, identity)
            assert (await client.get("/agents")).json()["agents"] == []

            await socket.register("greeter", providerName="Halima's")

            (row,) = (await client.get("/agents")).json()["agents"]
            assert (row["provider_key"], row["name"]) == (identity.public_key, "greeter")
            assert row["online"] is True
            assert row["provider_name"] == "Halima's"

    async with asyncio.timeout(2):
        while (await client.get("/agents")).json()["agents"][0]["online"]:
            await asyncio.sleep(0.01)


async def test_republishing_a_smaller_roster_withdraws_the_omitted_names(souk, client):
    """Core's rule, carried by this frame: register is the FULL roster,
    so a smaller batch takes the omitted names off live serving — their
    records stay, readable as online: false."""
    identity = Identity()
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter", "translator")
            roster = {a["name"]: a["online"] for a in (await client.get("/agents")).json()["agents"]}
            assert roster == {"greeter": True, "translator": True}

            await socket.register("greeter")

            roster = {a["name"]: a["online"] for a in (await client.get("/agents")).json()["agents"]}
            assert roster == {"greeter": True, "translator": False}


async def test_a_registration_mistake_is_answered_and_the_socket_stays(souk):
    """An error frame, not a teardown: a registration failure on an
    authenticated link is a caller mistake, and the link is still the
    credential for the corrected retry."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity)

            await socket.send({"type": "register", "agents": []})
            frame = await socket.recv()
            assert frame["type"] == "error"

            await socket.send({"type": "register", "agents": [{"name": "no spaces allowed"}]})
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert "name" in frame["message"]

            # Still open, still able to register.
            await socket.register("greeter")


async def test_delete_agent_removes_the_record_on_the_link_that_serves_it(souk, client):
    identity = Identity()
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, identity, "retiree")

            await socket.send({"type": "deleteAgent", "name": "retiree"})
            assert (await socket.recv()) == {"type": "deleted", "name": "retiree"}
            assert (await client.get("/agents")).json()["agents"] == []

            # Gone means gone — and answered, not dropped.
            await socket.send({"type": "deleteAgent", "name": "retiree"})
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert frame["name"] == "retiree"


async def test_deleting_an_agent_with_a_conversation_behind_it_is_refused(souk, client):
    """The one guard a deletion still has: history means the record is the
    past's, and the way to retire the agent is to stop offering it. Core's
    own words reach the provider, and the socket stays open."""
    identity = Identity()
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, identity, "busy")
            from funduq.models import AgentRef

            await souk.create_thread(AgentRef(provider_key=identity.public_key, name="busy"))

            await socket.send({"type": "deleteAgent", "name": "busy"})
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert "conversation" in frame["message"]
            assert len((await client.get("/agents")).json()["agents"]) == 1

            # The refusal cost the link nothing.
            await socket.register("busy", "second")


# --- runs over one socket ---------------------------------------------------


async def test_the_transport_keeps_no_state_per_run(souk):
    """Three runs offered over one socket; events go straight to core by
    runId. The transport's only per-run state is the acks it is waiting
    on, and once each run is accepted even that is empty."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            # Started *after* registration: with push delivery there is
            # nobody to offer a run to until somebody serves the name.
            handles = [await souk.start_run(ref, {"messages": []}) for _ in range(3)]
            streams = {h.run_id: h.events() for h in handles}
            run_ids = {h.run_id for h in handles}

            offered = {}
            for _ in range(3):
                frame = await socket.take()
                assert frame["agentName"] == "greeter"
                assert frame["runInput"]["runId"] == frame["runId"]
                offered[frame["runId"]] = frame
            assert set(offered) == run_ids

            threads = {h.run_id: h.thread_id for h in handles}
            for run_id in run_ids:
                await socket.send(
                    {
                        "type": "event",
                        "runId": run_id,
                        "event": {
                            "type": "RUN_STARTED",
                            "runId": run_id,
                            "threadId": threads[run_id],
                        },
                    }
                )
            for run_id in run_ids:
                async with asyncio.timeout(2):
                    assert (await anext(streams[run_id]))["type"] == "RUN_STARTED"

            for run_id in run_ids:
                await socket.send({"type": "finish", "runId": run_id})
            await _drain(souk, *run_ids)


async def test_a_reasoned_decline_fails_the_run_with_the_reason_recorded(souk):
    """The third value of the ack: a no *with a reason* is a permanent
    refusal. funduq fails the run, records the provider's words verbatim
    in failureReason, and does not offer it again — the run that used to
    sit `queued` forever while the reason lived in a log on somebody
    else's machine now says what happened, where the caller looks."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            handle = await souk.start_run(ref, {"messages": []})

            first = await socket.recv()
            assert first["type"] == "run"
            await socket.send(
                {
                    "type": "ack",
                    "runId": first["runId"],
                    "accepted": False,
                    "reason": "input does not validate as RunAgentInput: probe",
                }
            )
            # Permanently refused: failed now, not re-offered on reconnect.
            async with asyncio.timeout(2):
                while (await souk.get_run(handle.run_id)).status != "failed":
                    await asyncio.sleep(0.01)
            await socket.expect_nothing()

        stored = await souk.get_run(handle.run_id)
        assert stored.metadata["failureReason"] == (
            "input does not validate as RunAgentInput: probe"
        )
        assert souk.broker.get(handle.run_id) is None

        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            await socket.expect_nothing()


async def test_a_declined_offer_costs_the_run_nothing(souk):
    """Saying no is how a full provider says so, and the run stays
    funduq's. What it does *not* do is come straight back: funduq waits
    for something to change before offering again, and reconnecting —
    which re-registers, a change — is the one a real provider makes."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            handle = await souk.start_run(ref, {"messages": []})

            first = await socket.recv()
            assert first["type"] == "run"
            await socket.send({"type": "ack", "runId": first["runId"], "accepted": False})
            await socket.expect_nothing()

        # Declined, not failed: funduq still holds it, unclaimed.
        snapshot = souk.broker.get(handle.run_id)
        assert snapshot is not None and snapshot.claimed_by is None
        assert (await souk.get_run(handle.run_id)).status == "queued"

        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            again = await socket.take(handle.run_id)
            assert again["runId"] == first["runId"]
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)


async def test_a_cancel_reaches_the_socket_the_identity_has_open(souk):
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            handle = await souk.start_run(ref, {"messages": []})
            await socket.take(handle.run_id)
            await _claimed(souk, handle.run_id)

            await souk.cancel_run(handle.run_id)
            assert (await socket.recv()) == {"type": "cancel", "runId": handle.run_id}

            # A request, not an order: the run is 'cancelling' until its
            # stream actually ends, and the outcome is read off what arrived.
            async with asyncio.timeout(2):
                while (await souk.get_run(handle.run_id)).status != "cancelling":
                    await asyncio.sleep(0.01)
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)
    assert (await souk.get_run(handle.run_id)).status == "cancelled"


async def test_a_dropped_socket_fails_the_run_it_was_holding_and_a_reconnect_serves_afresh(souk):
    """What reconnect-mid-run means under funduq 0.0.6, asserted rather
    than assumed: a socket that drops while *holding* a claimed run has
    that run failed at once — took work, never ended it, the broker's
    `abandoned` fact — because "the party holding it is still here" is the
    one thing funduq owns and it just became false. (The older gateways'
    reconnect-and-finish is gone upstream with this; a run left merely
    *queued* still survives the drop — test_a_declined_offer_costs_the_
    run_nothing drives exactly that.)

    The reconnect — fresh ticket, fresh registration — is not punished:
    the same identity comes back listed and serves the next run end to
    end.
    """
    identity = Identity()

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            handle = await souk.start_run(ref, {"messages": []})
            await socket.take(handle.run_id)
            await socket.send(
                {
                    "type": "event",
                    "runId": handle.run_id,
                    "event": {
                        "type": "RUN_STARTED",
                        "runId": handle.run_id,
                        "threadId": handle.thread_id,
                    },
                }
            )
            async with asyncio.timeout(2):
                while not await souk.get_run_events(handle.run_id):
                    await asyncio.sleep(0.01)
            assert (await souk.get_run(handle.run_id)).status == "running"
        # the socket drops mid-run, run in hand

        async with asyncio.timeout(2):
            while (await souk.get_run(handle.run_id)).status != "failed":
                await asyncio.sleep(0.01)
        assert souk.broker.get(handle.run_id) is None
        stored = await souk.get_run(handle.run_id)
        assert stored.metadata["failureReason"] == "provider_left_holding_it"

        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            # A frame for the dead run is refused, not honoured…
            await socket.send(
                {
                    "type": "event",
                    "runId": handle.run_id,
                    "event": {"type": "RUN_FINISHED", "runId": handle.run_id},
                }
            )
            frame = await socket.recv()
            assert frame == {"type": "error", "runId": handle.run_id, "message": "event refused"}

            # …and the next run flows as if nothing happened.
            fresh = await souk.start_run(ref, {"messages": []})
            await socket.take(fresh.run_id)
            await socket.send({"type": "finish", "runId": fresh.run_id})
            await _drain(souk, fresh.run_id)


async def test_max_concurrent_runs_is_declared_at_hello_and_funduq_honours_it(souk):
    """Capacity is a fact about the provider, stated once in the hello —
    the only flow control on this wire — and funduq sizes its own bucket
    from it; nothing here counts anything."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(
                ws, souk, identity, "greeter", maxConcurrentRuns=1
            )
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            first = await souk.start_run(ref, {"messages": []})
            second = await souk.start_run(ref, {"messages": []})

            offered = await socket.take()
            # Capacity is spent: the second queued run is not offered…
            await socket.expect_nothing()
            # …until this one finishes and returns it.
            await socket.send({"type": "finish", "runId": offered["runId"]})
            next_run = await socket.take()
            assert {offered["runId"], next_run["runId"]} == {first.run_id, second.run_id}
            await socket.send({"type": "finish", "runId": next_run["runId"]})
            await _drain(souk, first.run_id, second.run_id)


async def test_a_run_enqueued_on_a_live_socket_is_offered_promptly(souk):
    """There is no poll interval to beat — the offer is a write to a
    socket funduq already holds."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            from funduq.models import AgentRef

            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            await asyncio.sleep(0.05)
            start = time.monotonic()
            handle = await souk.start_run(ref, {"messages": []})
            await socket.take(handle.run_id)
            assert time.monotonic() - start < 2
            await socket.send({"type": "finish", "runId": handle.run_id})
            await _drain(souk, handle.run_id)


async def test_a_frame_for_a_run_this_identity_does_not_hold_gets_an_error_frame(souk):
    """Holding an authenticated socket is not holding the run: core's
    ownership check answers, and the rejection comes back as an error
    frame rather than a closed connection."""
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            await socket.send(
                {"type": "event", "runId": "run_nobody", "event": {"type": "RUN_STARTED"}}
            )
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert frame["runId"] == "run_nobody"


# --- a registration that vanishes under a live socket -----------------------


async def test_a_provider_whose_agents_vanished_is_closed_so_it_re_registers(
    souk, client, monkeypatch
):
    """A registration that disappears underneath a live socket — a
    restored database, a funduq redeployed against a fresh one — leaves
    the broker serving an agent funduq's own roster no longer lists.
    Nothing routes to it, because `resolve_ref` cannot find it; nothing
    complains, because the socket is fine. A healthy container, an
    invisible agent, indefinitely.

    Closing is the repair, not a punishment: registration is what puts
    the name back, the SDK re-registers on every reconnect, so a provider
    told goodbye here returns listed.
    """
    monkeypatch.setattr(ws_provider, "OWNERSHIP_RECHECK_SECONDS", 0.05)
    identity = Identity()
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")
            assert (await client.get("/agents")).json()["agents"][0]["online"] is True

            async with souk.engine.begin() as conn:
                await conn.exec_driver_sql("DELETE FROM agents")
            assert (await client.get("/agents")).json()["agents"] == []

            with pytest.raises(WebSocketDisconnect) as excinfo:
                await socket._ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


# --- queries: the one frame on this wire that expects an answer --------------


async def _query(socket: _Socket, **params) -> dict:
    await socket.send(
        {
            "type": "query",
            "queryId": "q1",
            "method": "thread_messages",
            "params": params,
        }
    )
    frame = await socket.recv()
    assert frame["type"] == "queryResult", frame
    assert frame["queryId"] == "q1"
    return frame


async def _thread_with_messages(souk, ref, contents: list[str]) -> str:
    from funduq import repo

    thread_id = await souk.create_thread(ref)
    async with souk.session() as session:
        run = await repo.create_run(session, thread_id, ref, "ag-ui", {})
        await repo.append_thread_messages(
            session,
            thread_id,
            run["run_id"],
            [{"role": "user", "content": c} for c in contents],
        )
        await session.commit()
    return thread_id


async def test_a_provider_can_ask_for_the_history_its_run_input_never_carried(souk, register):
    """The gap the query exists for. A provider sees exactly what the
    *caller* sent for its run: an AG-UI client resends its whole history
    every turn, A2A's `message/send` carries one message, and the same
    agent cannot tell a tenth turn from a first. funduq has held the
    thread all along, and this is how it says so.
    """
    served = await register("greeter")
    thread_id = await _thread_with_messages(souk, served.ref(), ["one", "two"])

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, served.identity, "greeter")
            answer = await _query(socket, threadId=thread_id)

            assert [m["content"] for m in answer["result"]] == ["one", "two"]


async def test_limit_is_applied_by_funduq_not_by_the_caller(souk, register):
    """The parameter exists to keep the response frame bounded. Applied on
    the way back it would bound nothing — a months-old thread would already
    have crossed the wire to be trimmed."""
    served = await register("greeter")
    thread_id = await _thread_with_messages(souk, served.ref(), [str(i) for i in range(6)])

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, served.identity, "greeter")
            answer = await _query(socket, threadId=thread_id, limit=2)

            # The most recent, because context is wanted from the recent end.
            assert [m["content"] for m in answer["result"]] == ["4", "5"]


async def test_a_provider_cannot_read_a_thread_that_is_not_its_own(souk, register):
    """Thread ids are not guessable, but unguessable is not an
    authorization rule: a provider that served one run knows that thread
    id permanently, and would otherwise keep reading the conversation
    after being de-listed or after the agent moved to another stall.

    The refusal is the same as for a thread that does not exist — telling
    them apart would confirm a thread's existence to somebody who may not
    read it, which is the whole of what the check is for.
    """
    mine = await register("greeter")
    theirs = await register("greeter")
    their_thread = await souk.create_thread(theirs.ref())

    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, mine.identity, "greeter")

            refused = await _query(socket, threadId=their_thread)
            missing = await _query(socket, threadId="thread_does_not_exist")

            assert "result" not in refused
            assert refused["error"] == missing["error"]


@pytest.mark.parametrize(
    "params,expected",
    [
        pytest.param({}, "threadId", id="no-thread-id"),
        pytest.param({"threadId": "t", "limit": 0}, "limit", id="zero-limit"),
        pytest.param({"threadId": "t", "limit": "5"}, "limit", id="non-integer-limit"),
    ],
)
async def test_a_malformed_query_is_answered_not_dropped(souk, params, expected):
    """Answered on the same queryId, because the far side is waiting on
    exactly that: a query that gets no reply is a caller blocked until its
    timeout, for a mistake funduq could see immediately."""
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, Identity(), "greeter")
            answer = await _query(socket, **params)
            assert expected in answer["error"]


async def test_an_unknown_query_method_is_refused_by_name(souk):
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, Identity(), "greeter")
            await socket.send({"type": "query", "queryId": "q9", "method": "list_agents"})
            frame = await socket.recv()
            assert frame["queryId"] == "q9"
            assert "list_agents" in frame["error"]


# --- interjections: a capability the link declares, per agent ----------------


async def test_takes_interjections_is_answered_per_agent_and_honoured_at_registration(
    souk, client
):
    """Contract revision 12's field, over a wire.

    `takes_interjections(agent_name)` is **required** on every object
    handed to `attach_provider` and is *not* on the `ConnectedProvider`
    protocol, so omitting it type-checks perfectly and raises
    AttributeError inside the first `register_agents` — three layers from
    the cause. Core calls it per agent and *overwrites* whatever the
    incoming `Registration` said, so that the card's declaration is
    derived from the serving party rather than typed by an author.

    Over a wire the serving party is on the far end of this socket, so
    what this side can honestly answer from is the `register` frame's
    `takesInterjections` — asserted here both directly (the answer per
    name) and through its consequence: funduq writes the interjection
    extension's URI onto the card of the agent that declared it, and onto
    no other.
    """
    identity = Identity()
    async with _provider_client(souk) as ws_client:
        async with _connect(ws_client) as ws:
            socket = await _handshake(ws, souk, identity)
            await socket.send(
                {
                    "type": "register",
                    "agents": [
                        {"name": "interruptible", "takesInterjections": True},
                        {"name": "singleminded", "takesInterjections": False},
                        # Omitted entirely: the field is optional and an
                        # entry without it behaves exactly as before.
                        {"name": "silent"},
                    ],
                }
            )
            assert (await socket.recv())["type"] == "registered"

            from funduq.models import AgentRef

            cards = {
                name: (
                    await souk.get_agent(AgentRef(provider_key=identity.public_key, name=name))
                ).agent_card
                for name in ("interruptible", "singleminded", "silent")
            }

    extension = "https://github.com/hukaichun/funduq/ext/interjection/v1"
    assert extension in cards["interruptible"].get("extensions", [])
    assert extension not in cards["singleminded"].get("extensions", [])
    assert extension not in cards["silent"].get("extensions", [])


async def test_the_connection_answers_takes_interjections_for_the_roster_it_last_published(
    souk,
):
    """The unit half of the rule above, because the consequence alone
    cannot show the *shape* of the answer.

    `register` carries the FULL roster, so the declarations go with it: an
    agent dropped from a later register has no declaration any more,
    exactly as it has no live name. And a name this link never published
    answers False rather than raising — declaring a capability for
    somebody else's agent would be a guess, and core asks this method
    without checking first.
    """
    from souk_server.ws_provider import SocketProvider

    provider = SocketProvider("ab" * 32, asyncio.Queue(), None)
    provider.declare_interjections(
        [
            funduq_contract.Registration(name="interruptible", takesInterjections=True),
            funduq_contract.Registration(name="singleminded"),
        ]
    )

    assert provider.takes_interjections("interruptible") is True
    assert provider.takes_interjections("singleminded") is False
    assert provider.takes_interjections("never-registered") is False

    provider.declare_interjections([funduq_contract.Registration(name="singleminded")])
    assert provider.takes_interjections("interruptible") is False


# --- the two receipts the broker now reads -----------------------------------


async def test_cancel_is_a_receipt_that_the_ask_is_on_the_wire(souk):
    """`cancel` became `async` and returns `bool` at revision 11 — a
    receipt that the ask arrived, never an outcome. Returning None (what
    this used to do) logs a warning on every cancel, and returning an
    outcome would be a claim about a provider this side never observed."""
    from souk_server.ws_provider import SocketProvider

    outbound: asyncio.Queue = asyncio.Queue()
    provider = SocketProvider("ab" * 32, outbound, None)

    assert await provider.cancel("run_x") is True
    assert outbound.get_nowait() == {"type": "cancel", "runId": "run_x"}


async def test_a_verdict_for_a_run_nobody_is_waiting_on_is_answered_false_quietly(souk):
    """Offer lateness is not a path any more. `accept_late_ack` and the
    `answered_late` counter were withdrawn at revision 11: an answer
    arriving after the delivery window matches nothing and is answered
    false, because funduq has already taken the run back and will offer it
    again — a provider that really had it accepts the same run a second
    time and loses nothing.

    So the socket must stay quiet about it. An error frame here would
    teach every provider to log a scare on its own slow morning, for a
    condition the protocol calls normal.
    """
    identity = Identity()
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, identity, "greeter")

            await socket.send(
                {"type": "ack", "runId": "run_long_gone", "accepted": True}
            )
            await socket.expect_nothing()

            # …and the same for a reasoned refusal of a run nobody holds.
            await socket.send(
                {"type": "ack", "runId": "run_long_gone", "accepted": False, "reason": "no"}
            )
            await socket.expect_nothing()

            # The socket is still a working one.
            await socket.register("greeter")


async def test_an_ack_naming_no_run_at_all_is_answered_by_name(souk):
    """The one ack the contract's own `Verdict` catches for us: it
    correlates by an id, and a frame carrying none is a caller mistake
    worth a sentence rather than a silent drop."""
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, Identity(), "greeter")

            await socket.send({"type": "ack", "accepted": True})

            frame = await socket.recv()
            assert frame["type"] == "error"
            assert "ack" in frame["message"]


async def test_events_sent_in_the_same_breath_as_the_ack_are_not_lost(souk):
    """Accept and stream without pausing — and keep every event.

    An SDK-less provider answers `accepted` and starts reporting in the
    same breath, because nothing on this wire tells it to wait. That used
    to lose the opening of every such run: the verdict rode `deliver`'s
    return value while the events walked in through `report_event`, and
    nothing ordered the two roads, so the events arrived at a run funduq
    had said yes to but not yet written down. Found by running the full
    stack against the Go probe; reported as funduq#249 and fixed upstream
    at contract revision 17 by removing the second road — the verdict now
    enters through `answer_offer`, in the handler that read it, and
    everything behind it queues behind it.

    So this test no longer guards a workaround of ours; it guards that we
    still route the verdict through that door. Its weakness is worth
    stating: the old window was one event-loop turn wide, far too narrow
    to lose by racing it here, so a regression that re-parked the verdict
    on `deliver` would likely still pass this. The compose stack with the
    SDK-less probe is what actually catches that class, which is why it is
    in the verification bar and not beside it.
    """
    from funduq.models import AgentRef

    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            identity = Identity()
            socket = await _handshake(ws, souk, identity, "greeter")
            ref = AgentRef(provider_key=identity.public_key, name="greeter")
            handle = await souk.start_run(ref, {"messages": []})

            frame = await socket.recv()
            assert frame["type"] == "run"
            run_id = frame["runId"]

            # No await on anything of funduq's between the verdict and the
            # first event: the ordering a provider with its answer already
            # in hand produces.
            await socket.send({"type": "ack", "runId": run_id, "accepted": True})
            await socket.send(
                {
                    "type": "event",
                    "runId": run_id,
                    "event": {
                        "type": "RUN_STARTED",
                        "threadId": frame["threadId"],
                        "runId": run_id,
                    },
                }
            )
            await socket.send({"type": "finish", "runId": run_id})

            await socket.expect_nothing()
            assert (await souk.get_run(handle.run_id)).status not in {"queued", "running"}


async def test_a_report_naming_a_run_funduq_does_not_know_is_refused(souk):
    """`report_event` answers `False` for one thing only: no such run.

    It used to also mean "not yours", and this frame carried both. Since
    revision 17 attribution is judged by the run's own owner — a report
    from a key that does not hold the run is dropped and logged there,
    not answered synchronously — so the error frame below has exactly one
    meaning left, and a provider reading it can act on it.
    """
    async with _provider_client(souk) as client:
        async with _connect(client) as ws:
            socket = await _handshake(ws, souk, Identity(), "greeter")

            await socket.send(
                {
                    "type": "event",
                    "runId": "run_no_such_thing",
                    "event": {"type": "RUN_STARTED", "threadId": "t", "runId": "x"},
                }
            )

            frame = await socket.recv()
            assert frame["type"] == "error" and frame["message"] == "event refused"
