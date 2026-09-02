"""SoukProvider's WebSocket transport (souk_agent_sdk.client) against a
stub gateway speaking the /ws/provider frame protocol — the table in the
gateway repo's docs/server-mode.md, which that repo authors and this SDK
implements. The stub is the server half of one link: it mints tickets on
a tiny HTTP listener (`POST /tickets`), runs the two-frame handshake,
answers the `register` frame, pushes what a test tells it to, and
records every frame the provider sends back.

**The stub signs.** It holds a funduq identity and answers the handshake
with a real signature over `funduq_connect_payload(ticket, nonce)`,
because the thing being tested on this side is partly whether the
provider *checks* that — and a stub that skipped it would let a provider
that never verified anything pass.

One licence the stub takes: the deployed gateway serves `/tickets` and
`/ws/provider` from one listener, while the stub runs them on two ports
(websockets' server refuses HTTP requests with bodies). The provider's
derivation of the one from the other is covered separately
(`test_the_ws_url_is_the_http_url_with_the_scheme_swapped`), and the
tests point `_ws_url` at the stub's second port.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import secrets
from typing import Any

import pytest
import websockets
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from funduq_provider_sdk import (
    WrongFunduq,
    funduq_connect_payload,
    provider_connect_payload,
    verify_signature,
)

from souk_agent_sdk.client import (
    AgentHandle,
    SoukIdentityMismatch,
    SoukProvider,
    SoukQueryFailed,
)

RECEIVE_TIMEOUT = 2.0


class StubGateway:
    """One souk, as a provider sees it: a ticket desk and a work socket.

    `advertised_key` is what `/tickets` presents as `funduqPublicKey`;
    `answer_signer` is the key that actually signs the welcome's answer.
    Splitting them is what lets a test stand up an imposter — something
    that presents a key it cannot sign with.
    """

    def __init__(
        self,
        identity: Ed25519PrivateKey | None = None,
        advertised_key: str | None = None,
        answer_signer: Ed25519PrivateKey | None = None,
        refuse_register: str | None = None,
    ) -> None:
        self._identity = identity or Ed25519PrivateKey.generate()
        self.public_key = self._identity.public_key().public_bytes_raw().hex()
        self.advertised_key = advertised_key or self.public_key
        self._answer_signer = answer_signer or self._identity
        self.refuse_register = refuse_register
        self.ticket_requests: list[dict] = []
        self.tickets: list[str] = []
        self.hello: dict | None = None
        self.registers: list[dict] = []
        self.frames: asyncio.Queue = asyncio.Queue()
        self.connected = asyncio.Event()
        self._conn = None

    @property
    def register(self) -> dict | None:
        return self.registers[-1] if self.registers else None

    async def __aenter__(self) -> "StubGateway":
        self._http = await asyncio.start_server(self._serve_ticket, "127.0.0.1", 0)
        self.http_port = self._http.sockets[0].getsockname()[1]
        self._ws = await websockets.serve(self._handler, "127.0.0.1", 0)
        self.ws_port = self._ws.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc) -> None:
        self._http.close()
        await self._http.wait_closed()
        self._ws.close()
        await self._ws.wait_closed()

    async def _serve_ticket(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Just enough HTTP/1.1 for `POST /tickets` from httpx."""
        try:
            request_line = await reader.readline()
            content_length = 0
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin-1").partition(":")
                if name.strip().lower() == "content-length":
                    content_length = int(value.strip())
            body = await reader.readexactly(content_length) if content_length else b""
            assert request_line.startswith(b"POST /tickets "), request_line
            self.ticket_requests.append(json.loads(body))
            ticket = secrets.token_hex(16)
            self.tickets.append(ticket)
            payload = json.dumps(
                {"ticket": ticket, "funduqPublicKey": self.advertised_key}
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                + f"Content-Length: {len(payload)}\r\n".encode()
                + b"Connection: close\r\n\r\n"
                + payload
            )
            await writer.drain()
        finally:
            writer.close()

    async def _handler(self, ws) -> None:
        with contextlib.suppress(websockets.ConnectionClosed):
            self.hello = json.loads(await ws.recv())
            answer = self._answer_signer.sign(
                funduq_connect_payload(self.hello["ticket"], self.hello["nonce"])
            ).hex()
            await ws.send(
                json.dumps(
                    {"type": "welcome", "funduqPublicKey": self.advertised_key, "answer": answer}
                )
            )
            register = json.loads(await ws.recv())
            self.registers.append(register)
            if self.refuse_register is not None:
                await ws.send(json.dumps({"type": "error", "message": self.refuse_register}))
            else:
                await ws.send(
                    json.dumps(
                        {
                            "type": "registered",
                            "names": [agent["name"] for agent in register.get("agents", [])],
                        }
                    )
                )
            self._conn = ws
            self.connected.set()
            async for raw in ws:
                self.frames.put_nowait(json.loads(raw))

    async def push(self, frame: dict) -> None:
        await self._conn.send(json.dumps(frame))

    async def next_frame(self) -> dict:
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            return await self.frames.get()


def _provider(gateway: StubGateway, handles: list[AgentHandle], **kwargs: Any) -> SoukProvider:
    provider = SoukProvider(f"http://127.0.0.1:{gateway.http_port}", handles, **kwargs)
    # The deployed listener is one port; the stub's are two (see module
    # docstring). The derivation itself is covered by its own test.
    provider._ws_url = f"ws://127.0.0.1:{gateway.ws_port}/ws/provider"
    return provider


def _input(run_id: str, thread_id: str = "t1") -> dict:
    """A wire `input` the SDK will accept: it validates the frame into
    `ag_ui.core.RunAgentInput` before offering the run, so a partial dict
    is a decline now, not a lenient pass-through."""
    return {
        "threadId": thread_id,
        "runId": run_id,
        "state": None,
        "messages": [],
        "tools": [],
        "context": [],
        "forwardedProps": None,
    }


async def _echo_run_stream(run_input) -> Any:
    yield {"type": "RUN_STARTED", "runId": run_input.run_id}
    yield {"type": "RUN_FINISHED", "runId": run_input.run_id}


@contextlib.asynccontextmanager
async def _connected(gateway: StubGateway, provider: SoukProvider):
    """A connected provider whose runtime is running.

    `run_forever` starts the runtime and then loops on `_run_connection`;
    these tests drive the connection directly, so the start is theirs to
    do. Without it the socket comes up, the handshake completes, every
    offer is accepted — and nothing ever runs, which reads as a hang
    rather than as a missing call.
    """
    provider.runtime.start()
    conn = asyncio.create_task(provider._run_connection())
    try:
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            await gateway.connected.wait()
        yield conn
    finally:
        conn.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await conn
        await provider.runtime.aclose(cancel_in_flight=True)


# --- the address -------------------------------------------------------------


def test_the_ws_url_is_the_http_url_with_the_scheme_swapped(tmp_path):
    """One URL is the whole address in deployment: the work socket is the
    same listener as the ticket desk, scheme swapped. The stub gateway
    overrides this; the deployed path is this derivation."""
    provider = SoukProvider(
        "https://souk.example:8443/base/",
        [AgentHandle(name="echo", run_stream=_echo_run_stream)],
        identity_key_path=str(tmp_path / "k.key"),
    )
    assert provider._ws_url == "wss://souk.example:8443/base/ws/provider"


# --- the handshake ----------------------------------------------------------


async def test_the_ticket_is_fetched_for_this_key_and_the_hello_carries_it(tmp_path):
    """The out-of-band half first: `POST /tickets` names the provider's
    key, and the hello presents the minted ticket with the proof already
    computed — no names anywhere, and nothing four-frame left."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            max_concurrent_runs=2,
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            assert gateway.ticket_requests == [{"publicKey": provider.public_key}]
            assert gateway.hello["type"] == "hello"
            assert gateway.hello["version"] == 4
            assert gateway.hello["publicKey"] == provider.public_key
            assert gateway.hello["ticket"] == gateway.tickets[0]
            assert gateway.hello["maxConcurrentRuns"] == 2
            assert len(gateway.hello["nonce"]) == 32
            assert gateway.hello["proof"]
            # What the link serves is deliberately not in the handshake
            # any more: a ticket issued to one key cannot be replayed at
            # all, so the names moved to the register frame.
            assert "agentNames" not in gateway.hello


async def test_the_proof_names_the_funduq_and_signs_the_ticket_and_nonce(tmp_path):
    """Verified by rebuilding the payload from what the stub actually
    handed out and received — so a provider that signed the right shape
    over the wrong bytes fails here. The payload is upstream's
    `provider_connect_payload(funduq_key, ticket, nonce)`: the recipient's
    key is *in the signed bytes*, which is what stops a proof coaxed out
    by one funduq being relayed to attach at another."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            assert verify_signature(
                provider.public_key,
                gateway.hello["proof"],
                provider_connect_payload(
                    gateway.public_key, gateway.hello["ticket"], gateway.hello["nonce"]
                ),
            )


async def test_a_pinned_provider_refuses_a_mismatching_funduq_before_signing(tmp_path):
    """The pin is checked against the key `/tickets` presents, *before*
    anything is signed: a provider that signed first would already have
    named the wrong funduq in a proof it produced. No hello ever goes
    out."""
    async with StubGateway(advertised_key="ab" * 32) as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
            souk_public_key="cd" * 32,
        )
        with pytest.raises(SoukIdentityMismatch, match="not the"):
            await provider._run_connection()
        assert gateway.hello is None


async def test_a_pinned_provider_accepts_the_souk_it_pinned(tmp_path):
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
            souk_public_key=gateway.public_key,
        )
        async with _connected(gateway, provider):
            assert gateway.hello is not None


async def test_a_welcome_whose_answer_does_not_verify_is_refused_before_registering(tmp_path):
    """A key in the ticket answer is a claim, not a proof. This stub
    presents a real public key and signs the welcome with a different one
    — which is what anything relaying a genuine souk's advertised key,
    without its private half, would produce. Refused even *unpinned*: the
    proof bound this handshake to the advertised key, and the welcome
    must prove possession of that same key before the link is treated as
    open — so no register frame ever goes out."""
    async with StubGateway(answer_signer=Ed25519PrivateKey.generate()) as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        with pytest.raises(WrongFunduq, match="did not prove"):
            await provider._run_connection()
        assert gateway.register is None


# --- registration on the open link ------------------------------------------


async def test_registration_rides_the_link_unsigned_and_carries_the_whole_card(tmp_path):
    """After the verified welcome, the roster goes up as one `register`
    frame — name, description, agentCardExtra, metadata per agent,
    camelCase on the wire — and nothing in it is signed: the key was
    proved when the link opened."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [
                AgentHandle(
                    name="echo",
                    description="repeats things",
                    run_stream=_echo_run_stream,
                    agent_card_extra={"skills": [{"id": "echoing"}]},
                    metadata={"tier": "demo"},
                )
            ],
            identity_key_path=str(tmp_path / "k.key"),
            provider_name="Echo Stall",
        )
        async with _connected(gateway, provider):
            assert gateway.register == {
                "type": "register",
                "agents": [
                    {
                        "name": "echo",
                        "description": "repeats things",
                        "agentCardExtra": {"skills": [{"id": "echoing"}]},
                        "metadata": {"tier": "demo"},
                    }
                ],
                "providerName": "Echo Stall",
            }
            assert "signature" not in gateway.register


async def test_a_refused_registration_raises_with_souks_reason(tmp_path):
    """souk answers a bad roster with an `error` frame and keeps the
    socket open; this side raises rather than idling registered-as-
    nothing, so `run_forever`'s reconnect loop retries it."""
    async with StubGateway(refuse_register="name 'echo' is not yours") as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        provider.runtime.start()
        try:
            with pytest.raises(RuntimeError, match="not yours"):
                await provider._run_connection()
        finally:
            await provider.runtime.aclose(cancel_in_flight=True)


# --- runs over the socket ---------------------------------------------------


async def test_a_pushed_run_comes_back_as_an_ack_then_events_then_finish(tmp_path):
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r1",
                    "threadId": "t1",
                    "agentName": "echo",
                    "runInput": _input("r1"),
                }
            )
            frames = [await gateway.next_frame() for _ in range(4)]
            assert [f["type"] for f in frames] == ["ack", "event", "event", "finish"]
            assert frames[0] == {"type": "ack", "runId": "r1", "accepted": True}
            assert frames[1]["event"]["type"] == "RUN_STARTED"
            assert frames[2]["event"]["type"] == "RUN_FINISHED"
            assert all(f["runId"] == "r1" for f in frames)


async def test_a_cancel_interrupts_the_run_and_finish_still_goes_out(tmp_path):
    started = asyncio.Event()

    async def stuck_run_stream(run_input) -> Any:
        yield {"type": "RUN_STARTED", "runId": run_input.run_id}
        started.set()
        await asyncio.sleep(3600)  # a run that would never end on its own

    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="stuck", run_stream=stuck_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r1",
                    "threadId": "t1",
                    "agentName": "stuck",
                    "runInput": _input("r1"),
                }
            )
            assert (await gateway.next_frame())["type"] == "ack"
            assert (await gateway.next_frame())["type"] == "event"
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await started.wait()

            await gateway.push({"type": "cancel", "runId": "r1"})
            # Complying is the provider's choice, and this one complies:
            # the run's current await is interrupted, and finish — the
            # last word souk decides the outcome from — still goes out.
            assert (await gateway.next_frame()) == {"type": "finish", "runId": "r1"}


async def test_invalid_input_is_refused_with_the_reason_on_the_ack(tmp_path):
    """The permanent-refusal path: input failing `RunAgentInput`
    validation is a `Refusal`, not a transient decline — it reaches the
    wire as `accepted: false` plus the reason souk records verbatim,
    because the same bytes re-offered can never do better."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r_bad",
                    "threadId": "t1",
                    "agentName": "echo",
                    "runInput": {"runId": "r_bad"},  # no threadId/state/... — not a RunAgentInput
                }
            )
            ack = await gateway.next_frame()
            assert ack["type"] == "ack"
            assert ack["runId"] == "r_bad"
            assert ack["accepted"] is False
            assert "DeliveredRun" in ack["reason"]


async def test_a_run_for_an_unknown_agent_is_declined_without_taking_the_socket_down(tmp_path):
    """Declining is a real answer, so an agent this provider does not
    host produces `accepted: false` rather than silence. souk keeps the
    run, and this socket goes on serving."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r_alien",
                    "threadId": "t1",
                    "agentName": "not_ours",
                    "runInput": _input("r_alien"),
                }
            )
            # A bare decline, no reason: "not ours" is deliberately NOT a
            # permanent refusal — another provider hosting the name could
            # still take the run, and a reasoned ack would make souk fail
            # it for everybody.
            assert (await gateway.next_frame()) == {
                "type": "ack",
                "runId": "r_alien",
                "accepted": False,
            }
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r2",
                    "threadId": "t1",
                    "agentName": "echo",
                    "runInput": _input("r2"),
                }
            )
            frames = [await gateway.next_frame() for _ in range(4)]
            assert all(f["runId"] == "r2" for f in frames)


async def test_a_dropped_socket_does_not_end_the_run_and_its_frames_flush_after_reregistering(tmp_path):
    """The SDK half of "a dropped socket ends nothing".

    Three things make it true, and the third is new with the ticket wire.
    The outbound queue is not per-connection — a run is addressed by
    runId, not by the socket it arrived on — so frames a dead socket
    failed to carry go out on the next one. The *runtime* is not tied to
    the connection either: the agent keeps running while there is no
    socket at all, and finishes into the queue. And every reconnect is
    the full ceremony again — a fresh ticket, a fresh handshake, and a
    fresh `register`, because the roster lives on the link and died with
    it.
    """
    release = asyncio.Event()

    async def two_phase_run_stream(run_input) -> Any:
        yield {"type": "RUN_STARTED", "runId": run_input.run_id}
        await release.wait()
        yield {"type": "RUN_FINISHED", "runId": run_input.run_id}

    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="twophase", run_stream=two_phase_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        provider.runtime.start()
        conn = asyncio.create_task(provider._run_connection())
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            await gateway.connected.wait()
        await gateway.push(
            {
                "type": "run",
                "runId": "r1",
                "threadId": "t1",
                "agentName": "twophase",
                "runInput": _input("r1"),
            }
        )
        assert (await gateway.next_frame())["type"] == "ack"
        assert (await gateway.next_frame())["type"] == "event"

        # The socket drops mid-run, with the agent parked on `release`.
        await gateway._conn.close()
        with contextlib.suppress(Exception):
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await conn
        gateway.connected.clear()

        # A fresh connection — run_forever would do exactly this.
        conn2 = asyncio.create_task(provider._run_connection())
        try:
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await gateway.connected.wait()
            # The reconnect was a whole new admission: a second ticket
            # was minted and burnt, and the roster was published again.
            assert len(gateway.tickets) == 2
            assert len(gateway.registers) == 2
            release.set()
            frames = [await gateway.next_frame() for _ in range(2)]
            assert frames[0]["event"]["type"] == "RUN_FINISHED"
            assert frames[1] == {"type": "finish", "runId": "r1"}
        finally:
            conn2.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await conn2
            await provider.runtime.aclose(cancel_in_flight=True)


# --- queries: request/response over a one-way wire --------------------------


async def test_a_query_goes_out_with_a_correlation_id_and_its_answer_comes_back(tmp_path):
    """The first thing on this wire that expects a reply. Everything else a
    provider sends is fire-and-forget, which is why the queryId exists at
    all: several questions may be outstanding on one socket, and an answer
    has to find the caller that asked."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            asked = asyncio.create_task(provider.thread_messages("t1", limit=3))

            query = await gateway.next_frame()
            assert query["type"] == "query"
            assert query["method"] == "thread_messages"
            assert query["params"] == {"threadId": "t1", "limit": 3}
            assert query["queryId"]

            await gateway.push(
                {
                    "type": "queryResult",
                    "queryId": query["queryId"],
                    "result": [{"role": "user", "content": "hi"}],
                }
            )
            assert await asyncio.wait_for(asked, RECEIVE_TIMEOUT) == [
                {"role": "user", "content": "hi"}
            ]


async def test_two_queries_in_flight_get_their_own_answers(tmp_path):
    """Answered out of order, on purpose: correlation is the point of the
    id, and a queue would have served the wrong caller."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            first = asyncio.create_task(provider.thread_messages("t1"))
            second = asyncio.create_task(provider.thread_messages("t2"))
            q1 = await gateway.next_frame()
            q2 = await gateway.next_frame()

            for query in (q2, q1):
                await gateway.push(
                    {
                        "type": "queryResult",
                        "queryId": query["queryId"],
                        "result": [{"thread": query["params"]["threadId"]}],
                    }
                )

            assert await asyncio.wait_for(first, RECEIVE_TIMEOUT) == [{"thread": "t1"}]
            assert await asyncio.wait_for(second, RECEIVE_TIMEOUT) == [{"thread": "t2"}]


async def test_an_error_answer_raises_rather_than_returning_nothing(tmp_path):
    """`[]` is a real answer — a thread with nothing in it — so a failure
    that returned it would have an agent summarise an empty history as if
    it were the conversation."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            asked = asyncio.create_task(provider.thread_messages("t_not_mine"))
            query = await gateway.next_frame()
            await gateway.push(
                {
                    "type": "queryResult",
                    "queryId": query["queryId"],
                    "error": "no such thread for this provider",
                }
            )
            with pytest.raises(SoukQueryFailed, match="no such thread"):
                await asyncio.wait_for(asked, RECEIVE_TIMEOUT)


async def test_a_socket_that_dies_fails_its_outstanding_queries_at_once(tmp_path):
    """Not left to time out. The answer is already known, and a caller
    waiting the full timeout for a certainty is only a slower failure —
    and unlike a run, a query is not retried on the next connection: the
    agent asked mid-run, and whether it still wants the answer is the
    agent's to decide."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        provider.runtime.start()
        conn = asyncio.create_task(provider._run_connection())
        try:
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await gateway.connected.wait()
            asked = asyncio.create_task(provider.thread_messages("t1"))
            await gateway.next_frame()

            await gateway._conn.close()

            with pytest.raises(SoukQueryFailed, match="closed"):
                await asyncio.wait_for(asked, RECEIVE_TIMEOUT)
        finally:
            conn.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await conn
            await provider.runtime.aclose(cancel_in_flight=True)


async def test_an_answer_to_a_question_nobody_is_waiting_on_is_dropped(tmp_path):
    """A late reply — its caller timed out, or its socket already failed
    it — must not take the connection down with it."""
    async with StubGateway() as gateway:
        provider = _provider(
            gateway,
            [AgentHandle(name="echo", run_stream=_echo_run_stream)],
            identity_key_path=str(tmp_path / "k.key"),
        )
        async with _connected(gateway, provider):
            await gateway.push(
                {"type": "queryResult", "queryId": "never-asked", "result": []}
            )
            # Still serving.
            await gateway.push(
                {
                    "type": "run",
                    "runId": "r1",
                    "threadId": "t1",
                    "agentName": "echo",
                    "runInput": _input("r1"),
                }
            )
            assert (await gateway.next_frame())["type"] == "ack"
