"""The WS /ws/kyok relay (souk_server.ws_kyok) — the socket an LLM
provider serves completions over.

The round trips: a provider's /kyok/v1/chat/completions call answered
over the socket, streaming and not, plus the error path and requestId
multiplexing. The LLM provider arrives through the same v4 ticket
handshake as /ws/provider and then publishes its offerings **on the open
link** — `register {models}` / `registered` / `deleteModel` / `deleted`
— since the signed HTTP registration road is gone upstream and the link
is the credential.

What deliberately did not change: an answer is accepted only on the
connection its request was delivered to. A second authenticated socket —
same identity, same offering, so it passes every credential check there
is — presenting a valid requestId it was never delivered is refused. And
a later attach *takes over* the offering: funduq holds one connection
per role, the replacement serves, and the replaced socket's teardown
cannot take it down.

The agent-provider side of every test stays plain HTTP; that endpoint is
deliberately untouched.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

import httpx
import pytest
from httpx_ws import WebSocketDisconnect, aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

from funduq.kyok import KyokBinding, issue_kyok_token
from funduq.models import LlmRef
from souk_server.handshake import WIRE_VERSION
from souk_server.server import create_app

from tests.conftest import TEST_SIGNING_SECRET, Identity

RECEIVE_TIMEOUT = 2.0


def _kyok_headers(bearer: str, private_key, body: bytes) -> dict:
    timestamp = str(int(time.time()))
    payload = f"funduq-kyok-call:{bearer}:{timestamp}:{hashlib.sha256(body).hexdigest()}".encode()
    return {
        "Authorization": f"Bearer {bearer}",
        "X-Souk-Kyok-Timestamp": timestamp,
        "X-Souk-Kyok-Signature": private_key.sign(payload).hex(),
        "content-type": "application/json",
    }


def _chunk(content: str = "", role: str | None = None, finish_reason: str | None = None) -> dict:
    delta: dict = {}
    if role:
        delta["role"] = role
    if content:
        delta["content"] = content
    return {
        "id": "chatcmpl-1",
        "object": "chat.completion.chunk",
        "created": 1,
        "model": "kyok",
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }


class HoldingAgent:
    """Accepts its run and holds it open — the seat an agent occupies
    while its model client is out at /kyok/v1/chat/completions."""

    async def run_stream(self, agent_name: str, run_input):
        yield {
            "type": "RUN_STARTED",
            "threadId": run_input.thread_id,
            "runId": run_input.run_id,
        }
        await asyncio.Event().wait()


def _run_input(run_id: str, thread_id: str) -> dict:
    """A valid RunAgentInput wire shape: the broker refuses to enqueue for
    an unserved agent now, so these runs really reach a provider — whose
    runtime validates the envelope before accepting."""
    return {
        "threadId": thread_id,
        "runId": run_id,
        "state": None,
        "messages": [],
        "tools": [],
        "context": [],
        "forwardedProps": None,
    }


async def _live(serve, souk, llm: LlmRef, context=None):
    """A served agent (held mid-run, the way a real KYOK caller is), a run
    the broker is dispatching, a KYOK binding to `llm`, and a token naming
    run and agent — the setup every round trip shares. The binding is
    written the way protocols/agui.py writes it at opt-in; these tests
    supply the run, not the AG-UI road in.

    A real thread and run row back the in-memory run: event persistence
    has foreign keys to satisfy, and a run failing to persist its first
    event is failed — which reads three layers away as a 403 on the
    completions route.
    """
    from funduq import repo

    served = await serve(HoldingAgent(), "greeter")
    thread_id = await souk.create_thread(served.ref())
    async with souk.session() as session:
        created = await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
        await session.commit()
    run_id = created["run_id"]
    souk.enqueue_run(run_id, served.ref(), thread_id, _run_input(run_id, thread_id), "ag-ui")
    souk.kyok_relay.bind_run(run_id, KyokBinding(llm_provider=llm, context=context))
    return served, issue_kyok_token(run_id, served.ref(), TEST_SIGNING_SECRET), run_id


def _client(souk) -> httpx.AsyncClient:
    # One client, one app: ASGIWebSocketTransport falls through to plain
    # ASGITransport for HTTP, so the agent's completions POST and the LLM
    # provider's socket exercise the same instance.
    return httpx.AsyncClient(
        transport=ASGIWebSocketTransport(app=create_app(souk)), base_url="http://test"
    )


class _LlmSocket:
    """One /ws/kyok connection speaking the frame table directly, opening
    with the same ticket handshake as the provider socket (no
    maxConcurrentRuns) and registering its offerings on the open link."""

    def __init__(self, ws, identity: Identity) -> None:
        self._ws = ws
        self.identity = identity

    async def connect(self, souk, model_names: list[str] | None = None) -> None:
        await self.send(self.identity.hello(souk))
        welcome = await self.recv()
        assert welcome["type"] == "welcome", welcome
        assert welcome["funduqPublicKey"] == souk.identity_public_key
        if model_names is not None:
            await self.register(model_names)

    async def register(self, model_names: list[str], metadata: dict | None = None) -> dict:
        frame: dict = {"type": "register", "models": model_names}
        if metadata is not None:
            frame["metadata"] = metadata
        await self.send(frame)
        answer = await self.recv()
        assert answer == {"type": "registered", "names": sorted(model_names)}, answer
        return answer

    async def recv(self) -> dict:
        return json.loads(await self._ws.receive_text(timeout=RECEIVE_TIMEOUT))

    async def send(self, frame: dict) -> None:
        await self._ws.send_text(json.dumps(frame))

    async def answer(self, request_id: str, chunks: list[dict]) -> None:
        for chunk in chunks:
            await self.send({"type": "chunk", "requestId": request_id, "data": chunk})
        await self.send({"type": "done", "requestId": request_id})


# --- handshake, registration, deletion ---------------------------------------


@pytest.mark.parametrize(
    "first_frame",
    [
        {"type": "hello"},  # no version, no identity
        {"type": "hello", "version": WIRE_VERSION, "publicKey": "ab", "nonce": "n"},  # no ticket/proof
        {"type": "chunk", "requestId": "x"},  # anything else before hello
    ],
)
async def test_a_bad_hello_closes_the_socket(souk, first_frame):
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            await ws.send_text(json.dumps(first_frame))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008


async def test_a_proof_that_answers_no_ticket_is_refused(souk):
    """Same admission rule as the provider socket: no live ticket for this
    key, no link — closed by name, not served."""
    identity = Identity()
    hello = identity.hello(souk)
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            await ws.send_text(json.dumps({**hello, "ticket": "not-a-ticket"}))
            with pytest.raises(WebSocketDisconnect) as excinfo:
                await ws.receive_text(timeout=RECEIVE_TIMEOUT)
            assert excinfo.value.code == 1008
            # The close reason is capped at 123 bytes on the wire, so match the
            # front of core's sentence rather than a word the cap may cut.
            assert "connect proof" in (excinfo.value.reason or "")


async def test_registering_on_the_link_is_the_whole_llm_provider_arrival(souk):
    """The arrival a real LLM provider makes now: ticket handshake, then
    offerings published on the open link — visible on the roster with the
    metadata, online while attached, offline the moment the socket drops
    (record intact)."""
    identity = Identity()
    ref = LlmRef(provider_key=identity.public_key, name="gpt-test")
    async with _client(souk) as client:

        async def roster_row() -> dict:
            resp = await client.get("/llm-providers")
            assert resp.status_code == 200
            (row,) = resp.json()["offerings"]
            return row

        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            socket = _LlmSocket(ws, identity)
            await socket.connect(souk)
            assert (await client.get("/llm-providers")).json() == {"offerings": []}

            await socket.register(["gpt-test"], metadata={"family": "test"})

            assert souk.kyok_relay.serving(ref) is not None
            row = await roster_row()
            assert (row["provider_key"], row["name"]) == (identity.public_key, "gpt-test")
            assert row["metadata"] == {"family": "test"}
            assert row["online"] is True

        # And detached the moment the socket is gone — registered but
        # offline, the pre-flight glance a KYOK caller binds on.
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            while souk.kyok_relay.serving(ref) is not None:
                await asyncio.sleep(0.01)
        assert (await roster_row())["online"] is False


async def test_delete_model_on_the_link_removes_the_offering(souk):
    """Deletion happens on the link that serves it — "something is serving
    it" cannot be the guard when the caller is that something, so the
    offering is taken offline and the record removed in one act. Gone
    means gone: a second order is answered with an error frame, socket
    intact. (The refusal that remains is a live run bound to the offering
    — LlmOfferingInUse — which needs work in flight, not a connection.)"""
    identity = Identity()
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            socket = _LlmSocket(ws, identity)
            await socket.connect(souk, ["gpt-test"])

            await socket.send({"type": "deleteModel", "name": "gpt-test"})
            assert (await socket.recv()) == {"type": "deleted", "name": "gpt-test"}
            assert (await client.get("/llm-providers")).json() == {"offerings": []}

            await socket.send({"type": "deleteModel", "name": "gpt-test"})
            frame = await socket.recv()
            assert frame["type"] == "error"
            assert frame["name"] == "gpt-test"

            # The link survived both answers and can publish again.
            await socket.register(["gpt-test"])


async def test_a_registration_mistake_is_answered_and_the_socket_stays(souk):
    identity = Identity()
    async with _client(souk) as client:
        async with aconnect_ws("http://test/ws/kyok", client) as ws:
            socket = _LlmSocket(ws, identity)
            await socket.connect(souk)
            await socket.send({"type": "register", "models": []})
            frame = await socket.recv()
            assert frame["type"] == "error"
            await socket.register(["gpt-test"])


# --- round trips -------------------------------------------------------------


async def test_full_round_trip_non_streaming(souk, serve):
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm, context={"voucher": "v1"})
    try:
        body = json.dumps({"model": "kyok", "messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:
            # Attached before the agent calls: resolution is per call and
            # fails fast (503) on an unattached offering, by design.
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(souk, ["gpt-test"])

                agent_call = asyncio.ensure_future(
                    client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                )
                request = await socket.recv()
                assert request["type"] == "completionRequest"
                # The policy material keep-your-own-key promises the LLM
                # provider, on the frame itself.
                assert request["runId"] == run_id
                assert request["providerKey"] == served.public_key
                assert request["agentName"] == "greeter"
                assert request["llmName"] == "gpt-test"
                assert request["context"] == {"voucher": "v1"}
                assert request["body"]["messages"][0]["content"] == "hi"
                # The chain seat rides the envelope since revision 7 —
                # this run bound no authority, and absence is stated.
                assert request.get("actorChain") in (None, [])
                await socket.answer(
                    request["requestId"],
                    [
                        _chunk(content="hello", role="assistant"),
                        _chunk(content=" world", finish_reason="stop"),
                    ],
                )
                resp = await agent_call
        assert resp.status_code == 200, resp.text
        result = resp.json()
        assert result["choices"][0]["message"]["content"] == "hello world"
        assert result["choices"][0]["finish_reason"] == "stop"
    finally:
        souk.broker.forget(run_id)


async def test_full_round_trip_streaming(souk, serve):
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm)
    try:
        body = json.dumps({"model": "kyok", "messages": [], "stream": True}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(souk, ["gpt-test"])

                async def agent_call():
                    async with client.stream(
                        "POST",
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    ) as resp:
                        assert resp.status_code == 200
                        return [line async for line in resp.aiter_lines() if line]

                async def llm_serves():
                    request = await socket.recv()
                    await socket.answer(
                        request["requestId"],
                        [_chunk(content="hi", role="assistant", finish_reason="stop")],
                    )

                lines, _ = await asyncio.gather(agent_call(), llm_serves())
        assert lines[-1] == "data: [DONE]"
        assert any("hi" in line for line in lines[:-1])
    finally:
        souk.broker.forget(run_id)


async def test_an_error_frame_fails_the_completion_fast(souk, serve):
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm)
    try:
        body = json.dumps({"model": "kyok", "messages": [], "stream": True}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(souk, ["gpt-test"])

                async def agent_call():
                    async with client.stream(
                        "POST",
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    ) as resp:
                        return [line async for line in resp.aiter_lines() if line]

                async def llm_refuses():
                    request = await socket.recv()
                    await socket.send(
                        {
                            "type": "error",
                            "requestId": request["requestId"],
                            "message": "upstream LLM call failed",
                        }
                    )

                lines, _ = await asyncio.gather(agent_call(), llm_refuses())
        # The one line is the error frame — never a [DONE] after it.
        assert len(lines) == 1
        payload = json.loads(lines[0].removeprefix("data: "))
        assert "upstream LLM call failed" in payload["error"]["message"]
    finally:
        souk.broker.forget(run_id)


async def test_a_structured_refusal_reaches_the_agent_intact(souk, serve):
    """An error frame carrying a `refusal` dict arrives as the agent's
    error payload — data, not prose — in-stream for a streaming call, and
    on the 502 body for a non-streaming one. The vocabulary inside is the
    two roles' own; nothing on this path interprets it."""
    refusal = {"kind": "throttled", "retryAfter": 30}
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")

    for run_id, stream in (("run_refused_stream", True), ("run_refused_plain", False)):
        served, token, run_id = await _live(serve, souk, llm)
        try:
            body = json.dumps({"model": "kyok", "messages": [], "stream": stream}).encode()
            async with _client(souk) as client:
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    socket = _LlmSocket(ws, llm_identity)
                    await socket.connect(souk, ["gpt-test"])

                    async def agent_call():
                        if stream:
                            async with client.stream(
                                "POST",
                                "/kyok/v1/chat/completions",
                                content=body,
                                headers=_kyok_headers(token, served.identity._key, body),
                            ) as resp:
                                return [line async for line in resp.aiter_lines() if line]
                        return await client.post(
                            "/kyok/v1/chat/completions",
                            content=body,
                            headers=_kyok_headers(token, served.identity._key, body),
                        )

                    async def llm_refuses():
                        request = await socket.recv()
                        await socket.send(
                            {
                                "type": "error",
                                "requestId": request["requestId"],
                                "message": "refused by the LLM provider",
                                "refusal": refusal,
                            }
                        )

                    answer, _ = await asyncio.gather(agent_call(), llm_refuses())
            if stream:
                assert json.loads(answer[0].removeprefix("data: ")) == {"error": refusal}
            else:
                assert answer.status_code == 502
                assert answer.json()["error"] == refusal
        finally:
            souk.broker.forget(run_id)


async def test_one_socket_multiplexes_concurrent_completions(souk, serve):
    """requestId multiplexing: two completions in flight on one socket,
    answered out of order, each answer landing on its own completion."""
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm)
    try:

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(souk, ["gpt-test"])

                async def agent_call(prompt: str) -> str:
                    body = json.dumps({"model": "kyok", "messages": [{"role": "user", "content": prompt}]}).encode()
                    resp = await client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                    assert resp.status_code == 200, resp.text
                    return resp.json()["choices"][0]["message"]["content"]

                async def llm_serves():
                    first = await socket.recv()
                    second = await socket.recv()
                    # Answer in reverse order of arrival: each answer lands
                    # on its own completion, keyed by requestId.
                    for request in (second, first):
                        prompt = request["body"]["messages"][0]["content"]
                        await socket.answer(
                            request["requestId"],
                            [_chunk(content=f"re: {prompt}", role="assistant", finish_reason="stop")],
                        )

                first_answer, second_answer, _ = await asyncio.gather(
                    agent_call("one"), agent_call("two"), llm_serves()
                )
        assert first_answer == "re: one"
        assert second_answer == "re: two"
    finally:
        souk.broker.forget(run_id)


# --- the binding -------------------------------------------------------------


async def test_an_answer_is_only_accepted_on_the_socket_the_request_was_delivered_to(
    souk, serve
):
    """The security property, proven against the strongest intruder the
    model allows: the *same identity*, attached for the *same offering* —
    every credential check passes, and later completions would genuinely
    be its to serve. It presents a valid requestId it was not delivered,
    is refused with an error frame, and the completion still gets its
    real answer from the socket that holds it."""
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm)
    try:
        body = json.dumps({"model": "kyok", "messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:
            async with aconnect_ws("http://test/ws/kyok", client) as holder_ws:
                holder = _LlmSocket(holder_ws, llm_identity)
                await holder.connect(souk, ["gpt-test"])

                agent_call = asyncio.ensure_future(
                    client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                )
                request = await holder.recv()
                request_id = request["requestId"]

                async with aconnect_ws("http://test/ws/kyok", client) as intruder_ws:
                    intruder = _LlmSocket(intruder_ws, llm_identity)
                    await intruder.connect(souk, ["gpt-test"])
                    await intruder.send(
                        {
                            "type": "chunk",
                            "requestId": request_id,
                            "data": _chunk(content="injected", role="assistant", finish_reason="stop"),
                        }
                    )
                    refusal = await intruder.recv()
                    assert refusal["type"] == "error"
                    assert refusal["requestId"] == request_id

                await holder.answer(
                    request_id, [_chunk(content="real", role="assistant", finish_reason="stop")]
                )
                resp = await agent_call
        assert resp.json()["choices"][0]["message"]["content"] == "real"
        assert "injected" not in resp.text
    finally:
        souk.broker.forget(run_id)


async def test_a_later_attach_takes_over_the_offering_and_the_old_teardown_spares_it(
    souk, serve
):
    """funduq holds one connection per role: a re-attach under the same
    key replaces the old link, new completions resolve to the newcomer,
    and — because detach is connection-scoped — the replaced socket's own
    teardown cannot take the replacement offline.

    The old socket lives in its own task because httpx-ws contexts hold
    anyio cancel scopes, which must unwind in the task that entered them
    — the shape of the test, not of the property.
    """
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm)
    try:
        body = json.dumps({"model": "kyok", "messages": [{"role": "user", "content": "hi"}]}).encode()

        async with _client(souk) as client:
            old_attached = asyncio.Event()
            release_old = asyncio.Event()

            async def old_socket():
                async with aconnect_ws("http://test/ws/kyok", client) as ws:
                    await _LlmSocket(ws, llm_identity).connect(souk, ["gpt-test"])
                    old_attached.set()
                    await release_old.wait()

            old_task = asyncio.create_task(old_socket())
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await old_attached.wait()

            async with aconnect_ws("http://test/ws/kyok", client) as new_ws:
                new = _LlmSocket(new_ws, llm_identity)
                await new.connect(souk, ["gpt-test"])

                # The old socket goes away *after* the takeover — and its
                # teardown, being connection-scoped, is a no-op against
                # the link that replaced it.
                release_old.set()
                async with asyncio.timeout(RECEIVE_TIMEOUT):
                    await old_task
                assert souk.kyok_relay.serving(llm) is not None

                # The offering is still served — by the newcomer, which
                # answers the next completion end to end.
                agent_call = asyncio.ensure_future(
                    client.post(
                        "/kyok/v1/chat/completions",
                        content=body,
                        headers=_kyok_headers(token, served.identity._key, body),
                    )
                )
                request = await new.recv()
                assert request["type"] == "completionRequest"
                await new.answer(
                    request["requestId"],
                    [_chunk(content="takeover", role="assistant", finish_reason="stop")],
                )
                resp = await agent_call
        assert resp.json()["choices"][0]["message"]["content"] == "takeover"
    finally:
        souk.broker.forget(run_id)


async def test_a_dropped_socket_fails_its_in_flight_completions_fast(souk, serve):
    """A truncated answer must fail the completion, not complete it — and
    fail it now, not at the chunk-gap timeout."""
    llm_identity = Identity()
    llm = LlmRef(provider_key=llm_identity.public_key, name="gpt-test")
    served, token, run_id = await _live(serve, souk, llm)
    try:
        body = json.dumps({"model": "kyok", "messages": [], "stream": True}).encode()

        async with _client(souk) as client:

            async def agent_call():
                async with client.stream(
                    "POST",
                    "/kyok/v1/chat/completions",
                    content=body,
                    headers=_kyok_headers(token, served.identity._key, body),
                ) as resp:
                    return [line async for line in resp.aiter_lines() if line]

            async with aconnect_ws("http://test/ws/kyok", client) as ws:
                socket = _LlmSocket(ws, llm_identity)
                await socket.connect(souk, ["gpt-test"])
                call = asyncio.ensure_future(agent_call())
                request = await socket.recv()
                await socket.send(
                    {
                        "type": "chunk",
                        "requestId": request["requestId"],
                        "data": _chunk(content="half an ans", role="assistant"),
                    }
                )
            # the socket drops with the answer unfinished
            async with asyncio.timeout(5):
                lines = await call
        payload = json.loads(lines[-1].removeprefix("data: "))
        assert "disconnected mid-response" in payload["error"]["message"]
    finally:
        souk.broker.forget(run_id)
