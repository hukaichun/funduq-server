"""Covers KyokBridge — the caller-side half of KYOK (Keep Your Own Key),
an identified LLM provider speaking wire v4. See
souk_client_sdk/kyok_bridge.py's own docstring for the transport (ticket
over HTTP, two-frame handshake, registration on the open link) and
upstream funduq's docs/mechanisms/kyok.md for the design.

Uses a stub gateway speaking the wire-v4 protocol — one websockets server
whose HTTP side answers `POST /tickets` and whose WS side walks the
hello/welcome handshake and the register/registered exchange (no real
souk instance needed) — and monkeypatches litellm.acompletion directly
rather than adding another mocking layer for it: litellm is already a
runtime dependency here.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any

import litellm
import pytest
import websockets
from funduq_provider_sdk import (
    WrongFunduq,
    funduq_connect_payload,
    provider_connect_payload,
    verify_signature,
)
from funduq_provider_sdk.llm import CompletionRefused, ProviderIdentity

from souk_client_sdk.kyok_bridge import HANDSHAKE_VERSION, KyokBridge, _to_chunk_dict

RECEIVE_TIMEOUT = 2.0


# --- _to_chunk_dict: all three normalization paths -------------------------


class _ModelDumpChunk:
    def model_dump(self, mode: str = "python") -> dict:
        return {"via": "model_dump", "mode": mode}


class _DictMethodChunk:
    def dict(self) -> dict:
        return {"via": "dict_method"}


def test_to_chunk_dict_uses_model_dump_when_available():
    assert _to_chunk_dict(_ModelDumpChunk()) == {"via": "model_dump", "mode": "json"}


def test_to_chunk_dict_falls_back_to_dict_method():
    assert _to_chunk_dict(_DictMethodChunk()) == {"via": "dict_method"}


def test_to_chunk_dict_falls_back_to_plain_dict_conversion():
    assert _to_chunk_dict({"via": "plain_dict"}) == {"via": "plain_dict"}


# --- metadata ---------------------------------------------------------------


def test_run_metadata_names_the_offering_and_carries_the_context():
    bridge = KyokBridge("http://souk.local", model="test-model", api_key="key", offering="my-llm")
    assert bridge.run_metadata({"voucher": "v1"}) == {
        "kyok": {
            "llmProvider": {
                "providerKey": bridge.identity.public_key,
                "name": "my-llm",
            },
            "context": {"voucher": "v1"},
        }
    }
    # No context → no context key, not a null one: souk treats the field
    # as opaque and absent is the honest shape for "nothing shared".
    assert "context" not in bridge.run_metadata()["kyok"]


# --- the ticket endpoint ----------------------------------------------------


async def test_the_ticket_request_names_the_key_it_admits():
    """POST /tickets carries exactly {"publicKey"}, and the bridge reads
    back the ticket and funduq's key — the material the proof binds."""
    seen: dict = {}

    async def handle(reader, writer):
        raw = b""
        while b"\r\n\r\n" not in raw:
            raw += await reader.read(1024)
        head, _, body = raw.partition(b"\r\n\r\n")
        length = next(
            int(line.split(b":")[1]) for line in head.split(b"\r\n")
            if line.lower().startswith(b"content-length")
        )
        while len(body) < length:
            body += await reader.read(1024)
        seen["path"] = head.split(b" ")[1].decode()
        seen["body"] = json.loads(body)
        payload = json.dumps({"ticket": "t_1", "funduqPublicKey": "cd" * 32}).encode()
        writer.write(
            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
            + f"content-length: {len(payload)}\r\n\r\n".encode()
            + payload
        )
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        bridge = KyokBridge(f"http://127.0.0.1:{port}", model="m", api_key="k")
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            ticket, funduq_key = await bridge._fetch_ticket()
    finally:
        server.close()
        await server.wait_closed()
    assert seen["path"] == "/tickets"
    assert seen["body"] == {"publicKey": bridge.identity.public_key}
    assert (ticket, funduq_key) == ("t_1", "cd" * 32)


async def test_a_pinned_funduq_key_is_checked_at_ticket_time():
    """The pin refuses before any socket exists: a ticket endpoint
    presenting the wrong funduq key raises WrongFunduq out of the fetch."""
    async with StubGateway() as gateway:
        bridge = KyokBridge(
            f"http://127.0.0.1:{gateway.port}",
            model="m",
            api_key="k",
            funduq_public_key="ee" * 32,  # not the stub's key
        )
        with pytest.raises(WrongFunduq):
            await bridge._fetch_ticket()
    assert gateway.hello is None  # never connected


# --- the socket ------------------------------------------------------------


class StubGateway:
    """The gateway's half of wire v4, minimally: HTTP `POST /tickets` on
    the same listener, then per WS connection the hello/welcome handshake
    (signing the answer with its own funduq identity), the register/
    registered exchange, deleted echoes for deleteModel — recording every
    frame the bridge sends.

    One port, two protocols, like the real gateway: a small TCP front
    answers `/tickets` itself (websockets' own HTTP hook refuses any
    request with a body) and pipes every other connection through to the
    websockets server byte-for-byte."""

    def __init__(self, *, bad_answer: bool = False) -> None:
        self.funduq_identity = ProviderIdentity.generate()
        self.bad_answer = bad_answer
        self.ticket = "t_stub"
        self.ticket_requests: list[dict] = []
        self.hello: dict | None = None
        self.register: dict | None = None
        self.register_count = 0
        self.delete_frames: list[dict] = []
        self.frames: asyncio.Queue = asyncio.Queue()
        self.connected = asyncio.Event()
        self._conn = None
        self._pumps: set[asyncio.Task] = set()

    async def __aenter__(self) -> "StubGateway":
        self._ws_server = await websockets.serve(self._handler, "127.0.0.1", 0)
        self._ws_port = self._ws_server.sockets[0].getsockname()[1]
        self._front = await asyncio.start_server(self._front_conn, "127.0.0.1", 0)
        self.port = self._front.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc) -> None:
        self._front.close()
        await self._front.wait_closed()
        for task in list(self._pumps):
            task.cancel()
        self._ws_server.close()
        await self._ws_server.wait_closed()

    async def _front_conn(self, reader, writer) -> None:
        raw = b""
        while b"\r\n\r\n" not in raw:
            chunk = await reader.read(4096)
            if not chunk:
                writer.close()
                return
            raw += chunk
        head, _, body = raw.partition(b"\r\n\r\n")
        if head.split(b" ")[1] == b"/tickets":
            length = next(
                int(line.split(b":")[1]) for line in head.split(b"\r\n")
                if line.lower().startswith(b"content-length")
            )
            while len(body) < length:
                body += await reader.read(4096)
            self.ticket_requests.append(json.loads(body))
            payload = json.dumps(
                {"ticket": self.ticket, "funduqPublicKey": self.funduq_identity.public_key}
            ).encode()
            writer.write(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n"
                + f"content-length: {len(payload)}\r\nconnection: close\r\n\r\n".encode()
                + payload
            )
            await writer.drain()
            writer.close()
            return
        # Anything else is the WS upgrade: replay what we read, then pump.
        up_reader, up_writer = await asyncio.open_connection("127.0.0.1", self._ws_port)
        up_writer.write(raw)
        await up_writer.drain()

        async def pump(src, dst):
            try:
                while True:
                    data = await src.read(65536)
                    if not data:
                        break
                    dst.write(data)
                    await dst.drain()
            except (ConnectionError, asyncio.CancelledError):
                pass
            finally:
                with contextlib.suppress(Exception):
                    dst.close()

        for direction in (pump(reader, up_writer), pump(up_reader, writer)):
            task = asyncio.create_task(direction)
            self._pumps.add(task)
            task.add_done_callback(self._pumps.discard)

    async def _handler(self, ws) -> None:
        self.hello = json.loads(await ws.recv())
        answer = self.funduq_identity.sign(
            funduq_connect_payload(self.hello["ticket"], self.hello["nonce"])
        )
        if self.bad_answer:
            answer = "00" * 64
        await ws.send(
            json.dumps(
                {
                    "type": "welcome",
                    "funduqPublicKey": self.funduq_identity.public_key,
                    "answer": answer,
                }
            )
        )
        self.register = json.loads(await ws.recv())
        self.register_count += 1
        await ws.send(
            json.dumps({"type": "registered", "names": self.register.get("models", [])})
        )
        self._conn = ws
        self.connected.set()
        async for raw in ws:
            frame = json.loads(raw)
            if frame.get("type") == "deleteModel":
                self.delete_frames.append(frame)
                await ws.send(json.dumps({"type": "deleted", "name": frame["name"]}))
            else:
                self.frames.put_nowait(frame)

    async def push(self, frame: dict) -> None:
        await self._conn.send(json.dumps(frame))

    async def next_frame(self) -> dict:
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            return await self.frames.get()


async def _connected_bridge(gateway: StubGateway, **kwargs: Any):
    bridge = KyokBridge(
        f"http://127.0.0.1:{gateway.port}",
        model="test-model",
        api_key="key",
        reconnect_delay=0.05,
        **kwargs,
    )
    task = asyncio.create_task(bridge.serve_forever())
    async with asyncio.timeout(RECEIVE_TIMEOUT):
        await gateway.connected.wait()
    return bridge, task


async def _stop(task: asyncio.Task) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


async def test_the_hello_carries_the_ticket_and_a_verifiable_proof():
    """v4: one hello frame, computed before connecting — version, key,
    ticket, nonce and the proof over provider_connect_payload(funduq_key,
    ticket, nonce). No model names (registration moved onto the link) and
    no maxConcurrentRuns (kyok flow control is per-request)."""
    async with StubGateway() as gateway:
        bridge, task = await _connected_bridge(gateway, offering="my-llm")
        try:
            hello = gateway.hello
            assert hello["type"] == "hello"
            assert hello["version"] == HANDSHAKE_VERSION == 4
            assert hello["publicKey"] == bridge.identity.public_key
            assert hello["ticket"] == gateway.ticket
            assert "modelNames" not in hello
            assert "maxConcurrentRuns" not in hello
            assert verify_signature(
                bridge.identity.public_key,
                hello["proof"],
                provider_connect_payload(
                    gateway.funduq_identity.public_key, gateway.ticket, hello["nonce"]
                ),
            )
        finally:
            await _stop(task)


async def test_a_wrong_answer_in_the_welcome_is_refused_before_anything_flows():
    """The provider-protecting half of the handshake: an answer that does
    not verify over funduq_connect_payload(ticket, nonce) means the link
    is never treated open — no registration, no attach, just a reconnect
    loop that keeps refusing."""
    async with StubGateway(bad_answer=True) as gateway:
        bridge = KyokBridge(
            f"http://127.0.0.1:{gateway.port}", model="m", api_key="k", reconnect_delay=0.05
        )
        task = asyncio.create_task(bridge.serve_forever())
        try:
            await asyncio.sleep(0.3)  # several connect cycles
            assert gateway.hello is not None  # it did try
            assert gateway.register is None  # and never published
            assert not bridge.attached.is_set()
        finally:
            await _stop(task)


async def test_registration_travels_on_the_link_and_gates_attached():
    async with StubGateway() as gateway:
        bridge, task = await _connected_bridge(gateway, offering="my-llm")
        try:
            assert gateway.register == {"type": "register", "models": ["my-llm"]}
            async with asyncio.timeout(RECEIVE_TIMEOUT):
                await bridge.attached.wait()
        finally:
            await _stop(task)


async def test_every_connection_re_registers():
    """An open link serves exactly what it last published, so a reconnect
    that skipped registration would serve nothing — the same self-healing
    the HTTP-registration bridge had (AgentSoukServer#16), now structural:
    registration is a frame inside every connection."""
    async with StubGateway() as gateway:
        bridge, task = await _connected_bridge(gateway)
        try:
            assert gateway.register_count == 1
            gateway.connected.clear()
            await gateway._conn.close()
            async with asyncio.timeout(RECEIVE_TIMEOUT + bridge.reconnect_delay):
                await gateway.connected.wait()
            assert gateway.register_count == 2
        finally:
            await _stop(task)


async def test_serving_is_the_whole_lifecycle_in_one_block():
    """Attached and registered before the body runs, torn down on exit."""
    async with StubGateway() as gateway:
        bridge = KyokBridge(
            f"http://127.0.0.1:{gateway.port}", model="m", api_key="k", reconnect_delay=0.05
        )
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            async with bridge.serving():
                # The block only opens attached+registered — no race
                # against the roster for a run started here.
                assert bridge.attached.is_set()
                assert gateway.register is not None
        # And nothing is left serving after the block.
        assert not bridge.attached.is_set()


async def test_serving_withdraws_an_ephemeral_identity_on_exit():
    """An auto-minted key can never come back, so its offering is roster
    garbage the moment the block ends — serving() sends deleteModel over
    the still-open link on the way out. A persisted identity keeps its
    registration, same as an agent provider between connections."""
    async with StubGateway() as gateway:
        ephemeral = KyokBridge(
            f"http://127.0.0.1:{gateway.port}", model="m", api_key="k", reconnect_delay=0.05
        )
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            async with ephemeral.serving():
                pass
        assert gateway.delete_frames == [
            {"type": "deleteModel", "name": ephemeral.offering}
        ]

    async with StubGateway() as gateway:
        persisted = KyokBridge(
            f"http://127.0.0.1:{gateway.port}",
            model="m",
            api_key="k",
            reconnect_delay=0.05,
            identity=ProviderIdentity.generate(),
        )
        async with asyncio.timeout(RECEIVE_TIMEOUT):
            async with persisted.serving():
                pass
        assert gateway.delete_frames == []


async def test_a_refusal_from_the_handler_travels_as_a_structured_error_frame():
    """A handler raising CompletionRefused answers with its payload on
    the error frame, not prose — and the handler saw the whole
    DeliveredCompletion (actorChain included), which is the material its
    policy runs on."""
    seen: dict = {}

    async def refusing_handler(delivered):
        seen["delivered"] = delivered
        raise CompletionRefused({"kind": "throttled", "retryAfter": 30})
        yield  # pragma: no cover - makes this an async generator

    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway, handler=refusing_handler)
        try:
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_1",
                    "runId": "run_9",
                    "providerKey": "ab" * 32,
                    "agentName": "greeter",
                    "llmName": "kyok",
                    "context": {"voucher": "v1"},
                    "actorChain": ["ab" * 32],
                    "body": {"messages": []},
                }
            )
            frame = await gateway.next_frame()
            assert frame["type"] == "error"
            assert frame["refusal"] == {"kind": "throttled", "retryAfter": 30}
            assert seen["delivered"].run_id == "run_9"
            assert seen["delivered"].agent_name == "greeter"
            assert seen["delivered"].context == {"voucher": "v1"}
            assert seen["delivered"].actor_chain == ["ab" * 32]
        finally:
            await _stop(task)


async def test_a_completion_request_streams_back_as_chunks_then_done(monkeypatch):
    captured: dict = {}

    async def fake_acompletion(**kwargs):
        captured.update(kwargs)

        async def gen():
            yield {"choices": [{"delta": {"role": "assistant", "content": "hi"}}]}
            yield {"choices": [{"delta": {"content": " there"}}]}

        return gen()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway, api_base="http://llm.local")
        try:
            body = {
                "messages": [{"role": "user", "content": "hi"}],
                "tools": [{"type": "function"}],
                "temperature": 0.5,
            }
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_1",
                    "runId": "run-1",
                    "providerKey": "ab" * 32,
                    "agentName": "greeter",
                    "body": body,
                }
            )

            frames = [await gateway.next_frame() for _ in range(3)]
            assert [f["type"] for f in frames] == ["chunk", "chunk", "done"]
            assert all(f["requestId"] == "req_1" for f in frames)
            assert frames[0]["data"]["choices"][0]["delta"]["content"] == "hi"

            # The provider's whole request body reached litellm, on this
            # bridge's own key.
            assert captured["model"] == "test-model"
            assert captured["api_key"] == "key"
            assert captured["api_base"] == "http://llm.local"
            assert captured["messages"] == body["messages"]
            assert captured["tools"] == body["tools"]
            assert captured["temperature"] == 0.5
            assert captured["stream"] is True
        finally:
            await _stop(task)


async def test_an_llm_failure_becomes_one_error_frame(monkeypatch):
    async def fake_acompletion(**kwargs):
        raise RuntimeError("upstream boom")

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway)
        try:
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_1",
                    "runId": "run-1",
                    "providerKey": "ab" * 32,
                    "agentName": "greeter",
                    "body": {"messages": []},
                }
            )
            frame = await gateway.next_frame()
            assert frame == {"type": "error", "requestId": "req_1", "message": "upstream boom"}
        finally:
            await _stop(task)


async def test_concurrent_completions_multiplex_on_one_socket(monkeypatch):
    """Two requests in flight at once; their chunks interleave by
    requestId — the property that made the socket strictly better than
    poll_one's one-per-cycle handover."""
    release = asyncio.Event()

    async def fake_acompletion(**kwargs):
        prompt = kwargs["messages"][0]["content"]

        async def gen():
            if prompt == "slow":
                await release.wait()
            yield {"choices": [{"delta": {"content": f"re: {prompt}"}}]}

        return gen()

    monkeypatch.setattr(litellm, "acompletion", fake_acompletion)
    async with StubGateway() as gateway:
        _bridge, task = await _connected_bridge(gateway)
        try:
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_slow",
                    "runId": "run-1",
                    "providerKey": "ab" * 32,
                    "agentName": "greeter",
                    "body": {"messages": [{"role": "user", "content": "slow"}]},
                }
            )
            await gateway.push(
                {
                    "type": "completionRequest",
                    "requestId": "req_fast",
                    "runId": "run-1",
                    "providerKey": "ab" * 32,
                    "agentName": "greeter",
                    "body": {"messages": [{"role": "user", "content": "fast"}]},
                }
            )
            # The fast one answers while the slow one is still held open.
            first = await gateway.next_frame()
            assert first["requestId"] == "req_fast"
            release.set()
            rest = [await gateway.next_frame() for _ in range(3)]
            assert {"req_fast", "req_slow"} == {f["requestId"] for f in [first, *rest]}
        finally:
            await _stop(task)
