"""SoukClient's pause/resolve surface, and the completion shape the KYOK
bridge validates into.

Both are contract-revision moves rather than refactors. Revision 11 made
`DeliveredCompletion` (from `funduq_contract`) *be* the wire shape —
`funduq.kyok.CompletionRequest` and `DeliveredCompletion.from_request` are
gone, and there is nothing left to rebuild. Revision 16 made a resolve
proof sign the paused run's outstanding asks instead of a timestamp, which
is why this client has to surface those ask ids at all: without them a
caller cannot construct the proof, and a run bound to an actor chain can
never be answered.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from funduq_contract import DeliveredCompletion, resolve_payload
from funduq_provider_sdk import ProviderIdentity, verify_signature

from souk_client_sdk import SoukClient, resolution_proof

UPSTREAM_VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "upstream-contract-vectors.json").read_text()
)


def _vector(kind: str) -> dict:
    return next(v for v in UPSTREAM_VECTORS["vectors"] if v["kind"] == kind)


def _test_identity() -> ProviderIdentity:
    """Upstream's published test key, so the signature below is comparable
    to the vector byte for byte."""
    return ProviderIdentity(
        Ed25519PrivateKey.from_private_bytes(
            bytes.fromhex(UPSTREAM_VECTORS["test_key"]["private_key_hex"])
        )
    )


# --- the resolve proof -------------------------------------------------------


def test_a_resolve_proof_replays_upstreams_published_vector():
    """The bytes are `funduq-resolve:{run_id}:{sha256 of the ask ids,
    sorted and NUL-joined}` — replayed against upstream's own vector so
    the sorting, the separator and the hash are checked as bytes, not as
    a description of them.

    The wire proof is `{publicKey, signature}`: no timestamp, and no 60s
    freshness window, because binding the *instance* replaces the clock —
    a later pause has new ask ids, so this signature cannot answer it."""
    vector = _vector("resolution")
    identity = _test_identity()
    proof = resolution_proof(
        identity, vector["inputs"]["run_id"], vector["inputs"]["ask_ids"]
    )
    assert proof == {
        "publicKey": identity.public_key,
        "signature": vector["signature_hex"],
    }
    assert set(proof) == {"publicKey", "signature"}
    assert verify_signature(
        proof["publicKey"],
        proof["signature"],
        resolve_payload(vector["inputs"]["run_id"], vector["inputs"]["ask_ids"]),
    )


def test_the_ask_ids_are_a_set_the_caller_need_not_order():
    """Canonicalization lives in the payload builder and nowhere else, so
    the same asks in any order produce the same proof — and one ask id
    passed as a bare string is refused rather than silently hashed
    character by character."""
    identity = _test_identity()
    run_id = "run_1"
    assert resolution_proof(identity, run_id, ["b", "a"]) == resolution_proof(
        identity, run_id, ["a", "b"]
    )
    with pytest.raises(TypeError):
        resolution_proof(identity, run_id, "just-one-id")  # type: ignore[arg-type]


def test_a_proof_for_other_asks_is_not_the_proof_for_these():
    """A subset does not verify: the signature answers the run's
    outstanding asks *exactly*."""
    identity = _test_identity()
    whole = resolution_proof(identity, "run_1", ["a", "b"])
    assert not verify_signature(
        whole["publicKey"], whole["signature"], resolve_payload("run_1", ["a"])
    )


async def test_resolution_rides_the_runs_metadata(monkeypatch):
    """Where souk reads it: `metadata.resolution` on the resuming
    request, alongside whatever else the caller was already sending."""
    client = SoukClient("http://souk.example")
    seen = _stub_stream(monkeypatch, [])
    async for _ in client.run(
        _agent(),
        "hi",
        thread_id="t1",
        metadata={"kyok": {"llmProvider": {}}},
        resolution={"publicKey": "ab", "signature": "cd"},
    ):
        pass
    assert seen["body"]["metadata"]["resolution"] == {
        "publicKey": "ab",
        "signature": "cd",
    }
    assert seen["body"]["metadata"]["kyok"] == {"llmProvider": {}}


def _agent():
    from souk_client_sdk import Agent

    return Agent(provider="ab" * 8, name="echo", provider_key="ab" * 32)


def _stub_stream(monkeypatch, events: list[dict]) -> dict:
    """Points this client's httpx at a transport that answers one SSE
    stream of `events`, and records the request body it was given."""
    import httpx

    seen: dict = {}
    payload = "".join(
        f"event: message\ndata: {json.dumps(event)}\n\n" for event in events
    ).encode()

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            seen["body"] = json.loads(request.content)
            seen["url"] = str(request.url)
            return httpx.Response(
                200, headers={"content-type": "text/event-stream"}, content=payload
            )

    real = httpx.AsyncClient

    def _client(*args, **kwargs):
        kwargs["transport"] = _Transport()
        return real(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _client)
    return seen


# --- the asks a paused run is waiting on ------------------------------------


async def test_the_outstanding_asks_of_a_paused_run_are_surfaced_to_the_caller(monkeypatch):
    """A pause the caller cannot answer is a dead end. The ids live in
    core's one ask id space (funduq.pause: unanswered tool calls, plus the
    interrupts the RUN_FINISHED outcome names), and they are exactly what
    `resolution_proof` must sign."""
    client = SoukClient("http://souk.example")
    _stub_stream(
        monkeypatch,
        [
            {"type": "RUN_STARTED", "threadId": "t1", "runId": "r1"},
            {"type": "TOOL_CALL_START", "toolCallId": "tool_b"},
            {"type": "TOOL_CALL_START", "toolCallId": "tool_answered"},
            {"type": "TOOL_CALL_RESULT", "toolCallId": "tool_answered"},
            {
                "type": "RUN_FINISHED",
                "threadId": "t1",
                "runId": "r1",
                "outcome": {
                    "type": "interrupt",
                    "interrupts": [
                        {"id": "int_1"},
                        {"id": "i2", "toolCallId": "tool_b"},
                    ],
                },
            },
        ],
    )
    async for _ in client.run(_agent(), "hi", thread_id="t1"):
        pass

    assert client.last_run_id == "r1"
    assert sorted(client.last_outstanding_asks) == ["int_1", "tool_b"]
    # An answered tool call is not outstanding, and an interrupt naming a
    # tool call already seen is the same ask, not a second one.
    assert "tool_answered" not in client.last_outstanding_asks
    assert len(client.last_outstanding_asks) == 2

    # Which is the whole point: the proof that answers this pause.
    identity = _test_identity()
    proof = resolution_proof(
        identity, client.last_run_id, client.last_outstanding_asks
    )
    assert verify_signature(
        proof["publicKey"],
        proof["signature"],
        resolve_payload("r1", {"int_1", "tool_b"}),
    )


async def test_a_run_that_ends_without_pausing_leaves_no_asks(monkeypatch):
    client = SoukClient("http://souk.example")
    _stub_stream(
        monkeypatch,
        [
            {"type": "RUN_STARTED", "threadId": "t1", "runId": "r1"},
            {"type": "TOOL_CALL_START", "toolCallId": "tool_a"},
            {"type": "TOOL_CALL_RESULT", "toolCallId": "tool_a"},
            {"type": "RUN_FINISHED", "threadId": "t1", "runId": "r1"},
        ],
    )
    async for _ in client.run(_agent(), "hi", thread_id="t1"):
        pass
    assert client.last_outstanding_asks == []


# --- the completion shape ----------------------------------------------------


def test_a_completion_request_is_the_published_envelope():
    """Upstream's `delivered-completion` wire vector, validated straight
    into the model the bridge hands its handler — no `from_request`, no
    field mapping, and `body` is OpenAI's own request shape."""
    frame = next(v for v in UPSTREAM_VECTORS["wire"] if v["kind"] == "delivered-completion")["frame"]
    delivered = DeliveredCompletion.model_validate(frame)
    assert delivered.run_id == frame["runId"]
    assert delivered.provider_key == frame["providerKey"]
    assert delivered.body["model"] == "gpt-4"
    assert delivered.model_dump(by_alias=True) == frame


def test_a_completion_body_carries_extension_keys_verbatim():
    """`extra="allow"` on the body alone: clients merge `extra_body` at the
    top level, and a relay that dropped what it did not recognise would
    silently change what the caller asked the model for."""
    delivered = DeliveredCompletion.model_validate(
        {
            "runId": "r1",
            "providerKey": "ab" * 32,
            "agentName": "echo",
            "body": {
                "model": "gpt-4",
                "messages": [{"role": "user", "content": "hi"}],
                "some_vendor_knob": {"depth": 2},
            },
        }
    )
    assert delivered.body["some_vendor_knob"] == {"depth": 2}


def test_the_envelope_itself_forbids_unknown_fields():
    """Which is why the bridge strips `type`/`requestId` — its own
    transport vocabulary — before validating."""
    with pytest.raises(Exception):
        DeliveredCompletion.model_validate(
            {
                "type": "completionRequest",
                "requestId": "req_1",
                "runId": "r1",
                "providerKey": "ab" * 32,
                "agentName": "echo",
                "body": {"model": "gpt-4", "messages": []},
            }
        )
