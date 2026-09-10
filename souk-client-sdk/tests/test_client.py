"""SoukClient's pause/resolve surface, and the completion shape the KYOK
bridge validates into.

Both are contract-revision moves rather than refactors. Revision 11 made
`DeliveredCompletion` (from `funduq_contract`) *be* the wire shape —
`funduq.kyok.CompletionRequest` and `DeliveredCompletion.from_request` are
gone, and there is nothing left to rebuild. Revision 16 made a resolve
proof sign the paused run's outstanding asks instead of a timestamp, which
is why this client has to surface those ask ids at all: without them a
caller cannot construct the proof, and a run bound to an actor chain can
never be answered. Revision 19 then moved the *id* that proof signs from
the paused run to its lineage root — the A2A task id — which coincide on a
first pause and diverge on every one after, so `last_task_id` is surfaced
beside the asks and the two-pause test below is the one that can tell them
apart. Revision 20 moved the caller's whole bag to request level
(`forwardedProps`), where the tests here assert it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from funduq_contract import DeliveredCompletion, resolve_payload
from funduq_provider_sdk import ProviderIdentity, verify_signature

from souk_client_sdk import SoukClient, resolution_proof

_DOCS = Path(__file__).parent.parent.parent / "docs"
UPSTREAM_VECTORS = json.loads((_DOCS / "upstream-contract-vectors.json").read_text())
# Envelopes live in this repo's own vectors since contract revision 23 —
# upstream publishes only what a signature covers, and nothing signs an
# envelope (funduq#282).
WIRE_VECTORS = json.loads((_DOCS / "wire-vectors.json").read_text())


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


async def test_the_callers_declarations_ride_forwarded_props(monkeypatch):
    """Where souk reads them. Since contract revision 20 a caller's
    declarations to funduq — the KYOK opt-in and the resolution proof
    alike — are **request level**: `forwardedProps` on the AG-UI door, the
    request's `metadata` on A2A. funduq reads nothing from a message's own
    metadata, and the run row has no metadata column at all any more.

    This was a *silent* failure, which is why it gets its own test: sent
    in `body["metadata"]` the opt-in minted no grant, no exception was
    raised anywhere, and the agent simply answered with no model."""
    client = SoukClient("http://souk.example")
    seen = _stub_stream(monkeypatch, [])
    async for _ in client.run(
        _agent(),
        "hi",
        thread_id="t1",
        forwarded_props={"kyok": {"llmProvider": {}}},
        resolution={"publicKey": "ab", "signature": "cd"},
    ):
        pass
    assert seen["body"]["forwardedProps"]["resolution"] == {
        "publicKey": "ab",
        "signature": "cd",
    }
    assert seen["body"]["forwardedProps"]["kyok"] == {"llmProvider": {}}
    # And nowhere else: a second copy in the old place would let a stale
    # gateway keep passing while a current one silently ignored it.
    assert "metadata" not in seen["body"]


async def test_the_bag_merges_rather_than_replacing_the_callers_own(monkeypatch):
    """The old code *assigned* `forwardedProps` for one declaration, so a
    caller sending a KYOK opt-in and an interjection in the same call lost
    one of them without a word. Everything the caller sent survives beside
    what the SDK adds."""
    client = SoukClient("http://souk.example")
    seen = _stub_stream(monkeypatch, [])
    async for _ in client.run(
        _agent(),
        "hi",
        thread_id="t1",
        forwarded_props={"kyok": {"llmProvider": {}}, "mine": {"deep": [1, 2]}},
        resolution={"publicKey": "ab", "signature": "cd"},
    ):
        pass
    bag = seen["body"]["forwardedProps"]
    assert set(bag) == {"kyok", "mine", "resolution"}
    assert bag["mine"] == {"deep": [1, 2]}


async def test_metadata_is_the_old_name_for_the_same_bag(monkeypatch):
    """`metadata=` used to name this bag when it landed in
    `body["metadata"]`. Kept as an alias — `KyokBridge.run_metadata()`
    builds into it — and merged into the one bag rather than sent to the
    place nothing reads."""
    client = SoukClient("http://souk.example")
    seen = _stub_stream(monkeypatch, [])
    async for _ in client.run(
        _agent(),
        "hi",
        thread_id="t1",
        metadata={"kyok": {"llmProvider": {}}},
        forwarded_props={"mine": 1},
    ):
        pass
    assert seen["body"]["forwardedProps"] == {"kyok": {"llmProvider": {}}, "mine": 1}
    assert "metadata" not in seen["body"]


def test_there_is_no_agui_interjection_argument():
    """Removed rather than faked. Core reads an interjection declaration
    on the **A2A door only**; the AG-UI door never sets one, and the
    `forwardedProps` key this argument wrote to (`addressedRunId`) now
    belongs to funduq under `forwardedProps.funduq`. Keeping the parameter
    would promise a delivery no door makes."""
    import inspect

    assert "addressed_run_id" not in inspect.signature(SoukClient.run).parameters
    # And the docstring says where interjection *does* work, rather than
    # leaving a caller to find out by silence.
    assert "A2A door only" in (SoukClient.run.__doc__ or "")


def _agent():
    from souk_client_sdk import Agent

    return Agent(provider="ab" * 8, name="echo", provider_key="ab" * 32)


_REAL_ASYNC_CLIENT = __import__("httpx").AsyncClient


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

    # The *unpatched* class, captured once at import. Reading
    # `httpx.AsyncClient` here would capture a previous stub when a test
    # stubs twice — and the second stream would then quietly replay the
    # first one's events, which is a green test asserting nothing.
    real = _REAL_ASYNC_CLIENT

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

    # Which is the whole point: the proof that answers this pause. It
    # signs the *task* id — the lineage root — which on a first turn is
    # also the paused run's id. See the two-pause test below for why that
    # coincidence makes this one prove nothing about the id.
    assert client.last_task_id == "r1"
    identity = _test_identity()
    proof = resolution_proof(
        identity, client.last_task_id, client.last_outstanding_asks
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


async def test_a_finished_run_with_an_unanswered_tool_call_is_waiting_too(monkeypatch):
    """`funduq.pause.open_asks` is the single definition of the ask id
    space now, and it says: everything a *finished* run left waiting on —
    its interrupts **and** its unanswered tool calls — whether or not it
    finished on an interrupt outcome. Tracking only the interrupt outcome
    reported "nothing outstanding" for a run core considers open, and the
    caller could not build a proof it needed."""
    client = SoukClient("http://souk.example")
    _stub_stream(
        monkeypatch,
        [
            {"type": "RUN_STARTED", "threadId": "t1", "runId": "r1"},
            {"type": "TOOL_CALL_START", "toolCallId": "tool_a"},
            {"type": "RUN_FINISHED", "threadId": "t1", "runId": "r1"},
        ],
    )
    async for _ in client.run(_agent(), "hi", thread_id="t1"):
        pass
    assert client.last_outstanding_asks == ["tool_a"]


async def test_a_second_pause_signs_the_task_id_not_the_run_that_asked(monkeypatch):
    """The reason a single-pause test proves nothing. Revision 19 made
    answering a pause open the *next* run rather than reopen the paused
    one, and core verifies a resolution over `repo.root_of(the run that
    asked)` — the lineage root, the id A2A calls the task id. On the first
    pause root and run coincide; on the second they do not, and a proof
    signed over `last_run_id` fails to verify against the bytes core
    builds."""
    client = SoukClient("http://souk.example")

    def _pause(run_id: str, ask: str) -> list[dict]:
        return [
            {"type": "RUN_STARTED", "threadId": "t1", "runId": run_id},
            {
                "type": "RUN_FINISHED",
                "threadId": "t1",
                "runId": run_id,
                "outcome": {"type": "interrupt", "interrupts": [{"id": ask}]},
            },
        ]

    _stub_stream(monkeypatch, _pause("r1", "int_1"))
    async for _ in client.run(_agent(), "hi", thread_id="t1"):
        pass
    assert client.last_task_id == client.last_run_id == "r1"

    # The answer opens r2, a child of r1 — same lineage, same task id.
    _stub_stream(monkeypatch, _pause("r2", "int_2"))
    async for _ in client.run(
        _agent(),
        "answer",
        thread_id="t1",
        resume=[{"interruptId": "int_1", "status": "resolved"}],
    ):
        pass
    assert client.last_run_id == "r2"
    assert client.last_task_id == "r1"

    identity = _test_identity()
    proof = resolution_proof(identity, client.last_task_id, client.last_outstanding_asks)
    assert verify_signature(
        proof["publicKey"], proof["signature"], resolve_payload("r1", {"int_2"})
    )
    # Signed over the run that asked, it answers nothing core will build.
    assert not verify_signature(
        proof["publicKey"], proof["signature"], resolve_payload("r2", {"int_2"})
    )


async def test_a_fresh_turn_starts_a_new_task(monkeypatch):
    """The other half of the rule: a run that answers nothing starts its
    own lineage, so the task id follows it rather than staying pinned to a
    conversation that already settled."""
    client = SoukClient("http://souk.example")
    _stub_stream(
        monkeypatch,
        [
            {"type": "RUN_STARTED", "threadId": "t1", "runId": "r1"},
            {"type": "RUN_FINISHED", "threadId": "t1", "runId": "r1"},
        ],
    )
    async for _ in client.run(_agent(), "hi", thread_id="t1"):
        pass
    assert client.last_task_id == "r1"

    _stub_stream(
        monkeypatch,
        [
            {"type": "RUN_STARTED", "threadId": "t1", "runId": "r2"},
            {"type": "RUN_FINISHED", "threadId": "t1", "runId": "r2"},
        ],
    )
    async for _ in client.run(_agent(), "again", thread_id="t1"):
        pass
    assert client.last_task_id == "r2"


# --- the completion shape ----------------------------------------------------


def test_a_completion_request_is_the_published_envelope():
    """The `delivered-completion` envelope, validated straight into the
    model the bridge hands its handler — no `from_request`, no field
    mapping, and `body` is OpenAI's own request shape. This repo's vector
    since contract revision 23, byte-identical to upstream's last."""
    frame = WIRE_VECTORS["envelopes"]["delivered_completion"]["frame"]
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
