"""The A2A HTTP surface: pair routing, the version-negotiating
dispatcher, and the two errors that escape as HTTP.

The JSON-RPC layer is a2a-sdk's now, and version negotiation is the
reason it has to be: which protocol a request speaks rides the
`A2A-Version` header (absent means 0.3), and only the transport ever
sees a header. Both vocabularies are exercised here — `message/send`
headerless the way an unmodified v0.3 client sends it, and v1.0
`SendMessage` with the header the way this repo's own a2a_client speaks.

The two escapes are the writing-a-transport rules: `AgentNotFound` is a
404 on the route (the agent is the endpoint, resolved before the
dispatcher runs — never a JSON-RPC error inside a 200), and
`ThreadQueueFull` is a 429 that says retry, because the request was NOT
accepted. And `CancelTaskRequest.metadata` must pass through whole —
the cancel-authority proof rides in it, and a gateway that drops the
field silently refuses every cancel on a bound thread.
"""

from __future__ import annotations

from sqlalchemy import func, select

from funduq import repo
from funduq.errors import ThreadQueueFull
from funduq.protocols.a2a import A2AAdapter
from funduq.schema import runs


def _v03_send(text: str, *, context_id: str | None = None, metadata: dict | None = None) -> dict:
    message: dict = {
        "role": "user",
        "parts": [{"kind": "text", "text": text}],
        "messageId": "m1",
    }
    if context_id is not None:
        message["contextId"] = context_id
    params: dict = {"message": message}
    if metadata is not None:
        params["metadata"] = metadata
    return {"jsonrpc": "2.0", "id": "1", "method": "message/send", "params": params}


async def test_the_card_is_served_by_pair_and_says_where_the_rpc_is(client, register):
    served = await register("greeter", description="hi")

    resp = await client.get(f"/a2a/{served.path()}/.well-known/agent-card.json")

    assert resp.status_code == 200, resp.text
    # v1.0 replaced the card's single `url` with a list of interfaces, each
    # stating its own binding and protocol version. Core no longer fills it
    # in — the gateway does, because only it knows where it serves.
    interface = resp.json()["supportedInterfaces"][0]
    assert interface["url"].endswith(f"/a2a/{served.path()}/rpc")
    assert interface["protocolBinding"] == "JSONRPC"


async def test_the_full_public_key_addresses_the_same_agent_as_its_fingerprint(client, register):
    """Core tells the two apart by length, so both work. The fingerprint is
    what this gateway puts in a URL; the key is what a caller holding the
    real thing already has, and making it re-derive a short form to be
    understood would be a gateway inventing an identity funduq does not use."""
    served = await register("greeter", description="hi")

    by_fingerprint = await client.get(
        f"/a2a/{served.fingerprint}/greeter/.well-known/agent-card.json"
    )
    by_key = await client.get(
        f"/a2a/{served.public_key}/greeter/.well-known/agent-card.json"
    )

    assert by_fingerprint.status_code == by_key.status_code == 200
    assert by_fingerprint.json() == by_key.json()


async def test_two_providers_may_offer_the_same_name_and_neither_shadows_the_other(
    client, register
):
    """The collision is real and allowed; it is simply not addressable by
    name alone, so there is no winner to pick and no disambiguation to
    describe."""
    a = await register("greeter", description="from a")
    b = await register("greeter", description="from b")

    card_a = await client.get(f"/a2a/{a.path()}/.well-known/agent-card.json")
    card_b = await client.get(f"/a2a/{b.path()}/.well-known/agent-card.json")

    assert card_a.status_code == card_b.status_code == 200
    assert card_a.json()["description"] == "from a"
    assert card_b.json()["description"] == "from b"


async def test_the_pre_v1_card_path_is_not_served(client, register):
    """Only the current path. The old one was served for a while, answering
    with the *new* body — which has no top-level `url`, so a pre-v1 client
    found a card it could not use to locate the RPC endpoint. That is not an
    accommodation, and whether to offer a real one is a gateway decision."""
    served = await register("greeter", description="hi")

    resp = await client.get(f"/a2a/{served.path()}/.well-known/agent.json")

    assert resp.status_code == 404


async def test_an_unknown_agent_is_a_404_on_the_route_not_an_rpc_error_in_a_200(
    client, register
):
    """The writing-a-transport rule: the agent is the *endpoint*, resolved
    from the route before the dispatcher runs, so an unknown one means the
    address does not exist. A bare name is one such unknown — the by-name
    route is gone, not relaxed."""
    await register("greeter")

    assert (await client.get("/a2a/greeter/.well-known/agent-card.json")).status_code == 404
    assert (await client.post("/a2a/greeter/rpc", json=_v03_send("hi"))).status_code == 404
    assert (
        await client.post("/a2a/0" * 8 + "/greeter/rpc", json=_v03_send("hi"))
    ).status_code == 404


async def test_a_v03_client_gets_v03_shapes_without_any_header(client, serve):
    """An unmodified v0.3 client: `message/send`, no `A2A-Version` header.
    The compat adapter answers in v0.3's own shapes — lowercase state,
    `kind: task` — which is what such a client can parse."""
    served = await serve(None, "greeter")

    resp = await client.post(f"/a2a/{served.path()}/rpc", json=_v03_send("hi"))

    assert resp.status_code == 200, resp.text
    result = resp.json()["result"]
    assert result["kind"] == "task"
    assert result["status"]["state"] == "completed"


async def test_a_v10_client_speaks_the_native_vocabulary_with_the_header(client, serve):
    """This repo's own a2a_client path: `SendMessage`, `A2A-Version: 1.0`,
    proto-JSON shapes in both directions."""
    served = await serve(None, "greeter")

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        headers={"A2A-Version": "1.0"},
        json={
            "jsonrpc": "2.0",
            "id": "2",
            "method": "SendMessage",
            "params": {"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
        },
    )

    assert resp.status_code == 200, resp.text
    task = resp.json()["result"]["task"]
    assert task["status"]["state"] == "TASK_STATE_COMPLETED"


async def test_offline_target_fails_fast_instead_of_queueing(client, register, session):
    """Registered but unattached: nobody is serving it, so the run must end
    rather than wait — `online` is `is_serving`, so this needs no clock
    manipulation."""
    served = await register("translator")

    thread_id = await repo.create_thread(session, served.ref())
    await session.commit()

    resp = await client.post(
        f"/a2a/{served.path()}/rpc", json=_v03_send("hi", context_id=thread_id)
    )
    assert resp.status_code == 200, resp.text
    result = resp.json()["result"]
    assert result["status"]["state"] == "failed"

    run = (
        await session.execute(
            select(runs.c.status, runs.c.metadata).where(runs.c.run_id == result["id"])
        )
    ).mappings().first()
    assert run["status"] == "failed"
    assert run["metadata"]["failureReason"] == "agent_offline"


async def test_a2a_can_never_bypass_a_paused_run_even_with_a_resume_flag(
    client, register, session
):
    """An unaddressed second send on the same context, even one that
    tries the old metadata.resume=true convention, must not resolve an
    active, paused run's interrupt: only a message whose `taskId` names
    the paused task resumes it (funduq's conversation-queue rule). The
    stray send becomes its own run, queued behind the conversation — and
    the pause stands."""
    served = await register("approver")

    # Built directly via repo, not through a live send — that would block
    # draining a run nothing ever claims/finishes.
    thread_id = await repo.create_thread(session, served.ref())
    created = await repo.create_run(session, thread_id, served.ref(), "a2a", {})
    # Through the legal transitions — the status machine refuses a jump
    # straight from queued to paused, correctly.
    await repo.mark_run_status(session, created["run_id"], "running")
    await repo.mark_run_status(
        session, created["run_id"], "input-required", metadata={"interrupts": [{"id": "int_1"}]}
    )
    await session.commit()

    second = await client.post(
        f"/a2a/{served.path()}/rpc",
        json=_v03_send("approved", context_id=thread_id, metadata={"resume": True}),
    )
    assert second.status_code == 200, second.text
    result = second.json()["result"]
    # Its own run, not a second life for the paused one…
    assert result["id"] != created["run_id"]

    # …and the interrupt was not resolved by it: the paused run stands.
    paused = await repo.get_run(session, created["run_id"])
    assert paused.status == "input-required"
    assert (
        await session.execute(select(func.count()).select_from(runs))
    ).scalar() == 2


async def test_thread_queue_full_escapes_as_a_429_that_says_retry(
    client, register, monkeypatch
):
    """Backpressure leaves A2A's vocabulary on purpose: the request was
    NOT accepted, and a JSON-RPC error inside a 200 (which is where the
    dispatcher's own exception handling would land it) reads as anything
    but. The handler raises it as the one exception type the dispatcher
    re-raises, and this drives that whole path over HTTP."""

    async def full(self, agent, message, **kwargs):
        raise ThreadQueueFull("thread queue is full")

    monkeypatch.setattr(A2AAdapter, "send_task", full)
    served = await register("greeter")

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        headers={"A2A-Version": "1.0"},
        json={
            "jsonrpc": "2.0",
            "id": "3",
            "method": "SendMessage",
            "params": {"message": {"role": "ROLE_USER", "parts": [{"text": "hi"}]}},
        },
    )

    assert resp.status_code == 429
    assert "retry" in resp.json()["detail"]
    assert resp.headers.get("retry-after") is not None


async def test_cancel_passes_the_request_metadata_through_whole(
    client, register, monkeypatch
):
    """A run on a thread that bound an authority at birth can only be
    stopped by one of that thread's authorities, and the proof rides in
    `CancelTaskRequest.metadata` (`metadata.cancel`, with `resolution` /
    `delegation` beside it). A2A's cancel carries no message, so request
    metadata is the one place it can be — a gateway that drops the field
    silently refuses every cancel on a bound thread. Captured at the
    adapter boundary, because what matters is exactly what core is
    handed."""
    seen: dict = {}

    async def capture(self, agent, task_id, *, metadata=None):
        seen["task_id"] = task_id
        seen["metadata"] = metadata
        return None  # dispatcher answers TaskNotFound; the capture is the point

    monkeypatch.setattr(A2AAdapter, "cancel_task", capture)
    served = await register("greeter")
    authority = {
        "cancel": {"signature": "ab" * 32, "timestamp": 1234, "publicKey": "cd" * 32},
        "resolution": {"note": "operator"},
        "delegation": {"delegatePublicKey": "ef" * 32},
    }

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        json={
            "jsonrpc": "2.0",
            "id": "4",
            "method": "tasks/cancel",
            "params": {"id": "run_bound", "metadata": authority},
        },
    )

    assert resp.status_code == 200
    assert seen["task_id"] == "run_bound"
    assert seen["metadata"] == authority
