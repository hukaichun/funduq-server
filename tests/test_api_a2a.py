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

**The proofs are the rest of this file**, and both moved this round.
Reading a chain-bound run needs a view proof (contract revision 13), and
A2A read requests carry no caller data at all, so it rides the
`X-Funduq-View` header and reaches core through the handler's
`view_metadata_of` hook. Answering a paused one needs a resolve proof
that signs the *asks* rather than the clock (revision 16), which only
works if the door tells a caller what those asks are — so a paused run
carries them under `funduq/outstandingAsks`. Every proof here is signed
with the real payload builders, never a hand-written string: a test that
retyped the bytes could agree with itself while disagreeing with core.
"""

from __future__ import annotations

import json
import time

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from sqlalchemy import func, select

from funduq import repo
from funduq.errors import ThreadQueueFull
from funduq.protocols.a2a import A2AAdapter
from funduq.schema import runs
from funduq_contract import (
    cancel_payload,
    extend_chain,
    new_chain,
    resolve_payload,
    view_payload,
)
from souk_server.api_a2a import OUTSTANDING_ASKS_METADATA_KEY


def _now() -> int:
    """A real clock, because view and cancel proofs still carry one and
    core still enforces a 60-second window on both. Only *resolve* stopped
    signing time this round."""
    return int(time.time())


def _v03_send(
    text: str,
    *,
    context_id: str | None = None,
    task_id: str | None = None,
    metadata: dict | None = None,
) -> dict:
    message: dict = {
        "role": "user",
        "parts": [{"kind": "text", "text": text}],
        "messageId": "m1",
    }
    if context_id is not None:
        message["contextId"] = context_id
    if task_id is not None:
        message["taskId"] = task_id
    params: dict = {"message": message}
    if metadata is not None:
        params["metadata"] = metadata
    return {"jsonrpc": "2.0", "id": "1", "method": "message/send", "params": params}


class _Party:
    """One key on a chain, able to sign what that key is allowed to sign."""

    def __init__(self) -> None:
        self.key = Ed25519PrivateKey.generate()

    @property
    def public_key(self) -> str:
        return self.key.public_key().public_bytes_raw().hex()

    def sign(self, payload: bytes) -> str:
        return self.key.sign(payload).hex()

    def view_header(self, run_id: str, timestamp: int) -> dict[str, str]:
        """The `X-Funduq-View` header this gateway defines: compact JSON,
        the proof upstream's `view_payload` states, nothing else."""
        return {
            "X-Funduq-View": json.dumps(
                {
                    "publicKey": self.public_key,
                    "timestamp": timestamp,
                    "signature": self.sign(view_payload(run_id, timestamp)),
                },
                separators=(",", ":"),
            )
        }

    def resolution(self, run_id: str, ask_ids: list[str]) -> dict[str, str]:
        """A resolve proof: `{publicKey, signature}` and **no timestamp**.
        Revision 16 took the clock out — the signature binds the exact
        asks being answered, so a later pause's new ids are what makes it
        single-purpose, and there is no freshness window left to miss."""
        return {
            "publicKey": self.public_key,
            "signature": self.sign(resolve_payload(run_id, ask_ids)),
        }


async def _bound_paused_run(souk, session, served, *, chain: list[str], head: str, asks: list[str]):
    """A paused run on a thread bound to `chain`.

    Built through repo rather than by sending: a run that really pauses
    needs a provider to pause it, and what these tests are about is the
    door in front of the pause, not the pause itself. Through the legal
    status transitions, because the status machine refuses a jump straight
    from queued to input-required — correctly.
    """
    thread_id = await repo.create_thread(session, served.ref(), head_key=head)
    created = await repo.create_run(
        session, thread_id, served.ref(), "a2a", {}, head_key=head, actor_chain=chain
    )
    await repo.mark_run_status(session, created["run_id"], "running")
    await repo.mark_run_status(
        session,
        created["run_id"],
        "input-required",
        metadata={"interrupts": [{"id": ask} for ask in asks]},
    )
    await session.commit()
    return thread_id, created["run_id"]


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
            "params": {
                "message": {
                    "role": "ROLE_USER",
                    "parts": [{"text": "hi"}],
                    # Required, and upstream's handler is what says
                    # so: `validate_request_params` policed nothing in
                    # the hand-rolled handler this replaced, so a
                    # message with no id used to travel straight into
                    # core. It is now InvalidParams, before any adapter
                    # call — which is what a real v1.0 client already
                    # sends anyway.
                    "messageId": "m1",
                }
            },
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
            "params": {
                "message": {
                    "role": "ROLE_USER",
                    "parts": [{"text": "hi"}],
                    # Required, and upstream's handler is what says
                    # so: `validate_request_params` policed nothing in
                    # the hand-rolled handler this replaced, so a
                    # message with no id used to travel straight into
                    # core. It is now InvalidParams, before any adapter
                    # call — which is what a real v1.0 client already
                    # sends anyway.
                    "messageId": "m1",
                }
            },
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
    `CancelTaskRequest.metadata` (`metadata.cancel`, with `resolution`
    beside it). A2A's cancel carries no message, so request metadata is
    the one place it can be — a gateway that drops the field silently
    refuses every cancel on a bound thread. Captured at the adapter
    boundary, because what matters is exactly what core is handed.

    Two shapes here moved with contract revisions 15 and 16, and the
    absences are the assertion: **no `delegation` key**, because the
    session delegation certificate is gone and a grant is the
    authenticating seat's policy now; and **the resolve proof carries no
    timestamp**, because it signs the ask ids it answers rather than the
    clock. Cancel keeps its timestamp — that family did not move.
    """
    seen: dict = {}

    async def capture(self, agent, task_id, *, metadata=None):
        seen["task_id"] = task_id
        seen["metadata"] = metadata
        return None  # dispatcher answers TaskNotFound; the capture is the point

    monkeypatch.setattr(A2AAdapter, "cancel_task", capture)
    served = await register("greeter")
    authority = {
        "cancel": {"signature": "ab" * 32, "timestamp": 1234, "publicKey": "cd" * 32},
        "resolution": {"publicKey": "cd" * 32, "signature": "12" * 32},
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
    assert "delegation" not in seen["metadata"]
    assert "timestamp" not in seen["metadata"]["resolution"]


# --- the view proof: reading a chain-bound run ------------------------------


async def test_a_bound_run_read_without_a_view_proof_reads_as_absent(
    client, register, session, souk
):
    """Contract revision 13's exposure, from this door's side. A run whose
    thread bound a chain is not public any more: a read carrying no view
    proof is answered *absent*, not refused, because existence is part of
    what is guarded — a 403 would confirm the run to somebody who may not
    see it.

    The same read with a proof from the head succeeds, which is what makes
    the first answer a decision rather than a broken route.
    """
    head = _Party()
    served = await register("approver")
    chain = new_chain(head.key)
    _, run_id = await _bound_paused_run(
        souk, session, served, chain=chain, head=head.public_key, asks=["ask_1"]
    )
    get_task = {"jsonrpc": "2.0", "id": "9", "method": "tasks/get", "params": {"id": run_id}}

    bare = await client.post(f"/a2a/{served.path()}/rpc", json=get_task)

    assert bare.status_code == 200
    assert "result" not in bare.json(), bare.text

    proven = await client.post(
        f"/a2a/{served.path()}/rpc",
        json=get_task,
        headers=head.view_header(run_id, _now()),
    )

    assert proven.json()["result"]["id"] == run_id


async def test_a_malformed_view_header_is_absence_and_never_a_500(
    client, register, session, souk
):
    """Garbage in the header must not become a stack trace. Passing
    nothing is the designed answer: a caller holding a broken proof learns
    exactly what a caller holding none does, which is the whole point of
    answering absence."""
    head = _Party()
    served = await register("approver")
    _, run_id = await _bound_paused_run(
        souk, session, served, chain=new_chain(head.key), head=head.public_key, asks=["ask_1"]
    )

    for header in ("not json at all", "[]", '{"publicKey": "nope"}', ""):
        resp = await client.post(
            f"/a2a/{served.path()}/rpc",
            json={"jsonrpc": "2.0", "id": "9", "method": "tasks/get", "params": {"id": run_id}},
            headers={"X-Funduq-View": header},
        )
        assert resp.status_code == 200, (header, resp.text)
        assert "result" not in resp.json(), (header, resp.text)


async def test_a_mid_chain_hop_may_view_the_run_it_may_not_cancel(
    client, register, session, souk
):
    """The read circle is wider than the act circle, and one key proves
    both halves at once. `mid` is on the chain but is not its head — the
    responsibility flowed through it, so it may *look*; cancelling stays
    with the head and the serving provider, so the same key signing the
    same run is refused there.

    Two different payload families do this, which is why it is one test:
    if the gateway ever handed the view proof to the cancel door or the
    other way round, exactly one of these assertions would flip.
    """
    head, mid = _Party(), _Party()
    served = await register("approver")
    chain = extend_chain(mid.key, new_chain(head.key))
    _, run_id = await _bound_paused_run(
        souk, session, served, chain=chain, head=head.public_key, asks=["ask_1"]
    )
    now = _now()

    seen = await client.post(
        f"/a2a/{served.path()}/rpc",
        json={"jsonrpc": "2.0", "id": "9", "method": "tasks/get", "params": {"id": run_id}},
        headers=mid.view_header(run_id, now),
    )
    assert seen.json()["result"]["id"] == run_id, seen.text

    refused = await client.post(
        f"/a2a/{served.path()}/rpc",
        json={
            "jsonrpc": "2.0",
            "id": "10",
            "method": "tasks/cancel",
            "params": {
                "id": run_id,
                "metadata": {
                    "cancel": {
                        "publicKey": mid.public_key,
                        "timestamp": now,
                        "signature": mid.sign(cancel_payload(run_id, now)),
                    }
                },
            },
        },
    )
    assert "result" not in refused.json(), refused.text


# --- the resolve proof: answering a paused run ------------------------------


async def test_a_paused_run_says_what_it_is_waiting_on(client, register, session, souk):
    """The one genuinely new capability this round. A resolve proof signs
    the asks it answers, so a caller that cannot enumerate them has no
    proof to build — the pause would be unanswerable by anyone who was not
    already watching the stream that announced it. A2A has no field for
    them, so this door puts them on the Task's metadata."""
    head = _Party()
    served = await register("approver")
    _, run_id = await _bound_paused_run(
        souk,
        session,
        served,
        chain=new_chain(head.key),
        head=head.public_key,
        asks=["ask_1", "ask_2"],
    )

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        json={"jsonrpc": "2.0", "id": "9", "method": "tasks/get", "params": {"id": run_id}},
        headers=head.view_header(run_id, _now()),
    )

    task = resp.json()["result"]
    # Sorted, the order `resolve_payload` canonicalizes in.
    assert task["metadata"][OUTSTANDING_ASKS_METADATA_KEY] == ["ask_1", "ask_2"]


async def test_a_resolution_signing_the_right_asks_is_accepted(
    client, register, session, souk
):
    """Signed over exactly what the pause said it was waiting on, by the
    chain's head — and the run reopens. This is the road the ask ids above
    exist to make walkable, driven end to end."""
    head = _Party()
    served = await register("approver")
    chain = new_chain(head.key)
    thread_id, run_id = await _bound_paused_run(
        souk, session, served, chain=chain, head=head.public_key, asks=["ask_1", "ask_2"]
    )

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        json=_v03_send(
            "approved",
            context_id=thread_id,
            task_id=run_id,
            # The chain rides along because writing to a bound thread is
            # itself scoped to its head — the resolve proof answers the
            # *ask*, it does not grant membership of the conversation.
            metadata={
                "actorChain": chain,
                "resolution": head.resolution(run_id, ["ask_1", "ask_2"]),
            },
        ),
    )

    assert resp.status_code == 200, resp.text
    assert "error" not in resp.json(), resp.text
    # The same run continuing, not a new one queued behind it.
    assert resp.json()["result"]["id"] == run_id
    reopened = await repo.get_run(session, run_id)
    assert reopened.status != "input-required"


@pytest.mark.parametrize(
    "asks",
    [
        pytest.param(["ask_1"], id="a-subset-of-the-open-asks"),
        pytest.param(["ask_1", "ask_2", "ask_3"], id="one-ask-too-many"),
        pytest.param(["ask_9"], id="asks-from-some-other-pause"),
    ],
)
async def test_a_resolution_signing_the_wrong_asks_is_refused(
    client, register, session, souk, asks
):
    """The instance binding, from the outside. A proof is for one pause
    and one set of asks: signing a stale set, a partial set, or somebody
    else's is refused, and the run stays paused. There is no timestamp to
    get wrong here — this is what replaced the freshness window, and it is
    stricter, because a later pause has ids no old signature ever saw."""
    head = _Party()
    served = await register("approver")
    chain = new_chain(head.key)
    thread_id, run_id = await _bound_paused_run(
        souk, session, served, chain=chain, head=head.public_key, asks=["ask_1", "ask_2"]
    )

    resp = await client.post(
        f"/a2a/{served.path()}/rpc",
        json=_v03_send(
            "approved",
            context_id=thread_id,
            task_id=run_id,
            # A caller that is otherwise entirely in order: the right
            # chain, the right head, the right thread. The only thing
            # wrong is which asks the signature covers, so that is the
            # only thing this can be failing on.
            metadata={
                "actorChain": chain,
                "resolution": head.resolution(run_id, asks),
            },
        ),
    )

    assert "result" not in resp.json(), resp.text
    still_paused = await repo.get_run(session, run_id)
    assert still_paused.status == "input-required"
