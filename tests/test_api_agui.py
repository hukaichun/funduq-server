"""Covers the AG-UI HTTP surface: the optional POST /threads endpoint,
that /agui/... runs mint a fresh thread automatically for an
unrecognized threadId rather than requiring POST /threads first
(souk-no-forced-protocol-deviation) — real ag_ui.core.RunAgentInput
shape and all — and the actor-chain door behaviour: a valid chain is
verified, its head copied onto the run, and the chain relayed to the
agent verbatim plus funduq's own dispatch hop; a tampered one is a 401
at the door.

Every route takes the pair. "Offline" is arranged by simply not
attaching anyone — `online` is `is_serving`, so an agent nobody attached
is already offline.
"""

from __future__ import annotations

import json

from funduq import repo
from funduq_contract import verify_chain


def _run_input(thread_id: str, message: str = "hi") -> dict:
    """The real ag_ui.core.RunAgentInput wire shape — threadId/runId/
    state/messages/tools/context/forwardedProps all required by the real
    schema. runId is required by the schema but never adopted by funduq —
    any placeholder satisfies it.
    """
    return {
        "threadId": thread_id,
        "runId": "ignored",
        "state": None,
        "messages": [{"id": "whatever", "role": "user", "content": message}],
        "tools": [],
        "context": [],
        "forwardedProps": None,
    }


def _run_started(sse_body: str) -> dict:
    """The stream's own RUN_STARTED event — the standard, in-band place a
    client learns the resolved threadId and runId (no custom X-Souk-*
    headers — see souk-no-forced-protocol-deviation).
    """
    for line in sse_body.splitlines():
        if line.startswith("data: "):
            event = json.loads(line[len("data: ") :])
            if event.get("type") == "RUN_STARTED":
                return event
    raise AssertionError(f"no RUN_STARTED event found in: {sse_body!r}")


async def test_create_thread_by_pair_returns_a_real_thread_id(client, register):
    served = await register("greeter")

    first = await client.post(f"/threads/{served.path()}")
    assert first.status_code == 200, first.text
    assert first.json()["thread_id"].startswith("thread_")

    second = await client.post(f"/threads/{served.path()}")
    assert second.json()["thread_id"] != first.json()["thread_id"]


async def test_create_thread_for_an_unregistered_agent_404s(client, register):
    served = await register("greeter")

    # A real provider, a name it never registered.
    assert (await client.post(f"/threads/{served.fingerprint}/nobody")).status_code == 404
    # A real name, a provider that does not exist.
    assert (await client.post(f"/threads/{'0' * 16}/greeter")).status_code == 404


async def test_agui_run_mints_a_fresh_thread_for_an_unrecognized_thread_id(client, register):
    """AG-UI's `threadId` is caller-minted and required by the schema —
    an id funduq has never seen is a brand new conversation, not an error
    (unlike A2A's optional `contextId`). Nobody is attached, purely so
    the run resolves immediately instead of streaming forever waiting for
    a provider — unrelated to what this test checks.
    """
    served = await register("greeter")

    resp = await client.post(f"/agui/{served.path()}", json=_run_input("thread_made_up"))

    assert resp.status_code == 200, resp.text
    real_thread_id = _run_started(resp.text)["threadId"]
    assert real_thread_id.startswith("thread_")
    assert real_thread_id != "thread_made_up"


async def test_agui_run_against_an_offline_agent_fails_fast(client, register):
    served = await register("translator")

    created = await client.post(f"/threads/{served.path()}")
    thread_id = created.json()["thread_id"]

    resp = await client.post(f"/agui/{served.path()}", json=_run_input(thread_id))

    assert resp.status_code == 200
    assert _run_started(resp.text)["threadId"] == thread_id
    assert "RUN_ERROR" in resp.text
    assert "offline" in resp.text


async def test_agui_run_reaches_an_attached_provider(client, serve):
    """Serving is a live mapping, so there is nothing to fake: the
    provider is really there, and the stream really comes from it."""
    served = await serve(None, "greeter")

    resp = await client.post(f"/agui/{served.path()}", json=_run_input("thread_new"))

    assert resp.status_code == 200, resp.text
    types = [
        json.loads(line[len("data: ") :])["type"]
        for line in resp.text.splitlines()
        if line.startswith("data: ")
    ]
    assert types[0] == "RUN_STARTED"
    assert "TEXT_MESSAGE_CONTENT" in types
    assert types[-1] == "RUN_FINISHED"


async def test_agui_events_carry_no_null_padding(client, serve):
    """The relay rule: events are dumped `exclude_none=True`, so a
    default dump's `timestamp: null` / `rawEvent: null` never enters a
    caller's stream."""
    served = await serve(None, "greeter")

    resp = await client.post(f"/agui/{served.path()}", json=_run_input("thread_nulls"))

    assert resp.status_code == 200, resp.text
    for line in resp.text.splitlines():
        if line.startswith("data: "):
            event = json.loads(line[len("data: ") :])
            assert None not in event.values(), event


async def _offline_run_with_metadata(client, served, metadata):
    """The fast-fail path resolves synchronously, still going through
    ensure_thread/create_run with the real metadata first — so it is
    enough to check what got persisted without needing a live provider.
    """
    body = _run_input("thread_made_up")
    body["metadata"] = metadata
    return await client.post(f"/agui/{served.path()}", json=body)


async def test_agui_run_with_valid_actor_chain_stores_its_head(
    client, session, register, new_identity
):
    """funduq's part in caller identity is four verbs — verify, copy the
    head, relay, refuse. This is the copy: the chain's head key lands on
    the run row, and the chain itself is stored verbatim (revision 5: the
    chain funduq stores is the chain it dispatched)."""
    caller = new_identity()
    served = await register("greeter")
    chain = [caller.sign_hop()]
    assert verify_chain(chain).head == caller.public_key

    resp = await _offline_run_with_metadata(client, served, {"actorChain": chain})
    assert resp.status_code == 200, resp.text
    run_id = _run_started(resp.text)["runId"]

    run = await repo.get_run(session, run_id)
    assert run.head_key == caller.public_key
    # The chain funduq stores is the chain it dispatched (revision 5):
    # the caller's hops as a prefix, funduq's own dispatch hop after.
    stored = list(run.actor_chain)
    assert stored[: len(chain)] == chain
    assert len(stored) == len(chain) + 1


async def test_agui_run_with_invalid_actor_chain_401s(client, register):
    """A tampered chain is refused at the door, never carried —
    `funduq_contract.InvalidChain`, mapped app-wide to 401."""
    served = await register("greeter")

    body = _run_input("thread_made_up")
    body["metadata"] = {"actorChain": ["not-a-real-jwt"]}

    resp = await client.post(f"/agui/{served.path()}", json=body)

    assert resp.status_code == 401


async def test_agui_run_without_actor_chain_is_unaffected(client, session, register):
    served = await register("greeter")

    resp = await _offline_run_with_metadata(client, served, {})
    assert resp.status_code == 200, resp.text
    run_id = _run_started(resp.text)["runId"]

    run = await repo.get_run(session, run_id)
    assert run.head_key is None
    assert not run.actor_chain


async def test_the_chain_reaches_the_agent_verbatim_plus_funduqs_dispatch_hop(
    client, serve, new_identity, souk
):
    """No summary is produced: the agent verifies for itself, from
    `forwardedProps.actorChain` — the caller's hops unmodified **plus**
    funduq's own dispatch hop naming where it sent the run, so the chain
    arriving is one longer than the one the caller presented."""
    caller = new_identity()
    served = await serve(None, "greeter")
    chain = [caller.sign_hop()]

    body = _run_input("thread_chain")
    body["metadata"] = {"actorChain": chain}
    resp = await client.post(f"/agui/{served.path()}", json=body)
    assert resp.status_code == 200, resp.text
    assert "RUN_FINISHED" in resp.text

    seen = served.provider.seen_chain
    assert seen is not None and seen[: len(chain)] == chain
    assert len(seen) == len(chain) + 1
    verified = verify_chain(seen)
    assert verified.head == caller.public_key
    # The extra hop is funduq's, and it names the dispatch target.
    assert verified.hops[-1].actor_public_key == souk.identity_public_key
    assert verified.hops[-1].dispatched_to is not None


async def test_a_paused_run_tells_the_thread_reader_what_it_is_waiting_on(
    client, register, session
):
    """This read path exists for the caller a pause strands: the SSE
    stream closed when the run paused, and it comes back here to find out
    what happened. Since contract revision 16 "paused" is not enough of an
    answer — a resolve proof signs the ask ids themselves, so a caller
    that cannot enumerate them has no proof to build and the pause is
    unanswerable.

    Core summarises the active run as `{run_id, status}` and keeps the ids
    on the run's metadata; joining the two is this seat's job, so the
    snapshot carries them under `active_run.outstanding_asks`, sorted —
    the order `resolve_payload` canonicalizes in.
    """
    served = await register("approver")
    thread_id = await repo.create_thread(session, served.ref())
    created = await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
    await repo.mark_run_status(session, created["run_id"], "running")
    await repo.mark_run_status(
        session,
        created["run_id"],
        "input-required",
        metadata={
            "interrupts": [{"id": "int_2"}, {"id": "int_1"}],
            "pendingToolCalls": ["call_9"],
        },
    )
    await session.commit()

    snapshot = (await client.get(f"/threads/{thread_id}")).json()

    # One id space: interrupts and pending tool calls together, which is
    # exactly what `outstanding_asks` means by "everything it is waiting
    # on" and exactly what the proof must cover.
    assert snapshot["active_run"]["outstanding_asks"] == ["call_9", "int_1", "int_2"]


async def test_a_thread_with_nothing_outstanding_says_nothing(client, register, session):
    """The key's presence has to mean something, so it is absent when
    there is nothing to answer — a caller must not be able to read an
    empty list as "a pause with no asks", which would be a pause nobody
    could ever resolve."""
    served = await register("greeter")
    thread_id = await repo.create_thread(session, served.ref())
    created = await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
    await repo.mark_run_status(session, created["run_id"], "running")
    await session.commit()

    snapshot = (await client.get(f"/threads/{thread_id}")).json()

    assert snapshot["active_run"]["run_id"] == created["run_id"]
    assert "outstanding_asks" not in snapshot["active_run"]


async def test_an_unproven_write_to_a_bound_thread_is_answered_not_a_500(
    client, register, session
):
    """Failing to prove authority over a bound run is a caller mistake,
    not a server fault — and this door is where that has to be a status
    code.

    Three of the errors on this road are not `FunduqError`s at all:
    `InvalidResolution` and `InvalidCancel` are plain ValueErrors from
    core's identity module, and `ThreadMembershipRequired` is a bare
    `Exception` from its repo. Any one the app-wide handler does not name
    falls through to 500, which tells a caller that souk broke rather than
    that its proof did not verify. This drives the outermost of the three
    (membership is checked first) and asserts the property they share.
    The A2A door answers the same refusals inside its own JSON-RPC
    envelope; here the status code is the whole answer.
    """
    served = await register("approver")
    thread_id = await repo.create_thread(session, served.ref(), head_key="ab" * 32)
    created = await repo.create_run(
        session, thread_id, served.ref(), "ag-ui", {}, head_key="ab" * 32
    )
    await repo.mark_run_status(session, created["run_id"], "running")
    await repo.mark_run_status(
        session, created["run_id"], "input-required", metadata={"interrupts": [{"id": "int_1"}]}
    )
    await session.commit()

    resp = await client.post(
        f"/agui/{served.path()}",
        json={**_run_input(thread_id, "approved"), "forwardedProps": {"runId": created["run_id"]}},
    )

    assert resp.status_code == 403, resp.text
    assert "responsibility segment" in resp.json()["detail"]
