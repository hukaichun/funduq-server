"""Covers the AG-UI HTTP surface: the optional POST /threads endpoint,
that /agui/... runs mint a fresh thread automatically for an
unrecognized threadId rather than requiring POST /threads first
(souk-no-forced-protocol-deviation) — real ag_ui.core.RunAgentInput
shape and all — and the actor-chain door behaviour: a valid chain is
verified, its head recorded, and the chain relayed to the agent verbatim
plus funduq's own dispatch hop; a tampered one is a 401 at the door.

**The caller's declarations live in `forwardedProps`** (revision 20:
"the caller's bag is request-level on both doors; funduq reads nothing
from a message's own metadata"), and funduq's own keys sit one level
down under `forwardedProps.funduq` (revision 18). A chain presented there
must come with a `Funduq-Presenter` proof — origin is not possession, so
revision 21 refuses a chain at a door that cannot say who presented it —
and the header is signed over the request body, so it is built here from
the same bytes that are posted.

Every route takes the pair. "Offline" is arranged by simply not
attaching anyone — `online` is `is_serving`, so an agent nobody attached
is already offline.
"""

from __future__ import annotations

import json
import time

import pytest

from funduq import repo
from funduq.doors import head_key_of
from funduq_contract import verify_chain
from souk_server.presenter import PRESENTER_HEADER, presenter_payload


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


def _presenter_header(identity, body: bytes, *, timestamp: int | None = None) -> dict:
    """The `Funduq-Presenter` proof for exactly these bytes.

    Compact JSON, the same three fields every proof in this system uses,
    over `funduq-server-presenter:{publicKey}:{timestamp}:{sha256hex(
    body)}` — built from the module's own payload builder, never a
    retyped string, so a test cannot agree with itself while disagreeing
    with the gateway.
    """
    when = int(time.time()) if timestamp is None else timestamp
    return {
        PRESENTER_HEADER: json.dumps(
            {
                "publicKey": identity.public_key,
                "timestamp": when,
                "signature": identity.sign(
                    presenter_payload(identity.public_key, when, body)
                ),
            },
            separators=(",", ":"),
        )
    }


async def _post_run(client, served, body: dict, *, presenter=None, header=None):
    """POST one run, signing the presenter proof over the bytes actually
    sent — `json=` would re-serialize and the hash would cover different
    bytes, so the body is encoded once here and posted as content."""
    raw = json.dumps(body).encode()
    headers = {"content-type": "application/json"}
    if header is not None:
        headers.update(header)
    elif presenter is not None:
        headers.update(_presenter_header(presenter, raw))
    return await client.post(f"/agui/{served.path()}", content=raw, headers=headers)


async def _offline_run_with_props(client, served, props, *, presenter=None):
    """The fast-fail path resolves synchronously, still going through
    ensure_thread/create_run with the real bag first — so it is enough to
    check what got persisted without needing a live provider.
    """
    body = _run_input("thread_made_up")
    body["forwardedProps"] = props
    return await _post_run(client, served, body, presenter=presenter)


async def test_agui_run_with_valid_actor_chain_stores_its_head(
    client, session, register, new_identity
):
    """funduq's part in caller identity is four verbs — verify, record the
    head, relay, refuse. This is the record: the chain itself is stored
    verbatim (revision 5: the chain funduq stores is the chain it
    dispatched) and the head is *derived* from it — revision 19 deleted
    the `head_key` column, so `doors.head_key_of` reads it back off the
    chain rather than a copy that could disagree with it."""
    caller = new_identity()
    served = await register("greeter")
    chain = [caller.sign_hop()]
    assert verify_chain(chain).head == caller.public_key

    resp = await _offline_run_with_props(
        client, served, {"actorChain": chain}, presenter=caller
    )
    assert resp.status_code == 200, resp.text
    run_id = _run_started(resp.text)["runId"]

    run = await repo.get_run(session, run_id)
    assert head_key_of(run) == caller.public_key
    # The chain funduq stores is the chain it dispatched (revision 5):
    # the caller's hops as a prefix, funduq's own dispatch hop after.
    stored = list(run.actor_chain)
    assert stored[: len(chain)] == chain
    assert len(stored) == len(chain) + 1


async def test_a_chain_presented_without_the_header_is_a_401(client, register, new_identity):
    """Revision 21's whole point. A chain proves *origin* — these actors
    signed these hops — and says nothing about who is holding the bytes
    now. A door that cannot name the presenter cannot check the last hop,
    so core refuses the chain by name (`PresenterRequired`) rather than
    accepting it and answering absence to everyone afterwards, which was
    the worst of the three ways this could go.

    401, not 400: upstream's own instruction is "map it to authentication
    required, not bad request" — nothing about the request is malformed,
    what is missing is a credential.
    """
    caller = new_identity()
    served = await register("greeter")

    resp = await _offline_run_with_props(client, served, {"actorChain": [caller.sign_hop()]})

    assert resp.status_code == 401, resp.text


async def test_a_presenter_that_is_not_the_last_hop_is_refused(
    client, register, new_identity
):
    """A header that verifies is not a header that fits. The presented key
    has to be the chain's last hop, and that comparison is core's
    (`InvalidChain`), not this gateway's to pre-empt — one refusal in one
    place."""
    caller, someone_else = new_identity(), new_identity()
    served = await register("greeter")

    resp = await _offline_run_with_props(
        client, served, {"actorChain": [caller.sign_hop()]}, presenter=someone_else
    )

    assert resp.status_code == 401, resp.text


@pytest.mark.parametrize(
    "make_header",
    [
        pytest.param(lambda ident, raw: {PRESENTER_HEADER: "not json at all"}, id="unparseable"),
        pytest.param(
            lambda ident, raw: _presenter_header(ident, raw, timestamp=int(time.time()) - 3600),
            id="stale",
        ),
        pytest.param(
            lambda ident, raw: _presenter_header(ident, b"a different body"),
            id="signed-over-other-bytes",
        ),
    ],
)
async def test_a_broken_presenter_proof_is_a_401_never_a_500(
    client, register, new_identity, make_header
):
    """Every way of failing to prove the key reads the same: `None`, and
    then core's own `PresenterRequired`. Malformed, stale and lifted-onto-
    another-body are one answer because they are one question — "did
    anyone prove this key" — and answering them differently would tell an
    attacker which half of the proof to fix next.

    The body hash is what makes the third case fail: a captured header
    cannot be replayed onto a different call, the same property
    `kyok_call_payload` gets from binding a body hash.
    """
    caller = new_identity()
    served = await register("greeter")
    body = {**_run_input("thread_made_up"), "forwardedProps": {"actorChain": [caller.sign_hop()]}}
    raw = json.dumps(body).encode()

    resp = await client.post(
        f"/agui/{served.path()}",
        content=raw,
        headers={"content-type": "application/json", **make_header(caller, raw)},
    )

    assert resp.status_code == 401, resp.text


async def test_agui_run_with_invalid_actor_chain_401s(client, register, new_identity):
    """A tampered chain is refused at the door, never carried —
    `funduq_contract.InvalidChain`, mapped app-wide to 401. Presented with
    a perfectly good proof, so what fails is the chain and nothing else."""
    served = await register("greeter")

    resp = await _offline_run_with_props(
        client, served, {"actorChain": ["not-a-real-jwt"]}, presenter=new_identity()
    )

    assert resp.status_code == 401


async def test_agui_run_without_actor_chain_is_unaffected(client, session, register):
    """The scope of the seat, stated as a test: only a caller that
    presents a chain has to authenticate. No chain, no header, no
    change — a browser or curl keeps working exactly as before."""
    served = await register("greeter")

    resp = await _offline_run_with_props(client, served, {})
    assert resp.status_code == 200, resp.text
    run_id = _run_started(resp.text)["runId"]

    run = await repo.get_run(session, run_id)
    assert head_key_of(run) is None
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
    body["forwardedProps"] = {"actorChain": chain}
    resp = await _post_run(client, served, body, presenter=caller)
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


def _finished_asking(run_id: str, thread_id: str, interrupts: list[dict], tool_calls: list[str]) -> list[dict]:
    """The events a run that finished asking actually leaves behind.

    Revision 19 deleted the `input-required` status and the metadata that
    carried the ids: "a run that finished asking is `completed`, and that
    it left the thread waiting is read from its own events". So a test
    that wants a waiting thread has to write the events a real provider
    writes — an announced tool call nobody answered, and a RUN_FINISHED
    whose outcome is an interrupt.
    """
    events: list[dict] = [{"type": "RUN_STARTED", "runId": run_id, "threadId": thread_id}]
    for tool_call_id in tool_calls:
        events.append(
            {
                "type": "TOOL_CALL_START",
                "toolCallId": tool_call_id,
                "toolCallName": "ask",
            }
        )
    events.append(
        {
            "type": "RUN_FINISHED",
            "runId": run_id,
            "threadId": thread_id,
            "outcome": {"type": "interrupt", "interrupts": interrupts},
        }
    )
    return events


async def _run_that_finished_asking(
    session, served, thread_id, *, interrupts, tool_calls
) -> str:
    created = await repo.create_run(session, thread_id, served.ref(), {})
    run_id = created["run_id"]
    for seq, event in enumerate(
        _finished_asking(run_id, thread_id, interrupts, tool_calls)
    ):
        await repo.append_run_event(session, run_id, seq, event)
    await repo.mark_run_status(session, run_id, "running")
    await repo.mark_run_status(session, run_id, "completed")
    await session.commit()
    return run_id


async def test_a_paused_run_tells_the_thread_reader_what_it_is_waiting_on(
    client, register, session
):
    """This read path exists for the caller a pause strands: the SSE
    stream closed when the run finished asking, and it comes back here to
    find out what happened. Since contract revision 16 "paused" is not
    enough of an answer — a resolve proof signs the ask ids themselves, so
    a caller that cannot enumerate them has no proof to build and the
    pause is unanswerable.

    It is not the *active* run any more: revision 19 made a run that
    finished asking `completed`, so nothing is in flight and the answer
    hangs off its own key, `waiting_run`, read from the thread's latest
    run's events via `pause.open_asks`.
    """
    served = await register("approver")
    thread_id = await repo.create_thread(session, served.ref())
    run_id = await _run_that_finished_asking(
        session,
        served,
        thread_id,
        interrupts=[{"id": "int_2"}, {"id": "int_1"}],
        tool_calls=["call_9"],
    )

    snapshot = (await client.get(f"/threads/{thread_id}")).json()

    # One id space: interrupts and unanswered tool calls together, which
    # is exactly what `open_asks` means by "everything it is waiting on"
    # and exactly what the proof must cover.
    assert snapshot["waiting_run"] == {
        "run_id": run_id,
        "outstanding_asks": ["call_9", "int_1", "int_2"],
    }


async def test_a_thread_with_nothing_outstanding_says_nothing(client, register, session):
    """The key's presence has to mean something, so it is absent when
    there is nothing to answer — a caller must not be able to read an
    empty list as "a pause with no asks", which would be a pause nobody
    could ever resolve. `open_asks` is empty for an unfinished run too, so
    a run still in flight says nothing either."""
    served = await register("greeter")
    thread_id = await repo.create_thread(session, served.ref())
    created = await repo.create_run(session, thread_id, served.ref(), {})
    await repo.mark_run_status(session, created["run_id"], "running")
    await session.commit()

    snapshot = (await client.get(f"/threads/{thread_id}")).json()

    assert snapshot["active_run"]["run_id"] == created["run_id"]
    assert "waiting_run" not in snapshot


async def test_an_unproven_write_to_a_bound_thread_is_answered_not_a_500(
    client, register, session, new_identity
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
    stranger = new_identity()
    served = await register("approver")
    thread_id = await repo.create_thread(session, served.ref(), head_key="ab" * 32)
    await _run_that_finished_asking(
        session, served, thread_id, interrupts=[{"id": "int_1"}], tool_calls=[]
    )

    body = {
        **_run_input(thread_id, "approved"),
        "forwardedProps": {"actorChain": [stranger.sign_hop()]},
    }
    resp = await _post_run(client, served, body, presenter=stranger)

    assert resp.status_code == 403, resp.text
    assert "responsibility segment" in resp.json()["detail"]


async def test_a_second_reader_of_one_run_is_a_409_not_a_500():
    """Revision 21 admits one stream per run: two consumers of one queue
    would each get half the events, so the second `subscribe` raises
    `StreamTaken`. It reaches the AG-UI door and the facade raw — only the
    A2A path translates it — so without a row in the app's error map it
    would surface as a 500, telling a caller that souk broke when what
    happened is that somebody else is already reading.

    409, because it is a conflict with what is already happening: the
    request is well formed and the remedy is to wait or to read the run's
    stored events, not to send something different.
    """
    from fastapi import FastAPI
    from funduq.errors import StreamTaken
    from httpx import ASGITransport, AsyncClient

    from souk_server.deps import install_error_handlers

    app = FastAPI()

    @app.get("/boom")
    async def boom() -> dict:
        raise StreamTaken("run run_1 already has a reader")

    install_error_handlers(app)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as probe:
        resp = await probe.get("/boom")

    assert resp.status_code == 409, resp.text
    assert "already has a reader" in resp.json()["detail"]
