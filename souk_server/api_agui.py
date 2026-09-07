"""AG-UI HTTP surface: routes and SSE framing only.

What AG-UI *means* — minting a thread for an unrecognized threadId,
deciding whether a call starts a run or reports an active one,
fast-failing an offline agent — lives in funduq/protocols/agui.py, in
core. This file parses requests and frames results as SSE or JSON. It
does not map errors either: adapters raise funduq.errors and one handler
translates them for the whole app (see souk_server.deps.
install_error_handlers), because which status a failure deserves is a
property of the failure, not of the route that hit it.

Framing is entirely this side's now — `EventStream` carries the events,
not a serialization of them. Two rules constrain the serializer, both
upstream's: dump typed events `exclude_none=True` (a default dump
injects `timestamp: null` into the caller's stream), and relay an event
whose type funduq does not know **untouched** — a provider on a newer
AG-UI must not be cut off by an event type this gateway has not heard
of, so unknown events arrive here as the original mapping and go out as
exactly that.

`POST /threads` remains an *optional* way to obtain a thread_id upfront —
e.g. to show it in a UI before the first message — not a prerequisite:
forcing every caller through it would break a standard, unmodified AG-UI
client that has never heard of it (souk-no-forced-protocol-deviation).
"""

from __future__ import annotations

import json
from typing import Any

from ag_ui.core import RunAgentInput
from fastapi import APIRouter, Depends, Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from funduq import repo
from funduq.core import Funduq
from funduq.errors import AgentNotFound
from funduq.models import AgentRef
from funduq.pause import open_asks
from funduq.protocols.agui import AGUIAdapter, EventStream, ThreadSnapshot
from souk_server.deps import get_souk, resolve_ref
from souk_server.models import CreateThreadRequest, CreateThreadResponse
from souk_server.presenter import PRESENTER_HEADER, presenter_key_of

router = APIRouter()


async def _with_outstanding_asks(funduq: Funduq, thread_id: str, snapshot: dict) -> dict:
    """Say what this thread is waiting on, if anything.

    This read path exists for exactly the caller a pause strands: the SSE
    stream closed when the run finished asking, and the caller comes back
    here to find out what happened. Since contract revision 16 the answer
    it needs is not just "paused" but *what it is waiting on* — a resolve
    proof signs the ask ids themselves
    (`funduq-resolve:{root_run_id}:{sha256 of the sorted, NUL-joined
    ids}`), so a caller that cannot enumerate them cannot build a proof at
    all and the pause is unanswerable.

    **Not `active_run` any more.** Revision 19 deleted `input-required`:
    a run that finished asking is `completed`, so it is not the thread's
    *active* run and there is nothing in flight to hang the ids on. The
    question is now about the thread's latest run and its events —
    `pause.open_asks`, which is empty for a run that has not finished and
    for one that finished with nothing open — so the answer gets its own
    key, `waiting_run`, and is absent when there is nothing to answer. A
    caller must not be able to read an empty list as "a pause with no
    asks", which would be a pause nobody could ever resolve.

    `run_id` beside the ids because the two are used together and the
    caller has to name the run it is answering; sorted ids, matching the
    canonical order `resolve_payload` hashes in — one fewer thing for a
    signer to get wrong. Note the id a *proof* is signed over is the
    lineage root (an A2A task id), which for a first pause is this run
    itself; `funduq.lineage` names it for later ones.
    """
    async with funduq.session() as session:
        latest = await repo.latest_run_for_thread(session, thread_id)
        if latest is None or latest.cancel_requested_by is not None:
            return snapshot
        asks = open_asks(await repo.get_run_events(session, latest.run_id))
    if asks:
        snapshot["waiting_run"] = {
            "run_id": latest.run_id,
            "outstanding_asks": sorted(asks),
        }
    return snapshot


def encode_event(event: Any) -> str:
    """One AG-UI event as the SSE `data:` payload.

    Core hands events over as mappings already dumped `exclude_none=True`
    (and unknown-typed ones as the caller's original mapping, which must
    survive the trip byte-for-value). A typed model reaching here is
    dumped the same way, so the rule holds whichever shape arrives.
    """
    if isinstance(event, BaseModel):
        return event.model_dump_json(by_alias=True, exclude_none=True)
    return json.dumps(event)


async def _create_thread(funduq: Funduq, agent: AgentRef, body: CreateThreadRequest) -> CreateThreadResponse:
    if await funduq.get_agent(agent) is None:
        raise AgentNotFound(f"agent '{agent.name}' is not registered for that provider")
    return CreateThreadResponse(thread_id=await funduq.create_thread(agent, metadata=body.metadata))


@router.post("/threads/{provider}/{name}")
async def create_thread(
    provider: str,
    name: str,
    body: CreateThreadRequest = CreateThreadRequest(),
    funduq: Funduq = Depends(get_souk),
) -> CreateThreadResponse:
    return await _create_thread(funduq, await resolve_ref(funduq, provider, name), body)


@router.get("/threads/{thread_id}")
async def get_thread_snapshot(thread_id: str, funduq: Funduq = Depends(get_souk)) -> dict:
    """Lets a caller catch up on a thread without a live stream — e.g. after
    its original AG-UI SSE connection closed because the run it was watching
    paused, and it needs to know what has happened since.

    A thread waiting on an answer also says what it is waiting on, under
    `waiting_run` — see `_with_outstanding_asks`.
    """
    snapshot = await funduq.get_thread_snapshot(thread_id)
    if snapshot is None:
        raise AgentNotFound(f"thread '{thread_id}' not found")
    return await _with_outstanding_asks(funduq, thread_id, snapshot)


@router.get("/threads/{thread_id}/tree")
async def get_thread_tree(thread_id: str, funduq: Funduq = Depends(get_souk)) -> dict:
    """Full call-chain lineage rooted at `thread_id`, so whoever started the
    original call can later ask what their request actually fanned out to.
    Only as complete as callers chose to make it: a hop appears only if the
    caller recorded the lineage (real A2A `referenceTaskIds`, not a souk
    invention) when it called through this gateway.
    """
    tree = await funduq.get_thread_tree(thread_id)
    if tree is None:
        raise AgentNotFound(f"thread '{thread_id}' not found")
    return tree


async def _run_agent(
    funduq: Funduq, agent: AgentRef, body: RunAgentInput, request: Request
):
    # Who presented this request, proved over the bytes actually received:
    # the raw body, which FastAPI has already read and cached, so asking
    # for it again is free and gives exactly what the signature covers.
    # `None` for a caller that sent no proof — unchanged behaviour, and a
    # caller with no chain stays anonymous. A caller that *did* send a
    # chain is refused by core (`PresenterRequired` -> 401), not here:
    # one refusal in one place beats two that can disagree.
    presenter = presenter_key_of(request.headers.get(PRESENTER_HEADER), await request.body())
    result = await AGUIAdapter(funduq).run(agent, body, presenter_key=presenter)

    if isinstance(result, ThreadSnapshot):
        # The resolved thread_id is already the top-level `thread_id` field
        # of this body — the standard in-band place for it, so no custom
        # header is needed.
        return JSONResponse(jsonable_encoder(result.data))

    assert isinstance(result, EventStream)

    # No X-Souk-Thread-Id/X-Souk-Run-Id headers either: a run's own first
    # event is RUN_STARTED, which every compliant AG-UI provider emits with
    # threadId/runId copied from the RunAgentInput it was given. That is the
    # standard, in-band place a client learns them.
    async def stream():
        async for event in result.events:
            yield {"event": "message", "data": encode_event(event)}

    return EventSourceResponse(stream())


@router.post("/agui/{provider}/{name}", response_model=None)
async def run_agent_by_id(
    provider: str,
    name: str,
    body: RunAgentInput,
    request: Request,
    funduq: Funduq = Depends(get_souk),
) -> EventSourceResponse | JSONResponse:
    return await _run_agent(
        funduq, await resolve_ref(funduq, provider, name), body, request
    )
