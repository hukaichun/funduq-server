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
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from funduq.core import Funduq
from funduq.errors import AgentNotFound
from funduq.models import AgentRef
from funduq.pause import outstanding_asks
from funduq.protocols.agui import AGUIAdapter, EventStream, ThreadSnapshot
from souk_server.deps import get_souk, resolve_ref
from souk_server.models import CreateThreadRequest, CreateThreadResponse

router = APIRouter()


async def _with_outstanding_asks(funduq: Funduq, snapshot: dict) -> dict:
    """Add a paused active run's outstanding ask ids to a thread snapshot.

    This read path exists for exactly the caller a pause strands: the SSE
    stream closed when the run paused, and the caller comes back here to
    find out what happened. Since contract revision 16 the answer it needs
    is not just "paused" but *what it is waiting on* — a resolve proof
    signs the ask ids themselves
    (`funduq-resolve:{run_id}:{sha256 of the sorted, NUL-joined ids}`), so
    a caller that cannot enumerate them cannot build a proof at all and
    the pause is unanswerable.

    Core's snapshot summarises the active run as `{run_id, status}`; the
    ids live on the run's metadata, so this fetches the run rather than
    inventing a field for core to carry. Absent when there is nothing
    outstanding, so the key's presence means something.
    """
    active = snapshot.get("active_run")
    if not active:
        return snapshot
    run = await funduq.get_run(active["run_id"])
    if run is None:
        return snapshot
    asks = outstanding_asks(run.metadata or {})
    if asks:
        # Sorted, matching the canonical order `resolve_payload` hashes in
        # — one fewer thing for a signer to get wrong.
        active["outstanding_asks"] = sorted(asks)
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

    A paused active run also says what it is waiting on, under
    `active_run.outstanding_asks` — see `_with_outstanding_asks`.
    """
    snapshot = await funduq.get_thread_snapshot(thread_id)
    if snapshot is None:
        raise AgentNotFound(f"thread '{thread_id}' not found")
    return await _with_outstanding_asks(funduq, snapshot)


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


async def _run_agent(funduq: Funduq, agent: AgentRef, body: RunAgentInput):
    # presenter_key=None: this deployment has no authenticating seat in
    # front of the doors yet (see docs/server-mode.md on operational-
    # limits §1 — the gateway seat is where presenter auth goes when it
    # exists, and this call site is the plug point).
    result = await AGUIAdapter(funduq).run(agent, body, presenter_key=None)

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
    provider: str, name: str, body: RunAgentInput, funduq: Funduq = Depends(get_souk)
) -> EventSourceResponse | JSONResponse:
    return await _run_agent(funduq, await resolve_ref(funduq, provider, name), body)
