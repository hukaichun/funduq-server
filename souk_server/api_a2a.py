"""A2A HTTP surface: the package's own dispatcher over upstream's handler.

What A2A *means* — Task.id being funduq's run_id, contextId being
thread_id, what a second send does when a session already has a live run
— lives in funduq/protocols/a2a.py, in core, which hands back A2A's own
messages (`AgentCard`, `Task`, the update events) and writes no JSON-RPC
at all. The envelopes, method names, error codes and **version
negotiation** come from a2a-sdk's `JsonRpcDispatcher`, mounted here per
upstream's transport guide: which protocol version a request speaks
rides the `A2A-Version` HTTP header (absent means 0.3), and only the
transport ever sees a header — `enable_v0_3_compat=True` is what keeps
every v0.3 client answered in v0.3's own shapes.

**The handler between them is upstream's now** (funduq#225).
`A2ARequestHandler` is a real `a2a.server.RequestHandler` bound to one
agent: it owns `MessageToDict`, `validate_request_params`, the
`configuration.return_immediately` / `.history_length` mapping, and which
of A2A's operations funduq offers at all. This file had a hand-rolled
copy of that, and a copy of a mapping is a copy that drifts — the
configuration fields upstream deliberately does and does not honour were
simply absent from ours. What is left here is the two things a transport
must decide for itself: which errors leave A2A's vocabulary, and what
rides the wire that A2A has no field for.

Two errors are deliberately funduq's, because A2A has no word for either
and one that means something else would be worse:

- `AgentNotFound` → **404 on the route**, not a JSON-RPC error inside a
  200: the agent is the endpoint, resolved from the path before the
  dispatcher runs.
- `ThreadQueueFull` → **429, and say retry**: backpressure — the request
  was *not* accepted, and accept-then-expire is the lie this refuses to
  tell. Raised as a Starlette `HTTPException` from inside the handler
  because that is the one exception type the dispatcher re-raises
  instead of converting to a JSON-RPC internal error.

**The view proof rides a header.** Since contract revision 13, reading a
run whose thread is bound to a chain requires a signed view proof, and a
read without one is answered as absence — existence is part of what is
guarded. A2A's read requests carry no caller data at all, so the proof
has nowhere in the protocol to travel: it comes in as `X-Funduq-View`,
compact JSON `{"publicKey", "timestamp", "signature"}`, and reaches core
through `A2ARequestHandler(view_metadata_of=...)` as `{"view": {…}}`.
Absent or malformed passes nothing, and a bound run then reads as
absent — the designed answer, never a 500, because a 500 would tell an
unauthorized reader that there was something there to fail on.

`CancelTaskRequest.metadata` is passed through whole by the handler: a
run on a thread that bound an authority at birth can only be stopped by
one of that thread's authorities, and the proof rides in that field
(`metadata.cancel`, with `metadata.resolution` beside it). Drop the field
and every cancel on a bound thread is refused; forge nothing — funduq
verifies the signature, not the envelope. There is no `metadata.
delegation` any more: the session delegation certificate was removed at
revision 15, and a grant is the authenticating seat's policy now.

**A paused run says what it is waiting on.** A resolve proof signs the
ask it answers (revision 16: `funduq-resolve:{run_id}:{sha256 of the
sorted, NUL-joined ask ids}`), so a caller that cannot see the ask ids
cannot build one at all. Core knows them and A2A has no field for them,
which makes surfacing them this seat's job: `funduq/outstandingAsks` on
the Task's metadata, wherever this door hands back a paused run.

`presenter_key_of=None` on the handler: core's caller doors are not
independently safe (operational-limits §1) — a chain proves origin, not
possession — and the gateway seat is where presenter authentication goes
when this deployment grows one. That parameter is the plug point.

One way to address an agent: `/a2a/{provider}/{name}/...`. An agent *is*
`(provider_key, name)`, so addressing it takes both and takes nothing
funduq minted; `provider` may be the full public key or its 16-hex
fingerprint, which core tells apart by length.
"""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncGenerator
from typing import Any

from a2a.server.context import ServerCallContext
from a2a.server.events.event_queue import Event
from a2a.server.routes.jsonrpc_dispatcher import JsonRpcDispatcher
from a2a.types import a2a_pb2 as pb
from a2a.utils.constants import AGENT_CARD_WELL_KNOWN_PATH
from fastapi import APIRouter, Depends, Request
from google.protobuf.json_format import MessageToDict
from starlette.exceptions import HTTPException

from funduq.core import Funduq
from funduq.errors import AgentNotFound, ThreadQueueFull
from funduq.identity import provider_fingerprint
from funduq.models import AgentRef
from funduq.pause import outstanding_asks
from funduq.protocols.a2a import A2AAdapter, A2ARequestHandler, ServedInterface
from souk_server.config import ServingSettings
from souk_server.deps import get_serving_settings, get_souk, resolve_ref

logger = logging.getLogger("souk.api_a2a")

router = APIRouter()

# The header a view proof rides in. Named for what it proves rather than
# for this gateway, because the thing it carries is upstream's shape and a
# second transport speaking to the same core should spell it the same way.
VIEW_PROOF_HEADER = "x-funduq-view"

# Where a paused run's outstanding ask ids appear on a Task. Same
# namespace convention as core's own `funduq/cancelRequested`: a key that
# is visibly not A2A's, so nobody reads it as part of the protocol.
OUTSTANDING_ASKS_METADATA_KEY = "funduq/outstandingAsks"


def _interfaces(agent: AgentRef, serving: ServingSettings) -> list[ServedInterface]:
    """Where this gateway actually serves that agent.

    Core stopped naming URLs, which is right: it had been interpolating a
    route layout on behalf of every gateway that would ever serve it. The
    layout below is this repo's — `/a2a/{fingerprint}/{name}/rpc` — and
    saying so here is the whole of what changed.
    """
    base = serving.public_http_url.rstrip("/")
    return [
        ServedInterface(
            url=f"{base}/a2a/{provider_fingerprint(agent.provider_key)}/{agent.name}/rpc",
            binding="JSONRPC",
        )
    ]


def _escape(exc: Exception) -> Exception:
    """The two errors that leave A2A's vocabulary, sent up as HTTP.

    `HTTPException` is the one type the dispatcher re-raises rather than
    converting to a JSON-RPC `InternalError` inside a 200, so it is the
    only vehicle that reaches the route with the status intact. Everything
    else funduq raises here is either A2A's own error type (relayed by the
    dispatcher with the package's error codes) or a genuine 500.
    """
    if isinstance(exc, AgentNotFound):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, ThreadQueueFull):
        return HTTPException(
            status_code=429,
            detail=f"{exc} — retry after a moment; the request was not accepted",
            headers={"Retry-After": "1"},
        )
    return exc


def view_metadata_of(context: ServerCallContext) -> dict[str, Any] | None:
    """The `X-Funduq-View` header as the `{"view": …}` metadata core reads.

    **Nothing here judges the proof.** Whether the signature verifies,
    whether the signer is on the run's chain and whether the timestamp is
    inside the 60-second window are core's questions, asked against a run
    this function has never seen. All this does is get the caller's bytes
    across a protocol that has no field for them.

    Absent, unparseable, or not a JSON object → **pass nothing**, which
    makes a bound run read as absent. That is the designed answer rather
    than a swallowed error: a 400 here would tell a caller holding a
    malformed proof that there was a run behind the id worth fixing it
    for, and this door's whole rule is that an unauthorized read cannot
    tell absence from refusal.
    """
    raw = (context.state.get("headers") or {}).get(VIEW_PROOF_HEADER)
    if not raw:
        return None
    try:
        proof = json.loads(raw)
    except (TypeError, ValueError):
        logger.debug("ignoring an unparseable %s header", VIEW_PROOF_HEADER)
        return None
    if not isinstance(proof, dict):
        return None
    return {"view": proof}


async def _annotate_asks(funduq: Funduq, task: pb.Task | None) -> pb.Task | None:
    """Put a paused run's outstanding ask ids on the Task it comes back as.

    The one thing a caller cannot do without: a resolve proof signs the
    exact set of asks it answers, canonicalized inside
    `funduq_contract.resolve_payload`, so a caller that cannot enumerate
    them has no proof to build and no way to answer the pause. Core has
    the ids on the run's metadata and A2A has no field for them; this seat
    is where the two meet.

    Read off the run rather than the Task's state, so it answers the
    question actually asked — "is anything outstanding" — rather than a
    status name that may spell a pause differently tomorrow. Sorted,
    because the payload's canonical order is sorted and a caller reading
    them in that order is one fewer thing to get wrong.
    """
    if task is None:
        return None
    run = await funduq.get_run(task.id)
    if run is None:
        return task
    asks = outstanding_asks(run.metadata or {})
    if asks:
        task.metadata.update({OUTSTANDING_ASKS_METADATA_KEY: sorted(asks)})
    return task


class SoukA2ARequestHandler(A2ARequestHandler):
    """Upstream's handler, plus the two things a transport owns.

    Everything about *A2A* is inherited — the protobuf conversions, the
    parameter validation, the configuration mapping, which operations are
    offered. What is overridden is the pair of decisions that are
    genuinely this gateway's: which funduq errors escape as HTTP statuses
    rather than as JSON-RPC errors inside a 200, and surfacing a paused
    run's ask ids, which A2A has no field for and a caller cannot proceed
    without.
    """

    def __init__(self, funduq: Funduq, agent: AgentRef) -> None:
        super().__init__(
            funduq,
            agent,
            # No edge authentication in this deployment yet — see the
            # module docstring and operational-limits §1. This is the
            # plug point, left explicit rather than defaulted so that
            # growing one is an edit here and not a discovery.
            presenter_key_of=None,
            view_metadata_of=view_metadata_of,
        )
        self._funduq = funduq

    async def on_message_send(
        self, params: pb.SendMessageRequest, context: ServerCallContext
    ) -> pb.Task | pb.Message:
        try:
            sent = await super().on_message_send(params, context)
        except (AgentNotFound, ThreadQueueFull) as exc:
            raise _escape(exc) from exc
        if isinstance(sent, pb.Task):
            return await _annotate_asks(self._funduq, sent)
        return sent

    async def on_message_send_stream(
        self, params: pb.SendMessageRequest, context: ServerCallContext
    ) -> AsyncGenerator[Event]:
        try:
            async for event in super().on_message_send_stream(params, context):
                yield event
        except (AgentNotFound, ThreadQueueFull) as exc:
            raise _escape(exc) from exc

    async def on_get_task(
        self, params: pb.GetTaskRequest, context: ServerCallContext
    ) -> pb.Task | None:
        # None means not-this-agent's, or a bound run read without a valid
        # view proof — indistinguishable from not-found, which is the
        # point. An id naming nothing at all raises A2A's own
        # TaskNotFoundError inside the adapter.
        return await _annotate_asks(
            self._funduq, await super().on_get_task(params, context)
        )

    async def on_cancel_task(
        self, params: pb.CancelTaskRequest, context: ServerCallContext
    ) -> pb.Task | None:
        return await _annotate_asks(
            self._funduq, await super().on_cancel_task(params, context)
        )


# The path comes from a2a.utils.constants rather than being typed here, for
# the same reason every other A2A string does: v1.0 moved it (from
# `/.well-known/agent.json`), and this layer should learn that from the
# package rather than from a client failing against it. Only the current
# path is served — answering the old URL with the new body would hand a
# pre-v1 client a card it cannot use to locate the RPC endpoint.
@router.get("/a2a/{provider}/{name}" + AGENT_CARD_WELL_KNOWN_PATH)
async def agent_card_by_pair(
    provider: str,
    name: str,
    funduq: Funduq = Depends(get_souk),
    serving: ServingSettings = Depends(get_serving_settings),
) -> dict:
    agent = await resolve_ref(funduq, provider, name)
    card = await A2AAdapter(funduq).agent_card(agent, _interfaces(agent, serving))
    return MessageToDict(card, preserving_proto_field_name=False)


@router.post("/a2a/{provider}/{name}/rpc")
async def rpc_by_pair(
    provider: str,
    name: str,
    request: Request,
    funduq: Funduq = Depends(get_souk),
):
    # Resolved from the route before the dispatcher runs: an unknown agent
    # means the address does not exist, so it is a 404 here — never a
    # JSON-RPC error inside a 200.
    agent = await resolve_ref(funduq, provider, name)
    dispatcher = JsonRpcDispatcher(
        request_handler=SoukA2ARequestHandler(funduq, agent),
        enable_v0_3_compat=True,
    )
    return await dispatcher.handle_requests(request)
