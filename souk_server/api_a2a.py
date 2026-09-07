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

Three errors are deliberately funduq's, because A2A has no word for any
of them and one that means something else would be worse:

- `AgentNotFound` → **404 on the route**, not a JSON-RPC error inside a
  200: the agent is the endpoint, resolved from the path before the
  dispatcher runs.
- `ThreadQueueFull` → **429, and say retry**: backpressure — the request
  was *not* accepted, and accept-then-expire is the lie this refuses to
  tell. Raised as a Starlette `HTTPException` from inside the handler
  because that is the one exception type the dispatcher re-raises
  instead of converting to a JSON-RPC internal error.
- `PresenterRequired` → **401**, upstream's own instruction ("map it to
  authentication required, not bad request"). Authentication is the
  transport's business by construction — the header is not in A2A's
  vocabulary, so neither is its absence — and left unescaped it would
  reach the caller as a JSON-RPC *internal error* inside a 200: a server
  fault reported for a missing credential, saying nothing about what to
  send instead.

**One header answers both halves.** Revision 21 collapsed reading and
writing onto a single question — who is presenting this request — and
`presenter_key_of` is where the transport answers it: for a write it is
the key the chain's last hop must match, for a read it is who is looking.
The proof rides in `Funduq-Presenter` (souk_server/presenter.py), signed
over the request body it accompanies. Absent or malformed is `None`,
never an error: a bound run then reads as absent — the designed answer,
because a 500 would tell an unauthorized reader that there was something
there to fail on — and a *chain* presented with no proof is refused by
core, by name (`PresenterRequired` -> 401). The old `X-Funduq-View`
header is gone with the `view_metadata_of` hook that fed it.

`CancelTaskRequest.metadata` is passed through whole by the handler: a
run on a thread that bound an authority at birth can only be stopped by
one of that thread's authorities, and the proof rides in that field
(`metadata.cancel`, with `metadata.resolution` beside it). Drop the field
and every cancel on a bound thread is refused; forge nothing — funduq
verifies the signature, not the envelope. There is no `metadata.
delegation` any more: the session delegation certificate was removed at
revision 15, and a grant is the authenticating seat's policy now.

**A paused run says what it is waiting on.** A resolve proof signs the
asks it answers (revision 16: `funduq-resolve:{run_id}:{sha256 of the
sorted, NUL-joined ask ids}`), so a caller that cannot see the ask ids
cannot build one at all. Core knows them and A2A has no field for them,
which makes surfacing them this seat's job: `metadata.funduq.
outstandingAsks` on the Task, wherever this door hands back a task
waiting on an answer. Under `funduq`, beside core's own keys, because
revision 18 moved every key core writes there and a flat `funduq/…` key
now looks like a caller's rather than the serving layer's.

**A task is a lineage, not a run.** Revision 19 made an answer a *new*
run with `parentRunId`, and the task id names the lineage's **root**. So
the asks a caller must answer are the *tail's*, and reading them off
`get_run(task.id)` would compute a second-or-later pause off the wrong
run — the first run in the lineage, whose asks were answered turns ago.

One way to address an agent: `/a2a/{provider}/{name}/...`. An agent *is*
`(provider_key, name)`, so addressing it takes both and takes nothing
funduq minted; `provider` may be the full public key or its 16-hex
fingerprint, which core tells apart by length.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, Callable

from a2a.server.context import ServerCallContext
from a2a.server.events.event_queue import Event
from a2a.server.routes.jsonrpc_dispatcher import JsonRpcDispatcher
from a2a.types import a2a_pb2 as pb
from a2a.utils.constants import AGENT_CARD_WELL_KNOWN_PATH
from fastapi import APIRouter, Depends, Request
from google.protobuf.json_format import MessageToDict
from starlette.exceptions import HTTPException

from funduq.core import Funduq
from funduq.errors import AgentNotFound, PresenterRequired, ThreadQueueFull
from funduq.identity import provider_fingerprint
from funduq.models import AgentRef
from funduq.pause import open_asks
from funduq.protocols.a2a import A2AAdapter, A2ARequestHandler, ServedInterface
from funduq.protocols.a2a_translate import funduq_metadata_of
from funduq.props import OBSERVED_METADATA_KEY
from souk_server.config import ServingSettings
from souk_server.deps import get_serving_settings, get_souk, resolve_ref
from souk_server.presenter import PRESENTER_HEADER, presenter_key_of

logger = logging.getLogger("souk.api_a2a")

router = APIRouter()

# Where a waiting task's outstanding ask ids appear on a Task: under the
# one key everything funduq writes into A2A metadata lives beneath
# (`funduq.props.OBSERVED_METADATA_KEY`, beside core's own `interrupts`
# and `cancelRequested`), so the whole of what is visibly not A2A's sits
# in one place and nobody reads any of it as part of the protocol.
OUTSTANDING_ASKS_METADATA_KEY = "outstandingAsks"


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
    """The three errors that leave A2A's vocabulary, sent up as HTTP.

    `HTTPException` is the one type the dispatcher re-raises rather than
    converting to a JSON-RPC `InternalError` inside a 200, so it is the
    only vehicle that reaches the route with the status intact. Everything
    else funduq raises here is either A2A's own error type (relayed by the
    dispatcher with the package's error codes) or a genuine 500.
    """
    if isinstance(exc, AgentNotFound):
        return HTTPException(status_code=404, detail=str(exc))
    if isinstance(exc, PresenterRequired):
        return HTTPException(status_code=401, detail=str(exc))
    if isinstance(exc, ThreadQueueFull):
        return HTTPException(
            status_code=429,
            detail=f"{exc} — retry after a moment; the request was not accepted",
            headers={"Retry-After": "1"},
        )
    return exc


def _presenter_key_of(body: bytes) -> Callable[[ServerCallContext], str | None]:
    """The `presenter_key_of` hook for one request, closed over its body.

    A2A carries no field for a caller's identity — its read requests carry
    no caller data at all — so the proof travels in a header
    (`Funduq-Presenter`) and the transport is the party that checks it.
    a2a-sdk's default context builder copies the request's headers into
    `context.state["headers"]`, already lowercased, which is the whole of
    what this needs from the protocol.

    The hook is per-request rather than per-app because the proof is bound
    to the body: a signature over these exact bytes cannot be lifted onto
    another call, which is the property that makes a captured header
    worthless.

    Nothing here judges *whether* the key may do what the request asks.
    An unproven read answers absence; a chain whose last hop is not this
    key is `InvalidChain`; a chain with no proof at all is
    `PresenterRequired`. All three are core's to say, and saying any of
    them twice would eventually say them differently.
    """

    def read(context: ServerCallContext) -> str | None:
        headers = context.state.get("headers") or {}
        return presenter_key_of(headers.get(PRESENTER_HEADER), body)

    return read


async def _annotate_asks(funduq: Funduq, task: pb.Task | None) -> pb.Task | None:
    """Put a waiting task's outstanding ask ids on the Task it comes back as.

    The one thing a caller cannot do without: a resolve proof signs the
    exact set of asks it answers, canonicalized inside
    `funduq_contract.resolve_payload`, so a caller that cannot enumerate
    them has no proof to build and no way to answer the pause. Core has
    them in the run's events and A2A has no field for them; this seat is
    where the two meet.

    **The tail's asks, not the root's.** A task id names the run that
    started the lineage; every answer since has opened a new run under it.
    `funduq.lineage(task.id)` is that lineage, root first, so its last
    entry is the run actually waiting — reading `get_run(task.id)` instead
    would answer a second pause with the first one's ids, which no proof
    would ever match. (The proof is still signed over `task.id`: the root
    is the id a caller holds across every turn.)

    Read off the run's events rather than the Task's state, so it answers
    the question actually asked — "is anything outstanding" — rather than
    a status name that may spell a pause differently tomorrow; revision 19
    already deleted the one it used to spell it with. Sorted, because the
    payload's canonical order is sorted and a caller reading them in that
    order is one fewer thing to get wrong.

    Merged into whatever core already wrote under `funduq`, never
    assigned over it: a protobuf `Struct` field is replaced wholesale, so
    a plain update here would silently drop `interrupts` and
    `cancelRequested` — the two keys a caller most needs beside these.
    """
    if task is None:
        return None
    lineage = await funduq.lineage(task.id)
    if not lineage:
        return task
    asks = open_asks(await funduq.get_run_events(lineage[-1].run_id))
    if asks:
        ours = {**funduq_metadata_of(task), OUTSTANDING_ASKS_METADATA_KEY: sorted(asks)}
        task.metadata.update({OBSERVED_METADATA_KEY: ours})
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

    def __init__(self, funduq: Funduq, agent: AgentRef, body: bytes) -> None:
        super().__init__(
            funduq,
            agent,
            # The one hook core asks the transport to fill: who presented
            # this request. `body` is the raw request bytes the proof is
            # signed over, captured on the route before the dispatcher
            # parses anything — a proof bound to a different body is a
            # proof for a different call.
            presenter_key_of=_presenter_key_of(body),
        )
        self._funduq = funduq

    async def on_message_send(
        self, params: pb.SendMessageRequest, context: ServerCallContext
    ) -> pb.Task | pb.Message:
        try:
            sent = await super().on_message_send(params, context)
        except (AgentNotFound, PresenterRequired, ThreadQueueFull) as exc:
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
        except (AgentNotFound, PresenterRequired, ThreadQueueFull) as exc:
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
    # Read once, here: Starlette caches it, so the dispatcher parses the
    # very bytes the presenter proof covers rather than a re-read of the
    # stream, and the two can never disagree.
    dispatcher = JsonRpcDispatcher(
        request_handler=SoukA2ARequestHandler(funduq, agent, await request.body()),
        enable_v0_3_compat=True,
    )
    return await dispatcher.handle_requests(request)
