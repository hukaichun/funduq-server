"""A2A HTTP surface: the package's own dispatcher over a thin handler.

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

Two errors are deliberately funduq's, because A2A has no word for either
and one that means something else would be worse (upstream's
writing-a-transport.md):

- `AgentNotFound` → **404 on the route**, not a JSON-RPC error inside a
  200: the agent is the endpoint, resolved from the path before the
  dispatcher runs.
- `ThreadQueueFull` → **429, and say retry**: backpressure — the request
  was *not* accepted, and accept-then-expire is the lie this refuses to
  tell. Raised as a Starlette `HTTPException` from inside the handler
  because that is the one exception type the dispatcher re-raises
  instead of converting to a JSON-RPC internal error.

`CancelTaskRequest.metadata` is passed through whole: a run on a thread
that bound an authority at birth can only be stopped by one of that
thread's authorities, and the proof rides in that field
(`metadata.cancel`, with `metadata.resolution` / `metadata.delegation`
beside it). Drop the field and every cancel on a bound thread is
refused; forge nothing — funduq verifies the signature, not the
envelope.

`presenter_key=None` on every adapter call: core's caller doors are not
independently safe (operational-limits §1) — a chain proves origin, not
possession — and the gateway seat is where presenter authentication goes
when this deployment grows one. These call sites are the plug point.

One way to address an agent: `/a2a/{provider}/{name}/...`. An agent *is*
`(provider_key, name)`, so addressing it takes both and takes nothing
funduq minted; `provider` may be the full public key or its 16-hex
fingerprint, which core tells apart by length.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any

from a2a.server.context import ServerCallContext
from a2a.server.events.event_queue import Event
from a2a.server.request_handlers.request_handler import RequestHandler
from a2a.server.routes.jsonrpc_dispatcher import JsonRpcDispatcher
from a2a.types import a2a_pb2 as pb
from a2a.utils.constants import AGENT_CARD_WELL_KNOWN_PATH
from a2a.utils.errors import UnsupportedOperationError
from fastapi import APIRouter, Depends, Request
from google.protobuf.json_format import MessageToDict
from starlette.exceptions import HTTPException

from funduq.core import Funduq
from funduq.errors import AgentNotFound, ThreadQueueFull
from funduq.identity import provider_fingerprint
from funduq.models import AgentRef
from funduq.protocols.a2a import A2AAdapter, ServedInterface
from souk_server.config import ServingSettings
from souk_server.deps import get_serving_settings, get_souk, resolve_ref

router = APIRouter()


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


def _metadata(struct) -> dict[str, Any] | None:
    """A request's protobuf Struct metadata as the dict core takes —
    None when the caller sent none, so absence stays absence."""
    if struct is None:
        return None
    mapping = MessageToDict(struct)
    return mapping or None


class FunduqRequestHandler(RequestHandler):
    """a2a-sdk's handler interface over the typed `A2AAdapter`, for one
    agent — the one the route resolved. Which operations are offered and
    which are deliberately not mirrors core's own catalogue (see
    upstream's test_a2a_spec_methods.py); the not-offered ones answer
    with A2A's own `UnsupportedOperationError`.
    """

    def __init__(self, adapter: A2AAdapter, agent: AgentRef) -> None:
        self._adapter = adapter
        self._agent = agent

    async def on_message_send(
        self, params: pb.SendMessageRequest, context: ServerCallContext
    ) -> pb.Task | pb.Message:
        try:
            return await self._adapter.send_task(
                self._agent,
                MessageToDict(params.message),
                metadata=_metadata(params.metadata),
                presenter_key=None,
            )
        except (AgentNotFound, ThreadQueueFull) as exc:
            raise _escape(exc) from exc

    async def on_message_send_stream(
        self, params: pb.SendMessageRequest, context: ServerCallContext
    ) -> AsyncGenerator[Event]:
        try:
            stream = await self._adapter.send_task_streaming(
                self._agent,
                MessageToDict(params.message),
                metadata=_metadata(params.metadata),
                presenter_key=None,
            )
        except (AgentNotFound, ThreadQueueFull) as exc:
            raise _escape(exc) from exc
        async for event in stream:
            yield event

    async def on_get_task(
        self, params: pb.GetTaskRequest, context: ServerCallContext
    ) -> pb.Task | None:
        # None means not-this-agent's (indistinguishable from not-found,
        # which is the point); an id naming nothing at all raises A2A's
        # own TaskNotFoundError inside the adapter.
        return await self._adapter.get_task(self._agent, params.id)

    async def on_cancel_task(
        self, params: pb.CancelTaskRequest, context: ServerCallContext
    ) -> pb.Task | None:
        # metadata passed through whole — the cancel authority proof
        # (metadata.cancel / resolution / delegation) rides in it.
        return await self._adapter.cancel_task(
            self._agent, params.id, metadata=_metadata(params.metadata)
        )

    async def on_subscribe_to_task(
        self, params: pb.SubscribeToTaskRequest, context: ServerCallContext
    ) -> AsyncGenerator[Event]:
        stream = await self._adapter.resubscribe_task(self._agent, params.id)
        async for event in stream:
            yield event

    # --- deliberately not offered ------------------------------------
    # Push notifications: funduq pushes nothing outward on a caller's
    # behalf. Listing tasks and the extended card are a gateway's to
    # answer if it wants them; this one does not.

    async def on_create_task_push_notification_config(self, params, context):
        raise UnsupportedOperationError

    async def on_get_task_push_notification_config(self, params, context):
        raise UnsupportedOperationError

    async def on_list_task_push_notification_configs(self, params, context):
        raise UnsupportedOperationError

    async def on_delete_task_push_notification_config(self, params, context):
        raise UnsupportedOperationError

    async def on_list_tasks(self, params, context):
        raise UnsupportedOperationError

    async def on_get_extended_agent_card(self, params, context):
        raise UnsupportedOperationError


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
        request_handler=FunduqRequestHandler(A2AAdapter(funduq), agent),
        enable_v0_3_compat=True,
    )
    return await dispatcher.handle_requests(request)
