"""FastAPI dependencies that resolve the running `Funduq` instance.

The Funduq is put on the app (see souk_server.server.create_app) and read
back off the request here, so the HTTP layer holds no module-level state
of its own and two apps in one process can serve two differently-
configured instances.

This module is part of the serving layer, not core — it imports FastAPI.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from funduq.core import Funduq
from funduq.errors import (
    AgentInUse,
    AgentNotFound,
    FunduqError,
    InvalidRegistration,
    InvalidRunInput,
    KyokRejected,
    LlmOfferingInUse,
    LlmProviderNotFound,
    ProviderFingerprintTaken,
    RunNotFound,
    ThreadNotFound,
    ThreadOwnershipMismatch,
    ThreadQueueFull,
)
from funduq.identity import InvalidCancel, InvalidResolution, InvalidView
from funduq.models import AgentRef
from funduq.repo import ThreadMembershipRequired
from funduq_contract import InvalidChain
from souk_server.config import ServingSettings


def get_souk(request: Request) -> Funduq:
    return request.app.state.souk


def get_serving_settings(request: Request) -> ServingSettings:
    return request.app.state.serving_settings


async def get_session(funduq: Funduq = Depends(get_souk)) -> AsyncIterator[AsyncSession]:
    async with funduq.session() as session:
        yield session


# funduq.errors -> HTTP status. One mapping, registered once, rather than
# a try/except in every route: which status a given failure deserves is a
# property of the failure, not of the endpoint that happened to hit it.
# Both protocol surfaces raise the same errors, so writing this per-route
# meant writing it twice and letting the two drift.
_STATUS = {
    AgentNotFound: 404,
    LlmProviderNotFound: 404,
    ThreadNotFound: 404,
    RunNotFound: 404,
    ThreadOwnershipMismatch: 409,
    ProviderFingerprintTaken: 409,
    # Deletion refused while the thing has history or live runs — a
    # conflict with current state, not a bad request.
    AgentInUse: 409,
    LlmOfferingInUse: 409,
    InvalidRegistration: 401,
    # A tampered actor chain (`funduq_contract.InvalidChain`, which
    # replaced core's InvalidActorChain) is refused at the door.
    InvalidChain: 401,
    # The singular acts on a chain-bound run, refused for want of a proof
    # from one of its authorities. They are plain ValueErrors upstream, so
    # without these rows an unproven cancel or an unproven resolve reaches
    # a caller as a 500 — a server fault for a caller mistake, and one
    # that says nothing about what to send instead. `InvalidView` is here
    # for completeness only: the read doors answer an unproven view as
    # absence and never raise it outward (see api_a2a).
    InvalidCancel: 401,
    InvalidResolution: 401,
    InvalidView: 401,
    # Writing to a thread bound to a responsibility segment when you are
    # neither its head nor its serving provider. A bare `Exception`
    # upstream — not even a ValueError — so it is the likeliest of these
    # to reach a caller as a 500. 403 rather than 401: the caller
    # identified itself perfectly well (its chain verified), it simply is
    # not a member of this conversation.
    ThreadMembershipRequired: 403,
    InvalidRunInput: 400,
    # Backpressure: the thread's buffer is full and the request was NOT
    # accepted — say retry, never accept-then-expire. (The A2A door maps
    # this itself, before the JSON-RPC dispatcher can swallow it; this
    # entry covers the AG-UI door.)
    ThreadQueueFull: 429,
}


def _detail(exc: Exception) -> object:
    """Some errors carry more than their message is worth."""
    if isinstance(exc, ThreadNotFound):
        return f"thread '{exc}' not found"
    if isinstance(exc, InvalidChain):
        return f"invalid actor chain: {exc}"
    return str(exc)


def install_error_handlers(app: FastAPI) -> None:
    """Translate funduq's domain errors into responses for this app.

    Serving's job, not core's: an adapter says "no such agent" without
    knowing whether anyone is listening over HTTP. Any host mounting
    these routers needs this (or its own equivalent), or a domain error
    surfaces as a 500.
    """

    async def handle(_request: Request, exc: Exception) -> JSONResponse:
        status = _STATUS.get(type(exc), 500)
        content: dict = {"detail": _detail(exc)}
        if isinstance(exc, KyokRejected):
            status = exc.status
            # The LLM provider's structured refusal, relayed intact for a
            # non-streaming caller the same way the stream relays it
            # in-band — core carries it on the exception so the HTTP
            # layer doesn't flatten it to the detail string.
            if exc.refusal is not None:
                content["error"] = exc.refusal
        return JSONResponse(status_code=status, content=content)

    for error_type in (*_STATUS, KyokRejected, FunduqError):
        app.add_exception_handler(error_type, handle)


async def resolve_ref(funduq: Funduq, provider: str, name: str) -> AgentRef:
    """Turn a `(provider, name)` path pair into the agent it addresses.

    `provider` may be the full public key or its 16-hex fingerprint —
    core tells them apart by length, so one path segment takes either and
    a URL can stay short without giving up the unambiguous form.
    """
    found = await funduq.resolve_agent(provider, name)
    if found is None:
        raise AgentNotFound(f"no agent '{name}' under provider '{provider}'")
    return AgentRef(provider_key=found.provider_key, name=found.name)
