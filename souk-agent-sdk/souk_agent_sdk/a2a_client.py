"""Minimal streaming A2A client: calls another agent's `SendStreamingMessage`
and yields each `StreamResponse` as it arrives. Used by agent-template's
sub-agent-calling tool so a "main agent" can watch a sub-agent's progress live
instead of only seeing its final result.

Speaks the wire of `a2a-sdk` 1.1 — the same package the gateway now mounts
its A2A door on (`JsonRpcDispatcher`). Measured against a2a-sdk 1.1.2's own
`JsonRpcTransport`: the JSON-RPC method names are the gRPC service's
(`SendMessage`, `SendStreamingMessage`, `GetTask`, ...), which protocol
version a request speaks rides the `A2A-Version` HTTP header (no header
means 0.3 — the dispatcher's v0.3 compat would still answer, but with
v0.3's `message/send` vocabulary; this client says `1.0` and speaks 1.0),
`contextId`/`taskId` travel on the message rather than beside it, a text
part is a bare `{"text": ...}` with no discriminator, and each streamed
item is a `StreamResponse` whose single key says what it is
(`statusUpdate` / `artifactUpdate` / `task` / `msg`).

This file deliberately does *not* import a2a-sdk: it is a 20-line JSON-RPC
POST, and making a provider SDK carry protobuf to send one would cost more
than it protects. What protects it instead is the souk end, which mounts
the SDK's own dispatcher and would reject these shapes if they drifted.
"""

from __future__ import annotations

import hashlib
import json
import secrets
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
from funduq_provider_sdk import ProviderIdentity
from httpx_sse import aconnect_sse

# a2a-sdk 1.1's version negotiation: the header names the protocol the
# request speaks, and absence means 0.3. Constants mirrored from
# a2a.utils.constants (VERSION_HEADER / PROTOCOL_VERSION_CURRENT) rather
# than imported — see the module docstring for why a2a-sdk itself is not
# a dependency here.
A2A_VERSION_HEADER = "A2A-Version"
A2A_PROTOCOL_VERSION = "1.0"

# funduq's declared A2A extension for interjection: a request whose
# metadata carries this key asks to join the named run's turn in flight,
# rather than opening the next turn. It rides the *request's* metadata,
# beside `actorChain` — funduq has read nothing from a message's own
# metadata since contract revision 20 — and souk relays it to the
# addressed agent as `forwardedProps.funduq.addressedRunId` (revision 18
# moved funduq's own keys under one `funduq` key so a caller's bag and
# funduq's own can never be mistaken for each other).
INTERJECTION_EXTENSION_URI = "https://github.com/hukaichun/funduq/ext/interjection/v1"
ADDRESSED_RUN_METADATA_KEY = f"{INTERJECTION_EXTENSION_URI}/addressedRunId"


# Contract revision 21: a door that cannot say *who presented* a request
# must refuse a chain presented at it (`PresenterRequired`). The chain
# itself proves who signed each hop; it does not prove that the party on
# the far end of this connection is the one whose key ends it, and reading
# the presenter off the chain would make that check a tautology — the
# caller-impersonation hole upstream closed. So the presenter authenticates
# itself at the transport, and souk's `presenter_key_of` reads it here.
#
# The header is `Funduq-Presenter` — no `X-` prefix, deprecated by RFC 6648
# in 2012 — carrying compact JSON `{"publicKey", "timestamp", "signature"}`,
# the same three fields as every other proof in this system.
PRESENTER_HEADER = "Funduq-Presenter"

# The domain tag is *ours*, deliberately. `funduq-*` is upstream's
# namespace and funduq_contract publishes no builder for authenticating a
# presenter — squatting the namespace would mint a name that looks
# canonical and is not. When upstream ships one, this constant and
# `presenter_payload` below are what gets deleted.
PRESENTER_DOMAIN = "funduq-server-presenter"

# The freshness window the far side enforces, restated here only so a
# caller knows how stale a pre-signed header may be. Modelled on the
# cancel/view family and on `kyok_call_payload`, which likewise binds a
# body hash.
PRESENTER_FRESHNESS_SECONDS = 60


class PresenterIdentityRequired(ValueError):
    """A chain was passed with no identity to present it with.

    Raised here rather than sent: souk refuses a chain from a caller it
    cannot authenticate, so this request has a known answer, and a local
    error naming the missing half beats a 401 three layers away.
    """


def presenter_payload(public_key: str, timestamp: int, body_hash: str) -> bytes:
    """The exact bytes a presenter signs:

        funduq-server-presenter:{public_key}:{timestamp}:{sha256hex(body)}

    Each of the three earns its place. The **body hash** binds the proof
    to *this* request, so a captured header cannot be replayed onto a
    different call. The **timestamp** bounds replay of the same request to
    the freshness window. The **public key inside the payload** makes the
    proof name the key it claims, so it cannot answer a different question.

    `body_hash` is the lowercase hex sha256 of the request bytes actually
    put on the wire — which is why every send here serializes once and
    posts those same bytes, rather than letting httpx re-serialize.
    """
    return f"{PRESENTER_DOMAIN}:{public_key}:{timestamp}:{body_hash}".encode()


def presenter_proof(
    identity: ProviderIdentity, body: bytes, *, timestamp: int | None = None
) -> dict[str, Any]:
    """The `{publicKey, timestamp, signature}` proof that this identity is
    the party presenting `body`, now."""
    timestamp = int(time.time()) if timestamp is None else timestamp
    body_hash = hashlib.sha256(body).hexdigest()
    return {
        "publicKey": identity.public_key,
        "timestamp": timestamp,
        "signature": identity.sign(
            presenter_payload(identity.public_key, timestamp, body_hash)
        ),
    }


def presenter_headers(
    identity: ProviderIdentity | None, body: bytes, *, timestamp: int | None = None
) -> dict[str, str]:
    """`presenter_proof` as the header a request carries. No identity, no
    header — an anonymous caller, which is exactly what a call with no
    chain is, and souk keeps serving it."""
    if identity is None:
        return {}
    proof = presenter_proof(identity, body, timestamp=timestamp)
    return {PRESENTER_HEADER: json.dumps(proof, separators=(",", ":"))}


def _presented(
    body: dict[str, Any],
    identity: ProviderIdentity | None,
    *,
    actor_chain_present: bool,
) -> tuple[bytes, dict[str, str]]:
    """One JSON-RPC body, serialized once, with the presenter header over
    exactly those bytes.

    Serializing here and posting `content=` is the load-bearing detail: a
    signature over bytes httpx would go on to produce differently is a
    signature over nothing.
    """
    if actor_chain_present and identity is None:
        raise PresenterIdentityRequired(
            "actor_chain was given but identity was not: souk refuses a chain "
            "from a caller it cannot authenticate (PresenterRequired), so this "
            "call would be rejected. Pass identity= — the same ProviderIdentity "
            f"whose key ends the chain — so the {PRESENTER_HEADER} header can "
            "be signed."
        )
    raw = json.dumps(body, separators=(",", ":")).encode()
    headers = {
        A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION,
        "Content-Type": "application/json",
        **presenter_headers(identity, raw),
    }
    return raw, headers


def new_request_id() -> str:
    """A JSON-RPC request id, which is all this is. It used to mint a *task*
    id, back when the caller assigned one; the current spec has nowhere on
    the wire to put a caller-chosen task id, so the name was a leftover
    claiming something no longer true."""
    return f"req_{secrets.token_hex(12)}"


async def call_agent_streaming(
    a2a_rpc_url: str,
    message_text: str,
    *,
    request_id: str | None = None,
    context_id: str | None = None,
    task_id: str | None = None,
    addressed_run_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    actor_chain: list[str] | None = None,
    identity: ProviderIdentity | None = None,
    reference_task_ids: list[str] | None = None,
    timeout: float = 120.0,
) -> AsyncIterator[dict[str, Any]]:
    """`actor_chain`, if given, proves this call's identity (and, for a
    multi-hop chain, who it's ultimately acting on behalf of) to the
    callee's souk — see souk_agent_sdk.identity's new_actor_chain /
    extend_actor_chain (delegating to funduq_contract) for how to build
    one. Entirely optional: souk doesn't require callers to authenticate.
    It rides the *request*-level metadata as `actorChain`, which is where
    the gateway's adapter reads it.

    `identity` is the `ProviderIdentity` **presenting** this call — the one
    whose key ends `actor_chain`. Since contract revision 21 souk refuses a
    chain from a caller it cannot authenticate, so a chain without an
    identity is a request with a known answer and this client raises
    `PresenterIdentityRequired` instead of sending it. Given, the identity
    signs the `Funduq-Presenter` header over the exact bytes of this
    request. Without a chain it is optional: a chainless caller stays
    anonymous, which souk serves unchanged.

    `context_id`, if given, is real A2A (`Message.contextId` — the
    caller passes back whatever `contextId` it was returned on an
    earlier call to the same callee, per the spec's own session-
    continuation convention) to continue talking to the same callee
    thread. Omit it (the default) to always start a fresh one — lineage
    and continuity are orthogonal, a caller must opt into continuity
    explicitly.

    `task_id`, if given, is `Message.taskId` — how A2A addresses an
    existing task, and how souk addresses a *resume*: a paused run
    (`input-required`) is answered by sending the follow-up message with
    the paused run's id here.

    `addressed_run_id`, if given, declares an *interjection*: this
    message wants into the named run's turn while it is still in flight
    (distinct from a resume, which answers a run that paused). It rides
    under funduq's declared extension key
    (`ADDRESSED_RUN_METADATA_KEY`) on the *request's* metadata, beside
    `actorChain` — funduq reads nothing from a message's own metadata
    since revision 20 — and souk relays it to the agent as
    `forwardedProps.funduq.addressedRunId`.

    `reference_task_ids`, if given, is real A2A (`Message.referenceTaskIds`
    — "a list of other task IDs that this message references for
    additional context"): pass the caller's own current task id (e.g. its
    own run_id) to let souk record the lineage so a thread tree can show
    what a top-level call actually fanned out to. Purely informational
    per the A2A spec — it never implies session continuity; use
    `context_id` for that.
    """
    request_id = request_id or new_request_id()
    params = _send_message_params(
        message_text,
        context_id=context_id,
        task_id=task_id,
        addressed_run_id=addressed_run_id,
        metadata=metadata,
        actor_chain=actor_chain,
        reference_task_ids=reference_task_ids,
    )
    body = {"jsonrpc": "2.0", "id": request_id, "method": "SendStreamingMessage", "params": params}
    raw, headers = _presented(body, identity, actor_chain_present=actor_chain is not None)

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with aconnect_sse(
            client,
            "POST",
            a2a_rpc_url,
            content=raw,
            headers=headers,
        ) as event_source:
            async for sse in event_source.aiter_sse():
                payload = json.loads(sse.data)
                result = payload.get("result")
                if result is not None:
                    yield result


def _send_message_params(
    message_text: str,
    *,
    context_id: str | None = None,
    task_id: str | None = None,
    addressed_run_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    actor_chain: list[str] | None = None,
    reference_task_ids: list[str] | None = None,
) -> dict[str, Any]:
    """The `SendMessageRequest` params both send paths share — see
    `call_agent_streaming`'s docstring for what each argument means."""
    metadata = dict(metadata) if metadata else {}
    if actor_chain is not None:
        metadata["actorChain"] = actor_chain
    if addressed_run_id:
        # Request level, beside `actorChain`. It used to ride the message's
        # own metadata; funduq has read nothing from there since revision
        # 20, so the old location was a declaration nobody heard.
        metadata[ADDRESSED_RUN_METADATA_KEY] = addressed_run_id

    # v1.0 `Part` is a oneof, so the field name is the type — no `kind`, no
    # `type`. Role gained its enum prefix in the same move.
    message: dict[str, Any] = {
        "messageId": f"msg_{secrets.token_hex(12)}",
        "role": "ROLE_USER",
        "parts": [{"text": message_text}],
    }
    if reference_task_ids:
        message["referenceTaskIds"] = reference_task_ids
    if context_id:
        message["contextId"] = context_id
    if task_id:
        message["taskId"] = task_id

    params: dict[str, Any] = {"message": message}
    if metadata:
        params["metadata"] = metadata
    return params


async def _rpc(
    a2a_rpc_url: str,
    method: str,
    params: dict[str, Any],
    *,
    request_id: str | None = None,
    identity: ProviderIdentity | None = None,
    actor_chain_present: bool = False,
    timeout: float = 120.0,
) -> Any:
    """One JSON-RPC call, returning its `result` (which may be absent — a
    read the callee will not answer to this reader answers nothing, and
    that is the designed answer, not an error to raise)."""
    body = {
        "jsonrpc": "2.0",
        "id": request_id or new_request_id(),
        "method": method,
        "params": params,
    }
    raw, headers = _presented(body, identity, actor_chain_present=actor_chain_present)
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(a2a_rpc_url, content=raw, headers=headers)
        response.raise_for_status()
        payload = response.json()
    if payload.get("error") is not None:
        raise RuntimeError(f"{method} failed: {payload['error']}")
    return payload.get("result")


async def call_agent(
    a2a_rpc_url: str,
    message_text: str,
    *,
    request_id: str | None = None,
    context_id: str | None = None,
    task_id: str | None = None,
    addressed_run_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    actor_chain: list[str] | None = None,
    identity: ProviderIdentity | None = None,
    reference_task_ids: list[str] | None = None,
    return_immediately: bool = False,
    history_length: int | None = None,
    timeout: float = 120.0,
) -> dict[str, Any] | None:
    """The non-streaming half — `SendMessage`, answered with the settled
    `Task`. Every argument of `call_agent_streaming` means the same thing
    here, plus the two `SendMessageConfiguration` fields the gateway
    honours (the other two it deliberately does not):

    `return_immediately` answers with the Task as it *stands* rather than
    waiting for it to settle. souk's queued lane makes `submitted` a state
    with real duration, so this is how a polling caller learns that is
    where its run is, instead of blocking on a run nobody has claimed yet.

    `history_length` caps how many messages come back on the Task. These
    ride `configuration` only on `SendMessage`: A2A's streaming send has
    no place for them, since a stream is already incremental.
    """
    params = _send_message_params(
        message_text,
        context_id=context_id,
        task_id=task_id,
        addressed_run_id=addressed_run_id,
        metadata=metadata,
        actor_chain=actor_chain,
        reference_task_ids=reference_task_ids,
    )
    configuration: dict[str, Any] = {}
    if return_immediately:
        configuration["returnImmediately"] = True
    if history_length is not None:
        configuration["historyLength"] = history_length
    if configuration:
        params["configuration"] = configuration
    return await _rpc(
        a2a_rpc_url,
        "SendMessage",
        params,
        request_id=request_id,
        identity=identity,
        actor_chain_present=actor_chain is not None,
        timeout=timeout,
    )


async def get_task(
    a2a_rpc_url: str,
    task_id: str,
    *,
    identity: ProviderIdentity | None = None,
    history_length: int | None = None,
    request_id: str | None = None,
    timeout: float = 30.0,
) -> dict[str, Any] | None:
    """Read one task.

    **Absence arrives as A2A's own `TaskNotFound` (-32001), raised**, not
    as `None`: the JSON-RPC dispatcher turns a null result into that
    error, which is the point — a reader who may not see a task gets the
    answer a task that never existed gets. `None` is reachable only if a
    door ever answers a null result some other way; handle both.

    Pass `identity` — this provider's own `ProviderIdentity` — for any run
    whose thread is bound to an actor chain: a read is answered as the key
    the transport authenticated, and an unauthenticated reader is told
    "not found" whether or not the task exists. Revision 21 folded reads
    and writes into that one hook (`presenter_key_of`), so the header a
    read carries is the same `Funduq-Presenter` a chained send carries —
    there is no separate view proof any more; revision 22 moved only who
    *judges* the key, into the gateway. Any actor on the run's chain is
    inside its read circle, so a provider that delegated work can still
    watch the task it is on the chain of.

    Omitting it is right for an unbound run, which stays as public as its
    funduq-minted id.
    """
    params: dict[str, Any] = {"id": task_id}
    if history_length is not None:
        params["historyLength"] = history_length
    return await _rpc(
        a2a_rpc_url,
        "GetTask",
        params,
        request_id=request_id,
        identity=identity,
        timeout=timeout,
    )


async def resubscribe_task(
    a2a_rpc_url: str,
    task_id: str,
    *,
    identity: ProviderIdentity | None = None,
    request_id: str | None = None,
    timeout: float = 120.0,
) -> AsyncIterator[dict[str, Any]]:
    """Re-attach to a task's event stream (`SubscribeToTask`), yielding
    each `StreamResponse` — the read path for a run already in flight,
    e.g. after a dropped connection. Same presenter rule as `get_task`: a
    bound run streams nothing to a reader the transport could not name."""
    body = {
        "jsonrpc": "2.0",
        "id": request_id or new_request_id(),
        "method": "SubscribeToTask",
        "params": {"id": task_id},
    }
    raw, headers = _presented(body, identity, actor_chain_present=False)
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with aconnect_sse(
            client,
            "POST",
            a2a_rpc_url,
            content=raw,
            headers=headers,
        ) as event_source:
            async for sse in event_source.aiter_sse():
                payload = json.loads(sse.data)
                result = payload.get("result")
                if result is not None:
                    yield result
