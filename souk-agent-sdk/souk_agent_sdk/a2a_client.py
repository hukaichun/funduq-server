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

import json
import secrets
import time
from collections.abc import AsyncIterator
from typing import Any

import httpx
from funduq_contract import view_payload
from funduq_provider_sdk import ProviderIdentity
from httpx_sse import aconnect_sse

# a2a-sdk 1.1's version negotiation: the header names the protocol the
# request speaks, and absence means 0.3. Constants mirrored from
# a2a.utils.constants (VERSION_HEADER / PROTOCOL_VERSION_CURRENT) rather
# than imported — see the module docstring for why a2a-sdk itself is not
# a dependency here.
A2A_VERSION_HEADER = "A2A-Version"
A2A_PROTOCOL_VERSION = "1.0"

# funduq's declared A2A extension for interjection: a message whose
# metadata carries this key asks to join the named run's turn in flight,
# rather than opening the next turn. souk relays it to the addressed
# agent as `forwardedProps.addressedRunId`.
INTERJECTION_EXTENSION_URI = "https://github.com/hukaichun/funduq/ext/interjection/v1"
ADDRESSED_RUN_METADATA_KEY = f"{INTERJECTION_EXTENSION_URI}/addressedRunId"


# Contract revision 13: reading a run whose thread is bound to an actor
# chain demands a view proof, and its absence is answered as "not found"
# — existence is part of what is guarded, so a read that works today
# silently 404s once the callee is on rev 13 unless the proof is sent.
# A2A's read requests carry no caller data, so the proof rides the
# transport; this gateway takes it as a header with compact JSON
# `{"publicKey", "timestamp", "signature"}` (docs/server-mode.md).
#
# The read circle is wider than the act circle: every actor on the run's
# chain may sign a view, while cancel and resolve stay with the head and
# the serving provider.
VIEW_PROOF_HEADER = "X-Funduq-View"


def view_proof(
    identity: ProviderIdentity, run_id: str, *, timestamp: int | None = None
) -> dict[str, Any]:
    """The `{publicKey, timestamp, signature}` proof that this identity
    asks to see `run_id`, now.

    Still the timestamp family, unlike a resolve proof — a view is a
    standing capability rather than an answer to one particular ask, so
    there is nothing instance-shaped to bind it to and freshness has to
    come from the clock (60s window on the far side). The signed bytes
    are `funduq_contract.view_payload(run_id, timestamp)`, imported, not
    restated.
    """
    timestamp = int(time.time()) if timestamp is None else timestamp
    return {
        "publicKey": identity.public_key,
        "timestamp": timestamp,
        "signature": identity.sign(view_payload(run_id, timestamp)),
    }


def view_headers(
    identity: ProviderIdentity | None, run_id: str, *, timestamp: int | None = None
) -> dict[str, str]:
    """`view_proof` as the header a read carries. No identity, no header —
    which reads a bound run as absent, the designed answer, rather than an
    error."""
    if identity is None:
        return {}
    proof = view_proof(identity, run_id, timestamp=timestamp)
    return {VIEW_PROOF_HEADER: json.dumps(proof, separators=(",", ":"))}


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
    the message's metadata under funduq's declared extension key
    (`ADDRESSED_RUN_METADATA_KEY`); souk relays it to the agent as
    `forwardedProps.addressedRunId`.

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

    async with httpx.AsyncClient(timeout=timeout) as client:
        async with aconnect_sse(
            client,
            "POST",
            a2a_rpc_url,
            json=body,
            headers={A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION},
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
    if addressed_run_id:
        message["metadata"] = {ADDRESSED_RUN_METADATA_KEY: addressed_run_id}

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
    headers: dict[str, str] | None = None,
    timeout: float = 120.0,
) -> Any:
    """One JSON-RPC call, returning its `result` (which may be absent —
    a read of a bound run without a valid view proof answers nothing, and
    that is the designed answer, not an error to raise)."""
    body = {
        "jsonrpc": "2.0",
        "id": request_id or new_request_id(),
        "method": method,
        "params": params,
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            a2a_rpc_url,
            json=body,
            headers={A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION, **(headers or {})},
        )
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
        a2a_rpc_url, "SendMessage", params, request_id=request_id, timeout=timeout
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
    """Read one task. `None` means the callee answered absence.

    Pass `identity` — this provider's own `ProviderIdentity` — for any run
    whose thread is bound to an actor chain: since contract revision 13
    such a read demands a view proof, and without one the answer is
    "not found" whether or not the task exists. Any actor on the run's
    chain may sign one, so a provider that delegated work can still watch
    the task it is on the chain of.

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
        headers=view_headers(identity, task_id),
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
    e.g. after a dropped connection. Same view-proof rule as `get_task`:
    a bound run without one streams nothing."""
    body = {
        "jsonrpc": "2.0",
        "id": request_id or new_request_id(),
        "method": "SubscribeToTask",
        "params": {"id": task_id},
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        async with aconnect_sse(
            client,
            "POST",
            a2a_rpc_url,
            json=body,
            headers={
                A2A_VERSION_HEADER: A2A_PROTOCOL_VERSION,
                **view_headers(identity, task_id),
            },
        ) as event_source:
            async for sse in event_source.aiter_sse():
                payload = json.loads(sse.data)
                result = payload.get("result")
                if result is not None:
                    yield result
