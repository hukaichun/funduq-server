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
from collections.abc import AsyncIterator
from typing import Any

import httpx
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
