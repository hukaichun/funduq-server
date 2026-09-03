"""WS /ws/kyok: the socket an LLM provider connects out on.

The answering party is an **LLM provider**: a first-class provider kind
with the same Ed25519 identity machinery as an agent provider. It opens
this socket exactly like `/ws/provider` — the v4 ticket handshake, two
frames (see handshake.py), minus `maxConcurrentRuns`, which the
completion relay has no use for — and then publishes its model offerings
*on the open link* with a `register` frame (`{"models": [...],
"metadata"?}`), the mirror of the agent socket's. The old signed
`POST /llm-providers/register` road is gone upstream; the link is the
credential now. `deleteModel` removes an offering's record, refused by
core while a live run is bound to it.

What flows afterwards is the completion relay, inverted from the old
poll: core resolves a run's binding to an attached link per call
(`KyokAdapter.complete`) and calls `complete()` on it; this file writes
that request down the socket as a `completionRequest` frame — the
`DeliveredCompletion` envelope, which now carries `actorChain` — and
feeds `chunk`/`done`/`error` frames back as the `ChatCompletionChunk`
stream core is iterating. One socket serves concurrent completions,
multiplexed by `requestId`.

What survived every redesign, because it was the security fix worth
keeping: **an answer is accepted only on the connection its request was
delivered to.** Membership in this connection's in-flight table — not
anything a frame carries — is what authorizes an answer, so a requestId
is a multiplexing key within the connection that received it, never a
bearer capability on an open endpoint.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket
from openai.types.chat import ChatCompletionChunk

from funduq.errors import FunduqError, InvalidRegistration
from funduq.ids import new_id
from funduq_contract import DeliveredCompletion
from funduq_provider_sdk.llm import CompletionRefused
from souk_server.handshake import WIRE_VERSION
from souk_server.ws_common import (
    POLICY_VIOLATION,
    parse_frame,
    receive_hello,
    write_loop,
)

if TYPE_CHECKING:
    from funduq.core import Funduq

logger = logging.getLogger("souk.ws_kyok")

router = APIRouter()

# The longest a completion waits for the *next* frame of its answer. Not a
# per-completion deadline — a long generation streams for as long as it
# streams — but a gap this long means the provider is gone in a way the
# socket has not noticed, and the agent's HTTP call must fail rather than
# hang on it.
CHUNK_GAP_TIMEOUT_SECONDS = 120.0

# What this socket accepts after the handshake — read by the dispatch and
# published in docs/wire-vectors.json, asserted equal in tests.
INBOUND_FRAME_TYPES = frozenset({"register", "deleteModel", "chunk", "done", "error"})

# Sentinel closing one completion's answer queue.
_DONE = object()


class SocketLLMProvider:
    """`funduq.kyok.ConnectedLLMProvider` with a WebSocket underneath.

    Duck-typed against core's protocol, like `SocketProvider` beside it,
    and asserted against `_PROTOCOL_SURFACE` in the constructor for the
    same reason: an attribute core expects but this forgets would attach
    fine and fail inside the relay, three layers from the cause. Upstream
    withdrew the `CONNECTED_LLM_PROVIDER_ATTRS` list this used to read at
    revision 11 — the models are the single definition now — but the
    failure mode it guarded is unchanged, so the surface is named here.

    Like `SocketProvider`, deliberately exposes no `sign_connect` — the
    key lives on the far side of the socket, and the ticket, nonce and
    proof from the hello go to `attach_llm_provider` explicitly.

    Holds one queue per in-flight completion, keyed by the requestId this
    side minted. That table is connection-scoped on purpose — it *is* the
    binding described in the module docstring.
    """

    # The whole of what core reaches for on this object — `public_key` and
    # `complete`, with no cancel and no abandon (revision 10 withdrew the
    # one verb that was ever proposed for a caller that stopped listening).
    _PROTOCOL_SURFACE = ("public_key", "complete")

    def __init__(self, public_key: str, outbound: asyncio.Queue) -> None:
        missing = sorted(
            a for a in self._PROTOCOL_SURFACE if not hasattr(type(self), a)
        )
        if missing:
            raise TypeError(
                f"{type(self).__name__} is not a ConnectedLLMProvider: missing {missing}"
            )
        self._public_key = public_key
        self._outbound = outbound
        self._answers: dict[str, asyncio.Queue[Any]] = {}

    @property
    def public_key(self) -> str:
        return self._public_key

    def complete(self, request: DeliveredCompletion) -> AsyncIterator[ChatCompletionChunk]:
        """Write `request` to the wire and return the stream of its answer.

        The frame goes out here, not in the generator, so the request is
        on the wire the moment core holds the iterator — before anything
        awaits it.

        **The request IS the wire shape.** Since contract revision 11 core
        hands over the published `DeliveredCompletion` itself — its own
        `CompletionRequest` and the SDK's `from_request` translation are
        both gone — so this dumps what it was given (`by_alias=True`,
        `actorChain` included) and there is no mapping either end can get
        wrong. Its `body` is validated as OpenAI's own request shape on the
        way in, at the door: a body that is not a chat-completion request
        is a 400 from `KyokAdapter.complete` and never reaches this socket.
        """
        request_id = new_id("kyokreq")
        queue: asyncio.Queue[Any] = asyncio.Queue()
        self._answers[request_id] = queue
        self._outbound.put_nowait(
            {
                "type": "completionRequest",
                "requestId": request_id,
                **request.model_dump(by_alias=True, mode="json"),
            }
        )
        return self._answer_stream(request_id, queue)

    async def _answer_stream(
        self, request_id: str, queue: asyncio.Queue[Any]
    ) -> AsyncIterator[ChatCompletionChunk]:
        """One completion's answer, frame by frame. Raising is how this
        side fails the completion — core's `CompletionRelay` turns it into
        a 502 or an in-band error, so nothing here needs to know which
        shape the caller asked for. A chunk that is not a valid
        `ChatCompletionChunk` fails the same way: what an LLM provider
        returns is untrusted input, and core relays what this yields
        as-is.
        """
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), CHUNK_GAP_TIMEOUT_SECONDS)
                except asyncio.TimeoutError:
                    raise RuntimeError(
                        "LLM provider stopped answering mid-completion"
                    ) from None
                if item is _DONE:
                    return
                if isinstance(item, Exception):
                    raise item
                yield ChatCompletionChunk.model_validate(item)
        finally:
            self._answers.pop(request_id, None)

    def feed(self, request_id: str, item: Any) -> bool:
        """Route one inbound frame's payload to its completion. False if
        this connection was never delivered `request_id` (or it is over) —
        the refusal that used to be an open door."""
        queue = self._answers.get(request_id)
        if queue is None:
            return False
        queue.put_nowait(item)
        return True

    def fail_pending(self) -> None:
        """The socket is gone: a truncated answer must fail its
        completion, not complete it, and fail it now rather than at the
        gap timeout."""
        for queue in self._answers.values():
            queue.put_nowait(RuntimeError("LLM provider disconnected mid-response"))
        self._answers.clear()


def _hello_error(hello: dict[str, Any]) -> str | None:
    """What an LLM provider's hello must carry. Same checks and the same
    ordering rationale as the provider socket's — version first, by name —
    minus `maxConcurrentRuns`, which this socket does not speak."""
    version = hello.get("version")
    if version != WIRE_VERSION:
        if version is None:
            return (
                "hello has no version: this souk speaks wire "
                f"v{WIRE_VERSION}, the ticket handshake"
            )
        return f"unsupported wire version {version!r}; this souk speaks v{WIRE_VERSION}"
    if not isinstance(hello.get("publicKey"), str) or not hello["publicKey"]:
        return "hello needs a publicKey"
    if not isinstance(hello.get("ticket"), str) or not hello["ticket"]:
        return "hello needs a ticket — POST /tickets issues one"
    if not isinstance(hello.get("nonce"), str) or not hello["nonce"]:
        return "hello needs a nonce"
    if not isinstance(hello.get("proof"), str) or not hello["proof"]:
        return "hello needs a proof signed over the ticket"
    return None


@router.websocket("/ws/kyok")
async def kyok_socket(websocket: WebSocket) -> None:
    funduq: "Funduq" = websocket.app.state.souk

    await websocket.accept()
    hello = await receive_hello(websocket)
    if hello is None:
        return

    problem = _hello_error(hello)
    if problem:
        await websocket.close(code=POLICY_VIOLATION, reason=problem)
        return

    public_key = hello["publicKey"]
    outbound: asyncio.Queue = asyncio.Queue()
    link = SocketLLMProvider(public_key, outbound)
    try:
        answer = await funduq.attach_llm_provider(
            link,
            ticket=hello["ticket"],
            provider_nonce=hello["nonce"],
            proof=hello["proof"],
        )
    except (InvalidRegistration, ValueError) as e:
        await websocket.close(code=POLICY_VIOLATION, reason=str(e))
        return

    # funduq's half of the handshake, relayed — see ws_provider for why it
    # is queued before the read loop can process anything. No completion
    # can precede it either: an attached link serving no offerings resolves
    # nothing until its first register frame.
    outbound.put_nowait(
        {
            "type": "welcome",
            "funduqPublicKey": funduq.identity_public_key,
            "answer": answer,
        }
    )

    writer = asyncio.create_task(write_loop(websocket, outbound))
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                break
            frame = parse_frame(message)
            if frame is None:
                outbound.put_nowait({"type": "error", "message": "unparseable frame"})
                continue
            ftype = frame.get("type")
            if ftype not in INBOUND_FRAME_TYPES:
                outbound.put_nowait(
                    {"type": "error", "message": f"unknown frame type {ftype!r}"}
                )
                continue
            if ftype == "register":
                models = frame.get("models")
                metadata = frame.get("metadata")
                if not (
                    isinstance(models, list)
                    and models
                    and all(isinstance(m, str) and m for m in models)
                ):
                    outbound.put_nowait(
                        {"type": "error", "message": "register needs a non-empty 'models' list of strings"}
                    )
                    continue
                if metadata is not None and not isinstance(metadata, dict):
                    outbound.put_nowait(
                        {"type": "error", "message": "metadata must be an object"}
                    )
                    continue
                try:
                    registered = await funduq.register_llm_providers(
                        link, models, metadata
                    )
                except (FunduqError, ValueError) as e:
                    outbound.put_nowait({"type": "error", "message": str(e)})
                else:
                    outbound.put_nowait(
                        {"type": "registered", "names": sorted(registered)}
                    )
                continue
            if ftype == "deleteModel":
                name = frame.get("name")
                if not isinstance(name, str) or not name:
                    outbound.put_nowait(
                        {"type": "error", "message": "deleteModel needs a name"}
                    )
                    continue
                try:
                    await funduq.delete_llm_offering(link, name)
                except FunduqError as e:
                    outbound.put_nowait({"type": "error", "name": name, "message": str(e)})
                else:
                    outbound.put_nowait({"type": "deleted", "name": name})
                continue

            request_id = frame.get("requestId")
            if not isinstance(request_id, str):
                outbound.put_nowait(
                    {"type": "error", "message": "frame needs a requestId"}
                )
                continue
            if ftype == "chunk":
                data = frame.get("data")
                if not isinstance(data, dict):
                    outbound.put_nowait(
                        {
                            "type": "error",
                            "requestId": request_id,
                            "message": "chunk data must be an object",
                        }
                    )
                    continue
                accepted = link.feed(request_id, data)
            elif ftype == "done":
                accepted = link.feed(request_id, _DONE)
            else:  # error: the provider failing fast beats the gap timeout
                # A `refusal` dict rides the envelope core relays intact
                # (`CompletionRelay` reads it duck-typed off the
                # exception) — the provider's structured answer reaches
                # the calling agent instead of flattening to prose. The
                # vocabulary inside is the two roles' own; nothing here
                # interprets it.
                refusal = frame.get("refusal")
                accepted = link.feed(
                    request_id,
                    CompletionRefused(refusal)
                    if isinstance(refusal, dict)
                    else RuntimeError(
                        frame.get("message") or "LLM provider reported an error"
                    ),
                )
            if not accepted:
                # Not delivered on this socket, or already over — see the
                # module docstring for why membership is the check.
                outbound.put_nowait(
                    {
                        "type": "error",
                        "requestId": request_id,
                        "message": "no such in-flight completion on this connection",
                    }
                )
    finally:
        # Connection-scoped, so the teardown of a replaced socket cannot
        # take down the link that already re-attached and took over the
        # offering.
        funduq.detach_llm_provider(public_key, link)
        link.fail_pending()
        writer.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await writer
