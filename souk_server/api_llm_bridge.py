"""KYOK HTTP surface: one route only.

What KYOK *means* — the two-part authorization, the relay between a
provider and whoever is paying for its inference, reassembling streamed
chunks — lives in funduq/protocols/kyok.py, in core. This file lifts
headers off the request, frames the result as SSE or JSON, and maps
`KyokRejected` onto its status (through the app-wide handler).

- `POST /kyok/v1/chat/completions`: what a provider's OpenAI-compatible
  model client actually calls — it looks exactly like a real
  OpenAI-compatible host, which is the whole point of this side. Queues
  the request for the run's bound LLM provider, then blocks *this HTTP
  call* while relaying the answer back.

The SSE framing is this side's now: `CompletionRelay.stream()` yields
OpenAI's own `ChatCompletionChunk`s with a `CompletionFailure` as the
last item if the completion breaks, and no framing at all — the `data:`
lines, the `[DONE]` sentinel, and the error frame are written here,
which is also the only seat that can guarantee `[DONE]` never follows a
failure frame.

The answering side of the relay is `WS /ws/kyok` (souk_server/
ws_kyok.py): the LLM provider the run's binding names, attached over a
socket, with answers accepted only on the connection each request was
delivered to. See docs/server-mode.md for the frames.
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse, StreamingResponse

from funduq.core import Funduq
from funduq.protocols.kyok import CompletionFailure, KyokAdapter
from souk_server.deps import get_souk

logger = logging.getLogger("souk.api_llm_bridge")

router = APIRouter()


def _bearer(request: Request) -> str:
    header = request.headers.get("authorization", "")
    return header[len("Bearer ") :] if header.startswith("Bearer ") else header


@router.post("/kyok/v1/chat/completions", response_model=None)
async def chat_completions(
    request: Request, funduq: Funduq = Depends(get_souk)
) -> StreamingResponse | JSONResponse:
    relay = await KyokAdapter(funduq).complete(
        _bearer(request),
        await request.body(),
        timestamp=request.headers.get("x-souk-kyok-timestamp", ""),
        signature=request.headers.get("x-souk-kyok-signature", ""),
    )
    if not relay.stream_requested:
        # `collapsed` returns the openai package's `ChatCompletion` model —
        # dumped in json mode so what goes on the wire is exactly its
        # serialization, not the dict a model happens to be underneath.
        # A failure raises `KyokRejected`, answered by the app-wide handler
        # with the provider's structured refusal intact.
        return JSONResponse((await relay.collapsed()).model_dump(mode="json"))

    async def sse():
        # A failure here surfaces mid-stream: the response has already
        # started, so there is no status left to change — the caller gets
        # an error frame, and never a [DONE] after it.
        async for item in relay.stream():
            if isinstance(item, CompletionFailure):
                yield f"data: {json.dumps({'error': item.payload})}\n\n"
                return
            yield f"data: {item.model_dump_json(exclude_none=True)}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(sse(), media_type="text/event-stream")
