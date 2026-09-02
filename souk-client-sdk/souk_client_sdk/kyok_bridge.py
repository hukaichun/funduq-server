"""Optional Keep Your Own Key bridge: lets a souk-client-sdk caller pay
for a run's LLM usage with their own key instead of leaving that to the
agent provider. Purely additive — a caller that never touches this module
is simply not offering KYOK; `SoukClient.run()` alone is a complete,
ordinary caller either way. See upstream funduq's docs (mechanisms/kyok)
for the design, and this repo's docs/server-mode.md for the wire protocol
this speaks.

**This is an LLM provider.** The bridge holds an Ed25519 keypair
(`funduq_provider_sdk.ProviderIdentity`) and connects to `/ws/kyok` with
the wire-v4 ticket handshake:

1. `POST /tickets {"publicKey"}` over HTTP — the admission decision —
   returns a single-use ticket plus funduq's public key.
2. The bridge signs `provider_connect_payload(funduq_key, ticket, nonce)`
   *before* connecting, naming the funduq it means to reach.
3. One `hello` frame carries key, ticket, nonce and proof; funduq answers
   one `welcome` carrying its counter-signature over
   `funduq_connect_payload(ticket, nonce)`. The bridge verifies that
   answer — and any pinned funduq key — before treating the link open
   (`WrongFunduq` otherwise), so an imposter never receives a completion.
4. Registration then happens **on the open link**: a `register` frame
   naming the model offering, answered by `registered`. Nothing is signed
   past the handshake — the key was proved once, when the link opened.
   Every reconnect re-registers, because an open link serves exactly what
   it last published. Withdrawal is a `deleteModel` frame, answered by
   `deleted`.

The caller then opts a run in by naming the offering — `run_metadata()`
builds exactly that.

The transport after the handshake is what it always was: one WebSocket,
completion requests pushed down it (`completionRequest` =
`funduq_provider_sdk.llm.DeliveredCompletion`, camelCase), the real LLM's
chunks streamed back as frames multiplexed by requestId. An answer is
only accepted on the socket its request was delivered to, so a reconnect
starts fresh: completions in flight on a dead socket are failed by souk
immediately rather than retried here.

**Experimental**, same status as before: no state survives a crash; if
this process dies mid-run the run's provider sees errors, with no
retry/resume path on either end.

Uses litellm (https://github.com/BerriAI/litellm) to actually call the
real LLM, so this bridge isn't tied to one provider — model strings are
litellm's own ("anthropic/claude-...", "gemini/...", "openai/...", a
custom OpenAI-compatible `api_base`, ...) and its streaming chunks are
already OpenAI-shaped, which is exactly what souk's relay expects: no
translation layer needed here, just forward what litellm already gives us.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import litellm
import websockets
from funduq_provider_sdk import (
    WrongFunduq,
    funduq_connect_payload,
    new_nonce,
    verify_signature,
)
from funduq_provider_sdk.llm import (
    CompletionHandler,
    CompletionRefused,
    DeliveredCompletion,
    ProviderIdentity,
)
from pydantic import ValidationError

logger = logging.getLogger("souk_client_sdk.kyok_bridge")

WELCOME_TIMEOUT_SECONDS = 10.0

# The frame choreography matches souk_server's handshake; the bytes
# signed are funduq-contract's link-open family (provider_connect_payload
# / funduq_connect_payload), so this package restates no payload. v4 is
# the ticket handshake: the proof is computed before connecting, from the
# ticket and funduq key learned over HTTP.
HANDSHAKE_VERSION = 4


class KyokBridge:
    """One KYOK LLM provider serving one model offering with the caller's
    own key. Typical use is one block:

    ```python
    async with bridge.serving():
        async for event in client.run(agent, msg,
                                      metadata=bridge.run_metadata(ctx)):
            ...
    ```

    (`serve_forever()` remains callable separately for a long-lived bridge
    that outlives any single run; registration is part of every connection
    it opens, not a separate step.)

    `offering` is the model name callers address —
    `(identity.public_key, offering)` is the offering exactly as
    `(provider_key, name)` is an agent. `model`/`api_key`/`api_base` are
    what this bridge actually calls with, litellm-side, and souk never
    sees them.

    `funduq_public_key` pins the funduq this bridge will serve: the key
    `POST /tickets` returns must match it, and the welcome's answer must
    verify under it, else `WrongFunduq`. Unpinned, the bridge trusts the
    key it learned over TLS at ticket time — the proof binds that key
    into the handshake, and the answer proves possession of it.
    """

    def __init__(
        self,
        souk_http_url: str,
        model: str,
        api_key: str,
        *,
        api_base: str | None = None,
        offering: str = "kyok",
        identity: ProviderIdentity | None = None,
        funduq_public_key: str | None = None,
        reconnect_delay: float = 2.0,
        handler: CompletionHandler | None = None,
    ) -> None:
        self.souk_http_url = souk_http_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.api_base = api_base
        self.offering = offering
        # Ephemeral by default: a personal bridge's identity only needs to
        # outlive its runs. Pass a persisted one to keep a stable
        # provider_key across restarts. Remembered which, because it
        # decides cleanup: an ephemeral key can never come back, so the
        # offering registered under it is roster garbage the moment this
        # process exits — `serving()` withdraws it (deleteModel) on the
        # way out, while the socket is still up. A persisted identity
        # keeps its registration, same as an agent provider between
        # connections.
        self._ephemeral = identity is None
        self.identity = identity or ProviderIdentity.generate()
        self.funduq_public_key = funduq_public_key
        self.reconnect_delay = reconnect_delay
        # The interposition point the library guarantees (see the
        # funduq-provider-sdk README): every completion passes through
        # this before any money moves. None means the default litellm call
        # with this bridge's own key; a caller enforcing policy — a spend
        # ceiling, a model allow-list, refusing an actor chain it doesn't
        # recognise — wraps or replaces it, and may raise
        # `CompletionRefused` to answer with a structured refusal that
        # reaches the calling agent intact.
        self.handler = handler
        # Set while a connection is attached and registered (souk's
        # `registered` received), cleared when it drops. `serving()` waits
        # on it; polling code may read it.
        self.attached = asyncio.Event()
        self._outbound: asyncio.Queue | None = None
        self._deleted = asyncio.Event()

    @property
    def _ws_url(self) -> str:
        scheme, netloc, path, _query, _fragment = urlsplit(self.souk_http_url)
        ws_scheme = "wss" if scheme == "https" else "ws"
        return urlunsplit((ws_scheme, netloc, path.rstrip("/") + "/ws/kyok", "", ""))

    async def _fetch_ticket(self) -> tuple[str, str]:
        """One `POST /tickets` — the out-of-band half of the handshake.

        Returns (ticket, funduq_public_key). The ticket is single-use and
        short-lived, so this runs immediately before every connect. The
        returned key is what the proof will name; a pinned key that does
        not match it is refused here, before any socket exists.
        """
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{self.souk_http_url}/tickets",
                json={"publicKey": self.identity.public_key},
            )
            resp.raise_for_status()
            data = resp.json()
        funduq_key = data["funduqPublicKey"]
        if self.funduq_public_key is not None and funduq_key != self.funduq_public_key:
            raise WrongFunduq(
                f"ticket endpoint presented funduq key {funduq_key}, "
                f"but this bridge pinned {self.funduq_public_key}"
            )
        return data["ticket"], funduq_key

    def run_metadata(self, context: Any = None) -> dict[str, Any]:
        """The `metadata` a caller passes to opt a run into this bridge:
        names the offering, and carries `context` — opaque to souk,
        stripped before anything persists, delivered back to this bridge
        on every completion the run makes (a delegated run carries only
        what *its* caller submitted; bindings do not propagate)."""
        kyok: dict[str, Any] = {
            "llmProvider": {
                "providerKey": self.identity.public_key,
                "name": self.offering,
            }
        }
        if context is not None:
            kyok["context"] = context
        return {"kyok": kyok}

    async def deregister(self) -> None:
        """Withdraw this bridge's offering: a `deleteModel` frame on the
        open link, awaiting souk's `deleted`. Registration is unsigned on
        the wire now — the link authenticates — so withdrawal needs the
        link, and there is nothing to send when none is open.

        Best-effort by design: souk answers an `error` frame instead of
        `deleted` while a run is still bound to the offering, and a bridge
        tearing down has nothing useful to do about that — so refusals,
        timeouts and a missing link are logged, not raised. A stale row's
        only cost is roster noise; crashing a clean shutdown over it would
        cost more.
        """
        outbound = self._outbound
        if outbound is None or not self.attached.is_set():
            logger.warning(
                "kyok bridge has no open link to withdraw %r on; leaving the registration",
                self.offering,
            )
            return
        self._deleted.clear()
        outbound.put_nowait({"type": "deleteModel", "name": self.offering})
        try:
            async with asyncio.timeout(WELCOME_TIMEOUT_SECONDS):
                await self._deleted.wait()
        except TimeoutError:
            logger.warning(
                "kyok bridge: no 'deleted' confirmation for %r; leaving the registration",
                self.offering,
            )

    async def serve_forever(self) -> None:
        """Holds the `/ws/kyok` socket and serves every completion souk
        pushes down it — through `handler` if one was given, else calling
        the real LLM via litellm with this bridge's own api_key — and
        streams the response back as frames. Every connection fetches a
        fresh ticket, walks the handshake and re-registers the offering
        (an open link serves exactly what it last published, so a
        reconnect that skipped this would serve nothing), and reconnects
        on a drop. Runs until cancelled; `serving()` below wraps the whole
        lifecycle when the bridge lives alongside the run it serves.
        """
        while True:
            try:
                await self._serve_connection()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "kyok bridge connection lost; reconnecting in %.1fs", self.reconnect_delay
                )
            await asyncio.sleep(self.reconnect_delay)

    @contextlib.asynccontextmanager
    async def serving(self):
        """The whole lifecycle as one block: attached, registered, torn
        down — so \"run a KYOK-backed run\" stops being four manual steps
        with three ways to sequence them wrong.

        ```python
        async with bridge.serving():
            async for event in client.run(agent, msg,
                                          metadata=bridge.run_metadata(ctx)):
                ...
        ```

        Yields once the first attach is confirmed and the offering is
        registered (souk's `registered`), so a run started inside the
        block can't race the roster and fail its kyok binding. On exit an
        ephemeral identity's offering is withdrawn over the still-open
        link, then the serve task is cancelled and awaited; completions
        still in flight die with it, which is the crash-behavior this
        bridge has always documented.
        """
        task = asyncio.create_task(self.serve_forever())
        try:
            waiter = asyncio.create_task(self.attached.wait())
            done, _ = await asyncio.wait(
                {task, waiter}, return_when=asyncio.FIRST_COMPLETED
            )
            if task in done:
                # serve_forever only ends by raising; surface that instead
                # of yielding a bridge that isn't there.
                waiter.cancel()
                raise RuntimeError("kyok bridge failed before attaching") from task.exception()
            yield self
        finally:
            # Before the socket goes down — withdrawal travels on the
            # link now. Only for an identity this bridge minted itself; a
            # persisted one keeps its registration between runs.
            if self._ephemeral:
                await self.deregister()
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    async def _handshake(self, ws, ticket: str, funduq_key: str) -> None:
        """Two frames: hello out, welcome back — the proof was computable
        before connecting, because `POST /tickets` already named funduq's
        key. The welcome's `answer` is funduq's signature over
        `funduq_connect_payload(ticket, nonce)` (role-tagged, so neither
        side's proof reflects as the other's); it is verified before this
        returns, so nothing is ever produced for an imposter."""
        nonce = new_nonce()
        await ws.send(
            json.dumps(
                {
                    "type": "hello",
                    "version": HANDSHAKE_VERSION,
                    "publicKey": self.identity.public_key,
                    "ticket": ticket,
                    "nonce": nonce,
                    "proof": self.identity.sign_connect(funduq_key, ticket, nonce),
                }
            )
        )
        welcome = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if welcome.get("type") != "welcome":
            raise RuntimeError(f"expected welcome, got {welcome!r}")
        answer = welcome.get("answer")
        if not isinstance(answer, str) or not verify_signature(
            funduq_key, answer, funduq_connect_payload(ticket, nonce)
        ):
            raise WrongFunduq(
                "the funduq answering this link-open did not prove the key "
                "it presented at ticket time"
            )

    async def _register_on_link(self, ws) -> None:
        """Publish the offering on the open link and await souk's echo.
        Unsigned — the handshake already proved the key — and answered
        before anything else flows: an unregistered link serves nothing,
        so no completionRequest can precede the `registered`."""
        await ws.send(json.dumps({"type": "register", "models": [self.offering]}))
        reply = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if reply.get("type") == "error":
            raise RuntimeError(f"souk refused the registration: {reply.get('message')!r}")
        if reply.get("type") != "registered" or self.offering not in reply.get("names", []):
            raise RuntimeError(f"expected registered [{self.offering!r}], got {reply!r}")

    async def _serve_connection(self) -> None:
        ticket, funduq_key = await self._fetch_ticket()
        async with websockets.connect(self._ws_url) as ws:
            await self._handshake(ws, ticket, funduq_key)
            await self._register_on_link(ws)

            # Single writer: concurrent completions queue frames here
            # rather than interleaving sends on the socket directly.
            outbound: asyncio.Queue = asyncio.Queue()
            self._outbound = outbound
            self.attached.set()
            writer = asyncio.create_task(self._write_loop(ws, outbound))
            in_flight: set[asyncio.Task] = set()
            try:
                async for raw in ws:
                    frame = json.loads(raw)
                    kind = frame.get("type")
                    if kind == "completionRequest":
                        task = asyncio.create_task(
                            self._serve_one(outbound, frame["requestId"], frame)
                        )
                        in_flight.add(task)
                        task.add_done_callback(in_flight.discard)
                    elif kind == "deleted":
                        self._deleted.set()
                    elif kind == "error":
                        logger.warning("souk rejected a frame: %s", frame)
                    else:
                        logger.warning("unexpected frame from souk, ignoring: %s", frame)
            finally:
                self.attached.clear()
                self._outbound = None
                writer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await writer
                # Answers are only accepted on the socket their request
                # arrived on, and this one is gone — souk is already
                # failing these completions. Stop paying the LLM for
                # output with nowhere to go.
                for task in list(in_flight):
                    task.cancel()

    async def _write_loop(self, ws, outbound: asyncio.Queue) -> None:
        while True:
            await ws.send(json.dumps(await outbound.get()))

    async def _serve_one(self, outbound: asyncio.Queue, request_id: str, frame: dict[str, Any]) -> None:
        """One completion: through the handler (the interposition point),
        streaming its chunks back as `chunk` frames, then `done` — or one
        `error` frame if the call fails, so the waiting provider fails
        fast instead of timing out. A `CompletionRefused` raised by the
        handler puts its structured payload on the error frame, and souk
        relays it to the calling agent intact."""
        # The frame is the declared envelope (`DeliveredCompletion.
        # model_dump(by_alias=True)` on souk's side, actorChain included)
        # plus type/requestId; rebuilding is one validate, not a field
        # mapping. `funduq.kyok.CompletionRequest` and
        # `DeliveredCompletion.from_request` are gone (revision 11) —
        # this model *is* the wire shape, and its `body` is OpenAI's own
        # request TypedDict with extension keys allowed through verbatim.
        #
        # `type` and `requestId` are this transport's own vocabulary, not
        # the envelope's, and every crossing shape forbids unknown fields
        # — so they come off before the validate rather than being handed
        # to a model that would rightly refuse them.
        payload = {
            key: value
            for key, value in frame.items()
            if key not in ("type", "requestId")
        }
        try:
            delivered = DeliveredCompletion.model_validate(payload)
        except ValidationError as e:
            logger.warning("kyok bridge: malformed completionRequest %s: %s", request_id, e)
            outbound.put_nowait(
                {"type": "error", "requestId": request_id, "message": "malformed completionRequest"}
            )
            return
        try:
            stream = self.handler(delivered) if self.handler else self._call_llm(delivered)
            async for chunk in stream:
                outbound.put_nowait(
                    {"type": "chunk", "requestId": request_id, "data": _to_chunk_dict(chunk)}
                )
            outbound.put_nowait({"type": "done", "requestId": request_id})
        except asyncio.CancelledError:
            raise
        except CompletionRefused as e:
            logger.info("kyok bridge refused request_id=%s: %s", request_id, e.refusal)
            outbound.put_nowait(
                {
                    "type": "error",
                    "requestId": request_id,
                    "message": "refused by the LLM provider",
                    "refusal": e.refusal,
                }
            )
        except Exception as e:
            logger.exception("kyok bridge: LLM call failed for request_id=%s", request_id)
            outbound.put_nowait({"type": "error", "requestId": request_id, "message": str(e)})

    async def _call_llm(self, delivered: DeliveredCompletion):
        """The default handler: the real LLM via litellm, on this
        bridge's own key. Async generator, so a wrapping handler can
        iterate it after enforcing its own policy."""
        body = delivered.body
        stream = await litellm.acompletion(
            model=self.model,
            api_key=self.api_key,
            api_base=self.api_base,
            messages=body.get("messages", []),
            tools=body.get("tools") or None,
            temperature=body.get("temperature"),
            stream=True,
        )
        async for chunk in stream:
            yield chunk


def _to_chunk_dict(chunk: Any) -> dict:
    """One litellm streaming chunk as the plain dict souk relays — litellm
    returns pydantic-ish objects whose serialization surface varies by
    version, hence the three paths."""
    if hasattr(chunk, "model_dump"):
        return chunk.model_dump(mode="json")
    if hasattr(chunk, "dict"):
        return chunk.dict()
    return dict(chunk)
