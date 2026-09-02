"""The WebSocket a provider reaches souk on — transport, and nothing else.

What a provider *is* comes from `funduq_provider_sdk` (PyPI): the identity
and what it signs, the port an agent satisfies, and the loop that runs the
work. This module is the carrier for exactly that, and the split is the
point — `ProviderRuntime` never learns it is on a wire, and this file
never learns what an agent does.

Two frames carry the hand-over, because souk hands work over rather than
being asked for it: a `run` arrives, and the `ack` this side sends back
is the runtime's own answer to whether it took it. Declining is how a
full provider says so, and the run stays souk's problem — which is the
only channel capacity has, and why nothing here counts anything.

Nothing bearer-shaped is involved, and nothing replayable either. The
link opens on upstream's ticket handshake (funduq's
docs/provider-link.md is the settled design; the gateway repo's
docs/server-mode.md is the frame spec): the provider fetches a
single-use ticket over HTTP
(`POST /tickets`), signs a proof that *names the funduq it means to
reach*, and connects with two frames — hello, welcome. The welcome
carries funduq's counter-signature over the ticket and this side's
nonce, and it is verified before the link is treated as open. Then the
roster goes up on the authenticated link itself: `register` is unsigned,
because the key was proved once when the link opened.

Pass `souk_public_key` to say which souk this provider will talk to. Left
unset, the provider still binds its proof to whatever key the `/tickets`
answer presented (learned over TLS) and verifies the welcome under that
same key — enough to notice a broken or mid-handshake-substituted souk,
not enough to notice one substituted before the ticket was fetched.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import secrets
import ssl
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets
from pydantic import BaseModel, ValidationError
from funduq_provider_sdk import (
    AgentHandle,
    DeliveredRun,
    FunduqLink,
    HandleProvider,
    ProviderIdentity,
    ProviderRuntime,
    Refusal,
    WrongFunduq,
    funduq_connect_payload,
    new_nonce,
    verify_signature,
)


logger = logging.getLogger("souk_agent_sdk")

# How long to wait for `welcome` (and for `registered`) before giving up
# and reconnecting.
WELCOME_TIMEOUT_SECONDS = 10.0


# The handshake this side speaks. The frame choreography must match the
# gateway's (docs/server-mode.md is the spec of record); the *bytes
# signed* come from funduq-contract via funduq_provider_sdk
# (`provider_connect_payload` / `funduq_connect_payload`), so this
# package restates no payload. v4 is the ticket handshake: the old
# four-frame challenge/proof exchange collapsed to hello/welcome because
# the ticket — fetched out-of-band over `POST /tickets` — *is* the
# verifier-chosen challenge, and registration moved onto the open link.
HANDSHAKE_VERSION = 4

# How long an agent waits for souk to answer a question. Generous: it is a
# database read on the far side of a socket, and the failure it guards is
# a lost frame, not a slow one.
QUERY_TIMEOUT_SECONDS = 30.0


def dump_envelope(shape: BaseModel) -> dict[str, Any]:
    """A crossing shape (`DeliveredRun`, `Registration`, …) as its frame.

    `by_alias=True` and **never** `exclude_none`. `RunAgentInput` has
    required fields that are legitimately null — `state`,
    `forwardedProps` — and a stripped envelope fails `model_validate` on
    the far side, which turns a perfectly good run into a *permanent*
    refusal. The withdrawn codec used to enforce this; nothing upstream
    does now, so it is stated here, once, and tested.
    """
    return shape.model_dump(by_alias=True, mode="json")


def dump_event(event: Any) -> Any:
    """One AG-UI event as it goes on the wire — `exclude_none=True`, the
    opposite rule to `dump_envelope` above and just as load-bearing.

    An AG-UI event is *relayed to the caller's stream*, and AG-UI's
    optional fields (`timestamp`, `rawEvent`) dumped as explicit nulls
    are injected into somebody else's event stream. Both halves pull
    opposite ways, which is exactly why neither is left to a caller.

    Anything that is not a pydantic model — a plain dict from an agent
    that builds its own events — is passed through untouched.
    """
    if isinstance(event, BaseModel):
        return event.model_dump(by_alias=True, exclude_none=True, mode="json")
    return event


class SoukIdentityMismatch(WrongFunduq):
    """The souk at this URL is not the one this provider was told to trust.

    Raised rather than logged. The whole value of pinning a key is that a
    substituted souk is refused, and a provider that carried on after
    noticing would be pinning nothing. A subclass of upstream's
    `WrongFunduq`, because that is exactly what it reports — the funduq
    answering this link-open did not prove the key we hold it to.
    """


class SoukQueryFailed(Exception):
    """A question this provider asked souk did not come back.

    Raised rather than answered with an empty list, and the distinction is
    not pedantic: `thread_messages` returning `[]` is a real answer — a
    thread with nothing in it — and a caller that cannot tell that from
    "the socket died" will summarise an empty history as if it were the
    conversation.
    """


class SoukProvider(FunduqLink):
    """One identity, its agents, and the socket between them and souk.

    A `FunduqLink`, because over a wire this object genuinely is both
    directions: run frames arrive on the same socket that event frames
    leave by. The gateway's own `SocketProvider` is deliberately *not* one
    — it holds no runtime and only carries work outward.

    Constructing this attaches it to the runtime, so it must exist before
    the runtime is given work: events produced while the runtime has no
    link are dropped by design.
    """

    def __init__(
        self,
        souk_http_url: str,
        agents: list[AgentHandle],
        reconnect_delay: float = 2.0,
        max_concurrent_runs: int | None = None,
        identity_key_path: str = "souk_identity.key",
        ca_cert_path: str | None = None,
        provider_name: str | None = None,
        souk_public_key: str | None = None,
    ) -> None:
        # One URL is the whole address: the ticket endpoint and the work
        # socket are the same listener, scheme swapped for the second.
        self.souk_http_url = souk_http_url.rstrip("/")
        self.reconnect_delay = reconnect_delay
        self.ca_cert_path = ca_cert_path
        self.provider_name = provider_name
        # Which souk this provider will talk to, as a hex Ed25519 public
        # key. None means "whichever key the /tickets answer presents" —
        # the proof still names that key and the welcome is still verified
        # under it, so a swap *during* the handshake is caught either way.
        self.souk_public_key = souk_public_key
        # An instance attribute rather than derived on every use, so a
        # test that must split the two listeners can point it elsewhere.
        self._ws_url = self._derive_ws_url(self.souk_http_url)
        # `load_or_create` does not make the directory, and a provider's
        # key path is very often one it owns alone (`/data/…` on a fresh
        # volume, `./keys/…` in a checkout) — so this is the one thing
        # done before handing the path over.
        Path(identity_key_path).parent.mkdir(parents=True, exist_ok=True)
        self.identity = ProviderIdentity.load_or_create(identity_key_path)
        self.agents = {agent.name: agent for agent in agents}
        self._outbound: asyncio.Queue = asyncio.Queue()
        # Questions asked and not yet answered, by queryId. The only
        # per-request state on this side, and the reason the wire needs a
        # correlation id at all: everything else here is fire-and-forget.
        self._pending: dict[str, asyncio.Future] = {}
        # The provider's own loop, which knows nothing about any of this.
        # It reports through `self` — the link — and setting that is this
        # constructor's job, not the runtime's.
        self.runtime = ProviderRuntime(
            self.identity,
            HandleProvider(list(agents)),
            max_concurrent_runs=max_concurrent_runs,
        )
        self.runtime.link = self

    # ---- souk → provider

    @property
    def public_key(self) -> str:
        return self.identity.public_key

    @property
    def max_concurrent_runs(self) -> int | None:
        return self.runtime.max_concurrent_runs

    async def deliver(self, run: DeliveredRun) -> bool | Refusal:
        """souk hands a run down. Never called on this side — a socket
        provider is handed work by a `run` frame, which `_offer` below
        turns into `runtime.deliver`. It exists because `FunduqLink` names
        both directions and this is the half a wire routes differently.

        Three-valued, as upstream's link is: `True` accepted, `False` a
        transient decline (full right now — the run stays souk's), and a
        `Refusal` a permanent one.
        """
        return await self.runtime.deliver(run)

    def takes_interjections(self, agent_name: str) -> bool:
        """Whether the named agent takes interjections — a **method**, per
        contract revision 12's runtime. Read as an attribute it would be a
        bound method, which is truthy, which would declare every agent
        interjection-capable and never fail loudly."""
        return self.runtime.takes_interjections(agent_name)

    async def cancel(self, run_id: str) -> bool:
        """souk is asking for a run to stop. A request, and this provider
        complies — the runtime cancels the task, which is the only way to
        interrupt an arbitrary async generator. Reached from the `cancel`
        frame; souk never calls it directly on this side of a wire.

        The `True` is a receipt that the ask arrived, never an outcome:
        the run ends when the agent's generator ends, and `finish` is what
        says so. Returning `None` here logs a warning on every cancel.
        """
        self.runtime.cancel(run_id)
        return True

    # ---- provider → souk

    async def report_event(self, run_id: str, event: Any) -> None:
        self._outbound.put_nowait(
            {"type": "event", "runId": run_id, "event": dump_event(event)}
        )

    async def finish_run(self, run_id: str) -> None:
        self._outbound.put_nowait({"type": "finish", "runId": run_id})

    async def thread_messages(
        self, thread_id: str, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """This thread's messages, oldest first — the one thing a provider
        cannot work out for itself.

        What arrives in `run_input` is exactly what the *caller* sent for
        this run: an AG-UI client resends its whole history every turn,
        while A2A's `message/send` carries one message. The same agent
        cannot tell a tenth turn from a first, and souk has held the
        thread all along.

        `limit` is sent rather than applied on return. The parameter
        exists to keep the response frame bounded, and trimming after
        receiving would have already put a months-old thread on the wire.
        """
        query_id = secrets.token_hex(8)
        loop = asyncio.get_running_loop()
        waiter: asyncio.Future = loop.create_future()
        self._pending[query_id] = waiter
        self._outbound.put_nowait(
            {
                "type": "query",
                "queryId": query_id,
                "method": "thread_messages",
                "params": {"threadId": thread_id, "limit": limit},
            }
        )
        try:
            return await asyncio.wait_for(waiter, timeout=QUERY_TIMEOUT_SECONDS)
        except asyncio.TimeoutError:
            raise SoukQueryFailed(
                f"souk did not answer thread_messages({thread_id}) in "
                f"{QUERY_TIMEOUT_SECONDS:.0f}s"
            ) from None
        finally:
            self._pending.pop(query_id, None)

    def _resolve_query(self, frame: dict[str, Any]) -> None:
        waiter = self._pending.get(frame.get("queryId"))
        if waiter is None or waiter.done():
            # An answer to a question nobody is waiting on — the query
            # timed out, or its socket died and it was already failed.
            # Dropping it is right: the caller has had its answer.
            return
        if frame.get("error") is not None:
            waiter.set_exception(SoukQueryFailed(str(frame["error"])))
        else:
            waiter.set_result(frame.get("result") or [])

    def _fail_pending_queries(self, reason: str) -> None:
        """The socket is gone: nothing can answer these.

        Failed rather than left to time out, because the answer is already
        known and an agent waiting the full timeout for a certainty is
        just a slower failure. Not retried on the next connection either:
        the agent asked in the middle of a run, and whether that run still
        wants the answer is the agent's to decide, not this queue's.
        """
        for waiter in self._pending.values():
            if not waiter.done():
                waiter.set_exception(SoukQueryFailed(reason))
        self._pending.clear()

    @staticmethod
    def _derive_ws_url(http_url: str) -> str:
        scheme, netloc, path, query, fragment = urlsplit(http_url)
        ws_scheme = "wss" if scheme == "https" else "ws"
        return urlunsplit((ws_scheme, netloc, f"{path.rstrip('/')}/ws/provider", query, fragment))

    async def run_forever(self) -> None:
        """Stay connected, reconnecting on anything that is not shutdown.

        Every reconnect is the full ceremony again — a fresh ticket, a
        fresh handshake, and a fresh `register`: the roster lives on the
        link, so a link that is gone serves nothing and the next one must
        say again what it offers.
        """
        self.runtime.start()
        try:
            while True:
                try:
                    await self._run_connection()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception(
                        "souk connection lost; reconnecting in %.1fs", self.reconnect_delay
                    )
                await asyncio.sleep(self.reconnect_delay)
        finally:
            await self.runtime.aclose()

    async def _fetch_ticket(self) -> tuple[str, str]:
        """`POST /tickets` — the out-of-band half of the handshake.

        Upstream requires the ticket to travel on a channel that is not
        the link it authorises, and this HTTPS request is that channel.
        Whoever answers it is also telling us souk's public key, which is
        what the proof will *name* — so a pinned provider checks the key
        here, before signing anything at all.
        """
        async with httpx.AsyncClient(timeout=30.0, verify=self.ca_cert_path or True) as client:
            response = await client.post(
                f"{self.souk_http_url}/tickets", json={"publicKey": self.public_key}
            )
            response.raise_for_status()
            data = response.json()
        ticket = data.get("ticket")
        funduq_key = data.get("funduqPublicKey")
        if not isinstance(ticket, str) or not ticket:
            raise RuntimeError(f"{self.souk_http_url}/tickets answered without a ticket: {data!r}")
        if not isinstance(funduq_key, str) or not funduq_key:
            # Upstream made the identity key mandatory, so a souk with
            # nothing to present here is broken, not merely anonymous.
            raise RuntimeError(
                f"{self.souk_http_url}/tickets presented no funduqPublicKey — "
                "souk's identity key is required, so this is not a working souk"
            )
        if self.souk_public_key is not None and funduq_key != self.souk_public_key:
            raise SoukIdentityMismatch(
                f"{self.souk_http_url} is souk {funduq_key[:16]}…, not the "
                f"{self.souk_public_key[:16]}… this provider was told to trust"
            )
        return ticket, funduq_key

    async def _handshake(self, ws, ticket: str, funduq_key: str) -> None:
        """Two frames, and this side has already decided whom it is
        addressing before it sends the first.

        The proof is computed from the ticket and the key learned at
        `/tickets` time, so it *names the recipient*: a proof coaxed out
        by one souk cannot be relayed to attach at another — the verifier
        builds the payload with its own key and a mismatch simply fails
        the signature. The welcome then carries souk's answer, its
        signature over the ticket and our nonce under a distinct role
        tag, and it is verified before this link is treated as open —
        upstream's `confirm_connect` ceremony, performed here because the
        wire hands the answer over as a frame.
        """
        provider_nonce = new_nonce()
        await ws.send(
            json.dumps(
                {
                    "type": "hello",
                    "version": HANDSHAKE_VERSION,
                    "publicKey": self.public_key,
                    "ticket": ticket,
                    "nonce": provider_nonce,
                    "maxConcurrentRuns": self.runtime.max_concurrent_runs,
                    # The ticket is the verifier-chosen challenge, in the
                    # funduq-nonce seat of the connect payload.
                    "proof": self.identity.sign_connect(funduq_key, ticket, provider_nonce),
                }
            )
        )

        welcome = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if welcome.get("type") != "welcome":
            raise RuntimeError(f"expected welcome, got {welcome!r}")
        answer = welcome.get("answer")
        if not isinstance(answer, str) or not verify_signature(
            funduq_key, answer, funduq_connect_payload(ticket, provider_nonce)
        ):
            # The half of the handshake that protects the provider: a
            # welcome whose answer does not verify under the key we hold
            # this souk to means whatever answered does not possess it.
            raise SoukIdentityMismatch(
                f"{self.souk_http_url} answered the handshake but did not prove "
                f"possession of {funduq_key[:16]}… "
                f"(it presented {str(welcome.get('funduqPublicKey'))[:16]}…)"
            )

    async def _register(self, ws) -> None:
        """Publish the roster, on the open link.

        Unsigned, and that is the point of the handshake above: the key
        was proved once, when the link opened, and a per-operation
        signature would only re-prove it. Runs are only offered after
        souk answers `registered`, so this side waits for that answer
        before entering the frame loop — a refusal comes back as an
        `error` frame with the socket still open, and is raised so the
        reconnect loop retries rather than idling registered-as-nothing.
        """
        # `as_registration()` returns upstream's `Registration` *model* now
        # (revision 11), so the camelCase re-spelling this used to do by
        # hand is the model's own aliases — one dump, no field mapping.
        #
        # `takesInterjections` is re-derived from the runtime rather than
        # taken from the model, because the runtime is what actually
        # routes an interjection: it asks the provider for the agent's
        # hook, and a card that claimed more than the router would honour
        # is exactly the lie revision 12 removed. On the gateway side core
        # calls `connection.takes_interjections(name)` and overwrites what
        # the Registration carried; this frame is where a *remote* link
        # gets to answer that question at all.
        frame: dict[str, Any] = {
            "type": "register",
            "agents": [
                dump_envelope(
                    agent.as_registration().model_copy(
                        update={
                            "takes_interjections": self.runtime.takes_interjections(
                                agent.name
                            )
                        }
                    )
                )
                for agent in self.agents.values()
            ],
        }
        if self.provider_name is not None:
            frame["providerName"] = self.provider_name
        await ws.send(json.dumps(frame))

        answer = json.loads(await asyncio.wait_for(ws.recv(), WELCOME_TIMEOUT_SECONDS))
        if answer.get("type") == "error":
            raise RuntimeError(f"souk refused this registration: {answer.get('message')!r}")
        if answer.get("type") != "registered":
            raise RuntimeError(f"expected registered, got {answer!r}")
        logger.info(
            "registered %d agent(s) as provider %s", len(answer.get("names") or []), self.public_key
        )

    async def _run_connection(self) -> None:
        ticket, funduq_key = await self._fetch_ticket()

        ssl_context: ssl.SSLContext | None = None
        if self._ws_url.startswith("wss") and self.ca_cert_path:
            ssl_context = ssl.create_default_context(cafile=self.ca_cert_path)

        async with websockets.connect(self._ws_url, ssl=ssl_context) as ws:
            await self._handshake(ws, ticket, funduq_key)
            await self._register(ws)

            writer = asyncio.create_task(self._write_loop(ws))
            try:
                async for raw in ws:
                    frame = json.loads(raw)
                    kind = frame.get("type")
                    if kind == "run":
                        # The ack is the runtime's answer, not this file's
                        # opinion: it takes the run or it is full.
                        await self._offer(frame)
                    elif kind == "queryResult":
                        self._resolve_query(frame)
                    elif kind == "cancel":
                        await self.cancel(frame.get("runId"))
                    elif kind == "error":
                        # Not a path to handle, and deliberately not fatal.
                        # The commonest one is a verdict that missed the
                        # delivery window (5s upstream): souk had already
                        # taken the run back, so the ack matched nothing
                        # and was answered false. That is breakage being
                        # reported, not a protocol error — souk simply
                        # offers the same run again and this side accepts
                        # it again, which is why the socket stays up.
                        logger.warning("souk rejected a frame: %s", frame)
                    else:
                        logger.warning("unexpected frame from souk, ignoring: %s", frame)
            finally:
                # Queries die with the socket; runs do not. A run is
                # addressed by runId and its frames go out on whatever
                # connection is next, but a question was asked of *this*
                # connection and nothing will ever answer it.
                self._fail_pending_queries("souk connection closed")
                writer.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await writer

    async def _offer(self, frame: dict[str, Any]) -> None:
        run_id = frame.get("runId", "")
        agent_name = frame.get("agentName", "")
        # A name this provider does not host is declined here rather than
        # passed on. The runtime cannot answer it — `deliver` is a capacity
        # question and knows nothing about names — so accepting would start
        # a run that raises KeyError on its first step, report a bare
        # stream end, and have souk record it as *failed*. Declining leaves
        # it queued for a provider that does host the name, which is the
        # difference between "not me" and "broken" — and why this stays a
        # bare decline while a validation failure below carries a reason.
        #
        # souk should never send one: it offers a run only to a provider
        # registered for that agent. This is the answer for when it does.
        if agent_name not in self.agents:
            logger.warning(
                "declining run %s: this provider does not serve %r", run_id, agent_name
            )
            self._outbound.put_nowait({"type": "ack", "runId": run_id, "accepted": False})
            return
        # The frame *is* the declared envelope — `DeliveredRun.
        # model_dump(by_alias=True)` on souk's side, rebuilt here with
        # `model_validate`. No field mapping on either end; a frame that
        # does not validate is a permanent refusal, because the same bytes
        # re-offered can never do better. The reason rides the ack, which
        # keeps it three-valued.
        #
        # `type` is the transport's own vocabulary and not the envelope's,
        # and every crossing shape forbids unknown fields since revision
        # 11 — so it is dropped rather than handed to a model that would
        # (correctly) refuse it.
        payload = {key: value for key, value in frame.items() if key != "type"}
        try:
            delivered = DeliveredRun.model_validate(payload)
        except ValidationError as e:
            reason = f"frame does not validate as a DeliveredRun: {e}"
            logger.warning("refusing run %s: %s", run_id, reason)
            self._outbound.put_nowait(
                {"type": "ack", "runId": run_id, "accepted": False, "reason": reason}
            )
            return
        verdict = await self.deliver(delivered)
        if isinstance(verdict, Refusal):
            # A permanent refusal from the runtime — today, an interjection
            # addressed to an agent that takes none. Its reason travels, so
            # souk fails the run instead of re-offering bytes that can
            # never be accepted.
            logger.info("refusing run %s: %s", run_id, verdict.reason)
            self._outbound.put_nowait(
                {
                    "type": "ack",
                    "runId": run_id,
                    "accepted": False,
                    "reason": verdict.reason,
                }
            )
            return
        self._outbound.put_nowait(
            {"type": "ack", "runId": run_id, "accepted": bool(verdict)}
        )

    async def _write_loop(self, ws) -> None:
        while True:
            frame = await self._outbound.get()
            await ws.send(json.dumps(frame))
