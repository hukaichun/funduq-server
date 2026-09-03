"""Caller-side SDK: a thin AG-UI client for talking to an agent through a
souk, so callers (human-facing apps, or another agent acting as a plain
top-level caller) don't have to hand-roll HTTP+SSE or thread bookkeeping.

Not agent-facing — unrelated to souk-agent-sdk's work socket.

**An agent is `(provider, name)`.** souk mints no id for one, and a bare
display name is not unique — two independent providers may both offer
`translator` — so every route here takes both halves. `provider` is the
provider's Ed25519 public key or its 16-hex fingerprint; both work, and
both appear on every roster row.

A caller who only has a name calls `resolve` once. That is the whole of
what the gateway's deleted by-name routes were doing, except that this
does it where the caller can see the ambiguity and answer it, instead of
inside a route that had to either guess or refuse.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

import httpx
from funduq_provider_sdk import ProviderIdentity
from httpx_sse import aconnect_sse


def resolution_proof(
    identity: ProviderIdentity, run_id: str, ask_ids: list[str]
) -> dict[str, str]:
    """The proof that answers a paused run's asks: `{publicKey, signature}`.

    Contract revision 16: a resolve proof signs the *ask*, not the clock.
    The signed bytes are `funduq-resolve:{run_id}:{sha256 hex of the ask
    ids, sorted and NUL-joined}` — canonicalization lives in
    `funduq_contract.resolve_payload` and nowhere else, so both sides
    derive the same bytes from the same set. Order does not matter here:
    sorting is the builder's job, not the caller's.

    Consequently there is **no timestamp and no freshness window** on this
    one proof, unlike cancel and view: a later pause has new ask ids, so
    the signature never verifies against any ask but the one it was signed
    for, and replay against that same ask is consumed by the reopen's own
    status guard.

    `ask_ids` must be everything the run is still waiting on — see
    `SoukClient.last_outstanding_asks`. A subset does not verify.
    """
    return {
        "publicKey": identity.public_key,
        "signature": identity.sign_resolution(run_id, ask_ids),
    }


class AgentNotFound(Exception):
    """No listed agent by that name (under that provider, if one was given)."""


class AmbiguousAgentName(Exception):
    """Several providers offer this name, so it does not identify an agent.

    Carries every candidate so a caller can present the choice rather than
    having one made for it. `provider=` on the original call is how to say
    which one was meant.
    """

    def __init__(self, name: str, candidates: list["Agent"]) -> None:
        self.name = name
        self.candidates = candidates
        stalls = ", ".join(f"{c.provider_name or 'unnamed'} ({c.provider})" for c in candidates)
        super().__init__(
            f"{len(candidates)} providers offer '{name}': {stalls}. "
            f"Pass provider= to say which one."
        )


@dataclass(frozen=True)
class Agent:
    """One agent, addressable. `provider` is the fingerprint — short enough
    for a URL and what the roster leads with — while `provider_key` is the
    full public key, which is the thing to compare: the fingerprint is
    derived from it and is never authoritative.
    """

    provider: str
    name: str
    provider_key: str = ""
    description: str = ""
    online: bool = False
    provider_name: str | None = None

    @property
    def path(self) -> str:
        return f"{self.provider}/{self.name}"


class SoukClient:
    def __init__(self, souk_http_url: str, timeout: float = 300.0) -> None:
        self.souk_http_url = souk_http_url.rstrip("/")
        self.timeout = timeout
        self.last_thread_id: str | None = None
        self.last_run_id: str | None = None
        # What the last run this client streamed is still waiting on, if it
        # paused — its interrupt / tool-call ids, in core's one ask id
        # space. Surfaced because since contract revision 16 a caller
        # *cannot resolve a paused run without them*: the resolve proof
        # signs exactly this set (see `resolution_proof`), so a client that
        # only reported "it paused" left the caller unable to answer.
        self.last_outstanding_asks: list[str] = []

    async def roster(self) -> list[Agent]:
        """Everyone this souk lists, whether or not anybody is serving them."""
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(f"{self.souk_http_url}/agents")
            resp.raise_for_status()
        return [
            Agent(
                provider=row["fingerprint"],
                name=row["name"],
                provider_key=row["provider_key"],
                description=row.get("description", ""),
                online=row.get("online", False),
                provider_name=row.get("provider_name"),
            )
            for row in resp.json()["agents"]
        ]

    async def resolve(self, name: str, provider: str | None = None) -> Agent:
        """Turn a name into an address, once.

        Do this at startup and hold the `Agent`, rather than per call: it
        is a round trip, and the answer only changes when somebody
        registers or delists.

        Raises `AmbiguousAgentName` when several providers offer the name
        and none was given. That is deliberately an error rather than a
        first-match: picking a winner is how a caller reaches an agent it
        never meant to reach, and it is exactly why the gateway stopped
        serving by-name routes.
        """
        candidates = [
            agent
            for agent in await self.roster()
            if agent.name == name
            and (provider is None or provider in (agent.provider, agent.provider_key))
        ]
        if not candidates:
            where = f" under provider '{provider}'" if provider else ""
            raise AgentNotFound(f"no listed agent named '{name}'{where}")
        if len(candidates) > 1:
            raise AmbiguousAgentName(name, candidates)
        return candidates[0]

    async def create_thread(
        self, agent: Agent, *, metadata: dict[str, Any] | None = None
    ) -> str:
        """Obtain a thread_id upfront — e.g. to show it in a UI before the
        first message. `run()` below calls this for you when you don't pass
        a `thread_id`, so this is only needed when you want the id early.
        """
        url = f"{self.souk_http_url}/threads/{agent.path}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.post(url, json={"metadata": metadata} if metadata else {})
            resp.raise_for_status()
            return resp.json()["thread_id"]

    async def run(
        self,
        agent: Agent,
        message: str = "",
        *,
        thread_id: str | None = None,
        role: str = "user",
        metadata: dict[str, Any] | None = None,
        resume: list[dict[str, Any]] | None = None,
        addressed_run_id: str | None = None,
        resolution: dict[str, str] | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """POSTs a RunAgentInput to `/agui/{provider}/{name}` and yields each
        AG-UI event as it streams back. Pass `thread_id` from a previous
        call's `last_thread_id` to continue that conversation; omit it to
        start a new one — this calls `create_thread` for you in that case.

        `agent` is an `Agent` from `resolve` or `roster`. It is not a name:
        see this module's docstring for why the SDK asks for an address.

        `metadata` is stored on the run/thread as-is (minus
        `kyok.context`, which souk strips before anything persists) and,
        notably, is where a Keep Your Own Key caller opts the run into an
        LLM offering: `{"kyok": {"llmProvider": {"providerKey": ...,
        "name": ...}, "context": ...}}` — `KyokBridge.run_metadata()`
        builds it; see docs/keep-your-own-key.md in the souk repo.

        `resume` is AG-UI's own interrupt/resume mechanism
        (`ag_ui.core.ResumeEntry`: `{"interruptId": ..., "status":
        "resolved"|"cancelled", "payload": ...}`) — pass it, on the same
        `thread_id` a previous call's stream ended paused on (its last
        `RUN_FINISHED` carried `outcome.type == "interrupt"`), to resolve
        one or more of those interrupts. `message` isn't required
        alongside it — resolving an interrupt isn't necessarily saying
        anything new in the conversation, so an empty `message` sends no
        message at all rather than an empty one.

        `addressed_run_id` declares this message an *interjection* into a
        run already going on that thread (pass its id — a previous call's
        `last_run_id`): it rides as `forwardedProps.addressedRunId`, and
        souk delivers the message into that run instead of starting a new
        turn. Interjection is caller-declared, never inferred.

        `resolution`, if given, is the proof that this caller may answer a
        paused run bound to an actor chain — `{publicKey, signature}` from
        `resolution_proof(identity, run_id, ask_ids)`, riding
        `metadata.resolution` where souk reads it. Build it over
        `last_outstanding_asks` from the stream that paused; a run on an
        unbound thread needs none.
        """
        if thread_id is None:
            thread_id = await self.create_thread(agent)

        # The real ag_ui.core.RunAgentInput wire shape — threadId is the
        # only id souk actually uses; runId is required by the schema but
        # never read, so a placeholder satisfies it without meaning
        # anything. souk mints every thread id itself: an unseen threadId
        # here gets a fresh one, announced on RUN_STARTED (read below).
        body: dict[str, Any] = {
            "threadId": thread_id,
            "runId": str(uuid4()),
            "state": None,
            "messages": [],
            "tools": [],
            "context": [],
            "forwardedProps": None,
        }
        if message:
            body["messages"] = [{"id": str(uuid4()), "role": role, "content": message}]
        if resolution is not None:
            metadata = {**(metadata or {}), "resolution": resolution}
        if metadata is not None:
            body["metadata"] = metadata
        if resume is not None:
            body["resume"] = resume
        if addressed_run_id is not None:
            body["forwardedProps"] = {"addressedRunId": addressed_run_id}
        url = f"{self.souk_http_url}/agui/{agent.path}"

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with aconnect_sse(client, "POST", url, json=body) as event_source:
                # No custom header to read here — a real run's first
                # event is always RUN_STARTED, carrying the resolved,
                # real threadId/runId (souk substitutes its own if
                # `thread_id` above was unrecognized) — that's the
                # standard AG-UI place to learn them, not a souk-invented
                # side channel.
                self.last_thread_id = thread_id
                # The ask id space core pauses on, tracked as the stream
                # goes by: tool calls announced and not answered, plus the
                # interrupts the RUN_FINISHED outcome names. Mirrors
                # funduq.pause's `unanswered_tool_calls` /
                # `outstanding_asks` — the ids a resolve proof must sign.
                announced: list[str] = []
                answered: set[str] = set()
                interrupts: list[str] = []
                self.last_outstanding_asks = []
                async for sse in event_source.aiter_sse():
                    event = json.loads(sse.data)
                    kind = event.get("type")
                    if kind == "RUN_STARTED":
                        self.last_thread_id = event.get("threadId", thread_id)
                        self.last_run_id = event.get("runId")
                    elif kind == "TOOL_CALL_START":
                        announced.append(event["toolCallId"])
                    elif kind == "TOOL_CALL_RESULT":
                        answered.add(event["toolCallId"])
                    elif kind == "RUN_FINISHED":
                        outcome = event.get("outcome")
                        if isinstance(outcome, dict) and outcome.get("type") == "interrupt":
                            interrupts = [
                                ask
                                for ask in (
                                    interrupt.get("toolCallId") or interrupt.get("id")
                                    for interrupt in outcome.get("interrupts") or []
                                )
                                if ask
                            ]
                            outstanding = [
                                ask for ask in announced if ask not in answered
                            ]
                            outstanding += [
                                ask for ask in interrupts if ask not in outstanding
                            ]
                            self.last_outstanding_asks = outstanding
                    yield event
