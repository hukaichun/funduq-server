"""Pydantic schemas for the gateway's HTTP surface and wire frames.

`RunAgentInput` itself is deliberately *not* defined here — the inbound
`/agui/{provider}/{name}` request body uses `ag_ui.core.RunAgentInput`,
the real AG-UI schema, directly (see api_agui.py). A separate,
souk-flavored reimplementation of the same model used to live here,
which meant two different types with the same name, only one of which
was the real protocol.

The signed registration/deletion request bodies are gone with their
routes: registration and deletion moved onto the provider sockets, where
the open link is the credential (see ws_provider.py / ws_kyok.py).
`AgentRegistration` survives because the shape of one roster entry is
still this layer's to validate — it now describes an element of the
`register` frame's `agents` list, camelCase on the wire.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel


class TicketRequest(BaseModel):
    """`POST /tickets`: the out-of-band half of the v4 handshake.

    The body names the Ed25519 public key (hex) the ticket admits — a
    leaked ticket is worthless because only the named key can sign the
    proof that answers it.
    """

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    public_key: str = Field(min_length=1)


class TicketResponse(BaseModel):
    """The single-use ticket (valid ~60s, destroyed by the handshake that
    answers it) and funduq's public key — learned here, over TLS, which is
    what the connect proof then binds."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    ticket: str
    funduq_public_key: str


class AgentRegistration(BaseModel):
    """One agent in a `register` frame. camelCase on the wire
    (`agentCardExtra`), snake_case toward core — the same field list as
    upstream's `REGISTRATION_FIELDS`, compared in a test."""

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)

    name: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    description: str = ""
    agent_card_extra: dict[str, Any] = Field(default_factory=dict)
    # souk-internal, not exposed via the public A2A Agent Card — see
    # agents.metadata in funduq's schema.
    metadata: dict[str, Any] = Field(default_factory=dict)


class AgentRosterEntry(BaseModel):
    """One roster row on the wire.

    An agent *is* `(provider_key, name)` — funduq mints no id for anyone
    to hold — so the pair is what a caller addresses it by. `fingerprint`
    is the same identity in 16 hex, which is what this gateway puts in a
    URL; it is derived, never authoritative, and `provider_key` is the
    thing to compare.
    """

    model_config = ConfigDict(from_attributes=True)

    provider_key: str
    fingerprint: str
    name: str
    description: str = ""
    skills: list[dict[str, Any]] = Field(default_factory=list)
    joined_at: datetime
    last_seen_at: datetime
    online: bool
    # The optional storefront label for that key (funduq's providers
    # table), None if this provider never set one.
    provider_name: str | None = None


class RosterResponse(BaseModel):
    agents: list[AgentRosterEntry]


class LlmOfferingEntry(BaseModel):
    """One LLM offering on the wire — `AgentRosterEntry`'s mirror.

    `online` is the pre-flight answer a KYOK caller had no way to ask
    before binding a run: whether the offering it is about to name is
    attached *right now* (liveness stays a per-call fact after that —
    this is a glance, not a reservation).
    """

    model_config = ConfigDict(from_attributes=True)

    provider_key: str
    fingerprint: str
    name: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    joined_at: datetime
    last_seen_at: datetime
    online: bool
    provider_name: str | None = None


class LlmRosterResponse(BaseModel):
    offerings: list[LlmOfferingEntry]


class CreateThreadRequest(BaseModel):
    # The agent comes from the URL path (POST /threads/{provider}/{name})
    # — this body is just the optional extras.
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateThreadResponse(BaseModel):
    thread_id: str


class LivenessResponse(BaseModel):
    """`/healthz`. Nothing about funduq's dependencies belongs here — the
    response existing is the answer."""

    status: str


class HealthResponse(BaseModel):
    """`/readyz`. Facts, so an operator reading a 503 does not have to go
    and find out which of them was false. Carries no connection string and
    no driver message — see funduq.core.Health, and note that this
    endpoint is unauthenticated because a probe cannot hold a credential.

    `background_running` is gone with core's health sweeps; `dispatching`
    — whether the broker's dispatch loop is turning — is the fact that
    replaced it in `Health`, and readiness includes it.
    """

    ready: bool
    database: bool
    # The exception's type name, never its message.
    database_error: str | None = None
    schema_revision: str | None = None
    expected_schema_revision: str
    dispatching: bool
