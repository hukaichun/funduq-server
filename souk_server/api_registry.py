"""Ticket issuance and the read-only rosters: routes only.

The signed HTTP registration and deletion routes are gone. Registration
and deletion are operations on an open provider link now (`register` /
`deleteAgent` / `deleteModel` frames — see ws_provider.py, ws_kyok.py),
because the key is proved once, when the link opens, and a per-operation
signature would only re-prove it. What remains here:

- `POST /tickets` — the out-of-band half of the v4 handshake, serving
  BOTH sockets. Core keeps `issue_ticket` off the link's operation set on
  purpose: a ticket fetched over the link would mean the link existed
  before anything authorised it. **Issuing is the admission decision**,
  which makes this endpoint the future edge-auth plug point — a
  deployment that gates who may serve gates it here. Unauthenticated
  today, deliberately: this souk is an open market.
- The roster GETs, which are what a caller (or souk-directory) reads to
  discover agents and offerings and glance at their liveness.
"""

from fastapi import APIRouter, Depends

from funduq.core import Funduq
from funduq.identity import provider_fingerprint
from souk_server.deps import get_souk
from souk_server.models import (
    AgentRosterEntry,
    LlmOfferingEntry,
    LlmRosterResponse,
    RosterResponse,
    TicketRequest,
    TicketResponse,
)

router = APIRouter()


@router.post("/tickets", status_code=201)
async def issue_ticket(body: TicketRequest, funduq: Funduq = Depends(get_souk)) -> TicketResponse:
    """Mint a single-use ticket admitting `publicKey` to open a link.

    The response also carries funduq's public key: the provider learns it
    here, over TLS, and the connect proof it signs before connecting
    names that key — so a proof one funduq coaxes out cannot be relayed
    to attach at another, and the `answer` in the welcome frame is
    checked against the same pin.
    """
    return TicketResponse(
        ticket=funduq.issue_ticket(body.public_key),
        funduq_public_key=funduq.identity_public_key,
    )


@router.get("/llm-providers")
async def list_llm_providers(funduq: Funduq = Depends(get_souk)) -> LlmRosterResponse:
    """The offering roster, `GET /agents`' mirror — what a KYOK caller
    reads to discover an offering and to glance at its liveness before
    binding a run to it."""
    return LlmRosterResponse(
        offerings=[
            LlmOfferingEntry(
                **summary.model_dump(),
                fingerprint=provider_fingerprint(summary.provider_key),
            )
            for summary in await funduq.list_llm_providers()
        ]
    )


@router.get("/agents")
async def list_agents(funduq: Funduq = Depends(get_souk)) -> RosterResponse:
    return RosterResponse(agents=await _roster(funduq))


async def _roster(funduq: Funduq) -> list[AgentRosterEntry]:
    """The roster, with the fingerprint this gateway addresses agents by
    filled in beside the key it is derived from."""
    return [
        AgentRosterEntry(
            **summary.model_dump(),
            fingerprint=provider_fingerprint(summary.provider_key),
        )
        for summary in await funduq.list_agents()
    ]
