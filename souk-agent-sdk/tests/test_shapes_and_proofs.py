"""The two dump rules, the registration model, and the proofs this SDK
signs — the things that used to be policed by upstream constants and a
codec, and are now nobody's job but this package's.

Contract revision 11 withdrew every field-list constant
(`REGISTRATION_FIELDS`, `DELIVERED_RUN_FIELDS`, `CONNECTED_PROVIDER_ATTRS`,
`LINK_QUERY_METHODS`, …) along with the sans-io machines and their codec:
the models are the single definition, so there is nothing left for a list
to compare against. What the codec *also* did, and nothing upstream does
now, is enforce which dump each direction takes — so that lives here, in
`souk_agent_sdk.client.dump_envelope` / `dump_event`, with these tests
under it.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ag_ui.core import RunAgentInput, RunStartedEvent
from funduq_contract import DeliveredRun, Registration, view_payload
from funduq_provider_sdk import AgentHandle, ProviderIdentity, verify_signature

from souk_agent_sdk import a2a_client
from souk_agent_sdk.client import dump_envelope, dump_event

UPSTREAM_VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "upstream-contract-vectors.json").read_text()
)


def _vector(kind: str) -> dict:
    return next(v for v in UPSTREAM_VECTORS["vectors"] if v["kind"] == kind)


def _test_identity() -> ProviderIdentity:
    """Upstream's published test key, so a signature here is comparable to
    the vector byte for byte rather than merely self-consistent."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    return ProviderIdentity(
        Ed25519PrivateKey.from_private_bytes(
            bytes.fromhex(UPSTREAM_VECTORS["test_key"]["private_key_hex"])
        )
    )


async def _stream(run_input):  # pragma: no cover - never run
    yield {}


async def _interject(run_input, addressed_run_id):  # pragma: no cover - never run
    yield {}


# --- the registration model --------------------------------------------------


def test_as_registration_returns_a_model_that_declares_the_interjection_hook():
    """`as_registration()` returns a `Registration`, not a dict (revision
    11), and derives `takes_interjections` from the hook itself (revision
    12) — so the card cannot claim a capability the router will not
    honour. Anything still doing `**registration` or `registration["name"]`
    breaks here rather than at the far end of a socket."""
    plain = AgentHandle(name="plain", run_stream=_stream).as_registration()
    listens = AgentHandle(
        name="listens", run_stream=_stream, interject_stream=_interject
    ).as_registration()

    assert isinstance(plain, Registration)
    assert plain.takes_interjections is False
    assert listens.takes_interjections is True
    # camelCase on the wire is the model's own alias, not a mapping this
    # package maintains.
    assert dump_envelope(listens)["takesInterjections"] is True
    assert dump_envelope(plain)["name"] == "plain"


def test_a_registration_refuses_a_field_it_does_not_declare():
    """Every crossing shape is `extra="forbid"` now, which is why the
    transport strips its own vocabulary (`type`, `requestId`) before
    validating rather than handing a frame over whole."""
    with pytest.raises(Exception):
        Registration(name="x", takesSomethingElse=True)


# --- the two dump rules, which pull opposite ways ----------------------------


def test_an_envelope_keeps_its_nulls():
    """`RunAgentInput` has required fields that are legitimately null.
    Stripping them makes the far side's `model_validate` fail, which the
    transport answers as a *permanent* refusal — a good run turned into a
    dead one, reported as the provider's fault."""
    run = DeliveredRun(
        runId="r1",
        agentName="echo",
        runInput=RunAgentInput(
            threadId="t1",
            runId="r1",
            state=None,
            messages=[],
            tools=[],
            context=[],
            forwardedProps=None,
        ),
        threadId="t1",
    )
    frame = dump_envelope(run)
    assert frame["runId"] == "r1" and frame["agentName"] == "echo"
    # `forwardedProps` is the field that carries the rule now: required by
    # the model and legitimately null, so `exclude_none` would drop it and
    # the far side could not rebuild the input. `state` is NOT the example
    # to use — ag-ui 0.1.22 made it `Any = None` and omits it when unset,
    # deliberately ("a bare null on the wire reads as absent"), so
    # asserting it survives pins an older ag-ui rather than this rule. That
    # skew is exactly what broke a full stack run: a gateway on 0.1.22
    # emitting no `state` against a provider on 0.1.19 requiring one, which
    # is why every pyproject here floors ag-ui-protocol at 0.1.22.
    assert "forwardedProps" in frame["runInput"]
    assert frame["runInput"]["forwardedProps"] is None
    # And it survives the round trip that a run frame actually makes.
    assert DeliveredRun.model_validate(frame) == run


def test_an_event_strips_its_nulls():
    """The opposite rule, and just as load-bearing: an AG-UI event is
    relayed into somebody's stream, and `timestamp: null` / `rawEvent:
    null` are fields the caller never sent and should never see."""
    event = RunStartedEvent(threadId="t1", runId="r1")
    assert event.timestamp is None
    wire = dump_event(event)
    assert wire["type"] == "RUN_STARTED"
    assert wire["threadId"] == "t1" and wire["runId"] == "r1"
    assert "timestamp" not in wire
    assert "rawEvent" not in wire


def test_an_event_that_is_already_plain_data_is_passed_through():
    """Agents here yield dicts as often as models; nothing is invented for
    them."""
    assert dump_event({"type": "CUSTOM", "value": None}) == {
        "type": "CUSTOM",
        "value": None,
    }


# --- the proofs --------------------------------------------------------------


def test_a_resolve_proof_signs_the_ask_and_matches_upstreams_vector():
    """Revision 16: `sign_resolution(run_id, ask_ids)` — the ask, not the
    clock. Replayed against upstream's published vector, so the sorting,
    the NUL join and the sha256 are checked as bytes rather than as a
    description of them. The vector's ask ids are deliberately unsorted:
    canonicalization is the builder's job, never the caller's."""
    vector = _vector("resolution")
    identity = _test_identity()
    signature = identity.sign_resolution(
        vector["inputs"]["run_id"], vector["inputs"]["ask_ids"]
    )
    assert signature == vector["signature_hex"]
    # Same set, other order: the same signature, because sorting happens
    # inside the payload builder.
    assert (
        identity.sign_resolution(
            vector["inputs"]["run_id"], list(reversed(vector["inputs"]["ask_ids"]))
        )
        == signature
    )
    # And no timestamp rides in it — the wire proof is two fields.
    assert vector["payload_utf8"].startswith("funduq-resolve:")


def test_a_view_proof_is_the_header_a_bound_read_carries():
    """Revision 13. Verified against upstream's vector for the bytes, then
    against the header this SDK actually sends — compact JSON under
    `X-Funduq-View`, `{publicKey, timestamp, signature}`, still the
    timestamp family (unlike resolve)."""
    vector = _vector("view")
    identity = _test_identity()
    run_id = vector["inputs"]["run_id"]
    timestamp = vector["inputs"]["timestamp"]

    assert identity.sign(view_payload(run_id, timestamp)) == vector["signature_hex"]

    headers = a2a_client.view_headers(identity, run_id, timestamp=timestamp)
    proof = json.loads(headers[a2a_client.VIEW_PROOF_HEADER])
    assert proof == {
        "publicKey": identity.public_key,
        "timestamp": timestamp,
        "signature": vector["signature_hex"],
    }
    assert verify_signature(
        proof["publicKey"], proof["signature"], view_payload(run_id, timestamp)
    )
    # No identity, no header: a bound run then reads as absent, which is
    # the designed answer — never an error this side invents.
    assert a2a_client.view_headers(None, run_id) == {}


def test_delegation_signing_is_gone():
    """Revision 15 deleted the session delegation certificate. Nothing in
    this SDK may grow it back: a grant is the authenticating seat's policy
    now, and `sign_delegation` no longer exists to be called."""
    assert not hasattr(ProviderIdentity, "sign_delegation")
    import funduq_contract

    assert not hasattr(funduq_contract, "delegation_payload")
