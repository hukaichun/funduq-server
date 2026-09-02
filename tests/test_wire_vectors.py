"""docs/wire-vectors.json, consumed by the side that authors it.

The signed payloads this file used to vector were replaced by
`funduq_contract`'s connect family, vectored in the vendored
docs/upstream-contract-vectors.json — the authority followed the bytes.
What stays this repo's to publish and pin: the wire version, and each
socket's frame vocabulary. The other consumers (souk-agent-sdk,
souk-client-sdk, the Go pod-probe) check the same file, so every
implementation of the choreography still answers to one statement.
"""

from __future__ import annotations

import json
from pathlib import Path

import funduq_contract

from souk_server import handshake, ws_kyok, ws_provider

DOCS = Path(__file__).parent.parent / "docs"
VECTORS = json.loads((DOCS / "wire-vectors.json").read_text())


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == handshake.WIRE_VERSION == 4


def test_the_handshake_payloads_are_upstreams_not_restatements():
    """The point of re-exporting: the gateway signs and verifies exactly
    the bytes `funduq_contract` states — object identity, not equal
    output, so the vectors for them live upstream and cannot drift from
    this side."""
    assert handshake.provider_connect_payload is funduq_contract.provider_connect_payload
    assert handshake.funduq_connect_payload is funduq_contract.funduq_connect_payload
    assert handshake.new_nonce is funduq_contract.new_nonce


def test_the_payload_vectors_pointer_names_a_file_that_exists_at_the_pinned_revision():
    """The pointer is load-bearing — the Go pod-probe reads the vendored
    file through it, and a pointer at a missing file must fail loudly
    here rather than as a skip there."""
    vendored = DOCS.parent / VECTORS["payload_vectors"]
    payload_vectors = json.loads(vendored.read_text())
    assert payload_vectors["contract"]["revision"] == funduq_contract.CONTRACT_REVISION == 7


def test_the_frame_vocabulary_is_the_dispatched_one():
    assert set(VECTORS["frames"]["provider_socket_inbound"]) == ws_provider.INBOUND_FRAME_TYPES
    assert set(VECTORS["frames"]["kyok_socket_inbound"]) == ws_kyok.INBOUND_FRAME_TYPES


def test_the_registration_fields_are_upstreams():
    """The register frame's per-agent fields are `REGISTRATION_FIELDS` —
    a field added upstream without this gateway's frame model learning it
    would be silently dropped on the way into core."""
    from funduq_provider_sdk.contract import REGISTRATION_FIELDS

    from souk_server.models import AgentRegistration

    assert set(AgentRegistration.model_fields) == set(REGISTRATION_FIELDS)


def test_the_wire_carries_every_query_the_link_declares():
    """`contract.LINK_QUERY_METHODS` is upstream's list of what a provider
    may ask. A method added there without a frame here would compile, pass
    every test, and fail at a provider — so this is the one place the two
    are compared."""
    from funduq_provider_sdk.contract import LINK_QUERY_METHODS

    assert set(ws_provider.QUERY_METHODS) == set(LINK_QUERY_METHODS)
