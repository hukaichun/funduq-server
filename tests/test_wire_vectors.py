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
    assert payload_vectors["contract"]["revision"] == funduq_contract.CONTRACT_REVISION == 16


def test_the_frame_vocabulary_is_the_dispatched_one():
    assert set(VECTORS["frames"]["provider_socket_inbound"]) == ws_provider.INBOUND_FRAME_TYPES
    assert set(VECTORS["frames"]["kyok_socket_inbound"]) == ws_kyok.INBOUND_FRAME_TYPES


def test_the_register_frames_agent_shape_is_upstreams_registration():
    """The `register` frame's per-agent shape is
    `funduq_contract.Registration` itself — the model both ends import,
    not a local restatement of it — so what this file documents is read
    off the model rather than typed beside it.

    The pair of tests that used to stand here compared a local model and a
    local constant against `REGISTRATION_FIELDS` / `LINK_QUERY_METHODS`.
    Both constants were withdrawn at revision 11 for the reason this test
    embodies: with one definition there is nothing left for a field list
    to police.
    """
    documented = {f.rstrip("?") for f in VECTORS["frames"]["register_agent_entry"]}
    on_the_wire = {
        field.alias or name
        for name, field in funduq_contract.Registration.model_fields.items()
    }
    assert documented == on_the_wire
    # The one field this round adds, named rather than merely counted.
    assert "takesInterjections" in on_the_wire
