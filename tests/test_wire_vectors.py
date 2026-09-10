"""docs/wire-vectors.json, consumed by the side that authors it.

The signed payloads this file used to vector were replaced by
`funduq_contract`'s connect family, vectored in the vendored
docs/upstream-contract-vectors.json — the authority followed the bytes.
What stays this repo's to publish and pin: the wire version, this
gateway's own presenter proof — the one payload family here that upstream
does not define — and each socket's frame vocabulary. The other consumers (souk-agent-sdk,
souk-client-sdk, the Go pod-probe) check the same file, so every
implementation of the choreography still answers to one statement.
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import funduq_contract
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from funduq import identity

from souk_server import handshake, presenter, ws_kyok, ws_provider

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
    assert payload_vectors["contract"]["revision"] == funduq_contract.CONTRACT_REVISION == 22


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


def test_the_published_presenter_proof_is_the_one_the_gateway_verifies():
    """The only payload family this repo defines itself, so this file is
    its statement of record — the SDKs and the Go probe sign against it,
    and a payload that drifted from `souk_server.presenter` would be a
    proof nobody could build and nothing could verify.

    The tag is checked by name because it is deliberately **ours**:
    `funduq-server-presenter`, not `funduq-presenter`. The `funduq-*`
    namespace is upstream's, and squatting it would mint a name that looks
    canonical and is not — the exact failure the contract vectors exist to
    prevent. When upstream ships one, this test is what says so.
    """
    published = VECTORS["presenter_proof"]

    assert published["header"].lower() == presenter.PRESENTER_HEADER
    assert published["payload"].startswith(presenter.PRESENTER_DOMAIN + ":")
    assert presenter.PRESENTER_DOMAIN == "funduq-server-presenter"
    assert published["freshness_seconds"] == identity.SIGNATURE_FRESHNESS_WINDOW_SECONDS

    # The template, filled in and compared to the bytes the gateway builds.
    body = b'{"hello":"world"}'
    public_key, timestamp = "ab" * 32, 1757260000
    assert presenter.presenter_payload(public_key, timestamp, body).decode() == (
        published["payload"]
        .replace("{publicKey}", public_key)
        .replace("{timestamp}", str(timestamp))
        .replace("{sha256hex(request_body)}", hashlib.sha256(body).hexdigest())
    )


def test_a_presenter_proof_round_trips_and_only_for_its_own_body():
    """The property the body hash exists for, driven rather than asserted
    about: the same proof presented with any other body proves nothing."""
    key = Ed25519PrivateKey.generate()
    public_key = key.public_key().public_bytes_raw().hex()
    timestamp = int(time.time())
    body = b'{"threadId":"t","runId":"r"}'
    header = json.dumps(
        {
            "publicKey": public_key,
            "timestamp": timestamp,
            "signature": key.sign(
                presenter.presenter_payload(public_key, timestamp, body)
            ).hex(),
        },
        separators=(",", ":"),
    )

    assert presenter.presenter_key_of(header, body) == public_key
    assert presenter.presenter_key_of(header, body + b" ") is None
    assert presenter.presenter_key_of(None, body) is None
