"""This package against the published wire statement (docs/wire-vectors.json
at the repo root — regenerated for wire v4, the ticket handshake).

The payload restatement this test used to guard is gone twice over: the
bytes a provider signs come from `funduq-contract` (via
`funduq_provider_sdk`), imported rather than copied, so the byte-level
vectors live upstream and this side has nothing of its own to drift.
What remains local, and checked here: the handshake version this client
speaks, that the frame vocabulary the gateway publishes is the one this
client's dispatch implements, and that the payload builders this module
uses *are* the contract package's objects rather than lookalikes.
"""

from __future__ import annotations

import json
from pathlib import Path

import funduq_contract
import funduq_provider_sdk

from souk_agent_sdk import client

VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "wire-vectors.json").read_text()
)

# The provider-socket vocabulary of wire v4, per docs/server-mode.md:
# the two handshake frames, the registration family that moved onto the
# open link, and the post-attach frames (unchanged from v2/v3).
HANDSHAKE_FRAMES = {"hello", "welcome"}
REGISTRATION_FRAMES = {"register", "registered", "deleteAgent", "deleted"}
POST_ATTACH_FRAMES = {"run", "ack", "event", "finish", "cancel", "query", "queryResult", "error"}


def _all_listed_strings(node) -> set[str]:
    """Every string that appears inside a JSON *list* anywhere in the
    file. The frame vocabularies are lists of frame names; walking for
    them rather than addressing a fixed path keeps this test about the
    vocabulary, not about how the gateway chose to shape the document."""
    found: set[str] = set()
    if isinstance(node, dict):
        for value in node.values():
            found |= _all_listed_strings(value)
    elif isinstance(node, list):
        for item in node:
            if isinstance(item, str):
                found.add(item)
            else:
                found |= _all_listed_strings(item)
    return found


def test_the_published_version_is_the_spoken_one():
    # If this reads 2 or 3, the gateway-side regeneration of
    # docs/wire-vectors.json has not landed yet — the file is stale, not
    # this client.
    assert VECTORS["handshake_version"] == client.HANDSHAKE_VERSION == 4


def test_the_published_frame_vocabulary_is_the_implemented_one():
    published = _all_listed_strings(VECTORS.get("frames", {}))
    missing = (HANDSHAKE_FRAMES | REGISTRATION_FRAMES | POST_ATTACH_FRAMES) - published
    assert not missing, f"wire-vectors.json does not list frames this client speaks: {sorted(missing)}"


def test_the_signed_bytes_are_upstreams_not_a_restatement():
    """Object identity, not equality: the functions this client verifies
    and signs with must *be* funduq-contract's, so a byte-level change
    upstream is a change here by construction."""
    assert client.funduq_connect_payload is funduq_contract.funduq_connect_payload
    assert client.verify_signature is funduq_contract.verify_signature
    assert (
        funduq_provider_sdk.provider_connect_payload
        is funduq_contract.provider_connect_payload
    )
    # sign_connect signs exactly the contract's provider_connect_payload;
    # proven byte-for-byte since the identity object exposes no payload.
    identity = funduq_provider_sdk.ProviderIdentity.generate()
    proof = identity.sign_connect("fk", "ticket", "nonce")
    assert funduq_contract.verify_signature(
        identity.public_key,
        proof,
        funduq_contract.provider_connect_payload("fk", "ticket", "nonce"),
    )
