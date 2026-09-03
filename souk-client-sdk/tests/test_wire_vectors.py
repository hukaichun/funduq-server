"""The KYOK bridge against the published wire statement
(docs/wire-vectors.json at the repo root).

Like souk-agent-sdk's twin of this file, the payload half is gone: v4
signs upstream's link-open family (`ProviderIdentity.sign_connect` /
`funduq_connect_payload`), so the bridge restates no bytes and the
vectors for them live upstream (funduq-contract's contract-vectors). The
handshake version is what remains local and checked — plus one identity
assertion pinning the bridge's verify path to the upstream payload
function, not a restatement of it.
"""

from __future__ import annotations

import json
from pathlib import Path

import funduq_contract

from souk_client_sdk import kyok_bridge
from souk_client_sdk.kyok_bridge import HANDSHAKE_VERSION

VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "wire-vectors.json").read_text()
)


def test_the_published_version_is_the_spoken_one():
    assert VECTORS["handshake_version"] == HANDSHAKE_VERSION == 4


def test_the_answer_is_verified_over_upstreams_payload():
    """Object identity, not byte equality: the bridge imports the payload
    the welcome's answer is checked against, so it cannot drift from
    funduq-contract's statement."""
    assert kyok_bridge.funduq_connect_payload is funduq_contract.funduq_connect_payload
