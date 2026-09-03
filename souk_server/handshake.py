"""What each side signs to open a provider or LLM-provider socket.

The payloads are `funduq_contract`'s — the link-open pair
(`funduq-connect-provider:{funduq_key}:{ticket}:{provider_nonce}` and
`funduq-connect-funduq:{ticket}:{provider_nonce}`), vectored in
docs/upstream-contract-vectors.json (vendored from upstream's
contract-vectors.json at revision 7) — and this module only re-exports
them beside the version number, which remains a serving decision.

**What v4 changed.** Versions 1–3 were a challenge-response run over the
socket itself: hello, a challenge funduq signed, a proof, a welcome. The
ticket flow moves the freshness out of band — `POST /tickets` mints a
single-use, ~60s ticket naming the key it admits, and the provider signs
it *before* connecting — so the socket handshake collapses to two frames:

    provider → hello    { version: 4, publicKey, ticket, nonce, proof,
                          maxConcurrentRuns? }     # /ws/kyok: no maxConcurrentRuns
    souk     → welcome  { funduqPublicKey, answer }

The proof is `sign_connect(funduq_public_key, ticket, provider_nonce)`:
it names the recipient, so a proof one funduq coaxes out cannot be
relayed to attach at another, and it answers a ticket funduq chose and
destroys, so a recording is worthless. The names a link will serve are
deliberately *not* in it any more — registration happens on the open
link, and a ticket issued to one key cannot be replayed at all.

funduq no longer signs first; the shape of the mutual-identity property
changed with the flow. The proof binds the funduq key the provider
learned over TLS at ticket time, and `answer` — funduq's signature over
`funduq_connect_payload(ticket, provider_nonce)`, relayed in the welcome
— proves possession of it. A provider that pins verifies the answer
(`confirm_connect` / `WrongFunduq` in the SDK) before treating the link
as open.
"""

from __future__ import annotations

# Re-exported so the sockets and the tests keep one import site for
# handshake material. These are upstream's statements — object identity
# is asserted in tests/test_wire_vectors.py so they cannot drift into
# local restatements.
from funduq_contract import (  # noqa: F401
    funduq_connect_payload,
    new_nonce,
    provider_connect_payload,
)

# The wire version a provider must declare. Bumped when the frames or the
# signed bytes change, so a mismatch is refused by name instead of failing
# as a bad signature — which is the same symptom as an attack and would
# send whoever is debugging it somewhere unhelpful.
#
# v4: the ticket handshake (two frames, proof computed before connecting)
# replaced the in-band challenge-response, and registration moved onto the
# open link. v2/v3 partners fail here by name, not by signature.
WIRE_VERSION = 4
