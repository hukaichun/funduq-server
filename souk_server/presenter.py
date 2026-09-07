"""The authenticating seat: who presented this request.

Contract revision 21 refuses a chain presented at a door that cannot say
who presented it (`PresenterRequired`). A chain proves *origin* — that
these actors signed these hops — and proves nothing about possession: the
party holding the bytes may be anyone who ever saw them. Core therefore
asks the transport one question, through `presenter_key_of` on the A2A
door and `presenter_key=` on the AG-UI one, and refuses a chain when the
answer is nobody. This module is that answer.

**Scope, deliberately small.** Only a caller that *presents a chain*
needs to authenticate. A browser or curl calling an agent with no chain
keeps working exactly as before — no header, `None`, no change. So this
is not "auth on the gateway"; it is "a party that already holds an
Ed25519 key may prove it at the door", and today that party is always a
provider delegating to another agent (it has a key because it needed one
to attach).

**The proof.** Header `Funduq-Presenter` — no `X-` prefix, deprecated by
RFC 6648 in 2012; the two `X-Souk-Kyok-*` headers here predate this repo
owning the question. Its value is compact JSON, the same three fields
every other proof in this system uses:

    {"publicKey": "…", "timestamp": 1757260000, "signature": "…"}

signed over

    funduq-server-presenter:{public_key}:{timestamp}:{sha256hex(body)}

Three properties, each earning its place:

- **the body hash** binds the proof to *this* request, so a captured
  header cannot be replayed onto a different call — the same reason
  `kyok_call_payload` binds a body hash, and this is modelled on it;
- **the timestamp** bounds capture-and-replay of the same request to the
  60-second window the cancel/view family already uses
  (`funduq.identity.is_timestamp_fresh`, so there is one window);
- **the public key inside the payload** means the proof names the key it
  claims, so it cannot be presented as an answer to a different question.

**The domain tag is ours on purpose.** `funduq-server-presenter:`, not
`funduq-presenter:`. The `funduq-*` payload namespace is upstream's, and
this payload has no upstream definition — `funduq_contract` publishes six
payload builders and none authenticates a write. Squatting the namespace
would mint a name that looks canonical and is not, the exact failure the
contract vectors exist to prevent. So: implement under our own tag, file
it upstream, and when upstream ships a `presenter_payload`, swap and
delete this one.

**A bad header is `None`, never an error.** Absent, unparseable, stale,
or forged all read the same way here, and core then refuses the chain
itself with `PresenterRequired` (401). One refusal in one place beats two
that can disagree — and a chainless caller with a broken header keeps
working, which is right: nothing it sent depended on the proof. A header
that verifies but whose key is not the chain's last hop is likewise not
ours to pre-empt: that is core's `InvalidChain`.
"""

from __future__ import annotations

import hashlib
import json
import logging

from funduq.identity import is_timestamp_fresh
from funduq_contract import verify_signature

logger = logging.getLogger("souk.presenter")

# The header a presenter proof rides in, lowercased: Starlette hands
# headers over already folded, and a2a's context builder copies that
# mapping verbatim into `context.state["headers"]`.
PRESENTER_HEADER = "funduq-presenter"

# This repo's domain tag, not upstream's namespace — see the module
# docstring. Published in docs/wire-vectors.json, which is what the SDKs
# and the Go probe sign against.
PRESENTER_DOMAIN = "funduq-server-presenter"


def presenter_payload(public_key: str, timestamp: int, body: bytes) -> bytes:
    """The exact bytes a presenter signs to claim `public_key` for `body`."""
    return (
        f"{PRESENTER_DOMAIN}:{public_key}:{timestamp}:"
        f"{hashlib.sha256(body).hexdigest()}"
    ).encode()


def presenter_key_of(raw_header: str | None, body: bytes) -> str | None:
    """The key whoever sent `body` proved, or `None` for nobody.

    `None` covers every way the claim fails — no header, malformed JSON,
    missing fields, a timestamp outside the window, a signature that does
    not verify. The refusal that matters belongs to core, which sees a
    chain with no presenter and says so by name.
    """
    if not raw_header:
        return None
    try:
        proof = json.loads(raw_header)
        public_key = proof["publicKey"]
        timestamp = int(proof["timestamp"])
        signature = proof["signature"]
    except (TypeError, ValueError, KeyError):
        logger.debug("ignoring a malformed %s header", PRESENTER_HEADER)
        return None
    if not isinstance(public_key, str) or not isinstance(signature, str):
        return None
    if not is_timestamp_fresh(timestamp):
        logger.debug("a %s proof arrived outside the freshness window", PRESENTER_HEADER)
        return None
    if not verify_signature(public_key, signature, presenter_payload(public_key, timestamp, body)):
        logger.debug("a %s signature did not verify", PRESENTER_HEADER)
        return None
    return public_key
