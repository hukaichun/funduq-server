"""A provider's identity to any souk it connects to is its Ed25519
keypair — not an account souk issues. Generated once and persisted to
disk so restarting this process is still the same identity: a fresh key
is a fresh, unrelated provider, and everything addressed to the old one
(a thread's bound authority, a chain hop already signed) keeps pointing
at the orphaned identity, not this one. Treat the key file like any
other credential — back it up, don't commit it.

The byte-level truths — what a signature covers, what a chain hop is —
live upstream in `funduq-contract` now, and this module only re-shapes
them under the names this repo's providers already import. The helpers
here work on a raw `cryptography` private key; `funduq_provider_sdk.
ProviderIdentity` wraps the same operations for code that wants the
object form (and `SoukProvider` holds one of those).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from funduq_contract import extend_chain, new_chain, sign_hop  # noqa: F401 (sign_hop re-exported)


def load_or_create_identity(path: str | Path) -> Ed25519PrivateKey:
    path = Path(path)
    if path.exists():
        return Ed25519PrivateKey.from_private_bytes(path.read_bytes())
    private_key = Ed25519PrivateKey.generate()
    raw = private_key.private_bytes_raw()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    os.chmod(path, 0o600)
    return private_key


def public_key_hex(private_key: Ed25519PrivateKey) -> str:
    return private_key.public_key().public_bytes_raw().hex()


def sign(private_key: Ed25519PrivateKey, payload: bytes) -> str:
    return private_key.sign(payload).hex()


def new_actor_chain(private_key: Ed25519PrivateKey, subject: dict[str, Any] | None = None) -> list[str]:
    """Starts a fresh actor chain — one hop, whose signer is the head.

    Delegates to `funduq_contract.new_chain`, which is the statement of
    what a hop is (`{actorPublicKey, prevHash}`, and nothing else — no
    subject, no timestamps: contract revision 1 removed time from hops,
    and the chain now carries keys only). `subject` is accepted for
    callers written against the old shape and deliberately ignored: whom
    a key represents is a separate, opt-in disclosure upstream (a
    voucher), never a hop field, so there is nowhere honest to put it.
    """
    return new_chain(private_key)


def extend_actor_chain(private_key: Ed25519PrivateKey, prev_chain: list[str]) -> list[str]:
    """Adds one more hop to a chain this provider *received* as a caller
    (e.g. forwarded to it via A2A/AG-UI metadata) and is now relaying
    onward. The chain's integrity is souk's to check when it is used;
    this side only signs its own hop over the predecessor's hash —
    `funduq_contract.extend_chain`, verbatim.
    """
    if not prev_chain:
        raise ValueError(
            "extend_actor_chain requires a non-empty prev_chain — use new_actor_chain to originate one"
        )
    return extend_chain(private_key, prev_chain)
