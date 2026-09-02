"""Call-time identity proof for souk's Keep Your Own Key relay (see
docs/keep-your-own-key.md and souk.api_llm_bridge in the souk repo).

A KYOK token (souk.kyok) is an unforgeable bearer credential, but a
bearer credential alone only proves "whoever's calling holds a copy of
what souk minted" — not *who* is actually calling. An OpenAI-compatible
model client normally sends the exact same static `api_key` on every
request, which can't carry any fresher proof than that.

This `httpx.Auth` signs each outgoing request afresh with this provider's
own identity key — the same Ed25519 keypair already proven when its link
opened (see souk_agent_sdk.identity) — so souk can verify, per call,
that whoever is presenting the token really holds the private key for
the agent it names. Binding the signature to the token, a timestamp, and
a hash of the request body (not just the token alone) means a captured
signature can't be replayed against a different request, and the
timestamp bounds how long a captured one is usable at all.
"""

from __future__ import annotations

import hashlib
import time
from collections.abc import Generator

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from funduq_provider_sdk.identity import kyok_call_payload

from souk_agent_sdk.identity import sign


class KyokSigningAuth(httpx.Auth):
    """Pass as `http_client`'s `auth=` (or attach to any httpx client used
    for KYOK completion calls) alongside the usual `api_key`/`base_url` —
    this only adds headers, it never touches Authorization/api_key
    itself.
    """

    requires_request_body = True

    def __init__(self, private_key: Ed25519PrivateKey) -> None:
        self._private_key = private_key

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        token = request.headers.get("authorization", "").removeprefix("Bearer ")
        timestamp = int(time.time())
        body_hash = hashlib.sha256(request.content).hexdigest()
        # From funduq_provider_sdk (which re-exports funduq_contract's
        # statement) rather than spelled out here. What a provider signs
        # is the contract package's statement to make, and this one is
        # only the socket around it — written out a second time, the
        # operation prefix is a thing to forget.
        payload = kyok_call_payload(token, timestamp, body_hash)
        request.headers["X-Souk-Kyok-Timestamp"] = str(timestamp)
        request.headers["X-Souk-Kyok-Signature"] = sign(self._private_key, payload)
        yield request
