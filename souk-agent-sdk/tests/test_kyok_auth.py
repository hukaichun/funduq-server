"""Covers KyokSigningAuth — see souk_agent_sdk/kyok_auth.py's own
docstring. The bytes it signs are funduq-contract's `kyok_call_payload`;
the gateway's KYOK door reconstructs the same payload server-side and
must agree byte-for-byte, which the reconstruction tests below pin.
"""

from __future__ import annotations

import hashlib

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from souk_agent_sdk.kyok_auth import KyokSigningAuth


def _signed_request(private_key: Ed25519PrivateKey, token: str = "kyoktoken123", body: bytes = b"{}") -> httpx.Request:
    request = httpx.Request(
        "POST",
        "https://souk.example/kyok/v1/chat/completions",
        headers={"Authorization": f"Bearer {token}"},
        content=body,
    )
    auth = KyokSigningAuth(private_key)
    gen = auth.auth_flow(request)
    return next(gen)


def test_auth_flow_sets_timestamp_and_signature_headers():
    key = Ed25519PrivateKey.generate()
    signed = _signed_request(key)

    assert "X-Souk-Kyok-Timestamp" in signed.headers
    assert "X-Souk-Kyok-Signature" in signed.headers
    assert signed.headers["X-Souk-Kyok-Timestamp"].isdigit()


def test_auth_flow_leaves_method_url_and_authorization_untouched():
    key = Ed25519PrivateKey.generate()
    signed = _signed_request(key, token="unchanged-token")

    assert signed.method == "POST"
    assert str(signed.url) == "https://souk.example/kyok/v1/chat/completions"
    assert signed.headers["Authorization"] == "Bearer unchanged-token"


def test_signature_verifies_against_souk_side_reconstruction():
    """Reconstructs exactly what the gateway's KYOK door does server-side
    — funduq-contract's `kyok_call_payload(bearer, timestamp,
    sha256(body))` — and checks the signature verifies against the
    matching public key. Spelled out as bytes rather than imported, so
    this test stays the independent twin of the contract: a change to
    either side's payload format should break it.
    """
    key = Ed25519PrivateKey.generate()
    public_key = key.public_key()
    body = b'{"messages": []}'
    signed = _signed_request(key, token="kyoktoken123", body=body)

    timestamp = signed.headers["X-Souk-Kyok-Timestamp"]
    signature = bytes.fromhex(signed.headers["X-Souk-Kyok-Signature"])
    body_hash = hashlib.sha256(body).hexdigest()
    payload = f"funduq-kyok-call:kyoktoken123:{timestamp}:{body_hash}".encode()

    public_key.verify(signature, payload)  # raises InvalidSignature on failure


def test_the_signed_bytes_are_the_contract_packages():
    """The auth flow must sign funduq-contract's statement, not a local
    restatement — object identity makes an upstream byte change a change
    here by construction."""
    import funduq_contract
    from souk_agent_sdk import kyok_auth

    assert kyok_auth.kyok_call_payload is funduq_contract.kyok_call_payload


def test_different_body_produces_a_different_signature():
    key = Ed25519PrivateKey.generate()
    signed_a = _signed_request(key, body=b'{"a": 1}')
    signed_b = _signed_request(key, body=b'{"a": 2}')

    assert signed_a.headers["X-Souk-Kyok-Signature"] != signed_b.headers["X-Souk-Kyok-Signature"]


def test_bearer_prefix_is_stripped_before_signing():
    """auth_flow reads the token via `.removeprefix("Bearer ")` — signing
    with the raw `Authorization` header value (prefix included) would
    silently disagree with souk's own reconstruction, which uses the
    bare token (see api_llm_bridge._bearer_token).
    """
    key = Ed25519PrivateKey.generate()
    public_key = key.public_key()
    body = b"{}"
    signed = _signed_request(key, token="bare-token", body=body)

    timestamp = signed.headers["X-Souk-Kyok-Timestamp"]
    signature = bytes.fromhex(signed.headers["X-Souk-Kyok-Signature"])
    body_hash = hashlib.sha256(body).hexdigest()

    # Signed against the bare token, not "Bearer bare-token" — this is
    # the payload souk itself reconstructs and expects to verify.
    correct_payload = f"funduq-kyok-call:bare-token:{timestamp}:{body_hash}".encode()
    public_key.verify(signature, correct_payload)
