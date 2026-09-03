"""KYOK's one remaining HTTP route: `POST /kyok/v1/chat/completions`
(souk_server.api_llm_bridge) — the provider-facing, OpenAI-compatible
side, and every way its two-part authorization refuses a call. The
answering side of the relay is `WS /ws/kyok`; its round trips live in
tests/test_ws_kyok.py.

A KYOK token names `(provider_key, agent_name)`, and the signature it
demands is checked against `token.agent.provider_key` — the same key
that registered the name. The call payload is upstream's
`funduq_contract.kyok_call_payload` (`funduq-kyok-call:{token}:
{timestamp}:{sha256(body)}`), written out by hand here deliberately: a
shared helper would agree with itself, so a change to core's payload has
to show up as two independent statements disagreeing
(test_kyok_shipped_signer.py covers the third statement, the SDK's).

The broker refuses to enqueue a run for an agent nobody serves now, so
every live run here really is held by a provider — a `HoldingAgent`
sitting mid-run, which is exactly the seat a real KYOK caller occupies.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time

from funduq import repo
from funduq.kyok import issue_kyok_token
from funduq.models import AgentRef

from tests.conftest import TEST_SIGNING_SECRET


def _kyok_headers(bearer: str, private_key, body: bytes) -> dict:
    """Mirrors souk_agent_sdk.KyokSigningAuth.auth_flow — reimplemented
    here because that is the caller's half and this suite is testing the
    server's."""
    timestamp = str(int(time.time()))
    body_hash = hashlib.sha256(body).hexdigest()
    payload = f"funduq-kyok-call:{bearer}:{timestamp}:{body_hash}".encode()
    signature = private_key.sign(payload).hex()
    return {
        "Authorization": f"Bearer {bearer}",
        "X-Souk-Kyok-Timestamp": timestamp,
        "X-Souk-Kyok-Signature": signature,
    }


def _token(run_id: str, agent: AgentRef) -> str:
    return issue_kyok_token(run_id, agent, TEST_SIGNING_SECRET)


class HoldingAgent:
    """Accepts its run and holds it open, the way an agent mid-KYOK-call
    does."""

    async def run_stream(self, agent_name: str, run_input):
        yield {
            "type": "RUN_STARTED",
            "threadId": run_input.thread_id,
            "runId": run_input.run_id,
        }
        await asyncio.Event().wait()


async def _live_run(souk, serve, *names: str):
    """A served agent holding a live, dispatched run — with real thread
    and run rows behind it, because event persistence has foreign keys to
    satisfy and a run whose first event fails to persist is failed."""
    served = await serve(HoldingAgent(), *names)
    ref = served.ref()
    thread_id = await souk.create_thread(ref)
    async with souk.session() as session:
        created = await repo.create_run(session, thread_id, ref, "ag-ui", {})
        await session.commit()
    run_id = created["run_id"]
    souk.enqueue_run(
        run_id,
        ref,
        thread_id,
        {
            "threadId": thread_id,
            "runId": run_id,
            "state": None,
            "messages": [],
            "tools": [],
            "context": [],
            "forwardedProps": None,
        },
        "ag-ui",
    )
    return served, run_id


async def test_chat_completions_without_bearer_401s(client):
    resp = await client.post("/kyok/v1/chat/completions", content=b"{}")
    assert resp.status_code == 401


async def test_chat_completions_with_invalid_token_401s(client):
    resp = await client.post(
        "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": "Bearer not-a-real-token"}
    )
    assert resp.status_code == 401


async def test_chat_completions_run_not_in_broker_403s(client, souk, register):
    served = await register("greeter")
    token = _token("run_never_started", served.ref())
    resp = await client.post(
        "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
    )
    assert resp.status_code == 403


async def test_chat_completions_agent_mismatch_403s(client, souk, serve):
    """The token names a different agent than the run is for. Both halves
    of the pair are checked, so this covers the same-provider case too —
    a provider cannot spend one of its own agents' runs under another of
    its names."""
    served, run_id = await _live_run(souk, serve, "greeter", "translator")
    try:
        token = _token(run_id, served.ref("translator"))
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_same_name_under_another_provider_403s(
    client, souk, serve, register
):
    """The name matches; the key does not — two providers offering
    `greeter` is the case the demo market has on purpose."""
    mine, run_id = await _live_run(souk, serve, "greeter")
    theirs = await register("greeter")
    try:
        token = _token(run_id, theirs.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_cancelled_run_403s(client, souk, serve):
    served, run_id = await _live_run(souk, serve, "greeter")
    # Reaching in deliberately. `souk.cancel_run` on the run would settle
    # it through the provider and forget it, so the route would 403 on
    # "no such run" and this test would pass without ever exercising the
    # cancel check it is named for.
    souk.broker._runs[run_id].cancel_requested = True
    try:
        token = _token(run_id, served.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_missing_signature_headers_401s(client, souk, serve):
    served, run_id = await _live_run(souk, serve, "greeter")
    try:
        token = _token(run_id, served.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions", content=b"{}", headers={"Authorization": f"Bearer {token}"}
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_stale_timestamp_401s(client, souk, serve):
    served, run_id = await _live_run(souk, serve, "greeter")
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        body_hash = hashlib.sha256(body).hexdigest()
        stale_timestamp = str(int(time.time()) - 3600)
        payload = f"funduq-kyok-call:{token}:{stale_timestamp}:{body_hash}".encode()
        signature = served.identity._key.sign(payload).hex()
        resp = await client.post(
            "/kyok/v1/chat/completions",
            content=body,
            headers={
                "Authorization": f"Bearer {token}",
                "X-Souk-Kyok-Timestamp": stale_timestamp,
                "X-Souk-Kyok-Signature": signature,
            },
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_malformed_timestamp_401s(client, souk, serve):
    served, run_id = await _live_run(souk, serve, "greeter")
    try:
        token = _token(run_id, served.ref())
        resp = await client.post(
            "/kyok/v1/chat/completions",
            content=b"{}",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Souk-Kyok-Timestamp": "not-a-number",
                "X-Souk-Kyok-Signature": "deadbeef",
            },
        )
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_delisted_agent_403s(client, souk, serve):
    """The token names a real, live run — but the agent behind it has
    been delisted since, so there's no provider key on file to verify the
    call-time signature against. (An agent that was *never* registered
    cannot reach this state any more: the broker refuses to enqueue for
    an unserved name, so the delisting has to happen underneath the run.)
    """
    served, run_id = await _live_run(souk, serve, "greeter")
    async with souk.engine.begin() as conn:
        await conn.exec_driver_sql("DELETE FROM run_events")
        await conn.exec_driver_sql("DELETE FROM runs")
        await conn.exec_driver_sql("DELETE FROM threads")
        await conn.exec_driver_sql("DELETE FROM agents")
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        headers = _kyok_headers(token, served.identity._key, body)
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 403
    finally:
        souk.broker.forget(run_id)


async def test_chat_completions_bad_signature_401s(client, souk, serve):
    served, run_id = await _live_run(souk, serve, "greeter")
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        headers = _kyok_headers(token, served.identity._key, body)
        headers["X-Souk-Kyok-Signature"] = "00" * 64
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_a_signature_from_another_identity_401s(client, souk, serve, new_identity):
    """Holding the token is not enough — that is the whole point of the
    second part. The signature must come from the key that registered the
    name the token carries."""
    served, run_id = await _live_run(souk, serve, "greeter")
    imposter = new_identity()
    try:
        token = _token(run_id, served.ref())
        body = b"{}"
        headers = _kyok_headers(token, imposter._key, body)
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 401
    finally:
        souk.broker.forget(run_id)


async def test_a_run_with_no_kyok_binding_503s(client, souk, serve):
    """Authorization passed — the token is real, the run live, the
    signature the agent's own — but nothing ever bound this run to an LLM
    offering. Fail fast, the same shape as an offline agent."""
    served, run_id = await _live_run(souk, serve, "greeter")
    try:
        token = _token(run_id, served.ref())
        body = json.dumps({"model": "kyok", "messages": []}).encode()
        headers = {**_kyok_headers(token, served.identity._key, body), "content-type": "application/json"}
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 503
    finally:
        souk.broker.forget(run_id)


async def test_a_binding_to_an_unattached_offering_503s(client, souk, serve):
    """Bound at run start, but the LLM provider is not attached at call
    time. Resolution is per call and this is its fast-fail — the property
    that lets a provider drop and re-attach mid-run."""
    from funduq.kyok import KyokBinding
    from funduq.models import LlmRef

    served, run_id = await _live_run(souk, serve, "greeter")
    souk.kyok_relay.bind_run(
        run_id,
        KyokBinding(llm_provider=LlmRef(provider_key="aa" * 32, name="gpt-nowhere")),
    )
    try:
        token = _token(run_id, served.ref())
        body = json.dumps({"model": "kyok", "messages": []}).encode()
        headers = {**_kyok_headers(token, served.identity._key, body), "content-type": "application/json"}
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 503
    finally:
        souk.broker.forget(run_id)


async def test_a_body_that_is_not_a_chat_completion_request_is_a_400(
    client, souk, serve, monkeypatch
):
    """New at contract revision 11, and it fails *late* on purpose.

    A completion body is validated as OpenAI's own request shape now
    (`funduq_contract.CompletionBody`, extension keys allowed through
    verbatim) instead of being relayed as whatever JSON arrived — so a
    body missing `model`, which every real OpenAI client sends, is a 400
    with core's own words rather than a puzzling refusal from the far side
    of somebody else's socket.

    Bound *and* attached is what makes this a statement about the body:
    the validation sits after both are resolved, so a run with no binding
    (or a binding to nothing attached) answers 503 first and a test that
    skipped either would pass without ever reaching the check it names.
    Hence the stub link — nothing here is testing the socket.
    """
    from funduq.kyok import KyokBinding
    from funduq.models import LlmRef

    class _Attached:
        public_key = "aa" * 32

        def complete(self, request):  # pragma: no cover - never reached
            raise AssertionError("a refused body must never reach the link")

    served, run_id = await _live_run(souk, serve, "greeter")
    offering = LlmRef(provider_key="aa" * 32, name="gpt-nowhere")
    souk.kyok_relay.bind_run(run_id, KyokBinding(llm_provider=offering))
    monkeypatch.setattr(
        type(souk.kyok_relay), "serving", lambda self, ref: _Attached()
    )
    try:
        token = _token(run_id, served.ref())

        # No `model`: valid JSON, plausible-looking, not a chat-completion
        # request.
        body = json.dumps({"messages": [{"role": "user", "content": "hi"}]}).encode()
        headers = {
            **_kyok_headers(token, served.identity._key, body),
            "content-type": "application/json",
        }
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)

        assert resp.status_code == 400, resp.text
        assert "chat-completion request" in resp.json()["detail"]

        # Not valid JSON at all is the older, adjacent 400 — same status,
        # different sentence, and both must reach the caller as words.
        body = b"{not json"
        headers = {
            **_kyok_headers(token, served.identity._key, body),
            "content-type": "application/json",
        }
        resp = await client.post("/kyok/v1/chat/completions", content=body, headers=headers)
        assert resp.status_code == 400
        assert "valid JSON" in resp.json()["detail"]
    finally:
        souk.broker.forget(run_id)
