"""The object a real provider actually attaches to its model client,
against the real app.

Every other KYOK test in this repo writes the signing payload out by hand,
and that is deliberate — a shared helper would agree with itself, so a
change to core's payload has to show up as two independent statements
disagreeing. What that independence cannot catch is `KyokSigningAuth`
drifting from *both* of them: it is the one implementation nobody's
hand-written copy stands in for, and it is the one every provider ships.
(It builds its payload from `funduq_contract.kyok_call_payload`, via the
provider SDK's re-export, so today the drift it guards against is the
socket around that call — header names, hashing, the token lift.)
"""

from __future__ import annotations

import asyncio
import json

import httpx
from funduq import repo
from funduq.kyok import issue_kyok_token
from souk_agent_sdk.kyok_auth import KyokSigningAuth

from souk_server.server import create_app

from tests.conftest import TEST_SIGNING_SECRET


class HoldingAgent:
    async def run_stream(self, agent_name: str, run_input):
        yield {
            "type": "RUN_STARTED",
            "threadId": run_input.thread_id,
            "runId": run_input.run_id,
        }
        await asyncio.Event().wait()


async def test_the_signer_a_provider_ships_is_accepted_by_this_gateway(souk, serve):
    """No hand-written headers anywhere in this test. The token comes from
    core, the signature from the SDK, and the verification from the
    gateway — three separate statements of the same payload, meeting for
    the first time."""
    served = await serve(HoldingAgent(), "greeter")
    thread_id = await souk.create_thread(served.ref())
    async with souk.session() as session:
        created = await repo.create_run(session, thread_id, served.ref(), "ag-ui", {})
        await session.commit()
    run_id = created["run_id"]
    souk.enqueue_run(
        run_id,
        served.ref(),
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
    token = issue_kyok_token(run_id, served.ref(), TEST_SIGNING_SECRET)
    try:
        transport = httpx.ASGITransport(app=create_app(souk))
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post(
                "/kyok/v1/chat/completions",
                content=json.dumps({"model": "kyok", "messages": []}).encode(),
                headers={"Authorization": f"Bearer {token}", "content-type": "application/json"},
                auth=KyokSigningAuth(served.identity._key),
            )

        # 503 is the *pass*: authorization succeeded, and the call moved on
        # to resolving a KYOK binding this test never made. A signature the
        # gateway rejected would be 401, which is the failure being guarded.
        assert resp.status_code == 503, resp.text
    finally:
        souk.broker.forget(run_id)
