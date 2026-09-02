"""Deletion is connection-scoped now — what the signed HTTP orders
became.

The old `DELETE /agents` / `DELETE /llm-providers` routes took a signed
order because HTTP had no other way to know the key; the open link
proved it once, so deletion moved onto the socket (`deleteAgent` /
`deleteModel` frames — the happy paths and the has-history refusal live
with their sockets in test_ws_provider.py / test_ws_kyok.py). What this
file holds is the boundary the signed order used to draw, restated for
links:

- the HTTP mutation routes are gone, not relaxed;
- a link only ever deletes out of its *own* namespace — another key's
  agent of the same name is simply not there to delete, which is the
  connection-scoped reading of "the wrong signature is a 401 not a 404";
- nothing off-link can delete at all: the link is the credential, and
  core refuses a connection that never opened one.
"""

from __future__ import annotations

import json

import httpx
from httpx_ws import aconnect_ws
from httpx_ws.transport import ASGIWebSocketTransport

import pytest

from funduq.errors import InvalidRegistration
from souk_server.server import create_app

from tests.conftest import Identity


async def test_the_signed_http_mutation_routes_are_gone(client, register):
    """Gone, not merely refusing: registration and deletion have exactly
    one road now, the open link. (405 rather than 404 where the path
    still exists for GET.)"""
    served = await register("retiree")
    order = {"name": "retiree", "public_key": served.public_key, "signature": "00", "timestamp": 0}

    assert (await client.post("/agents/register", json={})).status_code in (404, 405)
    assert (await client.post("/llm-providers/register", json={})).status_code in (404, 405)
    assert (await client.request("DELETE", "/agents", json=order)).status_code in (404, 405)
    assert (await client.request("DELETE", "/llm-providers", json=order)).status_code in (404, 405)
    # The read side stayed.
    assert (await client.get("/agents")).status_code == 200
    assert (await client.get("/llm-providers")).status_code == 200


async def test_a_link_deletes_only_out_of_its_own_namespace(souk, client, register):
    """Two providers offer `greeter`. One deletes the name over its link:
    its own record goes, the other's survives untouched — a link holds no
    lever over anybody else's roster, and there is nothing to sign or to
    forge to reach for one."""
    victim = await register("greeter")

    async with httpx.AsyncClient(
        transport=ASGIWebSocketTransport(app=create_app(souk)), base_url="http://test"
    ) as ws_client:
        async with aconnect_ws("http://test/ws/provider", ws_client) as ws:
            deleter = Identity()
            await ws.send_text(json.dumps(deleter.hello(souk)))
            assert json.loads(await ws.receive_text(timeout=2))["type"] == "welcome"
            await ws.send_text(
                json.dumps({"type": "register", "agents": [{"name": "greeter"}]})
            )
            assert json.loads(await ws.receive_text(timeout=2))["type"] == "registered"

            await ws.send_text(json.dumps({"type": "deleteAgent", "name": "greeter"}))
            assert json.loads(await ws.receive_text(timeout=2)) == {
                "type": "deleted",
                "name": "greeter",
            }

    roster = (await client.get("/agents")).json()["agents"]
    assert [(a["provider_key"], a["name"]) for a in roster] == [
        (victim.public_key, "greeter")
    ]


async def test_nothing_off_link_can_delete(souk, register):
    """Core's own boundary, driven from this side: a connection that
    never opened a link is refused by name. The gateway holds no other
    entry point — this is what `require_open` guards for every deletion
    the socket relays."""
    served = await register("doomed")

    class NotALink:
        public_key = served.public_key

    with pytest.raises(InvalidRegistration):
        await souk.delete_agent(NotALink(), "doomed")
    assert await souk.get_agent(served.ref()) is not None
