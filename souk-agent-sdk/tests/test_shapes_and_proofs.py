"""The two dump rules, the registration model, and the proofs this SDK
signs — the things that used to be policed by upstream constants and a
codec, and are now nobody's job but this package's.

Contract revision 11 withdrew every field-list constant
(`REGISTRATION_FIELDS`, `DELIVERED_RUN_FIELDS`, `CONNECTED_PROVIDER_ATTRS`,
`LINK_QUERY_METHODS`, …) along with the sans-io machines and their codec:
the models are the single definition, so there is nothing left for a list
to compare against. What the codec *also* did, and nothing upstream does
now, is enforce which dump each direction takes — so that lives here, in
`souk_agent_sdk.client.dump_envelope` / `dump_event`, with these tests
under it.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest
from ag_ui.core import RunAgentInput, RunStartedEvent
from funduq_contract import DeliveredRun, Registration
from funduq_provider_sdk import AgentHandle, ProviderIdentity, verify_signature

from souk_agent_sdk import a2a_client
from souk_agent_sdk.client import dump_envelope, dump_event

UPSTREAM_VECTORS = json.loads(
    (Path(__file__).parent.parent.parent / "docs" / "upstream-contract-vectors.json").read_text()
)


def _vector(kind: str) -> dict:
    return next(v for v in UPSTREAM_VECTORS["vectors"] if v["kind"] == kind)


def _test_identity() -> ProviderIdentity:
    """Upstream's published test key, so a signature here is comparable to
    the vector byte for byte rather than merely self-consistent."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

    return ProviderIdentity(
        Ed25519PrivateKey.from_private_bytes(
            bytes.fromhex(UPSTREAM_VECTORS["test_key"]["private_key_hex"])
        )
    )


async def _stream(run_input):  # pragma: no cover - never run
    yield {}


async def _interject(run_input, addressed_run_id):  # pragma: no cover - never run
    yield {}


# --- the registration model --------------------------------------------------


def test_as_registration_returns_a_model_that_declares_the_interjection_hook():
    """`as_registration()` returns a `Registration`, not a dict (revision
    11), and derives `takes_interjections` from the hook itself (revision
    12) — so the card cannot claim a capability the router will not
    honour. Anything still doing `**registration` or `registration["name"]`
    breaks here rather than at the far end of a socket."""
    plain = AgentHandle(name="plain", run_stream=_stream).as_registration()
    listens = AgentHandle(
        name="listens", run_stream=_stream, interject_stream=_interject
    ).as_registration()

    assert isinstance(plain, Registration)
    assert plain.takes_interjections is False
    assert listens.takes_interjections is True
    # camelCase on the wire is the model's own alias, not a mapping this
    # package maintains.
    assert dump_envelope(listens)["takesInterjections"] is True
    assert dump_envelope(plain)["name"] == "plain"


def test_a_registration_refuses_a_field_it_does_not_declare():
    """Every crossing shape is `extra="forbid"` now, which is why the
    transport strips its own vocabulary (`type`, `requestId`) before
    validating rather than handing a frame over whole."""
    with pytest.raises(Exception):
        Registration(name="x", takesSomethingElse=True)


# --- the two dump rules, which pull opposite ways ----------------------------


def test_an_envelope_keeps_its_nulls():
    """`RunAgentInput` has required fields that are legitimately null.
    Stripping them makes the far side's `model_validate` fail, which the
    transport answers as a *permanent* refusal — a good run turned into a
    dead one, reported as the provider's fault."""
    run = DeliveredRun(
        runId="r1",
        agentName="echo",
        runInput=RunAgentInput(
            threadId="t1",
            runId="r1",
            state=None,
            messages=[],
            tools=[],
            context=[],
            forwardedProps=None,
        ),
        threadId="t1",
    )
    frame = dump_envelope(run)
    assert frame["runId"] == "r1" and frame["agentName"] == "echo"
    # `forwardedProps` is the field that carries the rule now: required by
    # the model and legitimately null, so `exclude_none` would drop it and
    # the far side could not rebuild the input. `state` is NOT the example
    # to use — ag-ui 0.1.22 made it `Any = None` and omits it when unset,
    # deliberately ("a bare null on the wire reads as absent"), so
    # asserting it survives pins an older ag-ui rather than this rule. That
    # skew is exactly what broke a full stack run: a gateway on 0.1.22
    # emitting no `state` against a provider on 0.1.19 requiring one, which
    # is why every pyproject here floors ag-ui-protocol at 0.1.22.
    assert "forwardedProps" in frame["runInput"]
    assert frame["runInput"]["forwardedProps"] is None
    # And it survives the round trip that a run frame actually makes.
    assert DeliveredRun.model_validate(frame) == run


def test_a_delivered_run_carries_funduqs_keys_under_one_key_of_its_own():
    """Revision 18 moved everything funduq writes into the delivered bag
    under a single reserved `funduq` key — the KYOK grant, the actor chain,
    the interjection target — and strips a caller's own `funduq` key on
    the way in. So an agent reading `forwardedProps.funduq` knows funduq
    put it there, and a caller's `addressedRunId` can never pass for one.

    Replayed from upstream's published `delivered-run` wire vector, so this
    asserts the shape upstream ships rather than the shape this file
    imagines; `funduq_provider_sdk.runtime` reads the interjection target
    from exactly this path.
    """
    frame = next(v for v in UPSTREAM_VECTORS["wire"] if v["kind"] == "delivered-run")["frame"]
    bag = frame["runInput"]["forwardedProps"]
    assert set(bag) == {"funduq"}
    assert set(bag["funduq"]) == {"kyok", "actorChain"}
    # The envelope round-trips through the model this transport validates
    # with, and the bag survives the trip intact.
    delivered = DeliveredRun.model_validate(frame)
    assert delivered.run_input.forwarded_props == bag
    assert delivered.run_input.forwarded_props["funduq"]["kyok"]["token"]
    assert dump_envelope(delivered)["runInput"]["forwardedProps"] == bag
    # Not asserted: byte equality of the whole `runInput`. Upstream's
    # vector was generated by an ag-ui that emits `parentRunId: null` and
    # `resume: null`, while 0.1.22 — this repo's floor — omits both when
    # unset, the same "a bare null reads as absent" choice it made for
    # `state`. Both are optional with a None default on either side, so
    # the skew is invisible to a run; asserting it here would pin an ag-ui
    # version rather than the nesting rule this test is about.


def test_an_event_strips_its_nulls():
    """The opposite rule, and just as load-bearing: an AG-UI event is
    relayed into somebody's stream, and `timestamp: null` / `rawEvent:
    null` are fields the caller never sent and should never see."""
    event = RunStartedEvent(threadId="t1", runId="r1")
    assert event.timestamp is None
    wire = dump_event(event)
    assert wire["type"] == "RUN_STARTED"
    assert wire["threadId"] == "t1" and wire["runId"] == "r1"
    assert "timestamp" not in wire
    assert "rawEvent" not in wire


def test_an_event_that_is_already_plain_data_is_passed_through():
    """Agents here yield dicts as often as models; nothing is invented for
    them."""
    assert dump_event({"type": "CUSTOM", "value": None}) == {
        "type": "CUSTOM",
        "value": None,
    }


# --- the proofs --------------------------------------------------------------


def test_a_resolve_proof_signs_the_ask_and_matches_upstreams_vector():
    """Revision 16: `sign_resolution(run_id, ask_ids)` — the ask, not the
    clock. Replayed against upstream's published vector, so the sorting,
    the NUL join and the sha256 are checked as bytes rather than as a
    description of them. The vector's ask ids are deliberately unsorted:
    canonicalization is the builder's job, never the caller's."""
    vector = _vector("resolution")
    identity = _test_identity()
    signature = identity.sign_resolution(
        vector["inputs"]["run_id"], vector["inputs"]["ask_ids"]
    )
    assert signature == vector["signature_hex"]
    # Same set, other order: the same signature, because sorting happens
    # inside the payload builder.
    assert (
        identity.sign_resolution(
            vector["inputs"]["run_id"], list(reversed(vector["inputs"]["ask_ids"]))
        )
        == signature
    )
    # And no timestamp rides in it — the wire proof is two fields.
    assert vector["payload_utf8"].startswith("funduq-resolve:")


def test_the_presenter_payload_is_the_bytes_this_repo_defined():
    """Revision 21: souk refuses a chain from a caller it cannot
    authenticate, so `a2a_client` signs `Funduq-Presenter` over

        funduq-server-presenter:{publicKey}:{timestamp}:{sha256hex(body)}

    Hand-built here, deliberately — the payload is *this repo's*, with no
    upstream vector to replay, so the only honest check is to restate the
    bytes independently and compare. If the two ever disagree, one of them
    is the bug and this test says which side moved.

    The domain tag is ours on purpose: `funduq-*` is upstream's namespace
    and funduq_contract publishes no builder for authenticating a
    presenter, so squatting it would mint a name that looks canonical and
    is not.
    """
    identity = _test_identity()
    body = b'{"jsonrpc":"2.0","id":"req_1","method":"SendMessage","params":{}}'
    timestamp = 1_700_000_000

    expected = (
        "funduq-server-presenter:"
        + identity.public_key
        + ":1700000000:"
        + hashlib.sha256(body).hexdigest()
    ).encode()
    assert a2a_client.presenter_payload(
        identity.public_key, timestamp, hashlib.sha256(body).hexdigest()
    ) == expected

    proof = a2a_client.presenter_proof(identity, body, timestamp=timestamp)
    assert proof["publicKey"] == identity.public_key
    assert proof["timestamp"] == timestamp
    assert verify_signature(proof["publicKey"], proof["signature"], expected)

    # And the header is compact JSON of exactly those three fields, under
    # the un-prefixed name (RFC 6648 deprecated `X-` in 2012).
    headers = a2a_client.presenter_headers(identity, body, timestamp=timestamp)
    assert set(headers) == {"Funduq-Presenter"}
    assert headers["Funduq-Presenter"] == json.dumps(proof, separators=(",", ":"))
    assert json.loads(headers["Funduq-Presenter"]) == proof

    # No identity, no header — a chainless caller stays anonymous, which
    # souk keeps serving.
    assert a2a_client.presenter_headers(None, body) == {}


def test_a_presenter_proof_is_bound_to_the_body_it_was_signed_over():
    """The body hash is what stops a captured header being replayed onto a
    different call — the same reason `kyok_call_payload` binds one."""
    identity = _test_identity()
    timestamp = 1_700_000_000
    first = a2a_client.presenter_proof(identity, b"{}", timestamp=timestamp)
    other = a2a_client.presenter_proof(identity, b'{"a":1}', timestamp=timestamp)
    assert first["signature"] != other["signature"]
    assert not verify_signature(
        identity.public_key,
        first["signature"],
        a2a_client.presenter_payload(
            identity.public_key, timestamp, hashlib.sha256(b'{"a":1}').hexdigest()
        ),
    )


async def test_a_chain_without_an_identity_is_refused_here_not_sent():
    """souk answers a chain it cannot attribute with `PresenterRequired`,
    so this request has a known answer before it leaves. Raising locally,
    naming the missing half, beats a 401 three layers away — and beats
    reading the presenter off the chain, which would make the last-hop
    check a tautology and restore the impersonation hole revision 21
    closed."""
    with pytest.raises(a2a_client.PresenterIdentityRequired) as excinfo:
        async for _ in a2a_client.call_agent_streaming(
            "http://souk.example/a2a", "hi", actor_chain=["hop"]
        ):
            pass  # pragma: no cover - the generator raises before yielding
    assert "identity" in str(excinfo.value)
    assert "Funduq-Presenter" in str(excinfo.value)

    with pytest.raises(a2a_client.PresenterIdentityRequired):
        await a2a_client.call_agent(
            "http://souk.example/a2a", "hi", actor_chain=["hop"]
        )


def test_a_chained_send_signs_the_exact_bytes_it_puts_on_the_wire():
    """The proof binds `sha256(body)`, so the bytes signed and the bytes
    sent must be one serialization — not two that happen to agree today.
    `_presented` is where that is guaranteed, and its raw output is what
    the send paths hand httpx as `content=`."""
    identity = _test_identity()
    body = {
        "jsonrpc": "2.0",
        "id": "req_1",
        "method": "SendMessage",
        "params": {"metadata": {"actorChain": ["hop"]}},
    }
    raw, headers = a2a_client._presented(body, identity, actor_chain_present=True)

    assert json.loads(raw) == body
    proof = json.loads(headers[a2a_client.PRESENTER_HEADER])
    assert verify_signature(
        proof["publicKey"],
        proof["signature"],
        a2a_client.presenter_payload(
            proof["publicKey"], proof["timestamp"], hashlib.sha256(raw).hexdigest()
        ),
    )
    assert headers["Content-Type"] == "application/json"


def test_an_interjection_declares_itself_at_the_request_level():
    """funduq has read nothing from a message's own metadata since
    revision 20: the caller's declarations — `actorChain` and the
    interjection target alike — are request-level on both doors. Written
    on the message, the declaration is simply never heard."""
    params = a2a_client._send_message_params(
        "hi", addressed_run_id="run_1", actor_chain=["hop"]
    )
    assert params["metadata"][a2a_client.ADDRESSED_RUN_METADATA_KEY] == "run_1"
    assert params["metadata"]["actorChain"] == ["hop"]
    assert "metadata" not in params["message"]


def test_the_presenter_is_the_key_that_ends_the_chain():
    """The two halves of a delegated call are one key in two shapes: the
    raw key that signs a hop, and the `ProviderIdentity` that signs the
    header. `provider_identity()` is what keeps them the same key — souk
    refuses a header whose key is not the chain's last hop (`InvalidChain`),
    which is a *different* failure from having no header at all, and one a
    caller reading two constructors could easily walk into."""
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from funduq_contract import verify_chain

    from souk_agent_sdk.identity import extend_actor_chain, new_actor_chain, provider_identity

    first = Ed25519PrivateKey.generate()
    last = Ed25519PrivateKey.generate()
    chain = extend_actor_chain(last, new_actor_chain(first))
    identity = provider_identity(last)

    verified = verify_chain(chain)
    assert verified.presenter == identity.public_key
    assert verified.head != identity.public_key  # the head is the first hop

    _, headers = a2a_client._presented(
        {"params": {"metadata": {"actorChain": chain}}},
        identity,
        actor_chain_present=True,
    )
    assert json.loads(headers[a2a_client.PRESENTER_HEADER])["publicKey"] == verified.presenter


def test_the_view_header_is_gone():
    """Revision 21 removed the `view_metadata_of` hook this SDK's
    `X-Funduq-View` fed, and made `presenter_key_of` serve reads and
    writes alike. One mechanism answers both now; a second one that no
    door reads would be a proof nobody checks."""
    assert not hasattr(a2a_client, "VIEW_PROOF_HEADER")
    assert not hasattr(a2a_client, "view_proof")
    assert not hasattr(a2a_client, "view_headers")
    # The contract still publishes the payload — a transport may have a
    # reader sign it for one read — but nothing here signs it.
    import funduq_contract

    assert hasattr(funduq_contract, "view_payload")


def test_delegation_signing_is_gone():
    """Revision 15 deleted the session delegation certificate. Nothing in
    this SDK may grow it back: a grant is the authenticating seat's policy
    now, and `sign_delegation` no longer exists to be called."""
    assert not hasattr(ProviderIdentity, "sign_delegation")
    import funduq_contract

    assert not hasattr(funduq_contract, "delegation_payload")


async def test_the_streaming_send_puts_the_signed_bytes_on_the_wire(monkeypatch):
    """Through httpx, not through `_presented` alone. The signature is over
    `sha256(body)`, so a send path that let httpx re-serialize would sign
    one byte string and transmit another — and the failure would surface as
    a 401 from souk, three layers from the cause. Verified here by
    recomputing the proof from the bytes the transport actually received.
    """
    import httpx

    identity = _test_identity()
    seen: dict = {}

    class _Transport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            seen["content"] = request.content
            seen["headers"] = request.headers
            return httpx.Response(
                200, headers={"content-type": "text/event-stream"}, content=b""
            )

    real = httpx.AsyncClient

    def _client(*args, **kwargs):
        kwargs["transport"] = _Transport()
        return real(*args, **kwargs)

    monkeypatch.setattr(httpx, "AsyncClient", _client)

    async for _ in a2a_client.call_agent_streaming(
        "http://souk.example/a2a",
        "hi",
        actor_chain=["hop"],
        identity=identity,
    ):
        pass  # pragma: no cover - the stub streams nothing

    proof = json.loads(seen["headers"][a2a_client.PRESENTER_HEADER])
    assert verify_signature(
        proof["publicKey"],
        proof["signature"],
        a2a_client.presenter_payload(
            proof["publicKey"],
            proof["timestamp"],
            hashlib.sha256(seen["content"]).hexdigest(),
        ),
    )
    # The chain rode request-level metadata, which is where the door reads it.
    assert json.loads(seen["content"])["params"]["metadata"]["actorChain"] == ["hop"]
    assert seen["headers"]["content-type"] == "application/json"
    assert seen["headers"]["A2A-Version"] == "1.0"
