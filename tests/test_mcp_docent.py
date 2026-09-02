"""The MCP docent (souk_server.mcp_docent) — the guide, not an admin
surface.

Two things this suite is for. The first is the boundary: the docent gives
directions and stops, so the test that matters most is the negative one —
no tool here starts, resumes or cancels anything (test_the_docent_offers
_no_way_to_run_anything). Invocation is A2A's, and an MCP tool wrapping
`start_run` would be a second, lossier path beside a standard one.

The second is that every answer stays *pointable*: an agent named without
its provider key cannot be placed on a map (souk-directory groups by
stall, and the AI-town layout derives a stall's coordinate by hashing that
key), so the key riding on every record is a requirement, not a detail.

The pure mapping is tested directly against souk's models; the tool layer
goes through a real MCP client over the ASGI app, because "the tool is
registered under the name an LLM will call" is exactly what reading the
decorator cannot tell you.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from mcp import Client

from funduq.identity import provider_fingerprint
from funduq.models import AgentSummary
from funduq_contract import Registration
from funduq_provider_sdk import InProcessLink, ProviderRuntime
from souk_server.mcp_docent import (
    _seen_ago,
    create_docent,
    describe_agent,
    describe_market,
    transport_security_for,
)

from tests.conftest import EchoAgent

BASE = "https://souk.example.com"


async def _register(souk, new_identity, provider_name: str, *agents: dict) -> str:
    """Register a stall, the only way one is registered now: publish on
    an open link, then close the link — registered-but-offline, which is
    the state most of this suite furnishes its market in.

    Note the shape: skills belong under `agent_card_extra`, because that
    is the only part of a registration funduq copies verbatim into the
    agent card (repo.register_agents builds the card from name +
    description + agent_card_extra, and drops anything else).
    souk-agent-sdk's AgentHandle sends them the same way, so a test
    registering them at the top level would be searching data no real
    provider produces.

    Not the `register` fixture: these stalls need names and descriptions
    and skills, which that fixture deliberately does not take — its job is
    to make a name funduq knows, and this one's is to furnish a market.
    """
    identity = new_identity()
    runtime = ProviderRuntime(identity, EchoAgent())
    runtime.start()
    link = InProcessLink(souk, runtime)
    await souk.attach_provider(link)
    await souk.register_agents(
        link,
        [Registration.model_validate(a) for a in agents],
        provider_name=provider_name,
    )
    souk.detach_provider(identity.public_key, link)
    await runtime.aclose()
    return identity.public_key


def _docent(souk) -> Client:
    """A real client against a real server, in memory — no socket, but
    every answer still crosses the protocol, which is what makes tool
    *names* and annotations part of what these tests check.

    Entered inside each test rather than supplied as a fixture: the client
    holds an anyio task group, and pytest-asyncio can finalise a fixture
    from a different task than it set it up in, which that cannot survive.
    """
    return Client(create_docent(souk, BASE), raise_exceptions=True)


def _summary(**overrides) -> AgentSummary:
    now = datetime.now(UTC)
    return AgentSummary(
        **{
            "provider_key": "aa" * 32,
            "name": "translator",
            "description": "Translates",
            "skills": [],
            "joined_at": now,
            "last_seen_at": now,
            "online": True,
            "provider_name": "Halima's",
            **overrides,
        }
    )


# --- the pure mapping -------------------------------------------------


def test_every_agent_carries_directions_and_its_provider_key():
    key = "bb" * 32
    described = describe_agent(_summary(provider_key=key, name="scribe"), BASE)

    # The pair route, which is the only one there is: a display name is not
    # an address, since two providers may legitimately offer the same one.
    short = provider_fingerprint(key)
    assert described["a2a_endpoint"] == f"{BASE}/a2a/{short}/scribe/rpc"
    assert described["agent_card_url"].startswith(f"{BASE}/a2a/{short}/scribe/.well-known/")
    # Both forms of the identity ride along, and the key is the one to
    # compare — the fingerprint is derived from it, never the other way.
    assert described["provider"]["public_key"] == key
    assert described["provider"]["fingerprint"] == short


def test_a_trailing_slash_on_the_base_url_does_not_double_up():
    described = describe_agent(_summary(name="scribe"), BASE + "/")
    short = provider_fingerprint("aa" * 32)
    assert described["a2a_endpoint"] == f"{BASE}/a2a/{short}/scribe/rpc"


def test_the_market_is_grouped_by_stall_not_flattened():
    market = describe_market(
        [
            _summary(name="translator", provider_key="aa" * 32),
            _summary(name="summarizer", provider_key="aa" * 32),
            _summary(name="translator", provider_key="bb" * 32, online=False),
        ],
        BASE,
    )

    assert market["agent_count"] == 3
    assert market["provider_count"] == 2
    assert market["online_count"] == 2
    stall = next(s for s in market["providers"] if s["provider_key"] == "aa" * 32)
    assert sorted(stall["offers"]) == ["summarizer", "translator"]
    assert stall["agent_count"] == 2


def test_last_seen_is_reported_in_words_beside_the_derived_flag():
    """`online` alone cannot distinguish "stepped away" from "gone for a
    week", and that difference is what a visitor deciding whether to wait
    is actually asking about."""
    now = datetime.now(UTC)
    assert _seen_ago(now).endswith("s ago")
    assert _seen_ago(now - timedelta(minutes=10)) == "10m ago"
    assert _seen_ago(now - timedelta(hours=5)) == "5h ago"
    assert _seen_ago(now - timedelta(days=3)) == "3d ago"
    # A naive timestamp (what SQLite hands back) must not raise.
    assert _seen_ago(datetime.now()).endswith("ago")


# --- through a real MCP client ---------------------------------------


async def test_the_docent_offers_no_way_to_run_anything(souk, new_identity):
    """The boundary, asserted rather than trusted: a docent gives
    directions. Talking to an agent is A2A's job (docs/server-mode.md)."""
    async with _docent(souk) as client:
        names = {t.name for t in (await client.list_tools()).tools}

        assert names == {"browse_souk", "search_agents", "describe_agent", "describe_stall"}
        forbidden = ("run", "start", "resume", "cancel", "register", "delete", "attach")
        assert not [n for n in names if any(word in n for word in forbidden)]


async def test_every_tool_declares_itself_read_only(souk, new_identity):
    async with _docent(souk) as client:
        for tool in (await client.list_tools()).tools:
            assert tool.annotations is not None, tool.name
            assert tool.annotations.read_only_hint is True, tool.name


async def test_browsing_an_empty_souk_says_so_rather_than_failing(souk, new_identity):
    async with _docent(souk) as client:
        market = (await client.call_tool("browse_souk", {"only_online": False})).structured_content
        assert market["agent_count"] == 0
        assert market["providers"] == []


async def test_browse_groups_registered_agents_by_stall(souk, new_identity):
    async with _docent(souk) as client:
        key = await _register(
            souk,
            new_identity,
            "Halima's Translations",
            {"name": "translator", "description": "Translates between languages"},
            {"name": "summarizer", "description": "Shortens documents"},
        )

        market = (await client.call_tool("browse_souk", {"only_online": False})).structured_content

        assert market["provider_count"] == 1
        stall = market["providers"][0]
        assert stall["provider_key"] == key
        assert stall["storefront_name"] == "Halima's Translations"
        assert sorted(stall["offers"]) == ["summarizer", "translator"]
        assert all(a["provider"]["public_key"] == key for a in stall["agents"])


async def test_search_finds_by_skill_and_answers_with_the_stall(souk, new_identity):
    async with _docent(souk) as client:
        await _register(souk, new_identity, "Halima's", {"name": "translator", "description": "Any language pair"})
        poet_key = await _register(
            souk,
            new_identity,
            "Yusuf's Workshop",
            {
                "name": "translator",
                "description": "Classical Arabic",
                "agent_card_extra": {"skills": [{"name": "translate", "tags": ["poetry"]}]},
            },
        )

        found = (await client.call_tool("search_agents", {"query": "POETRY"})).structured_content

        assert found["match_count"] == 1
        assert found["searched_count"] == 2
        assert found["agents"][0]["provider"]["public_key"] == poet_key


async def test_search_survives_a_visitors_sentence_not_just_a_keyword(souk, new_identity):
    """The caller is a model relaying a person, and a model handed a tool
    called "search" will pass the whole sentence. Whole-phrase-only
    matching found nothing for exactly the stall that was standing
    there — so any significant word counts."""
    await _register(
        souk,
        new_identity,
        "Yusuf's",
        {
            "name": "translator",
            "description": "Classical Arabic",
            "agent_card_extra": {"skills": [{"name": "translate", "tags": ["poetry"]}]},
        },
    )

    found = (
        await _search(souk, "who here can help me with poetry")
    )

    assert found["match_count"] == 1


async def _search(souk, query: str) -> dict:
    async with _docent(souk) as client:
        return (await client.call_tool("search_agents", {"query": query})).structured_content


async def test_short_words_in_a_sentence_do_not_match_the_whole_market(souk, new_identity):
    """The other half of the same decision: "the"/"for"/"with" must not
    turn a search into a roster dump."""
    await _register(souk, new_identity, "Halima's", {"name": "translator", "description": "Any language pair"})

    found = await _search(souk, "is the one for me")

    assert found["match_count"] == 0


async def test_search_with_no_match_is_an_empty_answer_not_an_error(souk, new_identity):
    async with _docent(souk) as client:
        await _register(souk, new_identity, "Halima's", {"name": "translator"})
        found = (await client.call_tool("search_agents", {"query": "blacksmith"})).structured_content
        assert found["match_count"] == 0 and found["agents"] == []


async def test_a_duplicate_name_hands_back_candidates_instead_of_guessing(souk, new_identity):
    """Two identities may register the same display name. There is no
    route that resolves a bare name any more, precisely because there was
    no right answer for it to give — and a docent that guessed would send
    the visitor to the wrong stall."""
    async with _docent(souk) as client:
        key_a = await _register(souk, new_identity, "Halima's", {"name": "translator"})
        key_b = await _register(souk, new_identity, "Yusuf's", {"name": "translator"})

        answer = (
            await client.call_tool("describe_agent", {"name": "translator"})
        ).structured_content

        assert answer["found"] is False
        assert answer["reason"] == "ambiguous_name"
        assert {c["provider"]["public_key"] for c in answer["candidates"]} == {key_a, key_b}
        # The candidates are directly usable: each carries a real address.
        assert all(c["a2a_endpoint"].endswith("/rpc") for c in answer["candidates"])


async def test_an_ambiguous_name_resolves_once_the_stall_is_named(souk, new_identity):
    """The second argument is what the visitor takes away from the
    ambiguous answer, and it is the provider — which is the whole shape of
    the change: a name plus who offers it, never a name alone."""
    async with _docent(souk) as client:
        await _register(souk, new_identity, "Halima's", {"name": "translator"})
        await _register(souk, new_identity, "Yusuf's", {"name": "translator"})
        candidates = (
            await client.call_tool("describe_agent", {"name": "translator"})
        ).structured_content["candidates"]
        wanted = candidates[0]

        answer = (
            await client.call_tool(
                "describe_agent",
                {"name": "translator", "provider": wanted["provider"]["fingerprint"]},
            )
        ).structured_content

        assert answer["provider_key"] == wanted["provider_key"]
        assert answer["a2a_endpoint"] == wanted["a2a_endpoint"]


async def test_the_full_key_names_a_stall_just_as_well_as_its_fingerprint(souk, new_identity):
    """Whichever form the visitor is holding. The docent hands out both on
    every record, so demanding one back would be asking them to convert
    between two spellings of the same identity."""
    async with _docent(souk) as client:
        key = await _register(souk, new_identity, "Halima's", {"name": "translator"})
        await _register(souk, new_identity, "Yusuf's", {"name": "translator"})

        answer = (
            await client.call_tool("describe_agent", {"name": "translator", "provider": key})
        ).structured_content

        assert answer["provider_key"] == key


async def test_a_stall_that_does_not_offer_that_name_is_a_plain_no(souk, new_identity):
    async with _docent(souk) as client:
        key = await _register(souk, new_identity, "Halima's", {"name": "translator"})

        answer = (
            await client.call_tool("describe_agent", {"name": "blacksmith", "provider": key})
        ).structured_content

        assert answer["found"] is False


async def test_an_unknown_agent_is_a_plain_no(souk, new_identity):
    async with _docent(souk) as client:
        answer = (
            await client.call_tool("describe_agent", {"name": "blacksmith"})
        ).structured_content
        assert answer["found"] is False
        assert "candidates" not in answer


async def test_describe_stall_answers_by_provider_key(souk, new_identity):
    async with _docent(souk) as client:
        key = await _register(souk, new_identity, "Yusuf's Workshop", {"name": "translator"}, {"name": "scribe"})

        answer = (await client.call_tool("describe_stall", {"provider_key": key})).structured_content

        assert answer["storefront_name"] == "Yusuf's Workshop"
        assert answer["agent_count"] == 2
        assert sorted(answer["offers"]) == ["scribe", "translator"]


async def test_an_unknown_stall_is_a_plain_no(souk, new_identity):
    async with _docent(souk) as client:
        answer = (
            await client.call_tool("describe_stall", {"provider_key": "ff" * 32})
        ).structured_content
        assert answer["found"] is False


async def test_the_roster_is_readable_both_flat_and_by_stall(souk, new_identity):
    async with _docent(souk) as client:
        await _register(souk, new_identity, "Halima's", {"name": "translator"}, {"name": "summarizer"})

        uris = {str(r.uri) for r in (await client.list_resources()).resources}
        assert {"souk://agents", "souk://providers"} <= uris

        flat = await client.read_resource("souk://agents")
        assert '"agent_count"' in flat.contents[0].text
        stalls = await client.read_resource("souk://providers")
        assert '"provider_count"' in stalls.contents[0].text


async def test_browsing_can_be_narrowed_to_who_can_answer_now(souk, register, serve):
    """The filter earns its place twice: a visitor asking "who can I talk
    to right now" is a real question, and a tool with no parameters at all
    has an empty argument schema — which a live model answered with
    malformed JSON (`{}""`), failing the call twice and killing the run,
    while every tool here that takes a parameter was called cleanly.

    Two stalls, one of them actually attended. The old version of this
    test registered a single agent and asserted it was online, because
    registering marked it seen and seen-recently *was* online — so the
    filter had nothing to exclude and would have passed unimplemented.
    """
    await register("translator")
    await serve(None, "scribe")

    async with _docent(souk) as client:
        everyone = (await client.call_tool("browse_souk", {"only_online": False})).structured_content
        assert everyone["agent_count"] == 2

        available = (
            await client.call_tool("browse_souk", {"only_online": True})
        ).structured_content
        assert available["agent_count"] == 1
        assert available["online_count"] == 1
        assert available["providers"][0]["offers"] == ["scribe"]


def test_a_wildcard_host_policy_disables_the_check_rather_than_listing_a_host():
    """The SDK matches `allowed_hosts` exactly (plus a `host:*` port
    pattern) and knows no wildcard, so passing a literal "*" allows one
    host — one named `*` — and fails closed. Measured, not read: the
    docent reaching souk by its compose service name got 421 from a
    config that read as "allow everything"."""
    wide = transport_security_for(["*"])
    assert wide.enable_dns_rebinding_protection is False

    pinned = transport_security_for(["souk:8000", "souk.example.com"])
    assert pinned.enable_dns_rebinding_protection is True
    assert "souk:8000" in pinned.allowed_hosts
    assert "https://souk.example.com" in pinned.allowed_origins
