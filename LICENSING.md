# Licensing

Two licences, split on one line: **what you run** is AGPL-3.0, **what you
build against** is Apache-2.0.

| Path | Licence | |
|---|---|---|
| `souk_server/`, `tests/`, `docs/`, repo root | **AGPL-3.0-only** | [LICENSE](LICENSE) |
| `souk-agent-sdk/` | Apache-2.0 | [LICENSE](souk-agent-sdk/LICENSE) |
| `souk-client-sdk/` | Apache-2.0 | [LICENSE](souk-client-sdk/LICENSE) |
| `agent-template/` | Apache-2.0 | [LICENSE](agent-template/LICENSE) |
| `providers/*` | Apache-2.0 | e.g. [LICENSE](providers/pydantic-ai-agent/LICENSE) |
| upstream `funduq` / `funduq-provider-sdk` / `funduq-contract` | Apache-2.0 | installed from PyPI, licensed upstream — nothing of theirs is vendored here but the contract vectors, which are test data |

The root `LICENSE` is the repository default; a directory with its own
`LICENSE` is governed by that one instead.

## Why the gateway is AGPL

Every other strong copyleft licence is satisfied by never shipping a
binary — and never shipping a binary is exactly what running a server is.
§13 is the clause that closes it: someone who modifies this gateway and
lets people talk to it over a network owes those people the modified
source, whether or not a copy ever changes hands. For a gateway, that is
the only version of copyleft with anything in it.

## Why the SDKs and templates are not

Because they are imported into somebody else's program, and AGPL there
would oblige every provider author to publish their agent. That would stop
anyone building a proprietary agent against this souk, which is not what
the gateway's licence is for — the point is that a *hosted, modified souk*
stays open, not that nobody may keep their own agent to themselves.

`souk-client-sdk` is the plainest case: it contains no souk code at all.
Its dependencies are httpx, httpx-sse, websockets, pydantic and litellm.
AGPL there would protect nothing and deter everyone.

And nobody has to use any of them. The wire is documented in
[docs/server-mode.md](docs/server-mode.md) and the provider contract is
stated upstream in `funduq-provider-sdk` and `funduq-contract`'s vectors;
a provider written from those owes this repository nothing at all. The SDKs are a convenience, and a
convenience with a licence attached is not one.

## Direction of travel

Apache-2.0 composes into an AGPL-3.0 work; the reverse does not. So the
gateway may depend on funduq core and on these SDKs, and the licence sits
on the side of the boundary where it has to. Nothing here changes funduq,
which is Apache-2.0 upstream and stays that way.

*Not legal advice — this file describes the intent behind the choice. The
licence texts themselves govern.*
