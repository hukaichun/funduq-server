# AI Town as souk's frontend

Status: **evaluation, nothing implemented.** Recorded so the reasoning —
and the measurements behind it — survive the session that produced it.

Copied from upstream (now [hukaichun/funduq](https://github.com/hukaichun/funduq),
then the AgentSouk repo), where it was written before the repo split
and before gRPC was removed, then updated against this repo at `fdcd1ed`.
What changed from the upstream text: the provider transport is now
`WS /ws/provider`, not gRPC (`server-mode.md`); the stall model has since
shipped in `souk-directory/`, so the section below says so; the one-line
souk change is now two changes in two repos; and the docent — a gap this
document's own central cut created — is now a built MCP surface with one
question it deliberately refuses to answer, plus an agent under
construction to speak it. The argument is untouched;
the facts under it moved, and one of its assumptions was corrected by the
implementation (see "The one question the docent cannot answer").

The subject is [a16z-infra/ai-town](https://github.com/a16z-infra/ai-town),
evaluated at `depth 1` in a scratch checkout. Every claim below is marked
either *measured* (a probe was run) or *read* (source or upstream docs),
per CLAUDE.md.

## The proposal

Render souk's roster as a market: **a provider is a stall, an agent is a
person in that stall.** A human walks the market, reads the signs, walks up
to a stall and talks to someone in it.

This is not a metaphor imposed on the data model. It is already the data
model. souk addresses an agent by *whose it is and what it is called*:

    resolve_agent(provider, name)   ->   7f3a91c2 / translator
                                          stall     person

`providers.fingerprint` is a stall number, `providers.display_name` is the
sign over it (nullable — "this identity never said", so an unsigned stall
shows its number), and `agents.name` is a person behind the counter. The
test fixture for `provider_name` has read `"Ada's Stall"` since before this
evaluation existed.

| souk | market | notes |
| --- | --- | --- |
| `providers.public_key` | the stallholder | Ed25519, the only id a provider has |
| `providers.fingerprint` | stall number | UNIQUE — see "Layout" below |
| `providers.display_name` | the sign | nullable; unsigned stalls show the number |
| `agents.name` | a person in the stall | one Player on one tile |
| `agents.last_seen_at` | open / shut | whole stall at once — see below |
| `max_claim` | how many customers at once | per *stall*, not per person |
| `threads.parent_thread_id` | going to ask another stall | the person walks over |

## The stall model already shipped (read)

Since this evaluation was written, `souk-directory/` moved into this repo
and it *already renders the table above*. Not by coincidence and not as a
metaphor bolted on afterwards: `groupByProvider` in `src/app.ts` keys on
`public_key` exactly as the mapping says, `provider_name` is an optional
label over that key, and `src/index.ts` emits `<section class="stall">`
with a stall name, a truncated key as the stall number, a
`N agents · M online` count, and `unnamed provider` where the sign is
null. The vocabulary in the CSS is `stall-name`, `stall-key`,
`stall-count`, `stall-cards`.

So the first five rows of the mapping are **implemented, in a shipped
frontend, as a grouped list.** That changes what this document is
proposing. AI Town is not what makes souk a market — souk-directory
already made it one. What AI Town adds is the two columns a list has no
way to draw: **space and motion.** Position (a stall stays put), capacity
as a visible queue rather than a number, and delegation as a journey
across the map.

It also sets the bar. A second frontend has to earn its cost against one
that already exists, needs no backend, and is a few hundred lines of
`tsc`-compiled TypeScript. The argument below — that the market renders
invariants prose has to explain — is the case for paying it.

## What AI Town actually is (read)

- Convex serverless, TypeScript. **No long-lived process** — queries,
  mutations, actions, and crons (`crons.interval` takes seconds).
- The engine is single-threaded per world and **exclusively owns** world
  state. Outside components may only mutate it by submitting `inputs`
  (upstream `ARCHITECTURE.md` states this as an invariant).
- An agent's turn is a fire-and-forget `internalAction`:
  `Agent.tick` → `startOperation('agentGenerateMessage')` →
  `chatCompletion()` → **one string** → `agentSendMessage` mutation.
  `ACTION_TIMEOUT` is 120s.
- Players block each other: `blockedWithPositions` rejects a position
  within `COLLISION_THRESHOLD` of another player.

So AI Town asks exactly one thing of a character's mind: given a message,
return text within 120 seconds.

## The call souk answers (measured)

A probe stood up an in-process `Souk`, registered two agents under one
identity, attached a provider, and called `A2AAdapter.send_task`:

- Blocking request/response returning a full A2A Task, `state: completed`,
  text in `artifacts[].parts[].text`. Well inside the 120s budget.
- `contextId` carried back continues the same thread; `Task.id` is new each
  turn — which is what "one conversation, many turns" needs.
- One provider hosting two agents received the correct `agent_id` for each,
  on its own thread.

A Convex action reaches this with a single `fetch()` to
`POST /a2a/id/{agent_id}/rpc`.

**Not** by pointing `LLM_API_URL` at souk. That seam is the LLM layer;
souk is a broker, not a model. The thing to replace is
`agentGenerateMessage` as a whole.

### Streaming is not lost (measured)

Upstream `ARCHITECTURE.md` says messages "are updated very frequently (when
streamed out from OpenAI)". In this version they are not: `util/llm.ts` has
a streaming overload, but all six call sites (`agent/conversation.ts` ×3,
`agent/memory.ts` ×3) use the non-streaming form. souk's blocking
`send_task` is therefore a same-shaped drop-in. Streaming later means
pointing at `/agui/id/{agent_id}` and feeding `TEXT_MESSAGE_CONTENT` deltas
into the same write path.

## Where the residents come from

The engine owns world state, so the roster reaches it through `inputs` and
nothing else: a Convex cron reads `GET /agents`, groups by `public_key` into
stalls, and submits `join` / `leave`.

Polling is the right mechanism, and upstream's `souk.on_change` hook does
not change that — twice over. It is an in-process callback, and AI Town has
no process to hold one. And even for a frontend that could, it fires on
registration and de-listing but **fires nothing when an agent goes stale**:
`online` is derived from `last_seen_at` against a window at query time, so
there is no instant to fire on (`server-mode.md` records the same caveat for
MCP notifications). A shutting stall is exactly the `leave` half of this
cron. Whoever watches a souk's roster polls it.

Say the consequence out loud, because it forecloses a whole class of
design: **no transaction spans the two stores**, and none can — town state
lives in Convex, souk's lives in Postgres, and there is no session shared
between them. This repo enforces the same rule on itself by hand where it
has a choice (`server-mode.md`, "Serving state stays out of core's
database"); the town gets it for free by having no choice. So the rendered
market is always slightly out of date, by construction, and nothing may be
built that assumes otherwise: a stall standing in the town is never
evidence that its agent is there to claim a run. The run is what decides,
and the map is a picture of a moment that has already passed.

**Two addresses, and they fail differently.** A stall's coordinate comes
from `public_key` — the identity, which never changes. The `agent_id`
inside every `a2a_endpoint` is *nearly* as stable, and the gap is worth
stating precisely, because an earlier draft of this section got it wrong in
the alarming direction.

`register_agents` looks up an existing `(public_key, name)` row with **no
`delisted_at` filter**, so a re-registration reuses the original id and
clears the de-listing. Measured three ways here: `scribe` kept a
byte-identical id across a provider restart someone else ran, across a
stop/start run here, and its stall came back with both of its ids
unchanged. It has to work this way — the SDK re-registers on *every*
reconnect, and ids that churned per reconnect would churn all day.

An `agent_id` changes only when the row is genuinely gone: a wiped or
restored database. That is an operator event, not a routine one, and it is
exactly the event behind the invisible-docent incident — the docent came
back under a new id because its row had vanished beneath its open socket.

So directions are durable in practice without being guaranteed. The town
should still re-read endpoints on each roster poll rather than cache them
across syncs, but the reason is correctness under a rare event, not churn
— and the cost of doing so is nothing, since it is already polling.
Anything genuinely durable — a coordinate, a stall's identity, a remembered
visit — keys off `public_key`, which never moves.

Two gaps in the current surfaces:

- **Appearance.** `Player.join` requires a `character` (a sprite from
  `data/characters.ts`); souk has no such concept. The right home is
  `agents.metadata` — the schema comment already scopes it as
  "souk-internal extension data … Not interpreted by souk itself" — and
  `repo.register_agents` does persist `agent.get("metadata", {})` (read).
  The column exists; nothing on the way out carries it.

  This was "one line" when souk was one repo. After the split it is two,
  one on each side of the boundary, and the second is easy to miss:
  upstream, `repo.list_agents` does not select `agents.c.metadata` and
  `AgentSummary` has no such field; here, `AgentRosterEntry`
  (`souk_server/models.py`) has none either, and `api_registry.py` builds
  it with `AgentRosterEntry(**a.model_dump())`. Pydantic's default is
  `extra='ignore'`, so **fixing only the upstream half fails silently** —
  no error, the sprite simply never reaches the town.

  Do **not** use `agent_card_extra`: that is merged into `agent_card`, which
  is served verbatim as the A2A Agent Card. A sprite name has no business on
  a protocol surface.

  The converse is a trap worth knowing, because it decides whether a stall
  is findable at all. `repo.register_agents` builds the card as
  `{name, description, **agent_card_extra}` and **silently drops every other
  key** (read), so `skills` reaches souk *only* through `agent_card_extra`.
  A provider sending top-level `skills` registers fine and lists fine — with
  no skills. The docent's `search_agents` matches over name, description and
  skill names/descriptions/tags, so that stall is a stall whose goods are
  not on display: reachable if you already know the name, invisible to
  anyone asking what the market sells. `souk-agent-sdk`'s `AgentHandle` gets
  this right; a hand-rolled registration is where it goes wrong.

- **Position.** `Player.join` picks a random free tile with no way to
  specify one. Without a patch, every world restart has all stallholders
  sprinting from random spots back to their stalls. An optional `position`
  argument is about three lines.

## Layout: the stall number is the stall's location

Providers arrive dynamically, and a stall must not move between syncs.
`fingerprint` is a stable short hash of the public key and is UNIQUE in the
schema — a second key hashing to the same prefix is refused by the database
rather than by a check that could race. Hashing it to a grid slot gives a
layout that is stable by construction and collision-free for the same reason
the address is.

Stalls need not block movement, so they can be a coordinate plus a render
overlay and **the engine does not change at all**. People do block each
other, so a stall footprint must give each of its agents its own tile.

## Two clocks

AI Town's conversations are physically gated: invite → walkingOver →
participating, and the participants must be within `CONVERSATION_DISTANCE`.
A run arriving over A2A from outside cannot wait for a sprite to cross the
map. The two cases render differently:

- **Started in the market by a human** — full physical flow. Walking over to
  a stall is the natural form of the request.
- **Started outside** — must not enter the conversation mechanism. Render it
  as `player.activity` (`{description, emoji, until}`), an existing
  primitive: the person shows as serving a request.

## The one real decision: stallholders do not wander

AI Town's `Agent.tick` decides on its own to go find someone to talk to. If
each character's mind is a real souk agent, **the town manufactures traffic
and spends real inference budget on NPC small talk.** For an
enterprise-internal deployment that is disqualifying.

An earlier draft of this evaluation proposed keeping the wandering (movement
costs nothing — `agentDoSomething` has a branch that only picks a
destination and calls no LLM) while removing conversation initiation. Under
the stall model that is still wrong: a stallholder stands at their stall.

**Characters do not move on their own at all.** What moves is customers —
humans, and agents delegating to another stall. Market activity is then
exactly real traffic, with no fake motion anywhere.

Three things fall out of that one cut:

1. No manufactured traffic.
2. **The thread-model mismatch disappears.** A souk thread belongs to *one*
   agent (`threads.agent_id` is NOT NULL); an AI Town conversation has
   *two* participants. That only collides for agent↔agent chat. Once every
   conversation is human↔agent or a real delegation, souk's model fits.
3. `convex/agent/` (955 lines of prompt engineering, memory, embeddings)
   and `Agent.tick`'s decision loop are both deletable. AI Town is reduced
   to a physical world and a renderer.

## The docent at the gate

The cut above creates a gap. If nobody wanders and the market has forty
stalls, a visitor arriving at the gate has no way to find anything — a
roster list has a search box, a market does not. Someone has to be asked.

That someone is a **docent**: an agent whose whole job is directions,
rendered here as a character standing at the entrance. It is the natural
consumer of this repo's MCP server
(`souk_server/mcp_docent.py`, mounted at `/mcp` on the same listener,
`fdcd1ed`), which is deliberately discovery-only — in its own words, it
hands out the map and A2A does the walking. In a list frontend that split
is a design principle you have to explain. Here it is just true: the docent
tells you which stall and where, and then you walk there and talk to the
person yourself.

The docent **is a stall** — the one at the gate, and its goods are
directions. It registers like any other provider, appears in `GET /agents`,
and is talked to over A2A exactly like the agents it points at.

This reverses an earlier draft of this section, which made the docent the
town's own character holding an MCP client directly. Being a provider is
better for a reason that has nothing to do with AI Town: a guide that lives
inside one frontend serves only that frontend. A guide that is an *agent*
serves every frontend souk has — `souk-directory`, the town, any MCP client
— and it becomes the reference consumer of the discovery surface, so a gap
in `/mcp` shows up as a docent that cannot answer, in a running process,
rather than as a review comment. It also shrinks what the town has to
build: point a character at one more agent, instead of writing an MCP
client in Convex.

The market becomes self-describing — the thing that tells you about the
souk is in the souk, and turns up in its own roster.

The earlier cut survives intact. The docent never initiates, so it
manufactures no traffic and costs no inference a visitor did not ask for.
And it holds no tool that is not read-only discovery: the moment a guide
can *act*, "there is no way to run anything through this" stops being true
of the guide, whatever `/mcp` still guarantees about itself.

The market's questions, and what the built surface answers (read):

| the visitor asks | answered by |
| --- | --- |
| who is here today? | `browse_souk` / `souk://providers` — grouped by stall, with `agent_count` and `online_count` per stall |
| what does this stall do? | `describe_stall(provider_key)` |
| which one do I want? | `search_agents(query)` — substring over name, description, and skill names/descriptions/tags |
| where do I go? | every record carries `provider: {public_key, storefront_name}` |
| can I talk to them now? | `online`, always paired with `last_seen` in words — but stall busyness is not answered; see below |
| how do I talk to them? | `a2a_endpoint`, and there the docent stops |

The fourth row is the one a non-spatial client would never ask for, and the
one this frontend cannot do without: the layout hashes the provider key to
a coordinate (see "Layout"), so an answer naming an agent without its
provider can say *who* but not *where*.

Three of the built surface's decisions are market-shaped, and worth keeping
if the town's guide is ever rewritten:

- **`a2a_endpoint` is always the id route**, never `/a2a/{name}/rpc`. Names
  are unique only within a provider, so the name route can 409 — and a
  direction that sometimes leads to an argument is not a direction.
- **A duplicate name hands back candidates, not a guess**
  (`{found: false, reason: "ambiguous_name", candidates: […]}`). Two stalls
  may both staff a "translator"; the docent asks which one rather than
  sending the visitor to the wrong stall.
- **`online` never travels alone.** A boolean cannot separate "stepped
  away" from "gone for a week", which is exactly what a visitor deciding
  whether to wait needs.

### The one question the docent cannot answer

"Is this stall busy?" — and the reason corrects an assumption this document
made earlier. The obvious fix looks like an upstream projection of
`max_claim` onto the roster. It isn't. The gateway learns a worker's
`maxClaim` from its `hello` frame and knows its in-flight count because it
drives the claim loop — so busyness is **per-process serving state, not a
property of the souk.** An answer built from it would read as authoritative
while being blind to every worker connected to another replica.

That is a sharper statement of this document's own thesis than the market
metaphor managed: capacity is the stall's statement about itself (see "The
stall is the unit of capacity"), and a directory that guesses at it is
inventing. Left unbuilt deliberately, and recorded as such in
`server-mode.md`.

## The stall is the unit of capacity

From `souk/worker.py`:

> Claiming and concurrency belong to the provider as a whole, not to any one
> of its agents.

So a stall has one shared queue. `max_claim=2` with three callers means one
waits outside. Backpressure becomes something you can see rather than a
number in a log.

Presence has the same granularity: `claim_work` refreshes `last_seen_at` for
**every agent the worker hosts**. All of a provider's agents go online and
offline together — there is no half-staffed stall. Measured: stopping
Yusuf's Workshop flipped `scribe` and `translator` to offline *in the same
poll*, about a minute after the heartbeat stopped, with no intermediate
state where one had gone and the other had not. Both stayed listed
throughout, keeping their skills and their provider key, and starting the
stall again brought both back.

A stall therefore opens or shuts as a unit, and a shut stall **stays on the
map**. That is the right rendering: the sign is still up, the goods are
still described, nobody is behind the counter.

## The self-delegation deadlock draws itself

souk documents, and deliberately does not fix, a deadlock: with
`max_claim=1`, an agent delegating to another agent *the same provider
hosts* hangs — the outer run holds the only slot and the inner one is never
claimed. Reproduced here independently: the outer run sat at `running` and
the agent was entered exactly once (measured).

On a market plan this needs no explanation:

> You are talking to someone in the stall. They turn to ask their colleague —
> but the stall serves one customer at a time, and that customer is you.

This is the strongest argument for the town as a frontend. An invariant that
otherwise takes three passages of prose and a commit message becomes
obvious, and the reason it is left unfixed becomes obvious with it: capacity
is the stall's statement about itself, not something souk should route
around.

Delegation to a *different* stall is the opposite — the person leaves their
counter and walks across the market. A delegation chain renders as a
journey.

An earlier draft of this document claimed that journey was "the one thing a
market shows that a roster list cannot". **That was wrong, and the list
that disproves it is the one in this repo.** `souk-directory` already
renders a delegation: `renderCallChain` reads `GET /threads/{id}/tree`,
`flattenRoute` walks it depth-first into an ordered list of stops, and the
result is drawn as a strip of dots joined by links, labelled *call chain
for this reply* — journey vocabulary, arrived at independently. Measured
here end to end: asking Zahra's `haggler` for wording "in writing" produced
a real cross-stall delegation to Yusuf's `scribe` — two different provider
keys — and the tree came back with the child under the root.

So the honest claim is narrower, and better for being narrower. **A list
shows a delegation's sequence. A map shows its shape and its cost.** Four
things the strip cannot hold, three of which its own source concedes:

- **Fan-out.** One call spawning three siblings flattens into one
  depth-first line. The code says so and says why — a tree widget would be
  overkill for a case that is usually linear. On a map, branching costs
  nothing to draw: three people leave at once.
- **Place.** `A → B → C` is an order, not a geography. It cannot say that B
  was next door and C was across the market, which is the difference
  between a cheap delegation and an expensive one.
- **Time.** The strip is drawn *for this reply* — after it lands. The walk
  happens while the run is in flight, and that is when a person watching
  wants to see it.
- **Waiting.** A walk to a stall already serving someone else stands there.
  That is `max_claim` rendered as a queue, and it is the same picture as
  the deadlock above.

The bar this sets is therefore higher than the earlier draft admitted. The
list does not merely group stalls; it already draws the chain. What a
market adds is not the chain's existence but its dimensions.

## Why not the other two directions

**Characters as souk providers** (so outsiders could call into the town) is
no longer blocked on souk's side. When this evaluation was written the only
remote transport was the gRPC `PollForWork` long poll; this repo has since
replaced it with `WS /ws/provider` (see `server-mode.md`) — JSON text
frames, the server pushing claimed runs down a persistent socket, cheap
enough that even a browser can be a provider. What blocks the direction now
is AI Town's side: someone has to *hold* that socket, and AI Town has no
long-lived process to hold it — actions are bounded (`ACTION_TIMEOUT` is
120s) and crons are minute-granular. The conclusion is unchanged, only its
reason moved: it would still need an external bridge process keeping the
socket open, and that bridge — not AI Town — would be the provider.

**KYOK** looks like a zero-change option, since `/kyok/v1/chat/completions`
presents as an OpenAI-compatible host. It is not: a KYOK token names a run
and is paired with a call-time signature (`souk/protocols/kyok.py`), so AI
Town would have to already be a provider inside a run. It depends on the
direction above.

The frontend framing needs neither, which is why it is the cheap one.

## Scope

**AI Town**

- add: roster sync cron — `GET /agents`, group by `public_key`, submit
  `join` / `leave`
- add: stall overlay and fingerprint-derived layout
- add: externally-started runs render as `player.activity`, outside the
  conversation mechanism
- rewrite: `aiTown/agentOperations.ts` — LLM call becomes a souk A2A fetch
- patch: `Player.join` accepts an optional position
- add: the docent's stall at the gate — a fixed character wired to the
  docent agent over A2A like any other. No MCP client in Convex: the
  docent is a provider, so the town just talks to it
- delete: `convex/agent/`, and `Agent.tick`'s decision loop

**souk (upstream funduq — core only since the split)**

- select `agents.c.metadata` in `repo.list_agents`; add the field to
  `AgentSummary`
- nothing else

**this repo (AgentSoukServer)**

- add `metadata` to `AgentRosterEntry` — without it the upstream change is
  dropped silently at `AgentRosterEntry(**a.model_dump())` (see
  "Appearance")
- nothing else: `GET /agents` already carries `public_key` and
  `provider_name`, and `POST /a2a/id/{agent_id}/rpc` is already the shape
  the town would call. Both changes are additive fields; no existing
  behaviour or caller moves.

## Known gap

**The people in the market are anonymous to souk.** A human walking up and
talking is a run whose caller is that human, but user identity is not built
(user identity is not provider identity). Such a run carries no
`actor_chain`. Not a blocker — but if "the delegation chain starts with a
person" is what the market is meant to show, that first link is missing.

## Evidence

| claim | basis |
| --- | --- |
| `send_task` blocks and returns a full Task; `contextId` continues a thread | measured (probe) |
| one provider, two agents, correct `agent_id` routing | measured (probe) |
| self-delegation deadlock: outer run at `running`, agent entered once | measured (reproduced) |
| all six `chatCompletion` call sites are non-streaming | measured (grep) |
| `claim_work` refreshes `last_seen_at` for every agent a worker hosts | read |
| the engine exclusively owns world state; `inputs` is the only way in | read (upstream `ARCHITECTURE.md`) |
| players block each other, so a stall needs one tile per agent | read |
| `repo.register_agents` persists `metadata`; `list_agents` does not project it | read |
| `souk-directory` groups by `public_key` and already calls the groups stalls | read (`src/app.ts`, `src/index.ts`, `style.css`) |
| `AgentRosterEntry` has no `metadata` and is built `(**a.model_dump())`, so an upstream-only fix is dropped silently | read (`souk_server/models.py`, `api_registry.py`) |
| `on_change` fires on registration and de-listing, never on going stale | read (upstream `souk/core.py`, `changes.py`) |
| stall busyness is per-process serving state, not a souk property, so no projection can answer it | read (`ws_provider.py`'s hello frame + claim loop) |
| the provider transport is `WS /ws/provider`; gRPC is removed | read (`server-mode.md`) |
| `register_agents` drops every key but `name`/`description`/`agent_card_extra`, so top-level `skills` never lands | read (upstream `repo.register_agents`) |
| the docent surface: 4 read-only tools, 3 resources, provider key on every record | read (`souk_server/mcp_docent.py`, `fdcd1ed`) |
| cross-stall delegation is real: Zahra's `haggler` → Yusuf's `scribe`, two provider keys, parent/child in `/threads/{id}/tree` | measured (probe against the demo stack) |
| `souk-directory` already renders that chain as a route strip; a list is not blind to delegation | read (`src/agent.ts`, `renderCallChain`/`flattenRoute`) |
| the route strip's name labels come from an `agent_id` → name join built once at page load, so they degrade to raw ids only if a row is recreated under an open page — rare, not routine | read |
| fan-out, place, time and waiting in a delegation — the four things the strip cannot hold | not implemented |
| external-run activity path (`player.activity`) | not implemented |
| the docent *agent* — a provider consuming `/mcp`, so every frontend gets a guide | implemented (`providers/pydantic-ai-agent/config.docent.yaml`, `3f6e0dc`) |
| `agent_id` **survives** re-registration, restart and de-listing — `register_agents` matches `(public_key, name)` with no `delisted_at` filter | measured (identical ids across two restarts and a stop/start) |
| it changes only when the row itself is gone (wiped/restored database) — the invisible-docent case | measured (that incident) |
| a whole stall goes offline together, stays listed, and comes back together | measured (stopped Yusuf's Workshop; both agents flipped in one poll) |
| the docent's stall rendered in the town | not implemented |
