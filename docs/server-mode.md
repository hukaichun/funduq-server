# Server mode: one port, WebSocket relay

Status: **implemented** (`souk_server/ws_provider.py`,
`souk_server/ws_kyok.py`), gRPC removed. Defines what this gateway
serves and over which transports. Supersedes the inherited HTTP+gRPC
split.

Upstream is the published `funduq` packages now (`funduq` 0.0.6,
`funduq-provider-sdk[llm]` 0.0.7, `funduq-contract` 0.0.9 — the repo is
[hukaichun/funduq](https://github.com/hukaichun/funduq)), and the signed
payloads and delivery envelopes on this wire are theirs, pinned at a
named **contract revision** (currently 16, vendored in
[`docs/upstream-contract-vectors.json`](upstream-contract-vectors.json)).
The *framing* — which frames exist, what each carries, the handshake's
shape on a socket — remains this repo's to decide, and this gateway has
no deployments outside this repo, so a wire change is still a hard
cutover selected by the `version` field rather than a staged migration.

**Revisions 8–16 changed nothing about the frame vocabulary.** The wire
is still v4 — same frames, same `handshake_version` — and the section
below on what upstream ships instead is the whole of what moved.

## The decision

One listener. Everything — callers, providers, KYOK bridges — arrives on
a single HTTP port. Outbound-claim stays (souk never connects to anyone;
that is the architecture, forced by NAT topology, and it is not up for
revision). What changes is the *carrier* for the two claim-based edges:
a persistent WebSocket each, replacing gRPC entirely.

| Who | Surface | Transport | Status |
|---|---|---|---|
| Callers | AG-UI (`/agui/*`, `/threads/*`), A2A (`/a2a/*`), registry (`GET /agents`, `GET /llm-providers`), health | HTTP + SSE | exists |
| Callers | MCP (the docent) | streamable HTTP at `/mcp`, same listener | built; see "MCP: the docent" below |
| Providers, both kinds | admission | `POST /tickets` | built; the out-of-band half of the handshake — see below |
| Providers | work relay + registration | `WS /ws/provider` | built |
| LLM providers (KYOK) | completion relay + registration | `WS /ws/kyok` | built |
| Provider's model client | `POST /kyok/v1/chat/completions` | HTTP (OpenAI-compatible by definition) | exists, unchanged |

gRPC is **removed**, not demoted to an option: `grpc_server.py`,
`grpc_gen/`, the proto generation step, the `grpcio`/`protobuf`
dependencies, port 50051, and the Dockerfile's stub-gen step all went. A
transport with zero users is not an option worth maintaining; the wire
*semantics* it carried were kept, because those were the hard-won part —
and they have since been restated as upstream's contract vectors, which
is a better record than a proto file ever was.

The signed registration and deletion HTTP routes are removed the same
way, one redesign later: registration is an operation on an open,
authenticated link now (see "Registration on the open link"), so
`POST /agents/register`, `POST /llm-providers/register` and the signed
deletion routes are gone, and only the read-only roster GETs remain.

What one port buys: one TLS certificate, one load-balancer rule, no
HTTP/2 requirement on proxies (wss is an HTTP/1.1 upgrade), and a
browser can be a provider — which is the audience that makes WebSocket
the right default rather than a fallback.

Core is untouched throughout. The provider port is core's to state, and
it has since been inverted again — funduq offers a run and the provider
answers, rather than the provider asking for work — so what a transport
carries today is `broker.ConnectedProvider`: who you are, how much you
will take at once, how to hand you a run, how to ask you to stop one. A
transport is just a carrier for that port, and this is the third carrier
after in-process and gRPC. The KYOK edge swaps the same way because the
LLM-provider link is likewise a transport-free port; only this repo's
serving layer changes.

## Shapes, not a machine — and the orderings live here now

This repo asked upstream for a sans-io protocol machine
([hukaichun/funduq#213](https://github.com/hukaichun/funduq/issues/213)):
frames in, frames and events out, so that the orderings a transport must
respect were code rather than prose in two repositories. It shipped in
`funduq-provider-sdk` 0.0.5 and was **withdrawn at contract revision
11**, along with the published frame vocabulary and its codec. Upstream's
reason, from the changelog, is worth quoting rather than paraphrasing:

> `funduq_provider_sdk.protocol` and `funduq_provider_sdk.llm.protocol`
> are gone, machines and all — over a wire the provider-initiated calls
> are plain request/response, so there is nothing left for a machine to
> order.

This document is where the consequence lands: **nothing upstream
enforces the orderings on this wire any more, so they are stated here and
tested here.** Welcome before anything else, offer-then-verdict, a roster
that replaces rather than appends, an answer accepted only on the
connection its request was delivered to — every one of them is this
repo's to keep, in the sections below, in `docs/wire-vectors.json`, and
in the three suites plus the Go probe that replay it.

What was adopted instead is the half of #213 that survived, and it is a
real improvement:

- **Every crossing shape is a pydantic model in `funduq_contract`**,
  defined once and imported by both ends: `Connect`, `Offer`, `Verdict`,
  `DeliveredRun`, `DeliveredCompletion`, `Registration`, `Refusal`. They
  are `frozen=True`, `populate_by_name=True` and — the part with teeth —
  **`extra="forbid"`**, so a misspelt key is a validation error naming
  the field at the door instead of travelling intact and being dropped in
  silence by a reader that cannot tell a typo from an omission. The
  gateway's own restatements of these shapes (a local `AgentRegistration`
  model, a claimed-run translation) are deleted rather than kept in step.
- **`A2ARequestHandler`** is upstream's real `a2a.server.RequestHandler`,
  and the gateway's hand-rolled equivalent is gone — see "The A2A door".

**Our envelopes stay flat, so both ends strip the transport key.** A run
frame is `{"type": "run", **DeliveredRun}`, not
`{"type": "run", "run": {…}}` — the frame shape predates the models and
did not change with them. Because the models forbid extras, the
transport's own `type` (and `requestId`, on a `completionRequest`) has to
come *off* the mapping before `model_validate`, or a perfectly good frame
fails validation on the very field that routed it. Nothing in the models
says so, and it is the kind of detail that costs a debugging round, so it
is written down here.

**The two dump rules, which pull opposite ways.** Upstream's withdrawn
codec used to enforce both; nothing does now, which is exactly why they
belong in the spec of record.

| what | how | what breaks otherwise |
|---|---|---|
| a frame envelope (`DeliveredRun`, `DeliveredCompletion`) | `model_dump(by_alias=True)`, **never** `exclude_none` | `RunAgentInput` has required fields that are legitimately null (`state`, `forwardedProps`); stripping them yields a `runInput` the far side cannot rebuild, and a perfectly good run comes back as a **permanent refusal** |
| a typed AG-UI **event** | `model_dump(…, exclude_none=True)` | `timestamp: null` and `rawEvent: null` are injected into the caller's stream |

They live one file apart here — `ws_provider.SocketProvider.deliver` and
`api_agui.encode_event` — and each says so where it is written, because
the rule that is right in one place is wrong in the other.

## Core's environment is read here now

`CoreSettings.from_env` was removed at revision 14, and core reads no
environment at all: configuration is an argument, and a deployment that
keeps it in the environment reads it itself. That deployment is this
gateway, so `souk_server/config.py:core_settings_from_env()` is where the
`FUNDUQ_*` names live now. **The names did not change** — compose,
`.env.example` and every existing deployment keep working — and neither
did any default: only variables actually present are passed, so each
field keeps core's own default and there is no second copy of one here.
An empty string is *unset*, which is what an operator means by
`FUNDUQ_DB_SCHEMA=` in a compose file.

Two things this repo now states rather than inherits. The pairs are
written out (`("db_schema", "FUNDUQ_DB_SCHEMA", str)`, …) instead of
derived from the model's field names, because the environment is a
published surface: a field renamed upstream must be a visible edit here
and not a silently renamed variable in everybody's deployment. And the
broker's three waits became `CoreSettings` fields at the same revision —
`unserved_timeout_seconds` (45), `deliver_timeout_seconds` (5),
`undelivered_window_seconds` (1800) — so they are optional `FUNDUQ_*`
knobs here too, documented in `.env.example`. That closes the last live
item of the adopter review this repo filed as
[hukaichun/funduq#181](https://github.com/hukaichun/funduq/issues/181).

## Addressing: an agent is `(provider, name)`

Every surface in that table takes both halves, and none takes anything
souk minted. There is no `agent_id` — core stopped minting one, because a
provider holding identifiers only souk could issue lost its whole
vocabulary whenever the database was replaced, with no way to rebuild it.

```
/a2a/{provider}/{name}/rpc      /a2a/{provider}/{name}/.well-known/agent-card.json
/agui/{provider}/{name}         /threads/{provider}/{name}
```

`{provider}` is the provider's Ed25519 public key or its 16-hex
fingerprint (`funduq_contract.provider_fingerprint`, `sha256(key)[:16]`);
core tells them apart by length. This gateway puts the fingerprint in
URLs and the roster carries both, with `provider_key` the one to
compare — the fingerprint is derived from it and never authoritative.

**The by-name routes are deleted, not deprecated.** A display name is not
unique: two identities may both register `translator`, and that is
allowed. A route taking a bare name therefore had to guess or refuse, and
guessing is how a caller reaches an agent it never meant to reach.

Resolving a name is still ordinary, and still supported — it is
`GET /agents`, done once by whoever holds the name, after which the pair
is what goes on the wire. Both SDKs do exactly this (`SoukClient.resolve`,
and the demo providers' sub-agent resolver), and both surface ambiguity to
their caller rather than picking. The difference from the old route is
only *where* the choice is made: somewhere the asker can answer it.

That is also why an address cannot be written into a config file. The
provider half is the callee's own key fingerprint, which does not exist
until that provider has started once and written its key — so a static
`a2a_url` for a sibling agent is unwriteable in principle, not merely
inconvenient. Providers resolve lazily, on first delegation, so a
delegation edge does not become a boot order that `depends_on` cannot
express.

## Provider relay: `WS /ws/provider`

Frames are JSON text messages, camelCase — matching the AG-UI/A2A wire
style, readable in devtools, and free for the browser providers that
justify ws in the first place. The frame vocabulary is published in
[`docs/wire-vectors.json`](wire-vectors.json) and asserted equal to the
gateway's dispatch sets in tests; the signed payloads and envelopes
inside the frames are upstream's, vectored in
[`docs/upstream-contract-vectors.json`](upstream-contract-vectors.json).

**funduq hands work over; it does not wait to be asked for it.** There is
no claim loop on either side. The broker knows which provider serves
which agent, offers each run to it, and the answer comes back as a frame.
The socket carries the offer; `funduq_provider_sdk.ProviderRuntime`, on
the far side, decides.

### Opening a socket: the ticket handshake

Handshake **v4**: two frames, with the admission step moved off the
socket entirely. The signed payloads are `funduq_contract`'s connect
family, and `souk_server/handshake.py` re-exports them beside the
version number (object identity asserted in tests, so they cannot drift
into local restatements — the same rule that once dissolved three
packages' private copies of these bytes).

**Step zero is `POST /tickets`, and it is not on the socket.** The
provider posts its public key and receives
`{"ticket": ..., "funduqPublicKey": ...}` — a single-use, ~60-second
ticket naming the key it admits, minted by core's `Funduq.issue_ticket`.
Upstream keeps `issue_ticket` off the link's operation set on purpose: a
ticket obtained over the link would mean the link existed before
anything authorised it. **Issuing is the admission decision** — a key
with no ticket cannot connect at all — which makes this endpoint the
edge-auth plug point: a deployment that gates who may serve gates it
here. It is unauthenticated today, deliberately; this souk is an open
market.

Holding the ticket *and* funduq's public key, the provider computes its
proof before connecting, and the socket exchange collapses to:

```
provider → hello    { version: 4, publicKey, ticket, nonce,
                      maxConcurrentRuns?, proof }    # /ws/kyok: no maxConcurrentRuns
souk     → welcome  { funduqPublicKey, answer }
```

```
proof  = identity.sign( provider_connect_payload(funduq_public_key, ticket, nonce) )
answer = funduq.sign(   funduq_connect_payload(ticket, nonce) )
```

The gateway relays the hello's ticket, nonce and proof into
`funduq.attach_provider(...)` (`attach_llm_provider` on the KYOK
socket); **core is the verifier**, exactly as it was for the old
challenge. `attach` returns `answer` — funduq's counter-signature under
its own role tag — and the welcome frame relays it. Every handshake
refusal closes with 1008 (policy violation) and a reason string; 1011
stays reserved for server-side failure the client didn't cause.

Why each piece is there:

- **The ticket is the freshness, and the verifier chose it.** A recorded
  exchange is worth nothing: the ticket is single-use and destroyed by
  the handshake that answers it. This is the property v1's
  challenge-response bought with two extra frames; the ticket buys it
  out of band, and a leaked ticket is worthless besides — only the named
  key can sign the answer, and a stranger cannot even burn it (the name
  is matched before the ticket is destroyed).
- **The proof names the recipient.** The pinned funduq key goes into the
  signed bytes, so a proof one funduq coaxes out cannot be relayed to
  attach at another — the verifying funduq builds the payload with its
  *own* key, and a mismatch simply fails the signature.
- **The connect payloads carry no names.** v2 signed the sorted agent
  names into the proof so a captured proof could not be replayed to
  serve a different roster; a ticket issued to one key cannot be
  replayed at all, so the names left the handshake and moved to where
  they now belong — registration frames on the open link.
- **The role tags** (`funduq-connect-provider` / `funduq-connect-funduq`)
  mean neither signature can be presented as the other.

**"souk signs first" changed shape, not substance.** The old handshake
had souk sign before the provider produced anything worth stealing, so a
provider could walk away from a souk it did not recognise. In v4 the
provider signs first on the socket — but it learned the funduq key over
TLS at ticket time and bound its proof to that key, so the proof is
worthless to any other funduq; and the `answer` in the welcome proves
the far side actually *holds* the key the ticket response named. The SDK
verifies it (`confirm_connect`, raising `WrongFunduq`) **before treating
the link as open** — upstream hands the answer over before the link is
recorded open, so a provider that raises there never appears in the
roster and never receives a run. The mutual-identity property survives;
the frames that carried it do not.

Pinning is still the recommended shape: `SoukProvider(souk_public_key=…)`
pins one funduq, and the same channel that carries the URL carries the
fingerprint. Unpinned, the provider verifies the answer against the key
`POST /tickets` returned — enough to notice a broken souk, not enough to
notice one substituted before ticket time — and logs the fingerprint so
the value to pin is in reach. TOFU is deliberately **not** built, for
the reason it never was: souk's key is provisioned, so a rotation would
jam every provider at once with per-provider pin-clearing as the
recovery; a configured key costs one line and has no such state.

**Channel binding is out — decided, not overlooked.** It is the standard
answer to a relay, and unusable here: a Zscaler-class proxy terminates and
re-originates TLS by design, so the two sides never derive the same value
and the check fails every time. Enforcing it would not harden the
deployment; it would lock out every enterprise running one, which is the
deployment this exists for. It was also never the fix — the defect is a
*stealable* credential, and the ticket flow closes that with an
intercepting proxy in the path:

| | before | after |
|---|---|---|
| see the traffic | yes | yes — it terminates TLS, that is its job |
| capture the credential, connect later as that provider | **yes** | **no** — the ticket is single-use and the proof names its funduq |
| tamper with frames on a live connection | yes | yes |

The bottom row stays open, deliberately: run inputs and events are not
individually signed, an intercepting proxy is in the trust model by
construction (the enterprise installed it and pushed its CA — see
`docs/threat-model.md`), and signing every frame is a large cost against
a threat the operator chose.

**`version` selects the handshake, and there is no compatibility
branch.** A v2/v3 client is refused by name ("this souk speaks wire v4,
the ticket handshake"), not by a bare signature failure — which is what
an attack looks like too, and would send whoever is debugging it
somewhere unhelpful. Dual-shape acceptance was skipped for the same
reason as every previous bump: every provider that exists is in this
repo behind one SDK, plus one Go probe whose whole point is tracking the
current wire.

**`welcome` is queued before attaching completes**, and that ordering is
still load-bearing for the frames behind it — everything this link will
ever receive queues after it. One old race is now structurally gone
rather than merely tested for: attach is nameless, so a fresh link
serves nothing and **no `run` frame can precede the first `registered`**,
where under v2 a provider with work waiting could legally see `run`
arrive as the first post-proof frame. The reconnect tests stay anyway;
orderings that are "structurally impossible" are exactly the ones worth
a regression net.

### Registration on the open link

Attach carries no names. What a link serves is what it last published,
via frames — the signed registration/deletion payloads (and their HTTP
routes) are gone upstream, because the key was proved once, when the
link opened, and a per-operation signature would only re-prove it. What
that asks of this transport is the ordinary thing: **an open link stays
the party that opened it** — one read loop, one identity, established at
the handshake and never rebound.

```
provider → register    {agents: [{name, description?, agentCardExtra?,
                                  metadata?, takesInterjections?}, …],
                        providerName?}                            # /ws/provider
provider → register    {models: [name, …], metadata?}              # /ws/kyok
souk     → registered  {names: [...]}     # echo of what is now live
provider → deleteAgent {name}    /    deleteModel {name}
souk     → deleted     {name}
souk     → error       {message, …}       # answered; the socket stays open
```

The semantics are core's, restated here because they shape the wire:

- **Not registered is offline.** An open link that has published nothing
  serves nothing; runs are only offered after the first `register`.
- **A roster replaces, it does not append.** `register` carries the full
  roster, and publishing a shorter one takes the omitted names offline.
  The SDK re-registers its whole roster on every reconnect, which is
  what makes reconnection self-repairing.
- **Delete is refused for an agent with a conversation behind it** — the
  one guard a deletion still has. Core's refusal reaches the provider
  verbatim in an `error` frame; a registration mistake is a caller error
  on an authenticated link, not a breach, so it is answered and the
  socket stays.
- **An agent entry *is* `funduq_contract.Registration`** (name,
  description, `agentCardExtra`, `metadata`, `takesInterjections`). The
  `REGISTRATION_FIELDS` list this used to be compared against was
  withdrawn at revision 11, and the gateway's own validation model with
  it: both ends validate into the one model, which is `extra="forbid"`,
  so a misspelt key is answered with an `error` frame naming the field.
  Two things the model does not police and this transport does: `agents`
  being a non-empty list at all, and the name being URL-shaped
  (`[A-Za-z0-9_-]{1,128}`) — core validates none of it, and a name with a
  slash in it would register happily and then be unaddressable on every
  road this gateway serves. Skills still reach souk only through
  `agentCardExtra`; everything else in the card is dropped silently by
  core, exactly as before.
- **`takesInterjections` is per agent, and the serving party answers
  it** (revision 12). Core calls
  `connection.takes_interjections(agent_name)` *inside*
  `register_agents` and **overwrites** whatever the incoming
  `Registration` carried, so the capability on the agent card is derived
  from whoever actually serves the agent rather than typed by an author.
  Over a wire that party is on the far side of the socket, so this frame
  field is the only thing the gateway can honestly answer from: the SDK
  derives it from the presence of the runtime's interjection hook, and
  `SocketProvider.takes_interjections` returns what the last roster
  declared (`False` for a name this link never published — declaring a
  capability for a name nobody registered would be a guess). The A2A card
  announces it in the standard slot, `capabilities.extensions` with the
  interjection extension's URI; the card declares *understanding*, not
  acceptance, since whether an interjection is taken is still the
  verdict's call at delivery time.

  The trap worth naming: `takes_interjections` is **not** on the
  `ConnectedProvider` protocol, so a connection object that omits it
  type-checks, attaches, and raises `AttributeError` at the first
  registration, three layers from the cause. That is why
  `SocketProvider` asserts its own surface at construction (below), and
  why the SDK's runtime hook being a **method** rather than a property
  matters: read as an attribute it is a truthy bound method, which
  declares every agent interjection-capable and never fails loudly.

### Once attached

| direction | frame | carries |
|---|---|---|
| ↑ | `{"type": "register", "agents": […], "providerName"?}` | the full roster this link serves — see above |
| ↓ | `{"type": "registered", "names": […]}` | what is now live, sorted |
| ↑ | `{"type": "deleteAgent", "name"}` | remove one record outright |
| ↓ | `{"type": "deleted", "name"}` | it is gone |
| ↓ | `{"type": "run", **DeliveredRun}` | an **offer**: the frame is upstream's declared envelope — `DeliveredRun.model_dump(by_alias=True)` and never `exclude_none` (`runId`, `agentName`, `runInput`, `threadId`, `metadata`), rebuilt on the far side with `model_validate` once the transport's `type` key is stripped. Since revision 11 core *hands `deliver` the `DeliveredRun` itself*, so there is no translation left on either side. Canonical frame in `docs/upstream-contract-vectors.json`'s `wire` section; neither end hand-writes the mapping |
| ↑ | `{"type": "ack", "runId", "accepted", "reason"?}` | whether this provider took it — and the answer is a **receipt**, produced from the provider's own state without asking the agent anything (upstream holds the next utterance of the same conversation until it lands, so a link that waits for the agent to start turns that round-trip into startup time). A bare `accepted: false` is how a full one says so — transient, souk re-offers later. `reason` makes the decline *permanent* (an input that does not parse): souk fails the run with the provider's words recorded verbatim and stops re-offering. souk invents no reason vocabulary; the string is the provider's own. The three names are `funduq_contract.Verdict`'s (accepted / declined / refused); the v4 frame keeps its boolean-plus-reason spelling and the gateway translates, so the *meaning* has one definition and the frame stays what every shipped provider sends |
| ↑ | `{"type": "event", "runId", "event"}` | one AG-UI event; authorized against the run's claim |
| ↑ | `{"type": "finish", "runId"}` | that run's stream ended |
| ↓ | `{"type": "cancel", "runId"}` | a request, not an order — outcome decided when the stream ends |
| ↑ | `{"type": "query", "queryId", "method", "params"}` | a question about the work souk gave this provider |
| ↓ | `{"type": "queryResult", "queryId", "result"?, "error"?}` | its answer, correlated by `queryId` |
| ↓ | `{"type": "error", "message", "runId"?/"name"?}` | server-side rejection of a frame (bad runId, not the holder, a refused registration or deletion) |

### Queries: the one thing here that expects an answer

Every other frame is fire-and-forget. `query` is not, and it is worth
being explicit about why it earns the machinery — a correlation id, a
pending map, a timeout, and a rule for a socket that dies mid-question.

**A provider sees exactly what the caller sent for its run, and nothing
more.** An AG-UI client resends its whole history every turn by
convention; A2A's `message/send` carries one message. The same agent,
unchanged, cannot tell a tenth turn from a first — and souk has held the
thread the whole time. `funduq_provider_sdk.FunduqLink.thread_messages`
is the question, and this is how it crosses a wire.

```json
↑ {"type": "query", "queryId": "9f3c…", "method": "thread_messages",
   "params": {"threadId": "thread_…", "limit": 20}}
↓ {"type": "queryResult", "queryId": "9f3c…", "result": [ …messages… ]}
```

- **`limit` is applied by souk**, not by the caller on return. The
  parameter exists to keep the response frame bounded; trimming after
  receiving would bound nothing and put a months-old thread on the wire to
  do it.
- **A provider may only read threads for agents it serves.** Not in the
  upstream design and added here. Thread ids are not guessable, but
  unguessable is not an authorization rule: a provider that served one run
  knows that thread id permanently, and would otherwise keep reading the
  conversation after being de-listed, or after the agent moved to another
  stall. A thread names its agent and an agent is `(provider_key, name)`,
  so souk can already make the comparison. "Not yours" and "no such
  thread" get the *same* answer — telling them apart would confirm a
  thread's existence to somebody who may not read it.
- **A malformed query is answered, not dropped.** The far side is waiting
  on that `queryId`; silence costs it the full timeout for a mistake souk
  could see at once.
- **A dead socket fails its outstanding queries immediately**, rather than
  leaving them to time out. A question was asked of *this* connection and
  nothing will ever answer it. It is not retried on reconnect either: the
  agent asked mid-run, and whether it still wants the answer is the
  agent's to decide.
- **What may be asked is deliberately short.** This is not a mirror of
  souk's API: every method admitted is one more frame type every
  transport must carry. The gateway used to read upstream's
  `contract.LINK_QUERY_METHODS`, which was withdrawn at revision 11 with
  the rest of the field-list constants — the models are the single
  definition now and there is no list left to read. So the one method is
  named in `ws_provider.QUERY_METHODS` and asserted to be a subset of
  `FunduqLink.__abstractmethods__` at import: the link ABC is the surface
  that would actually grow a second query, and a verb that stops existing
  upstream fails at import here rather than at a provider.

Adding frames like these does **not** bump `version`. They are additive:
a provider that never asks is unaffected, and an older gateway answers an
unknown frame type with `error`. The version selects the *handshake*,
which is the part that genuinely cannot interoperate across shapes.

### Which object is which

`FunduqLink` is one provider joined to one funduq — both directions, one
object — and the socket client in souk-agent-sdk is one, because over a
wire that is literally true: run frames arrive on the same socket event
frames leave by.

The gateway's `SocketProvider` is **not** one, and upstream's own docstring
says so. It sits on souk's side, holds an outbound queue and no runtime,
and only carries work outward. It satisfies souk's `ConnectedProvider`
protocol structurally, and checks itself against a named
`_PROTOCOL_SURFACE` at construction — because souk sizes a
capacity bucket from `max_concurrent_runs`, and a connection that forgets
it attaches perfectly well and then fails inside the broker, three layers
from the cause. That list used to be upstream's
`contract.CONNECTED_PROVIDER_ATTRS`, withdrawn at revision 11; the lesson
it encoded outlived it, so the surface is named locally — and it is
deliberately *wider* than the protocol declares, because
`takes_interjections` is not on `ConnectedProvider` at all and core calls
it during registration. The check is against the class rather than the
instance: `public_key` and `max_concurrent_runs` are properties, so
asking `self` inside `__init__` would run their getters against fields
not yet assigned and report every one of them missing. It deliberately exposes no `sign_connect`: core would
otherwise mint a ticket and sign on this object's behalf, and this object
*cannot* sign — the only holder of the key is the real provider on the
far side of the socket, which is the whole point of the handshake.

A declined offer costs the run nothing: it stays queued and is offered
again when something changes — a run arriving, a provider registering, one
of this provider's runs ending. Not immediately, deliberately: asking
again at once is asking a provider that just said no, with nothing about
the answer having changed.

**The conversation gate moved from claim to finish** (revision 11). One
thread has one active run, as before; what changed is *when* the next
utterance is offered — at the end of the turn rather than the moment the
current run is claimed. Nothing on this wire spells it, and that is the
point: a provider sees only that the next `run` frame for a thread
arrives after it sent `finish` for the previous one. The single bypass is
a declared interjection naming the thread's claimed head
(`forwardedProps.addressedRunId` on AG-UI, the A2A extension's metadata
key); naming anything else is rejected at the door, and a declaration
whose target settles before delivery degrades to an ordinary next turn.

Flow control is `maxConcurrentRuns`, declared once at hello. souk keeps a
bucket that size and offers nothing while it is empty. No credit frames
and no counting in the transport — the number is a fact about the
provider, and souk sees for itself when a run ends. Leaving it unset
declares *unlimited*, and upstream takes that at its word: an unlimited
provider that declines is counted `misdeclared`. Pacing is declared, not
improvised.

Two deadlines apply to an offer, and only one governs. souk wraps every
offer in `RunBroker.deliver_timeout_seconds` (5s, a `CoreSettings` field
since revision 14) because it has a single delivery loop and an offer
that never returns stops dispatch for everybody. The gateway's own
`ACK_TIMEOUT_SECONDS` is longer and is a backstop for a souk that offers
without a deadline; shortening it to "win" would put the same policy in
two places and let them drift.

**Answering late is not a path — it is a guard.** `accept_late_ack` and
the `answered_late` counter were withdrawn at revision 11: a verdict
arriving after the delivery window matches nothing and is answered
`false`. souk has already taken the run back and will simply offer it
again, so relaying a `false` for an offer this side thought it had
delivered is normal rather than an error, and the gateway deliberately
drops the "did anyone hear that" answer instead of sending an `error`
frame — which would teach every provider to log a scare on its own slow
morning.

**`cancel` returns a receipt, not an outcome.** It is `async` and returns
`bool` since revision 11 — "the ask is on the wire" — because funduq
decides the outcome from what the run's stream does next. Returning
`None` logs a warning on every cancel, and would be a lie in the other
direction if it tried to mean more.

**Liveness is not a heartbeat.** `online` is *registered on a live
link*: souk holds a connection serving that agent or it does not, so a
link's first `register` is what brings its agents online and a dropped
socket takes them offline in the same instant. There is no window to age
out of and no sweep behind the boolean. Because a link publishes one
roster, its agents go online and offline together, by construction.
`last_seen_at` is still recorded and still worth reading, but it answers
a different question: how long since anybody was here, which `online` no
longer says anything about. WebSocket ping/pong keeps intermediaries from
reaping idle sockets and is not the liveness signal.

**A dropped socket now ends every run it was holding — a real behavior
change, recorded honestly.** Under this repo's earlier wire, "a dropped
socket ends nothing" was a property probed and kept from the gRPC days:
events are addressed by `runId`, so a provider could reconnect and
report the rest, including how runs ended. funduq 0.0.4 decides
differently, and as the published core it wins: liveness is a fact funduq
holds, not a deduction from a timestamp, and **a provider that stops
serving while still holding a claimed run has taken work and never ended
it** — the run fails at once as `provider_left_holding_it`, and the same
observation increments the provider's `abandoned` quality counter
(enough of those and the provider is withdrawn from service). Only
*queued* runs survive a drop: nothing had been promised about them, so
they wait for the reconnect and are offered again. What reconnect-and-
finish used to cover — a blip mid-run — is now a failed run and a mark
against the provider, and an agent author should know that a provider's
socket is part of its run's fate. This repo's position — that a
grace window would let a reconnect finish honest work without reviving
the two-clocks problem upstream removed — is argued in
[hukaichun/funduq#214](https://github.com/hukaichun/funduq/issues/214);
until that lands, this document describes the published behavior, not
the preferred one.

At-least-once delivery (an ack per *event*) remains expressible and
remains unbuilt — the `ack` frame above answers an offer, not an event.
The `reserved 5` lesson travels as words here: a retired frame type's
name is never reused.

## KYOK relay: `WS /ws/kyok`

The socket an **LLM provider** connects out on — the party upstream's
KYOK redesign made first-class (upstream `docs/integration-contract.md`;
the per-mechanism pages it used to live in are gone).
The agent-provider-facing `POST /kyok/v1/chat/completions` endpoint is
untouched — an OpenAI-compatible URL is the whole point of that side.

This section previously described a different wire: an anonymous
"bridge" that rendezvoused with souk over a caller-minted `sessionId`,
with a paragraph of apology for everything a routing key that is secretly
a credential cannot do (who may open a session, whether two sockets are
the same party, what a token may safely carry). Upstream's answer was not
a better session id but an identity: the party answering completions
registers Ed25519 offerings like any provider and attaches like any
provider, and every one of those questions became answerable. The
`session_routing_key` fix this document used to describe — souk hashing
the session id before putting it in the token — is gone along with the
session id itself: a KYOK token now carries `{runId, providerKey,
agentName, exp}` and nothing caller-side at all.

The arrival is the agent provider's, rule for rule: the same
`POST /tickets`, the same two-frame handshake (same `handshake.py`
payloads, same version — the hello merely omits `maxConcurrentRuns`,
which a completion relay has no use for), and then a `register` frame on
the open link, `{"models": [...], "metadata"?}` where the provider
socket says `agents`. The signed `POST /llm-providers/register` road is
gone upstream — `sign_llm_registration` no longer exists — and the same
roster-replace and delete semantics apply (`deleteModel`, refused by
core while a live run is bound to the offering). Names are deliberately
not exclusive across identities: two providers both offering `gpt4` is
normal, and an offering is `(provider_key, name)` exactly as an agent
is. A socket that drops takes its offerings offline in the same instant,
and a later attach with the same identity takes the offering over.

A caller opts a run in with
`metadata: {"kyok": {"llmProvider": {"providerKey", "name"}, "context"}}`
— no extra connection, no SDK required. souk binds the run at start,
strips `context` from everything it persists, and resolves
binding → attached link per completion call; not attached is a fast 503,
the same shape as an offline agent (`souk-client-sdk`'s `KyokBridge` is
the reference LLM provider and builds that metadata via
`run_metadata()`).

| direction | frame | carries |
|---|---|---|
| ↑ | `{"type": "register", "models": […], "metadata"?}` | the offering roster this link serves |
| ↓ | `{"type": "registered", "names": […]}` | what is now live |
| ↑ | `{"type": "deleteModel", "name"}` | remove one offering's record |
| ↓ | `{"type": "deleted", "name"}` | it is gone |
| ↓ | `{"type": "completionRequest", "requestId", **DeliveredCompletion}` | upstream's declared envelope — `DeliveredCompletion.model_dump(by_alias=True)` (`runId`, `providerKey`, `agentName`, `body`, `llmName`, `context`, `actorChain` — the chain rides the envelope since contract revision 7): the run, the *proven* calling agent, which of this provider's models was addressed, the caller's opaque context, the actor chain, and the OpenAI-shaped body. Since revision 11 core hands `complete` this model itself — its own `CompletionRequest` and the SDK's `from_request` translation are both gone — and `body` is validated as OpenAI's own chat-completion request shape at the door, `model` among its required fields, with extension keys passed through verbatim: a body that is not a chat-completion request is a **400** from `KyokAdapter.complete` and never reaches this socket. `requestId` is this transport's, so it comes off the mapping before `model_validate`. Canonical frame in `docs/upstream-contract-vectors.json` |
| ↑ | `{"type": "chunk", "requestId", "data"}` | one OpenAI `chat.completion.chunk`; validated on souk's side, an invalid one fails the completion |
| ↑ | `{"type": "done", "requestId"}` | end of that response |
| ↑ | `{"type": "error", "requestId", "message", "refusal"?}` | provider-side failure or refusal, so the waiting completion fails fast instead of timing out — policy (throttling, billing, refusing a chain it does not recognise) is the LLM provider's, and this frame is how it says no. `refusal` is a structured payload relayed to the calling agent *intact* (in-stream as the `{"error": ...}` value, or as `error` on the non-streaming 502 body) — the envelope souk guarantees; the vocabulary inside is the two roles' own |
| ↓ | `{"type": "error", "requestId"?, "message"}` | server-side rejection of a frame (unknown type, a refused registration, or a `requestId` not in flight on this connection) — answered, not a teardown, same as the provider socket |

`requestId` multiplexing means one socket serves concurrent completions.
A gap of `CHUNK_GAP_TIMEOUT_SECONDS` (120s) between frames of one answer
fails that completion — not a per-completion deadline, a
provider-is-gone detector for the case the socket has not noticed.

Connection semantics, carried over or sharpened:

- **An answer is accepted only on the connection its request was
  delivered to.** This survived the redesign because it was the security
  fix worth keeping, and it now holds against a *stronger* intruder than
  the old socket ever faced: a second connection with the same identity,
  attached for the same offering — every credential check passes — is
  still refused an in-flight requestId it was not delivered.
  `tests/test_ws_kyok.py` drives exactly that. Membership in the
  connection's in-flight table, not anything a frame carries, is what
  authorizes an answer; a requestId is a multiplexing key within the
  connection that received it, never a bearer capability on an open
  route.
- **A socket dropping mid-answer fails its in-flight completions
  immediately** — a truncated answer must never pass as a complete one.
  Requests delivered but unanswered when a socket dies are not re-queued.
- **Two sockets, one identity, one offering**: the later attach takes
  over the offering for future completions (core's relay maps each
  offering to one live link). In-flight answers stay bound to their own
  socket, per the rule above.

Provider and KYOK stay **two endpoints**, not one multiplexed socket:
one carries runs for agents, the other completions for model offerings —
different roster, different frames, and one identity may hold both at
once. Merging them buys one route at the cost of role-dispatch on every
frame.

What souk still deliberately does not do: validate the LLM output a
provider returns (a provider must treat KYOK output as untrusted input
regardless), or impose a spend ceiling. The ceiling belongs to the LLM
provider, which is now an identified party with the material to enforce
one — the run id, the proven calling agent, the caller's context and the
actor chain arrive on every `completionRequest` frame
([hukaichun/funduq#26](https://github.com/hukaichun/funduq/issues/26)).

## The A2A door

Core no longer speaks JSON-RPC at all. `funduq.protocols.a2a.A2AAdapter`
hands back A2A's own messages — `AgentCard`, `Task`, the update events —
and writes no envelopes, no method names, no error codes. The gateway
mounts a2a-sdk's `JsonRpcDispatcher` over a `RequestHandler`
(`souk_server/api_a2a.py`), which puts the whole protocol vocabulary in
the package where it is versioned instead of hand-written here.

**The handler between them is upstream's now** (funduq#225).
`A2ARequestHandler` is a real `a2a.server.RequestHandler` bound to one
agent: it owns `MessageToDict`, `validate_request_params`, the
`configuration.return_immediately` / `configuration.history_length`
mapping — so a Task now comes back carrying history when the caller asked
for it — and which of A2A's operations funduq offers at all. This file
had a hand-rolled copy of that, and a copy of a mapping is a copy that
drifts: the configuration fields upstream deliberately does and does not
honour were simply *absent* from ours. `SoukA2ARequestHandler` subclasses
it for exactly two things, which are the two a transport owns — the
errors that leave A2A's vocabulary as HTTP statuses, and the paused-run
ask ids A2A has no field for. Everything else is inherited.

- **`enable_v0_3_compat=True`, and forgetting it drops every v0.3
  client.** Which protocol version a request speaks rides the
  `A2A-Version` HTTP header, and *no header means 0.3* — with the flag
  on, the dispatcher accepts the v0.3 method names and converts the
  shapes. That header is exactly why this cannot live in core: only the
  transport ever sees one, and the version decision belongs to the party
  holding the evidence.
- **Two errors deliberately leave A2A's vocabulary**, because A2A has no
  word for either and one that means something else would be worse:
  `AgentNotFound` is a **404 on the route** — the agent is the endpoint,
  resolved from the path before the dispatcher runs, never a JSON-RPC
  error inside a 200 — and `ThreadQueueFull` is a **429 with
  `Retry-After`**: backpressure, the request was *not* accepted, and
  accept-then-expire is the lie this refuses to tell.
- **Cancel metadata passes through whole.** A run on a thread that bound
  an authority at birth can only be stopped by one of that thread's
  authorities, and the proof rides in `CancelTaskRequest.metadata`
  (`metadata.cancel`, with `metadata.resolution` beside it). Drop the
  field and every cancel on a bound thread is refused; forge nothing —
  funduq verifies the signature, not the envelope. There is **no
  `metadata.delegation`**: the session delegation certificate was removed
  at revision 15 and nothing here signs, sends or relays one — see "The
  proofs" below.
- **`presenter_key=None` today, and the deployment invariant is
  upstream's operational-limits §1**: core's caller doors are not
  independently safe. Verifying a chain proves the head's key signed hop
  zero, never that whoever *presented* it holds that key — and the chain
  is not a secret, since every serving provider receives it verbatim. A
  deployment therefore puts an authenticating seat in front of the
  doors, and this gateway is that seat: the adapter call sites in
  `api_a2a.py` are the plug point where an edge-authenticated caller's
  key becomes `presenter_key`, at which point funduq refuses a chain
  whose last hop someone else signed. Unbuilt here (open market, no edge
  auth), and this paragraph is the record that the exposure is chosen,
  not missed.
- **Events are dumped `exclude_none=True`, and unknown event types are
  relayed untouched.** A default dump injects `timestamp: null` into the
  caller's stream; and funduq is a relay, so a provider on a newer AG-UI
  must not be cut off by an event type nobody here has heard of.

Interjection rides the standard extension point — the A2A extension's
metadata key, and `forwardedProps.addressedRunId` on AG-UI — and funduq
handles it; the gateway just relays. The agent card announces which
agents understand one in `capabilities.extensions`, from what the
serving link declared at registration.

### Which funduq error becomes which status

One mapping, registered once for the whole app
(`souk_server/deps.install_error_handlers`), because which status a
failure deserves is a property of the failure and not of the route that
hit it. The rows that matter for this revision are the singular acts:
`InvalidCancel`, `InvalidResolution` and `InvalidView` are plain
`ValueError`s upstream, so without a row each one reaches a caller as a
**500** — a server fault for a caller mistake, saying nothing about what
to send instead.

| error | status | why that one |
|---|---|---|
| `InvalidCancel`, `InvalidResolution`, `InvalidView` | **401** | the act was refused for want of a valid proof from one of the run's authorities. `InvalidView` is listed for completeness only: the read doors answer an unproven view as *absence* and never raise it outward |
| `ThreadMembershipRequired` | **403** | writing to a thread bound to a responsibility segment while being neither its head nor its serving provider. Not 401 — the caller identified itself perfectly well, its chain verified; it simply is not a member of this conversation. A bare `Exception` upstream, not even a `ValueError`, so it is the likeliest of these to have surfaced as a 500 |
| `InvalidChain` | 401 | a tampered actor chain, refused at the door (`funduq_contract.InvalidChain`, which replaced core's `InvalidActorChain`) |
| a KYOK body that is not a chat-completion request | **400** | `model` is required, and the body is validated as OpenAI's own request shape before any socket is touched |
| `ThreadQueueFull` | 429 | backpressure; the A2A door maps it itself before the JSON-RPC dispatcher can swallow it |

### The proofs: view, cancel, resolve

Three singular acts on a chain-bound run, and after revision 16 they are
no longer one family.

**A read needs a view proof** (revision 13), and its absence is answered
as **absence**: `get_task` and `resubscribe_task` on a run whose thread
is bound to a chain answer "not found" without one, because *existence
is part of what is guarded*. Unbound runs stay as public as their
funduq-minted ids. This is a real behavior change for a caller that read
such runs before, and it fails quietly by design — the read that used to
work now looks like a run that is not there.

**The read circle is wider than the act circle.** Every actor on the
run's chain may sign a view — the parties responsibility flowed through
may look — while cancel and resolve stay with the head and the serving
provider.

A2A read requests carry no caller data at all, so the proof has nowhere
in the protocol to travel and rides the transport instead:

```
X-Funduq-View: {"publicKey":"…","timestamp":…,"signature":"…"}
```

compact JSON, signed over `funduq-view:{run_id}:{timestamp}`, reaching
core through `A2ARequestHandler(view_metadata_of=…)` as `{"view": {…}}`.
The gateway judges none of it — whether the signature verifies, whether
the signer is on the chain, whether the timestamp is inside the 60-second
window are core's questions about a run this code has never seen. Absent
or malformed passes **nothing** rather than raising: a 400 there would
tell a caller holding a bad proof that there was a run behind the id
worth fixing it for, which is precisely what an unauthorized read must
not learn. `souk-agent-sdk`'s `view_headers()` builds the header from an
identity, and the same header name is what a thread read would use.

**A resolve proof signs the ask, not the clock** (revision 16). The
signed bytes are `funduq-resolve:{run_id}:{sha256 hex of the outstanding
ask ids, sorted and NUL-joined}`, canonicalized inside
`funduq_contract.resolve_payload` and nowhere else, and the wire proof
shrinks to `{publicKey, signature}` — **no timestamp, and the 60-second
freshness window does not apply to resolve at all**. Instance binding
replaces the clock: a later pause has new ids, so the proof never
verifies against any ask but the one it was signed for. Cancel and view
keep the timestamp family and the window; resolve was the one act where
replay changed what happened, and it is now the one act that cannot be
replayed.

**So a paused run must say what it is waiting on** — the one genuinely
new capability this gateway grew for revision 16, rather than an edit. A
caller that cannot enumerate the ask ids cannot build a proof at all, and
the pause is unanswerable. Core holds them on the run's metadata
(`funduq.pause.outstanding_asks`) and neither protocol has a field for
them, which makes surfacing them this seat's job:

| door | where the ids appear |
|---|---|
| A2A | `funduq/outstandingAsks` on the Task's `metadata` — the same visibly-not-A2A namespace as core's own `funduq/cancelRequested`, on every Task this door hands back |
| AG-UI / threads | `active_run.outstanding_asks` on `GET /threads/{id}` |

Both are read off the *run* rather than off a status name, so they answer
the question actually asked — is anything outstanding — and both are
sorted, matching the canonical order the payload hashes in, so a signer
has one fewer thing to get wrong. Absent when nothing is outstanding, so
the key's presence means something. (`souk-client-sdk` also tracks the
ids from the stream as it goes — announced tool calls not answered, plus
the interrupts the `RUN_FINISHED` outcome names — and exposes them as
`last_outstanding_asks`, for the caller that never left the stream.)

**Delegation is deleted** (revision 15). `delegation_payload`, the
`funduq-delegate` tag, `sign_delegation`, `metadata.delegation` and
`forwardedProps.delegation` are all gone, along with
`SESSION_TOKEN_TTL_SECONDS`; a proof now counts for exactly the key that
signed it. Upstream's reason is one this document agrees with from the
other side of the boundary: the certificate was the one piece of core's
identity machinery that *manufactured* authority ("B counts as A until
T") rather than recording a fact and demanding proof from the key it
names — an unscoped grant is policy, and policy belongs at the
authenticating seat, which holds the keys and decides which one signs.
That seat is this gateway. Nothing here ever issued one, so nothing here
had to be migrated; what changes is that a deployment wanting delegation
builds it where `presenter_key_of` plugs in, not by minting a certificate
core would honour.

## Health and lifecycle

Liveness and readiness stay two endpoints (`/healthz` touches nothing;
`/readyz` maps `Health.ready` to a status code). **Ready is core's
conjunction of three facts — database answers ∧ `schema_current` ∧
`dispatching`** — and `background_running` is gone from `Health` because
the health sweeps it reported no longer exist: upstream removed the
paused-run deadline and the sweep loop with it, so **a paused
(`input-required`) run now waits indefinitely**, across restarts. It
costs a row, not a slot; the parties that hold the lever (the asking
provider's `Interrupt.expires_at`, the caller that owes the answer) are
the ones funduq's clock could only have overruled. Relatedly, the final
status of a run is decided in a fixed order in which an interrupt
outcome outranks everything — a stream that ends on `RUN_FINISHED` with
unanswered tool calls is `input-required`, not `completed`: the run
stopped to ask, and is not filed as one that finished.

`Funduq.start()` returns the ids of runs it had to fail as orphaned by a
previous process; the gateway logs them (`souk_server/server.py`) so a
restart that ended work says which work it ended.

## MCP: the docent

MCP is served on the same HTTP listener, as an adapter **in this
repo**, not in core: the official SDK drags transport dependencies core
is forbidden to have, and MCP has no in-process consumer — its only rung
is the wire, so the "protocol translation is core" rule does not bind
it. Written as two layers (pure mapping over souk's types + SDK
binding) so the mapping can be promoted to core if a second consumer
ever appears.

**Scope: discovery, not invocation.** Talking to an agent is A2A's
job — souk already serves cards and JSON-RPC for exactly that, and
wrapping `start_run` in an MCP tool would build a second, lossier
invocation path beside a standard one (an earlier draft of this section
did exactly that, and it was scoped out on review). What MCP adds is
the thing A2A assumes you already did: *knowing what is in this souk.*
MCP hands out the map; A2A does the walking.

**Built** as `souk_server/mcp_docent.py`, mounted at `/mcp` on the same
listener (`tests/test_mcp_docent.py`; probed end to end with a real MCP
client over a real socket). The audience is a **docent** — the guide who
walks a visitor through the market — not an operator, which is what
fixes the surface below at "who is here, what do they do, where do I go".

- **Tools** (read-only, `read_only_hint` declared): `browse_souk` (the
  market grouped by stall), `search_agents(query)` (over name,
  description, skills and tags), `describe_agent(name, provider="")`,
  `describe_stall(provider_key)`. Tools rather than resources alone
  because most MCP clients wield tools far more readily.

  `browse_souk` takes a required `only_online` rather than no arguments
  at all, and that is a workaround, not a design: a live model called the
  zero-argument version with `{}""` — invalid JSON — retried identically,
  and killed the run, while every tool here that takes a parameter was
  called cleanly. Optional was not enough; the model still chose to send
  nothing, and sending nothing is what it does badly.
- **Resources.** `souk://providers` (stall-shaped) and `souk://agents`
  (flat, each record still carrying its provider), plus the
  `souk://agent/{provider}/{name}` template.
- **Every answer carries directions and a provider key.** The
  `a2a_endpoint` is the pair route, `/a2a/{fingerprint}/{name}/rpc`,
  because that is the only kind of address there is: a display name is
  not unique across providers, so a direction built from one leads
  somewhere only by luck. The provider's `public_key` rides on every
  agent record beside the fingerprint, because that is what makes an
  answer *placeable* — souk-directory groups by stall, and the AI-town
  layout derives a stall's map coordinate by hashing that key. The
  fingerprint is derived from the key and is never the thing to compare.
- **Ambiguity is handed back, not guessed.** A duplicate display name
  returns the candidates, each with its own provider and address, rather
  than picking one. There is no route left that could pick one, which is
  the point: the refusal moved to where the asker can answer it.
- **`online` never travels alone.** Each record pairs it with
  `last_seen` in words ("40s ago", "3d ago"), because the boolean cannot
  separate "stepped away" from "gone for a week" and that is the
  difference a visitor deciding whether to wait is asking about.
- **Notifications.** Not built. If built, necessarily paired with a
  poll: a change hook fires for registrations and de-listings, but a
  boolean derived from a live mapping has transitions no hook names —
  a directory that advertises live updates off registration events alone
  would miss the ones its users care most about. Not load-bearing either
  way.
- **Not exposed:** invocation (A2A's job), registration/identity
  (provider business), KYOK (bridge business), threads/runs (run
  observation is a different feature with a different audience — add
  it later if wanted, deliberately absent now), admin (deployment
  policy — the managed-gateway example's job).

Core serves all of it from `list_agents` alone
([hukaichun/funduq#31](https://github.com/hukaichun/funduq/issues/31), now
closed: typed query models landed, enumeration was withdrawn as
unneeded). Search filters that roster in Python rather than querying —
a market's worth of stalls is not a log, and if a deployment ever
outgrows it, that is when a core query earns its place.

**The docent is also a stall.**
`providers/pydantic-ai-agent/config.docent.yaml` runs it as an ordinary
provider — its own key, a row in the roster, runs served over
`/ws/provider` — reaching the market through `/mcp` as a real MCP
client rather than through `GET /agents`. Two things that buys: every
frontend gets a guide instead of each one building its own, and the
surface above acquires a consumer, so a question `/mcp` cannot answer
shows up as a guide that cannot answer it, in a running process rather
than in review. (`souk_tools.py` — the same capability as plain
function tools — named this exact moment as when to prefer a real MCP
server, and stays off in that config so the model has one way to ask,
not two.) It runs unthrottled on purpose: backpressure is a feature at a
working stall and a bad front door at the gate.

**The one question the docent cannot answer: "are they busy right
now?"** Capacity is per-stall in souk's model (`maxConcurrentRuns` is a
provider's budget across everything it hosts), and the roster carries
nothing about it — so "you'll have to wait, they're serving someone",
one of the few genuinely market-shaped answers a guide could give, is
unavailable. Note where the data actually is before reaching upstream
for it: this gateway knows each connected provider's declared budget (it
arrives in the hello frame) and the broker knows what is in flight, so
the honest version is per-process serving state, not a core projection —
and it would read as authoritative while being blind to providers
connected to another replica.

## Serving state stays out of core's database

**Today the gateway persists nothing.** Its only database access is
`deps.get_session`, which borrows core's session and hands it to a route;
every table in the deployment belongs to funduq, whose alembic chain
**ships inside the wheel** — `python -m funduq.migrate` is the whole
migration story, there is no `alembic.ini` anywhere in this repo, and the
one-shot `souk-migrate` compose service runs exactly that command,
reading `FUNDUQ_DATABASE_URL` / `FUNDUQ_DB_SCHEMA` from its environment.
Tests migrate through the same door (`funduq.migrate.migrate(url)`).
That is not an accident to preserve by luck — it is the state this rule
protects.

**When serving *does* need persistence, it is isolated from core's, and
the isolation is structural rather than a naming convention.** Whatever
it turns out to be — edge-auth records, rate limits, admin state, an MCP
event store for resumable streams — it does not get a table in core's
schema, and obviously never gets a revision into the wheel's chain: the
rule is not about a directory, it is that **no serving table lives in
core's schema**.

Three reasons, in the order they bite:

1. **A shared migration chain merges the two projects.** The chain is
   upstream's, versioned with core and shipped in its wheel. A gateway
   table added to core's schema outside that chain makes the schema a
   thing no single migration owns; added *through* it would need a fork
   of the package.
2. **It would break core's own readiness answer.** `Funduq.health`
   compares the database's revision against the one this build expects
   (`schema_current`). A chain carrying gateway revisions would move
   past what core expects, and core would report a database it is
   perfectly able to serve as not ready.
3. **The DDL/DML split is already load-bearing here.** `souk-migrate`
   exists so DDL runs with credentials the server itself never holds
   (README). Serving tables mean a *second* migrate step with the same
   split, not a merged one.

**Follow upstream's mechanism rather than inventing one** (see
`funduq.db_schema` and the wheel's `alembic/env.py`): a schema namespace
read from the environment (`FUNDUQ_DB_SCHEMA`), quoted in exactly one
place, ignored on SQLite (which has no schema namespace at all), with a
dependency-free module holding the constants so both the app and its
`env.py` can import them without dragging in required settings. The
serving version is the same shape under its own names — a
`SOUK_SERVER_DB_SCHEMA`, an `alembic/` in this repo, its own
expected-revision check.

Whether the two live in one database under separate schemas, or in two
databases entirely, is a deployment choice and both must keep working —
which sets the real test, and it is not the schema name:

> **No code path may put core state and serving state in one
> transaction.**

A shared session or a single `begin()` spanning both makes them one
database in practice however they are namespaced, and forecloses the
split deployment silently. Serving persistence therefore gets its own
engine and sessionmaker, never core's session.

## Where examples live

Split by what an example teaches, not by where it happens to run — and
since upstream became a set of PyPI packages, everything network-shaped
lives here, full stop:

| example | teaches | lives | why |
|---|---|---|---|
| `agent-template`, `providers/*` | writing a provider against **souk-agent-sdk** | **this repo** | the SDK they consume is this repo's; upstream ships libraries, not examples of serving them |
| the `demo` compose profile (gateway + docent + three stalls, one command) | what the whole system looks like running | **this repo**, `docker-compose.yml` | only this repo has a gateway to demo against |
| `providers/pod-probe-agent` (Go, no SDK) | the frame protocol in this document, directly | **this repo** | the frames are authored here, so their conformance probe belongs here — it replays `docs/wire-vectors.json` and the vendored upstream vectors, and it is the living proof that the wire is implementable from the documents alone |
| managed-gateway embedding (`create_app` wrapped in edge auth + an admin router over the `Funduq` facade) | how a deployment adds management without this repo shipping policy | **this repo**, `examples/` (unbuilt) | the embedding surface (`create_app`, `app.state.souk`) is this repo's contract |

## What moved out, and where upstream went

The gRPC removal list (listener, stubs, `grpcio`/`protobuf`, port 50051,
the stub-gen Docker layer) is history now — see the repo log if the
details matter.

The larger move since: upstream stopped being a submodule at all.
**funduq core arrives as PyPI packages** (`funduq`,
`funduq-provider-sdk[llm]`, `funduq-contract`), the `AgentSouk/`
directory is gone, and the boundary is unchanged in spirit — upstream is
the domain and the contract, published as wheels; this repo is every
socket, both SDKs (`souk-agent-sdk/`, `souk-client-sdk/`), the reference
providers and the directory UI, because a wire client is network code
and upstream keeps none: this repo owns both ends of every wire it
defines. What used to be "pinned by commit" is now pinned by version
bounds in each `pyproject.toml`, and what used to be a path into the
submodule (vectors, scripts, docs) now has an in-repo home:
`docs/upstream-contract-vectors.json` (vendored at contract revision 16,
regenerated by re-vendoring when the pin moves) and
`scripts/gen_dev_tls_cert.py`.

**Upstream's own documents thinned out with the machines.**
`docs/writing-a-transport.md` and `docs/link-protocol-machine.md` were
deleted at revision 11 — the second described the machine that was
withdrawn, and the first described a wire upstream no longer publishes.
What is left to read there is `docs/provider-link.md` (the settled link
design), `docs/integration-contract.md`, `docs/operational-limits.md` and
`docs/contract-changelog.md`, which is the file to open first when a pin
moves: it says what an implementation has to change, not merely what
was committed. Everything transport-shaped that those two pages used to
carry is in this document now, which is the honest arrangement — this
repo writes the frames.
