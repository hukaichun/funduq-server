# Authorization: three layers, and where each one can live

Status: **an inventory and what follows from it.** One of the three
layers below is built, one is half-built by accident, and one has no
expression anywhere in this repo. Nothing here proposes code; it names
the seams so that the next round changing any of them can say which one
it is changing. `docs/server-mode.md` remains the spec of record for the
wire; this is the spec of record for who may use it.

Written from the doors outward rather than from the code: the question
"what should this gateway expose, and who decides" has an answer that
does not depend on how `souk_server` happens to be arranged today.

## The principle the rest of this follows from

**A decision can only live where the evidence that settles it lives.**
Not "should" — *can*. Put an authorization check somewhere that cannot
see the facts it needs and one of two things happens: it asks for the
facts to be handed to it, which moves the trust boundary without saying
so, or it approximates, which is a second rule that will eventually
disagree with the first.

Contract revision 22 is upstream applying exactly this test and moving a
decision because of it ([funduq#275](https://github.com/hukaichun/funduq/issues/275)):
core can verify a chain and nobody else can, so *who the parties are*
stays there; core cannot know what a deployment's policy is, so *what
follows from being a party* left. The same test, applied to every door,
gives three layers rather than two.

## The three layers

**A — the record's own facts.** Does this chain verify? Is the presenter
the chain's last hop? Is this key the thread's head, or the provider
serving its agent? Does this signature cover exactly these ask ids? The
verification code and the rows it is checked against are both in core,
and putting a copy anywhere else means two answers to one question.
**Not delegable, in either direction.**

**B — who is at the door.** A TLS session, a client certificate, an SSO
cookie, a signed header. Only the process holding the connection can see
any of it, which is this gateway. The output is one value — a key, or
nothing — and its whole job is to be evidence the other two layers can
use. **Necessarily the transport's.**

**C — whether this identity is entitled here.** Tenancy, group
membership, plan, spend ceiling, "may this key trade in my market at
all", "may this caller invoke this agent". Core does not know. The
gateway does not know either — it knows *who*, not *whether*. Only the
deployment knows, and in a deployment of any size a different team owns
it. **Necessarily the operator's, which means this gateway's job is to
leave it a place to stand, not to answer it.**

The layers compose in one direction: B produces the identity, A produces
the facts about the record, C decides. A door that skips C has not
avoided a policy — it has hard-coded one.

## Who stands at a door

| principal | what it carries | which layer can see it |
|---|---|---|
| anonymous visitor | nothing | — |
| caller with a key, no chain | a key, able to sign over a request body | B |
| caller presenting a chain | an ordered, hash-linked, per-hop-signed delegation path | B sees the presenter; A verifies the path |
| provider | a key proved once at the link handshake, plus the agent names it publishes | A (the ticket and the counter-signed proof) |
| KYOK bridge | the same, plus a model offering | A |
| funduq itself | its Ed25519 identity; it signs a dispatch hop onto every chain it relays | A |
| the deployment's owner | tenancy, entitlement, spend — facts held in a system neither core nor this gateway can read | **C only** |

The last row is the one the current design has no vocabulary for.

## The doors, inventoried

Everything this gateway serves today, with the question each door is
*actually* asking underneath whatever it is spelled as.

### Public by construction

| door | the real question | layer |
|---|---|---|
| `GET /healthz`, `GET /readyz` | is this process alive | none |
| `GET /agents`, `GET /llm-providers` | — | none; a market's roster is its point |
| `GET /a2a/{provider}/{name}/.well-known/agent-card` | — | none |
| `/mcp` (four tools, three resources) | — | none; every one is derived from the roster and the docent serves nothing thread-shaped |

Nothing here needs a rule. Worth stating anyway, because "public by
construction" is a claim that has to survive the next tool somebody adds
to the docent — the invariant that keeps it true is that the docent
gives directions and stops.

### Admission

| door | the real question | layer |
|---|---|---|
| `POST /tickets` | **may this key serve in my market** | **C** |

One row, and it is the most consequential in this document. A ticket is
what admits a key to open either socket; a key that gets through here can
publish agents, be offered runs, and read the threads of the agents it
serves. The question is entirely about entitlement, which makes it
layer C by nature — and it is unauthenticated, deliberately, because an
open market is a coherent answer. What is missing is not a check. It is
that a deployment wanting a *different* answer has nowhere to put it.

### Links

| door | the real question | layer |
|---|---|---|
| `WS /ws/provider` | is this connection the key that ticket admitted | A (ticket, mutual proof) |
| `WS /ws/kyok` | the same | A |
| `query` frame on `/ws/provider` | may this provider read this thread | A + a rule (below) |

The handshake is settled and needs nothing: a single-use ticket, a proof
naming the funduq it is for, a counter-signature in the welcome. The
`query` frame is the odd one — see the reads section.

### Invocation

| door | the real question | layer |
|---|---|---|
| `POST /threads/{provider}/{name}` | does this agent exist | A only |
| `POST /agui/{provider}/{name}` | does the chain verify · is the presenter its last hop · on a bound thread, is this caller a member | A (all three) |
| `POST /a2a/{provider}/{name}/rpc` — sends | the same | A |
| `POST /a2a/…/rpc` — `CancelTask` | is this signed by the run's head or its serving provider | A |
| resolve (a send carrying a resolution) | is this signed by one of those two, over exactly the asks still open | A |
| `POST /kyok/v1/chat/completions` | is this grant this run's | A |

Every row is layer A, every row is answered, and none of them asks the
layer C question sitting behind all of them: **may this caller invoke
this agent at all.** Today the answer is "anyone who can reach the door",
which — like the open market above — is a defensible answer that nobody
chose.

Note `POST /threads/{provider}/{name}` checks only that the agent is
registered. A thread costs nothing and reveals nothing, so this is not a
gap on its own; it is a gap the moment C exists anywhere, because it is
the cheapest door into the same resource.

### Observation

| door | the real question | layer |
|---|---|---|
| `GetTask`, `ListTasks`, `SubscribeToTask` | may this key see this thread | **A + C, and the two are separable** |
| `GET /threads/{thread_id}` | the same | the same |
| `GET /threads/{thread_id}/tree` | the same | the same |
| `query` on `/ws/provider` | the same, plus: is this the provider serving it | the same |

This is the row where the split is visible, because upstream made it
visible. "Is this key a party to this thread" is layer A —
`Funduq.parties_of` is the answer and only core can derive it. "Parties
may read" is layer C — a policy choice, and a defensible one, but a
choice. Revision 22 handed over the second half and kept the first, which
is the correct cut.

What this repo did with the handover is keep revision 21's circle
unchanged, in `souk_server/reads.py`. That is the right *default* and it
is written as a constant: correct, and with no place for a deployment to
stand. The three A2A operations and the provider socket call it; the two
AG-UI thread reads do not call it at all, which `docs/server-mode.md`
records as an open decision.

### Administration

No doors. This is a correct omission and not a gap: `issue_ticket` **is**
the admission decision and `delete_agent` is an administrative act, so a
blanket projection of core's methods onto endpoints would publish both to
whoever can reach the door. When an admin surface is built it is a
separate door with a separate answer to layer C, not a widening of these.

## What the inventory shows

**One.** The API set is right. Nothing in it should be removed, and the
only thing missing is a surface nobody has asked for yet
(administration), which should arrive as its own door when it arrives.
This document is therefore not a proposal to change what is served.

**Two.** The gap is not a missing rule. It is that **layer C has no
expression anywhere in this repo.** Four doors are asking a layer C
question underneath: admission, the two invocation doors, and every
observation door. Of those, admission asks nothing, invocation asks
nothing, and observation has one hard-coded answer.

**Three.** **Admission outranks observation.** The worst outcome behind
the read doors is that somebody sees a conversation they should not. The
worst outcome behind `POST /tickets` is that somebody trades in your
market: publishes agents under a name your callers trust, receives runs,
and reads the threads of the agents it now serves. Both are worth fixing;
they are not the same size, and the read doors got attention first
because upstream moved and this one did not.

**Four.** The read rule is a default without a socket. Splitting it the
way the layers split it — `parties_of` for the fact, a replaceable
decision for the policy — costs little today, while there are four
callers and one shape, and costs progressively more with every door
added. This is the one place where the shape of the seam matters more
than the rule it currently holds.

**Five.** Layer B is the only layer that is finished. One mechanism,
`Funduq-Presenter`, signed over the exact request bytes, 60-second
window, and a caller with no chain is unaffected. It produces exactly
what the other layers need and nothing more. It is worth naming as the
model the other two seams should be judged against.

## The actor chain: evidence, not a decision

The chain is layer A evidence, and richer than any current consumer of
it. `verify_chain` returns hops **in order**, each carrying
`actorPublicKey`, `prevHash`, and optionally `dispatchedTo
{providerKey, name}` — and funduq appends a dispatch hop of its own to
every chain it relays. A run's chain therefore records the path the work
took through the market, agent by agent, signed at each step.

Two things follow, and they point in different directions.

**As evidence for policy, its shape is unused.** Every consumer today
flattens it: `readers_of` computes `set(actor_public_keys)`, discarding
order. Core keeps the distinction on the act side — only the head may
cancel — and the read side does not. A delegate three hops down has
exactly the head's read rights. Whether that is wrong is a layer C
question this document deliberately does not answer; what it does say is
that the material to answer it differently already exists and is being
thrown away at the point of use.

**As a feature, its `dispatchedTo` claims are the better answer to a
question already being asked worse.** `GET /threads/{thread_id}/tree`
reconstructs what a request fanned out to from A2A `referenceTaskIds` —
caller-recorded, and complete only as far as callers chose to make it.
The dispatch hops on the chain are the same graph, recorded by funduq
rather than by the caller. That is a read-side improvement with no
authorization content at all, and it should not be bundled with any of
the above.

**What the chain must not become.** A hop that carries an entitlement —
"the bearer may read this thread", "this delegate inherits my rights" —
is a credential that manufactures authority, which is the shape upstream
removed at revision 15 and refused again in
[funduq#240](https://github.com/hukaichun/funduq/issues/240). The chain
records that something happened and who answers for it. Layer C decides
what that is worth. Keeping those two apart is what lets the chain stay
verifiable forever while policy changes underneath it.

## Where this leaves the next round

In the order the inventory argues for, not the order of difficulty:

1. **Give layer C somewhere to stand.** One seam, taking who, what
   resource, and which operation — enough that admission, invocation and
   observation can all be expressed through it, with today's behaviour as
   the default implementation. Nothing about who may do what changes.
2. **Admission first among the three.** `POST /tickets` is the door
   where "open market" should become a stated default rather than an
   absence.
3. **Close the two observation doors that call no rule at all.**
   `GET /threads/{thread_id}` and `/tree`. This changes the behaviour of
   an endpoint that is public today, which is why it is a decision and
   not a fix.
4. **Feed `/tree` from dispatch hops.** Independent of all of the above,
   and purely additive.

Deliberately not on this list: changing who may read. Whether read rights
should attenuate along the chain, and whether they should be scoped per
run rather than per thread, are real questions — and they are layer C
questions, which means the answer is a policy, which means the seam has
to exist before the question can be asked properly.
