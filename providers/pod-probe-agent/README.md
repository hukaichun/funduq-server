# pod-probe-agent

A single static binary that comes alive **inside a pod**, dials out to a
souk, and answers read-only questions about that pod's state — file build
and modify times, directory listings, bounded file reads, process and
environment facts. Drop one into each pod and they form a network of live
probes you can ask from outside, through the same souk everything else here
uses.

It speaks the `/ws/provider` frame protocol from
[`docs/server-mode.md`](../../docs/server-mode.md) **directly** — no SDK, no
souk core, no Python. That is the point of it being here: it is the third
reference provider and the second independent implementation of the wire, so
anywhere the spec is loose or wrong, this binary is what surfaces it.

The connection is wire v4's three steps: fetch a single-use ticket and
souk's public key out of band (`POST /tickets` — the pin is checked here,
before anything is signed), open the socket with a two-frame handshake
(`hello` carrying a proof that *names* that key, `welcome` carrying souk's
verified counter-signature), then publish the roster on the open link with
a `register` frame and serve runs once souk echoes `registered`. Every
reconnect is the full ceremony again — the roster lives on the link.

## Why this exists

The motivating story: a colleague debugging a service in production goes
*into* the pod — `kubectl exec` — and, because it is fast, edits source right
there in the running container. It works, and it leaves a pod whose code no
longer matches its image, invisibly, until something else breaks on top of
it.

The fix is not to police the exec. It is to make going in **unnecessary**:
put an agent in the pod that answers, from outside, the questions he used to
go in to see. "What's the newest file under `/app`?" "Was anything modified
after the pod started?" "What processes are running?" — all answerable
without a human shell in the container.

And to make sure this agent can never *become* the problem it was built to
retire, it has **no write path at all**. Every operation reads. There is no
`exec`, no shell, no file write, no counterpart that mutates anything. A pod
compromised through this agent leaks read-only facts about one pod; it does
not become a foothold to change the pod, because the binary has no verb that
changes anything.

## The read-only invariant, and where it lives

- **The probe tools** ([`probe.go`](probe.go)) are pure `stdlib` + `/proc`:
  `stat`, directory listing, a **bounded** file read (64 KiB cap), a walk for
  the most-recently-modified files, a `/proc` process list, and an
  environment summary with secret-shaped values redacted. None writes.
- **The LLM is kept out of the control loop** ([`brain.go`](brain.go)). The
  agent gathers a fixed set of read-only facts *first*, then — only if a
  model is reachable — asks it to *interpret* those facts. The model never
  chooses what to read or walk, so a misled or prompt-injected model still
  cannot make the agent touch anything the deterministic pass did not already
  gather. Handing the model the tools directly would trade this property
  away; that is a deliberate non-goal.
- **The image is `scratch`** ([`Dockerfile`](Dockerfile)): no shell, no
  package manager, no second binary — the same minimal posture the agent
  inspects *for*, and nothing in it can be used to change the pod.

## Security shape

- **Outbound only.** souk never connects in; the agent dials out. The pod
  needs egress to the souk and no inbound port, no Service, no ingress rule.
- **No model key in the pod.** With a KYOK-opted-in run, LLM calls are routed
  through souk's `/kyok/v1` relay and paid for with the *caller's* key, each
  call signed afresh with this agent's identity. The binary holds no model
  credential.
- **Pinned souk.** `SOUK_PUBLIC_KEY` pins the souk's identity, checked
  against the key `POST /tickets` presents *before anything is signed* — a
  proof for a substitute souk is never even computed. Pinned or not, the
  proof names the key the ticket answer presented, and the welcome's
  counter-signature is verified under that same key before the link is
  treated as open.
- **Ephemeral identity by default.** With no volume, a fresh Ed25519 identity
  is minted each start — a restarted pod is a new stall, and its old roster
  row ages out. Mount a volume at `/data` to keep one stable.
- **Who may ask is the deployment's job.** An agent standing in a production
  pod means whoever can reach souk can run read-only probes in your pods. Put
  edge auth in front of souk (the managed-gateway example's shape); this
  binary does not ship a policy.

This agent is a call-home loop, which is the shape of a remote-access trojan
drawn without the labels — and one edit (give the model the tools, let them
write) is what separates this read-only probe from an implant. Why that edit
is the boundary, why the read-only design here is the defensive half of a
known dual-use pattern rather than caution, and what the gateway must enforce
so a different provider binary cannot quietly discard it: see
[`docs/threat-model.md`](../../docs/threat-model.md).

## Configuration

All via environment variables:

| var | default | meaning |
|---|---|---|
| `SOUK_HTTP_URL` | `http://souk:8000` | the souk to register and connect to |
| `SOUK_PUBLIC_KEY` | *(unset)* | pinned souk identity (hex); unset = connect to whatever answers, logged |
| `SOUK_IDENTITY_KEY_PATH` | `/data/probe_identity.key` | where the Ed25519 seed persists |
| `PROBE_AGENT_NAME` | `probe-<hostname>` | roster name; the pod name in a cluster |
| `PROBE_PROVIDER_NAME` | `pod probe` | storefront label |
| `PROBE_ROOT` | `/app` | subtree the "what changed" walk covers |
| `PROBE_MAX_CONCURRENT_RUNS` | `1` | capacity declared at hello |
| `LLM_BASE_URL` / `LLM_API_KEY` / `LLM_MODEL_NAME` | *(unset)* | own model endpoint, used when a run carries no KYOK token |
| `SOUK_KYOK_URL` | `<SOUK_HTTP_URL>/kyok/v1/chat/completions` | where KYOK-routed calls go |

## Run it

In this repo's compose stack, under the `probe` profile:

```bash
docker compose up --build souk pod-probe
```

(`souk` pulls in `paradedb` and `souk-migrate`.) The probe registers as
`pod-probe-demo`, inspecting `./providers` mounted read-only at `/app`. Find
it and ask it a question:

```bash
curl -s http://localhost:8000/agents | python3 -m json.tool
```

Then drive a run over A2A or AG-UI against `(provider_key, pod-probe-demo)` —
with no LLM configured it returns the raw read-only report, which is the mode
the wire is verified in.

## Tests

```bash
go test ./...
```

`wire_test.go` replays [`docs/wire-vectors.json`](../../docs/wire-vectors.json)
(handshake version and frame vocabulary) and
[`docs/upstream-contract-vectors.json`](../../docs/upstream-contract-vectors.json)
(upstream funduq's payload vectors, vendored at contract revision 7): it
asserts the connect proof, the welcome verification and the KYOK call
payload are **byte-identical** to what the gateway and funduq core verify
against, and that the deterministic Ed25519 signatures match the published
ones. A missing vector file **fails** the suite rather than skipping — a
checkout without the vectors cannot claim the wire is verified. That proves
the crypto core without running the stack; the live ordering behaviour
(register-then-run, reconnect-mid-run) is what the compose run exercises.
