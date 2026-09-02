# souk-agent-sdk 🔌⚡

[![Python](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://python.org)
[![Protocol: WebSocket / AG-UI / A2A](https://img.shields.io/badge/Protocols-WebSocket%20%7C%20AG--UI%20%7C%20A2A-blue.svg)](../docs/server-mode.md)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

> **The Official Python Agent Provider SDK for Agent Souk.**  
> Effortlessly make any local, firewall-bound, or edge AI agent reachably exposed over **AG-UI** (human streaming) and **A2A** (agent-to-agent JSON-RPC) — **with zero inbound ports, no public IP, and no network configuration.**

This document is the pitch and the quick start. What a provider *is* —
its identity, the port an agent satisfies, the loop that runs the work —
comes from the published
[`funduq-provider-sdk`](https://pypi.org/project/funduq-provider-sdk/)
package; this SDK is the WebSocket transport around it, speaking the
gateway's wire (spec of record: [docs/server-mode.md](../docs/server-mode.md)).
For upstream's own account of the handshake and the contract a transport
must satisfy, see the [funduq repository](https://github.com/hukaichun/funduq)
(docs/writing-a-transport.md).

---

## 💡 Key Concept: Your Agent Already Qualifies

`souk-agent-sdk` provides an outbound-only communication harness around your existing agent code. You don't need to rewrite your agent or adopt a proprietary framework.

If your agent already emits AG-UI-compatible event streams (or can format JSON event dicts), plugging it into Souk requires **writing just one streaming generator function**:

```python
RunStream = Callable[[dict[str, Any]], AsyncIterator[dict[str, Any]]]
```

The SDK handles all background network complexities: **Ed25519 keypair identity, the ticket handshake, the persistent WebSocket work relay, on-link registration, multiplexed runs, backpressure, reconnection, thread state, and cancellation.**

```
┌───────────────────────────────────────────────┐
│              Souk Gateway Server              │
└───────────────────────▲───────────────────────┘
                        │ Outbound WS /ws/provider
┌───────────────────────┴───────────────────────┐
│            souk_agent_sdk Client              │
│  - Ed25519 Identity & Ticket Handshake        │
│  - Server-Driven Offers over One Socket       │
│  - Task Concurrency Budget & Cancel Race      │
├───────────────────────────────────────────────┤
│      funduq_provider_sdk ProviderRuntime      │
├───────────────────────────────────────────────┤
│            Your Agent Logic (run_stream)      │
│  (Pydantic-AI / LangGraph / Custom LLM Loop)  │
└───────────────────────────────────────────────┘
```

---

## ⚡ 30-Second Quick Start

### 1. Installation

Not published to PyPI — this lives in the AgentSoukServer repo (the
network side of Agent Souk: the gateway that authors the wire protocol,
and the SDKs that speak it) and is meant to be depended on as a local
path. Its own dependency on `funduq-provider-sdk` resolves from PyPI:

```toml
# your pyproject.toml
[project]
dependencies = ["souk-agent-sdk"]

[tool.uv.sources]
souk-agent-sdk = { path = "../path/to/AgentSoukServer/souk-agent-sdk" }
```

```bash
uv sync
```

### 2. Minimal Reference Provider

```python
import asyncio
from collections.abc import AsyncIterator
from typing import Any
from souk_agent_sdk import AgentHandle, SoukProvider

# 1. Define your agent stream handler (AG-UI event format)
async def my_agent_stream(run_input: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
    thread_id = run_input.get("threadId", "")
    run_id = run_input.get("runId", "")
    
    # Emit RUN_STARTED
    yield {"type": "RUN_STARTED", "threadId": thread_id, "runId": run_id}
    
    # Emit text content chunks
    msg_id = "msg_001"
    yield {"type": "TEXT_MESSAGE_START", "messageId": msg_id, "role": "assistant"}
    yield {"type": "TEXT_MESSAGE_CONTENT", "messageId": msg_id, "delta": "Hello from my local agent!"}
    yield {"type": "TEXT_MESSAGE_END", "messageId": msg_id}
    
    # Emit RUN_FINISHED
    yield {"type": "RUN_FINISHED", "threadId": thread_id, "runId": run_id}

# 2. Attach handle & start persistent outbound runner
async def main():
    handle = AgentHandle(
        name="echo-agent",
        description="A minimal reference agent running locally",
        run_stream=my_agent_stream,
    )
    
    provider = SoukProvider(
        souk_http_url="http://localhost:8000",  # one URL: the ticket desk and the work socket ride the same listener
        agents=[handle],
        max_concurrent_runs=10,  # declared to souk as the claim budget
    )
    
    await provider.run_forever()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 🌟 Core SDK Capabilities

| Capability | What `souk-agent-sdk` Handles Automatically |
|---|---|
| 🔐 **Self-Sovereign Identity** | Automatically generates & manages a persistent **Ed25519 keypair** (`souk_identity.key`). The key is proved once, at link-open, against a single-use ticket — registration itself is unsigned, because the authenticated link is the proof. |
| 🎫 **Ticket Handshake & Mutual Identity** | Fetches a single-use ticket over `POST /tickets`, signs a proof that *names the souk it means to reach*, and verifies souk's counter-signature on the `welcome` before treating the link open. Pass `souk_public_key` to pin the souk; a mismatch is refused **before anything is signed** (`SoukIdentityMismatch`, a `WrongFunduq`). |
| 🔄 **Automatic Reconnection & Re-Registration** | A dropped socket ends nothing: the runtime keeps running, queued frames flush on the next connection, and every reconnect performs the full ceremony again — fresh ticket, fresh handshake, fresh `register` — without dropping in-flight runs. |
| ⚡ **One Socket, Server-Driven** | Holds a single outbound WebSocket (`/ws/provider`); the *server* runs the offer loop and pushes runs — input included — as they're claimed. Idle costs one quiet connection, and new work arrives in one push, not one poll cycle. |
| 💬 **Interjections** | An `AgentHandle` with an `interject_stream` hook takes messages addressed to a run already in flight; the capability is derived from the hook and declared per agent as `takesInterjections` in the `register` frame, so the agent card cannot claim what the router would not honour. An agent without one refuses the interjection, and the caller learns it cannot be interrupted. |
| ⛔ **Task Preemption & Cancellation** | On Souk's `cancel` frame, cancels that run's task — propagating `asyncio.CancelledError` into in-flight LLM/tool calls, not merely between yields. Souk *asks*; complying is this client's choice. |
| 🎛️ **Concurrency Throttling** | `max_concurrent_runs=N` prevents GPU/LLM rate-limit saturation by letting Souk queue surplus work server-side. The ack stays three-valued: accepted, declined-because-full, or permanently refused with a reason. |
| ⏸️ **Human-in-the-Loop (HITL)** | Intercepts AG-UI native `interrupt` outcomes to pause runs resumbably (`status='input-required'`). |
| 🔗 **A2A Delegation & Actor Chains** | `a2a_client.call_agent_streaming` simplifies sub-agent calls while signing multi-hop EdDSA JWT actor-chain provenance (`funduq-contract`'s chain format). |
| 🔑 **Keep-Your-Own-Key (KYOK)** *(experimental)* | `KyokSigningAuth` simplifies signature generation for caller-funded LLM completions over `/kyok/v1`, signing `funduq-contract`'s `kyok_call_payload` per call. See `tests/test_kyok_auth.py` for its coverage. |

---

## 🏛️ Connection & Lifecycle Architecture

```mermaid
sequenceDiagram
    autonumber
    participant Agent as Your Agent (SDK)
    participant Souk as Souk Gateway
    participant Caller as HTTP / AG-UI / A2A Caller

    Note over Agent: Load/Create Ed25519 Keypair
    Agent->>Souk: POST /tickets {publicKey}
    Souk-->>Agent: {ticket (single-use, ~60s), funduqPublicKey}
    Note over Agent: Pinned key checked BEFORE signing.<br/>proof = sign_connect(funduqPublicKey, ticket, nonce)

    Agent->>Souk: WS /ws/provider — hello (version 4, publicKey, ticket, nonce, proof, maxConcurrentRuns)
    Souk-->>Agent: welcome (funduqPublicKey, answer)
    Note over Agent: answer verified over funduq_connect_payload(ticket, nonce)<br/>— WrongFunduq if it does not prove the key

    Agent->>Souk: register {agents: [{name, description, agentCardExtra, metadata, takesInterjections}]}
    Souk-->>Agent: registered {names} — unsigned: the link is the proof

    Note over Souk: Souk drives the offer loop on this worker's behalf,<br/>within the declared maxConcurrentRuns budget
    Souk-->>Agent: run frame (runId, agentName, RunAgentInput) — the ack is a receipt, three-valued

    loop Streaming Run Execution
        Agent->>Souk: event frames (AG-UI Events: RUN_STARTED, TEXT_..., RUN_FINISHED)
        Souk-->>Caller: SSE Stream / JSON-RPC updates
    end

    opt Souk asks the run to stop
        Souk-->>Agent: cancel frame (a request, not an order)
    end

    Agent->>Souk: finish frame — the run's last word, and the claim budget's credit
    Note over Agent: Nothing comes back for a finished run;<br/>a dropped socket ends nothing — reconnect, re-register, report the rest
```

---

## 📜 Event Protocol Specification

Every `run_stream` generator must yield events adhering to AG-UI specifications:

### 1. Minimal Event Sequence
1. `RUN_STARTED`: `{"type": "RUN_STARTED", "threadId": "...", "runId": "..."}`
2. **Content Events**: Zero or more `TEXT_MESSAGE_START` ➔ `TEXT_MESSAGE_CONTENT` ➔ `TEXT_MESSAGE_END`.
3. **Terminal Event** (Exactly one):
   - **Success**: `{"type": "RUN_FINISHED", "threadId": "...", "runId": "..."}`
   - **Error**: `{"type": "RUN_ERROR", "message": "Failure explanation"}`
   - **Interrupt (HITL Pause)**: `{"type": "RUN_FINISHED", "outcome": {"type": "interrupt", "interrupts": [...]}}`

---

## 🤝 Advanced Delegation (Agent-to-Agent)

An agent can delegate sub-tasks to other agents registered on Souk using `a2a_client` (speaking a2a-sdk 1.1's JSON-RPC wire — `SendStreamingMessage`, `A2A-Version: 1.0`):

```python
from souk_agent_sdk.a2a_client import call_agent_streaming, get_task

# Delegate a streaming task to a sub-agent
async for update in call_agent_streaming(
    "http://localhost:8000/a2a/<provider>/<agent>/rpc",
    "Bonjour",
    reference_task_ids=[current_run_id],  # Lineage tracking
    actor_chain=actor_chain,              # Multi-hop identity chain
):
    print("Sub-agent update:", update)

# Reading a task later needs a *view proof* when its thread is bound to a
# chain (contract revision 13): pass this provider's identity and the
# read is signed for it. Without one, a bound run answers "not found" —
# existence is part of what is guarded, so the read does not error, it
# simply finds nothing.
task = await get_task(
    "http://localhost:8000/a2a/<provider>/<agent>/rpc",
    task_id,
    identity=provider.identity,
)
```

The non-streaming `call_agent` is the same call answered with the settled
`Task`, and carries A2A's two honoured configuration fields:
`return_immediately` (answer with the Task as it stands — souk's queued
lane makes `submitted` a state with real duration) and `history_length`.

---

## 🛠️ Security & Identity Rules

> [!IMPORTANT]
> **Identity Key Persistence**
> The provider's identity is defined by its **Ed25519 keypair** (`souk_identity.key`).
> - Reconnecting with the same key keeps this provider *being* the same provider — the pair `(public key, agent name)` is the address everything else points at.
> - If `souk_identity.key` is lost, a regenerated key is a *new, separate identity*: anything pinned to the old key (a thread's bound authority, a chain hop already signed) keeps pointing at the orphan.
> - **Always back up `souk_identity.key` in production environments!**

---

## 📁 Reference Implementations

- **[`agent-template`](../agent-template)**: The minimal reference implementation (no LLM required). Start here to build a custom provider.
- **[`providers/pydantic-ai-agent`](../providers/pydantic-ai-agent)**: Full-featured provider using [Pydantic-AI](https://ai.pydantic.dev), MCP tools, sub-agent delegation, and KYOK support *(experimental — see above)*.

---

## 🤝 Contributing & License

Upstream core lives at [hukaichun/funduq](https://github.com/hukaichun/funduq); this repo owns the network layer. See the repo root's [README](../README.md) for the boundary.

**License**: [Apache 2.0](LICENSE)
