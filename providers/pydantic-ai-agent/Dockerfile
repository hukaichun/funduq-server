# Build context is the repo root (only so souk-agent-sdk/ is reachable —
# pydantic-ai-agent is an independent project, not a workspace member).
# This image only ever copies souk-agent-sdk/ and
# providers/pydantic-ai-agent/ — nothing from souk_server/,
# souk-client-sdk/, or agent-template/ ends up in it.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
# souk-agent-sdk is transport only: what a provider *is* — its identity,
# the port an agent satisfies, the loop that runs the work — comes from
# funduq-provider-sdk, which uv installs from PyPI.
COPY souk-agent-sdk /app/souk-agent-sdk
COPY providers/pydantic-ai-agent /app/providers/pydantic-ai-agent

WORKDIR /app/souk-agent-sdk
RUN uv sync

WORKDIR /app/providers/pydantic-ai-agent
RUN uv sync

ENV AGENT_TEMPLATE_CONFIG=/app/config.yaml

CMD ["uv", "run", "pydantic-ai-agent"]
