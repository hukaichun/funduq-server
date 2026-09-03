# Build context is this repo's root. funduq core, the provider SDK and
# the contract package arrive from PyPI via uv.lock — there is no
# submodule any more. The remaining COPY rule still holds for in-repo
# path sources: every [tool.uv.sources] entry needs a COPY below, and a
# missing one fails at `uv sync` with "Distribution not found at", not
# at import time.
FROM python:3.12-slim

RUN pip install --no-cache-dir uv

WORKDIR /app
COPY pyproject.toml uv.lock .python-version /app/
# Dev-group only, and only for `tests/test_kyok_shipped_signer.py` — but
# `uv sync --group dev` below resolves the whole group, so a path source
# without a COPY fails the *build*, not a test.
COPY souk-agent-sdk /app/souk-agent-sdk
COPY souk_server /app/souk_server

RUN uv sync --group dev

EXPOSE 8000

CMD ["uv", "run", "souk-server"]
