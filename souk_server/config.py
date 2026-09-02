"""Serving configuration: everything that only exists because souk is being
exposed on a network.

Deliberately a separate class in a separate distribution from
`funduq.config.CoreSettings`. Core never reads any of this — it cannot, it
does not depend on this package — which is what makes "core knows a
database and nothing else" a property of the packaging rather than of
everyone's discipline. This serving layer keeps the `SOUK_*` prefix a
deployment already sets.

**Core's environment is now read here too.** `CoreSettings.from_env` was
removed at contract revision 14 and core reads no environment at all:
configuration is an argument, and a deployment that keeps it in the
environment reads it itself. That deployment is this gateway, so
`core_settings_from_env` below is where the `FUNDUQ_*` names live now. The
names did not change, so compose and `.env.example` keep working
unchanged — what changed is which package does the reading, which is the
honest place for it: reading an environment is a serving decision, and it
was always odd that the network-free half of the system owned one.
"""

from __future__ import annotations

import os
from collections.abc import Mapping

from funduq.config import CoreSettings
from pydantic_settings import BaseSettings, SettingsConfigDict

# The `FUNDUQ_*` name for each `CoreSettings` field this gateway reads,
# paired with the parser that turns a string into the field's type. Written
# out rather than derived from the model's field names: the environment is
# a published surface (docs/server-mode.md, .env.example, compose), so a
# field renamed upstream must be a visible edit here, not a silently
# renamed variable in everybody's deployment.
_CORE_ENV: tuple[tuple[str, str, type], ...] = (
    ("database_url", "FUNDUQ_DATABASE_URL", str),
    ("db_schema", "FUNDUQ_DB_SCHEMA", str),
    ("token_signing_secret", "FUNDUQ_TOKEN_SIGNING_SECRET", str),
    ("identity_private_key", "FUNDUQ_IDENTITY_PRIVATE_KEY", str),
    ("stale_hidden_window_seconds", "FUNDUQ_STALE_HIDDEN_WINDOW_SECONDS", int),
    ("thread_queue_limit", "FUNDUQ_THREAD_QUEUE_LIMIT", int),
    ("provider_quality_tolerance", "FUNDUQ_PROVIDER_QUALITY_TOLERANCE", int),
    # The broker's three waits became settings fields at revision 14 (the
    # last live item of the #181 adopter review this repo filed). Optional
    # everywhere: an operator who never sets them gets core's own defaults,
    # which are the same single definitions `RunBroker` builds itself from.
    ("unserved_timeout_seconds", "FUNDUQ_UNSERVED_TIMEOUT_SECONDS", float),
    ("deliver_timeout_seconds", "FUNDUQ_DELIVER_TIMEOUT_SECONDS", float),
    ("undelivered_window_seconds", "FUNDUQ_UNDELIVERED_WINDOW_SECONDS", float),
)


def core_settings_from_env(environ: Mapping[str, str] | None = None) -> CoreSettings:
    """`CoreSettings` from the `FUNDUQ_*` environment this gateway documents.

    Only names actually present are passed, so every field keeps core's own
    default and there is no second copy of any default here. **An empty
    string is unset**, not an empty value: a compose file with
    `FUNDUQ_DB_SCHEMA=` in it means "I did not set this", and passing `""`
    through would hand core a schema named the empty string. That is the
    one interpretation this function makes, and it is the one an operator
    means.

    `token_signing_secret` and `identity_private_key` are required by
    `CoreSettings` and are not defaulted here; their absence surfaces as a
    pydantic ValidationError naming them, at startup, which is where a
    missing identity should be found.
    """
    environ = os.environ if environ is None else environ
    values: dict[str, object] = {}
    for field, name, parse in _CORE_ENV:
        raw = environ.get(name, "")
        if raw == "":
            continue
        try:
            values[field] = parse(raw)
        except ValueError as e:
            raise ValueError(f"{name} is not a valid {parse.__name__}: {raw!r}") from e
    return CoreSettings(**values)


class ServingSettings(BaseSettings):
    """Everything that only exists because souk is being exposed on a
    network. Consumed by whoever actually binds the sockets —
    souk_server.server. Core never reads any of this, and since the split it
    could not: it does not depend on this package.
    """

    model_config = SettingsConfigDict(env_prefix="SOUK_")

    http_host: str = "0.0.0.0"
    http_port: int = 8000

    # Origins allowed to call souk's HTTP surface cross-origin (e.g. a
    # souk-directory instance served from a different origin). "*" is fine
    # for local development; tighten this for any real deployment, same as
    # token_signing_secret's default is only safe for a single-developer
    # local souk.
    cors_allow_origins: list[str] = ["*"]

    # Host header values the MCP docent (/mcp) will answer to. The MCP
    # SDK enables DNS-rebinding protection by default and allows only
    # localhost, which is right for a personal MCP server on a laptop and
    # wrong for a gateway: the first thing that happened when the docent
    # was moved into compose and reached souk by its service name was a
    # 421 Misdirected Request, with everything else on the same listener
    # working.
    #
    # Defaulting to "*" is a judgement about what is behind this door,
    # not a shrug: /mcp is read-only and serves exactly the roster
    # `GET /agents` already serves unauthenticated, so a rebinding attack
    # that reached it would learn nothing it could not fetch directly.
    # Pin it to real hostnames anyway in a deployment where that stops
    # being true.
    mcp_allowed_hosts: list[str] = ["*"]

    # Base URL callers use to reach this souk's HTTP surface, used to build
    # per-agent Agent Card URLs. Override in deployments behind a proxy/LB.
    # Deliberately not a core setting even though it ends up in protocol
    # content: core should not know what it is called on a network, so
    # whoever serves souk passes this to the protocol layer.
    public_http_url: str = "http://localhost:8000"

    # TLS for the one listener — every surface (/agents/register, /agui/*,
    # /a2a/*, /kyok/*, /ws/provider, /ws/kyok) rides it, wss included (a
    # plain HTTP/1.1 upgrade). Both left unset means plaintext — fine for
    # same-host development, never for a souk reachable over a real
    # network: without TLS, session tokens and signed requests are visible
    # to anyone on the path, and a captured registration signature is only
    # bounded by souk.identity.SIGNATURE_FRESHNESS_WINDOW_SECONDS (60s),
    # not prevented outright. See scripts/gen_dev_tls_cert.py for a
    # self-signed pair to test with; use a real CA-issued cert (or
    # terminate TLS at a reverse proxy in front of souk) for anything
    # else.
    http_tls_cert_path: str | None = None
    http_tls_key_path: str | None = None
