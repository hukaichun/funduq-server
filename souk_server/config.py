"""Serving configuration: everything that only exists because souk is being
exposed on a network.

Deliberately a separate class in a separate distribution from
`funduq.config.CoreSettings`. Core never reads any of this — it cannot, it
does not depend on this package — which is what makes "core knows a
database and nothing else" a property of the packaging rather than of
everyone's discipline. The prefixes diverged with the split: core reads
`FUNDUQ_*` (and only when asked to, via `CoreSettings.from_env()`), while
this serving layer keeps the `SOUK_*` prefix a deployment already sets.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


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
