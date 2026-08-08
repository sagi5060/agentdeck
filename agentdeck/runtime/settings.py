"""Layered runtime settings (OpenAI, Runner, Skills) backed by env + shared YAML.

A single ``config.yaml`` (resolved via ``APP_CONFIG_PATH`` → cwd →
packaged default) hosts every settings group keyed by section: ``openai:``,
``runner:``, ``session:``, ``shell:``, ``skill:``, ``mcp:``. Each :class:`BaseSettings` subclass reads only its section; shell
env vars (prefix-bound, e.g. ``OPENAI_BASE_URL``) override the file. The
project's ``.env`` (found from ``Path.cwd()``, never from this module's own
location) is loaded the first time :func:`get_settings` builds a
:class:`Settings` — not at import — so a ``chdir`` between ``import agentdeck``
and first use still lands on the right project (process env wins either way).
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic_settings.sources import YamlConfigSettingsSource

if TYPE_CHECKING:
    from importlib.resources.abc import Traversable

PACKAGED_DEFAULT_YAML = Path(__file__).resolve().parent / "config.default.yaml"
_CONFIG_PATH_ENV = "APP_CONFIG_PATH"

_SKILL_PREFIX = "skill_"


def resolve_env_file() -> Path:
    """The project's ``.env``: ``Path.cwd() / ".env"`` — no upward directory search
    (unlike ``dotenv.find_dotenv()``, which would just as silently load an unrelated
    ancestor's ``.env`` instead of the project's own).

    Resolved fresh by :func:`get_settings` on every call it actually builds, never at
    import time: cwd is what "my project" means for `agentdeck serve`, an installed
    package, and Compose alike, matching how ``mount_project_dir`` locates
    ``./.agentdeck`` (never module-relative, which lands in site-packages for an
    installed package — issue #16); binding it once at import would instead freeze
    whatever cwd happened to be current the moment ``agentdeck`` was first imported,
    which a caller is free to ``chdir`` away from before ever building `Settings`.
    """
    return Path.cwd() / ".env"


def resolve_config_path(explicit: str | Path | None = None) -> Path:
    """Resolve the shared YAML: explicit arg → ``APP_CONFIG_PATH`` → cwd → packaged default.

    Returning a path that doesn't exist is fine — the YAML source treats a
    missing file as empty, which lets env vars alone drive a fully-defaulted
    config. Resolved from ``Path.cwd()`` on every call, matching
    :func:`resolve_env_file` and how ``App`` locates ``./.agentdeck`` — never
    module-relative (issue #16).
    """
    chosen = explicit or os.environ.get(_CONFIG_PATH_ENV)
    if chosen:
        return Path(str(chosen)).expanduser()
    local = Path.cwd() / "config.yaml"
    return local if local.is_file() else PACKAGED_DEFAULT_YAML


class SectionedYamlSource(YamlConfigSettingsSource):
    """Read a single ``yaml[section]`` mapping so one config.yaml hosts many settings models.

    Permissive on missing files / sections: returns ``{}`` instead of raising,
    so an operator can omit a section entirely and rely on field defaults.
    """

    def __init__(self, settings_cls: type[BaseSettings], section: str | None):
        self._section = section
        super().__init__(settings_cls, yaml_file=resolve_config_path())

    # ``Path | Traversable`` because pydantic-settings widened this parameter and an override
    # may not narrow one (Liskov) — CI, resolving fresh, reads the widened base and rejected the
    # old signature. The dependency is unpinned (`>=2.4`), so both are in the field: the wide
    # annotation is the one that satisfies either base, and the body only needs ``is_file()``,
    # which both types provide.
    def _read_file(self, file_path: Path | Traversable) -> dict[str, Any]:
        if not file_path.is_file():
            return {}
        # ty: ignore[invalid-argument-type] — the same two-version split, seen from the other
        # side: against a `Path`-only base this argument is too wide. It is a `Path` at runtime
        # (the caller is pydantic-settings, resolving our own `yaml_file`), and the widened base
        # accepts both. Drop the ignore once `pydantic-settings` is pinned past the widening.
        data: Any = super()._read_file(file_path) or {}  # ty: ignore[invalid-argument-type]
        if not isinstance(data, Mapping):
            return {}
        if self._section is None:
            return dict(data)
        sub = data.get(self._section, {})
        return dict(sub) if isinstance(sub, Mapping) else {}


def _yaml_section_for_prefix(prefix: str) -> str:
    """Map an env_prefix to its YAML section name.

    ``OPENAI_`` → ``openai``, ``AGENTDECK_RUNNER_`` → ``runner``,
    ``AGENTDECK_SESSION_`` → ``session``, ``AGENTDECK_SHELL_`` → ``shell``,
    ``SKILL_`` → ``skill``.
    """
    name = prefix.strip().rstrip("_").lower()
    if name.startswith("agentdeck_"):
        name = name[len("agentdeck_") :]
    return name


def settings_config(prefix: str, **overrides: Any) -> SettingsConfigDict:
    """Build a ``model_config`` for any :class:`LayeredSettings` subclass.

    Every subclass binds its env prefix and YAML section through this helper;
    pass ``**overrides`` to extend (``protected_namespaces=()``, ``extra="allow"`` …).
    """
    base: dict[str, Any] = {
        "env_prefix": prefix,
        "case_sensitive": False,
        "extra": "ignore",
    }
    return SettingsConfigDict(**(base | overrides))


def _strip_skill_prefix(key: str) -> str:
    key = key.strip()
    if key.casefold().startswith(_SKILL_PREFIX):
        key = key[len(_SKILL_PREFIX) :]
    return key.lower()


def _as_env_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str, separators=(",", ":"))


class LayeredSettings(BaseSettings):
    """``BaseSettings`` with two additions: ``with_overrides`` for CLI flag layering
    and a YAML section source keyed off ``env_prefix`` (so one ``config.yaml`` can
    host every subgroup — ``openai:``, ``runner:``, …).

    Used by both runtime settings (``OpenAISettings`` etc.) and backend settings
    (``PolarionSettings`` etc.). One base class, one resolution algorithm.
    """

    def with_overrides(self, **overrides: Any) -> Self:
        applied = {k: v for k, v in overrides.items() if v is not None}
        return self.model_copy(update=applied) if applied else self

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        prefix = settings_cls.model_config.get("env_prefix", "")
        section = _yaml_section_for_prefix(prefix) if prefix else None
        return (init_settings, env_settings, SectionedYamlSource(settings_cls, section))


class OpenAISettings(LayeredSettings):
    """OpenAI-compatible endpoint configuration.

    Empty ``base_url`` means the SDK default (api.openai.com); point it at any
    OpenAI-compatible server (vLLM, Ollama, a corporate gateway) to override.
    """

    model_config = settings_config("OPENAI_", protected_namespaces=())
    model: str = Field(description="Model name passed to the host Agents SDK runner. No default — always required.")
    api_key: str = Field(
        default="",
        description="API key for the endpoint. What empty does depends on `ca_bundle`: unset (the common "
        "case), the OpenAI client falls through to its own `OPENAI_API_KEY` process-env lookup and errors on "
        "the first model call if that's empty too; with `ca_bundle` set, the empty value is passed straight "
        "through instead and just sends no Authorization header — the self-hosted/corporate-CA case doesn't "
        "need a placeholder value the way the common path does.",
    )
    base_url: str = Field(
        default="", description="OpenAI-compatible endpoint base URL. Empty uses the SDK default, api.openai.com."
    )
    # Path to a CA/cert bundle used to verify the endpoint's TLS cert. Point it at a
    # corporate CA or a self-signed cert to reach an internal OpenAI-compatible server
    # *without* disabling verification. Empty => system default trust store.
    ca_bundle: str = Field(
        default="",
        description="Path to a CA/certificate bundle for verifying the endpoint's TLS certificate. Empty uses "
        "the system's default trust store.",
    )

    def env_dict(self) -> dict[str, str]:
        env = {
            "OPENAI_API_KEY": self.api_key,
            "OPENAI_BASE_URL": self.base_url,
            "OPENAI_MODEL": self.model,
            "OPENAI_CA_BUNDLE": self.ca_bundle,
        }
        # Unset values stay unset in the sandbox — an empty OPENAI_BASE_URL would
        # override the OpenAI client's default endpoint resolution.
        return {k: v for k, v in env.items() if v}


class RunnerSettings(LayeredSettings):
    """Defaults for the host-side Agents SDK runner."""

    model_config = settings_config("AGENTDECK_RUNNER_")

    workflow_name: str = Field(
        default="local-sandbox-repl",
        description="Name recorded on the host Agents SDK run (`RunConfig.workflow_name`) — identifies which "
        "workflow produced a run in tracing/observability.",
    )
    temperature: float = Field(default=1.0, description="Sampling temperature for the host agent loop's model.")
    max_turns: int = Field(
        default=30, description="Maximum turns `Runner.run`/`run_streamed` may take before giving up."
    )
    # Cap on tokens per response for the HOST agent loop (Agents SDK ``ModelSettings``).
    # ``None`` = model default (uncapped).
    max_tokens: int | None = Field(
        default=None,
        description="Cap on tokens per response for the host agent loop's `ModelSettings`. `None` means the "
        "model's own default (uncapped).",
    )


class RuntimeSettings(LayeredSettings):
    """Knobs the Runtime itself reads.

    ``stale_run_after_seconds`` is how long an open run may write nothing before it stops
    holding its session. One session runs one turn at a time, and a run whose process was
    killed outright never records its ending — silence is the only thing that separates it
    from a turn still working, so the session would otherwise stay claimed for good. **One
    hour** by default: generous next to any real turn, short enough that a crash costs a
    session an hour rather than forever, and the trade is deliberate — a permanently wedged
    session is worse than a rare premature takeover. Two consequences worth knowing when
    tuning it: a session a killed process left claimed is refused until it elapses, and a run
    waiting on a human answer for longer than it is closed as failed the next time somebody
    starts a turn on that session.

    **Set it well above the longest stretch a healthy turn can go without writing an event** — a
    slow tool call, a long model call, a human thinking. This is the one setting here that can
    cost you the guarantee rather than tune it: shortened far enough, an open run looks abandoned
    while it is still working, so the next turn takes the session *from a live turn* and both run
    on one conversation. That is not a premature cleanup, it is one turn per session no longer
    holding. The lower bound is a property of the deployment, not of the code — how long a turn
    can be quiet — so it cannot be validated here; positivity is all that is enforced, and at or
    near zero the failure is immediate, since a run's own opening event is already older than the
    cutoff a caller computes a moment later.

    Mind the clock too. Each worker compares *its own* clock against timestamps its peers stamped,
    so across machines the effective window is this value minus the worst skew between them, and a
    worker running more than a window fast takes over live sessions on sight — the same lost
    guarantee, arrived at by skew instead of configuration. Keep the fleet on NTP and treat the
    window as a budget skew eats into.
    """

    model_config = settings_config("AGENTDECK_RUNTIME_")

    stale_run_after_seconds: float = Field(
        default=60.0 * 60.0,
        gt=0,
        description="How long, in seconds, an open run may go without writing an event before it is treated "
        "as abandoned and its session ownership is released for another worker to claim. Must be positive; set "
        "it above the longest gap a healthy turn can go quiet.",
    )

    @property
    def stale_run_after(self) -> timedelta:
        return timedelta(seconds=self.stale_run_after_seconds)


class LangfuseSettings(LayeredSettings):
    """Langfuse LLM-observability export config.

    Namespaced under ``AGENTDECK_LANGFUSE_`` like every other subgroup. The
    Langfuse SDK natively reads the bare ``LANGFUSE_HOST`` / ``LANGFUSE_PUBLIC_KEY``
    / ``LANGFUSE_SECRET_KEY``; we keep config grouped here and pass these in
    explicitly. Tracing stays off unless BOTH keys are present, so a bare
    checkout never ships spans anywhere.
    """

    model_config = settings_config("AGENTDECK_LANGFUSE_")

    public_key: str = Field(
        default="", description="Langfuse public key. Tracing stays off unless this and `secret_key` are both set."
    )
    secret_key: str = Field(
        default="", description="Langfuse secret key. Tracing stays off unless this and `public_key` are both set."
    )
    host: str = Field(
        default="http://localhost:3000",
        description="Legacy Langfuse endpoint (pre-4.x naming). Overridden by `base_url` when that is set.",
    )
    # Langfuse 4.x name for the endpoint; wins over ``host`` (kept as the legacy alias) when set.
    base_url: str = Field(
        default="", description="Langfuse 4.x endpoint. Wins over `host` (kept as the legacy alias) when set."
    )
    environment: str = Field(default="local", description="Langfuse `environment` tag attached to every exported span.")
    debug: bool = Field(default=False, description="Enable the Langfuse SDK's own debug logging.")
    sample_rate: float = Field(default=1.0, description="Fraction of traces exported to Langfuse, from 0.0 to 1.0.")
    # OTel resource ``service.name`` for every exported span (host + sandboxed skills).
    # Without it OpenTelemetry falls back to ``unknown_service``, leaving traces
    # unattributed in the Langfuse UI.
    service_name: str = Field(
        default="agentdeck",
        description="OpenTelemetry resource `service.name` for every exported span (host process and sandboxed "
        "skills). Without it, spans fall back to `unknown_service` and are unattributed in the Langfuse UI.",
    )

    @property
    def enabled(self) -> bool:
        return bool(self.public_key and self.secret_key)

    @property
    def endpoint(self) -> str:
        return self.base_url or self.host


class McpServerSettings(BaseModel):
    """One MCP server entry: transport + how to reach it.

    Mirrors a single value in Claude Code's ``mcpServers`` block. Extra keys
    are tolerated so a Claude-Code-shaped spec drops in unchanged. Only the
    HTTP transport is supported today (see ``agentdeck.adapters.tools.mcp``).
    """

    model_config = ConfigDict(extra="allow")

    type: str = "http"
    url: str = ""
    headers: dict[str, str] = Field(default_factory=dict)
    timeout: float | None = None


class McpSettings(LayeredSettings):
    """Named MCP servers an agent can depend on (transport / URL / headers).

    Replaces the old root ``.mcp.json``: servers now live in the shared
    ``config.yaml`` under ``mcp:`` (packaged default in ``config.default.yaml``)
    and override via ``AGENTDECK_MCP_SERVERS`` — a JSON object decoded like every
    other complex env field (cf. ``CHATKIT_CORS_ORIGINS``). pydantic-settings
    deep-merges the map across layers, so env need only restate what changes —
    e.g. ``{"agentdeck":{"url":"http://knowledge-mcp:8765/mcp"}}`` overrides just
    that server's URL and keeps the rest of its YAML spec. Agents reference
    servers by name via ``BaseAgent.mcp_server_names``; this class owns *how* to
    reach each one.
    """

    model_config = settings_config("AGENTDECK_MCP_")

    servers: dict[str, McpServerSettings] = Field(
        default_factory=dict,
        description="Named MCP servers, keyed by the name `BaseAgent.mcp_server_names` references. Set as a "
        'JSON object (e.g. `{"agentdeck":{"url":"http://host:8765/mcp"}}`) — deep-merged per server over '
        "the YAML `mcp:` default.",
    )

    def as_config(self) -> dict[str, dict[str, Any]]:
        """``{name: spec}`` in the shape :class:`agentdeck.adapters.tools.mcp.MCPLifecycle` consumes."""
        return {name: spec.model_dump(exclude_none=True) for name, spec in self.servers.items()}


class TavilySettings(LayeredSettings):
    """Tavily web-search API. One knob: ``TAVILY_API_KEY`` env var (or YAML ``tavily: api_key:``)."""

    model_config = settings_config("TAVILY_")

    api_key: str = Field(
        default="",
        description="Tavily web-search API key. Empty makes the `web_search` tool return an `error:` string "
        "instead of raising — it degrades the same way an unavailable MCP server does, rather than disappearing.",
    )


class CheckpointSettings(LayeredSettings):
    """LangGraph checkpointer backend for ``durable=True`` workflows.

    ``backend`` picks the saver (``sqlite`` for dev, ``postgres`` for prod,
    ``memory`` for tests — never persists past the process); ``url`` is the
    sqlite file path or the Postgres DSN. Resolving the saver classes lives in
    ``agentdeck.adapters.engines.langgraph.checkpointer`` — sqlite/postgres ship in the
    optional ``[durability]`` extra, so this settings model stays import-free of them.
    """

    model_config = settings_config("AGENTDECK_CHECKPOINT_")

    backend: str = Field(
        default="sqlite",
        description="Which LangGraph checkpointer backend `durable=True` workflows use: `sqlite` for dev, "
        "`postgres` for prod, or `memory` for tests (never persists past the process).",
    )
    url: str = Field(
        default="",
        description="Sqlite file path or Postgres DSN for the checkpointer. Empty sqlite falls back to "
        "`.agentdeck/checkpoints.sqlite3`.",
    )


class EventsSettings(LayeredSettings):
    """Where the Runtime's canonical event log is written.

    ``memory`` (the default) keeps it in the process and never touches disk, so a plain
    install needs no configuration and no writable project dir — at the cost of a log that
    grows for as long as the process lives and is gone when it exits. Point ``url`` at a
    file and set ``backend: sqlite`` for a log that survives a restart.

    ``redis`` (``url`` is a Redis URL) and ``postgres`` (``url`` is a DSN, and needs the
    ``[durability]`` extra) are the two that several workers can share: SQLite's durability
    rests on cross-process shared memory, so one file behind more than one machine is
    unsupported. Each keeps to its own keyspace, so an instance already holding LangGraph
    checkpoints or agent conversations is fine to reuse. A Redis instance used as the record
    wants ``appendonly yes`` and ``maxmemory-policy noeviction`` — this is a log, not a cache.
    """

    model_config = settings_config("AGENTDECK_EVENTS_")

    backend: str = Field(
        default="memory",
        description="Which backend stores the Runtime's canonical event log: `memory` (default, in-process, "
        "gone when the process exits), `sqlite`, `redis`, or `postgres` (needs the `[durability]` extra).",
    )
    url: str = Field(
        default="",
        description="File path (sqlite), Redis URL, or Postgres DSN for the event log. Required for every "
        "backend except `memory`.",
    )


class ControlSettings(LayeredSettings):
    """Where a run's pending control signals live — what pause and cancel are written to.

    ``memory`` (the default) keeps them in the process, which is all a single worker needs and
    all it can use: a signal written in one process is invisible to another, so with the
    default backend the ``agentdeck runs signal`` CLI and a second web worker cannot reach a
    run at all. Point ``url`` at a file and set ``backend: sqlite`` for signals that cross
    process boundaries — the same file the CLI's ``--control-db`` names. SQLite's cross-process
    story rests on shared memory, so one file behind more than one *machine* is unsupported;
    that one waits for a Redis control port.

    This is a tiny table of pending intent, not a log: nothing here is a record of what
    happened to a run — that is the event store's job, and the control events in it.
    """

    model_config = settings_config("AGENTDECK_CONTROL_")

    backend: str = Field(
        default="memory",
        description="Which backend stores a run's pending control signals: `memory` (default, reachable only "
        "from this process) or `sqlite` (crosses process boundaries — required for the `agentdeck runs signal` "
        "CLI to reach a run).",
    )
    url: str = Field(
        default="",
        description="Sqlite file path for the control backend. Required when `backend` is `sqlite`; matches "
        "the CLI's `--control-db`.",
    )


class SessionSettings(LayeredSettings):
    """Configuration for Redis-backed agent conversation memory.

    Shared infrastructure: plugins that bridge an external thread/message
    store to ``Runner.run_streamed`` (currently the ChatKit backend) read
    these settings to mint a per-session
    :class:`agents.extensions.memory.RedisSession`. Plugins decide
    whether ``redis_url`` is optional or required — the ChatKit backend
    treats it as required and raises at boot if unset.
    """

    model_config = settings_config("AGENTDECK_SESSION_")

    redis_url: str | None = Field(
        default=None,
        description="Redis URL for `RedisSession`-backed agent conversation memory "
        "(`agentdeck.adapters.engines.openai_agents.sessions.SessionFactory`). `None` falls back to one "
        "in-process `SQLiteSession` per session key — no persistence across a restart, no sharing across workers.",
    )
    redis_key_prefix: str = Field(
        default="agents:session", description="Key prefix under which `RedisSession` stores conversations in Redis."
    )
    # Per-session TTL in seconds. ``None`` = sessions persist indefinitely.
    redis_ttl: int | None = Field(
        default=None,
        description="Per-session TTL in seconds for Redis-backed conversations. `None` means sessions persist "
        "indefinitely.",
    )


class SkillsSettings(LayeredSettings):
    """Captures arbitrary ``SKILL_*`` keys (env + YAML ``skill:``); re-exports as ``UPPER_CASE``."""

    model_config = settings_config("SKILL_", extra="allow")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: Any,
        env_settings: Any,
        dotenv_settings: Any,
        file_secret_settings: Any,
    ) -> tuple[Any, ...]:
        # BaseSettings only auto-binds env vars matching declared fields
        # after prefix stripping; the inline ``skill_env`` source captures
        # every ``SKILL_*`` key so operators can declare arbitrary names.
        # YAML's ``skill:`` section is the file-side equivalent.

        def skill_env() -> dict[str, str]:
            return {
                _strip_skill_prefix(name): value
                for name, value in os.environ.items()
                if name.casefold().startswith(_SKILL_PREFIX)
            }

        return (
            init_settings,
            env_settings,
            skill_env,
            SectionedYamlSource(settings_cls, "skill"),
        )

    @model_validator(mode="before")
    @classmethod
    def _normalize_input_keys(cls, data: Any) -> Any:
        if not isinstance(data, Mapping):
            return data
        return {_strip_skill_prefix(k) if isinstance(k, str) else k: v for k, v in data.items()}

    def env_dict(self) -> dict[str, str]:
        return {
            name.upper(): rendered for name, value in self.model_dump().items() if (rendered := _as_env_value(value))
        }


class Settings(BaseModel):
    """Top-level settings aggregating each independently-loaded subgroup."""

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    # default_factory=ClsName goes through env-loaded construction at runtime
    # (pydantic-settings), but pyright sees the class' static signature. Wrap
    # in untyped lambdas so the required-field check on env-backed models
    # doesn't block strict typing.
    openai: OpenAISettings = Field(default_factory=lambda: OpenAISettings.model_validate({}))
    runner: RunnerSettings = Field(default_factory=lambda: RunnerSettings.model_validate({}))
    runtime: RuntimeSettings = Field(default_factory=lambda: RuntimeSettings.model_validate({}))
    checkpoint: CheckpointSettings = Field(default_factory=lambda: CheckpointSettings.model_validate({}))
    events: EventsSettings = Field(default_factory=lambda: EventsSettings.model_validate({}))
    control: ControlSettings = Field(default_factory=lambda: ControlSettings.model_validate({}))
    session: SessionSettings = Field(default_factory=lambda: SessionSettings.model_validate({}))
    skills: SkillsSettings = Field(default_factory=lambda: SkillsSettings.model_validate({}))
    langfuse: LangfuseSettings = Field(default_factory=lambda: LangfuseSettings.model_validate({}))
    mcp: McpSettings = Field(default_factory=lambda: McpSettings.model_validate({}))
    tavily: TavilySettings = Field(default_factory=lambda: TavilySettings.model_validate({}))

    def sandbox_env(self, extra: Mapping[str, str] | None = None) -> dict[str, str]:
        """Standard env every sandbox sees: ``OPENAI_*`` + ``SKILL_*`` + extras."""
        return self.openai.env_dict() | self.skills.env_dict() | dict(extra or {})


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    # Existing process env wins (override=False) so docker-compose / CI exports keep
    # priority over the file; a missing file is a silent no-op.
    load_dotenv(resolve_env_file(), override=False)
    return Settings()


def reset_settings_cache() -> None:
    get_settings.cache_clear()


__all__ = [
    "PACKAGED_DEFAULT_YAML",
    "CheckpointSettings",
    "ControlSettings",
    "EventsSettings",
    "LangfuseSettings",
    "McpServerSettings",
    "McpSettings",
    "OpenAISettings",
    "RunnerSettings",
    "RuntimeSettings",
    "SectionedYamlSource",
    "SessionSettings",
    "Settings",
    "SkillsSettings",
    "TavilySettings",
    "get_settings",
    "reset_settings_cache",
    "resolve_config_path",
    "resolve_env_file",
]
