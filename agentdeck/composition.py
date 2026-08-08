"""The composition root: the one place adapters are built and handed to a ``Runtime``.

Everything above this module takes ports; everything below it is an adapter. ``App`` calls
:func:`build_runtime` and so does every other entry point that needs a real Runtime — the
demo script, the compat surface's tests — so a Runtime is assembled the same way
everywhere instead of hand-wired per caller. A second front door (a code-first ``Deck()``)
becomes another caller of this function rather than a second assembly.

Only the parts a caller actually varies are arguments; the rest resolve from settings. That
resolution happens *here*, never inside an adapter: an engine that reached for
``get_settings()`` itself could not be handed a different endpoint by a caller, and a second
front door would have to mutate process state to get one. The ``resolve_*`` functions below
are what an entry point calls to fill an adapter's constructor in.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from agentdeck.adapters.caps.sandbox import open_sandbox
from agentdeck.adapters.control.memory import MemoryControlPort
from agentdeck.adapters.control.sqlite import SqliteControlPort
from agentdeck.adapters.engines.openai_agents.runconfig import RunSettings
from agentdeck.adapters.stores.memory import MemoryEventStore
from agentdeck.adapters.stores.sqlite import SqliteEventStore
from agentdeck.adapters.telemetry.langfuse.client import langfuse_sink
from agentdeck.agents.runners.base import default_use_responses, needs_sandbox
from agentdeck.runtime.discovery import InvocableRegistry
from agentdeck.runtime.observability import sandbox_trace_env
from agentdeck.runtime.service import Runtime
from agentdeck.runtime.settings import ControlSettings, EventsSettings, Settings, get_settings

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping, Sequence
    from contextlib import AbstractAsyncContextManager
    from datetime import datetime

    from agents import Agent

    from agentdeck.core.invocable import InvocableSpec
    from agentdeck.core.ports import ControlPort, EnginePort, EventSinkPort, EventStorePort


def build_runtime(
    *,
    engines: Sequence[EnginePort],
    invocables: Mapping[str, InvocableSpec] | None = None,
    store: EventStorePort | None = None,
    sinks: Sequence[EventSinkPort] | None = None,
    control: ControlPort | None = None,
    clock: Callable[[], datetime] | None = None,
) -> Runtime:
    """Wire ``engines`` into a Runtime over the project's invocables.

    ``invocables`` defaults to discovery over ``./.agentdeck`` — pass a mapping to run
    specs built in code instead. ``store`` defaults to the configured event store,
    ``control`` to the configured control port, and ``sinks`` to the configured telemetry —
    passing ``sinks=()`` is how a caller asks for none at all.

    ``clock`` no longer reaches anything that stamps an event. Timestamps are assigned by the
    store, in the same write that persists the event (ADR-D11), so holding time still means
    building the store with a clock — ``MemoryEventStore(clock=...)``, ``RedisEventStore(clock=...)``
    — and the two SQL stores read their backend's clock so that N workers on one database
    compare one clock rather than N. The keyword is still accepted and still forwarded, it
    decides nothing, and passing it warns. Removal is #158.
    """
    engines = tuple(engines)
    specs = InvocableRegistry(engines).load() if invocables is None else invocables
    store = store or resolve_event_store()
    control = control or resolve_control_port()
    if sinks is None:
        # Telemetry is a reader of the event stream, so it is wired here rather than opened
        # by whatever happens to be running: one sink covers agents, workflows and every
        # engine at once. ``None`` when Langfuse has no keys, which registers nothing at all.
        telemetry = langfuse_sink()
        sinks = () if telemetry is None else (telemetry,)
    if clock is None:
        return Runtime(engines, store, specs, sinks=sinks, control=control)
    return Runtime(engines, store, specs, sinks=sinks, clock=clock, control=control)


def resolve_run_settings(settings: Settings | None = None) -> RunSettings:
    """Everything one agent run is configured with, read out of settings once.

    Every field here is invisible to a fake-model test suite and decisive against a real
    endpoint — the CA bundle, the token cap, the provider's own base URL — which is why
    ``tests/test_run_config_parity.py`` compares the resolved result field by field rather
    than trusting a run that streamed to have been configured correctly.
    """
    resolved = settings if settings is not None else get_settings()
    return RunSettings(
        model=resolved.openai.model,
        api_key=resolved.openai.api_key,
        base_url=resolved.openai.base_url,
        ca_bundle=resolved.openai.ca_bundle,
        use_responses=default_use_responses(),
        workflow_name=resolved.runner.workflow_name,
        nest_handoff_history=True,
        temperature=resolved.runner.temperature,
        max_tokens=resolved.runner.max_tokens,
        max_turns=resolved.runner.max_turns,
    )


def resolve_checkpoint(settings: Settings | None = None) -> tuple[str, str]:
    """The ``(backend, url)`` a durable workflow checkpoints to.

    A pair of strings, not a saver: the sqlite/postgres savers live in the ``[durability]``
    extra, so naming a backend here must not import one — the langgraph adapter builds it at
    the first durable run and not before.
    """
    checkpoint = (settings if settings is not None else get_settings()).checkpoint
    return checkpoint.backend, checkpoint.url


def resolve_agent_sandbox() -> Callable[[Agent[Any]], AbstractAsyncContextManager[Any]]:
    """How the openai-agents engine opens a sandbox for an agent that needs one.

    Yields the SDK's own run-config handle rather than the port: attaching a sandbox to an SDK
    run means handing the SDK its own type, so there is nothing for core to describe. The engine
    treats it as opaque, which is why the type here is ``Any`` and not the adapter's class.
    """

    @asynccontextmanager
    async def scope(agent: Agent[Any]) -> AsyncIterator[Any]:
        # Opens when the top-level agent OR any reachable worker is a SandboxAgent: the SDK
        # requires ``run_config.sandbox`` before any handoff target executes, not just for
        # the first agent on the turn. ``open_sandbox`` joins an outer sandbox bound to the
        # current async context, so a nested run shares the caller's session.
        if not needs_sandbox(agent):
            yield None
            return
        async with open_sandbox(environment=get_settings().sandbox_env(), trace_env=sandbox_trace_env) as sandbox:
            yield sandbox.sandbox_run_config

    return scope


def resolve_workflow_workspace() -> Callable[[], AbstractAsyncContextManager[Any]]:
    """The sandbox a workflow's nodes run inside.

    Unconditional, unlike the agent scope: a ``SkillNode`` or a ``LoadFileNode`` calls
    ``require_sandbox()`` and raises without one, and which nodes a graph holds is not
    something this engine can see.
    """
    return lambda: open_sandbox(environment=get_settings().sandbox_env(), trace_env=sandbox_trace_env)


def resolve_control_port(settings: ControlSettings | None = None) -> ControlPort:
    """Build the control port named by ``backend``: ``memory`` (default) or ``sqlite``.

    Always built, never left off: a Runtime without one cannot pause or cancel anything, and a
    caller finding that out from an endpoint that silently did nothing is worse than the
    in-memory port's own limit — which is that only this process can reach the run.
    """
    control = settings if settings is not None else get_settings().control
    backend = control.backend.strip().lower()
    if backend == "memory":
        return MemoryControlPort()
    if backend == "sqlite":
        if not control.url:
            raise ValueError("the sqlite control port needs a file path: set AGENTDECK_CONTROL_URL")
        return SqliteControlPort(control.url)
    raise ValueError(f"unknown control backend {control.backend!r}; expected memory or sqlite")


def resolve_event_store(settings: EventsSettings | None = None) -> EventStorePort:
    """Build the event store named by ``backend``: ``memory`` (default), ``sqlite``,
    ``redis`` or ``postgres``.

    The last two are imported inside their own branch, not at module scope: this module is on
    the import path of every entry point, and Postgres needs the ``[durability]`` extra, so a
    top-level import would make that extra mandatory for anyone who only chats.
    """
    events = settings if settings is not None else get_settings().events
    backend = events.backend.strip().lower()
    if backend == "memory":
        return MemoryEventStore()
    if backend == "sqlite":
        if not events.url:
            raise ValueError("the sqlite event store needs a file path: set AGENTDECK_EVENTS_URL")
        return SqliteEventStore(events.url)
    if backend == "redis":
        if not events.url:
            raise ValueError("the redis event store needs a URL: set AGENTDECK_EVENTS_URL")
        from agentdeck.adapters.stores.redis import RedisEventStore

        return RedisEventStore(events.url)
    if backend == "postgres":
        if not events.url:
            raise ValueError("the postgres event store needs a DSN: set AGENTDECK_EVENTS_URL")
        try:
            from agentdeck.adapters.stores.postgres import PostgresEventStore
        except ImportError as exc:
            raise ImportError(
                'the postgres event store needs psycopg — install the "durability" extra: '
                'pip install "agentdeck[durability]"'
            ) from exc
        return PostgresEventStore(events.url)
    raise ValueError(f"unknown event store backend {events.backend!r}; expected memory, sqlite, redis or postgres")


__all__ = [
    "build_runtime",
    "resolve_agent_sandbox",
    "resolve_checkpoint",
    "resolve_control_port",
    "resolve_event_store",
    "resolve_run_settings",
    "resolve_workflow_workspace",
]
