"""The assembly seam: one function builds every Runtime, and App is one of its callers."""

import os
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import Any

import live_stores
import pytest
from project_engines import project_engines
from scripted_model import ScriptedModel, patch_provider, provider_of

from agentdeck.adapters.stores.memory import MemoryEventStore
from agentdeck.adapters.stores.sqlite import SqliteEventStore
from agentdeck.adapters.telemetry.langfuse import client as langfuse_client
from agentdeck.composition import build_runtime, resolve_event_store
from agentdeck.core.content import coerce_input
from agentdeck.core.context import RunContext
from agentdeck.errors import ConfigError, NotFoundError
from agentdeck.runtime.discovery import InvocableRegistry
from agentdeck.runtime.service import Runtime
from agentdeck.runtime.settings import EventsSettings, reset_settings_cache

AGENT_PY = """
from agentdeck.agents import BaseAgent

class Greeter(BaseAgent):
    instructions = "Greet the user."
"""

WORKFLOW_PY = """
from typing import TypedDict

from agentdeck.workflows import END, BaseWorkflow, StateGraph


class State(TypedDict, total=False):
    input: str
    shouted: str


class Shout(BaseWorkflow):
    state = State

    @classmethod
    def build_graph(cls):
        g = StateGraph(cls.state)
        g.add_node("shout", lambda s: {"shouted": s["input"].upper()})
        g.set_entry_point("shout")
        g.add_edge("shout", END)
        return g
"""

CTX = RunContext(tenant="local", principal="user:local", run_id="r1", trace_id="t1")


@dataclass
class Opened:
    """One observation the sink opened — what would have become a Langfuse span."""

    name: str
    kind: str
    session_id: str | None = None
    children: list["Opened"] = field(default_factory=list)

    def child(self, name: str, *, kind: str, input: Any = None, metadata: Any = None) -> "Opened":  # noqa: A002, ARG002 — the Tracer port's own signature
        opened = Opened(name=name, kind=kind)
        self.children.append(opened)
        return opened

    def finish(self, **_kwargs: Any) -> None:
        """What a span carries when it closes is the sink's business, tested there."""

    def shape(self) -> list[tuple[str, str]]:
        return [(child.name, child.kind) for child in self.children]


@dataclass
class RecordingTracer:
    """Stands in for the Langfuse SDK: every root the sink opened, in memory."""

    roots: list[Opened] = field(default_factory=list)

    def root(self, name: str, *, kind: str, session_id: str | None, **_kwargs: Any) -> Opened:
        opened = Opened(name=name, kind=kind, session_id=session_id)
        self.roots.append(opened)
        return opened

    def flush(self) -> None:
        """Nothing to ship."""


@pytest.fixture
def recorded_traces(monkeypatch):
    """Swap the Langfuse SDK for a recorder, so what the composition root wires is assertable
    without the ``[observability]`` extra, a key that reaches anything, or a network."""
    tracer = RecordingTracer()
    monkeypatch.setattr(langfuse_client, "_build_client", lambda _settings: None)
    monkeypatch.setattr(langfuse_client, "LangfuseTracer", lambda _client: tracer)
    return tracer


@pytest.fixture
def langfuse_keys(monkeypatch):
    """Set the two keys that decide whether Langfuse is configured at all.

    The settings cache is cleared on the way out as well as in: ``monkeypatch`` restores the
    environment but not an ``lru_cache``, and a leaked one would leave every later test in
    this process running with Langfuse on.
    """

    def _set(public_key: str, secret_key: str) -> None:
        monkeypatch.setenv("AGENTDECK_LANGFUSE_PUBLIC_KEY", public_key)
        monkeypatch.setenv("AGENTDECK_LANGFUSE_SECRET_KEY", secret_key)
        reset_settings_cache()

    yield _set
    reset_settings_cache()


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(textwrap.dedent(AGENT_PY))
    (root / "workflows" / "shout").mkdir(parents=True)
    (root / "workflows" / "shout" / "workflow.py").write_text(textwrap.dedent(WORKFLOW_PY))
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]
    return tmp_path


def test_the_project_engine_set_covers_both_bundle_shapes():
    """Discovery refuses a project whose workflows have no engine, so both are registered."""
    engines = project_engines()
    assert sorted(engine.engine for engine in engines) == ["langgraph", "openai-agents"]
    assert [type(engine).__name__ for engine in engines] == ["OpenAIAgentsEngine", "LangGraphEngine"]


def test_app_wires_the_same_engines_this_suite_builds_by_hand(project):
    """What keeps ``tests/project_engines.py`` honest: the facade is the only production
    caller, so a test set that stopped matching its wiring would be testing nothing."""
    from agentdeck import App

    app = App()
    app.load()

    assert [type(engine).__name__ for engine in app.runtime._engines.values()] == [
        type(engine).__name__ for engine in project_engines()
    ]


async def test_build_runtime_discovers_the_project_when_given_no_invocables(project):
    runtime = build_runtime(engines=project_engines(), store=MemoryEventStore())

    kinds = [event.kind async for event in runtime.run("Shout", coerce_input("hello"), CTX)]

    assert kinds == ["run.started", "node.updated", "run.completed"]


async def test_build_runtime_takes_explicit_specs_and_a_store_that_holds_time_still(project):
    """A caller with specs in hand skips discovery, and freezes time by handing in a store with a
    clock — which is the only seam that decides a ``ts`` now (ADR-D11). ``build_runtime``'s own
    ``clock`` keyword no longer reaches anything that stamps an event."""
    frozen = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    engines = project_engines()
    specs = InvocableRegistry(engines).load()

    runtime = build_runtime(engines=engines, invocables=specs, store=MemoryEventStore(clock=lambda: frozen))
    stamps = {event.ts async for event in runtime.run("Shout", coerce_input("hello"), CTX)}

    assert stamps == {frozen}


async def test_build_runtime_refuses_an_unknown_invocable(project):
    runtime = build_runtime(engines=project_engines(), store=MemoryEventStore())

    with pytest.raises(NotFoundError):
        [event async for event in runtime.run("Nope", coerce_input("hello"), CTX)]


def test_resolve_event_store_defaults_to_memory():
    assert isinstance(resolve_event_store(EventsSettings(backend="memory")), MemoryEventStore)


def test_resolve_event_store_builds_sqlite_from_a_path(tmp_path):
    store = resolve_event_store(EventsSettings(backend="sqlite", url=str(tmp_path / "events.sqlite3")))

    assert isinstance(store, SqliteEventStore)
    store.close()


def test_resolve_event_store_rejects_sqlite_without_a_path():
    with pytest.raises(ValueError, match="AGENTDECK_EVENTS_URL"):
        resolve_event_store(EventsSettings(backend="sqlite"))


def test_resolve_event_store_rejects_an_unknown_backend():
    with pytest.raises(ValueError, match="unknown event store backend"):
        resolve_event_store(EventsSettings(backend="not-a-backend"))


def test_resolve_event_store_builds_redis_from_a_url():
    """No server needed: the client connects lazily, so wiring is checkable without one."""
    from agentdeck.adapters.stores.redis import RedisEventStore

    store = resolve_event_store(EventsSettings(backend="redis", url="redis://localhost:6379/0"))

    assert isinstance(store, RedisEventStore)


def test_resolve_event_store_builds_postgres_from_a_dsn():
    live_stores.require_psycopg()
    from agentdeck.adapters.stores.postgres import PostgresEventStore

    store = resolve_event_store(EventsSettings(backend="postgres", url="postgresql://localhost/whatever"))

    assert isinstance(store, PostgresEventStore)


@pytest.mark.parametrize("backend", ["redis", "postgres"])
def test_resolve_event_store_rejects_a_shared_backend_without_a_url(backend):
    with pytest.raises(ValueError, match="AGENTDECK_EVENTS_URL"):
        resolve_event_store(EventsSettings(backend=backend))


def test_choosing_a_store_does_not_make_the_durability_extra_mandatory():
    """``composition`` is on every entry point's import path and ``psycopg`` is an optional
    extra, so its import has to stay inside the branch that asks for it. A fresh interpreter,
    because this one has already imported half the world."""
    probe = "import agentdeck.composition, sys; print('psycopg' in sys.modules)"
    done = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, timeout=120, check=True)

    assert done.stdout.strip() == "False"


async def test_a_configured_langfuse_traces_a_workflow_run_under_its_session(project, recorded_traces, langfuse_keys):
    """The gap this closes: nothing in production ever registered the sink, and the observation
    v1 opened around a workflow run was handed an identity nobody bound, so a workflow reached
    Langfuse as an anonymous trace at best. A Runtime built the ordinary way now traces the run
    from its own events — session included, node by node.
    """
    langfuse_keys("pk-lf-test", "sk-lf-test")
    runtime = build_runtime(engines=project_engines(), store=MemoryEventStore())

    async for _ in runtime.run("Shout", coerce_input("hello"), replace(CTX, session_id="s-1")):
        pass
    await runtime.drain()

    [trace] = recorded_traces.roots
    assert (trace.name, trace.kind, trace.session_id) == ("Shout", "chain", "s-1")
    assert trace.shape() == [("shout", "span"), ("run.usage", "generation")]


async def test_a_chat_turn_reaches_langfuse_under_its_own_session(project, recorded_traces, langfuse_keys, monkeypatch):
    """The identity ``App.chat`` already gave its trace, kept while its owner changes: the
    engine no longer opens an observation of its own, so if the sink did not carry the session
    across, every chat turn would go anonymous the moment the wrapping span was removed."""
    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("hi",))))
    langfuse_keys("pk-lf-test", "sk-lf-test")
    from agentdeck import App

    app = App()
    result = await app.chat("Greeter", "wa-123", "hello")
    await app.aclose()

    assert result.output == "hi"
    [trace] = recorded_traces.roots
    assert (trace.name, trace.kind, trace.session_id) == ("Greeter", "agent", "wa-123")


async def test_an_unconfigured_langfuse_leaves_the_run_untraced(project, recorded_traces, langfuse_keys):
    """Without keys there is no sink in the list at all, so a run never reaches this adapter —
    the same silence v1 kept, and what makes the wiring safe to do unconditionally."""
    langfuse_keys("", "")
    runtime = build_runtime(engines=project_engines(), store=MemoryEventStore())

    async for _ in runtime.run("Shout", coerce_input("hello"), CTX):
        pass
    await runtime.drain()

    assert recorded_traces.roots == []


def test_wiring_telemetry_does_not_make_the_observability_extra_mandatory():
    """Every ``build_runtime`` call consults Langfuse now, so the SDK import has to stay behind
    the keys. A fresh interpreter with the keys explicitly cleared, because this one has
    already imported half the world and a developer's own keys would mask the answer."""
    probe = (
        "import sys;"
        "from agentdeck.adapters.engines.stub import StubEngine;"
        "from agentdeck.composition import build_runtime;"
        "build_runtime(engines=[StubEngine()], invocables={});"
        "assert 'langfuse' not in sys.modules, sorted(m for m in sys.modules if 'langfuse' in m);"
        "print('no keys, no sdk')"
    )
    blank = {"AGENTDECK_LANGFUSE_PUBLIC_KEY": "", "AGENTDECK_LANGFUSE_SECRET_KEY": ""}
    done = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True, timeout=120, env={**os.environ, **blank}
    )

    assert done.returncode == 0, done.stderr
    assert "no keys, no sdk" in done.stdout


def test_app_has_no_runtime_before_load(project):
    from agentdeck import App

    with pytest.raises(ConfigError, match="call App.load()"):
        _ = App().runtime


async def test_app_composes_one_runtime_over_the_whole_project(project):
    """``App`` is a caller of the seam, not a second assembly: its Runtime covers every
    discovered bundle, workflows included."""
    from agentdeck import App

    app = App()
    app.load()

    assert isinstance(app.runtime, Runtime)
    kinds = [event.kind async for event in app.runtime.run("Shout", coerce_input("hello"), CTX)]
    assert kinds == ["run.started", "node.updated", "run.completed"]
    await app.aclose()
    await app.aclose()  # idempotent, with a Runtime to drain
