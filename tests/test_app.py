"""One end-to-end check of the App entry point against a scratch .agentdeck/."""

import asyncio
import sys
import textwrap

import pytest
from scripted_model import ScriptedModel, patch_provider, provider_of

AGENT_PY = """
from pydantic import BaseModel

from agentdeck.agents import BaseAgent

class Greeter(BaseAgent):
    instructions = "Greet the user."

class Greeting(BaseModel):
    greeting: str

class Structured(BaseAgent):
    instructions = "Answer as JSON."
    output_type = Greeting
"""

WORKFLOW_PY = """
from pydantic import BaseModel
from agentdeck.workflows import END, BaseWorkflow, StateGraph

class State(BaseModel):
    text: str = ""

class HelloFlow(BaseWorkflow):
    state = State

    @classmethod
    def build_graph(cls):
        g = StateGraph(cls.state)
        g.add_node("shout", lambda s: {"text": s.text.upper()})
        g.set_entry_point("shout")
        g.add_edge("shout", END)
        return g
"""

SKILL_MD = """---
name: echo-skill
description: Echo input back.
---
Run `scripts/run.py`.
"""


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(textwrap.dedent(AGENT_PY))
    (root / "workflows" / "hello_flow").mkdir(parents=True)
    (root / "workflows" / "hello_flow" / "workflow.py").write_text(textwrap.dedent(WORKFLOW_PY))
    (root / "skills" / "echo-skill" / "scripts").mkdir(parents=True)
    (root / "skills" / "echo-skill" / "SKILL.md").write_text(SKILL_MD)
    (root / "skills" / "echo-skill" / "scripts" / "run.py").touch()
    monkeypatch.chdir(tmp_path)
    # the project alias is process-global; drop stale mounts from other tests
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]
    from agentdeck import App

    return App()


def test_load_discovers_everything(project):
    assert project.load() == {
        "agents": ["Greeter", "Structured"],
        "workflows": ["HelloFlow"],
        "skills": ["echo-skill"],
    }


def test_run_workflow(project):
    out = asyncio.run(project.run_workflow("HelloFlow", {"text": "hi"}))
    assert out["text"] == "HI"


def test_run_workflow_with_no_state_defaults_to_an_empty_object(project):
    """``state=None``'s old meaning ("no updates") has to survive wrapping it in a
    ``DataBlock``, which cannot carry ``None`` as a graph's state."""
    out = asyncio.run(project.run_workflow("HelloFlow"))
    assert out["text"] == ""


def _ctx(session_id):
    """A throwaway context of App's own tenant, for reading its log back in a test —
    exactly what ``surfaces/serve/compat.run_context`` builds for an HTTP request."""
    from agentdeck.surfaces.serve.compat import run_context

    return run_context(session_id)


def test_the_apps_tenant_matches_the_http_compat_layers(project):
    """``App`` mints its own context rather than importing the HTTP surface's, but the two
    tenants must still agree: the store buckets its log by ``(tenant, log_key)``, so a drift
    here would silently split one session's history into two logs depending on which entry
    point ran the turn — exactly what ``test_the_runtime_and_the_python_api_share_one_conversation``
    (tests/test_serve_compat.py) would start failing to catch.
    """
    from agentdeck import app as app_module
    from agentdeck.surfaces.serve import compat

    assert app_module._TENANT == compat.V1_TENANT
    assert app_module._PRINCIPAL == compat.V1_PRINCIPAL


def test_the_apps_structured_output_carrier_matches_the_engines(project):
    """Spelled out independently in both places for the same reason the HTTP surface spells
    its own copy out (D10); this is what keeps the two from drifting apart."""
    from agentdeck import app as app_module
    from agentdeck.adapters.engines.openai_agents import engine as openai_agents_engine

    assert app_module._LEGACY_STRUCTURED_OUTPUT == openai_agents_engine.STRUCTURED_OUTPUT


def test_run_workflow_is_recorded_in_the_log(project):
    """``run_workflow`` used to drive the compiled graph directly and write nothing to the
    event log; it now plays on the Runtime like every other turn."""
    out = asyncio.run(project.run_workflow("HelloFlow", {"text": "hi"}, thread_id="t-hello"))
    assert out["text"] == "HI"

    events = asyncio.run(project.store.read("t-hello", _ctx("t-hello")))
    assert [event.kind for event in events] == ["run.started", "node.updated", "run.completed"]


def test_run_agent_is_recorded_and_returns_a_turn_result(project, monkeypatch):
    """``run_agent`` used to hand back the SDK's own ``RunResult`` and write nothing to the
    log; it now does neither — read the run back by its own ``run_id`` to prove it landed.
    """
    patch_provider(
        monkeypatch,
        provider_of(ScriptedModel(deltas=("hi",), input_tokens=3, output_tokens=4)),
    )

    result = asyncio.run(project.run_agent("Greeter", "hello"))

    assert result.output == "hi"
    assert (result.usage.input_tokens, result.usage.output_tokens) == (3, 4)
    assert result.session_id is None

    events = asyncio.run(project.store.read(result.run_id, _ctx(None)))
    assert [event.kind for event in events] == [
        "run.started",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


def test_chat_is_recorded_and_returns_a_turn_result(project, monkeypatch):
    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("hi",))))

    result = asyncio.run(project.chat("Greeter", "s1", "hello"))

    assert result.output == "hi"
    assert result.session_id == "s1"
    events = asyncio.run(project.store.read("s1", _ctx("s1")))
    assert [event.kind for event in events] == [
        "run.started",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


def test_a_failed_chat_still_leaves_run_failed_in_the_log(project, monkeypatch):
    """The issue's own motivation for this PR, asserted directly: a run that raises is still
    written down, even though nobody read the stream to the end by hand."""
    patch_provider(
        monkeypatch,
        provider_of(ScriptedModel(deltas=("par",), raises=RuntimeError("boom"))),
    )

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(project.chat("Greeter", "s1", "hello"))

    events = asyncio.run(project.store.read("s1", _ctx("s1")))
    assert [event.kind for event in events] == ["run.started", "text.delta", "run.failed"]


def test_a_structured_chat_output_survives_as_validated_data(project, monkeypatch):
    """``RunCompleted.output`` can only hold text; v1's compat engine carries a validated
    ``output_type`` result alongside it, and ``chat``'s ``TurnResult`` must still surface it
    as data rather than the stringified JSON the terminal event itself carries."""
    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=('{"greeting": "hi"}',))))

    result = asyncio.run(project.chat("Structured", "s1", "hello"))

    assert result.output == {"greeting": "hi"}


def test_chat_stream_yields_canonical_events_and_is_recorded(project, monkeypatch):
    """``chat_stream`` used to yield raw text deltas plus a ``StreamDone`` sentinel; it now
    yields the run's own :class:`~agentdeck.core.events.Event`\\ s — a breaking change,
    declared in the CHANGELOG."""
    from agentdeck.core.events import RunCompleted, TextDelta

    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("Hel", "lo"))))

    async def _collect():
        return [event async for event in project.chat_stream("Greeter", "s1", "hello")]

    events = asyncio.run(_collect())

    assert [event.kind for event in events] == [
        "run.started",
        "text.delta",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]
    assert "".join(event.payload.text for event in events if isinstance(event.payload, TextDelta)) == "Hello"
    assert next(e for e in events if isinstance(e.payload, RunCompleted)).payload.output[0].text == "Hello"

    # the stream itself already recorded every one of those events; store.read proves it
    # rather than the caller having to trust chat_stream's own bookkeeping
    stored = asyncio.run(project.store.read("s1", _ctx("s1")))
    assert [event.kind for event in stored] == [event.kind for event in events]


def test_chat_stream_closes_the_runtime_generator_on_abandonment(project, monkeypatch):
    """A caller that stops mid-stream must not leave the run open in the log holding its
    session forever: closing only ``chat_stream``'s own frame would abandon the Runtime's
    generator to the GC instead of closing it, the same trap :func:`_turn_result` (the
    non-streamed methods) avoids by draining to a natural end rather than returning early."""
    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("Hel", "lo"))))

    async def _scenario():
        stream = project.chat_stream("Greeter", "s1", "hello")
        first = await anext(stream)
        second = await anext(stream)
        await stream.aclose()
        # read back inside the same loop, right after aclose() — asyncio.run()'s own
        # asyncgen teardown on exit would close an abandoned inner generator anyway,
        # which would make this assertion pass regardless of what chat_stream does.
        return first, second, await project.store.read("s1", _ctx("s1"))

    first, second, events = asyncio.run(_scenario())
    assert (first.kind, second.kind) == ("run.started", "text.delta")
    assert [event.kind for event in events] == ["run.started", "text.delta", "run.cancelled"]


def test_chat_and_chat_stream_share_one_session(project, monkeypatch):
    """Same guarantee the old ``HeadlessRunner``-backed methods gave: one ``session_id`` is
    one conversation whichever App method ran the turn."""
    model = ScriptedModel(deltas=("hi",))
    patch_provider(monkeypatch, provider_of(model))

    async def _scenario():
        async for _ in project.chat_stream("Greeter", "s1", "first"):
            pass
        await project.chat("Greeter", "s1", "second")

    asyncio.run(_scenario())

    # two model calls, and the second turn's input carries the first turn's history
    assert model.calls == 2
    assert "first" in str(model.inputs[-1])


def test_run_agent_works_without_an_explicit_load(project, monkeypatch):
    """The recorded path has to be at least as convenient as the one it replaces: a quick
    script calling ``App().run_agent(...)`` must not have to remember ``load()`` first."""
    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("hi",))))

    assert project._runtime is None  # nothing has called load() yet
    result = asyncio.run(project.run_agent("Greeter", "hello"))
    assert result.output == "hi"
    assert project._runtime is not None  # run_agent composed it


def test_sessions_keyed_by_id(project):
    assert project.session_for("a") is project.session_for("a")
    assert project.session_for("a") is not project.session_for("b")


def test_pause_and_resume_reach_the_runtime_this_app_composed(project, monkeypatch):
    """The wiring, end to end and with nothing hand-built: ``App.pause_run`` writes to the very
    control port ``App.load`` gave its Runtime, the run stops at its own safe point, and
    ``App.resume_run`` plays it on to completion.

    Signalled before the turn opens on purpose — a signal landing mid-stream is pinned in
    ``tests/test_run_control.py``, where the model can be held at an exact point. What this adds
    is that no test wires the port itself: if the composition root stopped building one, or
    ``App`` reached for a different one, the pause below would go nowhere.
    """
    from scripted_model import ScriptedModel, patch_provider, provider_of

    from agentdeck.core.content import coerce_input
    from agentdeck.surfaces.serve.compat import run_context

    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("hi",))))
    project.load()
    ctx = run_context("s-control")  # exactly what the chat route builds for a request

    async def _flow():
        assert await project.pause_run(ctx.run_id, "operator stepped away") is True
        paused = [event async for event in project.runtime.run("Greeter", coerce_input("hello"), ctx)]
        resumed = await project.resume_run(ctx.run_id)
        return paused, resumed

    paused, resumed = asyncio.run(_flow())

    assert [event.kind for event in paused][-3:] == ["control.requested", "control.observed", "run.paused"]
    assert next(e.payload.reason for e in paused if e.kind == "run.paused") == "operator stepped away"
    assert [event.kind for event in resumed][0] == "run.resumed"
    assert [event.kind for event in resumed][-1] == "run.completed"
    assert asyncio.run(project.resume_run(ctx.run_id)) == []  # nothing left to resume


@pytest.fixture(autouse=True)
def _reset_mcp_lifecycle():
    from agentdeck.adapters.tools.mcp.lifecycle import MCPLifecycle

    yield
    MCPLifecycle.reset()


class FakeSessionFactory:
    """Stand-in for the Redis-backed SessionFactory; counts aclose() calls."""

    def __init__(self):
        self.closed = 0
        self.sessions = {}

    def session_for(self, session_id):
        from agents import SQLiteSession

        return self.sessions.setdefault(session_id, SQLiteSession(session_id))

    async def aclose(self):
        self.closed += 1


def test_open_close_lifecycle(project):
    """open -> chat-plumbing-level usage (no live model) -> aclose, SQLite fallback."""
    from agentdeck import App

    async def scenario() -> App:
        async with App.open() as app:
            assert app.session_factory is None  # no AGENTDECK_SESSION_REDIS_URL in test env
            assert app.session_for("s1") is app.session_for("s1")
            assert app.inventory["agents"] == ["Greeter", "Structured"]  # load() ran and stashed the inventory
        return app  # aclose() already ran once via the `async with` exit

    app = asyncio.run(scenario())
    asyncio.run(app.aclose())  # idempotent: closing an already-closed app must not raise


def test_only_the_app_that_started_mcp_shuts_it_down(project, monkeypatch):
    """The MCP registry is process-wide: a bare App must not tear down someone else's servers."""
    from agentdeck import App
    from agentdeck.adapters.tools.mcp.lifecycle import MCPLifecycle

    calls = []

    async def spy():
        calls.append(1)

    monkeypatch.setattr(MCPLifecycle, "shutdown", staticmethod(spy))

    asyncio.run(App().aclose())
    assert calls == []

    async def scenario():
        async with App.open():
            pass

    asyncio.run(scenario())
    assert calls == [1]


def test_injected_session_factory_is_used_and_closed_once(project, monkeypatch):
    """The DI seam bypasses `from_settings` and `aclose()` closes the injection exactly once."""
    from agentdeck import App
    from agentdeck import app as app_module
    from agentdeck.adapters.engines.openai_agents.sessions import SessionFactory

    def boom(_settings):
        raise AssertionError("from_settings must not be called when a factory is injected")

    monkeypatch.setattr(SessionFactory, "from_settings", staticmethod(boom))
    fake = FakeSessionFactory()

    async def scenario() -> App:
        async with App.open(session_factory=fake) as app:
            assert app.session_factory is fake
            # Tenant-scoped, because the engine's own store is what mints it now: two tenants
            # are free to pick the same session id, and an unprefixed key would hand them one
            # conversation. Same key whichever entry point the turn arrived through.
            assert app.session_for("s1") is fake.sessions[f"{app_module._TENANT}:s1"]
        return app

    app = asyncio.run(scenario())
    assert fake.closed == 1
    asyncio.run(app.aclose())
    assert fake.closed == 1


def test_injected_session_factory_closed_when_load_fails(tmp_path, monkeypatch):
    """A failure inside open() (broken bundle) must still close the injected factory."""
    from agentdeck.errors import ConfigError

    root = tmp_path / ".agentdeck"
    (root / "agents" / "broken").mkdir(parents=True)
    (root / "agents" / "broken" / "agent.py").write_text("raise RuntimeError('broken bundle')\n")
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]
    from agentdeck import App

    fake = FakeSessionFactory()

    async def scenario():
        async with App.open(session_factory=fake):
            pass

    # the raw RuntimeError is now wrapped in a ConfigError naming the offending bundle path
    with pytest.raises(ConfigError, match="agents/broken/agent.py"):
        asyncio.run(scenario())
    assert fake.closed == 1


def test_old_layout_raises_clear_config_error(tmp_path, monkeypatch):
    """A pre-0.3 project (bundles straight under the project root) fails loudly, not silently."""
    from agentdeck.errors import ConfigError

    root = tmp_path / ".agentdeck"
    (root / "greeter").mkdir(parents=True)
    (root / "greeter" / "agent.py").write_text(textwrap.dedent(AGENT_PY))
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]
    from agentdeck import App

    with pytest.raises(ConfigError, match="agents/<bundle>/agent.py"):
        App().load()
