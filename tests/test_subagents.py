"""spawn_subagent tool: allowlist, depth guard, parallel fan-out, opt-in wiring."""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from agents import Runner
from agents.tool_context import ToolContext

from agentdeck.agents.base import BaseAgent, BaseSandboxAgent
from agentdeck.agents.registry import AgentRegistry
from agentdeck.agents.subagents import _depth, spawn_subagent_tool
from agentdeck.errors import NotFoundError


class Worker(BaseAgent):
    handoff_description = "does the work"


class Supervisor(BaseAgent):
    subagents = ["Worker"]


class FakeRegistry:
    """Stand-in for AgentRegistry with a fixed roster — no project dir needed."""

    def __init__(self, agents):
        self._agents = agents

    def get(self, name):
        try:
            return self._agents[name]
        except KeyError:
            raise NotFoundError(f"No agent named {name!r}.") from None


def _ctx() -> ToolContext:
    return ToolContext(context=None, tool_name="spawn_subagent", tool_call_id="1", tool_arguments="{}")


def _invoke(tool, agent: str, task: str) -> str:
    payload = f'{{"agent": "{agent}", "task": "{task}"}}'
    return asyncio.run(tool.on_invoke_tool(_ctx(), payload))


@pytest.fixture(autouse=True)
def _reset_depth():
    token = _depth.set(0)
    yield
    _depth.reset(token)


def _stub_headless_runner(monkeypatch):
    """Replace HeadlessRunner.from_agent with a fake that echoes the task, no live model call."""
    from agentdeck.agents.runners.headless import HeadlessRunner

    class FakeRunner:
        async def run(self, task, *, session=None):
            await asyncio.sleep(0)  # yield control so gathered spawns genuinely interleave
            return SimpleNamespace(final_output=f"handled:{task}")

    monkeypatch.setattr(HeadlessRunner, "from_agent", staticmethod(lambda agent, **kw: FakeRunner()))


def test_spawn_allowed_agent_returns_final_output(monkeypatch):
    _stub_headless_runner(monkeypatch)
    tool = spawn_subagent_tool(Supervisor, registry=FakeRegistry({"Worker": Worker}))

    result = _invoke(tool, "Worker", "do the thing")

    assert result == "handled:do the thing"


def test_spawn_outside_allowlist_returns_error_string_and_run_continues(monkeypatch):
    _stub_headless_runner(monkeypatch)
    tool = spawn_subagent_tool(Supervisor, registry=FakeRegistry({"Worker": Worker}))

    result = _invoke(tool, "Intruder", "do the thing")

    assert result.startswith("error: unknown_subagent")
    assert "Intruder" in result
    # a disallowed name doesn't raise — the tool call completes and the agent can retry
    assert _invoke(tool, "Worker", "retry") == "handled:retry"


def test_depth_exhausted_returns_error_string(monkeypatch):
    _stub_headless_runner(monkeypatch)
    tool = spawn_subagent_tool(Supervisor, registry=FakeRegistry({"Worker": Worker}))
    _depth.set(1)  # simulates being invoked from inside an already-spawned subagent

    result = _invoke(tool, "Worker", "do the thing")

    assert result == "error: subagent_depth_exhausted: subagents cannot spawn further subagents"


def test_parallel_spawns_both_complete(monkeypatch):
    _stub_headless_runner(monkeypatch)
    tool = spawn_subagent_tool(Supervisor, registry=FakeRegistry({"Worker": Worker}))

    async def scenario():
        return await asyncio.gather(
            tool.on_invoke_tool(_ctx(), '{"agent": "Worker", "task": "a"}'),
            tool.on_invoke_tool(_ctx(), '{"agent": "Worker", "task": "b"}'),
        )

    results = asyncio.run(scenario())

    assert results == ["handled:a", "handled:b"]
    assert _depth.get() == 0  # each gathered task resets its own depth; nothing leaks to the caller


def test_agent_without_subagents_gets_no_tool():
    class Plain(BaseAgent):
        instructions = "hi"

    kwargs = Plain._kwargs()

    assert "tools" not in kwargs


def test_agent_with_subagents_gets_spawn_tool():
    kwargs = Supervisor._kwargs()

    assert len(kwargs["tools"]) == 1
    assert kwargs["tools"][0].name == "spawn_subagent"


def test_spawning_sandbox_agent_triggers_attach_sandbox(monkeypatch):
    """Sandbox interplay (issue #11 risk area): spawning a BaseSandboxAgent subagent must
    still make HeadlessRunner.attach_sandbox() detect the SandboxAgent and bind run_config.sandbox
    before the turn runs — real HeadlessRunner/attach_sandbox, only the SDK boundary is faked.
    """

    class SandboxWorker(BaseSandboxAgent):
        handoff_description = "sandboxed worker"

    opened = []

    @asynccontextmanager
    async def fake_open(**kwargs):
        opened.append(kwargs)
        yield SimpleNamespace(sandbox_run_config="fake-sandbox-config")

    # Patched where the runner looks it up: the opener is a module-level function, so the
    # runner's own binding is the one that has to be replaced.
    monkeypatch.setattr("agentdeck.agents.runners.base.open_sandbox", fake_open)

    async def fake_run(agent, message, *, run_config, max_turns, session=None):
        assert run_config.sandbox == "fake-sandbox-config"  # attach_sandbox wired it in first
        return SimpleNamespace(final_output="sandboxed result", context_wrapper=SimpleNamespace(usage=None))

    monkeypatch.setattr(Runner, "run", fake_run)

    tool = spawn_subagent_tool(Supervisor, registry=FakeRegistry({"Worker": SandboxWorker}))
    result = _invoke(tool, "Worker", "do sandboxed thing")

    assert result == "sandboxed result"
    assert opened  # open_sandbox was entered -> _needs_sandbox's isinstance(SandboxAgent) branch fired


def test_spawn_subagent_tool_defaults_to_project_registry():
    """No registry passed -> falls back to AgentRegistry(_PROJECT_ALIAS), not a hard dependency."""
    from agentdeck.agents.subagents import _PROJECT_ALIAS

    tool = spawn_subagent_tool(Supervisor)

    assert tool.name == "spawn_subagent"
    assert _PROJECT_ALIAS == "agentdeck_project"
    assert isinstance(AgentRegistry(_PROJECT_ALIAS), AgentRegistry)
