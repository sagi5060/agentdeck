"""The MCP tool source behind ``ToolSourcePort``, and the v1 prompt behavior it must not change.

No socket is ever opened: the SDK's own ``connect``/``cleanup`` are stubbed, so the
adapter's registry, soft-fail and banner logic all run for real.
"""

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest
from agents.mcp import MCPServerStreamableHttp

from agentdeck.adapters.tools.mcp import (
    MCP_SERVER_NAMES_KEY,
    MCPLifecycle,
    MCPServerStreamableHttpResilient,
    MCPToolSource,
    load_mcp_config,
    mcp_status_banner,
)
from agentdeck.agents import BaseAgent
from agentdeck.core.invocable import InvocableKind, InvocableSpec
from agentdeck.core.ports import ToolSet, ToolSourcePort

# Unreachable on purpose — nothing dials it, and a test that regressed into dialling hangs
# on connect refusal rather than silently talking to a real server.
KNOWLEDGE = {"type": "http", "url": "http://127.0.0.1:1/mcp"}

INSTRUCTIONS = "Answer from the knowledge base."

# Pinned byte-for-byte: this text is a prompt, so an "harmless" rewording changes every
# degraded agent's instructions and invalidates upstream prompt caches.
BANNER = (
    "## Runtime MCP status\n\n"
    "The following MCP server(s) you depend on are UNAVAILABLE: `knowledge`.\n"
    "Tools from those servers are NOT exposed to you in this turn.\n"
    "If the user's request requires those tools, reply with exactly:\n\n"
    "    error: mcp_unavailable: <server-name>\n\n"
    "and stop. Do not invent results. Do not retry. Do not call other "
    "tools as a substitute unless the user explicitly asks for an "
    "alternative.\n\n---\n\n"
)


class Researcher(BaseAgent):
    instructions = INSTRUCTIONS
    mcp_server_names = ("knowledge",)


def _spec(*names):
    metadata = {MCP_SERVER_NAMES_KEY: list(names)} if names else {}
    return InvocableSpec(name="Researcher", kind=InvocableKind.AGENT, engine="openai-agents", metadata=metadata)


@pytest.fixture(autouse=True)
def _isolated_lifecycle(monkeypatch):
    """A clean registry and no configured servers, whatever the developer's config.yaml says."""
    from agentdeck.runtime.settings import reset_settings_cache

    monkeypatch.setenv("AGENTDECK_MCP_SERVERS", "{}")
    reset_settings_cache()
    MCPLifecycle.reset()
    yield
    MCPLifecycle.reset()
    reset_settings_cache()


@pytest.fixture
def connect(monkeypatch):
    """Connect servers without a socket; return a ``refuse(name)`` switch for failures."""
    refused: set[str] = set()

    async def _connect(self):
        if self.name in refused:
            raise PermissionError(f"{self.name}: no")  # not transient — no retry backoff to wait out

    async def _cleanup(self):
        return None

    monkeypatch.setattr(MCPServerStreamableHttp, "connect", _connect)
    monkeypatch.setattr(MCPServerStreamableHttp, "cleanup", _cleanup)
    return refused


def _start(config):
    asyncio.run(MCPLifecycle.startup(config))


def test_the_mcp_source_is_a_tool_source_port():
    assert isinstance(MCPToolSource(), ToolSourcePort)


def test_an_invocable_declaring_no_servers_gets_an_empty_tool_set():
    assert MCPToolSource().resolve(_spec()) == ToolSet()


def test_connected_servers_are_handed_over_with_no_notice(connect):
    _start({"knowledge": KNOWLEDGE})

    tools = MCPToolSource().resolve(_spec("knowledge"))

    assert tools.tools == (MCPLifecycle.resolve_or_pending("knowledge"),)
    assert isinstance(tools.tools[0], MCPServerStreamableHttpResilient)
    assert tools.unavailable == ()
    assert tools.notice == ""


def test_a_server_that_refused_to_connect_is_reported_not_raised(connect):
    connect.add("knowledge")

    _start({"knowledge": KNOWLEDGE})  # a dead MCP host must not fail startup

    tools = MCPToolSource().resolve(_spec("knowledge"))
    assert tools.tools == ()
    assert tools.unavailable == ("knowledge",)
    assert tools.notice == BANNER


def test_an_unknown_server_is_reported_not_raised(connect):
    _start({"knowledge": KNOWLEDGE})

    tools = MCPToolSource().resolve(_spec("absent"))

    assert tools.tools == ()
    assert tools.unavailable == ("absent",)
    assert "`absent`" in tools.notice


def test_nothing_configured_at_all_still_resolves(connect):
    tools = MCPToolSource().resolve(_spec("knowledge"))

    assert tools.tools == ()
    assert tools.unavailable == ("knowledge",)


def test_available_and_missing_servers_resolve_in_one_pass(connect):
    connect.add("down")

    _start({"knowledge": KNOWLEDGE, "down": KNOWLEDGE})

    tools = MCPToolSource().resolve(_spec("knowledge", "down"))
    assert [server.name for server in tools.tools] == ["knowledge"]
    assert tools.unavailable == ("down",)
    assert "`down`" in tools.notice


def test_a_lone_server_name_is_not_read_as_characters(connect):
    _start({"knowledge": KNOWLEDGE})

    spec = InvocableSpec(
        name="Researcher",
        kind=InvocableKind.AGENT,
        engine="openai-agents",
        metadata={MCP_SERVER_NAMES_KEY: "knowledge"},
    )

    tools = MCPToolSource().resolve(spec)
    assert [server.name for server in tools.tools] == ["knowledge"]
    assert tools.unavailable == ()


def test_a_declared_name_that_is_not_a_string_is_reported_missing(connect):
    _start({"knowledge": KNOWLEDGE})

    spec = InvocableSpec(
        name="Researcher",
        kind=InvocableKind.AGENT,
        engine="openai-agents",
        metadata={MCP_SERVER_NAMES_KEY: [None, 5]},
    )

    tools = MCPToolSource().resolve(spec)
    assert tools.tools == ()
    assert tools.unavailable == ("None", "5")


def test_the_banner_is_empty_when_nothing_is_missing():
    assert mcp_status_banner([]) == ""


def test_the_banner_names_every_missing_server():
    assert mcp_status_banner(["knowledge"]) == BANNER
    assert "`a`, `b`" in mcp_status_banner(["a", "b"])


def test_v1_agent_prompt_is_untouched_while_its_server_is_up(connect):
    _start({"knowledge": KNOWLEDGE})

    agent = Researcher.build()

    assert agent.instructions == INSTRUCTIONS  # byte-identical, so prompt caches keep hitting
    assert [server.name for server in agent.mcp_servers] == ["knowledge"]


def test_v1_agent_prompt_carries_the_banner_while_its_server_is_down(connect):
    connect.add("knowledge")
    _start({"knowledge": KNOWLEDGE})

    agent = Researcher.build()

    assert agent.instructions == BANNER + INSTRUCTIONS
    assert agent.mcp_servers == []


def test_the_mcp_client_is_banned_outside_this_adapter(tmp_path):
    """The import law: only ``adapters/tools/mcp`` may import ``agents.mcp``.

    Enforced by ruff's banned-api, not import-linter, which rejects a subpackage of an
    external package outright — so the guard is worth pinning where it can't rot silently.
    """
    repo_root = Path(__file__).resolve().parents[1]
    offender = tmp_path / "another_engine.py"
    offender.write_text("from agents.mcp import MCPServer\n")

    ruff = Path(sys.executable).parent / "ruff"
    result = subprocess.run(
        [str(ruff), "check", "--config", "pyproject.toml", "--no-cache", str(offender)],
        capture_output=True,
        text=True,
        cwd=repo_root,
        timeout=120,
    )

    assert "TID251" in result.stdout
    assert "`agents.mcp` is banned" in result.stdout


def test_the_v1_import_paths_re_export_the_relocated_objects():
    """``agentdeck.agents.mcp`` is a shim: same objects, so patching one patches both."""
    import agentdeck.agents as v1_agents
    import agentdeck.agents.mcp as v1_mcp

    assert v1_mcp.MCPLifecycle is MCPLifecycle
    assert v1_agents.MCPLifecycle is MCPLifecycle
    assert v1_mcp.mcp_status_banner is mcp_status_banner
    assert v1_mcp.MCPServerStreamableHttpResilient is MCPServerStreamableHttpResilient
    assert v1_mcp.load_mcp_config is load_mcp_config
