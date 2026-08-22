"""``Deck``: the v3 composition root. One test per "Done when" item in #164's 4d slice  -
``Deck.asgi()`` and the golden-wire invariants are covered in 4e; this file is the Python API.
"""

from __future__ import annotations

import asyncio
import contextlib
import importlib
import json
import socket
import sys
import textwrap
from dataclasses import replace
from typing import TYPE_CHECKING, Any

import pytest
from agents import Agent as SDKAgent
from agents import WebSearchTool, function_tool
from pydantic import BaseModel

from agentdeck import WorkflowCtx, workflow
from agentdeck.adapters.stores.memory import MemoryEventStore
from agentdeck.adapters.tools.mcp.lifecycle import MCPLifecycle
from agentdeck.authoring import Agent
from agentdeck.core.content import coerce_input
from agentdeck.core.context import RunContext  # noqa: TC001  -  the node below resolves it at runtime
from agentdeck.core.control import Signal
from agentdeck.core.events import RunStarted
from agentdeck.core.status import RunStatus
from agentdeck.deck import _CLOSE_ATTEMPTS, _CLOSE_GRACE, Deck, TurnResult, _new_context, _turn_result
from agentdeck.errors import (
    ConfigError,
    DuplicateKeyError,
    NotFoundError,
    RunStateError,
    RunSuspendedError,
    SessionBusyError,
    UnsupportedControlError,
)
from agentdeck.mcp import MCP
from agentdeck.skills import Skills
from agentdeck.testing import ScriptedModel, patch_model

if TYPE_CHECKING:
    from agentdeck.authoring.native import NativeDefinition


def _greeter(name: str = "Greeter", **kwargs: Any) -> Agent:
    return Agent(name=name, instructions="Greet the user.", **kwargs)


def _hand_the_process_on() -> None:
    """One Deck holds the process at a time; a handful of cases below compare two catalogs
    built back to back. They open neither deck, so there is no `aclose()` to release it."""
    Deck._release()


@pytest.fixture
def scripted():
    """Patches the model provider every real agent run in this file plays against, so a turn
    through the Runtime never reaches for a real endpoint."""
    model = ScriptedModel(deltas=["hi"])
    with patch_model(model):
        yield model


@pytest.fixture
def mcp_connect(monkeypatch):
    """Connects every MCP server the resilient transport builds without opening a socket;
    returns a `refuse.add(name)` switch to make one server's connect fail instead."""
    from agentdeck.adapters.tools.mcp import MCPServerStreamableHttpResilient

    refuse: set[str] = set()

    async def _connect(self: Any) -> None:
        if self.name in refuse:
            raise PermissionError(f"{self.name}: no")  # not transient  -  no retry backoff to wait out

    async def _cleanup(self: Any) -> None:
        return None

    monkeypatch.setattr(MCPServerStreamableHttpResilient, "connect", _connect)
    monkeypatch.setattr(MCPServerStreamableHttpResilient, "cleanup", _cleanup)
    return refuse


def _shout_workflow(name: str = "Shout") -> NativeDefinition:
    async def shout(ctx: WorkflowCtx, input: str = "") -> dict[str, str]:
        return {"input": input, "shouted": input.upper()}

    return workflow(shout, name=name)


def _approval_workflow(name: str = "Approval") -> NativeDefinition:
    """Parks on ``ctx.ask`` and continues on the next line once answered  -  the shape every
    pending/answer case below drives."""

    async def approval(ctx: WorkflowCtx, request: str = "") -> dict[str, str]:
        decision = await ctx.ask(request, options=["yes", "no"])
        return {"request": request, "decision": decision, "outcome": f"yes:{request}" if decision == "yes" else "no"}

    return workflow(approval, name=name)


def _write_skill(root, dirname: str, *, description: str = "does a thing") -> None:
    skill_dir = root / dirname
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "\n".join(["---", f"name: {dirname}", f"description: {description}", "---", "Body."])
    )


@pytest.fixture(autouse=True)
def _reset_mcp_lifecycle():
    """Every test here starts from a clean process-wide registry  -  it's shared state."""
    MCPLifecycle.reset()
    yield
    MCPLifecycle.reset()


@pytest.fixture
def no_project(tmp_path, monkeypatch):
    """A cwd with no ``.agentdeck`` at all  -  proves the code-first constructor needs none."""
    monkeypatch.chdir(tmp_path)


# --- Deck(...) builds and runs an agent, with no ./.agentdeck on disk -----------------------


@pytest.mark.asyncio
async def test_deck_builds_and_runs_an_agent_with_no_project_on_disk(no_project, scripted):
    deck = Deck(agents=[_greeter()])
    deck.build()

    async with deck:
        result = await deck.run("Greeter", "hi there")

    assert isinstance(result, TurnResult)
    assert result.output == "hi"


@pytest.mark.asyncio
async def test_deck_runs_a_workflow_with_no_project_on_disk(no_project):
    deck = Deck(workflows=[_shout_workflow()])
    deck.build()

    async with deck:
        result = await deck.run("Shout", "hi")

    assert result == {"input": "hi", "shouted": "HI"}


# --- Deck.from_project() produces an equivalent deck from the directory layout --------------


def test_from_project_matches_the_equivalent_code_first_deck(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(
        textwrap.dedent("""
        from agentdeck.authoring import Agent

        greeter = Agent(name="Greeter", instructions="Greet the user.")
        """)
    )
    (root / "workflows" / "shout").mkdir(parents=True)
    (root / "workflows" / "shout" / "workflow.py").write_text(
        textwrap.dedent("""
        from agentdeck import WorkflowCtx, workflow

        @workflow(name="Shout")
        async def shout(ctx: WorkflowCtx, input: str = "") -> dict[str, str]:
            return {"input": input, "shouted": input.upper()}
        """)
    )
    _write_skill(root / "skills", "booking", description="Books things.")
    monkeypatch.chdir(tmp_path)

    from_project = Deck.from_project()
    _hand_the_process_on()
    code_first = Deck(agents=[_greeter()], workflows=[_shout_workflow()], skills=Skills(root / "skills"))

    def agent_shape(agent: Agent) -> tuple[str, str, tuple[str, ...], tuple[str, ...]]:
        # Same catalog, not just the same names: instructions plus every tool/handoff name,
        # so a `from_project()` that discovered the right name with the wrong body still fails.
        tools = tuple(getattr(t, "name", getattr(t, "__name__", str(t))) for t in agent.tools)
        handoffs = tuple(h if isinstance(h, str) else h.name for h in agent.handoffs)
        return agent.name, agent.instructions, tools, handoffs

    def catalog(
        deck: Deck,
    ) -> tuple[frozenset[tuple[str, str, tuple[str, ...], tuple[str, ...]]], frozenset[str], frozenset[str]]:
        return (
            frozenset(agent_shape(a) for a in deck.agents.values()),
            frozenset(deck.workflows),
            frozenset(deck.skills.build()),
        )

    assert (
        catalog(from_project)
        == catalog(code_first)
        == (
            frozenset({("Greeter", "Greet the user.", (), ())}),
            frozenset({"Shout"}),
            frozenset({"booking"}),
        )
    )


# --- one Deck per process -------------------------------------------------------------------


def _write_agent_project(root, *, bundle: str, agent: str):
    """A ``.agentdeck`` under ``root`` holding one agent named ``agent`` in ``agents/<bundle>/``."""
    project = root / ".agentdeck"
    (project / "agents" / bundle).mkdir(parents=True)
    (project / "agents" / bundle / "agent.py").write_text(
        textwrap.dedent(f"""
        from agentdeck.authoring import Agent

        it = Agent(name="{agent}", instructions="Greet the user.")
        """)
    )
    return project


@pytest.mark.asyncio
async def test_a_second_live_deck_is_refused_naming_both_projects(tmp_path):
    """Every project mounts under one module alias, so the second deck used to inherit the
    first's bundles in silence. v3 refuses it instead, and says which two projects collided."""
    alpha = _write_agent_project(tmp_path / "alpha", bundle="greeter", agent="Alpha")
    beta = _write_agent_project(tmp_path / "beta", bundle="greeter", agent="Beta")
    deck = Deck.from_project(alpha)

    with pytest.raises(ConfigError) as refused:
        Deck.from_project(beta)

    message = str(refused.value)
    assert str(alpha) in message
    assert str(beta) in message
    assert "#213" in message  # the deferred multi-deck work, so the refusal is not a dead end
    await deck.aclose()


@pytest.mark.asyncio
async def test_a_refused_second_deck_leaves_the_live_ones_namespace_alone(tmp_path):
    """The refusal must not damage the deck it protects.

    ``from_project`` mounts before it constructs, and mounting evicts the alias' cached
    submodules and repoints it at the new root  -  so refusing inside ``__init__`` alone would
    leave the surviving deck's namespace resolving against a project it does not own. Harmless
    while it only reads live objects, and not harmless the moment a durable workflow resumes and
    re-imports a bundle class. Hence the pre-mount refusal, pinned here.
    """
    alpha = _write_agent_project(tmp_path / "alpha", bundle="greeter", agent="Alpha")
    beta = _write_agent_project(tmp_path / "beta", bundle="greeter", agent="Beta")
    deck = Deck.from_project(alpha)
    deck.build()

    with pytest.raises(ConfigError):
        Deck.from_project(beta)

    alias = sys.modules["agentdeck_project"]
    assert alias.__path__ == [str(alpha)], "the refused project stole the alias"
    reimported = importlib.import_module("agentdeck_project.agents.greeter.agent")
    assert reimported.it.name == "Alpha", "a re-import after the refusal resolved to the wrong project"
    await deck.aclose()


@pytest.mark.parametrize("build_first", [False, True])
@pytest.mark.asyncio
async def test_a_closed_deck_hands_the_process_to_the_next_one(no_project, build_first):
    """Sequential decks are the supported shape, and `aclose()` is reachable from `NEW` and
    `BUILT` alike  -  a deck that was never opened still holds the process until it is closed."""
    first = Deck(agents=[_greeter()])
    if build_first:
        first.build()
    await first.aclose()

    second = Deck(agents=[_greeter(name="Second")])

    assert set(second.build().agents) == {"Second"}


def test_a_construction_that_failed_does_not_hold_the_process(no_project):
    """The claim is the last thing the constructor does: a Deck that raised on its way up never
    took the process, so one bad call cannot poison every later one."""
    with pytest.raises(ConfigError, match="Dup"):
        Deck(agents=[_greeter(name="Dup"), _greeter(name="Dup")])

    assert set(Deck(agents=[_greeter()]).agents) == {"Greeter"}


@pytest.mark.asyncio
async def test_closing_a_stale_deck_leaves_the_live_ones_claim_alone(no_project):
    """A deck only ever releases *its own* claim. The suite reaches this state through the
    internal release hook, since the constructor is what makes it unreachable otherwise."""
    stale = Deck(agents=[_greeter()])
    _hand_the_process_on()
    live = Deck(agents=[_greeter()])

    await stale.aclose()

    with pytest.raises(ConfigError, match="already live"):
        Deck(agents=[_greeter()])
    await live.aclose()


@pytest.mark.asyncio
async def test_a_sequential_deck_reads_its_own_bundles_not_the_previous_projects(tmp_path):
    """#204's own reproduction: two projects whose bundle directories share a name. Rebinding
    the alias parent leaves ``<alias>.agents.greeter.agent`` in ``sys.modules``, so without the
    eviction the second deck imports the first project's file and discovers its agent."""
    alpha = _write_agent_project(tmp_path / "alpha", bundle="greeter", agent="Alpha")
    beta = _write_agent_project(tmp_path / "beta", bundle="greeter", agent="Beta")

    first = Deck.from_project(alpha)
    assert set(first.agents) == {"Alpha"}
    await first.aclose()

    second = Deck.from_project(beta)

    assert set(second.agents) == {"Beta"}
    await second.aclose()


# --- context= is a type on the constructor and a value per run --------------------------------


def test_the_constructor_declares_a_context_type_and_the_run_supplies_the_value():
    """The parameter was deleted in #182 for being accepted and then refused at run time. It is
    back because it now does something: the type it declares is what ``build()`` checks every
    ``ToolCtx[...]`` in the catalog against (``tests/test_context_validation.py``), while the
    value still arrives per run."""
    deck = Deck(agents=[_greeter()], context=object)

    assert deck.build() is deck


@pytest.mark.asyncio
async def test_run_and_stream_accept_a_context_for_an_agent(no_project, scripted):
    """Accepted here because it arrives somewhere: a tool declaring ``ToolCtx[...]`` reads it
    (``tests/test_tool_compilation.py``). An agent with no such tool simply ignores the value."""
    deck = Deck(agents=[_greeter()])
    async with deck:
        assert (await deck.run("Greeter", "hi", context=object())).output == "hi"
        assert [event async for event in deck.stream("Greeter", "hi", context=object())]


# --- skills= and mcp= each accept a bare path and a capability object -----------------------


def test_skills_coercion_accepts_a_bare_path_and_a_capability_object(tmp_path):
    _write_skill(tmp_path, "booking")

    from_path = Deck(skills=tmp_path)
    _hand_the_process_on()
    from_object = Deck(skills=Skills(tmp_path))

    assert set(from_path.skills.build()) == set(from_object.skills.build()) == {"booking"}


def test_mcp_coercion_accepts_a_bare_path_and_a_capability_object(tmp_path):
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(json.dumps({"mcpServers": {"docs": {"url": "http://host/mcp"}}}))

    from_path = Deck(mcp=mcp_json)
    _hand_the_process_on()
    from_object = Deck(mcp=MCP(mcp_json))

    from_path.build()
    from_object.build()
    # `mcp` is deliberately not a public property (only agents/workflows/skills/settings are  -
    # see the module docstring); build() succeeding without a "no mcp= configured" ConfigError
    # is itself proof the bare path coerced into a working MCP the same as the object did.


# --- root-name collisions and unknown-name references all fail build() ---------------------


def test_two_agents_sharing_a_name_raise_at_construction():
    """`{a.name: a for a in agents}` would collapse a duplicate to whichever came last with
    no error  -  the same silent shadow the discovery path already refuses."""
    with pytest.raises(ConfigError, match="Dup"):
        Deck(agents=[_greeter(name="Dup"), _greeter(name="Dup")])


def test_two_workflows_sharing_a_name_raise_at_construction():
    with pytest.raises(ConfigError, match="Dup"):
        Deck(workflows=[_shout_workflow(name="Dup"), _shout_workflow(name="Dup")])


def test_agent_and_workflow_sharing_a_name_fails_build_naming_both():
    deck = Deck(agents=[_greeter(name="Twin")], workflows=[_shout_workflow(name="Twin")])

    # a bare `match="Twin"` would still pass a regression to a message naming only one kind  -
    # pin that both are named, not just that the shared name appears somewhere in the text.
    with pytest.raises(ConfigError, match=r"agent.*Twin.*workflow|workflow.*Twin.*agent"):
        deck.build()


def test_unknown_skill_name_fails_build(tmp_path):
    _write_skill(tmp_path, "booking")
    deck = Deck(agents=[_greeter(skills=["not-configured"])], skills=tmp_path)

    with pytest.raises(ConfigError, match="not-configured"):
        deck.build()


def test_declaring_skills_with_no_skills_configured_at_all_fails_build():
    deck = Deck(agents=[_greeter(skills=["booking"])])

    with pytest.raises(ConfigError, match="no skills="):
        deck.build()


def test_unknown_mcp_name_fails_build(tmp_path):
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(json.dumps({"mcpServers": {"docs": {"url": "http://host/mcp"}}}))
    deck = Deck(agents=[_greeter(mcp=["not-configured"])], mcp=mcp_json)

    with pytest.raises(ConfigError, match="not-configured"):
        deck.build()


def test_declaring_mcp_with_no_mcp_configured_at_all_fails_build():
    deck = Deck(agents=[_greeter(mcp=["calendar"])])

    with pytest.raises(ConfigError, match="no mcp="):
        deck.build()


# --- a plain callable in tools= is compiled, and one that cannot be is refused loudly ------
# (#172 rejected every bare callable; #166 makes one the canonical declaration, because a
# callable annotated ToolCtx[...] cannot be pre-decorated without leaking that parameter)


def test_agent_tool_that_is_a_bare_named_function_is_compiled():
    def lookup(q: str) -> str:
        """Look something up."""
        return q

    deck = Deck(agents=[_greeter(tools=[lookup])])
    deck.build()

    (tool,) = deck._invocables["Greeter"].native.tools
    assert tool.name == "lookup"
    assert sorted(tool.params_json_schema["properties"]) == ["q"]


def test_agent_tool_that_is_a_bare_lambda_is_compiled_under_its_own_unhelpful_name():
    """Pinned rather than policed: a lambda compiles, and the model is shown ``<lambda>``.
    Naming a tool well is the author's job, not something ``build()`` refuses over."""
    deck = Deck(agents=[_greeter(tools=[lambda q: q])])
    deck.build()

    (tool,) = deck._invocables["Greeter"].native.tools
    assert tool.name == "<lambda>"


def test_agent_tool_whose_signature_cannot_be_read_fails_build_naming_both():
    """A decorator that dropped ``functools.wraps`` leaves nothing to build a schema from  -  and
    nothing that could rule out a ``ToolCtx[...]`` parameter either, so compiling it anyway would
    drop that argument at the first call."""

    def destroying(fn):
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)

        return wrapper

    @destroying
    def lookup(q: str) -> str:
        return q

    deck = Deck(agents=[_greeter(tools=[lookup])])

    with pytest.raises(ConfigError, match="Greeter") as exc_info:
        deck.build()
    message = str(exc_info.value)
    assert "wrapper" in message  # names the callable it was actually handed
    assert "signature could not be read" in message


def test_agent_tool_that_is_not_callable_at_all_fails_build():
    deck = Deck(agents=[_greeter(tools=["find_slots"])])

    with pytest.raises(ConfigError, match="Greeter") as exc_info:
        deck.build()
    assert "neither a callable nor an Agents SDK tool object" in str(exc_info.value)


def test_a_raw_sdk_agent_in_the_catalog_is_refused_at_construction():
    """agentdeck #451: a raw SDK agent has a ``.name``, so the catalog admitted it and ``build()``
    died on ``.skills``. It is legitimate as a handoff target, which is where the refusal points."""
    raw = SDKAgent(name="raw", instructions="hi")

    with pytest.raises(ConfigError, match="raw") as exc_info:
        Deck(agents=[raw])
    assert "handoffs=" in str(exc_info.value)


def test_agent_tool_wrapped_with_function_tool_builds_cleanly():
    @function_tool
    def lookup(q: str) -> str:
        """Look something up."""
        return q

    deck = Deck(agents=[_greeter(tools=[lookup])])

    deck.build()  # no raise


def test_agent_tool_that_is_a_hosted_sdk_tool_builds_cleanly():
    """A ``FunctionTool``-only check would reject this  -  pins that the structural check
    accepts any SDK tool object, not just the one built by ``@function_tool``."""
    deck = Deck(agents=[_greeter(tools=[WebSearchTool()])])

    deck.build()  # no raise


def test_build_is_idempotent(tmp_path):
    _write_skill(tmp_path, "booking")
    deck = Deck(agents=[_greeter(skills=["booking"])], skills=tmp_path)

    deck.build()
    same_deck = deck.build()  # a second call must not re-validate, re-compile, or raise

    assert same_deck is deck


# --- build() performs no network I/O and starts no MCP server, asserted --------------------


def test_build_starts_no_mcp_server(monkeypatch, tmp_path):
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(json.dumps({"mcpServers": {"docs": {"url": "http://host/mcp"}}}))

    def _refuse(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("MCPLifecycle.startup must not run during build()")

    monkeypatch.setattr(MCPLifecycle, "startup", _refuse)
    deck = Deck(agents=[_greeter(mcp=["docs"])], mcp=mcp_json)

    deck.build()  # must not touch the patched startup at all


def test_build_touches_no_network(monkeypatch, no_project):
    def _refuse_connect(*_a: Any, **_k: Any) -> Any:
        raise AssertionError("build() must not open a network connection")

    monkeypatch.setattr(socket.socket, "connect", _refuse_connect)
    deck = Deck(agents=[_greeter()], workflows=[_shout_workflow()])

    deck.build()  # a raised AssertionError from the patch would fail this test, not pass it


# --- mutating the catalog after build() raises ----------------------------------------------


def test_mutating_the_catalog_after_build_raises():
    deck = Deck(agents=[_greeter()])
    deck.build()

    with pytest.raises(TypeError):
        deck.agents["Intruder"] = _greeter(name="Intruder")


# --- CLOSED is terminal: reopening a closed Deck raises rather than leaking resources -------


@pytest.mark.asyncio
async def test_reopening_a_closed_deck_raises(no_project, scripted):
    """Silently reopening would build a second runtime/store/engine set and start MCP again,
    while the eventual `aclose()` sees the stale `_closed` guard and skips draining or closing
    any of it  -  a leak dressed up as reuse."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        await deck.run("Greeter", "hi")

    with pytest.raises(ConfigError, match="already closed"):
        async with deck:
            pass


# --- run/stream before OPEN raise -------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_before_open_raises(no_project):
    deck = Deck(agents=[_greeter()])
    deck.build()

    with pytest.raises(ConfigError, match="not open"):
        await deck.run("Greeter", "hi")


@pytest.mark.asyncio
async def test_an_unknown_root_name_is_a_not_found_error(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        with pytest.raises(NotFoundError, match="No agent or workflow named 'unknown'"):
            await deck.run("unknown", "hi")


@pytest.mark.asyncio
async def test_stream_before_open_raises(no_project):
    deck = Deck(agents=[_greeter()])
    deck.build()

    with pytest.raises(ConfigError, match="not open"):
        async for _ in deck.stream("Greeter", "hi"):
            pass


@pytest.mark.asyncio
async def test_runs_collection_ops_after_close_raise_not_open(no_project):
    """``deck.runs.start/get/list`` all forward through ``_require_open()``  -  a closed deck
    raises the same ``ConfigError`` every other op does."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        pass

    with pytest.raises(ConfigError, match="not open"):
        await deck.runs.start("Greeter", "hi")
    with pytest.raises(ConfigError, match="not open"):
        await deck.runs.get("r-1")
    with pytest.raises(ConfigError, match="not open"):
        await deck.runs.list()


@pytest.mark.asyncio
async def test_run_handle_ops_after_close_raise_not_open(no_project, scripted):
    """A ``Run`` held from before the deck closed still forwards every op through
    ``_require_open()``  -  a handle outlives the deck it came from, but not what it can do."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        run = await deck.runs.start("Greeter", "hi")

    with pytest.raises(ConfigError, match="not open"):
        await run.status()
    with pytest.raises(ConfigError, match="not open"):
        await run.pause("operator stepped away")
    with pytest.raises(ConfigError, match="not open"):
        await run.resume()
    with pytest.raises(ConfigError, match="not open"):
        await run.cancel()
    with pytest.raises(ConfigError, match="not open"):
        await run.pending()
    with pytest.raises(ConfigError, match="not open"):
        await run.answer("yes")
    with pytest.raises(ConfigError, match="not open"):
        [event async for event in run.events()]
    with pytest.raises(ConfigError, match="not open"):
        await run


# --- opening a Deck wires the MCP servers build() only resolved as unconnected (#173) -------
#
# build() compiles every agent's mcp= against MCPLifecycle before __aenter__ has connected
# anything, so the first resolution always reads every declared server as missing. Without a
# second pass at open time, the agent that actually runs a turn is stuck with the servers it
# read as missing at build time, and its instructions falsely carry the "UNAVAILABLE" banner
# even once the server is up.


def _write_mcp_json(tmp_path) -> Any:
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(json.dumps({"mcpServers": {"calendar": {"url": "http://host/mcp"}}}))
    return mcp_json


@pytest.mark.asyncio
@pytest.mark.parametrize("build_first", [False, True], ids=["direct-open", "build-then-open"])
async def test_opening_a_deck_wires_its_connected_mcp_server_into_the_compiled_agent(
    no_project, tmp_path, mcp_connect, build_first
):
    deck = Deck(agents=[_greeter(mcp=["calendar"])], mcp=_write_mcp_json(tmp_path))
    if build_first:
        deck.build()  # the documented pattern: validate early, open later  -  must not stick

    async with deck:
        compiled = deck._invocables["Greeter"].native
        assert [server.name for server in compiled.mcp_servers] == ["calendar"]
        assert compiled.instructions == "Greet the user."  # no banner: the server is up


@pytest.mark.asyncio
async def test_a_refused_mcp_connect_still_leaves_the_degraded_banner_after_open(no_project, tmp_path, mcp_connect):
    mcp_connect.add("calendar")
    deck = Deck(agents=[_greeter(mcp=["calendar"])], mcp=_write_mcp_json(tmp_path))

    async with deck:
        compiled = deck._invocables["Greeter"].native
        assert compiled.mcp_servers == []
        assert "UNAVAILABLE: `calendar`" in compiled.instructions


@pytest.mark.asyncio
async def test_mcp_refresh_keeps_a_skills_disclosure_intact(no_project, tmp_path, mcp_connect):
    _write_skill(tmp_path, "booking")
    reference = Deck(agents=[_greeter(skills=["booking"])], skills=tmp_path)
    reference.build()
    expected_instructions = reference._invocables["Greeter"].native.instructions

    _hand_the_process_on()
    deck = Deck(agents=[_greeter(skills=["booking"], mcp=["calendar"])], skills=tmp_path, mcp=_write_mcp_json(tmp_path))
    deck.build()
    stale = deck._invocables["Greeter"].native.instructions
    assert stale != expected_instructions  # the banner is baked in before anything connects
    assert "UNAVAILABLE" in stale

    async with deck:
        refreshed = deck._invocables["Greeter"].native.instructions

    assert refreshed == expected_instructions  # banner gone, disclosure intact either way


def test_build_logs_no_false_not_found_warning_for_a_configured_mcp_server(tmp_path, caplog):
    """Before the fix, resolving a genuinely-configured server before any server connects
    fell back to a bare, un-configured ``MCPLifecycle`` and logged this exact line  -  the
    silent-drop wording #173 flags, even though the server is not, in fact, missing."""
    deck = Deck(agents=[_greeter(mcp=["calendar"])], mcp=_write_mcp_json(tmp_path))

    with caplog.at_level("WARNING"):
        deck.build()

    assert not any("not found in config" in record.message for record in caplog.records)


# --- ownership: a deck closes an MCP(...) it opened, never a store passed in ----------------


@pytest.mark.asyncio
async def test_deck_closes_the_mcp_it_opened(no_project, monkeypatch, tmp_path, scripted):
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(json.dumps({"mcpServers": {}}))
    shutdown_calls = []

    async def _spy_shutdown() -> None:
        shutdown_calls.append(1)

    monkeypatch.setattr(MCPLifecycle, "shutdown", staticmethod(_spy_shutdown))
    deck = Deck(agents=[_greeter()], mcp=mcp_json)

    async with deck:
        await deck.run("Greeter", "hi")

    assert shutdown_calls == [1]


@pytest.mark.asyncio
async def test_deck_does_not_close_a_store_passed_in(no_project, scripted):
    from agentdeck.adapters.stores.memory import MemoryEventStore

    class _SpyStore(MemoryEventStore):
        def __init__(self) -> None:
            super().__init__()
            self.aclose_calls = 0

        async def aclose(self) -> None:
            self.aclose_calls += 1

    store = _SpyStore()
    deck = Deck(agents=[_greeter()], _store=store)

    async with deck:
        await deck.run("Greeter", "hi")

    assert store.aclose_calls == 0


@pytest.mark.asyncio
async def test_deck_closes_a_store_it_built_itself(no_project, monkeypatch, scripted):
    """The other half of the ownership rule: dropping `_owns_store`'s branch entirely would
    still pass every other test here, since none of them build a store worth spying on."""
    from agentdeck.adapters.stores.memory import MemoryEventStore

    class _SpyStore(MemoryEventStore):
        def __init__(self) -> None:
            super().__init__()
            self.aclose_calls = 0

        async def aclose(self) -> None:
            self.aclose_calls += 1

    store = _SpyStore()
    monkeypatch.setattr("agentdeck.deck.resolve_event_store", lambda: store)
    deck = Deck(agents=[_greeter()])

    async with deck:
        await deck.run("Greeter", "hi")

    assert store.aclose_calls == 1


# --- the private `_executors=` seam exists and is exercised -----------------------------------


def test_engines_seam_restricts_which_engines_a_catalog_may_use(no_project):
    """A private, test-only override (never in the documented constructor, same as the
    Runtime's own ``tests/contract/`` seam): naming only the stub engine means an ordinary
    ``Agent``  -  which needs "openai-agents"  -  fails ``build()`` instead of silently compiling."""
    deck = Deck(agents=[_greeter()], _executors=("stub",))

    with pytest.raises(ConfigError, match="openai-agents"):
        deck.build()


def test_engines_seam_accepts_the_matching_default_engines(no_project):
    """The same seam, given the real default engine names, builds exactly like the default
    constructor  -  proving the restriction above comes from the *set*, not the seam itself."""
    from agentdeck.adapters.executors.native import NativeExecutor
    from agentdeck.adapters.executors.openai_agents import OpenAIAgentsExecutor

    deck = Deck(agents=[_greeter()], _executors=(OpenAIAgentsExecutor.name, NativeExecutor.name))

    deck.build()  # no raise


# --- asgi() opens and closes through the ASGI lifespan --------------------------------------


def test_asgi_opens_and_closes_the_deck_through_the_lifespan(no_project, scripted):
    from fastapi.testclient import TestClient

    deck = Deck(agents=[_greeter()])
    api = deck.asgi()

    assert deck._state == "NEW"
    with TestClient(api) as client:
        assert deck._state == "OPEN"
        response = client.post("/agents/Greeter/chat", json={"session_id": "s", "message": "hi"})
        assert response.status_code == 200
    assert deck._state == "CLOSED"


def test_asgi_health_reflects_this_decks_catalog(no_project, tmp_path):
    from fastapi.testclient import TestClient

    _write_skill(tmp_path, "booking")
    deck = Deck(agents=[_greeter()], workflows=[_shout_workflow()], skills=tmp_path)

    with TestClient(deck.asgi()) as client:
        response = client.get("/health")

    assert response.json() == {
        "status": "ok",
        "agents": ["Greeter"],
        "workflows": ["Shout"],
        "skills": ["booking"],
    }


# --- v1's convenience carried across as `run`/`stream`, behave the same on Deck ------------


def _reader_ctx(session_id: str | None) -> RunContext:
    """A throwaway context of the Deck's own namespace, for reading its log back in a test  -
    exactly what ``serve.py``'s compat routes build for an HTTP request."""
    return RunContext(run_id="reader", session_id=session_id)


@pytest.mark.asyncio
async def test_run_is_recorded_and_returns_a_turn_result(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        result = await deck.run("Greeter", "hello")
        assert result.output == "hi"
        assert result.session_id is None
        events = await deck._runtime.store.read_run(replace(_reader_ctx(None), run_id=result.run_id))

    assert [event.kind for event in events] == [
        "run.started",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


@pytest.mark.asyncio
async def test_run_with_a_session_id_is_recorded_and_returns_a_turn_result(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        result = await deck.run("Greeter", "hello", session_id="s1")
        assert result.output == "hi"
        assert result.session_id == "s1"
        events = await deck._runtime.store.read_session(_reader_ctx("s1"))

    assert [event.kind for event in events] == [
        "run.started",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


@pytest.mark.asyncio
async def test_a_failed_run_still_leaves_run_failed_in_the_log(no_project):
    """A run that raises is still written down, even though nobody read the stream to the end
    by hand."""
    deck = Deck(agents=[_greeter()])

    with patch_model(ScriptedModel(deltas=("par",), raises=RuntimeError("boom"))):
        async with deck:
            with pytest.raises(RuntimeError, match="boom"):
                await deck.run("Greeter", "hello", session_id="s1")
            events = await deck._runtime.store.read_session(_reader_ctx("s1"))

    assert [event.kind for event in events] == ["run.started", "text.delta", "run.failed"]


class _Greeting(BaseModel):
    greeting: str


@pytest.mark.asyncio
async def test_a_structured_run_output_survives_as_validated_data(no_project):
    """An ``output_type`` result rides ``run.completed`` as a ``DataBlock``, and ``run``'s
    ``TurnResult`` must surface it as data rather than joining it as text."""
    deck = Deck(agents=[Agent(name="Structured", instructions="Answer as JSON.", output_type=_Greeting)])

    with patch_model(ScriptedModel(deltas=('{"greeting": "hi"}',))):
        async with deck:
            result = await deck.run("Structured", "hello", session_id="s1")

    assert result.output == {"greeting": "hi"}


@pytest.mark.asyncio
async def test_stream_yields_canonical_events_and_is_recorded(no_project):
    from agentdeck.core.events import RunCompleted, TextDelta

    deck = Deck(agents=[_greeter()])

    with patch_model(ScriptedModel(deltas=("Hel", "lo"))):
        async with deck:
            events = [event async for event in deck.stream("Greeter", "hello", session_id="s1")]

            assert [event.kind for event in events] == [
                "run.started",
                "text.delta",
                "text.delta",
                "usage.reported",
                "message.completed",
                "run.completed",
            ]
            assert "".join(e.payload.text for e in events if isinstance(e.payload, TextDelta)) == "Hello"
            assert next(e for e in events if isinstance(e.payload, RunCompleted)).payload.output[0].text == "Hello"

            # the stream itself already recorded every one of those events; store.read proves it
            # rather than the caller having to trust stream's own bookkeeping
            stored = await deck._runtime.store.read_session(_reader_ctx("s1"))

    assert [event.kind for event in stored] == [event.kind for event in events]


@pytest.mark.asyncio
async def test_an_abandoned_stream_leaves_execution_running_to_completion(no_project):
    """A caller that stops reading ``stream()`` only stops *watching*  -  the run itself is a
    deck-owned task from the moment ``_start`` returns it, and keeps executing to its own
    natural end regardless of whether anybody is still attached to observe it
    (docs/design/run-identity.md §9). This used to be the opposite: closing ``stream()``'s own
    frame closed the Runtime's generator underneath it, which is exactly the coupling between
    observing and executing this design removes.
    """
    deck = Deck(agents=[_greeter()])

    with patch_model(ScriptedModel(deltas=("Hel", "lo"))):
        async with deck:
            stream = deck.stream("Greeter", "hello", session_id="s1")
            first = await anext(stream)
            second = await anext(stream)
            await stream.aclose()
            # Deterministic settlement, not a sleep-and-hope: the execution task itself is the
            # signal that this segment is over.
            execution = deck._executions.get(first.run_id)
            if execution is not None:
                await execution[1]
            events = await deck._runtime.store.read_session(_reader_ctx("s1"))

    assert (first.kind, second.kind) == ("run.started", "text.delta")
    assert [event.kind for event in events] == [
        "run.started",
        "text.delta",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


# --- execution ownership: one owner, any number of observers (docs/design/run-identity.md §9,
# §14's "Execution ownership" matrix) --------------------------------------------------------


@pytest.mark.asyncio
async def test_two_observers_of_one_run_both_see_every_event_and_it_executes_once(no_project):
    """Neither observer drives the run: two independent readers of one run's events both see
    everything it produced, and the model was called exactly once."""
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)
    deck = Deck(agents=[_greeter()])

    async def _collect(events: Any) -> list[Any]:
        return [event async for event in events]

    with patch_model(model):
        async with deck:
            opening, task = await deck._start("Greeter", coerce_input("hello"), session_id="s1")
            ctx = RunContext(run_id=opening.run_id, session_id=opening.session_id, namespace=opening.namespace)

            watcher_a = asyncio.create_task(_collect(deck._events(ctx)))
            watcher_b = asyncio.create_task(_collect(deck._events(ctx)))
            # Caught mid-turn, inside its own await for the next event  -  the same window a
            # second observer would attach in for real  -  before letting it finish.
            await model.holding.wait()
            hold.set()
            events_a, events_b = await asyncio.gather(watcher_a, watcher_b)
            await task
            logged = await deck._runtime.store.read_session(_reader_ctx("s1"))

    expected = ["run.started", "text.delta", "text.delta", "usage.reported", "message.completed", "run.completed"]
    assert [e.kind for e in events_a] == expected
    assert [e.kind for e in events_b] == expected
    assert [e.kind for e in logged] == expected
    assert model.calls == 1


@pytest.mark.asyncio
async def test_events_replay_in_full_for_a_run_that_already_finished(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        result = await deck.run("Greeter", "hello", session_id="s1")
        ctx = RunContext(run_id=result.run_id, session_id="s1")
        replayed = [event async for event in deck._events(replace(ctx, run_id=result.run_id))]

    assert [e.kind for e in replayed] == [
        "run.started",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


@pytest.mark.asyncio
async def test_a_second_handle_awaits_the_same_result_as_the_first(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        result = await deck.run("Greeter", "hello", session_id="s1")
        ctx = RunContext(run_id=result.run_id, session_id="s1")
        second = await _turn_result(deck._events(replace(ctx, run_id=result.run_id)))

    assert second == result


@pytest.mark.asyncio
async def test_a_run_recovered_by_another_deck_can_be_observed_and_awaited(no_project, scripted):
    """What a second process sees: no locally-owned execution task at all, only the store this
    Deck shares with whichever one ran the turn  -  the ``get()``-recovered row of the
    execution-ownership matrix, modelled here as a second ``Deck`` over the first one's store."""
    store = MemoryEventStore()
    deck_a = Deck(agents=[_greeter()], _store=store)
    async with deck_a:
        result = await deck_a.run("Greeter", "hello", session_id="s1")

    deck_b = Deck(agents=[_greeter()], _store=store)
    async with deck_b:
        assert deck_b._executions == {}  # this "process" never started the run
        ctx = RunContext(run_id=result.run_id, session_id="s1")
        recovered = await _turn_result(deck_b._events(replace(ctx, run_id=result.run_id)))

    assert recovered == result


@pytest.mark.asyncio
async def test_deck_run_and_start_then_await_produce_identical_logs(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        via_run = await deck.run("Greeter", "hello", session_id="s1")
        logged_via_run = await deck._runtime.store.read_session(_reader_ctx("s1"))

        opening, task = await deck._start("Greeter", coerce_input("hello"), session_id="s2")
        await task
        ctx = RunContext(run_id=opening.run_id, session_id="s2")
        via_start = await _turn_result(deck._events(replace(ctx, run_id=opening.run_id)))
        logged_via_start = await deck._runtime.store.read_session(_reader_ctx("s2"))

    assert [e.kind for e in logged_via_run] == [e.kind for e in logged_via_start]
    assert via_run.output == via_start.output


@pytest.mark.asyncio
async def test_a_finished_runs_execution_task_is_retired_promptly(no_project, scripted):
    """No leak: the moment a run settles, its entry is gone  -  ``aclose()`` never has anything
    left over from a run nobody is still executing."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        await deck.run("Greeter", "hello", session_id="s1")
        assert deck._executions == {}


@pytest.mark.asyncio
async def test_closing_a_deck_cancels_a_run_still_in_flight_and_says_so(no_project, caplog):
    """``aclose()`` settles or cancels every run it is still executing, and says which
    (docs/design/run-identity.md §9). A run genuinely mid-turn when the deck closes has no
    chance to finish on its own, so it is cancelled, and the Runtime's own
    ``asyncio.CancelledError`` arm is what writes ``run.cancelled`` for it."""
    hold = asyncio.Event()  # never set: this run is held for the deck's whole lifetime
    model = ScriptedModel(deltas=("one", "two"), hold=hold)
    deck = Deck(agents=[_greeter()])

    with patch_model(model), caplog.at_level("INFO"):
        async with deck:
            opening, task = await deck._start("Greeter", coerce_input("hi"), session_id="s1")
            # `holding` is announced one line before the model parks, so a cancel in that same
            # tick reaches a turn the SDK has not suspended yet and is swallowed (#412). Waiting
            # for the delta it wrote first is the observable that the turn is genuinely parked.
            await _wait_until(lambda: _has_delta(deck, "s1"))
            assert not task.done()
        assert task.cancelled()

    assert "run %s was cancelled" not in caplog.text  # the format string, not the rendered line
    assert f"run {opening.run_id} was cancelled" in caplog.text
    events = await deck._runtime.store.read_session(RunContext(run_id="reader", session_id="s1"))
    assert [e.kind for e in events][-1] == "run.cancelled"


@pytest.mark.asyncio
@pytest.mark.timeout(30)
async def test_closing_a_deck_returns_even_when_a_run_never_observes_its_cancellation(no_project, caplog):
    """``aclose()`` asks twice and then stops waiting (#412) rather than betting on the run taking
    a cancellation at all, and writes the abandoned run's own ``run.cancelled`` on the way past:
    left open it would be exactly the ghost state ``stale_run_after`` exists to recover. The task
    stays alive, and the run goes on writing, so no append it starts from there may reach the log
    past that event.
    """

    class _UncancellableStore(MemoryEventStore):
        """A write that does not take a cancellation, which is what a driver call already inside a
        thread is, freed in the moment the abandoning write lands."""

        def __init__(self) -> None:
            super().__init__()
            self.writing = asyncio.Event()
            self.release = asyncio.Event()

        async def append(self, payloads, ctx, origin):
            if any(payload.kind == "run.cancelled" for payload in payloads):
                # Freed here, and not after the close returns, because the turns between the
                # abandoning write and the guard taking hold are the whole window: a run released
                # any later has already lost every chance to append past its own terminal event.
                self.release.set()
            if any(payload.kind == "text.delta" for payload in payloads):
                self.writing.set()
                while not self.release.is_set():
                    with contextlib.suppress(asyncio.CancelledError):
                        await self.release.wait()
            return await super().append(payloads, ctx, origin)

    store = _UncancellableStore()
    deck = Deck(agents=[_greeter()], _store=store)
    loop = asyncio.get_running_loop()
    task: asyncio.Task[None] | None = None

    try:
        with patch_model(ScriptedModel(deltas=("one", "two"))), caplog.at_level("ERROR"):
            async with deck:
                opening, task = await deck._start("Greeter", coerce_input("hi"), session_id="s1")
                await store.writing.wait()
                started = loop.time()
                await deck.aclose()
                elapsed = loop.time() - started

        assert elapsed < _CLOSE_ATTEMPTS * _CLOSE_GRACE + 1
        assert f"run {opening.run_id} ignored its cancellation" in caplog.text
        with contextlib.suppress(BaseException):
            await task
        kinds = [event.kind for event in await store.read_session(_reader_ctx("s1"))]
        # The one write already suspended inside the store when the run was abandoned still lands
        # (#421). Every append the run starts after that is refused, so nothing else follows.
        assert kinds[kinds.index("run.cancelled") + 1 :] == ["text.delta"]
    finally:
        store.release.set()
        if task is not None:
            task.cancel()
            with contextlib.suppress(BaseException):
                await task


@pytest.mark.asyncio
async def test_run_and_stream_share_one_session(no_project):
    """Same guarantee v1 gave: one ``session_id`` is one conversation whichever Deck method ran
    the turn."""
    model = ScriptedModel(deltas=("hi",))
    deck = Deck(agents=[_greeter()])

    with patch_model(model):
        async with deck:
            async for _ in deck.stream("Greeter", "first", session_id="s1"):
                pass
            await deck.run("Greeter", "second", session_id="s1")

    # two model calls, and the second turn's input carries the first turn's history
    assert model.calls == 2
    assert "first" in str(model.inputs[-1])


def test_sessions_keyed_by_id(no_project):
    deck = Deck(agents=[_greeter()])

    assert deck.session_for("a") is deck.session_for("a")
    assert deck.session_for("a") is not deck.session_for("b")


# --- docs/design/run-identity.md §14's test matrix, discharged at the public surface: a
# runtime-level or a private-helper test proves nothing about `deck.runs`/`Run` themselves
# (#314's own lesson, restated for #322). ----------------------------------------------------


@pytest.mark.asyncio
async def test_two_consumers_of_run_events_both_see_every_event_and_it_executes_once(no_project):
    """Execution ownership, through the public surface: two readers of one ``Run``'s
    ``events(follow=True)`` both see everything it produced, and the model was called once."""
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)
    deck = Deck(agents=[_greeter()])

    async def _collect(events: Any) -> list[Any]:
        return [event async for event in events]

    with patch_model(model):
        async with deck:
            run = await deck.runs.start("Greeter", "hello", session_id="s1")

            watcher_a = asyncio.create_task(_collect(run.events(follow=True)))
            watcher_b = asyncio.create_task(_collect(run.events(follow=True)))
            await model.holding.wait()
            hold.set()
            events_a, events_b = await asyncio.gather(watcher_a, watcher_b)
            result = await run

    expected = ["run.started", "text.delta", "text.delta", "usage.reported", "message.completed", "run.completed"]
    assert [e.kind for e in events_a] == expected
    assert [e.kind for e in events_b] == expected
    assert model.calls == 1
    assert result.output == "Hello"


@pytest.mark.asyncio
async def test_a_second_handle_from_get_awaits_the_same_result_as_the_first(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        run = await deck.runs.start("Greeter", "hello", session_id="s1")
        first = await run
        second_handle = await deck.runs.get(run.id, namespace=run.namespace)
        second = await second_handle

    assert second == first


@pytest.mark.asyncio
async def test_run_events_replay_in_full_for_a_run_recovered_after_it_already_finished(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        run = await deck.runs.start("Greeter", "hello", session_id="s1")
        await run
        recovered = await deck.runs.get(run.id)
        replayed = [event async for event in recovered.events()]

    assert [e.kind for e in replayed] == [
        "run.started",
        "text.delta",
        "usage.reported",
        "message.completed",
        "run.completed",
    ]


@pytest.mark.asyncio
async def test_deck_run_and_runs_start_then_await_produce_identical_logs(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        via_run = await deck.run("Greeter", "hello", session_id="s1")
        logged_via_run = await deck._runtime.store.read_session(_reader_ctx("s1"))

        run = await deck.runs.start("Greeter", "hello", session_id="s2")
        via_start = await run
        logged_via_start = await deck._runtime.store.read_session(_reader_ctx("s2"))

    assert [e.kind for e in logged_via_run] == [e.kind for e in logged_via_start]
    assert via_run.output == via_start.output


@pytest.mark.asyncio
async def test_starting_on_a_session_held_running_raises_session_busy(no_project):
    """The ``Session ownership`` row, ``RUNNING`` cell: a second ``runs.start`` on a session
    whose run is still live is refused, naming the run that holds it."""
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)
    deck = Deck(agents=[_greeter()])

    with patch_model(model):
        async with deck:
            running = await deck.runs.start("Greeter", "hello", session_id="s-running")
            await model.holding.wait()
            with pytest.raises(SessionBusyError, match=running.id):
                await deck.runs.start("Greeter", "hello again", session_id="s-running")
            hold.set()
            await running


@pytest.mark.asyncio
async def test_starting_on_a_session_held_paused_raises_session_busy(no_project):
    """The ``PAUSED`` cell of the same row."""
    deck = Deck(agents=[_greeter()])
    async with deck:
        stream = deck.stream("Greeter", "hi there", session_id="s-paused")
        started = await anext(stream)
        run = await deck.runs.get(started.run_id)
        await run.pause("operator stepped away")
        [event async for event in stream]
        assert await run.status() is RunStatus.PAUSED

        with pytest.raises(SessionBusyError, match=run.id):
            await deck.runs.start("Greeter", "hi again", session_id="s-paused")


@pytest.mark.asyncio
async def test_starting_on_a_session_held_waiting_answer_raises_session_busy(no_project, monkeypatch):
    """The ``WAITING_ANSWER`` cell of the same row."""
    async with _parked_approval(monkeypatch) as (deck, parked):
        assert await parked.status() is RunStatus.WAITING_ANSWER
        with pytest.raises(SessionBusyError, match=parked.id):
            await deck.runs.start("Approval", {"request": "wed 3pm"}, session_id="t-1")


@pytest.mark.asyncio
async def test_get_does_not_corrupt_a_session_id_that_equals_its_own_run_id(no_project):
    """The exact case #397 fixes: a caller-chosen ``session_id`` that happens to equal the run's
    own ``run_id``. The old recovery compared the stored key against the run id and returned
    ``None`` for a match, indistinguishable from a run with no session at all."""
    deck = Deck(agents=[_greeter()])
    async with deck:
        ctx = RunContext(run_id="same-id", session_id="same-id")
        await deck._runtime.store.append(
            [RunStarted(invocable="Greeter", kind_of_invocable="agent", input=[])], ctx, "Greeter"
        )
        run = await deck.runs.get("same-id")
        assert run.session_id == "same-id"


@pytest.mark.asyncio
async def test_a_session_named_after_an_open_standalone_runs_id_does_not_collide_with_it(no_project, scripted):
    """5.0's whole point: a standalone run and a session are different keyspaces now. The old
    ``log_key`` encoding made a standalone run's own id its log key too, so a session named
    after it collided with that run  -  the busy message it raised named a session that did not
    exist, held by the very run being refused. Starting on that session now succeeds instead."""
    deck = Deck(agents=[_greeter()])
    async with deck:
        stream = deck.stream("Greeter", "hi there")  # no session_id: a standalone run
        started = await anext(stream)
        standalone = await deck.runs.get(started.run_id)
        await standalone.pause("operator stepped away")
        [event async for event in stream]
        assert await standalone.status() is RunStatus.PAUSED

        run = await deck.runs.start("Greeter", "hi again", session_id=standalone.id)
        assert await run.status() is RunStatus.RUNNING
        await run


@pytest.mark.asyncio
async def test_get_on_a_running_run_and_on_a_completed_one_are_both_usable(no_project):
    """The ``Handles`` row: ``get(id)`` works identically whether the run it names is still
    ``RUNNING`` or already ``COMPLETED``."""
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)  # a second delta is what `hold` stalls on
    deck = Deck(agents=[_greeter()])

    with patch_model(model):
        async with deck:
            live = await deck.runs.start("Greeter", "hello", session_id="s-live")
            await model.holding.wait()  # the turn is genuinely still RUNNING here
            same_live = await deck.runs.get(live.id)
            assert await same_live.status() is RunStatus.RUNNING
            hold.set()
            await live

            done = await deck.runs.start("Greeter", "hello", session_id="s-done")
            await done
            same_done = await deck.runs.get(done.id)
            assert await same_done.status() is RunStatus.COMPLETED


@pytest.mark.asyncio
async def test_get_by_namespace_and_key_resolves_to_the_same_run_as_get_by_id(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        run = await deck.runs.start("Greeter", "hello", session_id="s1", namespace="acme", key="order-1234")
        await run

        by_id = await deck.runs.get(run.id, namespace="acme")
        by_key = await deck.runs.get(namespace="acme", key="order-1234")

    assert by_key.id == by_id.id == run.id


@pytest.mark.asyncio
async def test_two_handles_on_one_run_agree_a_cancel_through_one_is_visible_through_the_other(no_project, monkeypatch):
    async with _parked_approval(monkeypatch) as (deck, run):
        other_handle = await deck.runs.get(run.id)

        await run.cancel("operator said stop")

        assert await other_handle.status() is RunStatus.CANCELLED


@pytest.mark.asyncio
async def test_a_duplicate_start_on_the_same_key_raises_naming_the_holder(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        held = await deck.runs.start("Greeter", "hello", session_id="s1", namespace="acme", key="order-1234")
        await held

        with pytest.raises(DuplicateKeyError, match=held.id):
            await deck.runs.start("Greeter", "hello", session_id="s2", namespace="acme", key="order-1234")


@pytest.mark.asyncio
async def test_different_namespaces_sharing_one_key_are_two_runs_neither_visible_to_the_other(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        acme = await deck.runs.start("Greeter", "hello", session_id="s1", namespace="acme", key="order-1234")
        await acme
        globex = await deck.runs.start("Greeter", "hello", session_id="s1", namespace="globex", key="order-1234")
        await globex

        assert acme.id != globex.id

        by_key_acme = await deck.runs.get(namespace="acme", key="order-1234")
        by_key_globex = await deck.runs.get(namespace="globex", key="order-1234")
        assert by_key_acme.id == acme.id
        assert by_key_globex.id == globex.id

        with pytest.raises(NotFoundError):
            await deck.runs.get(acme.id, namespace="globex")


@pytest.mark.asyncio
async def test_generated_ids_are_unique_across_runs(no_project, scripted):
    deck = Deck(agents=[_greeter()])

    async with deck:
        first = await deck.runs.start("Greeter", "hello", session_id="s1")
        await first
        second = await deck.runs.start("Greeter", "hello", session_id="s2")
        await second

    assert first.id != second.id


@pytest.mark.asyncio
@pytest.mark.parametrize("session_of", ["s-completed", "s-failed"])
async def test_a_terminal_session_admits_a_fresh_start_no_session_busy_error(no_project, session_of):
    """The other half of the Session ownership row: a session whose run ended  -  however it
    ended  -  releases it, and a fresh ``runs.start`` on it succeeds rather than raising."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        if session_of == "s-completed":
            with patch_model(ScriptedModel(deltas=("hi",))):
                run = await deck.runs.start("Greeter", "hello", session_id=session_of)
                await run
        else:
            with patch_model(ScriptedModel(deltas=("par",), raises=RuntimeError("boom"))):
                run = await deck.runs.start("Greeter", "hello", session_id=session_of)
                with pytest.raises(RuntimeError, match="boom"):
                    await run

        with patch_model(ScriptedModel(deltas=("hi again",))):
            resumed = await deck.runs.start("Greeter", "hello again", session_id=session_of)
            await resumed


# The CANCELLED case of the same row is already covered by
# test_a_cancel_against_a_parked_run_ends_it_immediately below: a fresh `deck.run(...)` on the
# same session succeeds once the parked run is cancelled.


# --- run.pending() names what a parked run waits on, run.answer() answers it -------------


@pytest.mark.asyncio
async def test_pending_lists_a_paused_run_and_answer_completes_it(no_project, monkeypatch):
    """``run.answer`` pairs with ``deck.runs.list(status=WAITING_ANSWER)``: a caller lists the
    inbox, gets a handle on one, answers it  -  no name, thread id, or session id by hand."""
    from agentdeck.runtime.settings import reset_settings_cache

    reset_settings_cache()
    try:
        deck = Deck(workflows=[_approval_workflow()])

        async with deck:
            paused = await deck.run("Approval", "tue 9am", session_id="t-1")
            assert paused["type"] == "interrupt"

            [run] = await deck.runs.list(status=RunStatus.WAITING_ANSWER)
            assert run.id  # minted by the Runtime, not supplied by the caller
            pending = await run.pending()
            assert pending is not None
            assert pending["payload"]["question"] == "tue 9am"

            await run.answer("yes")
            result = await run
    finally:
        reset_settings_cache()

    assert result == {"request": "tue 9am", "decision": "yes", "outcome": "yes:tue 9am"}


@pytest.mark.asyncio
async def test_get_of_an_unknown_id_raises_not_found(no_project):
    deck = Deck(workflows=[_approval_workflow()])

    async with deck:
        with pytest.raises(NotFoundError, match="nonexistent"):
            await deck.runs.get("nonexistent")


@pytest.mark.asyncio
async def test_a_waiter_wakes_on_a_parked_run_rather_than_hanging(no_project, monkeypatch):
    """#325's wait primitive wakes on suspension, not just on a terminal outcome  -  its own
    review found this pinned only implicitly, by tests that would hang if it broke rather than
    fail. ``asyncio.wait_for`` is the point: a regression here times out and fails loudly
    instead of wedging the whole suite.
    """
    from agentdeck.runtime.settings import reset_settings_cache

    reset_settings_cache()
    try:
        deck = Deck(workflows=[_approval_workflow()])
        async with deck:
            run = await deck.runs.start("Approval", "tue 9am", session_id="t-wake")
            with pytest.raises(RunSuspendedError):
                await asyncio.wait_for(run, timeout=5)
    finally:
        reset_settings_cache()


# --- the control port is read where a stopped run is claimed, not only where it is resumed ---


@contextlib.asynccontextmanager
async def _parked_approval(monkeypatch, *, namespace: str | None = None):
    """A workflow run stopped at its interrupt  -  ``WAITING_ANSWER``, holding its session, with
    nothing left polling the gate. The state every case below signals against.

    ``namespace`` defaults to ``None``, the ordinary case every existing caller here exercises;
    passing one parks the run outside the default namespace, for the one case that needs it.
    """
    from agentdeck.runtime.settings import reset_settings_cache

    reset_settings_cache()
    try:
        deck = Deck(workflows=[_approval_workflow()])
        async with deck:
            parked = await deck.run("Approval", "tue 9am", session_id="t-1", namespace=namespace)
            assert parked["type"] == "interrupt"
            [run] = await deck.runs.list(namespace=namespace, status=RunStatus.WAITING_ANSWER)
            yield deck, run
    finally:
        reset_settings_cache()


@pytest.mark.asyncio
async def test_a_cancel_against_a_parked_run_ends_it_immediately(no_project, monkeypatch):
    """#229, #311. A cancel against a parked run used to be recorded and honored by nothing at
    all  -  only ``resume_run`` polled the control port, and an approval does not come back that
    way. #229 then made it honored, but only once some later ``run.answer`` happened to claim
    the run  -  a technicality became a real risk once #311 stopped a stale timer from ever
    reclaiming a parked run's session on its own, since a cancel nobody's answer ever noticed
    would now wedge the session forever. The cancel claims and terminates the run itself, so the
    request becomes the two events that are its whole honest story  -  no ``control.observed``,
    because the run reached no safe point; it was already stopped when the cancel landed  -  and
    the session is free the moment the cancel call returns, not on whichever later call notices.
    """
    async with _parked_approval(monkeypatch) as (deck, run):
        await run.cancel("operator said stop")

        assert await run.status() is RunStatus.CANCELLED
        log = await deck._require_open().store.read_session(_new_context("t-1"))
        assert [event.kind for event in log][-3:] == ["run.resumed", "control.requested", "run.cancelled"]
        assert next(e.payload.reason for e in log if e.kind == "run.cancelled") == "operator said stop"

        # A now-superfluous answer still raises  -  the run is gone, not merely refused.
        with pytest.raises(NotFoundError):
            await run.answer("yes")

        # And the session it held is free: a new run on it succeeds instead of raising
        # SessionBusyError, which is the whole point of cancelling it.
        resumed = await deck.run("Approval", {"request": "wed 3pm"}, session_id="t-1")
        assert resumed["type"] == "interrupt"


@pytest.mark.asyncio
async def test_a_cancel_against_a_parked_run_in_a_real_namespace_still_ends_it(no_project, monkeypatch):
    """Review found the eager cancel unreachable from here for any run outside the default
    namespace: ``RunOps.cancel``/``Deck._cancel`` did not accept or pass one, so the lookup
    ``Runtime._cancel_suspended`` needs to find the run always scanned ``namespace=None`` and
    silently missed it  -  cancel still returned ``True``, but nothing closed and the session
    stayed held, forever, now that no timer ever reclaims it either. This is the same case as
    ``test_a_cancel_against_a_parked_run_ends_it_immediately`` above, run through the public
    surface with a real namespace rather than through the Runtime directly, since the Runtime
    layer never had this gap.
    """
    async with _parked_approval(monkeypatch, namespace="acme") as (deck, run):
        await run.cancel("operator said stop")

        log = await deck._require_open().store.read_session(
            RunContext(run_id="reader", session_id="t-1", namespace="acme")
        )
        assert [event.kind for event in log][-3:] == ["run.resumed", "control.requested", "run.cancelled"]
        assert next(e.payload.reason for e in log if e.kind == "run.cancelled") == "operator said stop"

        # The session it held is free: a new run on it succeeds instead of raising
        # SessionBusyError, which is the whole point of cancelling it.
        resumed = await deck.run("Approval", {"request": "wed 3pm"}, session_id="t-1", namespace="acme")
        assert resumed["type"] == "interrupt"


@pytest.mark.asyncio
async def test_a_cancel_in_one_namespace_leaves_the_same_key_in_another_untouched(no_project, scripted):
    """#315/#320: both control ports keyed a pending signal by ``run_id`` alone, so ``acme``'s
    ``order-1234`` and ``globex``'s ``order-1234`` shared one row  -  a cancel meant for one
    landed on both, and ``consume()``'s compare-and-set made them fight over the same slot.

    #324 changes the mechanism, not the intent: ``key`` is no longer the run's address at all,
    so the two namespaces below sharing one key cannot even name the same id to collide over  -
    each gets its own, read straight off its own ``run.started``. The isolation this guards is
    now asserted through that real id rather than a caller-chosen one, the surface a caller
    actually holds (docs/design/run-identity.md).
    """
    deck = Deck(agents=[_greeter()])
    async with deck:
        globex_stream = deck.stream("Greeter", "hi there", key="order-1234", namespace="globex")
        globex_started = await anext(globex_stream)
        acme_stream = deck.stream("Greeter", "hi there", key="order-1234", namespace="acme")
        acme_started = await anext(acme_stream)

        assert globex_started.run_id != acme_started.run_id  # same key, two unrelated runs

        # Signalled before either stream is asked to produce more, so the first safe point
        # each hits is the one that observes it  -  deterministic without a clock or a sleep.
        acme_run = await deck.runs.get(acme_started.run_id, namespace="acme")
        await acme_run.cancel("acme said stop")

        globex = [globex_started, *[event async for event in globex_stream]]
        acme = [acme_started, *[event async for event in acme_stream]]

    assert [event.kind for event in globex][-1] == "run.completed"  # a different tenant, same key
    assert [event.kind for event in acme][-1] == "run.cancelled"  # the run the signal actually named


@pytest.mark.asyncio
async def test_pause_and_resume_isolate_between_namespaces_sharing_one_key(no_project, scripted):
    """The mandatory row (docs/design/run-identity.md §14, "control-plane isolation"): acme's
    and globex's runs, alive at once under the identical caller key, pause and resume
    independently. #315 could only prove this through the Runtime, since ``deck.runs.pause``/
    ``resume`` took no namespace; ``Run.pause``/``resume`` do, because a ``Run`` carries its own.
    """
    deck = Deck(agents=[_greeter()])
    async with deck:
        globex_stream = deck.stream("Greeter", "hi there", key="order-1234", namespace="globex")
        globex_started = await anext(globex_stream)
        acme_stream = deck.stream("Greeter", "hi there", key="order-1234", namespace="acme")
        acme_started = await anext(acme_stream)

        assert globex_started.run_id != acme_started.run_id  # same key, two unrelated runs

        acme_run = await deck.runs.get(acme_started.run_id, namespace="acme")
        globex_run = await deck.runs.get(globex_started.run_id, namespace="globex")
        # Signalled before either stream is asked to produce more, so the first safe point
        # each hits is the one that observes it  -  deterministic without a clock or a sleep.
        await acme_run.pause("acme stepped away")

        globex = [globex_started, *[event async for event in globex_stream]]
        acme = [acme_started, *[event async for event in acme_stream]]

        assert [event.kind for event in globex][-1] == "run.completed"  # a different tenant, unaffected
        assert [event.kind for event in acme][-1] == "run.paused"  # the run the pause actually named

        await acme_run.resume()
        assert await acme_run.status() is RunStatus.COMPLETED
        await globex_run.resume()  # already completed: a no-op, not an error
        assert await globex_run.status() is RunStatus.COMPLETED


@pytest.mark.asyncio
async def test_answer_isolates_between_namespaces_sharing_one_key(no_project, monkeypatch):
    """The mandatory row's other half: acme's and globex's runs, both parked ``WAITING_ANSWER``
    under the identical caller key, alive at once  -  answering one never touches the other."""
    from agentdeck.runtime.settings import reset_settings_cache

    reset_settings_cache()
    try:
        deck = Deck(workflows=[_approval_workflow()])
        async with deck:
            # One session id, deliberately: the session claim is keyed by
            # ``(namespace, session_id)``, so the same id in two namespaces is two conversations.
            # Sharing it here means the key claim below is the only thing left that could collide.
            acme_paused = await deck.run("Approval", "tue 9am", session_id="t-1", namespace="acme", key="order-1234")
            globex_paused = await deck.run(
                "Approval", "wed 3pm", session_id="t-1", namespace="globex", key="order-1234"
            )
            assert acme_paused["type"] == globex_paused["type"] == "interrupt"
            assert acme_paused["id"] != globex_paused["id"]  # same key, two unrelated runs

            acme_run = await deck.runs.get(acme_paused["id"], namespace="acme")
            globex_run = await deck.runs.get(globex_paused["id"], namespace="globex")

            await acme_run.answer("yes")
            acme_result = await acme_run

            # globex's own run is untouched: still waiting, still answerable with its own decision.
            assert await globex_run.status() is RunStatus.WAITING_ANSWER
            await globex_run.answer("no")
            globex_result = await globex_run

        assert acme_result["decision"] == "yes"
        assert globex_result["decision"] == "no"
    finally:
        reset_settings_cache()


@pytest.mark.asyncio
async def test_a_pause_against_a_parked_run_refuses_the_answer_and_stays_pending(no_project, monkeypatch):
    """The design's deliberate cell. Lifting the pause would let an answer silently override an
    operator who said stop, so the answer is refused and *both* intents survive  -  the run is
    still waiting, and the pause is still pending for whoever reads next. A refusal that ate the
    intent would lose the very stop it cited.
    """
    async with _parked_approval(monkeypatch) as (deck, run):
        await run.pause("operator stepped away")

        with pytest.raises(RunStateError, match="override"):
            await run.answer("yes")

        assert await run.status() is RunStatus.WAITING_ANSWER
        control = deck._require_open()._control
        assert (await control.poll(run.id)).verb is Signal.PAUSE


@pytest.mark.asyncio
async def test_resuming_a_run_that_is_waiting_for_an_answer_names_answer(no_project, monkeypatch):
    """``_paused`` used to narrow its listing to ``PAUSED``, so a parked run came back
    indistinguishable from one that does not exist and ``resume`` returned ``[]``  -  silence, to a
    caller who is in fact holding the run's only answer. It refuses now, and names the verb."""
    async with _parked_approval(monkeypatch) as (deck, run):
        with pytest.raises(RunStateError, match=r"run\.answer\(\.\.\.\)"):
            await run.resume()

        assert await run.status() is RunStatus.WAITING_ANSWER


@pytest.mark.asyncio
async def test_answering_a_paused_run_names_resume(no_project, scripted):
    """The mirror image, and the other half of why legality is a table rather than a listing
    filter: a paused run is not waiting for a value, and "No pending run" told a caller nothing
    about which of the two verbs would have worked."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        stream = deck.stream("Greeter", "hi there")
        started = await anext(stream)  # the run's own minted id, before it reaches a safe point
        run = await deck.runs.get(started.run_id)
        await run.pause("operator stepped away")
        [event async for event in stream]

        assert await run.status() is RunStatus.PAUSED
        with pytest.raises(RunStateError, match=r"run\.resume\(\)"):
            await run.answer("yes")


@pytest.mark.asyncio
async def test_pause_and_resume_reach_the_runtime_this_deck_composed(no_project, scripted):
    """The wiring, end to end and with nothing hand-built: ``run.pause`` writes to the very
    control port this Deck's own Runtime got, the run stops at its own safe point, and
    ``run.resume`` plays it on to completion."""
    deck = Deck(agents=[_greeter()])

    async with deck:
        stream = deck.stream("Greeter", "hi there")
        started = await anext(stream)  # the run's own minted id, before it reaches a safe point
        run = await deck.runs.get(started.run_id)
        await run.pause("operator stepped away")
        paused = [started, *[event async for event in stream]]
        await run.resume()
        resumed = [event async for event in run.events()][len(paused) :]

    assert [event.kind for event in paused][-3:] == ["control.requested", "control.observed", "run.paused"]
    assert next(e.payload.reason for e in paused if e.kind == "run.paused") == "operator stepped away"
    assert [event.kind for event in resumed][0] == "run.resumed"
    assert [event.kind for event in resumed][-1] == "run.completed"


@pytest.mark.asyncio
async def test_injected_session_factory_is_used_and_closed_once(no_project, monkeypatch, scripted):
    """The DI seam bypasses ``SessionFactory.from_settings``, and ``aclose()`` closes the
    injection exactly once."""
    from agentdeck.adapters.executors.openai_agents.sessions import SessionFactory

    def boom(_settings: Any) -> Any:
        raise AssertionError("from_settings must not be called when a factory is injected")

    monkeypatch.setattr(SessionFactory, "from_settings", staticmethod(boom))

    class _FakeSessionFactory:
        """Stand-in for the Redis-backed SessionFactory; counts aclose() calls."""

        def __init__(self) -> None:
            self.closed = 0
            self.sessions: dict[str, Any] = {}

        def session_for(self, session_id: str) -> Any:
            from agents import SQLiteSession

            return self.sessions.setdefault(session_id, SQLiteSession(session_id))

        async def aclose(self) -> None:
            self.closed += 1

    fake = _FakeSessionFactory()
    deck = Deck(agents=[_greeter()], session_factory=fake)

    async with deck:
        # namespace-scoped, because the engine's own store mints the key: two namespaces are
        # free to pick the same session id, and an unprefixed key would hand them one conversation
        assert deck.session_for("s1") is fake.sessions[":s1"]

    assert fake.closed == 1


async def _wait_until(condition, *, timeout: float = 5.0, interval: float = 0.01) -> None:
    """Poll ``condition`` (a zero-arg async callable returning a bool) until it is true, never
    sleeping a fixed guess at how long the sweep should take  -  see coding-standards §8, "assert
    the promise, not the timing"."""
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    while not await condition():
        if loop.time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)


async def _has_delta(deck: Deck, session_id: str) -> bool:
    events = await deck._runtime.store.read_session(RunContext(run_id="reader", session_id=session_id))
    return any(event.kind == "text.delta" for event in events)


async def _pending_is_empty(deck: Deck) -> bool:
    return not await deck.runs.list(status=RunStatus.WAITING_ANSWER)


# --- run.can, and lifecycle methods that refuse rather than report ---


@pytest.mark.asyncio
async def test_a_running_run_offers_pause_and_cancel_but_not_resume(no_project):
    """The first row of ``can_of``'s table, read off a real handle: what a live run offers is
    the stop half, and there is no pause yet for a resume to lift."""
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)
    deck = Deck(agents=[_greeter()])

    with patch_model(model):
        async with deck:
            run = await deck.runs.start("Greeter", "hello", session_id="s-can")
            await model.holding.wait()

            assert (run.can.pause, run.can.resume, run.can.cancel) == (True, False, True)

            hold.set()
            await run
            await run.status()  # `can` reads the last status this handle saw, so refresh it
            assert (run.can.pause, run.can.resume, run.can.cancel) == (False, False, False)


@pytest.mark.asyncio
async def test_a_paused_run_offers_resume_instead_of_pause(no_project):
    """The second row, and the one a UI's two buttons are wired to."""
    deck = Deck(agents=[_greeter()])
    async with deck:
        stream = deck.stream("Greeter", "hi there", session_id="s-can-paused")
        started = await anext(stream)
        run = await deck.runs.get(started.run_id)
        await run.pause("operator stepped away")
        [event async for event in stream]

        assert await run.status() is RunStatus.PAUSED
        assert (run.can.pause, run.can.resume, run.can.cancel) == (False, True, True)


@pytest.mark.asyncio
async def test_a_recovered_handle_knows_what_it_can_do_without_being_asked(no_project, monkeypatch):
    """``deck.runs.get`` reads a status to find the run at all, so the handle it hands back
    answers ``can`` from that read rather than from a second one the caller has to make."""
    async with _parked_approval(monkeypatch) as (deck, parked):
        recovered = await deck.runs.get(parked.id)

        assert recovered.can.cancel is True
        assert recovered.can.resume is False  # waiting for a value, which `answer` supplies


@pytest.mark.asyncio
async def test_resuming_a_run_that_wants_an_answer_refuses_and_names_answer(no_project, monkeypatch):
    """Strictness where 4.x returned quietly: a resume against ``WAITING_ANSWER`` used to do
    nothing at all, which reads as "resumed" to the caller who asked for it."""
    async with _parked_approval(monkeypatch) as (deck, parked):
        with pytest.raises(RunStateError, match="run.answer"):
            await parked.resume()

        assert await parked.status() is RunStatus.WAITING_ANSWER


@pytest.mark.asyncio
async def test_pausing_a_run_that_has_already_ended_is_quiet(no_project, scripted):
    """A no-op is not a refusal: the run is stopped, which is what the caller wanted. Only a
    state that would have to be overridden raises."""
    deck = Deck(agents=[_greeter()])
    async with deck:
        run = await deck.runs.start("Greeter", "hello", session_id="s-over")
        await run

        await run.pause("too late")
        await run.cancel("also too late")


@pytest.mark.asyncio
async def test_a_deck_with_no_control_backend_says_so_instead_of_returning_false(no_project):
    """4.x answered ``False`` here, which a caller could not tell apart from "the run had already
    ended". The signal was never recorded, so nothing was ever going to happen."""
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)
    deck = Deck(agents=[_greeter()])

    with patch_model(model):
        async with deck:
            run = await deck.runs.start("Greeter", "hello", session_id="s-uncontrolled")
            await model.holding.wait()
            deck._runtime._control = None

            with pytest.raises(UnsupportedControlError, match="AGENTDECK_CONTROL"):
                await run.pause("operator stepped away")

            hold.set()
            await run


@pytest.mark.asyncio
async def test_a_run_whose_engine_cannot_suspend_refuses_pause_outright(no_project):
    """The capability half of ``can``, on the handle. Set by hand because every engine this
    deck can register suspends today  -  the first one that does not arrives with the invocation
    resolver (agentdeck #337), and this is the branch it will land on.
    """
    hold = asyncio.Event()
    model = ScriptedModel(deltas=("Hel", "lo"), hold=hold)
    deck = Deck(agents=[_greeter()])

    with patch_model(model):
        async with deck:
            run = await deck.runs.start("Greeter", "hello", session_id="s-unsuspendable")
            await model.holding.wait()
            run._suspendable = False

            assert (run.can.pause, run.can.resume, run.can.cancel) == (False, False, True)
            with pytest.raises(UnsupportedControlError, match="does not suspend"):
                await run.pause()

            hold.set()
            await run
