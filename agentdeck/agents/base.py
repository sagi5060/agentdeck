"""Declarative ``Agent`` / ``SandboxAgent`` factories driven by class attrs."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

from agents import Agent, AgentHooks, ModelSettings
from agents.handoffs import Handoff
from agents.sandbox import SandboxAgent

from agentdeck.agents.capabilities import CapabilitiesSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from agents.agent_output import AgentOutputSchemaBase
    from agents.tool import FunctionTool

# Sidecar registries, keyed by ``id(tool)`` because the SDK's ``FunctionTool``
# isn't hashable. Records: (i) the wrapped inner ``Agent`` so a non-sandbox
# supervisor exposing a sandbox worker via ``as_tool`` still triggers
# ``RunConfig(sandbox=...)``; (ii) plain function tools whose body needs an
# active sandbox session. Both registries are process-lifetime — tools live as
# long as the agent that holds them, so we don't track GC.
_INNER_AGENT: dict[int, Agent[Any]] = {}
_SANDBOX_TOOL_IDS: set[int] = set()


def inner_agent_of(tool: FunctionTool) -> Agent[Any] | None:
    """Return the inner ``Agent`` a ``BaseAgent.as_tool`` wrapper hides, if any."""
    return _INNER_AGENT.get(id(tool))


def is_sandbox_tool(tool: FunctionTool) -> bool:
    """True iff the tool was declared to need sandbox lifecycle hooks."""
    return id(tool) in _SANDBOX_TOOL_IDS


class BaseAgent:
    """Declarative base for SDK :class:`Agent` instances.

    Subclasses set class attributes; :meth:`build` forwards the populated
    ones to the SDK constructor (empty / unset values drop out so the SDK
    keeps its defaults).
    """

    name: ClassVar[str] = ""
    instructions: ClassVar[str] = ""
    handoff_description: ClassVar[str | None] = None

    model: ClassVar[str | None] = None
    model_settings: ClassVar[Mapping[str, Any]] = {}

    tools: ClassVar[Sequence[Any]] = ()
    handoffs: ClassVar[Sequence[Any]] = ()
    output_type: ClassVar[type[Any] | AgentOutputSchemaBase | None] = None
    hooks: ClassVar[AgentHooks | None] = None

    # Names of MCP servers from the ``mcp:`` settings group this agent
    # depends on. The lifecycle owns transport / URL / headers;
    # the agent just says which named servers it needs. Names that
    # don't appear in the config — or whose startup connect failed —
    # are dropped at build time, so an unreachable MCP host degrades
    # the agent rather than crashing the backend.
    mcp_server_names: ClassVar[Sequence[str]] = ()

    # Registry names spawnable via ``spawn_subagent``; empty (default) adds no such tool.
    subagents: ClassVar[Sequence[str]] = ()

    @classmethod
    def build(cls) -> Agent:
        return cls._build({})

    @classmethod
    def _build(cls, building: dict[type[BaseAgent], Agent[Any]]) -> Agent[Any]:
        return _build_cached(cls, lambda: Agent(**cls._kwargs()), building)

    @classmethod
    async def run(cls, message: Any = None, **runner_options: Any) -> Any:
        """One-shot headless run; returns the SDK ``RunResult``."""
        from agentdeck.agents.runners.headless import HeadlessRunner

        return await HeadlessRunner.from_agent(cls.build(), **runner_options).run(message)

    @classmethod
    def as_tool(cls, **as_tool_kwargs: Any) -> FunctionTool:
        """Expose this agent as a :class:`agents.tool.FunctionTool`.

        Thin wrapper around the SDK's :meth:`agents.Agent.as_tool` that
        also builds the agent. Records the inner ``Agent`` in the
        module-level ``_INNER_AGENT`` sidecar so the runner can detect
        tools that wrap sandbox-needing agents and attach
        ``RunConfig(sandbox=...)`` before the nested turn starts.

        Subtlety: ``as_tool`` typically runs at *module import* (catalog
        agents wire their workers at class-body time), which is **before**
        any backend lifespan has fired :class:`agentdeck.agents.MCPLifecycle.
        startup`. A snapshot of ``mcp_servers`` and the MCP-status banner
        captured then would freeze "unavailable" into the wrapped agent
        forever, even after the lifecycle later connects the server.

        To keep the wrapped agent honest, we refresh ``instructions`` and
        ``mcp_servers`` from :meth:`_kwargs` on *every* invocation before
        delegating to the SDK's body. The Agent dataclass is mutable, so
        the run loop picks up the new values on the next turn.
        """
        built = cls.build()
        tool = built.as_tool(**as_tool_kwargs)
        _wrap_agent_tool_invoke(tool, cls, built)
        _INNER_AGENT[id(tool)] = built
        return tool

    @classmethod
    def _kwargs(cls) -> dict[str, Any]:
        # Empty list/dict attrs drop out so the SDK can apply its own defaults.
        instructions, mcp_servers = _build_instructions_and_mcp(cls)
        tools = list(cls.tools)
        if cls.subagents:
            from agentdeck.agents.subagents import spawn_subagent_tool

            tools.append(spawn_subagent_tool(cls))
        fields: dict[str, Any] = {
            "name": cls.name or cls.__name__,
            "instructions": instructions,
            "handoff_description": cls.handoff_description,
            "model": cls.model,
            "model_settings": ModelSettings(**cls.model_settings) if cls.model_settings else None,
            "tools": tools or None,
            "output_type": cls.output_type,
            "hooks": cls.hooks,
            "mcp_servers": mcp_servers or None,
        }
        return {k: v for k, v in fields.items() if v is not None}


def _wrap_agent_tool_invoke(
    tool: FunctionTool,
    cls: type[BaseAgent],
    inner: Agent[Any],
) -> None:
    """Make an ``as_tool`` sub-agent invocation re-resolve MCP state from the lifecycle.

    The SDK's ``Agent.as_tool`` body closes over the inner ``Agent`` it was given. We replace
    ``on_invoke_tool`` with a wrapper that, on each call, recomputes the inner agent's
    ``instructions`` and ``mcp_servers`` via :func:`_build_instructions_and_mcp` — so a server
    unavailable at import time but live by the first request stops presenting as missing, and
    vice-versa. Tracing is NOT done here: OpenInference (the Agents-SDK instrumentation) already
    emits the tool + nested-agent spans for an ``as_tool`` call.
    """
    original = tool.on_invoke_tool

    async def _on_invoke_tool(context: Any, input_json: str) -> Any:
        instructions, mcp_servers = _build_instructions_and_mcp(cls)
        inner.instructions = instructions
        inner.mcp_servers = list(mcp_servers)
        return await original(context, input_json)

    tool.on_invoke_tool = _on_invoke_tool


def _build_instructions_and_mcp(cls: type[BaseAgent]) -> tuple[str, list[Any]]:
    """Resolve MCP servers and prepend a status banner when needed.

    On the happy path (no declared servers, or every declared server
    connected) the agent's ``instructions`` are returned unchanged so
    upstream prompt caches stay warm. When at least one declared
    server is unavailable, prepend the strict-protocol banner so the
    model surfaces ``error: mcp_unavailable: <name>`` instead of
    fabricating results — see :func:`agentdeck.agents.mcp.mcp_status_banner`.
    """
    if not cls.mcp_server_names:
        return cls.instructions, []
    from agentdeck.agents.mcp import mcp_status_banner, resolve_agent_mcp_status

    available, missing = resolve_agent_mcp_status(cls.mcp_server_names)
    banner = mcp_status_banner(missing)
    instructions = banner + cls.instructions if banner else cls.instructions
    return instructions, list(available)


# Kept a literal (not imported from app.py) so lazy registry construction still resolves it.
_PROJECT_ALIAS = "agentdeck_project"


def _build_cached(
    cls: type[BaseAgent],
    ctor: Callable[[], Agent[Any]],
    building: dict[type[BaseAgent], Agent[Any]],
) -> Agent[Any]:
    """Construct ``cls``'s agent once per :meth:`BaseAgent.build` call, breaking handoff cycles.

    ``building`` is threaded through recursive handoff resolution (mirrors the ``_seen``-set
    pattern in ``agents/runners/base.py``'s ``_needs_sandbox``): the ``Agent`` is registered
    before its handoffs are resolved, so a cycle (A -> B -> A) returns the same in-progress
    instance instead of recursing forever; its ``handoffs`` list is mutated in place once
    resolved, so peers holding the earlier reference see the final value.
    """
    cached = building.get(cls)
    if cached is not None:
        return cached
    agent = ctor()
    building[cls] = agent
    agent.handoffs = _resolve_handoffs(cls.handoffs, building) or []
    return agent


def _resolve_handoffs(
    handoffs: Sequence[Any],
    building: dict[type[BaseAgent], Agent[Any]],
) -> list[Agent[Any] | Handoff[Any, Agent[Any]]]:
    """Resolve declarative handoff entries into SDK ``Agent`` / ``Handoff`` instances.

    Entries may be a :class:`BaseAgent` subclass (built lazily here so peer agents don't have
    to be constructed at import time), a registry name (``str``, resolved via the same
    discovery registry ``App`` uses — breaks bidirectional-import cycles), an already-built
    ``Agent``, or a fully constructed ``Handoff``.
    """
    resolved: list[Agent[Any] | Handoff[Any, Agent[Any]]] = []
    for entry in handoffs:
        if isinstance(entry, str):
            entry = _lookup_agent_class(entry)
        if isinstance(entry, type) and issubclass(entry, BaseAgent):
            resolved.append(entry._build(building))
            continue
        if isinstance(entry, (Agent, Handoff)):
            resolved.append(entry)
            continue
        raise TypeError(
            f"handoffs entry must be a BaseAgent subclass, Agent, Handoff, or str; got {type(entry).__name__}.",
        )
    return resolved


def _lookup_agent_class(name: str) -> type[BaseAgent]:
    from agentdeck.agents.registry import AgentRegistry

    return AgentRegistry(_PROJECT_ALIAS).get(name)


class BaseSandboxAgent(BaseAgent):
    """Reference sandbox agent with every supported class-level knob.

    Class attributes are the public configuration surface for agentdeck agents.
    ``BaseSandboxAgent.build()`` reads these attributes and constructs an SDK
    ``SandboxAgent``:

    ``name``
        Human-readable agent name. If omitted, the Python class name is used.

    ``instructions``
        System prompt for the model. Keep operational behavior here: role,
        priorities, tool-use rules, output expectations, and domain policy.

    ``handoff_description``
        Optional short description used when another agent decides whether to
        hand work to this agent.

    ``model``
        Optional per-agent model override. ``None`` means the runner-level
        ``OPENAI_MODEL`` setting supplies the model.

    ``model_settings``
        Dictionary forwarded to ``agents.ModelSettings(**model_settings)`` at
        build time. Use it for inference controls such as temperature, token
        budget, parallel tool calls, and metadata. Only include keys accepted
        by the installed Agents SDK.

    ``tools``
        Already-built non-sandbox SDK tools available to the agent. Sandbox
        tools such as shell, skills, filesystem, and memory are configured
        through ``capabilities`` instead.

    ``output_type``
        Optional schema for structured final responses. Pass a Pydantic model
        for the SDK default (strict JSON schema, every field required), or
        wrap it in ``AgentOutputSchema(Model, strict_json_schema=False)`` to
        keep fields with defaults optional — useful with smaller chat-
        completions models that stall on short prompts when forced to fill
        every required field. Set ``None`` for free-form text.

    ``hooks``
        Optional ``AgentHooks`` instance for lifecycle callbacks. Hooks are
        useful for telemetry or local bookkeeping; they should not replace
        the agent's instructions.

    ``capabilities``
        ``CapabilitiesSpec`` controls sandbox features:
        ``shell`` configures sandbox command execution caps;
        ``skills`` and ``skills_dir`` expose selected skill bundles;
        ``compaction`` configures context compaction;
        ``filesystem`` configures sandbox filesystem tools;
        ``memory`` configures SDK memory storage/read/generation behavior.
    """

    capabilities: ClassVar[CapabilitiesSpec] = CapabilitiesSpec()

    @classmethod
    def build(cls) -> SandboxAgent:
        return cast("SandboxAgent", cls._build({}))

    @classmethod
    def _build(cls, building: dict[type[BaseAgent], Agent[Any]]) -> Agent[Any]:
        return _build_cached(
            cls,
            lambda: SandboxAgent(**cls._kwargs(), capabilities=cls.capabilities.build(agent_model=cls.model)),
            building,
        )


__all__ = ["BaseAgent", "BaseSandboxAgent", "inner_agent_of", "is_sandbox_tool"]
