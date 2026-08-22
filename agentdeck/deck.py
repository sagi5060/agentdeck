"""``Deck``  -  the v3 composition root: agents, workflows, skills and MCP servers become one
catalog, either handed in directly or discovered from a project directory.

    from agents import function_tool

    from agentdeck import Agent, Deck

    @function_tool
    def find_slots(day: str) -> str:
        \"\"\"Find free appointment slots on a given day.\"\"\"
        ...

    booking_agent = Agent(name="booking", instructions="...", tools=[find_slots])
    deck = Deck(agents=[booking_agent], skills="./skills", mcp=".mcp.json")
    deck.build()

    async with deck:
        result = await deck.run("booking", "hello")

Two constructors, one primitive: ``Deck(...)`` (code-first) and ``Deck.from_project(path)``
(today's ``.agentdeck/`` directory layout, unchanged)  -  ``from_project`` discovers the same four
arguments the plain constructor takes and hands them to it, so there is exactly one catalog
mechanism underneath either front door.

Lifecycle: ``NEW -> build() -> BUILT -> (async with) -> OPEN -> CLOSED``. ``build()`` validates
every name a catalog references (skills, MCP servers) and compiles every
agent/workflow to an ``InvocableSpec``  -  reading local files, never the network, and idempotent.
The catalog is immutable from construction: :attr:`agents` and :attr:`workflows` are read-only
mappings, so nothing after ``build()`` can invalidate what it already checked. Opening starts
what ``build()`` deliberately left alone  -  the MCP lifecycle, the Runtime's executors and event
store, the observers on its event stream  -  and closing tears down only what this Deck itself
started (the ownership rule: configuration this Deck instantiated is its to close; an object
the caller constructed and handed in stays the caller's).
"""

from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from collections import ChainMap
from contextlib import aclosing, suppress
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, cast

from agentdeck.adapters.executors.native import NativeExecutor
from agentdeck.adapters.executors.openai_agents import ExecutionStore, OpenAIAgentsExecutor, SessionFactory
from agentdeck.adapters.executors.openai_agents.runconfig import validate_model_requirements
from agentdeck.adapters.tools.mcp.lifecycle import MCPLifecycle
from agentdeck.authoring.agent import Agent
from agentdeck.authoring.compile import compile_agent, refresh_mcp_status
from agentdeck.authoring.injection import declared_context_type
from agentdeck.authoring.interrupts import interrupt_result
from agentdeck.authoring.native import NativeDefinition
from agentdeck.authoring.skills import skills_resolver
from agentdeck.composition import (
    build_runtime,
    resolve_control_port,
    resolve_event_store,
    resolve_observers,
    resolve_run_settings,
)
from agentdeck.core.content import DataBlock, TextBlock, coerce_input
from agentdeck.core.context import RunContext
from agentdeck.core.control import Signal
from agentdeck.core.events import (
    TERMINAL_KINDS,
    RunCancelled,
    RunCompleted,
    RunFailed,
    RunInterrupted,
    RunPaused,
)
from agentdeck.core.invocable import AgentInstance, InvocableKind, InvocableSpec
from agentdeck.core.ports import Observer
from agentdeck.core.status import PRECONDITIONS, SUSPENDED_KINDS, Controls, Operation, RunStatus, Verdict, can_of
from agentdeck.errors import (
    AgentdeckError,
    ConfigError,
    NotFoundError,
    RunStateError,
    RunSuspendedError,
    UnsupportedControlError,
)
from agentdeck.mcp import MCP
from agentdeck.runtime.discovery import EXECUTOR_FOR_KIND, InvocableRegistry
from agentdeck.runtime.registry import PROJECT_DIR, PluginRegistry, mount_project_dir
from agentdeck.runtime.settings import Settings, get_settings
from agentdeck.skills import Skills

if TYPE_CHECKING:
    import builtins
    from collections.abc import AsyncGenerator, AsyncIterator, Generator, Mapping, Sequence

    from agents.memory.session import Session

    from agentdeck.authoring.interrupts import InterruptResult
    from agentdeck.core.content import Input
    from agentdeck.core.events import Event, Usage
    from agentdeck.core.ports import EventStorePort, Executor
    from agentdeck.runtime.service import PendingRun, Runtime

# The engine names a Deck's catalog always targets  -  read off each engine's own ``ClassVar``,
# never an instance, so ``build()`` can validate "an engine is registered" without constructing
# anything that could touch the network. See the module docstring's lifecycle note.
_DEFAULT_EXECUTORS: tuple[str, ...] = (OpenAIAgentsExecutor.name, NativeExecutor.name)

_State = Literal["NEW", "BUILT", "OPEN", "CLOSED"]

logger = logging.getLogger(__name__)


def _validate_observers(observers: Sequence[Observer] | None) -> None:
    """Refuse anything in ``observers=`` that is not an ``Observer``, at build() time.

    Cheap, and it is the difference between a name in a traceback and a run that reaches an
    ``await observer.emit(...)`` on an object with no ``emit``  -  where the dispatch's own
    breaker would swallow it as "this one keeps failing" and the events would just go missing.
    """
    for observer in observers or ():
        if not isinstance(observer, Observer):
            raise ConfigError(
                f"observers= takes Observer instances; got {type(observer).__name__}. An "
                "observer implements `async def emit(self, event)` and subclasses "
                "`agentdeck.core.ports.Observer`."
            )


class TurnResult:
    """One agent turn's outcome, assembled from its own ``run.completed``  -  never the SDK's
    own result object, so a caller depends on agentdeck's event schema rather than on
    whichever engine ran the turn.

    ``run_id`` (and ``session_id``, for a conversational turn) name the run this came from,
    so a caller who wants more than ``output`` and ``usage`` can read the rest of it back
    from the event log instead of this object growing a field for everything the log already
    carries.
    """

    __slots__ = ("output", "run_id", "session_id", "usage")

    def __init__(self, *, output: Any, usage: Usage, run_id: str, session_id: str | None = None) -> None:
        self.output = output
        self.usage = usage
        self.run_id = run_id
        self.session_id = session_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TurnResult):
            return NotImplemented
        return (self.output, self.usage, self.run_id, self.session_id) == (
            other.output,
            other.usage,
            other.run_id,
            other.session_id,
        )

    def __repr__(self) -> str:
        return f"TurnResult(output={self.output!r}, usage={self.usage!r}, run_id={self.run_id!r}, session_id={self.session_id!r})"


def _new_context(session_id: str | None = None) -> RunContext:
    """A context for the internal ports that still take one  -  the execution store, the event
    store. The Runtime does not: it takes run options and mints its own.
    """
    return RunContext(run_id=str(uuid.uuid4()), session_id=session_id)


# Seconds :meth:`Deck._events` may go before re-reading the store for a run this process is not
# itself driving  -  recovered through another handle, another worker, or one this call did not
# start. A run this process *is* executing wakes this the instant its task produces something (or
# settles), at no interval cost at all (``asyncio.wait`` returns the moment the task finishes,
# never merely on the clock); this constant only bounds the case with no local task to wake it,
# the same trade :data:`agentdeck.core.control.CONTROL_POLL_INTERVAL` makes for a run watching its
# own control port.
_FOLLOW_POLL_INTERVAL = 0.05

# Seconds :meth:`Deck.aclose` gives one cancelled run to settle, and how many times it asks. A
# cancel a run takes at all is recorded within one scheduler wake and one append the store hands
# to a thread, so a second is margin for a loaded box or a network store. Twice, because a cancel
# landing in a turn that is already tearing down is eaten by that teardown and the next one is
# taken. The loop is serial, so a close holding several wedged runs costs 2s each.
_CLOSE_GRACE = 1.0
_CLOSE_ATTEMPTS = 2


async def _drain(events: AsyncGenerator[Event, None]) -> None:
    """Advance ``events``  -  a live ``runtime.run()`` generator  -  to its end and discard what it
    yields. Persist-before-yield already wrote and fanned out each one before this loop ever
    sees it, so nothing here needs to keep them: this is only what makes the run advance at all,
    now that no caller's own ``async for`` has to (docs/design/run-identity.md §9).
    """
    async with aclosing(events):
        async for _ in events:
            pass


async def _turn_result(events: AsyncGenerator[Event, None]) -> TurnResult:
    """A run's own ``run.completed`` as a :class:`TurnResult`.

    Drains ``events`` to its natural end rather than returning the moment ``run.completed``
    is seen  -  closing the Runtime's generator any earlier throws ``GeneratorExit`` into it one
    line before it notices its own terminal event, recording a spurious ``run.cancelled`` right
    after a run that in fact finished cleanly.

    Raises if the stream ends without one: the engine's own exception already reached the
    caller in that case, so the only way this is hit is a run suspended by a pause or a cancel.
    """
    result: TurnResult | None = None
    async with aclosing(events):
        async for event in events:
            payload = event.payload
            if isinstance(payload, RunCompleted):
                data = next((block.data for block in payload.output if isinstance(block, DataBlock)), None)
                if data is not None:
                    output = data
                else:
                    output = "".join(block.text for block in payload.output if isinstance(block, TextBlock))
                result = TurnResult(
                    output=output, usage=payload.usage, run_id=event.run_id, session_id=event.session_id
                )
    if result is None:
        raise RuntimeError(
            "the run ended without completing (paused or cancelled)  -  resume it with "
            "(await deck.runs.get(run_id)).resume(), or inspect the event log for what happened."
        )
    return result


async def _workflow_result(events: AsyncGenerator[Event, None]) -> tuple[Any, bool]:
    """A workflow run's final state (or the interrupt it paused on), plus whether anything
    actually ran for this call.

    ``applied`` is what keeps a lost resume claim from reading as success: that race produces a
    stream with no terminal event at all, since the winner's events belong to its own segment.
    """
    result: Any = None
    applied = False
    async with aclosing(events):
        async for event in events:
            payload = event.payload
            if isinstance(payload, RunInterrupted):
                result, applied = interrupt_result(payload.payload, payload.thread_id or "", id=event.run_id), True
            elif isinstance(payload, RunCompleted):
                result = next((block.data for block in payload.output if isinstance(block, DataBlock)), None)
                applied = True
    return result, applied


def _content_for(root: Agent | NativeDefinition, input: Any) -> Input:
    """``input`` coerced the way ``root``'s own kind expects it  -  shared by every caller that
    begins a run (:meth:`Deck.run`, :meth:`Deck.stream`, :meth:`Runs.start`), so the coercion
    rule lives in exactly one place.

    A native definition takes whatever it was given: its executor binds the value to the body's
    own parameters, so a string stays a string rather than being wrapped as content.
    """
    if isinstance(root, Agent):
        return coerce_input(input)
    return coerce_input(input) if isinstance(input, str) else [DataBlock(data=input)]


def _invoked_name(deck: Deck, target: Any) -> str:
    """The catalog name ``ctx.invoke(target, ...)`` named  -  a name, or something this deck holds.

    A bare object (a plain callable, an SDK agent, a compiled graph) waits for the invocation
    resolver, so there is one rule and no special case. A definition is resolved by identity
    rather than accepted on its own word, because a run's log records its invocable by *name* and
    that name is what ``answer``, ``resume`` and a cancel-while-suspended resolve back through: a
    child of a definition the catalog does not hold would run, and then be unanswerable. An
    :class:`AgentInstance` meets the same check rather than being exempted from it  -  that is what
    registering a minted agent is *for*, and one from another deck fails here exactly as a
    definition from another deck does.
    """
    if isinstance(target, str):
        return target
    if isinstance(target, AgentInstance):
        if deck._instance(target.name) is not target:
            raise ConfigError(
                f"ctx.invoke() was given the agent {target.name!r}, which this deck does not hold "
                f"under that name. An agent minted by ctx.agents belongs to the deck that minted "
                f"it, and a child run is resolved by the name its log records."
            )
        return target.name
    if not isinstance(target, NativeDefinition):
        raise ConfigError(
            f"ctx.invoke() takes a catalog name, a @tool/@workflow definition, or an agent from "
            f"ctx.agents; got a {type(target).__name__}. Running an arbitrary object needs the "
            f"invocation resolver, which is not built yet  -  declare it with @tool or @workflow, "
            f"or register it under a name and invoke it by that."
        )
    if deck._workflows.get(target.name) is not target:
        raise ConfigError(
            f"ctx.invoke() was given the {target.kind.value} {target.name!r}, which this deck's "
            f"catalog does not hold under that name. A child run is resolved by the name its log "
            f"records, so answering, resuming or cancelling one needs the definition in the "
            f"catalog: add it to Deck(workflows=[...])."
        )
    return target.name


def _invocation_input(root: Agent | NativeDefinition, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
    """``ctx.invoke(target, *args, **kwargs)`` as the single value a run is opened with.

    A call binds to the target's own signature and a run is opened with one value, so the two meet
    here: one positional argument *is* that value, and anything wider becomes the mapping
    ``deck.run(name, {...})`` already binds by name (``docs/design/execution-api.md``). Which is
    why an agent takes one and only one: it has no parameters for names to bind to.
    """
    if len(args) == 1 and not kwargs:
        return args[0]
    if not args:
        return dict(kwargs) if kwargs else None
    names = () if isinstance(root, Agent) else root.parameters
    if len(args) > len(names):
        raise ConfigError(
            f"ctx.invoke() passed {len(args)} positional arguments to {root.name!r}, which takes "
            f"{len(names)} ({', '.join(names) or 'none'})."
        )
    positional = dict(zip(names, args, strict=False))
    if repeated := sorted(positional.keys() & kwargs.keys()):
        raise ConfigError(
            f"ctx.invoke() gave {root.name!r} {', '.join(repeated)} twice, once positionally and "
            f"once by keyword  -  exactly as calling it directly would."
        )
    return positional | kwargs


def _copied(source: Agent, overrides: dict[str, Any]) -> Agent:
    """``source`` with ``overrides`` applied. Field by field because an ``Agent`` is immutable and
    its ``__slots__`` are exactly its keyword arguments, so a field added there arrives here."""
    return Agent(**{slot: getattr(source, slot) for slot in Agent.__slots__} | overrides)


class _Agents:
    """``ctx.agents``: mint an agent this deck's catalog does not hold.

    Deck-side because minting is registration  -  a run resolves its invocable by name on every
    answer, resume and cancel, so an agent nothing holds is one a paused child could never be
    continued from. Running what it hands back is still ``ctx.invoke``.
    """

    def __init__(self, deck: Deck) -> None:
        self._deck = deck

    def create(self, **declaration: Any) -> AgentInstance:
        """Declare an agent from scratch: the same keywords :class:`Agent` takes.

            helper = ctx.agents.create(name="triage", instructions="Rank these by urgency.")

        The name it comes back under is the one given plus a mint, since two calls that chose one
        name are two agents and each child run records which it ran.
        """
        return self._deck._mint(Agent(**declaration))

    def fork(self, source: str | Agent | AgentInstance, /, **overrides: Any) -> AgentInstance:
        """Copy ``source``  -  a catalog name, an ``Agent``, or an instance  -  with ``overrides``.

            careful = ctx.agents.fork("Writer", instructions="Draft it in under 100 words.")

        ``source`` is required rather than defaulting to ``ctx.agent``: ``agent`` is a
        :class:`~agentdeck.core.context.ToolCtx` member and ``agents`` a
        :class:`~agentdeck.core.context.WorkflowCtx` one, so nothing ever holds both and the
        default could only ever be ``None``. Forking the agent you are inside is therefore
        unreachable rather than merely undefaulted, which is intended: a tool is a leaf, and one
        that could mint a variant of its own agent is orchestrating.
        """
        return self._deck._mint(_forked(self._deck, source, overrides))


def _forked(deck: Deck, source: str | Agent | AgentInstance, overrides: dict[str, Any]) -> Agent:
    """``source`` resolved to the declaration a fork copies. A name goes through the same lookup
    ``ctx.invoke`` does, so an unknown one fails once, in one message."""
    if isinstance(source, AgentInstance):
        source = cast("Agent", source.declaration)
    elif isinstance(source, str):
        source = cast("Agent", deck._root(source))
    if not isinstance(source, Agent):
        raise ConfigError(
            f"ctx.agents.fork() copies an agent: a catalog name, an Agent, or one ctx.agents "
            f"minted. Got a {type(source).__name__}."
        )
    return _copied(source, overrides)


async def _aclose_store(store: EventStorePort) -> None:
    """Best-effort teardown for a store this Deck built itself  -  never one passed in.

    The stores are inconsistent in shape (``SqliteEventStore.close`` is sync, the Redis/Postgres
    stores are ``async aclose``, and the port itself requires neither), so this checks for
    either rather than asking every caller to know which store it got.
    """
    if hasattr(store, "aclose"):
        await store.aclose()  # ty: ignore[call-non-callable]  -  duck-typed: EventStorePort itself declares neither
    elif hasattr(store, "close"):
        store.close()  # ty: ignore[call-non-callable]  -  same reason


def _named_mapping(items: Sequence[Any], arg_name: str, expected: type | None = None) -> Mapping[str, Any]:
    # Mirrors PluginRegistry's own collision rule: `{a.name: a for a in agents}` would collapse
    # a duplicate to whichever came last with no error, the same silent shadow this rule refuses
    # on the discovery path.
    found: dict[str, Any] = {}
    for item in items:
        if expected is not None and not isinstance(item, expected):
            # Anything with a `.name` used to reach the catalog and die at `.skills` in build()
            # (#451). Checked before the name is read, so a bare string is refused here too.
            raise ConfigError(
                f"Deck({arg_name}=...) takes agentdeck {expected.__name__} declarations, not the "
                f"{type(item).__module__}.{type(item).__name__} {getattr(item, 'name', item)!r}. An "
                f"Agents SDK agent is legitimate as a handoff target instead: "
                f"Agent(name=..., handoffs=[...])."
            )
        if item.name in found:
            raise ConfigError(
                f"two entries in {arg_name}= both use the name {item.name!r}; one name is one "
                f"invocable  -  rename one of them."
            )
        found[item.name] = item
    return MappingProxyType(found)


def _coerce_skills(value: str | Path | Sequence[str | Path] | Skills | None) -> Skills | None:
    if value is None or isinstance(value, Skills):
        return value
    if isinstance(value, str | Path):
        return Skills(value)
    return Skills(*value)


def _coerce_mcp(value: str | Path | MCP | None) -> MCP | None:
    if value is None or isinstance(value, MCP):
        return value
    return MCP(value)


# The one Deck currently holding the process, or ``None``. Discovery mounts every project under
# a single module alias and the MCP lifecycle keys its servers class-wide, so a second live Deck
# reads the first one's bundles and shares its servers. Enforced here rather than made to work:
# v3 is single-deck by ruling, and this refuses the case loudly instead of failing open.
_live_deck: Deck | None = None


def _origin(project_path: Path | None) -> str:
    return str(project_path) if project_path is not None else "a code-first Deck(...), no project dir"


def _refuse_second_deck(incoming: str) -> None:
    """Raise if a Deck already holds the process. Separate from :func:`_claim_process` because
    ``from_project`` has to refuse *before* it mounts: mounting evicts the live Deck's cached
    bundle modules and rebinds the alias to the new root, so refusing afterwards would leave the
    surviving Deck pointing at a project it does not own  -  fine until a durable workflow resumes
    and re-imports a bundle class, which would then resolve against the wrong directory."""
    if _live_deck is None:
        return
    raise ConfigError(
        f"a Deck is already live in this process ({_origin(_live_deck._project_path)}); "
        f"agentdeck v3 supports one Deck per process, so this one ({incoming}) would read the "
        "first one's bundles and share its MCP servers. Close the first with "
        "`await deck.aclose()` before constructing another. Two decks side by side is "
        "deferred  -  agentdeck issue #213."
    )


def _claim_process(deck: Deck) -> None:
    global _live_deck
    _refuse_second_deck(_origin(deck._project_path))
    _live_deck = deck


def _release_process(deck: Deck) -> None:
    # Only its own claim: a Deck closed after a later one took the process must not free that
    # later one's slot.
    global _live_deck
    if _live_deck is deck:
        _live_deck = None


class Deck:
    """One catalog of agents, workflows, skills and MCP servers, and the lifecycle over it.

    Construct with ``agents=``/``workflows=`` (bare :class:`~agentdeck.authoring.agent.Agent`
    instances and ``@workflow`` definitions  -  never wrapped, per
    ``docs/delivery/deck-capability-wrapper-pattern.md``) and ``skills=``/``mcp=`` (a bare path,
    a sequence of paths, or the capability object itself  -  coerced either way).

    ``context=`` declares the *type* of the application context this catalog's callables receive
     -  the class, not an instance of it. The value itself arrives per run (:meth:`run`,
    :meth:`stream`, :meth:`Runs.start`), and a tool, a dynamic-instructions callable, an agent
    hook or a native definition declaring a :class:`~agentdeck.core.context.ToolCtx` parameter
    receives it. Declaring the type is what makes :meth:`build` able to check every such
    parameter against it before anything runs; a deck that declares none still runs exactly the
    same, with the requirement unchecked until the callable is played.

    ``observers=`` are the read-only taps on this Deck's event stream  -  telemetry, cost, audit,
    any :class:`~agentdeck.core.ports.Observer`, and :class:`agentdeck.observers.LangfuseObserver`
    is the one agentdeck ships. Each one's ``start()`` is called once, while the Deck opens,
    before any run  -  so which run happens to come first never decides whether tracing is on.
    Three states, and the default is not a fourth: ``None`` (the default) opens the configured
    Langfuse observer if ``AGENTDECK_LANGFUSE_*`` names one and nothing otherwise; a sequence
    opens exactly those, in order, and suppresses the settings-derived one; ``()`` opens none at
    all. An observer is fire-and-forget by the port's contract  -  one that is slow or raises
    costs its own backlog and never a run.

    There is no ``deck.observers`` property, for the reason there is no ``runtime`` or ``store``:
    nothing needs one, and a property is additive later while removing one is not.


    Public properties are :attr:`agents`, :attr:`workflows`, :attr:`skills` and :attr:`settings`
    only  -  never ``runtime`` or ``store``, the infrastructure this class exists to hide.
    :attr:`runs` is :class:`Runs`, the collection that finds or starts a :class:`Run`: once a
    caller holds one, every op that acts on it  -  :meth:`Run.pause`, :meth:`Run.cancel`,
    :meth:`Run.resume`, :meth:`Run.answer`, :meth:`Run.status`, :meth:`Run.pending`  -  lives on
    the handle itself, since :meth:`run` and :meth:`stream` already claim the verb for
    *starting* one.

    **One Deck per process.** Constructing a second one while the first is still open raises
    ``ConfigError`` naming both projects. Sequential decks are fine: close one, construct the
    next. Two side by side is deferred to agentdeck issue #213.
    """

    def __init__(
        self,
        *,
        agents: Sequence[Agent] = (),
        workflows: Sequence[NativeDefinition] = (),
        skills: str | Path | Sequence[str | Path] | Skills | None = None,
        mcp: str | Path | MCP | None = None,
        context: object = None,
        observers: Sequence[Observer] | None = None,
        session_factory: SessionFactory | None = None,
        # Private-by-name test seams  -  never part of the documented constructor, exactly like
        # ``tests/contract/``'s need for ``_executors=`` on the Runtime this composes. A bare
        # engine-name string restricts `build()`'s "is this engine registered" check without
        # constructing anything (see `_DEFAULT_EXECUTORS`); a live `Executor` is what
        # `__aenter__` needs to actually play a run on  -  a string-only override never opens.
        _executors: Sequence[Executor | str] | None = None,
        _store: EventStorePort | None = None,
        _session_factory: SessionFactory | None = None,
        # Not a test seam like the two below: the bundle path each discovered ``agents``/
        # ``workflows`` entry came from, so a compile failure at build() can still name it  -
        # ``from_project`` is the only caller, since a code-first entry has no bundle to name.
        _bundle_of: Mapping[str, str] | None = None,
        # Likewise ``from_project``'s alone: the project this catalog was discovered from, so
        # the one-Deck-per-process refusal can name it.
        _project_path: Path | None = None,
    ) -> None:
        self._agents: Mapping[str, Agent] = _named_mapping(agents, "agents", Agent)
        self._workflows: Mapping[str, NativeDefinition] = _named_mapping(workflows, "workflows")
        self._skills_obj = _coerce_skills(skills)
        self._mcp_obj = _coerce_mcp(mcp)
        self._context_type = declared_context_type(context)
        self._observers_arg = observers
        self._session_factory_arg = session_factory if session_factory is not None else _session_factory
        self._executors_arg = _executors
        self._store_arg = _store
        self._bundle_of = _bundle_of or {}
        self._project_path = _project_path

        self._state: _State = "NEW"
        self._invocables: Mapping[str, InvocableSpec] | None = None
        self._executor_instances: tuple[Executor, ...] | None = None
        self._runtime: Runtime | None = None
        self._sessions: ExecutionStore | None = None
        # The one execution owner per run (docs/design/run-identity.md §9): keyed by run_id,
        # populated by `_start`, and popped by `_execution_done` the moment the task settles  -
        # whichever way  -  so `_events` degrades to reading the store once nothing local is
        # left to wake it. The namespace rides along because `aclose` addresses the store with
        # it for a run that has to be closed from outside, and holds only the id.
        self._executions: dict[str, tuple[str | None, asyncio.Task[None]]] = {}
        # What ``ctx.agents.create()``/``fork()`` minted, under names minted with them. A run
        # resolves its invocable by name on every answer, resume and cancel, so an agent the
        # catalog does not hold is one this deck has to hold instead  -  for the rest of its life,
        # since nothing else knows when the last handle on a minted agent is gone.
        self._minted: dict[str, InvocableSpec] = {}
        self._owns_store = False
        self._started_observers: tuple[Observer, ...] = ()
        self._started_mcp = False
        self._closed = False
        self._runs = Runs(self)
        # Last, so a constructor that raises above (a duplicate name, an unreadable capability)
        # leaves the process free for the next attempt instead of poisoning it.
        _claim_process(self)

    @classmethod
    def from_project(cls, path: str | Path = PROJECT_DIR, **kwargs: Any) -> Deck:
        """The ``./.agentdeck`` (or ``path``) directory layout, unchanged  -  discovers the same
        ``agents=``/``workflows=``/``skills=``/``mcp=`` the plain constructor takes and hands
        them to it, so both front doors build the same catalog.

        ``**kwargs`` forwards anything else (``observers=``, the private test seams) straight to
        the constructor, same as calling it directly.
        """
        # Before mounting, not after: see :func:`_refuse_second_deck`. The constructor claims
        # the process, but by then this method has already evicted and rebound the alias.
        _refuse_second_deck(str(Path(path).resolve()))
        package = mount_project_dir(path)
        agent_registry = PluginRegistry(
            package, base_class=Agent, module_name="agent", type_dir="agents", label="agent"
        )
        agents = list(agent_registry.list(refresh=True).values())
        workflow_registry = PluginRegistry(
            package, base_class=NativeDefinition, module_name="workflow", type_dir="workflows", label="workflow"
        )
        workflows = list(workflow_registry.list(refresh=True).values())
        project_root = Path(path).resolve()
        # ``.mcp.json`` lives at the project root  -  a sibling of ``.agentdeck/``, not inside it.
        # For the default ``path`` this is also where ``.env`` resolves from. An explicit
        # non-default ``path`` only matches that if
        # the caller also runs from its parent. Its absence means "no servers" rather than a
        # configuration error, the same fail-open rule an empty ``mcp.servers`` map always had.
        mcp_json = project_root.parent / ".mcp.json"
        return cls(
            agents=agents,
            workflows=workflows,
            skills=Skills(project_root / "skills"),
            mcp=MCP(mcp_json) if mcp_json.is_file() else None,
            _bundle_of={**agent_registry.bundle_files(), **workflow_registry.bundle_files()},
            _project_path=project_root,
            **kwargs,
        )

    @property
    def agents(self) -> Mapping[str, Agent]:
        return self._agents

    @property
    def workflows(self) -> Mapping[str, NativeDefinition]:
        return self._workflows

    @property
    def skills(self) -> Skills | None:
        return self._skills_obj

    @property
    def runs(self) -> Runs:
        """``start``/``get``/``list``  -  the collection that finds or starts a :class:`Run`. See
        :class:`Runs`."""
        return self._runs

    @property
    def settings(self) -> Settings:
        return get_settings()

    def build(self) -> Deck:
        """Validate the whole catalog and compile every agent/workflow to an ``InvocableSpec``.

        Idempotent: a second call is a no-op once ``BUILT`` (or later). Reads local files
        (every ``SKILL.md``, the MCP file) but opens no connection and starts no MCP server  -
        executors are named, never constructed, until :meth:`__aenter__` actually needs one.

        Registering the MCP server specs (``MCPLifecycle.configure``, itself network-free) here
        means an agent's ``mcp=`` compiles against known-but-not-yet-connected names rather than
        unknown ones  -  the only warning this can still log is a genuine open-time drop, not a
        false "not found in config" for a server that will, in fact, connect once opened.

        When this Deck declared ``context=``, every ``ToolCtx[...]`` a tool, an instructions
        callable, a hook or a workflow node requires is checked against it here, and an
        incompatible one raises :class:`~agentdeck.errors.ContextTypeError` naming both types.
        Only what the runtime can decide is decided  -  see
        :func:`~agentdeck.authoring.injection.check_context_type` for what defers instead.

        ``observers=`` are shape-checked here for the same reason, and *only* shape-checked:
        no observer is started, no telemetry client is constructed and no exporter is contacted,
        so a deck with Langfuse configured still validates where Langfuse is unreachable.
        """
        if self._state != "NEW":
            return self
        _validate_observers(self._observers_arg)
        run_settings = resolve_run_settings()
        validate_model_requirements(
            (
                (agent.name, agent.model if agent.model is not None else run_settings.model)
                for agent in self._agents.values()
            ),
            run_settings,
        )
        skills_by_name = self._skills_obj.build() if self._skills_obj is not None else {}
        mcp_names = frozenset(self._mcp_obj.build()) if self._mcp_obj is not None else frozenset()
        if self._mcp_obj is not None:
            MCPLifecycle.configure(self._mcp_obj.config())
        for agent in self._agents.values():
            self._validate_agent_skills(agent, skills_by_name)
            self._validate_agent_mcp(agent, mcp_names)
        engine_names = tuple(self._executors_arg) if self._executors_arg is not None else _DEFAULT_EXECUTORS
        registry = InvocableRegistry(engine_names)
        self._invocables = registry.load(
            agents=list(self._agents.values()),
            workflows=list(self._workflows.values()),
            resolve_skills=skills_resolver(self._skills_obj) if self._skills_obj is not None else None,
            bundle_of=self._bundle_of,
            context_type=self._context_type,
            delegate=self._delegate,
        )
        self._state = "BUILT"
        return self

    def _validate_agent_skills(self, agent: Agent, skills_by_name: Mapping[str, Any]) -> None:
        if not agent.skills:
            return
        if self._skills_obj is None:
            raise ConfigError(
                f"agent {agent.name!r} declares skills={list(agent.skills)!r}, but this Deck has no skills= configured."
            )
        if unknown := sorted(set(agent.skills) - set(skills_by_name)):
            raise ConfigError(
                f"agent {agent.name!r} declares unknown skill(s) {unknown}. Available: {sorted(skills_by_name)}."
            )

    def _validate_agent_mcp(self, agent: Agent, mcp_names: frozenset[str]) -> None:
        if not agent.mcp:
            return
        if self._mcp_obj is None:
            raise ConfigError(
                f"agent {agent.name!r} declares mcp={list(agent.mcp)!r}, but this Deck has no mcp= configured."
            )
        if unknown := sorted(set(agent.mcp) - mcp_names):
            raise ConfigError(
                f"agent {agent.name!r} declares unknown MCP server(s) {unknown}. Available: {sorted(mcp_names)}."
            )

    async def __aenter__(self) -> Deck:
        """Open: build (if not yet), start the MCP lifecycle, and compose the Runtime.

        Everything ``build()`` deliberately left alone happens here  -  constructing the real
        executors, the event store, the session factory, the telemetry client, and connecting
        every configured MCP server (soft per-server failure, same as today). MCP status on
        every already-compiled agent is refreshed right after, since ``build()`` resolved it
        before anything connected.

        Every observer's ``start()`` runs exactly once, here, and before the Runtime exists  -
        they are registered as it is assembled, so a run can never be the thing that turns
        observability on. That ordering is what #181 and #162 are both about: telemetry used to
        be built underneath this, from settings, and started by whichever run came first.
        """
        if self._state == "CLOSED":
            # CLOSED is terminal: aclose()'s own idempotency guard would otherwise skip
            # draining/closing everything a second open builds fresh, on the mistaken belief
            # there was nothing left to do.
            raise ConfigError("this Deck is already closed; construct a new one to open again.")
        self.build()
        if self._state == "OPEN":
            return self
        if self._executors_arg is not None:
            live = [e for e in self._executors_arg if not isinstance(e, str)]
            if len(live) != len(self._executors_arg):
                raise ConfigError(
                    "_executors= given as bare engine-name strings only restricts build()'s "
                    "validation; opening a Deck needs live Executor instances to run on."
                )
            self._executor_instances = tuple(live)
        else:
            self._executor_instances = (
                OpenAIAgentsExecutor(self._ensure_sessions(), settings=resolve_run_settings()),
                NativeExecutor(self._invoke, _Agents(self)),
            )
        self._owns_store = self._store_arg is None
        store = self._store_arg if self._store_arg is not None else resolve_event_store()
        # Resolved and started here, not in ``build_runtime``: an observer opens a live client,
        # so when that happens is a lifecycle decision (#181), and doing it while a Runtime was
        # assembled is what #162's first defect was. Caller-named observers are taken as-is and
        # suppress the settings-derived one entirely  -  a Deck told which taps to open must not
        # quietly open a Langfuse client beside them.
        observers = resolve_observers() if self._observers_arg is None else tuple(self._observers_arg)
        for observer in observers:
            # An observer that refuses the open (Langfuse with no keys) must not leave the ones
            # before it holding a client nobody will ever close  -  this open is not going to
            # finish, and there is no ``__aexit__`` for a ``__aenter__`` that raised.
            try:
                await observer.start()
            except BaseException:
                for started in self._started_observers:
                    await started.close()
                self._started_observers = ()
                raise
            self._started_observers = (*self._started_observers, observer)
        self._runtime = build_runtime(
            executors=self._executor_instances,
            # A view, not a copy: what ``ctx.agents`` mints after this point has to be resolvable
            # by the Runtime that is already open, and a minted name never shadows a catalog one
            # because it carries a mint the catalog cannot have written.
            invocables=ChainMap(self._minted, dict(self._invocables or {})),
            store=store,
            sinks=observers,
            control=resolve_control_port(),
        )
        await MCPLifecycle.startup(self._mcp_obj.config() if self._mcp_obj is not None else None)
        self._started_mcp = True
        if self._mcp_obj is not None:
            # build() compiled every agent's mcp= against MCPLifecycle before any server had
            # connected, so its tools/banner are stale the moment startup() above finishes  -
            # correct the compiled agent in place before anything can run a turn against it.
            invocables = self._invocables
            assert invocables is not None  # build() just above guarantees this
            agents = list(self._agents.values())
            refresh_mcp_status({name: invocables[name].native for name in self._agents}, agents)
        self._state = "OPEN"
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Flush the Runtime's sinks, close what this Deck itself opened, leave the rest.

        The ownership rule with no exemption: an ``MCP(...)`` this Deck holds is configuration
        it is the one to shut down, regardless of whether the caller built the object or a bare
        path was coerced into one. A store passed in via the private ``_store=`` seam is never
        touched  -  it was the caller's before this Deck ever saw it. Idempotent, and reachable
        from every state  -  a Deck built but never opened still has a process claim to give up.

        For observers the rule bites at construction, not here: a Deck given ``observers=``
        builds no telemetry client of its own. Every observer still gets its ``close``, a
        caller's own included, because that call means "the stream has ended, write out what you
        buffered"  -  skipping it to look respectful of ownership would discard exactly the audit
        or cost records the observer was registered to keep.

        Nothing shuts the Langfuse SDK down. Measured against langfuse 4.14.1, the SDK holds one
        resource manager per public key in a process-global cache and ``shutdown()`` marks it
        dead without evicting it  -  so the next Deck opened in this process (sequential decks are
        supported; see :func:`_refuse_second_deck`) would be handed the dead one and export
        nothing, silently. Flushing is what the deck owes; the SDK's own ``atexit`` stops its
        threads at the scope that resource actually has.
        """
        if self._closed:
            return
        self._closed = True
        # Every run this Deck itself is executing settles or is cancelled here, before the
        # runtime drains and the store closes below  -  both are things a still-writing task
        # needs. A task already done has already settled on its own (`run.completed`/
        # `run.failed`/`run.cancelled` is already the log's last word on it); one still running
        # is cancelled, which the Runtime's own `asyncio.CancelledError` arm turns into a
        # `run.cancelled` for (persist-before-yield means that write lands before this awaits
        # the task out). Every way is logged, so a close that had work in flight says which.
        for run_id, (namespace, task) in list(self._executions.items()):
            if task.done():
                logger.info("closing deck: run %s had already settled", run_id)
                continue
            for _ in range(_CLOSE_ATTEMPTS):
                task.cancel()
                try:
                    # Shielded, so a grace that runs out leaves the task running rather than
                    # cancelling this close's own wait on it.
                    await asyncio.wait_for(asyncio.shield(task), timeout=_CLOSE_GRACE)
                except TimeoutError:
                    # A cancel delivered into a turn that was already tearing down is eaten by
                    # that teardown; the next one lands at the next await (#412).
                    continue
                except asyncio.CancelledError:
                    # The run's own cancellation, arriving back through the shield. A cancel of
                    # this close instead leaves the task uncancelled, and has to keep travelling.
                    if not task.cancelled():
                        raise
                break
            else:
                logger.error("closing deck: run %s ignored its cancellation; abandoning it", run_id)
                await self._require_open().close_cancelled(
                    run_id,
                    f"abandoned by the closing deck: it took no cancellation in {_CLOSE_ATTEMPTS * _CLOSE_GRACE}s",
                    namespace=namespace,
                )
                continue
            logger.info("closing deck: run %s was cancelled", run_id)
        self._executions.clear()
        try:
            # Draining closes the sinks, which is what finishes the Langfuse sink's open
            # observations and pushes the SDK's buffer out of the process.
            if self._runtime is not None:
                await self._runtime.drain()
            if self._sessions is not None:
                await self._sessions.aclose()
            for executor in self._executor_instances or ():
                # A native workflow parked mid-await is a coroutine this deck started; nothing
                # else will ever answer it once the deck is closing.
                await executor.aclose()
            if self._owns_store and self._runtime is not None:
                await _aclose_store(self._runtime.store)
        finally:
            if self._started_mcp:
                self._started_mcp = False
                await MCPLifecycle.shutdown()
            self._state = "CLOSED"
            _release_process(self)

    @staticmethod
    def _release() -> None:
        # The test suite's safety net for the process claim: a Deck constructed by a sync test
        # and never opened has no `await deck.aclose()` to release it. Not public API.
        global _live_deck
        _live_deck = None

    def _ensure_sessions(self) -> ExecutionStore:
        if self._sessions is None:
            factory = self._session_factory_arg
            if factory is None:
                factory = SessionFactory.from_settings(self.settings.session)
            self._sessions = ExecutionStore(factory)
        return self._sessions

    def _require_open(self) -> Runtime:
        if self._state != "OPEN" or self._runtime is None:
            raise ConfigError("this Deck is not open: use `async with deck:` (or `await deck.__aenter__()`) first.")
        return self._runtime

    def _root(self, name: str) -> Agent | NativeDefinition:
        if name in self._agents:
            return self._agents[name]
        if name in self._workflows:
            return self._workflows[name]
        if (instance := self._instance(name)) is not None:
            return cast("Agent", instance.declaration)
        raise NotFoundError(
            f"No agent or workflow named {name!r}. Available: {sorted({*self._agents, *self._workflows})}."
        )

    async def _start(
        self,
        name: str,
        content: Input,
        *,
        context: object = None,
        session_id: str | None = None,
        namespace: str | None = None,
        key: str | None = None,
    ) -> tuple[Event, asyncio.Task[None]]:
        """Begin one run and hand the rest of its execution to a deck-owned task, returning as
        soon as its own ``run.started`` is durable  -  whether or not anybody ever asks to see
        this run again (docs/design/run-identity.md §9).

        Everything a caller of ``runtime.run()`` used to have to keep draining for the run to
        advance at all now happens here, once, in :func:`_drain`. This is the one path that awaits
        the claim before handing the task back, so it is also the one where a claim failure
        (``SessionBusyError``, ``DuplicateKeyError``, an unknown invocable) still surfaces
        synchronously to the caller  -  exactly as it did when ``runtime.run()``'s first event
        was pulled directly. :meth:`_invoke` drains the same way for a child and cannot: it
        returns the handle without awaiting, so a child's claim failure reaches whoever awaits it.
        """
        runtime = self._require_open()
        agen = runtime.run(name, content, context=context, session_id=session_id, namespace=namespace, key=key)
        opening = await anext(agen)
        task = asyncio.create_task(_drain(agen))
        self._executions[opening.run_id] = (namespace, task)
        task.add_done_callback(functools.partial(self._execution_done, opening.run_id))
        return opening, task

    def _invoke(self, parent: RunContext, target: Any, *args: Any, **kwargs: Any) -> Run:
        """The invoker seam behind ``ctx.invoke``: begin a child run of ``target`` and hand back
        its handle, having awaited nothing.

        Synchronous because the handle *is* the return value and ``await`` on it is the result, so
        there is no point in the call at which a body could await the opening claim. That is also
        why the child's id is minted here rather than read back off its ``run.started``: the
        handle needs one before the claim lands. The execution task is registered under it
        straight away, so :meth:`aclose` and :meth:`Run._result` own a child exactly as they own a
        top-level run.

        A run in its own right, and its own session: the parent holds the one it was started on,
        and a second turn against it would be refused. What it does inherit is the parent's
        ``namespace``, which is the isolation boundary, and the parent's ``context`` by reference,
        which is the application's environment for the whole invocation rather than for one run.
        The edge itself is recorded, on the child's ``run.started``: a cancel follows it down and
        a delegated turn's cost rolls up along it, and both have to be true of a log nobody was
        watching live.
        """
        runtime = self._require_open()
        name = _invoked_name(self, target)
        root = self._root(name)
        content = _content_for(root, _invocation_input(root, args, kwargs))
        run_id = str(uuid.uuid4())
        # Before the task, because a bound refused inside it would reach the body as a handle on a
        # run that was never opened rather than as an error at the ``ctx.invoke`` that asked.
        runtime.delegate(run_id, parent.run_id, name)
        task = asyncio.create_task(
            _drain(
                runtime.run(
                    name,
                    content,
                    context=parent.data,
                    namespace=parent.namespace,
                    run_id=run_id,
                    parent_run_id=parent.run_id,
                )
            )
        )
        self._executions[run_id] = (parent.namespace, task)
        task.add_done_callback(functools.partial(self._execution_done, run_id))
        return Run(
            self,
            id=run_id,
            key=None,
            namespace=parent.namespace,
            session_id=None,
            context=parent.data,
            seen=RunStatus.RUNNING,
            suspendable=runtime.suspends(name),
        )

    async def _delegate(self, parent: RunContext, name: str, task: str) -> Any:
        """What a ``subagents=`` tool does when the model calls it: run that agent as a child of
        the turn that delegated, and hand back what it finished with.

        The tool's result, not the run handle: the model asked a question and gets an answer. A
        child that fails raises here, which the SDK records on ``tool.call.completed.error`` and
        tells the model about  -  a delegation that went wrong is never an empty success.
        """
        result = await self._invoke(parent, name, task)
        return result.output if isinstance(result, TurnResult) else result

    def _mint(self, declaration: Agent) -> AgentInstance:
        """Hold an agent this catalog does not, and hand back what invokes it.

        The name gets a mint of its own, because two ``create()`` calls that chose the same one
        are two different agents and every run addresses its invocable by the name its log
        records. Compiled here rather than at the first invoke, so a declaration that cannot be
        built fails at the line that wrote it.

        Against this deck's own catalog, so a minted agent may delegate the way a declared one
        does: ``subagents=`` travels with a fork, and one compiled against nothing would refuse
        every name its source could reach.
        """
        self._require_open()
        minted = _copied(declaration, {"name": f"{declaration.name}#{uuid.uuid4().hex[:8]}"})
        instance = AgentInstance(name=minted.name, declaration=minted)
        self._minted[minted.name] = InvocableSpec(
            name=minted.name,
            kind=InvocableKind.AGENT,
            executor=EXECUTOR_FOR_KIND[InvocableKind.AGENT],
            native=compile_agent(
                minted, context_type=self._context_type, catalog=self._agents, delegate=self._delegate
            ),
            metadata={"agent": instance},
        )
        return instance

    def _instance(self, name: str) -> AgentInstance | None:
        """The agent behind ``name``, whether the catalog holds it or ``ctx.agents`` minted it."""
        spec = self._minted.get(name) or (self._invocables or {}).get(name)
        return spec.metadata.get("agent") if spec is not None else None

    def _execution_done(self, run_id: str, task: asyncio.Task[None]) -> None:
        """Retire a settled execution task, whatever way it settled  -  completed, suspended, or
        cancelled by :meth:`aclose`. The exception, if any, is retrieved here purely so asyncio
        does not log it as unretrieved when nobody ever calls :meth:`run`/:meth:`stream` again
        for this run; a caller who does still sees it, since a task's exception is cached, not
        consumed, by reading it once.
        """
        self._executions.pop(run_id, None)
        with suppress(asyncio.CancelledError):
            task.exception()

    async def _events(self, ctx: RunContext, *, from_seq: int = 0) -> AsyncGenerator[Event, None]:
        """One execution segment's own events, replayed from ``from_seq`` and tailed until the
        segment stops advancing  -  a terminal outcome or a suspension, whichever it reaches
        first. Reads only the store, never the Runtime (docs/design/run-identity.md §9): any
        number of these may run over one run's segment at once, in this process or another
        sharing the store, without ever stealing its events from each other or advancing it.

        A run this process is itself executing wakes this the moment its task produces
        something new, or settles, at no interval cost; one this process did not start  -
        recovered through another handle, another worker, or already over by the time anybody
        looked  -  is read by polling every :data:`_FOLLOW_POLL_INTERVAL` instead.
        """
        store = self._require_open().store
        seq = from_seq
        while True:
            batch = await store.read_run(ctx, from_seq=seq)
            for event in batch:
                yield event
                seq = event.seq + 1
                if event.kind in TERMINAL_KINDS or event.kind in SUSPENDED_KINDS:
                    return
            execution = self._executions.get(ctx.run_id)
            if execution is not None and not execution[1].done():
                await asyncio.wait({execution[1]}, timeout=_FOLLOW_POLL_INTERVAL)
            else:
                await asyncio.sleep(_FOLLOW_POLL_INTERVAL)

    async def run(
        self,
        name: str,
        input: Any,
        *,
        context: object = None,
        session_id: str | None = None,
        namespace: str | None = None,
        key: str | None = None,
    ) -> TurnResult | Any:
        """Run ``name``  -  an agent or a workflow, whichever this catalog holds it as  -  and
        return its outcome: a :class:`TurnResult` for an agent, the final state (or an
        :class:`~agentdeck.authoring.interrupts.InterruptResult`) for a workflow.

        ``context`` is the application's own environment for this run  -  a database handle, a
        client, whatever the code the run reaches needs. A tool, a dynamic-instructions callable
        or a workflow node that declares a :class:`~agentdeck.core.context.ToolCtx` parameter
        receives it; the model never does, and it is never written to the event log. The same
        object serves the whole run, by reference.

        ``key`` is an optional stable application identifier  -  for lookup and idempotency, never
        the run's address: the run's own id is always minted, never derived from it. Reusing a
        ``(namespace, key)`` pair whose run already started raises ``DuplicateKeyError`` rather
        than replaying that run, since this call always begins a new one.
        """
        root = self._root(name)
        self._require_open()
        content = _content_for(root, input)
        opening, task = await self._start(
            name, content, context=context, session_id=session_id, namespace=namespace, key=key
        )
        # Settled before the log is read, not after: the segment's own exception (if any) has
        # to preempt `_turn_result`'s "ended without completing" fallback, which would otherwise
        # be all a caller ever saw of a run that in fact raised.
        await task
        ctx = RunContext(run_id=opening.run_id, session_id=opening.session_id, namespace=opening.namespace)
        events = self._events(ctx)
        if isinstance(root, Agent):
            return await _turn_result(events)
        result, _ = await _workflow_result(events)
        return result

    async def stream(
        self,
        name: str,
        input: Any,
        *,
        context: object = None,
        session_id: str | None = None,
        namespace: str | None = None,
        key: str | None = None,
    ) -> AsyncGenerator[Event, None]:
        """Streaming counterpart to :meth:`run`: yields the run's own canonical events.

        Starting and observing are two different things now (docs/design/run-identity.md §9):
        the run advances in a deck-owned task from the moment :meth:`_start` returns it, and a
        caller that stops reading this generator  -  closes it, or has the task driving it
        cancelled out from under it  -  only stops *watching*. It does not stop the run, which
        keeps executing to its own natural end regardless.
        """
        root = self._root(name)
        self._require_open()
        content = _content_for(root, input)
        opening, task = await self._start(
            name, content, context=context, session_id=session_id, namespace=namespace, key=key
        )
        ctx = RunContext(run_id=opening.run_id, session_id=opening.session_id, namespace=opening.namespace)
        async with aclosing(self._events(ctx)) as events:
            async for event in events:
                yield event
        # Read only once the segment's own events are all yielded, so a mid-stream failure's
        # exception reaches the caller right after its `run.failed`  -  the same place it did
        # when a caller's own `async for` drove the failing generator directly.
        await task

    async def _pause(self, run_id: str, reason: str | None = None, namespace: str | None = None) -> bool:
        """Implementation behind :meth:`Run.pause`, and the v1 wire's blind signal (naming no
        namespace is what an unnamespaced HTTP caller means, not an omission here)."""
        return await self._require_open().signal(run_id, Signal.PAUSE, reason, namespace=namespace)

    async def _cancel(self, run_id: str, reason: str | None = None, namespace: str | None = None) -> bool:
        """Implementation behind :meth:`Run.cancel`, and the v1 wire's blind signal."""
        return await self._require_open().signal(run_id, Signal.CANCEL, reason, namespace=namespace)

    async def _resume(
        self, run_id: str, reason: str | None = None, *, context: object = None, namespace: str | None = None
    ) -> list[Event]:
        """Implementation behind :meth:`Run.resume`."""
        return [
            event
            async for event in self._require_open().resume_run(
                run_id, context=context, reason=reason, namespace=namespace
            )
        ]

    async def _pending(self, namespace: str | None = None) -> list[PendingRun]:
        """Implementation behind :meth:`Run.pending` and :meth:`Runs.list`'s
        ``status=WAITING_ANSWER`` narrowing."""
        return await self._require_open().pending(namespace=namespace)

    async def _answer(self, run_id: str, value: Any, *, context: object = None, namespace: str | None = None) -> Any:
        """Implementation behind :meth:`Run.answer`."""
        runtime = self._require_open()
        pending = next((run for run in await runtime.pending(namespace=namespace) if run.run_id == run_id), None)
        if pending is None:
            # The inbox lists WAITING_ANSWER runs only, so a miss means some other state  -  and
            # which one decides whether the caller is told to lift a pause, told the run is over,
            # or told nothing answers to this id at all.
            raise await self._not_answerable(run_id, namespace)
        result, applied = await _workflow_result(
            runtime.resume(
                pending.invocable,
                value,
                context=context,
                run_id=pending.run_id,
                session_id=pending.session_id,
                namespace=namespace,
            )
        )
        if not applied:
            # Nothing was played: a lost race, or a run the routing ended instead of answering.
            # Re-read the state rather than repeating a guess  -  after a cancel served here the
            # run is terminal, and "no pending run" is the true answer to what was asked.
            raise await self._not_answerable(run_id, namespace)
        return result

    async def _not_answerable(self, run_id: str, namespace: str | None = None) -> AgentdeckError:
        """Why this run is not one an answer can land on, in the words :data:`PRECONDITIONS`
        wrote for whoever was refused.

        Only a ``REFUSED`` state gets the new error. A run the log never heard of, one that had
        already ended, and one answered by somebody else between the listing and this read all
        keep the ``NotFoundError`` they have always raised  -  the caller's question was "which
        pending run is this", and for all three the answer is still "none"."""
        status = await self._require_open().status(run_id, namespace=namespace)
        if status is None:
            return NotFoundError(f"No pending run {run_id!r}.")
        allowed = PRECONDITIONS[status, Operation.ANSWER]
        if allowed.verdict is not Verdict.REFUSED:
            return NotFoundError(f"No pending run {run_id!r}.")
        return RunStateError(f"run {run_id!r} cannot be answered: {allowed.why}")

    def session_for(self, session_id: str) -> Session:
        """Conversation memory for ``session_id``  -  the engine's own store, so a turn started
        here and one started over HTTP land in the same conversation."""
        return self._ensure_sessions().session_for(_new_context(session_id))

    def asgi(self) -> Any:
        """The ASGI app ``agentdeck serve`` runs: a FastAPI app whose lifespan opens this Deck
        on startup and closes it on shutdown, so a mounted Deck needs no separate
        ``async with``. The HTTP contract is v1's own, unchanged (``tests/golden/`` proves it
        byte-for-byte)  -  building it lives in ``agentdeck.serve`` (the one module allowed to
        import FastAPI), not here, so ``agentdeck.deck`` stays free of that dependency.
        """
        from agentdeck.serve import build_asgi_app

        return build_asgi_app(self)


_NO_CONTROL_PORT = (
    "the run could not be told to {verb}: this deck has no control backend, so nothing was "
    "recorded and nothing will happen. Set AGENTDECK_CONTROL  -  `memory://` reaches runs in this "
    "process, `sqlite:///<path>` reaches runs in another."
)

# What ``run.can`` assumes about a handle that never learned which invocable opened its run: the
# store's summary projects status, not origin, so ``Runs.get`` and ``Runs.list`` have no name to
# ask :meth:`Runtime.suspends` about. Exact rather than optimistic today, since every registered
# engine suspends.
# ponytail: assumed, not read. The first executor that answers False (a plain callable, agentdeck
# #337) is what puts the origin on ``RunSummary`` and turns this into a lookup.
_RECOVERED_SUSPENDABLE = True


def _completed_result(deck: Deck, event: Event, payload: RunCompleted) -> Any:
    """A run's own ``run.completed`` as the value :meth:`Run.__await__` hands back  -  a
    :class:`TurnResult` for an agent, the body's own return value for a workflow.

    Keyed off ``event.origin`` (constant for one run, whichever event carries it) rather than
    a name the caller supplied: a handle recovered through :meth:`Runs.get` never held one, and
    reading it here is what lets :meth:`Run.__await__` work identically whether this process
    started the run or only ever looked it up.
    """
    data = next((block.data for block in payload.output if isinstance(block, DataBlock)), None)
    if isinstance(deck._root(event.origin), Agent):
        output = (
            data
            if data is not None
            else "".join(block.text for block in payload.output if isinstance(block, TextBlock))
        )
        return TurnResult(output=output, usage=payload.usage, run_id=event.run_id, session_id=event.session_id)
    return data


class Run:
    """A deck-bound handle on one run  -  not a second runtime. It holds no engine, store, MCP
    registry or observer, and delegates every operation back through the deck's own
    infrastructure (docs/design/run-identity.md §3); if it ever grows one of those, the design
    is wrong.

    A handle caches no authoritative state: :attr:`id`, :attr:`key`, :attr:`namespace` and
    :attr:`session_id` are the run's identity, fixed at construction, never its live status  -
    two handles on one run always agree, because the durable store is the only thing either
    ever reads from. Constructed only by :meth:`Runs.start` and :meth:`Runs.get`, never
    directly.

    The context a caller passed to :meth:`Runs.start` is retained here for same-process
    continuation (:meth:`resume`, :meth:`answer`)  -  never written to the log, and never
    recovered by :meth:`Runs.get`: a handle rehydrated after a restart has durable state and no
    context, and supplies ``None`` to whichever of those it calls.
    """

    __slots__ = ("_context", "_deck", "_seen", "_suspendable", "id", "key", "namespace", "session_id")

    def __init__(
        self,
        deck: Deck,
        *,
        id: str,
        key: str | None,
        namespace: str | None,
        session_id: str | None,
        context: object,
        seen: RunStatus,
        suspendable: bool,
    ) -> None:
        self._deck = deck
        self.id = id
        self.key = key
        self.namespace = namespace
        self.session_id = session_id
        self._context = context
        self._seen = seen
        self._suspendable = suspendable

    @property
    def can(self) -> Controls:
        """Which of :meth:`pause`, :meth:`resume` and :meth:`cancel` are available right now.

        Informational, and read off the status this handle last saw rather than the store: it is
        for a UI's button states and for branching, and the run may end between reading it and
        acting on it. The lifecycle methods below are the authoritative answer, which is why they
        raise rather than returning a second, weaker one.
        """
        return can_of(self._seen, suspendable=self._suspendable)

    def _rc(self) -> RunContext:
        return RunContext(run_id=self.id, session_id=self.session_id, namespace=self.namespace)

    async def status(self) -> RunStatus:
        """This run's current status. Never ``None``: a handle only ever names a run that has
        at least started, or one this deck is still opening."""
        runtime = self._deck._require_open()
        status = await runtime.status(self.id, namespace=self.namespace)
        if status is None:
            # The one handle that can outrun its own ``run.started``: ``ctx.invoke`` mints the id
            # and returns before the claim lands, so this window is a run that is opening.
            opening = self._deck._executions.get(self.id)
            assert opening is not None and not opening[1].done(), (
                f"a Run handle always names a run that exists: {self.id!r}"
            )
            status = RunStatus.RUNNING
        self._seen = status
        return status

    async def pause(self, reason: str | None = None) -> None:
        """Ask the run to stop at its next safe point, and record why  -  recorded, not stopped:
        a run inside a tool call stops at its own next safe point, and its own ``run.paused``
        event is what reports that it did.

        Strict: :class:`~agentdeck.errors.RunStateError` if the run's state refuses a pause,
        :class:`~agentdeck.errors.UnsupportedControlError` if it can never take one. Returns
        quietly when there is nothing to do, which is a run already paused or already over.
        """
        if not await self._admits(Operation.PAUSE):
            return
        if not await self._deck._pause(self.id, reason, self.namespace):
            raise UnsupportedControlError(_NO_CONTROL_PORT.format(verb="pause"))

    async def resume(self) -> None:
        """Continue a paused run.

        Strict, on the same terms as :meth:`pause`: a run waiting for an answer refuses (call
        :meth:`answer` instead), and one already running or already over returns quietly.
        """
        if not await self._admits(Operation.RESUME):
            return
        await self._deck._resume(self.id, context=self._context, namespace=self.namespace)

    async def cancel(self, reason: str | None = None) -> None:
        """Ask the run to stop for good. A live run stops at its next safe point; one already
        paused or waiting on an answer has none left to reach, so it ends immediately instead.

        Strict, on the same terms as :meth:`pause`. A run that has already ended returns
        quietly: it is stopped, which is what was asked for.
        """
        if not await self._admits(Operation.CANCEL):
            return
        if not await self._deck._cancel(self.id, reason, self.namespace):
            raise UnsupportedControlError(_NO_CONTROL_PORT.format(verb="cancel"))

    async def _admits(self, operation: Operation) -> bool:
        """Whether ``operation`` is worth attempting, raising if it is refused outright.

        Reads the live status rather than :attr:`can`  -  which is the snapshot of whatever this
        handle last saw, and deliberately not authoritative. Refreshing it here is what keeps
        the two from drifting for a caller that checks ``can`` after every op.
        """
        if operation is not Operation.CANCEL and not self._suspendable:
            raise UnsupportedControlError(
                f"run {self.id!r} cannot {operation.value}: the engine running it does not suspend, so "
                f"there is no pause to record or lift. Cancelling it is what ends it early; "
                f"`run.can.{operation.value}` says so before the call."
            )
        allowed = PRECONDITIONS[await self.status(), operation]
        if allowed.verdict is Verdict.REFUSED:
            raise RunStateError(f"run {self.id!r} cannot {operation.value}: {allowed.why}")
        return allowed.verdict is Verdict.LEGAL

    async def pending(self) -> InterruptResult | None:
        """What this run is waiting to be answered about, or ``None`` if it is not
        ``WAITING_ANSWER``."""
        for run in await self._deck._pending(self.namespace):
            if run.run_id == self.id:
                return interrupt_result(run.payload, run.thread_id, id=self.id)
        return None

    async def answer(self, value: Any) -> None:
        """Answer the interrupt this run is paused on. Raises ``RunStateError`` if it is not,
        in fact, waiting for one  -  paused instead (call :meth:`resume`), or already over.

        Raises ``ValueError`` if ``value`` is not something the log can carry, before anything is
        claimed, so the run stays answerable: an answer the log cannot hold would resume the run on
        a value no replay and no other process could reproduce.

        ``value`` is resupplied against the context :meth:`Runs.start` was given, not
        recovered: the interrupted run's own copy was never written to the log, so a node that
        read it before the interrupt reads ``None`` on this replay if this handle has none
        either.
        """
        await self._deck._answer(self.id, value, context=self._context, namespace=self.namespace)

    def events(self, *, from_seq: int = 0, follow: bool = False) -> AsyncIterator[Event]:
        """This run's own canonical events, from ``from_seq``.

        ``follow=False`` (the default) is a snapshot: whatever the log holds right now, once.
        ``follow=True`` tails it like :meth:`Deck.stream`  -  replaying what already happened and
        then waiting on whatever comes next, whether or not this process is the one executing
        it. Neither ever advances the run: reading is not driving.

        A follow ends at its own segment's boundary  -  a terminal event or a suspension  -  the
        same as :meth:`Deck.stream`. Following a run that was later resumed past an interrupt
        replays only up to that interrupt; call again (or use ``follow=False``) to see what
        came after.
        """
        if follow:
            return self._deck._events(self._rc(), from_seq=from_seq)
        return self._snapshot(from_seq)

    async def _snapshot(self, from_seq: int) -> AsyncIterator[Event]:
        store = self._deck._require_open().store
        for event in await store.read_run(self._rc(), from_seq=from_seq):
            yield event

    def __await__(self) -> Generator[Any, None, Any]:
        """The result: a :class:`TurnResult` for an agent, the body's own return value for a
        workflow. Blocks while the run is still ``RUNNING``, wherever it is executing.

        Raises rather than blocks once the run has stopped without finishing: ``RunStateError``
        (as a :class:`~agentdeck.errors.RunSuspendedError`, carrying ``.pending``) for
        ``PAUSED``/``WAITING_ANSWER``  -  there is no timeout parameter to wait either out, and
        the caller who wants one polls :meth:`status`/:meth:`pending` instead of blocking
        forever on nobody ever calling :meth:`resume`/:meth:`answer`  -  and ``RuntimeError`` for
        ``CANCELLED`` or a ``FAILED`` this process did not itself execute (one it did raises the
        engine's own exception instead, the same as ``await`` on the task driving it always
        would).
        """
        return self._result().__await__()

    async def _result(self) -> Any:
        """Wait out ``RUNNING``, however many resumed segments that takes, then read the run's
        own true last event and decide the outcome from it.

        Not :meth:`Deck._events`: that reads one *segment* and stops at its own first
        terminal-or-suspended boundary, which is exactly wrong here  -  a run this handle's
        :meth:`answer` already drove past its interrupt has that ``run.interrupted`` sitting
        earlier in the very same log, and a segment read from ``seq`` 0 stops there without
        ever reaching the ``run.completed`` that came after it. Folding :meth:`Runtime.status`
        instead is what a segment boundary cannot fool: it is the run's current state, however
        many segments produced it, and this loop only re-checks it, never a stale one.
        """
        deck = self._deck
        runtime = deck._require_open()
        while True:
            if await self.status() is not RunStatus.RUNNING:
                break
            execution = deck._executions.get(self.id)
            if execution is not None:
                # The real exception, traceback included  -  the same settle `Deck.run()`
                # already does before it ever reads the log, so a run this process is
                # executing raises its own failure rather than a `RuntimeError`
                # reconstructed from `run.failed`.
                await execution[1]
            else:
                await asyncio.sleep(_FOLLOW_POLL_INTERVAL)
        events = await runtime.store.read_run(self._rc())
        last = events[-1] if events else None
        if last is None:
            raise RuntimeError(f"run {self.id!r} has no events recorded at all.")
        payload = last.payload
        if isinstance(payload, RunCompleted):
            return _completed_result(deck, last, payload)
        if isinstance(payload, RunFailed):
            raise RuntimeError(f"run {self.id!r} failed: {payload.message}")
        if isinstance(payload, RunCancelled):
            raise RuntimeError(f"run {self.id!r} was cancelled: {payload.reason}")
        if isinstance(payload, RunInterrupted):
            pending = interrupt_result(payload.payload, payload.thread_id or "", id=self.id)
            raise RunSuspendedError(self.id, RunStatus.WAITING_ANSWER, pending=pending)
        if isinstance(payload, RunPaused):
            raise RunSuspendedError(self.id, RunStatus.PAUSED)
        raise RuntimeError(f"run {self.id!r} ended on {last.kind!r}, which awaiting it does not recognize.")


class Runs:
    """``deck.runs``  -  the collection that finds or starts a :class:`Run`. Three operations,
    and no per-run operation duplicated here: once a caller holds a :class:`Run`, every op
    that acts on it lives on the handle itself (docs/design/run-identity.md §3).

    Lives in ``agentdeck/deck.py`` rather than its own module, for the same reason
    :class:`Run` does: both need ``Deck``'s private machinery, and ``Deck`` needs
    :class:`Runs` back for :attr:`Deck.runs`, so splitting either across modules would be a
    circular import for no gain.
    """

    __slots__ = ("_deck",)

    def __init__(self, deck: Deck) -> None:
        self._deck = deck

    async def start(
        self,
        name: str,
        input: Any,
        *,
        session_id: str | None = None,
        namespace: str | None = None,
        key: str | None = None,
        context: object = None,
    ) -> Run:
        """Begin one run and hand back a live handle to it. Execution continues in a
        deck-owned task regardless of whether this handle, or any other, is ever read or
        awaited (docs/design/run-identity.md §9)  -  conceptually the same admission
        :meth:`Deck.run` makes, just handed back as a :class:`Run` instead of awaited inline.

        Raises ``SessionBusyError`` naming the run that holds ``session_id`` while it is
        ``RUNNING``, ``PAUSED`` or ``WAITING_ANSWER``  -  the three states that hold a session  -
        and ``DuplicateKeyError`` naming the run that already holds ``(namespace, key)``: a
        duplicate start never replays the run that holds the key, it refuses.
        """
        deck = self._deck
        root = deck._root(name)
        deck._require_open()
        content = _content_for(root, input)
        opening, _task = await deck._start(
            name, content, context=context, session_id=session_id, namespace=namespace, key=key
        )
        return Run(
            deck,
            id=opening.run_id,
            key=key,
            namespace=opening.namespace,
            session_id=opening.session_id,
            context=context,
            seen=RunStatus.RUNNING,
            suspendable=deck._require_open().suspends(name),
        )

    async def get(self, id: str | None = None, *, namespace: str | None = None, key: str | None = None) -> Run:
        """Rehydrate a handle to a run that already exists. Never creates, starts, resumes,
        claims ownership or moves lifecycle state  -  returns a run in any state, terminal
        included, and raises ``NotFoundError`` for one this namespace has never heard of. No
        fuzzy search, no cross-namespace guessing.

        Two forms, exactly one of them: the canonical ``id`` (optionally scoped to a
        non-default ``namespace``, the same keyword every other op on this collection takes),
        or ``namespace=``/``key=``, the application identity :meth:`start` adopted it under.

        Takes no ``context``: a run recovered this way has durable identity and durable state,
        never the ephemeral value a live process held for it  -  see :meth:`start`.
        """
        if (id is None) == (key is None):
            raise ValueError("deck.runs.get(...) takes exactly one of a positional id, or key=.")
        runtime = self._deck._require_open()
        if key is not None:
            lookup = RunContext(run_id=str(uuid.uuid4()), namespace=namespace)
            id = await runtime.store.find_by_key(lookup, key)
            if id is None:
                raise NotFoundError(f"No run in namespace {namespace!r} has claimed key {key!r}.")
        assert id is not None  # the branch above always sets it when key was given instead
        summary = await runtime.find(id, namespace=namespace)
        if summary is None:
            raise NotFoundError(f"No run {id!r} in namespace {namespace!r}.")
        return Run(
            self._deck,
            id=id,
            key=key,
            namespace=namespace,
            session_id=summary.session_id,
            context=None,
            seen=summary.status,
            suspendable=_RECOVERED_SUSPENDABLE,
        )

    # The return type says `builtins.list`, not the bare name: this method is itself named
    # `list`, and a checker resolving the annotation against this class's own namespace would
    # find the method before the type.
    async def list(
        self, *, namespace: str | None = None, status: RunStatus | None = None, limit: int | None = None
    ) -> builtins.list[Run]:
        """Every run in ``namespace``, optionally narrowed to one ``status`` and capped at
        ``limit``. Stays scoped to one namespace  -  no cross-namespace listing
        (docs/design/run-identity.md §15); an operator view spanning several is a caller-side
        loop over this, not a parameter here.
        """
        runtime = self._deck._require_open()
        ctx = RunContext(run_id=str(uuid.uuid4()), namespace=namespace)
        summaries = await runtime.store.list_runs(ctx, status=status, limit=limit)
        return [
            Run(
                self._deck,
                id=summary.run_id,
                key=None,
                namespace=namespace,
                session_id=summary.session_id,
                context=None,
                seen=summary.status,
                suspendable=_RECOVERED_SUSPENDABLE,
            )
            for summary in summaries
        ]


__all__ = ["Deck", "Run", "TurnResult"]
