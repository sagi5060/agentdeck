"""Single entry point: one object that discovers and serves a project's
agents, workflows, and skills.

    from agentdeck import App

    app = App()                   # serves ./.agentdeck: agents/<bundle>/agent.py,
    app.load()                    # workflows/<bundle>/workflow.py and a skills/ dir

    app.agents.get("FileAgent")                       # BaseAgent subclass
    app.workflows.get("TranslateAndSummarize")        # BaseWorkflow subclass
    app.skills.get("md-segment-translate")            # SkillBundle

    result = await app.run_agent("FileAgent", "hello")   # TurnResult: .output, .usage
    state  = await app.run_workflow("TranslateAndSummarize", {"source_path": p})

``load()`` eagerly imports every bundle, builds every agent, and compiles
every workflow graph, so configuration errors surface at startup instead of
mid-conversation; the turn-starting methods below call it themselves on first
use, so skipping it by hand costs nothing. Every turn any of them runs plays on
the same Runtime the HTTP surface does, and is recorded the same way — read it
back with :attr:`App.store`.

For anything long-running — and for every deployment using Redis sessions or
MCP servers — prefer ``App.open()``: it runs ``load()``, starts the MCP
lifecycle, and guarantees ``aclose()`` on exit, so the Redis client and MCP
servers are never leaked::

    async with App.open() as app:
        turn = await app.chat("FileAgent", session_id="wa-123", message="hi")
"""

from __future__ import annotations

import uuid
from contextlib import aclosing, asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from agentdeck.adapters.engines.langgraph import LangGraphEngine
from agentdeck.adapters.engines.openai_agents import ExecutionStore, OpenAIAgentsEngine, SessionFactory
from agentdeck.adapters.tools.mcp.lifecycle import MCPLifecycle
from agentdeck.agents.registry import AgentRegistry
from agentdeck.composition import (
    build_runtime,
    resolve_agent_sandbox,
    resolve_checkpoint,
    resolve_run_settings,
    resolve_workflow_workspace,
)
from agentdeck.core.content import DataBlock, TextBlock, coerce_input
from agentdeck.core.context import RunContext
from agentdeck.core.control import Signal
from agentdeck.core.events import Custom, NodeUpdated, RunCompleted, RunInterrupted
from agentdeck.errors import ConfigError, NotFoundError
from agentdeck.runtime.registry import PROJECT_DIR, _package_dir, mount_project_dir
from agentdeck.runtime.settings import Settings, get_settings
from agentdeck.skills.bundle import SkillRegistry
from agentdeck.workflows.interrupts import interrupt_result
from agentdeck.workflows.registry import WorkflowRegistry
from agentdeck.workflows.timers import wake_at_of

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator

    from agents.memory.session import Session

    from agentdeck.core.events import Event, Usage
    from agentdeck.core.ports import EventStorePort
    from agentdeck.runtime.service import PendingRun, Runtime
    from agentdeck.workflows.interrupts import InterruptResult

# Every Python call through App mints its own context: one tenant/principal, since this
# facade has no auth story of its own yet — a real per-caller principal arrives with a real
# auth layer, not here. Mirrors what v1's HTTP compat layer does for the same reason
# (``surfaces/serve/compat.py``'s ``V1_TENANT``/``V1_PRINCIPAL``), duplicated rather than
# imported so `App` depends on nothing under `surfaces/`; a pinned test keeps the two equal,
# because the store buckets its log by ``(tenant, log_key)`` and a drift here would split one
# session's history into two logs depending on which entry point ran the turn.
_TENANT = "local"
_PRINCIPAL = "user:local"

# The openai-agents engine namespaces a validated ``output_type`` result here as well as
# putting it on ``RunCompleted.output`` as a ``DataBlock``, because v1's wire can only carry
# text and the HTTP surface reads the custom event to build it. Spelled out rather than
# imported, the same reason ``surfaces/serve/compat.py`` spells out its own copy: a facade
# that imported an adapter would invert the direction the wiring depends on, and a pinned
# test keeps the two equal.
_LEGACY_STRUCTURED_OUTPUT = "openai_agents.structured_output"


def _require_aware(now: datetime) -> datetime:
    if now.tzinfo is None:
        raise ValueError(f"due_resumes/tick require a timezone-aware `now`; got naive {now!r}.")
    return now


@dataclass(frozen=True, slots=True)
class TurnResult:
    """One agent turn's outcome, assembled from its own ``run.completed`` — never the SDK's
    own result object, so a caller depends on agentdeck's event schema rather than on
    whichever engine ran the turn.

    ``run_id`` (and ``session_id``, for a conversational turn) name the run this came from,
    so a caller who wants more than ``output`` and ``usage`` can read the rest of it back
    with :attr:`App.store` instead of this object growing a field for everything the log
    already carries.
    """

    output: Any
    usage: Usage
    run_id: str
    session_id: str | None = None


def _new_context(session_id: str | None = None) -> RunContext:
    return RunContext(
        tenant=_TENANT,
        principal=_PRINCIPAL,
        run_id=str(uuid.uuid4()),
        trace_id=str(uuid.uuid4()),
        session_id=session_id,
    )


def _resume_context(paused: PendingRun) -> RunContext:
    """The context that continues an already-open run: its ``run_id`` is the paused run's
    own, because that is the run whose ``WAITING_HUMAN`` -> ``RUNNING`` claim the resume has
    to win — a fresh id would name a run the log has never heard of."""
    return RunContext(
        tenant=_TENANT,
        principal=_PRINCIPAL,
        run_id=paused.run_id,
        trace_id=str(uuid.uuid4()),
        session_id=paused.session_id,
    )


async def _turn_result(events: AsyncGenerator[Event, None], ctx: RunContext) -> TurnResult:
    """A run's own ``run.completed`` (plus whatever it names, en route), as a :class:`TurnResult`.

    Drains ``events`` to its natural end rather than returning the moment ``run.completed``
    is seen: closing the Runtime's generator any earlier throws ``GeneratorExit`` into it one
    line before it notices its own terminal event, which is what the Runtime's own "abandoned
    mid-stream" handling is for — and it would record a spurious ``run.cancelled`` right
    after the real ``run.completed``, over a run that in fact finished cleanly.

    Raises if the stream ends without one: the engine's own exception already reached the
    caller in that case (the Runtime records ``run.failed`` before re-raising), so the only
    way this is actually hit is a run suspended by a pause or a cancel racing this call —
    genuinely new once a Python turn plays on the Runtime, since nothing outside HTTP could
    signal one before.
    """
    structured: Any = None
    result: TurnResult | None = None
    async with aclosing(events):
        async for event in events:
            payload = event.payload
            if isinstance(payload, Custom) and payload.name == _LEGACY_STRUCTURED_OUTPUT:
                structured = payload.data.get("output")
            elif isinstance(payload, RunCompleted):
                data = next((block.data for block in payload.output if isinstance(block, DataBlock)), None)
                if data is not None:
                    output = data
                elif structured is not None:
                    output = structured
                else:
                    output = "".join(block.text for block in payload.output if isinstance(block, TextBlock))
                result = TurnResult(output=output, usage=payload.usage, run_id=ctx.run_id, session_id=ctx.session_id)
    if result is None:
        raise RuntimeError(
            f"run {ctx.run_id!r} ended without completing (paused or cancelled) — resume it with "
            "App.resume_run, or inspect App.store for what happened."
        )
    return result


async def _workflow_result(events: AsyncGenerator[Event, None]) -> tuple[Any, bool]:
    """A workflow run's final state (or the interrupt it paused on), plus whether the graph
    actually did anything for this call.

    Mirrors ``surfaces/serve/compat.py``'s own ``_terminal`` (duplicated rather than
    imported, for the same reason the context above is): a lost resume claim or a thread
    already at ``END`` both produce an empty or update-free stream, and ``applied`` is what
    keeps that from reading as the stale success langgraph would otherwise hand back.
    """
    result: Any = None
    applied = False
    async with aclosing(events):
        async for event in events:
            payload = event.payload
            if isinstance(payload, RunInterrupted):
                result, applied = interrupt_result(payload.payload, payload.thread_id or ""), True
            elif isinstance(payload, NodeUpdated):
                applied = True
            elif isinstance(payload, RunCompleted):
                result = next((block.data for block in payload.output if isinstance(block, DataBlock)), None)
    return result, applied


@dataclass(slots=True)
class App:
    """Facade over the three plug-in registries plus settings.

    Always serves the ``./.agentdeck`` project dir of the current working
    directory: ``agents/<bundle>/agent.py``, ``workflows/<bundle>/workflow.py``, ``skills/``.
    """

    agents: AgentRegistry = field(init=False)
    workflows: WorkflowRegistry = field(init=False)
    skills: SkillRegistry = field(init=False)
    # DI seam for tests: pass a prebuilt factory (or one wrapping fakeredis) to skip
    # `from_settings`'s real Redis client entirely.
    session_factory: SessionFactory | None = None
    inventory: dict[str, list[str]] = field(init=False, default_factory=dict)
    _sessions: ExecutionStore = field(init=False)
    _closed: bool = field(init=False, default=False)
    _started_mcp: bool = field(init=False, default=False)
    _runtime: Runtime | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        package = mount_project_dir()
        self.agents = AgentRegistry(package)
        self.workflows = WorkflowRegistry(package)
        self.skills = SkillRegistry((_package_dir(package) or Path(PROJECT_DIR)) / "skills")
        if self.session_factory is None:
            self.session_factory = SessionFactory.from_settings(self.settings.session)
        # One conversation memory for this process, whether the turn arrived here or through
        # HTTP: both play on the Runtime below, so both reach the engine's own store.
        self._sessions = ExecutionStore(self.session_factory)

    @property
    def settings(self) -> Settings:
        return get_settings()

    @property
    def runtime(self) -> Runtime:
        """The wired Runtime this App composed — what the HTTP surface runs chats on.

        Built by :meth:`load`, because a Runtime over a project that cannot be discovered
        is not something to hand a surface.
        """
        if self._runtime is None:
            raise ConfigError("no Runtime yet: call App.load() (or use App.open()) first.")
        return self._runtime

    @property
    def store(self) -> EventStorePort:
        """The event log every recorded turn appends to — the read side of what
        :meth:`run_agent`, :meth:`chat`, :meth:`run_workflow` and :meth:`resume_workflow`
        write to. Read a turn back with ``await app.store.read(log_key, ctx)``, where
        ``log_key`` is a :class:`TurnResult`'s ``session_id`` (or ``run_id``, for a
        session-less run) and ``ctx`` is any :class:`~agentdeck.core.context.RunContext`
        of this App's tenant.

        Same lifetime rule as :attr:`runtime`: composed by :meth:`load`.
        """
        return self.runtime.store

    def load(self) -> dict[str, list[str]]:
        """Discover and *instantiate* everything; raises on the first broken bundle.

        Returns ``{"agents": [...], "workflows": [...], "skills": [...]}`` and stashes
        it on ``self.inventory`` so callers don't have to re-run the compile pass, and
        composes the Runtime the HTTP surface serves from.
        """
        agents = self.agents.list(refresh=True)
        workflows = self.workflows.list(refresh=True)
        skills = self.skills.list(refresh=True)
        for agent_cls in agents.values():
            agent_cls.build()
        for wf_cls in workflows.values():
            wf_cls.build()  # compiles + caches the LangGraph graph
        for bundle in skills.values():
            bundle.output_schema  # noqa: B018 — imports/validates the declared schema
        self.inventory = {
            "agents": sorted(agents),
            "workflows": sorted(workflows),
            "skills": sorted(skills),
        }
        # One assembly seam, one caller: everything this App hands a surface comes from
        # `build_runtime`, so a second front door adds a caller instead of a second wiring.
        self._runtime = build_runtime(
            engines=(
                OpenAIAgentsEngine(
                    self._sessions,
                    settings=resolve_run_settings(),
                    sandbox=resolve_agent_sandbox(),
                ),
                LangGraphEngine(
                    durable_checkpoint=resolve_checkpoint(),
                    workspace=resolve_workflow_workspace(),
                ),
            )
        )
        return self.inventory

    def _ensure_runtime(self) -> Runtime:
        """The Runtime a turn-starting method plays on, composed via :meth:`load` on first
        use — so ``App().chat(...)`` without an explicit ``load()`` still gets a fully
        discovered project instead of :attr:`runtime`'s ``ConfigError``. The recorded path
        has to be at least as convenient as the one it replaces, or the old shape survives
        by being easier.
        """
        if self._runtime is None:
            self.load()
        return self.runtime

    async def run_agent(self, name: str, message: Any) -> TurnResult:
        """One-shot run of a discovered agent, recorded on the Runtime like every other
        turn — read it back with :attr:`store`.
        """
        self.agents.get(name)  # v1's message ("No agent named ...") if it doesn't exist
        runtime = self._ensure_runtime()
        ctx = _new_context()
        return await _turn_result(runtime.run(name, coerce_input(message), ctx), ctx)

    async def run_workflow(self, name: str, state: Any = None, *, thread_id: str | None = None) -> Any:
        """One run of a discovered workflow, recorded on the Runtime; returns the final state.

        ``thread_id`` scopes the run's session — required for a ``durable=True`` workflow
        (so a later call with the same id resumes it), ignored otherwise.

        A run that stops on ``langgraph.types.interrupt()`` returns an
        :class:`~agentdeck.workflows.interrupts.InterruptResult`
        (``{"type": "interrupt", "payload": ..., "thread_id": ...}``) instead of a final
        state; feed the human's answer back with :meth:`resume_workflow`.
        """
        self.workflows.get(name)  # v1's message ("No workflow named ...") if it doesn't exist
        runtime = self._ensure_runtime()
        # `None`'s default meaning here is "no updates", which a data block can only carry
        # as `{}` — `DataBlock(data=None)` would reach the langgraph engine as a null state
        # and fail its own "must be a JSON object" check.
        run = runtime.run(name, [DataBlock(data=state if state is not None else {})], _new_context(thread_id))
        result, _ = await _workflow_result(run)
        return result

    async def resume_workflow(self, name: str, thread_id: str, value: Any) -> Any:
        """Answer the interrupt paused on ``thread_id``; returns the final state or the next interrupt.

        The interrupted node re-runs from its start with ``interrupt()`` returning ``value``,
        so anything it did before pausing happens twice — keep interrupt nodes pure.
        """
        self.workflows.get(name)
        runtime = self._ensure_runtime()
        paused = await self._paused_workflow_run(runtime, name, thread_id)
        result, applied = await _workflow_result(runtime.resume(name, thread_id, value, _resume_context(paused)))
        if not applied:
            # Either the claim went to somebody else between the listing above and this
            # resume, or the thread was already at `END` and langgraph replayed its stale
            # final state without ever touching `value` — neither may be reported as success.
            raise NotFoundError(f"No paused run of {name!r} on thread {thread_id!r}.")
        return result

    async def _paused_workflow_run(self, runtime: Runtime, name: str, thread_id: str) -> PendingRun:
        paused = next(
            (
                run
                for run in await runtime.pending(_new_context())
                if run.invocable == name and run.thread_id == thread_id
            ),
            None,
        )
        if paused is None:
            raise NotFoundError(f"No paused run of {name!r} on thread {thread_id!r}.")
        return paused

    async def pending_interrupts(self, name: str | None = None) -> list[InterruptResult]:
        """Approval inbox: every thread paused on an interrupt, for one workflow or all of them."""
        workflows = [self.workflows.get(name)] if name else list(self.workflows.list().values())
        pending: list[InterruptResult] = []
        for workflow in workflows:
            pending.extend(await workflow.pending())
        return pending

    async def due_resumes(self, now: datetime | None = None) -> list[InterruptResult]:
        """Timer-paused threads (``sleep_until``) whose wake time has passed — a filtered
        view of :meth:`pending_interrupts`. ``now`` defaults to the current UTC time and
        must be timezone-aware if given.
        """
        now = _require_aware(now) if now is not None else datetime.now(UTC)
        pending = await self.pending_interrupts()
        return [p for p in pending if (wake_at := wake_at_of(p["payload"])) is not None and wake_at <= now]

    async def tick(self, now: datetime | None = None) -> list[Any]:
        """Resume every thread whose ``sleep_until`` timer is due; resume value is its wake
        timestamp. Callers own the schedule (cron, systemd timer, a loop) — agentdeck runs
        no daemon of its own.
        """
        now = _require_aware(now) if now is not None else datetime.now(UTC)
        results = []
        for workflow in self.workflows.list().values():
            for pending in await workflow.pending():
                wake_at = wake_at_of(pending["payload"])
                if wake_at is not None and wake_at <= now:
                    results.append(await workflow.resume(pending["thread_id"], wake_at))
        return results

    async def run_workflow_stream(
        self,
        name: str,
        state: Any = None,
        *,
        thread_id: str | None = None,
        **runner_options: Any,
    ) -> AsyncIterator[dict[str, Any] | InterruptResult]:
        """Streaming counterpart to :meth:`run_workflow`: a ``node_update`` event per completed
        node, a ``custom`` event per nested :class:`~agentdeck.workflows.nodes.AgentNode`'s text
        delta (or any :func:`~langgraph.config.get_stream_writer` call), then one terminal
        ``done`` event carrying the final state. Same ``thread_id`` semantics as ``run_workflow``.

        A run that pauses on ``langgraph.types.interrupt()`` ends with an
        :class:`~agentdeck.workflows.interrupts.InterruptResult` event instead of ``done``;
        answer it with :meth:`resume_workflow`.

        Unlike ``run_workflow``, this does not yet play on the Runtime: it drives the graph
        directly, so a run started here writes nothing to the event log and cannot be found
        by :meth:`resume_workflow`'s own lookup — start on :meth:`run_workflow` (or
        :meth:`resume_workflow`) for one thread if you need both the log and this stream.
        """
        async for event in self.workflows.get(name).run_stream(state, thread_id=thread_id, **runner_options):
            yield event

    def session_for(self, session_id: str) -> Session:
        """Conversation memory for ``session_id`` — Redis when ``AGENTDECK_SESSION_REDIS_URL``
        is set, otherwise an in-process SQLite session (dev/test fallback, lost on exit).

        The engine's own store, not a second one: a turn started here and a turn started over
        HTTP have to land in the same conversation, and they only do if there is one store and
        one key scheme. The key is tenant-scoped, which is why this goes through a context
        rather than the bare id.
        """
        return self._sessions.session_for(_new_context(session_id))

    async def pause_run(self, run_id: str, reason: str | None = None) -> bool:
        """Ask the run to stop at its next safe point, and record why.

        Returns whether the request was recorded — not whether the run has stopped, which at
        the moment of asking nobody can know: the run may be inside a tool call that has to
        return first. Watch the run's own events for ``run.paused`` to learn that it did. Both
        idempotent and race-free by construction: asking twice records one request, and asking
        after the run ended does nothing at all.
        """
        return await self.runtime.signal(run_id, Signal.PAUSE, reason)

    async def cancel_run(self, run_id: str, reason: str | None = None) -> bool:
        """Ask the run to stop for good at its next safe point. Same answer as
        :meth:`pause_run`, and the same reason for it; the run's ``run.cancelled`` is what says
        it happened. Cancellation is terminal — a cancelled run cannot be resumed.

        Cancelling a run that is already **paused** is honored by the next :meth:`resume_run`,
        which ends it instead of continuing it: a paused run has no loop reaching safe points, so
        nothing else can turn the request into an effect. The cancel is never lost and never
        overridden, but a paused run nobody picks up again stays paused.
        """
        return await self.runtime.signal(run_id, Signal.CANCEL, reason)

    async def resume_run(self, run_id: str, reason: str | None = None) -> list[Event]:
        """Continue a paused run, returning every event the continuation produced.

        Empty means nothing was resumed: this run is not paused — finished, cancelled, still
        running, or already picked up by somebody else. Unlike pause and cancel, resuming is
        not a signal a live run notices, because a paused run has no loop left to notice
        anything: this call plays it on, so it returns when the run does.
        """
        return [event async for event in self.runtime.resume_run(run_id, _new_context(), reason)]

    async def chat(self, name: str, session_id: str, message: Any) -> TurnResult:
        """One conversational turn: same ``session_id`` → same history across calls, recorded
        on the Runtime like every other turn — read it back with :attr:`store`.
        """
        self.agents.get(name)
        runtime = self._ensure_runtime()
        ctx = _new_context(session_id)
        return await _turn_result(runtime.run(name, coerce_input(message), ctx), ctx)

    async def chat_stream(self, name: str, session_id: str, message: Any) -> AsyncIterator[Event]:
        """Streaming counterpart to :meth:`chat`: yields the run's own canonical
        :class:`~agentdeck.core.events.Event`\\ s (``text.delta`` for each token, ``run.completed``
        last) instead of raw text and a :class:`~agentdeck.agents.runners.StreamDone` sentinel —
        the same events :attr:`store` would hand back after the fact, live as they're recorded.

        ``aclosing`` the Runtime's own generator, not just this one: a caller that walks away
        mid-stream throws ``GeneratorExit`` into *this* frame, and closing only it would
        abandon the Runtime's generator to the GC instead — which finalizes it in a fresh
        context, and leaves the run open in the log holding its session until
        ``stale_run_after`` gives it up.
        """
        self.agents.get(name)
        runtime = self._ensure_runtime()
        async with aclosing(runtime.run(name, coerce_input(message), _new_context(session_id))) as run:
            async for event in run:
                yield event

    @classmethod
    @asynccontextmanager
    async def open(cls, *, session_factory: SessionFactory | None = None) -> AsyncIterator[App]:
        """Build, ``load()``, and start the MCP lifecycle; ``aclose()`` runs on exit (even on error).

            async with App.open() as app:
                ...

        ``session_factory`` is the DI seam for tests (e.g. a fake wrapping fakeredis).
        """
        app = cls(session_factory=session_factory)
        try:
            app.load()
            await MCPLifecycle.startup()
            app._started_mcp = True
            yield app
        finally:
            await app.aclose()

    async def aclose(self) -> None:
        """Flush the Runtime's sinks, then close the Redis session client and MCP servers.

        Idempotent — safe to call twice.
        """
        if self._closed:
            return
        self._closed = True
        try:
            if self._runtime is not None:
                # queued sink emits die with the event loop otherwise, losing the last
                # few audit/cost events of the process
                await self._runtime.drain()
            await self._sessions.aclose()
        finally:
            # the MCP registry is process-wide: only tear it down if this App started it
            if self._started_mcp:
                self._started_mcp = False
                await MCPLifecycle.shutdown()


__all__ = ["App", "TurnResult"]
