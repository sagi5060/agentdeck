"""Langfuse observability — the single owner of all tracing policy.

One primitive, one identity mechanism: :func:`trace_run` opens a single observation and
propagates the ambient session identity via Langfuse's native ``propagate_attributes``
(OpenTelemetry context). The Langfuse span processor then stamps ``session.id`` /
``user.id`` / ``trace.name`` onto *every* span created within, whether it comes from the
OpenAI Agents SDK (OpenInference), a LangGraph node, or a sandboxed skill that adopted the
``TRACEPARENT``/``BAGGAGE`` carriers. Called at the run root (agent turn / workflow) it
names the trace and carries its I/O; called again for a nested unit (a skill) it re-affirms
the same identity
and, because it becomes the current OTel span, makes that unit's sandboxed LLM calls nest
under a named span instead of floating up to the root. No per-LLM-call code, no bridge.

:func:`init_observability` instruments the OpenAI **Agents SDK** (OpenInference's
``OpenAIAgentsInstrumentor``) so every agent, generation, and — crucially — every TOOL
call (MCP, shell ``exec_command``, function tools, ``as_tool`` sub-agents) becomes a span
with its input/output, natively nested. Sandboxed skills add their own generations from the
subprocess (see below). :func:`trace_run` sits on top only to name the trace + carry
trace-level I/O + propagate the session; the tool/agent tree comes from the SDK.

The one cosmetic cost of SDK-level instrumentation is a couple of wrapper spans per run
(the SDK "workflow"/"turn" spans); the ``sandbox.*`` lifecycle noise is filtered out (see
:func:`_should_export_span`).

Three runtimes, fed here:

* **agents** (host) — a turn wraps ``Runner.run`` in :func:`trace_run` (``kind="agent"``);
  OpenInference nests the agent → generation → tool spans (and any ``as_tool`` sub-agent)
  under it, so the full orchestration + every tool result is visible.
* **workflows** (host) — a run wraps ``graph.ainvoke`` in :func:`trace_run`, and each skill
  invocation is wrapped again (``kind="tool"``, in :class:`~agentdeck.skills.SkillExecutor`),
  so every skill's LLM call nests under one named skill span under one run root. No
  LangChain callback needed.
* **sandboxed skills** (subprocess) — :func:`sandbox_trace_env` hands the skill the
  ``LANGFUSE_*`` env plus ``TRACEPARENT``/``BAGGAGE`` (of whatever span is current when the
  sandbox opens — the skill span); ``skill_runtime.install_tracing`` calls ``get_client()``
  on it, so the skill's spans nest under the caller's active span.

Disabled (no keys) → every function is a cheap no-op. Bootstrap is idempotent.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

from agentdeck.runtime.settings import LangfuseSettings, get_settings

if TYPE_CHECKING:
    from collections.abc import Generator

    from opentelemetry.sdk.trace import ReadableSpan  # ty: ignore[unresolved-import] — [observability] extra

    from agentdeck.runtime.capture import Capture

logger = logging.getLogger(__name__)

_initialized = False

# Langfuse observation types we open ourselves (a subset of Langfuse's ``as_type``).
ObservationKind = Literal["agent", "chain", "tool", "span"]

# Graceful degradation when Langfuse is unreachable. The OTLP HTTP exporter retries a
# failed batch with exponential backoff *up to its timeout*, logging a WARNING each try;
# against a dead backend that is pure noise that also blocks flush-on-exit (host + every
# skill subprocess). We bound the per-export timeout (seconds — the exporter reads this
# env, default 10) so retries give up fast, and raise the exporter's log level so the
# transient-retry WARNINGs are silent. A genuine, non-retryable config error (bad keys,
# 4xx) is logged at ERROR and still surfaces. Telemetry must never slow or spam a real run.
_EXPORT_TIMEOUT_ENV = "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT"
_EXPORT_TIMEOUT_SECONDS = "5"
_OTLP_EXPORTER_LOGGER = "opentelemetry.exporter.otlp.proto.http.trace_exporter"


def degrade_export_quietly() -> None:
    """Bound OTLP export retries and silence their transient-failure WARNINGs.

    Called host-side before the client is built and mirrored in the sandbox (the subprocess
    reads :data:`_EXPORT_TIMEOUT_ENV` from :func:`sandbox_trace_env` and re-quiets the
    logger in ``skill_runtime.install_tracing``), so a down Langfuse degrades gracefully in
    every process. Idempotent.
    """
    os.environ.setdefault(_EXPORT_TIMEOUT_ENV, _EXPORT_TIMEOUT_SECONDS)
    logging.getLogger(_OTLP_EXPORTER_LOGGER).setLevel(logging.ERROR)


def _should_export_span(span: ReadableSpan) -> bool:
    """Drop the Agents SDK's ``sandbox.*`` lifecycle spans; keep everything else.

    ``SandboxSession`` wraps every op (``prepare_agent`` / ``running`` / ``start`` /
    ``exec``) in an ``@instrumented_op`` span that OpenInference forwards to Langfuse — pure
    infra noise (and a duplicate ``running`` on each turn) that buries the agent / generation
    / tool spans. Filtering by name is scope-agnostic (all Agents-SDK spans share one
    OpenInference scope) and never touches an LLM generation or a real tool call.
    """
    return not (span.name or "").startswith("sandbox.")


def init_observability(settings: LangfuseSettings | None = None) -> bool:
    """Start the Langfuse client + Agents-SDK instrumentation once. Returns whether active.

    Safe to call from every entry point (agent runner, workflow runner, CLI, skill executor):
    the first active call wins, the rest are no-ops. Returns ``False`` when Langfuse is disabled
    so callers can set ``RunConfig.tracing_disabled`` accordingly.
    """
    global _initialized
    if _initialized:
        return True
    lf = settings if settings is not None else get_settings().langfuse
    if not lf.enabled:
        return False

    from agents import set_trace_processors
    from langfuse import Langfuse  # ty: ignore[unresolved-import] — [observability] extra
    from openinference.instrumentation.openai_agents import (  # ty: ignore[unresolved-import] — [observability] extra
        OpenAIAgentsInstrumentor,
    )

    # Name the OTel resource before Langfuse builds its TracerProvider — ``Resource.create``
    # reads ``OTEL_SERVICE_NAME``, so this replaces the default ``unknown_service``. Respect
    # an operator-set value.
    os.environ.setdefault("OTEL_SERVICE_NAME", lf.service_name)
    # Bound + quiet the exporter before it is built, so a down Langfuse can't slow or spam a run.
    degrade_export_quietly()

    # Constructing the client registers Langfuse's span processor on the global OTel
    # TracerProvider (it also stamps the propagated session/user/name onto every span it
    # sees, and drops the ``sandbox.*`` lifecycle noise via ``should_export_span``).
    Langfuse(
        public_key=lf.public_key,
        secret_key=lf.secret_key,
        base_url=lf.endpoint,
        environment=lf.environment,
        debug=lf.debug,
        sample_rate=lf.sample_rate,
        should_export_span=_should_export_span,
    )
    # Instrument the Agents SDK: OpenInference maps every agent / generation / tool call
    # (MCP, shell, function tools, ``as_tool`` sub-agents) to a Langfuse span with its
    # input/output — the tool visibility a knowledge/orchestrator agent needs. Drop the SDK's
    # default OpenAI-backend trace exporter first; let OpenInference append its own processor.
    set_trace_processors([])
    OpenAIAgentsInstrumentor().instrument(exclusive_processor=False)
    _initialized = True
    logger.info(
        "Langfuse observability active (service=%s, endpoint=%s, environment=%s)",
        lf.service_name,
        lf.endpoint,
        lf.environment,
    )
    return True


def _identity(capture: Capture | None) -> tuple[str | None, str | None, dict[str, str]]:
    """Split a core ``Capture`` into Langfuse ``(session_id, user_id, metadata)``."""
    if capture is None:
        return None, None, {}
    session_id = str(capture.session_id) if capture.session_id is not None else None
    user_id = str(capture.author_id) if capture.author_id is not None else None
    metadata = {"actor": capture.actor.value} if capture.actor is not None else {}
    return session_id, user_id, metadata


@dataclass(slots=True)
class RunTrace:
    """Handle to the observation :func:`trace_run` opened. ``set_output`` fills its output.

    Langfuse derives a trace's input/output from its root observation, so a run opens one
    span (carrying the input) and the caller reports the result via :meth:`set_output`
    once it is known — including after a stream is fully consumed. Passing ``error`` marks
    the observation failed (Langfuse ``level="ERROR"``); the Langfuse field vocabulary
    stays here, the single owner. A no-op wrapper when Langfuse is disabled, so callers
    stay unconditional.
    """

    _span: Any = None

    def set_output(self, output: Any = None, *, error: str | None = None) -> None:
        if self._span is None:
            return
        if error is None:
            self._span.update(output=output)
        else:
            self._span.update(output=output, level="ERROR", status_message=error)


@contextmanager
def trace_run(
    capture: Capture | None = None,
    *,
    name: str,
    kind: ObservationKind = "chain",
    input: Any = None,
    session_id: str | None = None,
) -> Generator[RunTrace, None, None]:
    """Open one Langfuse observation for a run, session-tagged. Auto-detects root vs nested.

    At a **run root** (agent turn / workflow / standalone skill — nothing traced above it)
    this propagates the ambient session identity (``session.id`` / ``user.id`` / ``actor``
    from ``capture``) via Langfuse's native ``propagate_attributes`` and NAMES the trace.
    That identity + name then lands on every span created within — the LLM generations here,
    a sub-agent or skill nested below, sandboxed-skill spans via ``TRACEPARENT``/``BAGGAGE``
    — with no per-span code. Langfuse reads the trace's I/O off this root, so ``input``
    (here) and :meth:`RunTrace.set_output` give the trace its input/output.

    When called for a **nested unit** (a skill under a workflow, an ``as_tool`` sub-agent
    under an agent) it detects the active trace and only opens a child observation — it does
    NOT re-propagate, because re-setting ``trace_name`` would clobber the trace's name with
    the child's, and the session is already on the context. Becoming the current OTel span
    is what makes the unit's own LLM/sandbox calls nest under it instead of floating up.

    ``session_id``, when given, names the chat session at a run root (e.g. ``App.chat``'s
    caller-supplied id) and wins over the capture-derived one. No caller in the package
    supplies a ``capture`` any more — the telemetry sink reads identity off the event
    envelope instead — so it defaults to none and only a direct caller still has the hook.

    A cheap no-op when Langfuse is disabled.
    """
    if not _initialized:
        yield RunTrace()
        return
    from langfuse import get_client  # ty: ignore[unresolved-import] — [observability] extra
    from opentelemetry import trace as _otel  # ty: ignore[unresolved-import] — [observability] extra

    # Nested (already inside a traced run) → inherit identity + trace name; just add structure.
    if _otel.get_current_span().get_span_context().is_valid:
        with get_client().start_as_current_observation(name=name, as_type=kind, input=input) as span:
            yield RunTrace(span)
        return

    from langfuse import propagate_attributes  # ty: ignore[unresolved-import] — [observability] extra

    capture_session_id, user_id, metadata = _identity(capture)
    with (
        propagate_attributes(
            session_id=session_id or capture_session_id, user_id=user_id, trace_name=name, metadata=metadata or None
        ),
        get_client().start_as_current_observation(name=name, as_type=kind, input=input) as span,
    ):
        yield RunTrace(span)


def sandbox_trace_env(settings: LangfuseSettings | None = None) -> dict[str, str]:
    """The Langfuse env a sandboxed skill needs, or ``{}`` when disabled.

    A skill runs in its own process and can't inherit the host's config, so we hand it
    the same ``LANGFUSE_*`` env the SDK reads natively (``get_client()`` owns the OTLP
    endpoint, auth and flush), the shared ``OTEL_SERVICE_NAME``, and the W3C
    ``TRACEPARENT``/``BAGGAGE`` carriers so its spans nest under the caller's active span
    and keep Langfuse's trace attributes. A sandbox injects it alongside
    ``SANDBOX_CAPTURE``. Does not require :func:`init_observability`: a skill run outside an
    agent/workflow still exports, just as a root trace (no carrier to adopt).
    """
    lf = settings if settings is not None else get_settings().langfuse
    if not lf.enabled:
        return {}
    from opentelemetry.propagate import inject  # ty: ignore[unresolved-import] — [observability] extra

    # Standard LANGFUSE_* env — get_client() reads these natively; environment matches
    # the host client so skill spans land in the same Langfuse environment. The bounded
    # OTLP timeout rides along so a down backend can't stall the skill's flush-on-exit.
    env = {
        "LANGFUSE_HOST": lf.endpoint,
        "LANGFUSE_BASE_URL": lf.endpoint,
        "LANGFUSE_PUBLIC_KEY": lf.public_key,
        "LANGFUSE_SECRET_KEY": lf.secret_key,
        "LANGFUSE_TRACING_ENVIRONMENT": lf.environment,
        "OTEL_SERVICE_NAME": lf.service_name,
        _EXPORT_TIMEOUT_ENV: os.environ.get(_EXPORT_TIMEOUT_ENV, _EXPORT_TIMEOUT_SECONDS),
    }
    # The global W3C propagator carries the active span (trace-context) and Langfuse's
    # trace attributes (baggage) across the process edge in one shot.
    carrier: dict[str, str] = {}
    inject(carrier)
    if traceparent := carrier.get("traceparent"):
        env["TRACEPARENT"] = traceparent  # W3C trace-context; adopted as the sandbox span's parent
    if baggage := carrier.get("baggage"):
        env["BAGGAGE"] = baggage  # W3C baggage; keeps Langfuse trace attrs across the process edge
    return env


__all__ = ["RunTrace", "init_observability", "sandbox_trace_env", "trace_run"]
