"""Agent runners for graph nodes and tool wrappers: one-shot ``run`` and streamed ``run_streamed``."""

from __future__ import annotations

from contextlib import aclosing
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, cast

from agents import Runner, RunResult

from agentdeck.agents.runners.base import BaseRunner
from agentdeck.runtime.observability import trace_run

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, AsyncIterator


@dataclass(frozen=True, slots=True)
class StreamDone:
    """Terminal sentinel yielded by :meth:`HeadlessRunner.run_streamed` after the last delta.

    Carries what a streamed turn would otherwise lose: the SDK's own ``final_output``
    (validated model for an ``output_type`` agent, last assistant message otherwise —
    not the re-joined deltas, which disagree for tool-using agents) and the turn's
    token usage.
    """

    final_output: Any = None
    usage: dict[str, int] = field(default_factory=dict)


def _session_id(session: Any) -> str | None:
    """Pull the chat session id off the SDK session (``SQLiteSession``/``RedisSession``), if any."""
    return getattr(session, "session_id", None)


def _usage_of(result: Any) -> dict[str, int]:
    """Flatten the SDK's ``Usage`` into a JSON-safe dict (empty when unavailable)."""
    usage = getattr(getattr(result, "context_wrapper", None), "usage", None)
    if usage is None:
        return {}
    return {
        "requests": usage.requests,
        "input_tokens": usage.input_tokens,
        "output_tokens": usage.output_tokens,
        "total_tokens": usage.total_tokens,
    }


@dataclass(slots=True)
class HeadlessRunner(BaseRunner):
    """Single-invocation runner that joins or opens a sandbox."""

    async def run(self, message: Any = None, *, session: Any = None) -> RunResult:
        # One root observation carries the turn's identity + input/output; OpenInference's
        # spans nest under it. Nested inside a workflow run, this becomes a child of the
        # workflow's root span, re-affirming the same session.
        with trace_run(name=self.agent.name, kind="agent", input=message, session_id=_session_id(session)) as tr:
            async with self.attach_sandbox():
                result = await Runner.run(
                    self.agent,
                    message,
                    run_config=self.run_config,
                    max_turns=self.max_turns,
                    session=session,
                )
                tr.set_output(result.final_output)
                return result

    async def run_streamed(self, message: Any = None, *, session: Any = None) -> AsyncIterator[str | StreamDone]:
        """Async-generator counterpart to :meth:`run`: yields text deltas, then one :class:`StreamDone`.

        Same trace span, sandbox lifecycle, ``run_config``/``max_turns``/``session`` as
        ``run`` — they just span the whole generator instead of one await, since the
        trace's output (and the sandbox's teardown) can only happen once the caller has
        drained every delta.

        The SDK's run loop is a detached task, so an abandoned or closed generator must
        cancel it explicitly: ASGI servers don't ``aclose()`` a response body iterator on
        client disconnect, and without the cancel the turn would keep running while the
        sandbox is torn down under it.
        """
        with trace_run(name=self.agent.name, kind="agent", input=message, session_id=_session_id(session)) as tr:
            async with self.attach_sandbox():
                result = Runner.run_streamed(
                    self.agent,
                    message,
                    run_config=self.run_config,
                    max_turns=self.max_turns,
                    session=session,
                )
                # The SDK annotates stream_events() as AsyncIterator, but it is an async
                # generator — the cast is what lets aclosing() see its aclose().
                stream = cast("AsyncGenerator[Any, None]", result.stream_events())
                try:
                    async with aclosing(stream) as events:
                        async for event in events:
                            # Only the raw model text deltas matter for a chat UI; tool-call /
                            # handoff / agent-updated events are structural noise here.
                            if event.type == "raw_response_event" and event.data.type == "response.output_text.delta":
                                yield event.data.delta
                except BaseException as exc:  # includes GeneratorExit / CancelledError on abandonment
                    tr.set_output(error=f"{type(exc).__name__}: {exc}")
                    raise
                finally:
                    result.cancel()
                tr.set_output(result.final_output)
                # Deltas alone can't reconstruct the turn's result, so hand callers the
                # SDK's own final_output (plus usage) instead of making them re-derive it.
                yield StreamDone(final_output=result.final_output, usage=_usage_of(result))


__all__ = ["HeadlessRunner", "StreamDone"]
