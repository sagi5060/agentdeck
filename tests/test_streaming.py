"""chat_stream / run_streamed / the SSE endpoint: no live model, fakes the SDK boundary."""

import json
import sys
import textwrap
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest
from scripted_model import ScriptedModel, patch_provider, provider_of

from agentdeck.agents.runners.headless import HeadlessRunner, StreamDone

AGENT_PY = """
from agents import function_tool

from agentdeck.agents import BaseAgent


@function_tool
def lookup_slot(day: str) -> str:
    "Return the fixed free slot for a day."
    return f"{day} 09:00"


class Greeter(BaseAgent):
    instructions = "Greet the user."


class Tooler(BaseAgent):
    instructions = "Use the tool, then answer."
    tools = [lookup_slot]
"""


def _usage_frame(requests: int, input_tokens: int, output_tokens: int) -> dict[str, int]:
    """v1's aggregate usage dict, in v1's key order."""
    return {
        "requests": requests,
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": input_tokens + output_tokens,
    }


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(textwrap.dedent(AGENT_PY))
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]
    from agentdeck import App

    return App()


def _delta_event(text: str) -> SimpleNamespace:
    # Duck-types agents.stream_events.RawResponsesStreamEvent wrapping a
    # ResponseTextDeltaEvent — the only fields run_streamed reads.
    return SimpleNamespace(
        type="raw_response_event",
        data=SimpleNamespace(type="response.output_text.delta", delta=text),
    )


def _other_event() -> SimpleNamespace:
    # A non-text-delta event (tool call, handoff, ...) — must be skipped.
    return SimpleNamespace(type="run_item_stream_event", data=SimpleNamespace(type="tool_called"))


@dataclass
class FakeRunResultStreaming:
    """Duck-types ``agents.result.RunResultStreaming`` for the surface run_streamed uses."""

    events: list
    final_output: str
    cancelled: int = 0
    context_wrapper: object = field(
        default_factory=lambda: SimpleNamespace(
            usage=SimpleNamespace(requests=1, input_tokens=3, output_tokens=4, total_tokens=7)
        )
    )

    async def stream_events(self):
        for event in self.events:
            yield event

    def cancel(self, mode="immediate"):
        self.cancelled += 1


async def test_run_streamed_yields_deltas_incrementally(project, monkeypatch):
    agent_cls = project.agents.get("Greeter")
    runner = HeadlessRunner.from_agent(agent_cls.build())

    events = [_delta_event("Hel"), _other_event(), _delta_event("lo"), _delta_event("!")]
    fake_result = FakeRunResultStreaming(events=events, final_output="Hello!")
    captured_kwargs = {}

    def fake_run_streamed(agent, message, **kwargs):
        captured_kwargs.update(kwargs)
        return fake_result

    monkeypatch.setattr("agentdeck.agents.runners.headless.Runner.run_streamed", fake_run_streamed)

    sentinel_session = object()
    chunks = [c async for c in runner.run_streamed("hi", session=sentinel_session)]

    assert chunks[:-1] == ["Hel", "lo", "!"]
    # The turn ends with the SDK's own final_output + usage, not the re-joined deltas.
    assert chunks[-1] == StreamDone(
        final_output="Hello!",
        usage={"requests": 1, "input_tokens": 3, "output_tokens": 4, "total_tokens": 7},
    )
    # The detached SDK run loop is always cancelled once the generator is done with it.
    assert fake_result.cancelled == 1
    # run_config / max_turns / session are threaded through exactly like HeadlessRunner.run.
    assert captured_kwargs["run_config"] is runner.run_config
    assert captured_kwargs["max_turns"] == runner.max_turns
    assert captured_kwargs["session"] is sentinel_session


async def test_run_streamed_cancels_sdk_run_on_abandonment(project, monkeypatch):
    """A caller that stops mid-stream (client disconnect) must not leave the run loop alive."""
    agent_cls = project.agents.get("Greeter")
    runner = HeadlessRunner.from_agent(agent_cls.build())

    fake_result = FakeRunResultStreaming(events=[_delta_event("a"), _delta_event("b")], final_output="ab")
    monkeypatch.setattr(
        "agentdeck.agents.runners.headless.Runner.run_streamed",
        lambda agent, message, **kwargs: fake_result,
    )

    stream = runner.run_streamed("hi")
    assert await anext(stream) == "a"
    await stream.aclose()

    assert fake_result.cancelled == 1


async def test_chat_returns_a_turn_result_not_the_sdks_runresult(project, monkeypatch):
    """``chat`` used to hand back the SDK's own ``RunResult``; it now plays on the Runtime
    (issue #137) and returns a :class:`~agentdeck.app.TurnResult` assembled from the run's
    own ``run.completed`` — a caller depends on agentdeck's event schema, never on the SDK.
    """
    patch_provider(monkeypatch, provider_of(ScriptedModel(deltas=("echo:hi",))))

    result = await project.chat("Greeter", "s1", "hi")

    assert result.output == "echo:hi"
    assert result.session_id == "s1"


async def test_chat_stream_uses_same_session_as_chat(project, monkeypatch):
    """One ``session_id`` is one conversation whichever App method ran the turn — the same
    guarantee the old ``HeadlessRunner``-backed methods gave, now proven at the SDK boundary
    instead of by stubbing ``HeadlessRunner.from_agent`` directly (which ``chat``/``chat_stream``
    no longer call: both play on the Runtime)."""
    from agentdeck.core.events import RunCompleted

    model = ScriptedModel(deltas=("echo:hi",))
    patch_provider(monkeypatch, provider_of(model))

    events = [event async for event in project.chat_stream("Greeter", "s1", "first")]
    result = await project.chat("Greeter", "s1", "second")

    streamed_output = next(e.payload.output[0].text for e in events if isinstance(e.payload, RunCompleted))
    assert streamed_output == "echo:hi" == result.output
    # two model calls, and the second turn's input carries the first turn's own message —
    # proof the two App methods shared one `session_for("s1")` rather than each starting fresh.
    assert model.calls == 2
    assert "first" in str(model.inputs[-1])


def _sse_frames(text: str) -> list[tuple[str, dict]]:
    """Parse an SSE body into ``(event_name, data)`` pairs; unnamed frames are "message"."""
    frames = []
    for block in text.strip().split("\n\n"):
        name = "message"
        data = ""
        for line in block.splitlines():
            if line.startswith("event: "):
                name = line.removeprefix("event: ")
            elif line.startswith("data: "):
                data = line.removeprefix("data: ")
        frames.append((name, json.loads(data)))
    return frames


@pytest.fixture
def serve_client(project, monkeypatch):
    """TestClient over the real FastAPI app, with only the model scripted.

    The endpoint runs on the Runtime App composes, so the stub goes at the SDK boundary
    (v1's resolved provider) rather than at ``App.chat_stream``: everything in between is
    what these tests are about.
    """
    from fastapi.testclient import TestClient

    from agentdeck.serve import create_app

    def _client(model):
        patch_provider(monkeypatch, provider_of(model))
        # context manager runs the lifespan; without it every endpoint is 503
        return TestClient(create_app())

    return _client


def test_stream_endpoint_emits_deltas_then_done(serve_client):
    with serve_client(ScriptedModel(deltas=("Hel", "lo"), input_tokens=3, output_tokens=4)) as client:
        response = client.post("/agents/Greeter/chat?stream=true", json={"session_id": "s1", "message": "hi"})

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"
    assert _sse_frames(response.text) == [
        ("message", {"delta": "Hel"}),
        ("message", {"delta": "lo"}),
        # one model call, so `requests` is 1 and the totals are that call's
        ("done", {"output": "Hello", "usage": _usage_frame(1, 3, 4)}),
    ]


def test_stream_endpoint_counts_every_model_call_in_usage(serve_client):
    """``usage.requests`` is v1's count of model calls — two here, the tool round and the answer."""
    model = ScriptedModel(deltas=("done",), tool_name="lookup_slot", input_tokens=5, output_tokens=2)
    with serve_client(model) as client:
        response = client.post("/agents/Tooler/chat?stream=true", json={"session_id": "s1", "message": "hi"})

    assert model.calls == 2
    assert _sse_frames(response.text)[-1] == ("done", {"output": "done", "usage": _usage_frame(2, 10, 4)})


def test_stream_endpoint_rejects_missing_session_id(serve_client):
    with serve_client(ScriptedModel()) as client:
        response = client.post("/agents/Greeter/chat?stream=true", json={"message": "hi"})

    # 4xx before any header is sent — not a 200 that streams nothing.
    assert response.status_code == 422
    assert "session_id" in response.json()["detail"]


def test_stream_endpoint_reports_mid_stream_failure(serve_client):
    model = ScriptedModel(deltas=("par",), raises=RuntimeError("secret internal detail"))
    with serve_client(model) as client:
        response = client.post("/agents/Greeter/chat?stream=true", json={"session_id": "s1", "message": "hi"})

    frames = _sse_frames(response.text)
    assert frames[0] == ("message", {"delta": "par"})
    assert frames[-1] == ("error", {"error": "RuntimeError"})
    assert "secret internal detail" not in response.text
