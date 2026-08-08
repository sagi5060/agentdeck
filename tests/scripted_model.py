"""A scripted ``agents.models.interface.Model`` for the tests that drive a real chat turn.

The SDK boundary is the only thing stubbed: everything above it — v1's run config, the
compat engine, the Runtime, the surface's frame rendering — is the code under test. Patch
``agentdeck.agents.runners.base.OpenAIProvider`` with :func:`provider_of` so v1's resolved
``RunConfig`` hands out this model instead of reaching for a real endpoint.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from agents.items import ModelResponse
from agents.models.interface import Model
from agents.usage import Usage
from openai.types.responses import (
    Response,
    ResponseCompletedEvent,
    ResponseFunctionToolCall,
    ResponseOutputMessage,
    ResponseOutputText,
    ResponseTextDeltaEvent,
    ResponseUsage,
)
from openai.types.responses.response_usage import InputTokensDetails, OutputTokensDetails

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Sequence

MODEL_NAME = "fake-scripted"


def _usage(input_tokens: int, output_tokens: int) -> ResponseUsage:
    return ResponseUsage(
        input_tokens=input_tokens,
        input_tokens_details=InputTokensDetails(cached_tokens=0),
        output_tokens=output_tokens,
        output_tokens_details=OutputTokensDetails(reasoning_tokens=0),
        total_tokens=input_tokens + output_tokens,
    )


class ScriptedModel(Model):
    """Answers in ``deltas``; optionally calls ``tool_name`` once first, or raises mid-stream.

    ``inputs`` records what each call was handed, so a test can prove two turns shared one
    session without reaching into the session store.
    """

    def __init__(
        self,
        deltas: Sequence[str] = ("hi",),
        *,
        final_text: str | None = None,
        tool_name: str | None = None,
        raises: BaseException | None = None,
        hold: asyncio.Event | None = None,
        input_tokens: int = 3,
        output_tokens: int = 4,
    ) -> None:
        self.deltas = tuple(deltas)
        # The completed message, when it must differ from the joined deltas — which is how a
        # test tells "the SDK's final_output" apart from "the deltas, re-joined".
        self.final_text = final_text
        self.tool_name = tool_name
        self.raises = raises
        # Stall the turn after its first delta until `hold` is set, announcing it on `holding`.
        # A test that has to catch a consumer *inside* its next-event await needs the run to
        # stop where it says, not where a sleep happens to land.
        self.hold = hold
        self.holding = asyncio.Event()
        self.calls = 0
        self.inputs: list[Any] = []
        self._input_tokens = input_tokens
        self._output_tokens = output_tokens

    def _response(self, output: list[Any]) -> Response:
        return Response(
            id=f"resp_scripted_{self.calls}",
            created_at=0.0,
            model=MODEL_NAME,
            object="response",
            output=output,
            parallel_tool_calls=False,
            tool_choice="auto",
            tools=[],
            usage=_usage(self._input_tokens, self._output_tokens),
        )

    async def stream_response(self, _instructions: Any = None, input: Any = None, *_a: Any, **_k: Any) -> AsyncIterator:
        self.calls += 1
        self.inputs.append(input)
        if self.tool_name is not None and self.calls == 1:
            yield ResponseCompletedEvent(
                response=self._response(
                    [
                        ResponseFunctionToolCall(
                            id="fc_scripted_1",
                            call_id="call_scripted_1",
                            name=self.tool_name,
                            arguments="{}",
                            type="function_call",
                        )
                    ]
                ),
                sequence_number=0,
                type="response.completed",
            )
            return
        for index, delta in enumerate(self.deltas):
            if index and self.hold is not None:
                self.holding.set()
                await self.hold.wait()
            yield ResponseTextDeltaEvent(
                content_index=0,
                delta=delta,
                item_id="msg_scripted_1",
                logprobs=[],
                output_index=0,
                sequence_number=index,
                type="response.output_text.delta",
            )
        if self.raises is not None:
            raise self.raises
        text = self.final_text if self.final_text is not None else "".join(self.deltas)
        yield ResponseCompletedEvent(
            response=self._response(
                [
                    ResponseOutputMessage(
                        id="msg_scripted_1",
                        content=[ResponseOutputText(annotations=[], text=text, type="output_text")],
                        role="assistant",
                        status="completed",
                        type="message",
                    )
                ]
            ),
            sequence_number=len(self.deltas),
            type="response.completed",
        )

    async def get_response(self, _instructions: Any = None, input: Any = None, *_a: Any, **_k: Any) -> ModelResponse:
        self.calls += 1
        self.inputs.append(input)
        text = self.final_text if self.final_text is not None else "".join(self.deltas)
        return ModelResponse(
            output=[
                ResponseOutputMessage(
                    id="msg_scripted_1",
                    content=[ResponseOutputText(annotations=[], text=text, type="output_text")],
                    role="assistant",
                    status="completed",
                    type="message",
                )
            ],
            usage=Usage(
                requests=1,
                input_tokens=self._input_tokens,
                output_tokens=self._output_tokens,
                total_tokens=self._input_tokens + self._output_tokens,
            ),
            response_id="resp_scripted_1",
        )


def provider_of(model: Model) -> type:
    """A drop-in for ``OpenAIProvider`` that hands every lookup ``model``."""

    class _Provider:
        def __init__(self, **_kwargs: Any) -> None:
            pass

        def get_model(self, _name: str | None = None) -> Model:
            return model

    return _Provider


PROVIDER_TARGETS = (
    # v1's runner glue, still what an AgentNode and BaseAgent.run configure a turn through
    "agentdeck.agents.runners.base.OpenAIProvider",
    # the openai-agents adapter, which is what every Runtime-played turn configures through
    "agentdeck.adapters.engines.openai_agents.runconfig.OpenAIProvider",
)


def patch_provider(monkeypatch: Any, provider: type) -> None:
    """Point every place a run's model provider is built at ``provider``.

    Two places, because two paths still resolve a run config: the Runtime plays a turn
    through the adapter, while a workflow node driving an agent of its own still goes
    through v1's runner. A test that patched only one would pass while the other reached
    for a real endpoint.
    """
    for target in PROVIDER_TARGETS:
        monkeypatch.setattr(target, provider)


__all__ = ["MODEL_NAME", "PROVIDER_TARGETS", "ScriptedModel", "patch_provider", "provider_of"]
