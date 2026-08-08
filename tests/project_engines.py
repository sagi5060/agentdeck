"""The engine set a discovered project runs on, assembled the way ``App.load`` assembles it.

A test that wants a Runtime over a ``.agentdeck/`` project needs both engines wired with the
same resolved settings the facade wires them with; spelling that out per test file is how the
two quietly stop being the same thing. ``test_composition.py`` pins this against what ``App``
actually built, so the copy cannot drift without failing.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from agentdeck.adapters.engines.langgraph import LangGraphEngine
from agentdeck.adapters.engines.openai_agents import OpenAIAgentsEngine
from agentdeck.composition import (
    resolve_agent_sandbox,
    resolve_checkpoint,
    resolve_run_settings,
    resolve_workflow_workspace,
)

if TYPE_CHECKING:
    from agentdeck.core.ports import EnginePort


def project_engines() -> tuple[EnginePort, ...]:
    return (
        OpenAIAgentsEngine(settings=resolve_run_settings(), sandbox=resolve_agent_sandbox()),
        LangGraphEngine(durable_checkpoint=resolve_checkpoint(), workspace=resolve_workflow_workspace()),
    )


__all__ = ["project_engines"]
