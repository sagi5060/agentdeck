"""Sandbox-aware LangGraph workflows.

Build the graph with LangGraph's :class:`StateGraph`; drop in
:class:`SkillNode`, :class:`AgentNode`, or :class:`SandboxAgentNode`
to give skills or agents a turn — they share the workflow's
sandbox. :class:`LoadFileNode` pulls a sandbox file back
into state. Call :func:`interrupt` in a node of a ``durable=True``
workflow to pause for a human decision (see ``workflows.interrupts``),
or :func:`sleep_until` to pause until a wall-clock moment
(see ``workflows.timers``; drive due wakeups with ``App.tick()``).
"""

from langgraph.graph import END, StateGraph
from langgraph.types import interrupt

from agentdeck.workflows.base import BaseWorkflow
from agentdeck.workflows.interrupts import InterruptResult
from agentdeck.workflows.nodes import AgentNode, LoadFileNode, SandboxAgentNode, SkillExecutionError, SkillNode
from agentdeck.workflows.registry import WorkflowRegistry
from agentdeck.workflows.runners import BaseWorkflowRunner, DevWorkflowRunner
from agentdeck.workflows.state import coerce_input, dump_state, json_default
from agentdeck.workflows.timers import sleep_until

__all__ = [
    "END",
    "AgentNode",
    "BaseWorkflow",
    "BaseWorkflowRunner",
    "DevWorkflowRunner",
    "InterruptResult",
    "LoadFileNode",
    "SandboxAgentNode",
    "SkillExecutionError",
    "SkillNode",
    "StateGraph",
    "WorkflowRegistry",
    "coerce_input",
    "dump_state",
    "interrupt",
    "json_default",
    "sleep_until",
]
