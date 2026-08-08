"""The ports: small, role-shaped interfaces the outer rings implement.

Each is the narrowest thing its caller needs — an engine depends on nothing but the payloads it
yields, a telemetry sink on ``emit`` and never on the store behind it.

Only interfaces live here. The vocabulary and policy that go with a port are core's, one ring
in: what a control signal means is in :mod:`agentdeck.core.control`, what a run's status means
is in :mod:`agentdeck.core.status`.
"""

from agentdeck.core.ports.control import ControlPort
from agentdeck.core.ports.engine import EnginePort
from agentdeck.core.ports.sandbox import ExecResult, SandboxPort
from agentdeck.core.ports.sink import EventSinkPort
from agentdeck.core.ports.store import EventStorePort, RunSummary, SessionClaim
from agentdeck.core.ports.tools import ToolSet, ToolSourcePort

__all__ = [
    "ControlPort",
    "EnginePort",
    "EventSinkPort",
    "EventStorePort",
    "ExecResult",
    "RunSummary",
    "SandboxPort",
    "SessionClaim",
    "ToolSet",
    "ToolSourcePort",
]
