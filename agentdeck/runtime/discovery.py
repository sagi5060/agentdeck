"""Discovery: the ``.agentdeck/`` project dir becomes the invocables a Runtime can run.

One registry for every shape a project authors — an agent bundle and a workflow bundle
both come out as an ``InvocableSpec``, so the Runtime is handed one mapping and never
learns which shape a name was authored in. Skills stay out: no engine plays a ``SKILL.md``
bundle, so a spec for one could only fail at the moment somebody ran it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

from agentdeck.agents.registry import AgentRegistry
from agentdeck.core.invocable import InvocableKind, InvocableSpec
from agentdeck.errors import ConfigError
from agentdeck.runtime.registry import mount_project_dir
from agentdeck.workflows.registry import WorkflowRegistry

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from agentdeck.core.ports import EnginePort

# Which engine plays which bundle shape: a bundle names no engine of its own, the shape it
# was authored in decides. Written as strings rather than read off the adapters, because an
# adapter import here would invert the direction the Runtime's wiring depends on; a test
# pins each literal to its adapter's own ``engine`` so the two can't drift.
# ponytail: one engine per kind, forever — the day a second engine plays one shape, the
# engine belongs on the spec (authored per bundle), not in this table.
ENGINE_FOR_KIND: Final[Mapping[InvocableKind, str]] = {
    InvocableKind.AGENT: "openai-agents",
    InvocableKind.WORKFLOW: "langgraph",
}

# Where a workflow's opt-in durability travels to the engine that acts on it: the langgraph
# adapter reads ``spec.metadata[DURABLE_KEY]`` to decide whether to resolve the configured
# checkpointer at all. Spelled out rather than imported, for the reason above; the same test
# that pins the engine names pins this one to the adapter's own constant.
DURABLE_KEY: Final[str] = "durable"


class InvocableRegistry:
    """The one registry of what a project can run, built from its ``.agentdeck/`` bundles.

    Construct it with the engines the Runtime was given; :meth:`load` then returns the
    mapping the Runtime takes, and raises instead if the project asks for an engine nobody
    registered — a wiring mistake belongs at startup, not in the middle of a run.
    """

    def __init__(self, engines: Sequence[EnginePort]) -> None:
        self._engines = frozenset(engine.engine for engine in engines)

    def load(self) -> Mapping[str, InvocableSpec]:
        """Import every bundle under ``./.agentdeck`` and compile it to an ``InvocableSpec``.

        Eager on purpose: a bundle that can't be imported, an agent that can't be built and
        an engine that isn't registered all fail here, the way ``App.load()`` fails.
        """
        package = mount_project_dir()
        specs: dict[str, InvocableSpec] = {}
        for name, agent in AgentRegistry(package).list(refresh=True).items():
            self._add(specs, name, InvocableKind.AGENT, agent.build())
        for name, workflow in WorkflowRegistry(package).list(refresh=True).items():
            # uncompiled: the langgraph adapter compiles the graph itself, around the checkpointer
            # ``durable`` names — which is why that flag travels with the spec rather than staying
            # on a class only v1 can see.
            self._add(
                specs,
                name,
                InvocableKind.WORKFLOW,
                workflow.build_graph(),
                metadata={DURABLE_KEY: workflow.durable},
            )
        return specs

    def _add(
        self,
        specs: dict[str, InvocableSpec],
        name: str,
        kind: InvocableKind,
        native: Any,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        # Only catches a collision across kinds; a collision within one kind (two agent
        # bundles exporting the same class name) already raised inside the v1 scan that fed this.
        if name in specs:
            raise ConfigError(
                f"two bundles are both named {name!r} (kinds: {specs[name].kind.value} and {kind.value}); "
                "one name is one invocable — rename one of the classes."
            )
        engine = ENGINE_FOR_KIND[kind]
        if engine not in self._engines:
            raise ConfigError(
                f"{kind.value} {name!r} needs engine {engine!r}, which is not registered. "
                f"Registered: {sorted(self._engines)}."
            )
        specs[name] = InvocableSpec(name=name, kind=kind, engine=engine, native=native, metadata=metadata or {})


__all__ = ["DURABLE_KEY", "ENGINE_FOR_KIND", "InvocableRegistry"]
