# `agentdeck/`

Declarative layer over the [OpenAI Agents SDK](https://github.com/openai/openai-agents-python)
and [LangGraph](https://langchain-ai.github.io/langgraph/).

agentdeck owns **configuration** — settings, capabilities, sandbox manifest,
runner glue, graph compilation, plug-in discovery. Execution stays in the
SDK / LangGraph.

```text
agentdeck/
    runtime/       # shared primitives: settings, workspace, plugin registry, SDK event helpers
    skills/        # SkillBundle + SkillRegistry + SkillExecutor + typed outputs
    agents/        # declarative wrapper over the Agents SDK
    workflows/     # declarative wrapper over LangGraph
```

---

## `agentdeck.runtime` — shared primitives

Everything that is neither agent- nor workflow-specific.

| Module | Responsibility |
| --- | --- |
| [`settings.py`](runtime/settings.py) | Layered `Settings` (`OpenAISettings`, `RunnerSettings`, `SkillsSettings`). Loaded from process env / `.env`. |
| [`workspace.py`](runtime/workspace.py) | `Workspace` — owns one `SandboxSession`, shared across nested agents / workflows / skills via a `ContextVar`. |
| [`registry.py`](runtime/registry.py) | Generic `PluginRegistry` — discovers any `<package>/<type_dir>/<bundle>/<module>.py` plug-in. |
| [`events.py`](runtime/events.py) | Helpers that read text deltas, tool args, and tool outputs out of Agents SDK stream items. |

## `agentdeck.skills` — deterministic skill execution

| Module | Responsibility |
| --- | --- |
| [`bundle.py`](skills/bundle.py) | `SkillBundle` — parses `SKILL.md` frontmatter; `SkillRegistry` discovers bundles under a root directory. |
| [`executor.py`](skills/executor.py) | `SkillExecutor` — runs a bundle inside a `Workspace`, parses `key=value` lines into `SkillResult`. |
| [`output.py`](skills/output.py) | `SkillOutputSchema` — Pydantic base for typed workflow-only skill outputs.

## `agentdeck.agents` — Agents SDK runtime

Class-attribute-driven wrapper over `agents.Agent` and `agents.sandbox.SandboxAgent`.

| Module | Responsibility |
| --- | --- |
| [`base.py`](agents/base.py) | `BaseAgent` and `BaseSandboxAgent` declarative bases. |
| [`capabilities/`](agents/capabilities/) | `CapabilitiesSpec` and per-capability specs: `shell`, `skills`, `compaction`, `filesystem`, `memory`. |
| [`runners/`](agents/runners/) | `BaseRunner` ABC, `HeadlessRunner` (single-shot `run` + streamed `run_streamed`), `StreamDone` sentinel. |
| [`registry.py`](agents/registry.py) | `AgentRegistry` — auto-discovery of `BaseAgent` subclasses. |

### Declaring an agent

```python
from agentdeck.agents import BaseAgent, BaseSandboxAgent, CapabilitiesSpec


class Summarizer(BaseAgent):
    name = "Summarizer"
    instructions = "Summarize the user's document concisely."
    output_type = MyPydanticSchema
    model_settings = {"temperature": 0.2}


class FileAgent(BaseSandboxAgent):
    name = "File Handler"
    instructions = "..."
    capabilities = CapabilitiesSpec(
        shell=True,
        skills=["md-segment-translate"],
        skills_dir=SHARED_SKILLS_DIR,
    )
```

Both classes are used the same way:

```python
agent = FileAgent.build()  # -> SandboxAgent (or Agent for BaseAgent)
result = await FileAgent.run("hi")  # -> one-shot RunResult via HeadlessRunner
```

### `BaseAgent` class attributes

| Attribute | Type | Purpose |
| --- | --- | --- |
| `name` | `str` | Defaults to the class name. |
| `instructions` | `str` | System prompt. |
| `handoff_description` | `str \| None` | Shown to peer agents in handoff routing. |
| `model` | `str \| None` | Per-agent model override. |
| `model_settings` | `dict` | Forwarded to `agents.ModelSettings(**dict)`. |
| `tools` | `list` | Already-built tool instances. |
| `handoffs` | `list` | Peer agents to hand off to. Entries may be `BaseAgent` subclasses (built lazily), already-built `Agent` instances, or `Handoff` objects. |
| `output_type` | `type` | Pydantic schema for structured output. |
| `hooks` | `AgentHooks` | Lifecycle hooks. |

`BaseSandboxAgent` adds `capabilities: CapabilitiesSpec`.

### `CapabilitiesSpec`

```python
CapabilitiesSpec(
    shell=ShellSpec(yield_floor_ms=300_000),  # or True for env defaults
    skills=["md-segment-translate"],  # list[str] under skills_dir
    skills_dir=SHARED_SKILLS_DIR,  # Path to a skill-bundle directory
    compaction=CompactionSpec(threshold=0.9),  # or True for runtime defaults
    filesystem=FilesystemSpec(configure_tools=fn),  # or True
    memory=MemorySpec(layout={...}, read={...}),  # or True
)
```

Each structured capability accepts `True` (defaults) / `False` (off).
`ShellSpec` defaults come from `AGENTDECK_SHELL_*` env vars.
`compaction.threshold` is either an integer (absolute token cap) or a float
in `(0, 1]` (fraction of the model's context window — needs a known `model`).

> **Transport note.** The Agents SDK provider defaults to Responses. Set
> `OPENAI_USE_RESPONSES=false` for Chat Completions-only compatible servers.
> Two SDK pieces that need special handling under Chat Completions have
> transparent in-file shims next to their capability:
> [`compaction.py`](agents/capabilities/compaction.py) overrides
> `Compaction.sampling_params` to return `{}`, and
> [`filesystem.py`](agents/capabilities/filesystem.py) swaps the
> `apply_patch` `CustomTool` for a `FunctionTool` wrapper.

### Runners

`BaseRunner.from_agent(agent, **overrides)` resolves `Settings`, builds the
SDK `RunConfig`, and assembles a `Manifest` (env + input files). Subclasses
choose how the agent is driven:

`HeadlessRunner` performs a single `Runner.run(...)`; it inherits or opens a
`Workspace` when the agent needs one. Used by graph nodes, tool wrappers, and
`App.run_agent` / `App.chat`.

---

## `agentdeck.workflows` — LangGraph runtime

Class-attribute-driven wrapper over `langgraph.graph.StateGraph`.

| Module | Responsibility |
| --- | --- |
| [`base.py`](workflows/base.py) | `BaseWorkflow` declarative base. `build_graph` returns a LangGraph `StateGraph`; class attributes compile to a `CompiledStateGraph` (cached). `BaseWorkflow.as_tool()` exposes the workflow as a `FunctionTool`. |
| [`nodes.py`](workflows/nodes.py) | Node callables — `SkillNode`, `LoadFileNode`, `AgentNode`, `SandboxAgentNode`. |
| [`state.py`](workflows/state.py) | `coerce_input`, `dump_state`, `json_default` — shared state helpers. |
| [`runners/`](workflows/runners/) | `BaseWorkflowRunner` ABC + `DevWorkflowRunner` (single-shot `ainvoke`). |
| [`registry.py`](workflows/registry.py) | `WorkflowRegistry` — auto-discovery of `BaseWorkflow` subclasses. |

### Declaring a workflow

```python
from pydantic import BaseModel
from catalog import SHARED_SKILLS
from catalog.skills.md_to_segment_translate import MarkdownSegmentTranslateOutput
from agentdeck.workflows import (
    END,
    AgentNode,
    BaseWorkflow,
    LoadFileNode,
    SkillNode,
    StateGraph,
)

TRANSLATE = SHARED_SKILLS.get("md-segment-translate")


class State(BaseModel):
    source_path: str
    translate_out: MarkdownSegmentTranslateOutput | None = None
    summary_input: str = ""
    summary: object | None = None


class TranslateAndSummarize(BaseWorkflow):
    state = State

    @classmethod
    def build_graph(cls) -> StateGraph:
        g = StateGraph(cls.state)
        g.add_node(
            "translate",
            SkillNode(
                TRANSLATE,
                argv=lambda s: [s.source_path, "--target-language", "English"],
                into="translate_out",  # typed schema instance
            ),
        )
        g.add_node(
            "load",
            LoadFileNode(
                path=lambda s: s.translate_out and s.translate_out.translated_md,
                into="summary_input",
            ),
        )
        g.add_node(
            "summarize",
            AgentNode(
                DocumentSummaryAgent,
                input_key="summary_input",
                output_key="summary",
            ),
        )
        g.set_entry_point("translate")
        g.add_edge("translate", "load")
        g.add_edge("load", "summarize")
        g.add_edge("summarize", END)
        return g


await TranslateAndSummarize.run({"source_path": "/abs/path.md"})
TranslateAndSummarize.as_tool()  # -> agents.tool.FunctionTool
```

### Node callables

| Node | When to use |
| --- | --- |
| Plain callable | Pure transitions — any sync or async function on the state. |
| `AgentNode` / `SandboxAgentNode` | Drive a `BaseAgent` once via `HeadlessRunner`. |
| `SkillNode` | Run a skill bundle deterministically and write its typed output into state. |
| `LoadFileNode` | Read a sandbox file (path picked off state via a callable) into a state field, optionally parsed. |

### `SkillNode` — typed skill outputs

A skill bundle that opts in declares a Pydantic schema in its `SKILL.md`
frontmatter:

```yaml
metadata:
  output_schema: "__init__.py:DocumentParserOutput"
```

The declared schema module lives next to `SKILL.md` and subclasses
`agentdeck.skills.SkillOutputSchema`. Workflows import the class
statically (`from catalog.skills.doc_parser import
DocumentParserOutput`); the bundle accessor (`bundle.output_schema`)
returns the same class object so `isinstance` and Pydantic validation
work identically. `SkillNode(bundle, into="parser_out")` runs the skill,
parses its stdout into that schema, and stores the *instance* in
`state[parser_out]` — downstream nodes read fields directly:

```python
LoadFileNode(
    path=lambda s: s.classify_out.output_srd_json if s.classify_out else None,
    into="srd",
    parse=SRDDocument.model_validate_json,
)
```

For skills without a declared schema, `SkillNode(..., update=lambda state, raw_result: {...})`
exposes the raw `SkillResult`.

> **Workflow-only.** Skill output schemas are imported by workflows, not by
> agents. The agent's contract with the skill is the prose in `SKILL.md`
> plus the `key=value` lines on stdout — the typed surface only exists on
> the deterministic, host-side side of the boundary. Don't import
> `SkillOutputSchema` (or any skill's typed schema module) from agent code.

### Interop directions

* **Workflow runs Agent** — `AgentNode` reads `input_key`, drives the
  agent through `HeadlessRunner`, writes `output_key`.
* **Agent runs Workflow** — `BaseWorkflow.as_tool()` exposes a compiled
  workflow as a `FunctionTool` whose args schema is the workflow's
  Pydantic state.

---

## Plug-in discovery

`PluginRegistry` walks `<package>/<type_dir>/<bundle>/<module>.py` and indexes
every subclass of a base class declared in those modules. Reused by:

* `AgentRegistry`    — `<package>/agents/<bundle>/agent.py`       → `BaseAgent` subclasses.
* `WorkflowRegistry` — `<package>/workflows/<bundle>/workflow.py` → `BaseWorkflow` subclasses.

An old-style project dir without the `agents/` / `workflows/` type
subdirectory raises a `ConfigError` pointing at the new layout instead of
silently discovering nothing.

```python
from agentdeck.agents import AgentRegistry

reg = AgentRegistry("catalog", type_dir="agents")
reg.list()  # {"FileAgent": <class>, ...}
reg.get("FileAgent")  # <class FileAgent>
```

---


## Settings

Layered Pydantic-Settings models. See [`runtime/settings.py`](runtime/settings.py)
for definitions and the [main README](../README.md#configuration) for the
env-var reference.

```python
from agentdeck.runtime.settings import get_settings

s = get_settings()  # cached
s.openai.model  # "cyankiwi/..."
s.runner.max_turns  # 30
s.skills.env_dict()  # {"GAIA_PARSER_URL": "...", ...}
s.sandbox_env()  # OPENAI_* + SKILL_* dict for the sandbox manifest
```

---
