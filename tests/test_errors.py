"""One exception hierarchy: registry misses and serve.py's HTTP mapping."""

import sys
import textwrap

import pytest
from fastapi.testclient import TestClient
from scripted_model import ScriptedModel, patch_provider, provider_of

from agentdeck.errors import AgentdeckError, ConfigError, NotFoundError, SkillError
from agentdeck.runtime.registry import PluginRegistry
from agentdeck.skills.executor import SkillEnvError, SkillExecutionError

AGENT_PY = """
from agentdeck.agents import BaseAgent

class Greeter(BaseAgent):
    instructions = "Greet the user."
"""

# Same class name as AGENT_PY's, authored under a second bundle — the "copied greeter/,
# forgot to rename the class" repro from #82.
GREETER_V2_AGENT_PY = """
from agentdeck.agents import BaseAgent

class Greeter(BaseAgent):
    instructions = "Greet the user, v2."
"""

# One bundle, one class, bound under a second name — an alias kept after a rename. Not a
# collision: it is the same class object claiming its own name twice, not two classes.
ALIASED_AGENT_PY = """
from agentdeck.agents import BaseAgent

class Greeter(BaseAgent):
    instructions = "Greet the user."

GreeterAgent = Greeter
"""


@pytest.fixture
def project(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(textwrap.dedent(AGENT_PY))
    monkeypatch.chdir(tmp_path)
    # the project alias is process-global; drop stale mounts from other tests
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]


@pytest.fixture
def duplicate_class_name_project(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(textwrap.dedent(AGENT_PY))
    (root / "agents" / "greeter-v2").mkdir(parents=True)
    (root / "agents" / "greeter-v2" / "agent.py").write_text(textwrap.dedent(GREETER_V2_AGENT_PY))
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]


@pytest.fixture
def aliased_class_project(tmp_path, monkeypatch):
    root = tmp_path / ".agentdeck"
    (root / "agents" / "greeter").mkdir(parents=True)
    (root / "agents" / "greeter" / "agent.py").write_text(textwrap.dedent(ALIASED_AGENT_PY))
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]


def test_registry_miss_is_agentdeck_error():
    registry = PluginRegistry(
        package="agentdeck.agents", base_class=object, module_name="agent", type_dir="agents", label="agent"
    )
    with pytest.raises(AgentdeckError):
        registry.get("does-not-exist")


def test_not_found_error_message_is_plain():
    # serve.py puts str(exc) in the 404 body — no KeyError-style requoting.
    assert str(NotFoundError("no such thing")) == "no such thing"


def test_skill_errors_are_agentdeck_errors():
    assert issubclass(SkillEnvError, SkillError)
    assert issubclass(SkillEnvError, AgentdeckError)
    assert issubclass(SkillExecutionError, SkillError)
    assert issubclass(SkillExecutionError, AgentdeckError)


def test_unknown_agent_chat_returns_404_with_body(project):
    from agentdeck.serve import create_app

    # context manager runs the lifespan; without it every endpoint is 503
    with TestClient(create_app()) as client:
        response = client.post("/agents/unknown/chat", json={"session_id": "s", "message": "hi"})
    assert response.status_code == 404
    assert response.json()["detail"].startswith("No agent named 'unknown'.")


def test_two_same_kind_bundles_sharing_a_class_name_raise_naming_both(duplicate_class_name_project):
    """#82: a copied bundle that forgot to rename its class must not silently shadow the original."""
    from agentdeck import App

    with pytest.raises(ConfigError) as excinfo:
        App().load()
    message = str(excinfo.value)
    # Quoted, not bare substrings: "agents/greeter" is itself a substring of
    # "agents/greeter-v2", so a bare-substring check passes even if the message only
    # ever named the second bundle — pin the exact quoted forms the message emits.
    assert "'agents/greeter'" in message
    assert "'agents/greeter-v2'" in message
    assert message.count("Greeter") >= 1


def test_one_bundle_aliasing_its_own_class_is_not_a_collision(aliased_class_project):
    """A bundle binding one class under two names (an alias kept after a rename) must still load.

    ``vars(module)`` yields one entry per *binding*, not per class — ``GreeterAgent = Greeter``
    must not trip the same-name guard against itself.
    """
    from agentdeck import App

    inventory = App().load()
    assert inventory["agents"] == ["Greeter"]


def test_bundle_import_failure_is_wrapped_with_its_path(tmp_path, monkeypatch):
    """A bundle that raises at import (SyntaxError, missing dep) used to surface a raw traceback."""
    root = tmp_path / ".agentdeck"
    (root / "agents" / "broken").mkdir(parents=True)
    (root / "agents" / "broken" / "agent.py").write_text("raise RuntimeError('boom')\n")
    monkeypatch.chdir(tmp_path)
    for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
        del sys.modules[mod]
    from agentdeck import App

    with pytest.raises(ConfigError, match="agents/broken/agent.py") as excinfo:
        App().load()
    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_skill_error_returns_500_without_leaking_stderr(project, monkeypatch):
    from agentdeck.serve import create_app

    secret = "Traceback: AWS_SECRET_ACCESS_KEY=hunter2"
    # The turn fails at the SDK boundary, so the error travels the whole real path — engine,
    # Runtime, surface — the way a failing tool or skill inside a turn does.
    model = ScriptedModel(raises=SkillExecutionError("greeter", 1, secret))
    patch_provider(monkeypatch, provider_of(model))

    with TestClient(create_app()) as client:
        response = client.post("/agents/Greeter/chat", json={"session_id": "s", "message": "hi"})
    assert response.status_code == 500
    assert response.json() == {"detail": "internal error"}
    assert "hunter2" not in response.text
