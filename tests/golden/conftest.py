"""Golden-suite fixtures: the real FastAPI app over the committed fixture project, with
the model provider swapped for :class:`ScriptedProvider` and every env knob pinned.
"""

import sys
from contextlib import contextmanager
from pathlib import Path

import pytest

pytest.importorskip("fastapi")

FIXTURE_PROJECT = Path(__file__).parent / "fixture_project"

# Env that must not leak in from a developer's shell or a stray .env: Redis sessions,
# Langfuse export and the sqlite checkpointer would all reach outside the test, and
# max_turns below the scripted two would truncate the recorded turn.
_PINNED_ENV = {
    "AGENTDECK_CHECKPOINT_BACKEND": "memory",
    "AGENTDECK_CHECKPOINT_URL": "",
    "AGENTDECK_EVENTS_BACKEND": "memory",
    "AGENTDECK_EVENTS_URL": "",
    "AGENTDECK_SESSION_REDIS_URL": "",
    "AGENTDECK_LANGFUSE_PUBLIC_KEY": "",
    "AGENTDECK_LANGFUSE_SECRET_KEY": "",
    "AGENTDECK_MCP_SERVERS": "{}",
    "AGENTDECK_RUNNER_MAX_TURNS": "30",
    "OPENAI_API_KEY": "golden",
    "OPENAI_BASE_URL": "",
    "OPENAI_MODEL": "fake-golden",
}


@pytest.fixture
def make_client(monkeypatch):
    """Factory of independent clients — the stability test needs two fresh ones in a row."""
    from fake_model import ScriptedProvider
    from fastapi.testclient import TestClient
    from scripted_model import patch_provider

    from agentdeck.adapters.engines.langgraph.checkpointer import _memory_saver
    from agentdeck.runtime.settings import PACKAGED_DEFAULT_YAML, reset_settings_cache
    from agentdeck.serve import create_app

    for key, value in _PINNED_ENV.items():
        monkeypatch.setenv(key, value)
    # .env and config.yaml resolve from cwd at settings-build time, and chdir below puts
    # that at fixture_project (neither file lives there) — APP_CONFIG_PATH is still
    # pinned to the shipped defaults as a belt-and-suspenders guard against either
    # appearing there later.
    monkeypatch.setenv("APP_CONFIG_PATH", str(PACKAGED_DEFAULT_YAML))
    patch_provider(monkeypatch, ScriptedProvider)
    monkeypatch.chdir(FIXTURE_PROJECT)

    @contextmanager
    def _make():
        # The memory saver is a process-wide @cache; a stale one would carry a previous
        # capture's paused threads into this one's /pending body.
        _memory_saver.cache_clear()
        reset_settings_cache()
        for mod in [m for m in sys.modules if m.startswith("agentdeck_project")]:
            del sys.modules[mod]
        with TestClient(create_app()) as client:
            yield client

    yield _make
    reset_settings_cache()
