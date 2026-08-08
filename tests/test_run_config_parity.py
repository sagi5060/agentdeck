"""Both places a run gets configured, reduced to one comparable fingerprint.

A field dropped in resolution — the CA bundle, the token cap, the model provider's own
base URL — is invisible to a fake-model suite: it changes nothing until a real endpoint
refuses the call. So this fingerprint, not the suite at large, is what stands between the
move of that resolution into the openai-agents adapter and a regression only production
sees.

Two resolvers are compared against one expectation, because two paths still build a run
config: the Runtime plays every recorded turn through the adapter, while a workflow node
driving an agent of its own still goes through v1's ``BaseRunner.from_agent``. They have to
agree field for field, or which entry point a turn arrived through decides what the model
was asked. The v1 half goes when v1's runner glue does.
"""

from __future__ import annotations

import ssl
from pathlib import Path
from typing import TYPE_CHECKING, Any

import certifi
import httpx
import pytest
from agents import Agent, OpenAIProvider

from agentdeck.adapters.engines.openai_agents.runconfig import build_run_config
from agentdeck.agents.runners.headless import HeadlessRunner
from agentdeck.composition import resolve_run_settings
from agentdeck.runtime import observability
from agentdeck.runtime.settings import reset_settings_cache

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from agents import RunConfig
    from openai import AsyncOpenAI

# Every field the fingerprint asserts is set here explicitly, including the ones whose
# value is the default: the project's own `.env` is loaded by `get_settings`, so deleting
# a variable hands the answer to whatever the developer happens to have on disk while CI,
# with no `.env`, gets a different one.
_SETTINGS_ENV = {
    "OPENAI_MODEL": "parity-model",
    "OPENAI_API_KEY": "parity-key",
    "OPENAI_BASE_URL": "https://gateway.invalid/v1",
    "OPENAI_CA_BUNDLE": "",
    "OPENAI_USE_RESPONSES": "true",
    "AGENTDECK_RUNNER_WORKFLOW_NAME": "parity-flow",
    "AGENTDECK_RUNNER_TEMPERATURE": "0.25",
    "AGENTDECK_RUNNER_MAX_TURNS": "7",
    "AGENTDECK_RUNNER_MAX_TOKENS": "512",
    "AGENTDECK_LANGFUSE_PUBLIC_KEY": "",
    "AGENTDECK_LANGFUSE_SECRET_KEY": "",
}


def _ca_subjects(context: ssl.SSLContext) -> tuple[str, ...]:
    return tuple(sorted(str(cert.get("subject", "")) for cert in context.get_ca_certs()))


def _httpx_trust(client: httpx.AsyncClient) -> tuple[str, ...]:
    """Which CAs this client will accept a TLS certificate from.

    The ``verify=<path>`` an OpenAI CA bundle becomes cannot be read back off the httpx
    client, so the trust store it produced is asserted instead — that store is the whole
    point of the setting. Private attributes the whole way down: the httpx version is
    pinned, and this file outlives it.
    """
    transport: Any = client._transport
    return _ca_subjects(transport._pool._ssl_context)


def _trusted_cas(client: AsyncOpenAI) -> tuple[str, ...]:
    return _httpx_trust(client._client)


def _default_ca_trust() -> tuple[str, ...]:
    """httpx's own trust store — what a client configured with no CA bundle must still have."""
    return _httpx_trust(httpx.AsyncClient())


def _one_certificate_bundle(tmp_path: Path) -> Path:
    """A CA bundle holding exactly one real certificate, so the trust store it produces is
    unmistakably not the default one. Sourced from certifi (httpx's own bundle) because the
    certificate has to parse — a hand-written placeholder is rejected at load."""
    bundle = tmp_path / "corporate-ca.pem"
    first, _, _ = Path(certifi.where()).read_text().partition("-----END CERTIFICATE-----")
    bundle.write_text(f"{first}-----END CERTIFICATE-----\n")
    return bundle


def _fingerprint(config: RunConfig, max_turns: int) -> dict[str, object]:
    """A resolved run config as plain comparable values, SDK client objects excluded."""
    provider = config.model_provider
    assert isinstance(provider, OpenAIProvider)
    client = provider._get_client()
    return {
        "workflow_name": config.workflow_name,
        "model": config.model,
        "nest_handoff_history": config.nest_handoff_history,
        "tracing_disabled": config.tracing_disabled,
        "temperature": config.model_settings.temperature,
        "max_tokens": config.model_settings.max_tokens,
        "include_usage": config.model_settings.include_usage,
        "max_turns": max_turns,
        "model_provider": type(provider).__name__,
        "use_responses": provider._use_responses,
        "base_url": str(client.base_url),
        "api_key": client.api_key,
        "trusted_cas": _trusted_cas(client),
    }


def _expected(**overrides: object) -> dict[str, object]:
    return {
        "workflow_name": "parity-flow",
        "model": "parity-model",
        "nest_handoff_history": True,
        "tracing_disabled": True,
        "temperature": 0.25,
        "max_tokens": 512,
        "include_usage": True,
        "max_turns": 7,
        "model_provider": "OpenAIProvider",
        "use_responses": True,
        "base_url": "https://gateway.invalid/v1/",
        "api_key": "parity-key",
        "trusted_cas": _default_ca_trust(),
    } | overrides


@pytest.fixture
def resolved_run_configs(monkeypatch: pytest.MonkeyPatch) -> Iterator[Callable[..., list[dict[str, object]]]]:
    """Fingerprint both resolvers for one explicit set of settings."""

    def resolve(**env: str) -> list[dict[str, object]]:
        for key, value in (_SETTINGS_ENV | env).items():
            monkeypatch.setenv(key, value)
        # `init_observability` latches on a module global, and its answer is what decides
        # v1's `tracing_disabled` — without this, a test that ran earlier in the process
        # picks the value asserted here.
        monkeypatch.setattr(observability, "_initialized", False)
        reset_settings_cache()
        v1 = HeadlessRunner.from_agent(Agent(name="parity"))
        settings = resolve_run_settings()
        return [
            _fingerprint(v1.run_config, v1.max_turns),
            _fingerprint(build_run_config(settings), settings.max_turns),
        ]

    yield resolve
    reset_settings_cache()


def test_both_resolvers_cover_every_settings_field(
    resolved_run_configs: Callable[..., list[dict[str, object]]],
) -> None:
    assert resolved_run_configs() == [_expected(), _expected()]


def test_ca_bundle_becomes_the_clients_trust_store(
    resolved_run_configs: Callable[..., list[dict[str, object]]], tmp_path: Path
) -> None:
    bundle = _one_certificate_bundle(tmp_path)

    fingerprints = resolved_run_configs(OPENAI_CA_BUNDLE=str(bundle), OPENAI_USE_RESPONSES="false")

    # The CA bundle moves base_url and api_key off the provider and onto a client of its
    # own, so every other field is re-asserted here rather than assumed to have survived.
    expected = _expected(
        use_responses=False,
        trusted_cas=_ca_subjects(ssl.create_default_context(cafile=str(bundle))),
    )
    assert fingerprints == [expected, expected]
