"""The environment ``open_sandbox`` hands a sandbox, and who wins when two sources disagree.

None of this is visible to the contract suite — the port has no environment method, because only
the ring that owns a real sandbox can inject one. It is also the part with a security argument
behind it: a sandboxed skill reads its provenance and its trace parent out of this env, so a
caller-supplied value winning where a host-built one should is the failure worth a test.
"""

from __future__ import annotations

from agentdeck.adapters.caps.sandbox import UnixSandbox, open_sandbox
from agentdeck.runtime.capture import CAPTURE_ENV, Capture


def env_of(sandbox: UnixSandbox) -> dict[str, str]:
    """The environment the next ``exec`` will resolve, off the live manifest."""
    return sandbox.session.state.manifest.environment.value


async def test_caller_env_overrides_an_injected_default() -> None:
    """Injected values are defaults: a caller naming the same key means it."""
    async with open_sandbox(
        environment={"OTEL_SERVICE_NAME": "mine"},
        trace_env=lambda: {"OTEL_SERVICE_NAME": "injected", "LANGFUSE_HOST": "https://lf"},
    ) as sandbox:
        assert env_of(sandbox)["OTEL_SERVICE_NAME"] == "mine"
        assert env_of(sandbox)["LANGFUSE_HOST"] == "https://lf"


async def test_host_owned_trace_carriers_beat_a_stale_caller_value() -> None:
    """The one inversion of the rule above: a caller's ``TRACEPARENT`` is almost always a stale
    copy, and honouring it detaches the sandbox's spans from the span that is actually open."""
    async with open_sandbox(
        environment={"TRACEPARENT": "00-stale-stale-01", "BAGGAGE": "old=1"},
        trace_env=lambda: {"TRACEPARENT": "00-live-live-01", "BAGGAGE": "new=1"},
    ) as sandbox:
        assert env_of(sandbox)["TRACEPARENT"] == "00-live-live-01"
        assert env_of(sandbox)["BAGGAGE"] == "new=1"


async def test_joining_a_session_recaptures_the_current_carriers() -> None:
    """A skill opens its span and *then* joins the run's sandbox, so the join is the only moment
    its own span can become the sandbox's trace parent."""
    async with open_sandbox(trace_env=lambda: {"TRACEPARENT": "00-run-run-01"}) as outer:
        assert env_of(outer)["TRACEPARENT"] == "00-run-run-01"
        async with open_sandbox(trace_env=lambda: {"TRACEPARENT": "00-skill-skill-01"}) as inner:
            assert inner is outer
            assert env_of(inner)["TRACEPARENT"] == "00-skill-skill-01"


async def test_a_sandbox_with_no_capture_is_stamped_with_its_session_id() -> None:
    """Nobody passes an identity today, and a skill still has to report *some* session — the
    session id is only knowable after create, which is why this is a post-create stamp."""
    async with open_sandbox() as sandbox:
        stamped = Capture.model_validate_json(env_of(sandbox)[CAPTURE_ENV])
        assert stamped.session_id == str(sandbox.session.state.session_id)


async def test_an_explicit_capture_in_the_caller_env_is_left_alone() -> None:
    """The fallback fills a gap; it never overwrites an identity the host already built."""
    supplied = Capture(session_id="s-42", author_id="a-1").model_dump_json(exclude_none=True)
    async with open_sandbox(environment={CAPTURE_ENV: supplied}) as sandbox:
        assert env_of(sandbox)[CAPTURE_ENV] == supplied


async def test_no_trace_env_leaves_the_caller_environment_untouched() -> None:
    """Langfuse off must not mean an environment rebuilt around an empty injection."""
    async with open_sandbox(environment={"ONLY": "mine"}) as sandbox:
        assert env_of(sandbox)["ONLY"] == "mine"
