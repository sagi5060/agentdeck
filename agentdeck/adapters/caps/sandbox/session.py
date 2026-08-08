"""The Agents SDK's sandbox as a :class:`SandboxPort` — one session shared by everything nested.

``open_sandbox`` is the whole lifecycle: it is what an outer scope opens and what a nested call
re-enters, because a workflow's node, the agent it invokes and the skill that agent runs all have
to see the same working directory or the files one writes are invisible to the next.

The environment a sandbox starts with is this module's other job. It is host-built by
construction: a skill reads its provenance and its trace parent out of the env, so anything
sourced from model output could forge both. ``trace_env`` is injected rather than imported —
what the carriers are is telemetry's business, and an adapter that reached for the tracer would
own a second external system.
"""

from __future__ import annotations

import io
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from agents.run_config import SandboxRunConfig
from agents.sandbox import Manifest
from agents.sandbox.entries import BaseEntry, File, LocalDir
from agents.sandbox.manifest import Environment
from agents.sandbox.sandboxes.unix_local import UnixLocalSandboxClient
from agents.sandbox.session.manifest_ops import apply_entry_batch
from agents.sandbox.workspace_paths import SandboxPathGrant

from agentdeck.core.ports.sandbox import ExecResult, SandboxPort, bind_sandbox, current_sandbox
from agentdeck.runtime.capture import CAPTURE_ENV, Capture

if TYPE_CHECKING:
    import os
    from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence

    from agents.sandbox.session import SandboxSession
    from agents.sandbox.session.sandbox_client import BaseSandboxClient

logger = logging.getLogger(__name__)

INPUT_FILES_DIR = "input_files"

# Host-owned, so a caller's stale value may never win: these carry the active span across the
# process edge, and a sandbox that adopted an old parent would hang its skill's LLM calls off a
# span that already closed.
_TRACE_CARRIERS = ("TRACEPARENT", "BAGGAGE")


@dataclass(slots=True)
class UnixSandbox(SandboxPort):
    """One ``SandboxSession`` on the local Unix sandbox client.

    ``sandbox_run_config`` is the part no port can carry: the openai-agents engine needs the
    session as the SDK's own type, so the handle stays here and only the composition root — which
    builds adapters anyway — ever touches it.
    """

    client: BaseSandboxClient[Any]
    session: SandboxSession
    _started: bool = False

    @property
    def sandbox_run_config(self) -> SandboxRunConfig:
        """This session as the SDK's run-config handle, for an engine to pass straight through."""
        return SandboxRunConfig(client=self.client, session=self.session)

    async def read_text(self, path: str | Path, encoding: str = "utf-8") -> str:
        await self._ensure_started()
        stream = await self.session.read(Path(path))
        try:
            return stream.read().decode(encoding)
        finally:
            stream.close()

    async def write_bytes(self, path: str | Path, content: bytes) -> None:
        await self._ensure_started()
        rel = Path(path)
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"write path must be relative and must not escape the sandbox: {path!r}")
        if rel.parent.parts and rel.parent != Path("."):
            await self.session.mkdir(rel.parent, parents=True)
        await self.session.write(rel, io.BytesIO(content))

    async def mount_dir(self, src: Path, at: str | Path, *, read_only: bool = True) -> None:
        # v0.17.0+ materializes a ``LocalDir`` outside the SDK process base_dir only against an
        # explicit grant, so the grant is part of mounting rather than something a caller
        # remembers separately. Granting before the session starts matches a fresh manifest's
        # own ordering.
        resolved = Path(src).resolve()
        manifest = self.session.state.manifest
        grant = SandboxPathGrant(path=str(resolved), read_only=read_only)
        if grant not in manifest.extra_path_grants:
            manifest.extra_path_grants = (*manifest.extra_path_grants, grant)
        await self._ensure_started()
        # NOTE: ``_manifest_base_dir`` is the only way to obtain the session's workdir; the SDK
        # does not expose a public accessor. See pyproject's ``reportPrivateUsage = "warning"``
        # carve-out for cross-package _helpers.
        entries: list[tuple[Path, BaseEntry]] = [(Path(at), LocalDir(src=resolved))]
        await apply_entry_batch(self.session, entries, base_dir=self.session._manifest_base_dir())

    async def exec(self, *cmd: str, timeout: float | None = None) -> ExecResult:
        await self._ensure_started()
        result = await self.session.exec(*cmd, timeout=timeout, shell=False)
        return ExecResult(stdout=result.stdout, stderr=result.stderr, exit_code=result.exit_code)

    def merge_environment(self, environment: Mapping[str, str] | None) -> None:
        """Fold env into the live manifest so the next ``exec`` sees it.

        How a nested call contributes its own environment to a session it did not open. The Unix
        sandbox re-resolves ``manifest.environment`` on every exec, so mutating it after creation
        propagates without a restart. Later writers win, matching a fresh open's precedence —
        which is what re-injects the *current* span's carriers when a skill joins an outer
        session, so its subprocess spans nest under the skill rather than the run root.
        """
        if not environment:
            return
        self.session.state.manifest.environment.value.update(environment)

    async def _ensure_started(self) -> None:
        # SDK Runner starts on the first turn, but direct IO callers may hit the session before
        # any agent turn fires.
        if self._started:
            return
        if not await self.session.running():
            await self.session.start()
        self._started = True


@asynccontextmanager
async def open_sandbox(
    *,
    environment: Mapping[str, str] | None = None,
    input_files: Sequence[str | os.PathLike[str]] | None = None,
    manifest_root: str | os.PathLike[str] | None = None,
    trace_env: Callable[[], Mapping[str, str]] | None = None,
) -> AsyncIterator[UnixSandbox]:
    """Open a sandbox, or join the one already bound to this async context.

    Pinning ``manifest_root`` flips ``workspace_root_owned=False`` in the SDK so the teardown
    skips the rmtree and the host directory survives for something else to read.

    ``trace_env`` supplies the host-built environment a sandboxed process cannot derive itself —
    the trace carriers of whatever span is current *at this call*, which is why it is a callable
    and not a mapping: a skill opens its span before joining, and the join has to re-capture.
    """
    injected = dict(trace_env()) if trace_env is not None else {}
    if injected:
        # Precedence: injected defaults < explicit caller env < host-owned trace carriers.
        # Carriers win last so a stale caller ``TRACEPARENT``/``BAGGAGE`` can't detach the
        # sandbox from the current span.
        carriers = {key: injected[key] for key in _TRACE_CARRIERS if key in injected}
        environment = {**injected, **(dict(environment) if environment else {}), **carriers}
    # Only a session this adapter opened can be joined — a foreign implementation has no
    # manifest to fold into, so the honest answer there is a session of our own.
    if isinstance(active := current_sandbox(), UnixSandbox):
        active.merge_environment(environment)
        yield active
        return

    client = UnixLocalSandboxClient()
    session = await client.create(manifest=_build_manifest(environment, input_files, manifest_root), options=None)
    # No identity from the caller? The sandbox session id is only known post-create, so stamp it
    # onto the live manifest as a minimal fallback capture — ``exec`` re-resolves ``environment``
    # each call. An explicit ``environment`` key already rode in via the manifest and is left
    # intact.
    if CAPTURE_ENV not in session.state.manifest.environment.value:
        session.state.manifest.environment.value[CAPTURE_ENV] = Capture(
            session_id=str(session.state.session_id),
        ).model_dump_json(exclude_none=True)
    sandbox = UnixSandbox(client=client, session=session)
    try:
        with bind_sandbox(sandbox):
            yield sandbox
    finally:
        # Per-phase try/except so a teardown failure can't mask the body's exception or skip the
        # next phase. Failures are logged loudly.
        try:
            await session.aclose()
        except Exception:  # noqa: BLE001 — per-phase teardown; log loudly so caller's exception isn't masked and the next teardown phase still runs
            logger.exception("sandbox teardown: session.aclose failed")
        try:
            await client.delete(session)
        except Exception:  # noqa: BLE001 — per-phase teardown; same justification as session.aclose above
            logger.exception("sandbox teardown: client.delete failed")


def input_file_targets(
    input_files: Sequence[str | os.PathLike[str]] | None,
) -> Iterator[tuple[Path, bytes]]:
    """Yield ``(sandbox path, content)`` for each host file mounted under ``input_files/``.

    Basenames must be unique: the sandbox layout is flat, so two same-named inputs would have one
    silently overwrite the other.
    """
    seen: set[str] = set()
    for raw in input_files or ():
        path = Path(raw).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(path)
        if path.name in seen:
            raise ValueError(f"Duplicate input_files basename: {path.name}")
        seen.add(path.name)
        yield Path(INPUT_FILES_DIR) / path.name, path.read_bytes()


def _build_manifest(
    environment: Mapping[str, str] | None,
    input_files: Sequence[str | os.PathLike[str]] | None,
    manifest_root: str | os.PathLike[str] | None,
) -> Manifest:
    entries: dict[str | Path, BaseEntry] = {
        str(target): File(content=content) for target, content in input_file_targets(input_files)
    }
    kwargs: dict[str, Any] = {
        "environment": Environment(value=dict(environment or {})),
        "entries": entries,
    }
    if manifest_root is not None:
        kwargs["root"] = str(Path(manifest_root))
    return Manifest(**kwargs)


__all__ = ["UnixSandbox", "input_file_targets", "open_sandbox"]
