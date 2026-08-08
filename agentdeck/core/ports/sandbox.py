"""The scratch filesystem and shell an invocable's work happens inside.

Three rings call this and each wants something different — an engine wants an opaque handle to
hand its SDK, a file-loading node wants one read, a skill wants to mount a bundle and run it —
so the port carries only the operations a caller actually makes. The engine's handle is
deliberately absent: it is engine-native by nature, so it stays on the adapter class the
composition root already holds, and core never names an SDK type to describe it.

The port is the narrow half. The wide half — opening a session, injecting its environment,
tearing it down — belongs to whoever owns the external sandbox, because none of it is
expressible without naming one.

A sandbox is also *ambient*: a workflow's nodes and a skill's file reads receive no handle from
their caller, they run inside whatever scope the run opened. :func:`bind_sandbox` and
:func:`require_sandbox` are that binding, and they live here rather than beside the
implementation so a consumer can find the current sandbox while importing core alone.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


@dataclass(frozen=True, slots=True)
class ExecResult:
    """What one :meth:`SandboxPort.exec` produced.

    Bytes, not text: a subprocess's output is untrusted and may not be valid UTF-8, so the
    decision to decode it (and how to handle what won't decode) belongs to the caller reading
    it, not to the sandbox that carried it across.
    """

    stdout: bytes
    stderr: bytes
    exit_code: int


class SandboxPort(ABC):
    """One open sandbox: a working directory plus the ability to run something in it.

    Paths are sandbox-relative throughout, and a write may not escape the working directory —
    that is the port's contract, not one implementation's caution, because a caller passing a
    model-influenced path must get the same refusal whichever sandbox is mounted.

    Starting the underlying session is nobody's business but the implementation's: every method
    here works whether or not anything has run yet.
    """

    @abstractmethod
    async def read_text(self, path: str | Path, encoding: str = "utf-8") -> str:
        """Decode the file at ``path``."""

    @abstractmethod
    async def write_bytes(self, path: str | Path, content: bytes) -> None:
        """Write ``content`` to ``path``, creating parent directories.

        Raises :exc:`ValueError` for an absolute path or one containing ``..``: a write that
        leaves the sandbox is the failure the sandbox exists to prevent.
        """

    @abstractmethod
    async def mount_dir(self, src: Path, at: str | Path, *, read_only: bool = True) -> None:
        """Make the host directory ``src`` readable inside the sandbox at ``at``.

        The host path is trusted application configuration — a skill bundle's own directory,
        never a path from model output. Implementations grant access to ``src`` themselves, so a
        caller never handles the grant separately from the mount that needs it.
        """

    @abstractmethod
    async def exec(self, *cmd: str, timeout: float | None = None) -> ExecResult:
        """Run ``cmd`` in the sandbox and wait for it.

        Argv, never a shell string: the one caller runs an interpreter against a path it built,
        and taking a shell string would make quoting its problem and injection its risk.
        """


_current: ContextVar[SandboxPort | None] = ContextVar("agentdeck_sandbox", default=None)


@contextmanager
def bind_sandbox(sandbox: SandboxPort) -> Iterator[None]:
    """Make ``sandbox`` the ambient one for the duration of this context.

    A ContextVar, so a nested run inherits the caller's sandbox and one scope's teardown cannot
    unbind another's. Enter and exit from the same async context — a generator abandoned to the
    garbage collector is finalized in a fresh one, where the reset raises.
    """
    token = _current.set(sandbox)
    try:
        yield
    finally:
        _current.reset(token)


def current_sandbox() -> SandboxPort | None:
    """The ambient sandbox, or ``None`` outside any scope."""
    return _current.get()


def require_sandbox() -> SandboxPort:
    """The ambient sandbox, or raise naming what the caller has to wrap the call in.

    For the callers that cannot degrade — reading a file the sandbox holds has no answer
    without one — as opposed to those that only bind a handle when a sandbox exists.
    """
    if (sandbox := _current.get()) is None:
        raise RuntimeError(
            "No active sandbox. Wrap the call in `async with open_sandbox(): ...`.",
        )
    return sandbox
