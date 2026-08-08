"""The sandbox implementations the contract suite holds to one behaviour, and the fake.

The fake plays a script for ``exec`` the way ``StubEngine`` plays one for a run: a dict from argv
to result. That is what lets one suite assert the same invariants against both — a fake cannot
run a process, but every implementation can be asked what a *known* command produced, and the
file operations around it are real on either side.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from agentdeck.adapters.caps.sandbox import open_sandbox
from agentdeck.core.ports.sandbox import ExecResult, SandboxPort, bind_sandbox

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable, Mapping
    from contextlib import AbstractAsyncContextManager

# One command every implementation answers for, so ``exec``'s result shape is contract-tested
# rather than tested only where a real process exists.
PROBE_CMD = ("python3", "-c", "import sys; sys.stdout.write('out'); sys.stderr.write('err'); sys.exit(3)")
PROBE_RESULT = ExecResult(stdout=b"out", stderr=b"err", exit_code=3)


@dataclass(slots=True)
class FakeSandbox(SandboxPort):
    """A dict for a filesystem and a script for ``exec``.

    Deliberately not a shortcut past the port's rules: the escape check on ``write_bytes`` is
    duplicated here on purpose, because it is the port's contract and a second implementation
    getting it wrong is exactly what the suite exists to catch.
    """

    script: Mapping[tuple[str, ...], ExecResult] = field(default_factory=dict)
    files: dict[str, bytes] = field(default_factory=dict)
    mounts: dict[str, Path] = field(default_factory=dict)

    async def read_text(self, path: str | Path, encoding: str = "utf-8") -> str:
        key = str(Path(path))
        if key in self.files:
            return self.files[key].decode(encoding)
        for at, src in self.mounts.items():
            if (rel := _under(key, at)) is not None:
                return (src / rel).read_text(encoding=encoding)
        raise FileNotFoundError(path)

    async def write_bytes(self, path: str | Path, content: bytes) -> None:
        rel = Path(path)
        if rel.is_absolute() or ".." in rel.parts:
            raise ValueError(f"write path must be relative and must not escape the sandbox: {path!r}")
        self.files[str(rel)] = content

    async def mount_dir(self, src: Path, at: str | Path, *, read_only: bool = True) -> None:
        self.mounts[str(Path(at))] = Path(src).resolve()

    async def exec(self, *cmd: str, timeout: float | None = None) -> ExecResult:
        if (result := self.script.get(tuple(cmd))) is None:
            raise AssertionError(f"the fake sandbox has no scripted result for {cmd!r}")
        return result


def _under(path: str, at: str) -> Path | None:
    """``path`` relative to the mount point ``at``, or ``None`` if it is not inside it."""
    candidate, mount = Path(path), Path(at)
    return candidate.relative_to(mount) if candidate.is_relative_to(mount) else None


@asynccontextmanager
async def open_fake_sandbox() -> AsyncIterator[FakeSandbox]:
    """The fake, bound ambiently just as the real opener binds itself."""
    sandbox = FakeSandbox(script={PROBE_CMD: PROBE_RESULT})
    with bind_sandbox(sandbox):
        yield sandbox


@dataclass(frozen=True)
class SandboxCase:
    """One implementation to hold to the port's contract."""

    id: str
    opener: Callable[[], AbstractAsyncContextManager[SandboxPort]]


SANDBOX_CASES = (
    SandboxCase(id="unix-local", opener=open_sandbox),
    SandboxCase(id="fake", opener=open_fake_sandbox),
)


__all__ = [
    "PROBE_CMD",
    "PROBE_RESULT",
    "SANDBOX_CASES",
    "FakeSandbox",
    "SandboxCase",
    "open_fake_sandbox",
]
