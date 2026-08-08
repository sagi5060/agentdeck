"""What every ``SandboxPort`` promises, asserted against each implementation.

The engine suite's reason applies unchanged here: a second sandbox that diverges on any of these
diverges silently, because the consumers — a file-loading node, a skill mounting its bundle —
never learn which one they were handed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sandbox_cases import PROBE_CMD, PROBE_RESULT, SANDBOX_CASES, SandboxCase

from agentdeck.core.ports.sandbox import current_sandbox, require_sandbox

if TYPE_CHECKING:
    from collections.abc import AsyncIterator
    from pathlib import Path

    from agentdeck.core.ports.sandbox import SandboxPort


@pytest.fixture(params=SANDBOX_CASES, ids=lambda case: case.id)
def sandbox_case(request: pytest.FixtureRequest) -> SandboxCase:
    return request.param


@pytest.fixture
async def sandbox(sandbox_case: SandboxCase) -> AsyncIterator[SandboxPort]:
    async with sandbox_case.opener() as opened:
        yield opened


async def test_written_bytes_read_back_as_text(sandbox: SandboxPort) -> None:
    await sandbox.write_bytes("note.txt", "héllo".encode())
    assert await sandbox.read_text("note.txt") == "héllo"


async def test_write_creates_missing_parent_directories(sandbox: SandboxPort) -> None:
    """A caller writing ``a/b/c.txt`` never has to make ``a/b`` first — skills emit into
    per-skill subtrees that nothing created in advance."""
    await sandbox.write_bytes("deep/er/still/c.txt", b"landed")
    assert await sandbox.read_text("deep/er/still/c.txt") == "landed"


async def test_absolute_write_path_is_refused(sandbox: SandboxPort) -> None:
    with pytest.raises(ValueError, match="must not escape the sandbox"):
        await sandbox.write_bytes("/etc/passwd", b"nope")


async def test_write_path_climbing_out_is_refused(sandbox: SandboxPort) -> None:
    """The escape that matters: a relative path is not automatically an inside path."""
    with pytest.raises(ValueError, match="must not escape the sandbox"):
        await sandbox.write_bytes("../../escaped.txt", b"nope")


async def test_mounted_directory_is_readable_at_its_mount_point(
    sandbox: SandboxPort,
    tmp_path: Path,
) -> None:
    """How a skill bundle gets in: a host directory outside the sandbox, readable inside it."""
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "entry.py").write_text("print('hi')\n")

    await sandbox.mount_dir(tmp_path / "pkg", "mounted/pkg")

    assert await sandbox.read_text("mounted/pkg/entry.py") == "print('hi')\n"


async def test_exec_reports_streams_as_bytes_and_the_exit_code(sandbox: SandboxPort) -> None:
    """Both streams and a non-zero exit, since a skill's failure path reads all three."""
    result = await sandbox.exec(*PROBE_CMD)
    assert result == PROBE_RESULT


async def test_the_open_sandbox_is_the_ambient_one(sandbox: SandboxPort) -> None:
    """The binding consumers depend on: a node holding no handle still finds this sandbox."""
    assert current_sandbox() is sandbox
    assert require_sandbox() is sandbox


async def test_leaving_the_scope_unbinds_the_sandbox(sandbox_case: SandboxCase) -> None:
    """Nothing outlives its scope — a stale sandbox would serve reads from a deleted workdir."""
    async with sandbox_case.opener():
        pass
    assert current_sandbox() is None
    with pytest.raises(RuntimeError, match="No active sandbox"):
        require_sandbox()
