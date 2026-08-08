"""Resolve a LangGraph checkpointer for the langgraph engine's runs.

Relocated from ``agentdeck.runtime.checkpointer``, which was written for v1's
``BaseWorkflow`` durability but holds exactly the state a checkpointer engine must keep
private to its own adapter (ADR-D5: execution state belongs to the engine that produced
it, never shared or derived by an outer ring) — the same relationship ``sessions.py`` has
to the openai-agents adapter. ``agentdeck.workflows.base`` (v1, frozen behavior) imports
this module directly and translates its own ``CheckpointSettings`` into the plain
``(backend, url)`` this function takes. ``memory`` ships with core ``langgraph`` and needs nothing extra;
``sqlite`` / ``postgres`` live in the optional ``[durability]`` extra
(``langgraph-checkpoint-sqlite`` / ``langgraph-checkpoint-postgres``) and are imported
lazily, only when actually requested, with a clear install hint if the extra is missing.

Connection lifecycle: one saver per backend+url **per event loop**, so repeated calls
against the same file reuse the same connection instead of opening one per compile, without
handing a second loop a saver that is bound to the first (see ``_per_loop``).
"""

from __future__ import annotations

import asyncio
import threading
from functools import cache, partial
from typing import TYPE_CHECKING, Any, TypeVar
from weakref import WeakKeyDictionary

if TYPE_CHECKING:
    from asyncio import AbstractEventLoop
    from collections.abc import Callable, Coroutine, MutableMapping

    from langgraph.checkpoint.base import BaseCheckpointSaver

_DURABILITY_HINT = 'install the "durability" extra: pip install "agentdeck[durability]"'

_T = TypeVar("_T")

_savers: MutableMapping[AbstractEventLoop, dict[tuple[str, str], BaseCheckpointSaver]] = WeakKeyDictionary()


def _per_loop(backend: str, url: str, build: Callable[[], BaseCheckpointSaver]) -> BaseCheckpointSaver:
    """Cache ``build``'s saver against the running event loop rather than the process.

    The async sqlite and postgres savers hold asyncio primitives — a ``Lock``, and under it a
    connection — that bind to the first loop to *contend* for them. A process-wide cache
    therefore hands a second loop a saver the first one owns, and the second loop's first
    concurrent access dies with "bound to a different event loop": fine for a server, which
    is one loop for its lifetime, and broken for anything that runs more than one.

    Resolved with no loop running (a script that compiles its graph before ``asyncio.run``),
    nothing is cached: there is no loop to key on, and the saver will bind to whichever one
    first uses it — so caching it is precisely how the same breakage would come back. That
    costs one connection per resolution on a path that resolves once.

    What the weak keying does *not* buy: a saver ends up referencing the loop it bound to
    (through that same lock), so each entry keeps its own key alive and nothing is collected
    until ``_savers`` itself is. A process that runs many loops in a row therefore accumulates
    one connection and one aiosqlite thread per loop — the wrong half of a trade whose right
    half is a saver that works on the second loop. Zero effect on a server. ponytail: bounding
    it means closing the savers at loop shutdown, i.e. owning their lifecycle — worth doing
    when something long-lived actually runs loops in a row.
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return build()
    per_loop = _savers.setdefault(loop, {})
    key = (backend, url)
    if key not in per_loop:
        per_loop[key] = build()
    return per_loop[key]


def _run_sync(coro: Coroutine[None, None, _T]) -> _T:
    """Run ``coro`` to completion, whether or not an event loop is already running.

    The engine may resolve a checkpointer lazily from *inside* an async ``start()`` call,
    so plain ``asyncio.run`` would collide with the running loop. The one-shot bootstrap
    connection (aiosqlite's async handshake) is cheap enough to hand to a throwaway
    thread+loop in that case; all later query traffic runs on the caller's own loop.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    result: list[_T] = []
    error: list[BaseException] = []

    def _runner() -> None:
        try:
            result.append(asyncio.run(coro))
        except BaseException as exc:  # noqa: BLE001 — re-raised on the calling thread below
            error.append(exc)

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()
    if error:
        raise error[0]
    return result[0]


def resolve_checkpointer(backend: str, url: str = "") -> BaseCheckpointSaver:
    """Build the checkpointer named by ``backend`` (``memory`` / ``sqlite`` / ``postgres``).

    ``url`` is the sqlite file path or the Postgres DSN — primitives, not a settings
    object, so this adapter takes core plus langgraph and nothing else. Raises
    ``ValueError`` for an unknown backend and ``ImportError`` (with an install hint) when
    ``sqlite``/``postgres`` is requested but the ``[durability]`` extra isn't installed.
    """
    normalized = backend.strip().lower()
    if normalized == "memory":
        return _memory_saver()
    if normalized == "sqlite":
        return _sqlite_saver(url)
    if normalized == "postgres":
        return _postgres_saver(url)
    raise ValueError(f"unknown checkpoint backend {backend!r}; expected sqlite, postgres, or memory")


@cache
def _memory_saver() -> BaseCheckpointSaver:
    """Process-wide on purpose, unlike the durable two: ``MemorySaver`` is plain dicts with
    no loop-bound primitive, and sharing it is what lets ``durable = True`` on the memory
    backend resume across two ``asyncio.run`` calls at all — its threads live nowhere else."""
    from langgraph.checkpoint.memory import MemorySaver

    return MemorySaver()


def _sqlite_saver(url: str) -> BaseCheckpointSaver:
    """``AsyncSqliteSaver`` for ``url``, one connection per event loop (see ``_per_loop``)."""
    return _per_loop("sqlite", url, partial(_build_sqlite_saver, url))


def _build_sqlite_saver(url: str) -> BaseCheckpointSaver:
    try:
        import aiosqlite  # ty: ignore[unresolved-import] — [durability] extra
        from langgraph.checkpoint.sqlite import aio as sqlite_aio  # ty: ignore[unresolved-import] — [durability] extra
    except ImportError as exc:
        raise ImportError(
            f"checkpoint backend 'sqlite' needs langgraph-checkpoint-sqlite — {_DURABILITY_HINT}"
        ) from exc

    # AsyncSqliteSaver.__init__ needs a running loop — build it inside _run_sync, matching postgres.
    path = url or ".agentdeck/checkpoints.sqlite3"

    async def _connect_and_build() -> BaseCheckpointSaver:
        conn = aiosqlite.connect(path)
        # aiosqlite's per-connection worker thread is non-daemon, and nothing ever closes a
        # cached connection, so a normal exit would hang forever joining it.
        conn._thread.daemon = True  # noqa: SLF001 — aiosqlite exposes no public way to set this
        await conn
        saver = sqlite_aio.AsyncSqliteSaver(conn)
        await saver.setup()
        return saver

    saver: Any = _run_sync(_connect_and_build())
    return saver


def _postgres_saver(url: str) -> BaseCheckpointSaver:
    """``AsyncPostgresSaver`` for ``url``, one connection per event loop (see ``_per_loop``)."""
    if not url:
        raise ValueError("checkpoint backend 'postgres' needs a DSN")
    return _per_loop("postgres", url, partial(_build_postgres_saver, url))


def _build_postgres_saver(url: str) -> BaseCheckpointSaver:
    try:
        from langgraph.checkpoint.postgres.aio import (  # ty: ignore[unresolved-import] — [durability] extra
            AsyncPostgresSaver,
        )
    except ImportError as exc:
        raise ImportError(
            f"checkpoint backend 'postgres' needs langgraph-checkpoint-postgres — {_DURABILITY_HINT}",
        ) from exc

    # Async saver, same reason as sqlite: the engine always calls ``ainvoke``/``astream``.
    # ``from_conn_string`` is an async contextmanager owning the connection; we enter it
    # manually and let the caller cache the saver.
    saver: Any = _run_sync(AsyncPostgresSaver.from_conn_string(url).__aenter__())
    _run_sync(saver.setup())
    return saver


__all__ = ["resolve_checkpointer"]
