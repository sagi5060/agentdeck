"""LangGraph checkpointer support for ``BaseWorkflow`` (issue #3): ``durable=True``
compiles with a checkpointer resolved from ``AGENTDECK_CHECKPOINT_*`` settings and
threads ``thread_id`` through so state accumulates per thread; ``durable=False``
(the default) is byte-for-byte today's behavior.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap

import pytest
from pydantic import BaseModel

from agentdeck.runtime.settings import reset_settings_cache
from agentdeck.workflows import END, BaseWorkflow, StateGraph


class CounterState(BaseModel):
    count: int = 0


def _make_counter_workflow(*, durable: bool) -> type[BaseWorkflow]:
    """A fresh workflow class per test — ``_compiled`` is cached on the class itself."""

    class CounterFlow(BaseWorkflow):
        state = CounterState

        @classmethod
        def build_graph(cls):
            g = StateGraph(cls.state)
            # Reads ``s.count`` off whatever state the runner hands the node — the
            # resumed checkpoint on a repeat call with the same thread_id, or the
            # schema default on a fresh thread — so accumulation is observable.
            g.add_node("inc", lambda s: {"count": s.count + 1})
            g.set_entry_point("inc")
            g.add_edge("inc", END)
            return g

    CounterFlow.durable = durable
    return CounterFlow


@pytest.fixture(autouse=True)
def _clean_settings_cache():
    reset_settings_cache()
    yield
    reset_settings_cache()


def test_durable_false_ignores_thread_id_and_never_persists():
    """Zero behavior change for ``durable=False``: no checkpoint, no thread scoping."""
    wf = _make_counter_workflow(durable=False)

    first = asyncio.run(wf.run(None, thread_id="same"))
    second = asyncio.run(wf.run(None, thread_id="same"))

    assert first["count"] == 1
    assert second["count"] == 1  # nothing was persisted between calls


def test_durable_true_without_thread_id_raises():
    wf = _make_counter_workflow(durable=True)

    with pytest.raises(ValueError, match="thread_id"):
        asyncio.run(wf.run(None))


def test_durable_memory_backend_resumes_by_thread_id(monkeypatch):
    monkeypatch.setenv("AGENTDECK_CHECKPOINT_BACKEND", "memory")
    reset_settings_cache()
    wf = _make_counter_workflow(durable=True)

    async def _scenario():
        first = await wf.run(None, thread_id="a")
        second = await wf.run(None, thread_id="a")
        other_thread = await wf.run(None, thread_id="b")
        return first, second, other_thread

    first, second, other_thread = asyncio.run(_scenario())

    assert first["count"] == 1
    assert second["count"] == 2  # resumed thread "a"'s checkpoint
    assert other_thread["count"] == 1  # thread "b" starts fresh


def test_durable_sqlite_backend_persists_across_invokes(tmp_path, monkeypatch):
    """Same-loop sequential invokes, mirroring one long-lived server process."""
    pytest.importorskip("langgraph.checkpoint.sqlite", reason="needs the [durability] extra")
    db_path = tmp_path / "checkpoints.sqlite3"
    monkeypatch.setenv("AGENTDECK_CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setenv("AGENTDECK_CHECKPOINT_URL", str(db_path))
    reset_settings_cache()
    wf = _make_counter_workflow(durable=True)

    async def _scenario():
        first = await wf.run(None, thread_id="a")
        second = await wf.run(None, thread_id="a")
        return first, second

    first, second = asyncio.run(_scenario())

    assert first["count"] == 1
    assert second["count"] == 2
    assert db_path.exists()


def test_durable_sqlite_backend_builds_outside_an_event_loop(tmp_path, monkeypatch):
    """Issue #15: ``App.load()`` calls ``BaseWorkflow.build()`` synchronously, with no
    event loop running at all — not even nested inside a caller's loop. The sqlite
    saver must not assume one exists.
    """
    pytest.importorskip("langgraph.checkpoint.sqlite", reason="needs the [durability] extra")
    db_path = tmp_path / "checkpoints.sqlite3"
    monkeypatch.setenv("AGENTDECK_CHECKPOINT_BACKEND", "sqlite")
    monkeypatch.setenv("AGENTDECK_CHECKPOINT_URL", str(db_path))
    reset_settings_cache()
    wf = _make_counter_workflow(durable=True)

    wf.build()  # must not raise RuntimeError: no running event loop


_RESTART_SCRIPT = """
import asyncio
from pydantic import BaseModel
from agentdeck.workflows import END, BaseWorkflow, StateGraph

class State(BaseModel):
    count: int = 0

class CounterFlow(BaseWorkflow):
    state = State
    durable = True

    @classmethod
    def build_graph(cls):
        g = StateGraph(cls.state)
        g.add_node("inc", lambda s: {"count": s.count + 1})
        g.set_entry_point("inc")
        g.add_edge("inc", END)
        return g

result = asyncio.run(CounterFlow.run(None, thread_id="restart-test"))
print(result["count"])
"""


def test_durable_sqlite_backend_resumes_after_process_restart(tmp_path, monkeypatch):
    """The issue's actual acceptance test: a fresh process, same sqlite file and
    thread_id, picks up where the last process left off — no shared Python object,
    no shared event loop, just the file on disk.
    """
    pytest.importorskip("langgraph.checkpoint.sqlite", reason="needs the [durability] extra")
    db_path = tmp_path / "checkpoints.sqlite3"
    env = {
        **os.environ,
        "AGENTDECK_CHECKPOINT_BACKEND": "sqlite",
        "AGENTDECK_CHECKPOINT_URL": str(db_path),
    }

    first = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_RESTART_SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )
    second = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(_RESTART_SCRIPT)],
        capture_output=True,
        text=True,
        env=env,
        check=True,
    )

    assert int(first.stdout.strip()) == 1
    assert int(second.stdout.strip()) == 2  # new process, same file+thread_id: resumed


def test_unknown_backend_raises(monkeypatch):
    monkeypatch.setenv("AGENTDECK_CHECKPOINT_BACKEND", "not-a-backend")
    reset_settings_cache()
    wf = _make_counter_workflow(durable=True)

    with pytest.raises(ValueError, match="unknown checkpoint backend"):
        asyncio.run(wf.run(None, thread_id="a"))


def test_postgres_resolves_the_async_saver(monkeypatch):
    """Wiring test, no server: the postgres arm must construct the ASYNC saver
    (the runner always calls ``ainvoke``; the sync saver raises NotImplementedError).
    Stubs the [durability] postgres module so no connection is attempted.
    """
    import sys
    import types

    from agentdeck.adapters.engines.langgraph.checkpointer import resolve_checkpointer

    class StubSaver:
        def __init__(self):
            self.setup_awaited = False

        async def setup(self):
            self.setup_awaited = True

    stub_instance = StubSaver()

    class StubAsyncPostgresSaver:
        @staticmethod
        def from_conn_string(url):
            class _Ctx:
                async def __aenter__(self):
                    return stub_instance

            return _Ctx()

    aio_mod = types.ModuleType("langgraph.checkpoint.postgres.aio")
    aio_mod.AsyncPostgresSaver = StubAsyncPostgresSaver
    pg_mod = types.ModuleType("langgraph.checkpoint.postgres")
    pg_mod.aio = aio_mod
    monkeypatch.setitem(sys.modules, "langgraph.checkpoint.postgres", pg_mod)
    monkeypatch.setitem(sys.modules, "langgraph.checkpoint.postgres.aio", aio_mod)

    # unique DSN defeats _postgres_saver's @cache across test runs
    saver = resolve_checkpointer("postgres", "postgresql://stub/wiring-test")

    assert saver is stub_instance
    assert stub_instance.setup_awaited


def test_postgres_without_url_raises():
    from agentdeck.adapters.engines.langgraph.checkpointer import resolve_checkpointer

    with pytest.raises(ValueError, match="DSN"):
        resolve_checkpointer("postgres", "")


def test_run_sync_propagates_exceptions_from_inside_a_loop():
    """A failing bootstrap coroutine must surface its real error, not IndexError."""
    from agentdeck.adapters.engines.langgraph.checkpointer import _run_sync

    async def _boom():
        raise RuntimeError("real cause")

    async def _inside_loop():
        with pytest.raises(RuntimeError, match="real cause"):
            _run_sync(_boom())

    asyncio.run(_inside_loop())


def test_a_sqlite_checkpointer_is_rebuilt_for_each_event_loop_that_asks_for_one(tmp_path):
    """One process, two loops, one checkpoint file: the second loop must not be handed the
    first's saver.

    The async savers hold asyncio primitives — a ``Lock``, and under it a connection — that
    bind to the first loop to *contend* for them, so an uncontended saver crosses loops by
    luck and a busy one raises "bound to a different event loop". Contention is therefore
    created here on purpose, which is also the only condition under which a server would
    ever have noticed.
    """
    pytest.importorskip("langgraph.checkpoint.sqlite", reason="needs the [durability] extra")
    from agentdeck.adapters.engines.langgraph import resolve_checkpointer

    path = str(tmp_path / "checkpoints.sqlite3")
    config = {"configurable": {"thread_id": "t-1", "checkpoint_ns": ""}}

    async def _resolve_and_use():
        saver = resolve_checkpointer("sqlite", path)
        await asyncio.gather(*(saver.aget_tuple(config) for _ in range(4)))
        return saver

    first = asyncio.run(_resolve_and_use())
    second = asyncio.run(_resolve_and_use())

    assert second is not first


def test_one_loop_still_gets_one_checkpointer_however_often_it_asks(tmp_path):
    """The per-loop rebuild is not "stop caching": a server compiling several durable graphs
    on its own loop still shares the one connection, which is what the cache is for."""
    pytest.importorskip("langgraph.checkpoint.sqlite", reason="needs the [durability] extra")
    from agentdeck.adapters.engines.langgraph import resolve_checkpointer

    path = str(tmp_path / "checkpoints.sqlite3")

    async def _resolve_twice():
        return resolve_checkpointer("sqlite", path), resolve_checkpointer("sqlite", path)

    first, second = asyncio.run(_resolve_twice())

    assert first is second
