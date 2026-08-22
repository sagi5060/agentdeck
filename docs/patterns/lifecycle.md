# Run Lifecycle

One concept has one home. Externally initiated lifecycle operations (cancel, pause, resume, answer) are methods on the existing handles (`Run` in `deck.py`, `Runtime.signal` in `runtime/service.py`); state transitions flow through the runtime's state machine.

Good (real, `deck.py`): timeout-like behavior asks the existing handle:

```python
await run.cancel(reason="timeout")
```

Bad, the classic agent artifact:

```python
class TimeoutManager:            # second lifecycle path
    def _terminate_run(self): ...
```

A close or abandon path that writes nothing says so, and separates the anomalous half from the benign one. The caller has already logged that it closed the run, so a bare `return` leaves the operator record and the log disagreeing with nothing in between to explain which is right.

Good (real, `runtime/service.py`):

```python
if started is None:
    logger.error("run %s is being abandoned but has no run.started to close it against", run_id)
    return
if (status := await self._store.run_status(ctx)) is not RunStatus.RUNNING:
    logger.debug("run %s is %s, not RUNNING; nothing to close", run_id, status)
    return
```

Bad:

```python
if started is None or await self._store.run_status(ctx) is not RunStatus.RUNNING:
    return
```

Rules:
- No `*Manager`/`*Controller`/`*Handler` class whose responsibility an existing handle owns; check `uv run scripts/repomap.py` first.
- An early return on a `close_*`/`_close_*`/`_abandon*` path names the run and why it wrote nothing: a run already over is `debug`, an unreadable opening is `error`.
- Never mutate run state directly; go through the transition path.
- `asyncio.create_task` belongs to the runtime (`runtime/dispatch.py`, `runtime/service.py`, `deck.py`); a task created elsewhere bypasses the run's cancellation and drain guarantees (enforced by ruff TID251). One exception, allowlisted with its own owner: the native executor runs a `@workflow` body that parks mid-await, so the coroutine outlives the call that started it  -  it is held in that executor's own registry and cancelled by its `aclose()`.
- Control verbs are signals, not errors: honoring a cancel raises `ControlSignalled`, which records the effect.
