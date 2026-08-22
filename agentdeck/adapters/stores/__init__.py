"""Event-log stores  -  implementations of ``EventStorePort``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from agentdeck.core.status import RunStatus
from agentdeck.errors import RunStateError

if TYPE_CHECKING:
    from agentdeck.core.context import RunContext


def _refuse_if_cancelled(status: RunStatus | None, ctx: RunContext) -> None:
    """Refuse the append a store is inside when the run is already ``CANCELLED``.

    Shared by all four stores rather than written out in each: the refusal is one decision the
    port states and every backend owes, and it is called from inside whatever makes that
    backend's write indivisible  -  the only place a write already suspended in there can still
    be stopped. It lives here and not on the port because ``agentdeck.core`` may not name an
    error type.

    Cancelled and not every terminal status: a takeover's ``run.failed`` is advisory, and a run
    that turns out to be alive after being stepped over goes on writing and may reclaim its own
    session. That is deliberate (ADR-D11 §5) and this refusal leaves it alone.
    """
    if status is RunStatus.CANCELLED:
        raise RunStateError(f"run {ctx.run_id!r} was cancelled; nothing can be appended to it any more")
