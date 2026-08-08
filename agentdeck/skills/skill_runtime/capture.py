"""The one ``SANDBOX_CAPTURE`` reader every capture-aware skill shares.

Provenance on a skill-minted entity has two clearly separated owners:

* **Who / which session** — ``session_id`` and ``author_id`` ride in from the
  sandbox-injected :data:`CAPTURE_ENV`. Built host-side, so a skill can neither
  forge nor supply them. The skill never sets these.
* **Role and why** — ``actor`` and ``rationale`` belong to the *flow*. A skill sets
  ``actor`` to the role that produced *this* entity (``SYSTEM`` for an automatic
  transform such as translate or a mechanical split, ``AGENT`` for LLM judgment such
  as classify/synthesize) and ``rationale`` to the model's caption or a short note.
"""

from __future__ import annotations

import os

from agentdeck.runtime.capture import CAPTURE_ENV, Capture, CaptureActor


def capture(*, actor: CaptureActor | None = None, rationale: str | None = None) -> Capture | None:
    """Provenance for a minted entity: env-injected session identity, with the
    flow's ``actor``/``rationale`` layered on top.

    Returns ``None`` only when there is no injected identity *and* the flow set
    neither ``actor`` nor ``rationale`` (the plain-upload case).
    """
    raw = os.getenv(CAPTURE_ENV)
    base = Capture.model_validate_json(raw) if raw else None
    updates: dict[str, object] = {}
    if actor is not None:
        updates["actor"] = actor
    if rationale is not None:
        updates["rationale"] = rationale
    if not updates:
        return base
    return (base or Capture()).model_copy(update=updates)


__all__ = ["CAPTURE_ENV", "capture"]
