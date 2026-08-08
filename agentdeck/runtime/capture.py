"""Provenance capture — who/which session produced an entity, and why.

Formerly imported from the (never-extracted) ``agentdecks_core`` package; the
model is small enough to own here. Two clearly separated owners:

* **Identity** — ``session_id`` / ``author_id`` are built host-side and
  injected into the sandbox env when it opens, so a skill can neither forge nor
  supply them.
* **Role and why** — ``actor`` / ``rationale`` belong to the flow that minted
  the entity.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel

# The one host↔sandbox wire contract: the env var a sandbox serializes a
# ``Capture`` into (built host-side, unspoofable in the sandbox) and the skill
# runtime's ``capture()`` reads back. Both ends import this single name.
CAPTURE_ENV = "SANDBOX_CAPTURE"


class CaptureActor(StrEnum):
    SYSTEM = "system"
    AGENT = "agent"
    USER = "user"


class Capture(BaseModel):
    session_id: str | None = None
    author_id: str | None = None
    actor: CaptureActor | None = None
    rationale: str | None = None


__all__ = ["CAPTURE_ENV", "Capture", "CaptureActor"]
