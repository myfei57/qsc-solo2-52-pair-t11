"""The reconciliation watermark value object.

Distinct from the store's commit watermark: commit says "durable", the
reconciliation watermark says "checked and externally visible".  It can move
backwards when a rollback voids checkpoints, but only by appending a void; the
value itself is always recomputed from the stream, never edited in place.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..errors import RecordError
from .checkpoint import Checkpoint, CheckpointStatus, STATUS_VALID


class BoundaryError(RecordError):
    """A read or advance crossed the reconciliation boundary illegally."""

    code = "reconcile_boundary"
    http_status = 409


@dataclass(frozen=True, slots=True)
class ReconcileWatermark:
    """How far the stream has been reconciled and is safe to show outside."""

    through_seq: int
    checkpoint_id: str | None
    tick: int
    digest: str | None
    status: str = STATUS_VALID

    @property
    def reconciled(self) -> bool:
        return self.through_seq > 0 and self.status == STATUS_VALID

    def describe(self) -> dict[str, Any]:
        return {
            "through_seq": self.through_seq,
            "checkpoint_id": self.checkpoint_id,
            "tick": self.tick,
            "digest": self.digest,
            "status": self.status,
        }

    def covers(self, seq: int) -> bool:
        """Return whether ``seq`` is inside the reconciled, still-valid range."""

        return self.status == STATUS_VALID and seq <= self.through_seq


EMPTY_WATERMARK = ReconcileWatermark(through_seq=0, checkpoint_id=None, tick=0, digest=None)


def watermark_from(checkpoints: list[CheckpointStatus], *, now: int) -> ReconcileWatermark:
    """Recompute the watermark from checkpoint/void history.

    Checkpoints extend a prefix: 1..8 then 9..10.  When a checkpoint is voided
    every boundary it established is retired and the watermark falls back to
    the newest *unvoided* checkpoint before it (the surviving prefix).  It
    never keeps advertising a retired boundary.  Because a later rollback can
    only void checkpoints at or beyond its point, the surviving valid
    checkpoints are exactly a leading run of ``checkpoints``.
    """

    watermark: ReconcileWatermark = EMPTY_WATERMARK
    previous_through = 0
    for index, entry in enumerate(checkpoints):
        checkpoint: Checkpoint = entry.checkpoint
        if not entry.valid:
            # This checkpoint is retired.  The surviving boundary is the
            # newest still-valid checkpoint that precedes it; anything at or
            # beyond a voided checkpoint cannot advertise (a void only happens
            # because a rollback reached that checkpoint's range).
            watermark = EMPTY_WATERMARK
            for earlier in checkpoints[:index]:
                if earlier.valid:
                    watermark = ReconcileWatermark(
                        through_seq=earlier.checkpoint.through_seq,
                        checkpoint_id=earlier.checkpoint.checkpoint_id,
                        tick=earlier.checkpoint.tick,
                        digest=earlier.checkpoint.digest,
                    )
            return watermark
        if checkpoint.through_seq <= previous_through:
            raise BoundaryError(
                "a checkpoint must extend the reconciled range",
                checkpoint=checkpoint.checkpoint_id,
                through_seq=checkpoint.through_seq,
                current=previous_through,
            )
        watermark = ReconcileWatermark(
            through_seq=checkpoint.through_seq,
            checkpoint_id=checkpoint.checkpoint_id,
            tick=checkpoint.tick,
            digest=checkpoint.digest,
        )
        previous_through = checkpoint.through_seq
    return watermark
