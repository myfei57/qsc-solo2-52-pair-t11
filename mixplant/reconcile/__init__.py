"""Reconciliation watermark, void policy and as-of read boundary.

The record stream is append only and already carries a *commit* watermark.
Commit only means "durable"; it does not mean "checked".  This package adds a
second, independent boundary: the reconciliation checkpoint.  External reads
are only allowed to see records a checkpoint has vouched for.

Nothing here is mutable history.  Checkpoints and voids are themselves records
appended to the same stream; voiding a checkpoint appends a void record and
never rewrites or deletes the checkpoint it names.
"""

from .checkpoint import (
    CHECKPOINT_KIND,
    VOID_KIND,
    Checkpoint,
    CheckpointStatus,
    Void,
    checkpoint_from_record,
    checkpoint_payload,
    void_from_record,
    void_payload,
)
from .ledger import ReconcileResult, ReconciliationLedger
from .policy import VOID_AFFECTED, VOID_SEGMENT, VoidPolicy, policy_for, policy_table
from .watermark import (
    BoundaryError,
    ReconcileWatermark,
)

__all__ = [
    "CHECKPOINT_KIND",
    "VOID_KIND",
    "BoundaryError",
    "Checkpoint",
    "CheckpointStatus",
    "ReconcileResult",
    "ReconcileWatermark",
    "ReconciliationLedger",
    "VOID_AFFECTED",
    "VOID_SEGMENT",
    "Void",
    "VoidPolicy",
    "checkpoint_from_record",
    "checkpoint_payload",
    "policy_for",
    "policy_table",
    "void_from_record",
    "void_payload",
]
