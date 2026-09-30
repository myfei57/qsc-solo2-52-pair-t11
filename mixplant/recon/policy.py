"""Fixed record type policy: who is a ledger record and how a void spreads.

The scope of a void is *decided by the record type* and this table is the
single, immutable decision.  It is deliberately code, not configuration, so a
running station cannot widen or narrow the blast radius of a void without a
versioned deployment.

Two scopes exist:

``segment``
    A segment (batch summary, roll-up, published recipe/conclusion, baseline)
    is an aggregate of the records around it.  Voiding it is meaningless for
    any individual row, so the whole reconciled segment is invalidated: the
    anchor and every later reconciliation are voided.

``record``
    A line item is independently meaningful.  Voiding it removes only the
    affected record; reconciliations of unrelated, still surviving records are
    preserved.
"""

from __future__ import annotations

from ..errors import ValidationError
from ..store.records import Record

MARK_KIND = "recon.mark"
VOID_KIND = "recon.void"
ROLLBACK_KIND = "recon.rollback"
PUBLISH_KIND = "recon.publish"
RECON_KINDS = (MARK_KIND, VOID_KIND, ROLLBACK_KIND, PUBLISH_KIND)

POLICY_RECORD = "record"
POLICY_SEGMENT = "segment"

# Control records are bookkeeping of the platform itself; they are never the
# substance of the ledger and are never reconciled as line items.
_CONTROL_GROUPS = frozenset({"recon", "store", "station"})

# Leaf kinds (``<group>.<leaf>``) that aggregate other rows.  Fixed by type.
_SEGMENT_KINDS = frozenset(
    {
        "agg.batch",
        "audit.rollup",
        "recipe.published",
        "version.baseline",
        "sampler.report",
    }
)


def is_accountable(record: Record) -> bool:
    """Return whether ``record`` is ledger substance the outside world can read.

    Control records (reconciliation marks, tombstones, boot markers, clock
    ticks) and the bus-to-ledger audit mirrors are plumbing, not the ledger.
    """

    if record.is_tombstone:
        return False
    if record.group in _CONTROL_GROUPS:
        return False
    if record.kind == "audit.event":
        return False
    return True


def policy_for_kind(kind: str) -> str:
    """Return the fixed void scope for an accountable ``kind``."""

    if kind in _SEGMENT_KINDS:
        return POLICY_SEGMENT
    return POLICY_RECORD


def policy_for(record: Record) -> str:
    """Return the void scope of an accountable record or refuse the call."""

    if not is_accountable(record):
        raise ValidationError("record type cannot be reconciled", kind=record.kind, seq=record.seq)
    return policy_for_kind(record.kind)


def is_segment_kind(kind: str) -> bool:
    return kind in _SEGMENT_KINDS
