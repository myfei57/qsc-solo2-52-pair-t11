"""How a void spreads, fixed by the record type policy.

Two pure questions live here:

* ``explicit_void_targets`` answers "the operator voided this record": a
  ``record`` type removes only the affected record, a ``segment`` type removes
  the anchor and the reconciled rows of its segment.
* ``rollback_cascade`` answers "the ledger was rolled back to ``point``":
  reconciliations whose anchor disappeared are always invalidated, and a
  segment anchor whose segment was cut is invalidated in full, while a
  ``record`` type keeps every reconciliation on or before the rollback point.

Both functions describe *which reconciliations must be voided*.  The events
that carry the answer are appended elsewhere; nothing here deletes anything.
"""

from __future__ import annotations

from typing import Iterable

from ..store.records import Record
from ..store.tombstone import effective_records, hidden_sequences
from .events import ReconEvent, event_from_record
from .policy import POLICY_RECORD, POLICY_SEGMENT, is_accountable, policy_for_kind

RECON_GROUP = "recon"


def _accountable(records: Iterable[Record]) -> list[Record]:
    return [rec for rec in records if not rec.is_tombstone and is_accountable(rec)]


def segment_end(kinds: tuple[tuple[int, str], ...], anchor_seq: int) -> int:
    """Return the exclusive end of the segment opened by ``anchor_seq``.

    A segment runs from one segment anchor up to (not including) the next
    segment anchor; with no later anchor it runs to the end of the stream.
    """

    for seq, kind in kinds:
        if seq > anchor_seq and policy_for_kind(kind) == POLICY_SEGMENT:
            return seq
    return 1 << 30


def effective_marks(events: Iterable[ReconEvent]) -> dict[int, ReconEvent]:
    """Return the standing mark of every target (latest mark, not later voided)."""

    marked: dict[int, ReconEvent] = {}
    for event in events:
        if event.is_mark:
            if event.target_seq not in marked or event.seq > marked[event.target_seq].seq:
                marked[event.target_seq] = event
        elif event.is_void and event.target_seq in marked:
            if event.seq > marked[event.target_seq].seq:
                marked.pop(event.target_seq)
    return marked


def explicit_void_targets(
    surviving: Iterable[Record],
    anchor_seq: int,
) -> tuple[tuple[int, str], ...]:
    """Return ``(target_seq, policy)`` rows an explicit void must invalidate."""

    surviving = tuple(surviving)
    by_seq = {rec.seq: rec for rec in surviving}
    anchor = by_seq.get(anchor_seq)
    if anchor is None or not is_accountable(anchor):
        return ()
    policy = policy_for_kind(anchor.kind)
    if policy == POLICY_RECORD:
        return ((anchor_seq, POLICY_RECORD),)

    kinds = tuple(sorted((rec.seq, rec.kind) for rec in surviving if is_accountable(rec)))
    end = segment_end(kinds, anchor_seq)
    events = tuple(event for rec in surviving if (event := event_from_record(rec)) is not None)
    closed = effective_marks(events)
    targets = [(anchor_seq, POLICY_SEGMENT)]
    for target_seq, mark in closed.items():
        if anchor_seq < target_seq < end:
            targets.append((target_seq, policy_for_kind(by_seq[target_seq].kind)))
    return tuple(targets)


def rollback_cascade(
    committed: Iterable[Record],
    point_seq: int,
) -> tuple[tuple[int, str, str], ...]:
    """Return ``(target_seq, policy, reason)`` rows a rollback must void.

    ``committed`` is the full committed stream *before* the rollback appends,
    tombstones included, so the policy of a record that is about to disappear
    can still be read.
    """

    committed = tuple(committed)
    already_hidden = hidden_sequences(committed)
    # The records this rollback is about to remove: currently surviving, after
    # the point, and not part of the append only recon control stream.
    pending_removed = frozenset(
        rec.seq
        for rec in effective_records(committed)
        if rec.seq > point_seq and rec.group != RECON_GROUP
    )
    hidden = already_hidden | pending_removed
    survivors = tuple(rec for rec in committed if not rec.is_tombstone and rec.seq not in already_hidden)
    by_seq = {rec.seq: rec for rec in committed if not rec.is_tombstone}

    events = tuple(event for rec in survivors if (event := event_from_record(rec)) is not None)
    closed = effective_marks(events)

    voids: list[tuple[int, str, str]] = []
    seen: set[int] = set()

    def add(target_seq: int, policy: str, reason: str) -> None:
        if target_seq not in seen:
            seen.add(target_seq)
            voids.append((target_seq, policy, reason))

    # 1. Reconciliation whose anchor the rollback removed is always dead.
    for target_seq in sorted(closed):
        if target_seq in pending_removed:
            kind = by_seq[target_seq].kind
            add(target_seq, policy_for_kind(kind), f"rollback:{point_seq}:anchor_removed")

    # 2. Segment anchors on/before the point whose segment was cut lose the
    #    whole segment; record types keep their reconciled prefix.
    kinds = tuple(sorted((rec.seq, rec.kind) for rec in _accountable(committed)))
    for target_seq in sorted(closed):
        anchor = by_seq.get(target_seq)
        if anchor is None or target_seq in seen or target_seq > point_seq:
            continue
        if policy_for_kind(anchor.kind) != POLICY_SEGMENT:
            continue
        end = segment_end(kinds, target_seq)
        if any(target_seq < seq < end for seq in pending_removed):
            add(target_seq, POLICY_SEGMENT, f"rollback:{point_seq}:segment_cut")

    return tuple(voids)
