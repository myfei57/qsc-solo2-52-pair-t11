"""Rollback through tombstones.

Nothing is ever removed from the stream.  A rollback appends tombstone
records that name the sequences they neutralise, and every reader hides
exactly those sequences.
"""

from __future__ import annotations

from typing import Iterable

from .records import Record

# Boundary and action metadata is retired by its own append-only rules
# (reconcile.void / action.void), never neutralised by a domain rollback.
ROLLBACK_EXEMPT_KINDS = frozenset(
    {"reconcile.checkpoint", "reconcile.void", "action.writeoff", "action.void"}
)


def hidden_sequences(records: Iterable[Record]) -> frozenset[int]:
    """Return the sequences neutralised by the tombstones in ``records``."""

    return frozenset(rec.tombstone_of for rec in records if rec.tombstone_of is not None)


def effective_records(records: Iterable[Record]) -> tuple[Record, ...]:
    """Return the records that survive tombstoning, tombstones excluded."""

    materialised = tuple(records)
    hidden = hidden_sequences(materialised)
    return tuple(rec for rec in materialised if not rec.is_tombstone and rec.seq not in hidden)


def rollback_targets(records: Iterable[Record], after_seq: int) -> tuple[int, ...]:
    """Return the committed sequences a rollback down to ``after_seq`` hides.

    Reconciliation and action metadata is exempt: a checkpoint or a void is
    historical fact about what was checked, and rolling back domain records
    must not erase or tombstone it.  The reconciliation layer retires a
    checkpoint by appending a ``reconcile.void`` instead.
    """

    effective = effective_records(records)
    return tuple(
        rec.seq
        for rec in effective
        if rec.seq > after_seq and rec.kind not in ROLLBACK_EXEMPT_KINDS
    )
