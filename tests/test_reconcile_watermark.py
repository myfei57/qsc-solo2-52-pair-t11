"""The reconciliation watermark gates external reads and survives rollback."""

from __future__ import annotations

import pytest

from mixplant.clock import ManualClock
from mixplant.ids import SequenceIds
from mixplant.reconcile.ledger import ReconciliationLedger
from mixplant.reconcile.policy import VOID_AFFECTED, VOID_SEGMENT
from mixplant.reconcile.watermark import BoundaryError
from mixplant.store.repository import RecordRepository


def _ledger() -> tuple[RecordRepository, ReconciliationLedger]:
    store = RecordRepository.in_memory(ManualClock(), SequenceIds())
    return store, ReconciliationLedger(store, SequenceIds())


def test_unreconciled_committed_records_are_invisible_externally() -> None:
    store, ledger = _ledger()
    store.publish("agg.weigh", "agg", 0, {"active": True})
    assert ledger.readable_records() == ()
    assert ledger.watermark().through_seq == 0


def test_reconcile_advances_the_boundary() -> None:
    store, ledger = _ledger()
    record = store.publish("agg.weigh", "agg", 0, {"active": True})
    result = ledger.reconcile()
    assert result.idempotent is False
    assert result.watermark.through_seq == record.seq
    assert len(ledger.readable_records()) == 1
    assert result.checkpoint.digest


def test_reconcile_refuses_an_empty_stream() -> None:
    _store, ledger = _ledger()
    with pytest.raises(BoundaryError):
        ledger.reconcile()


def test_reconcile_is_idempotent_by_key() -> None:
    store, ledger = _ledger()
    store.publish("agg.weigh", "agg", 0, {"active": True})
    first = ledger.reconcile(idem_key="k")
    store.publish("audit.entry", "audit", 0, {"active": True})
    second = ledger.reconcile(idem_key="k")
    assert second.record.seq == first.record.seq
    assert second.idempotent is True


def test_reconcile_at_the_tip_is_a_noop_once_the_slice_is_unchanged() -> None:
    store, ledger = _ledger()
    store.publish("agg.weigh", "agg", 0, {"active": True})
    ledger.reconcile()
    repeat = ledger.reconcile()
    assert repeat.idempotent is True


def test_rollback_voids_every_checkpoint_past_the_point() -> None:
    store, ledger = _ledger()
    first = store.publish("agg.weigh", "agg", 0, {"active": True})
    ledger.reconcile(idem_key="c1")
    store.publish("audit.entry", "audit", 0, {"active": True})
    ledger.reconcile(idem_key="c2")

    tombstoned = tuple(seq for seq in range(first.seq + 1, store.watermark().committed_seq + 1))
    voids = ledger.void_after(
        first.seq,
        tombstoned_seqs=tombstoned,
        affected_kinds={"agg.weigh": VOID_SEGMENT, "audit.entry": VOID_AFFECTED},
    )
    assert len(voids) == 1
    statuses = [entry.valid for entry in ledger.checkpoints()]
    assert statuses == [True, False]
    # boundary retreats to the surviving prefix, not to zero
    assert ledger.watermark().through_seq == first.seq


def test_void_is_idempotent_by_key() -> None:
    store, ledger = _ledger()
    first = store.publish("agg.weigh", "agg", 0, {"active": True})
    ledger.reconcile(idem_key="c1")
    store.publish("audit.entry", "audit", 0, {"active": True})
    ledger.reconcile(idem_key="c2")
    common = dict(
        tombstoned_seqs=(first.seq + 1,),
        affected_kinds={"audit.entry": VOID_AFFECTED},
        idem_key="v",
    )
    once = ledger.void_after(first.seq, **common)
    twice = ledger.void_after(first.seq, **common)
    assert len(once) == 1
    assert [void.void_id for void in twice] == [void.void_id for void in once]
    assert sum(1 for record in store.committed_records() if record.kind == "reconcile.void") == 1


def test_segment_policy_voids_the_whole_kind_range() -> None:
    store, ledger = _ledger()
    weigh_one = store.publish("agg.weigh", "agg", 0, {"kg": 1})
    weigh_two = store.publish("agg.weigh", "agg", 0, {"kg": 2})
    audit = store.publish("audit.entry", "audit", 0, {"active": True})
    ledger.reconcile()
    # neutralise the first weighing; segment scope must pull the second in too
    # but leave the audit record of the different kind alone.
    voids = ledger.void_after(
        weigh_one.seq - 1,
        tombstoned_seqs=(weigh_one.seq,),
        affected_kinds={"agg.weigh": VOID_SEGMENT, "audit.entry": VOID_AFFECTED},
    )
    affected = set(voids[0].affected_seqs)
    assert weigh_one.seq in affected and weigh_two.seq in affected
    assert audit.seq not in affected


def test_affected_policy_only_voids_neutralised_records() -> None:
    store, ledger = _ledger()
    audit = store.publish("audit.entry", "audit", 0, {"active": True})
    weigh = store.publish("agg.weigh", "agg", 0, {"kg": 1})
    ledger.reconcile()
    voids = ledger.void_after(
        audit.seq - 1,
        tombstoned_seqs=(audit.seq,),
        affected_kinds={"audit.entry": VOID_AFFECTED},
    )
    assert voids[0].affected_seqs == (audit.seq,)
    assert weigh.seq not in voids[0].affected_seqs


def test_history_returns_the_boundary_valid_at_the_tick() -> None:
    store, ledger = _ledger()
    first = store.publish("agg.weigh", "agg", 0, {"active": True})
    first_checkpoint = ledger.reconcile()
    first_tick = first_checkpoint.checkpoint.tick
    store._clock.advance(5)
    second = store.publish("audit.entry", "audit", 0, {"active": True})
    second_checkpoint = ledger.reconcile()
    second_tick = second_checkpoint.checkpoint.tick
    store._clock.advance(5)
    ledger.void_after(
        first.seq,
        tombstoned_seqs=(second.seq,),
        affected_kinds={"audit.entry": VOID_AFFECTED},
    )
    # the current boundary retreated to the first checkpoint ...
    assert ledger.watermark().through_seq == first.seq
    # ... but a tick after the second checkpoint still reports it as valid then
    assert ledger.checkpoint_at(second_tick).through_seq == second.seq
    assert ledger.checkpoint_at(first_tick).through_seq == first.seq


def test_voided_checkpoint_record_stays_in_the_journal() -> None:
    store, ledger = _ledger()
    first = store.publish("agg.weigh", "agg", 0, {"active": True})
    first_checkpoint = ledger.reconcile(idem_key="c1")
    store.publish("audit.entry", "audit", 0, {"active": True})
    second_checkpoint = ledger.reconcile(idem_key="c2")
    ledger.void_after(
        first.seq,
        tombstoned_seqs=(first.seq + 1,),
        affected_kinds={"audit.entry": VOID_AFFECTED},
    )
    kinds = [record.kind for record in store.committed_records()]
    assert kinds.count("reconcile.checkpoint") == 2
    assert kinds.count("reconcile.void") == 1
    # the physical checkpoint record is untouched and still addressable
    assert second_checkpoint.record.seq in {record.seq for record in store.committed_records()}
    assert first_checkpoint.record.seq in {record.seq for record in store.committed_records()}
