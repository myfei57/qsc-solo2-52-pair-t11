"""Digest anchors tie external, ledger and audit reads to one boundary."""

from __future__ import annotations

from mixplant.clock import ManualClock
from mixplant.ids import SequenceIds
from mixplant.reconcile.digest import (
    anchor_projection,
    digest_slice,
    digest_value,
    projections_agree,
)
from mixplant.store.repository import RecordRepository


def _store() -> RecordRepository:
    return RecordRepository.in_memory(ManualClock(), SequenceIds())


def test_digest_is_deterministic_for_the_same_slice() -> None:
    store = _store()
    store.publish("agg.weigh", "agg", 0, {"kg": 1})
    store.publish("audit.entry", "audit", 0, {"active": True})
    records = store.committed_records()
    assert digest_slice(records, through_seq=2) == digest_slice(records, through_seq=2)
    assert digest_slice(records, through_seq=1) != digest_slice(records, through_seq=2)


def test_digest_changes_when_a_tombstone_is_appended() -> None:
    store = _store()
    first = store.publish("agg.weigh", "agg", 0, {"kg": 1})
    store.publish("audit.entry", "audit", 0, {"active": True})
    before = digest_slice(store.committed_records(), through_seq=2)
    store.tombstone(first.seq, reason="rb", origin="console", generation=0)
    after = digest_slice(store.committed_records(), through_seq=store.watermark().committed_seq)
    assert before != after


def test_projection_digests_agree_only_when_identical() -> None:
    projections = anchor_projection(external={"a": 1}, ledger={"a": 1}, audit={"a": 1})
    assert projections_agree(projections) is True
    different = anchor_projection(external={"a": 1}, ledger={"a": 2}, audit={})
    assert projections_agree(different) is False


def test_digest_value_is_stable_across_machines() -> None:
    assert digest_value({"b": 2, "a": 1}) == digest_value({"a": 1, "b": 2})
