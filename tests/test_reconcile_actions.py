"""Write-off (核销) and void (作废) are idempotent under concurrency.

The fixed rule is void wins: a void supersedes an earlier write-off and a
write-off arriving after a void is refused.  Every action appends; nothing is
ever deleted.
"""

from __future__ import annotations

import pytest

from mixplant.clock import ManualClock
from mixplant.errors import OrderViolationError, UnknownRecordError
from mixplant.ids import SequenceIds
from mixplant.reconcile_actions import WriteOffService
from mixplant.store.repository import RecordRepository


def _service() -> tuple[RecordRepository, WriteOffService]:
    store = RecordRepository.in_memory(ManualClock(), SequenceIds())
    return store, WriteOffService(store, SequenceIds())


def test_writeoff_appends_once_for_a_repeated_key() -> None:
    store, actions = _service()
    target = store.publish("audit.entry", "audit", 0, {"active": True})
    first = actions.writeoff(target.seq, idem_key="k")
    second = actions.writeoff(target.seq, idem_key="k")
    assert first["idempotent"] is False
    assert second["idempotent"] is True
    assert sum(1 for record in store.committed_records() if record.kind == "action.writeoff") == 1
    assert actions.status()["active_writeoffs"]


def test_void_appends_once_for_a_repeated_key() -> None:
    store, actions = _service()
    target = store.publish("audit.entry", "audit", 0, {"active": True})
    actions.void(target.seq, idem_key="v")
    actions.void(target.seq, idem_key="v")
    assert sum(1 for record in store.committed_records() if record.kind == "action.void") == 1


def test_void_supersedes_a_prior_writeoff_but_keeps_the_history() -> None:
    store, actions = _service()
    target = store.publish("audit.entry", "audit", 0, {"active": True})
    actions.writeoff(target.seq, idem_key="w")
    actions.void(target.seq, idem_key="v", reason="fraud")
    status = actions.status()
    assert not status["active_writeoffs"]
    assert len(status["superseded_writeoffs"]) == 1
    # the write-off record still exists; it was not rewritten or deleted
    assert sum(1 for record in store.committed_records() if record.kind == "action.writeoff") == 1


def test_writeoff_after_void_is_refused() -> None:
    store, actions = _service()
    target = store.publish("audit.entry", "audit", 0, {"active": True})
    actions.void(target.seq, idem_key="v")
    with pytest.raises(OrderViolationError):
        actions.writeoff(target.seq, idem_key="w-late")


def test_distinct_keys_both_act_but_void_still_wins() -> None:
    store, actions = _service()
    target = store.publish("audit.entry", "audit", 0, {"active": True})
    # two concurrent writers with different keys both append a write-off; the
    # void that races them still retires the target.
    actions.writeoff(target.seq, idem_key="w-1")
    actions.writeoff(target.seq, idem_key="w-2")
    actions.void(target.seq, idem_key="v")
    assert not actions.status()["active_writeoffs"]


def test_action_target_must_be_committed() -> None:
    _store, actions = _service()
    with pytest.raises(UnknownRecordError):
        actions.writeoff(99, idem_key="w")
    with pytest.raises(UnknownRecordError):
        actions.void(99, idem_key="v")


def test_action_requires_an_idempotency_key() -> None:
    store, actions = _service()
    target = store.publish("audit.entry", "audit", 0, {"active": True})
    with pytest.raises(Exception):  # noqa: PT011 - validation of empty key
        actions.writeoff(target.seq, idem_key="")
