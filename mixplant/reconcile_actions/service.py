"""Write-off / void service with a fixed concurrency rule.

Precedence is frozen here, not chosen at call time: **void wins**.  If a void
and a write-off race for the same target, the void is the surviving outcome;
a write-off that lands after the void is refused.  A void that lands after a
write-off retires it (by appending a superseding marker) rather than deleting
it.  Repeating either call with its idempotency key returns the original
record, so concurrent retries from a network blip never double-act.
"""

from __future__ import annotations

from ..errors import OrderViolationError, UnknownRecordError, ValidationError
from ..ids import SequenceIds
from ..store.repository import RecordRepository
from .actions import (
    ACTION_ACTIVE,
    VOIDACTION_KIND,
    WRITEOFF_KIND,
    VoidActionRecord,
    WriteOffRecord,
)


class WriteOffService:
    """Serialises write-off and void actions through one append log."""

    origin = "action"

    __slots__ = ("_store", "_ids")

    def __init__(self, store: RecordRepository, ids: SequenceIds) -> None:
        self._store = store
        self._ids = ids

    def _journal(self):
        return self._store.committed_records()

    def _find(self, kind: str, idem_key: str):
        for record in self._journal():
            if record.kind == kind and record.payload.get("idem_key") == idem_key:
                return record
        return None

    def _void_of(self, target_seq: int) -> VoidActionRecord | None:
        found = None
        for record in self._journal():
            if record.kind == VOIDACTION_KIND and int(record.payload.get("target_seq", -1)) == target_seq:
                found = VoidActionRecord.from_record(record)
        return found

    def writeoff(self, target_seq: int, *, idem_key: str, issuer: str = "console", detail=None):
        """Write off one target, refusing if a void already won the race."""

        self._require_target(target_seq)
        existing = self._find(WRITEOFF_KIND, idem_key)
        if existing is not None:
            return {"record": WriteOffRecord.from_record(existing).describe(), "idempotent": True}
        if self._void_of(target_seq) is not None:
            raise OrderViolationError(
                "target is already voided; a write-off cannot undo a void",
                target_seq=target_seq,
            )
        record_obj = WriteOffRecord(
            action_id=self._ids.next("wroff"),
            target_seq=target_seq,
            idem_key=idem_key,
            tick=self._store.watermark().tick,
            issuer=issuer,
            detail=dict(detail or {}),
            state=ACTION_ACTIVE,
        )
        record = self._store.publish(WRITEOFF_KIND, self.origin, 0, record_obj.describe())
        return {"record": WriteOffRecord.from_record(record).describe(), "idempotent": False}

    def void(self, target_seq: int, *, idem_key: str, issuer: str = "console", reason: str = ""):
        """Void one target.  Idempotent on key; supersedes a prior write-off."""

        self._require_target(target_seq)
        existing = self._find(VOIDACTION_KIND, idem_key)
        if existing is not None:
            return {"record": VoidActionRecord.from_record(existing).describe(), "idempotent": True}
        record_obj = VoidActionRecord(
            action_id=self._ids.next("voida"),
            target_seq=target_seq,
            idem_key=idem_key,
            tick=self._store.watermark().tick,
            issuer=issuer,
            reason=reason,
        )
        record = self._store.publish(VOIDACTION_KIND, self.origin, 0, record_obj.describe())
        return {"record": VoidActionRecord.from_record(record).describe(), "idempotent": False}

    def _require_target(self, target_seq: int) -> None:
        if target_seq < 1:
            raise ValidationError("target sequence must be positive", target_seq=target_seq)
        if target_seq > self._store.watermark().committed_seq:
            raise UnknownRecordError(
                "action target was never committed", action="writeoff", seq=target_seq
            )

    def status(self) -> dict:
        writeoffs = [
            WriteOffRecord.from_record(record).describe()
            for record in self._journal()
            if record.kind == WRITEOFF_KIND
        ]
        voids = [
            VoidActionRecord.from_record(record).describe()
            for record in self._journal()
            if record.kind == VOIDACTION_KIND
        ]
        voided_targets = frozenset(item["target_seq"] for item in voids)
        active = [
            item for item in writeoffs if item["target_seq"] not in voided_targets and item["state"] == ACTION_ACTIVE
        ]
        return {
            "writeoffs": writeoffs,
            "voids": voids,
            "active_writeoffs": active,
            "superseded_writeoffs": [
                item for item in writeoffs if item["target_seq"] in voided_targets
            ],
        }
