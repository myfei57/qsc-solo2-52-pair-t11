"""The append only reconciliation events, read back from the record stream."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from ..errors import ValidationError
from ..store.records import Record
from .policy import MARK_KIND, PUBLISH_KIND, ROLLBACK_KIND, VOID_KIND

ACTION_RECONCILE = "reconcile"
ACTION_SETTLE = "settle"

MARK_ACTIONS = frozenset({ACTION_RECONCILE, ACTION_SETTLE})


@dataclass(frozen=True, slots=True)
class ReconEvent:
    """One surviving reconciliation control event in projection order."""

    seq: int
    kind: str
    tick: int
    origin: str
    target_seq: int
    action: str
    idem_key: str
    policy: str | None
    reason: str
    payload: Mapping[str, Any]

    @property
    def is_mark(self) -> bool:
        return self.kind == MARK_KIND

    @property
    def is_void(self) -> bool:
        return self.kind == VOID_KIND

    @property
    def is_rollback(self) -> bool:
        return self.kind == ROLLBACK_KIND

    @property
    def is_publish(self) -> bool:
        return self.kind == PUBLISH_KIND


def _field(payload: Mapping[str, Any], name: str) -> Any:
    if name not in payload:
        raise ValidationError("reconciliation event is missing a field", field=name)
    return payload[name]


def event_from_record(record: Record) -> ReconEvent | None:
    """Project one surviving control record, or ``None`` when it is not one."""

    if record.kind not in (MARK_KIND, VOID_KIND, ROLLBACK_KIND, PUBLISH_KIND):
        return None
    payload = record.payload
    action = str(payload.get("action", ACTION_RECONCILE if record.kind == MARK_KIND else "void"))
    policy = payload.get("policy")
    reason = str(payload.get("reason", ""))
    if record.kind == PUBLISH_KIND:
        target = int(payload.get("seq", record.seq))
        idem = str(payload.get("idem_key", f"publish:{target}"))
    else:
        target = int(_field(payload, "target_seq"))
        idem = str(payload.get("idem_key", f"{record.kind}:{target}"))
    return ReconEvent(
        seq=record.seq,
        kind=record.kind,
        tick=record.tick,
        origin=record.origin,
        target_seq=target,
        action=action,
        idem_key=idem,
        policy=None if policy is None else str(policy),
        reason=reason,
        payload=MappingProxyType(dict(payload)),
    )
