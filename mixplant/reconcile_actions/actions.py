"""Record shapes for write-off and void actions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..errors import ValidationError
from ..store.records import Record

WRITEOFF_KIND = "action.writeoff"
VOIDACTION_KIND = "action.void"

ACTION_ACTIVE = "active"
ACTION_VOIDED = "voided"


@dataclass(frozen=True, slots=True)
class WriteOffRecord:
    """A write-off against one target sequence."""

    action_id: str
    target_seq: int
    idem_key: str
    tick: int
    issuer: str
    detail: Mapping[str, Any]
    state: str = ACTION_ACTIVE

    def __post_init__(self) -> None:
        if not self.action_id:
            raise ValidationError("a write-off needs an id")
        if self.target_seq < 1:
            raise ValidationError("a write-off needs a target sequence", target_seq=self.target_seq)
        if not self.idem_key:
            raise ValidationError("a write-off needs an idempotency key")
        if self.state not in (ACTION_ACTIVE, ACTION_VOIDED):
            raise ValidationError("unknown write-off state", state=self.state)

    @property
    def voided(self) -> bool:
        return self.state == ACTION_VOIDED

    def describe(self) -> dict[str, Any]:
        return {
            "active": not self.voided,
            "action_id": self.action_id,
            "target_seq": self.target_seq,
            "idem_key": self.idem_key,
            "tick": self.tick,
            "issuer": self.issuer,
            "state": self.state,
            "detail": dict(self.detail),
        }

    @classmethod
    def from_record(cls, record: Record) -> "WriteOffRecord":
        payload = record.payload
        state = ACTION_VOIDED if payload.get("active") is False else str(payload.get("state", ACTION_ACTIVE))
        return cls(
            action_id=str(payload["action_id"]),
            target_seq=int(payload["target_seq"]),
            idem_key=str(payload["idem_key"]),
            tick=int(payload.get("tick", record.tick)),
            issuer=str(payload.get("issuer", record.origin)),
            detail=dict(payload.get("detail") or {}),
            state=state,
        )


@dataclass(frozen=True, slots=True)
class VoidActionRecord:
    """A void of one target sequence (append only)."""

    action_id: str
    target_seq: int
    idem_key: str
    tick: int
    issuer: str
    reason: str

    def __post_init__(self) -> None:
        if not self.action_id:
            raise ValidationError("a void needs an id")
        if self.target_seq < 1:
            raise ValidationError("a void needs a target sequence", target_seq=self.target_seq)
        if not self.idem_key:
            raise ValidationError("a void needs an idempotency key")

    def describe(self) -> dict[str, Any]:
        return {
            "active": True,
            "action_id": self.action_id,
            "target_seq": self.target_seq,
            "idem_key": self.idem_key,
            "tick": self.tick,
            "issuer": self.issuer,
            "reason": self.reason,
        }

    @classmethod
    def from_record(cls, record: Record) -> "VoidActionRecord":
        payload = record.payload
        return cls(
            action_id=str(payload["action_id"]),
            target_seq=int(payload["target_seq"]),
            idem_key=str(payload["idem_key"]),
            tick=int(payload.get("tick", record.tick)),
            issuer=str(payload.get("issuer", record.origin)),
            reason=str(payload.get("reason", "")),
        )
