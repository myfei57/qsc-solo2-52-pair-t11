"""Checkpoint and void value objects and their journal encoding.

A checkpoint says "every committed record with ``seq <= through_seq`` was
reconciled at ``tick`` and the views agreed on ``digest``".  A void says "the
checkpoint named by ``checkpoint_id`` no longer counts as of ``tick`` because a
rollback reached its range".  Both live in the append only stream, so the
history of what *was* the truth at a past tick survives every void.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from ..errors import ValidationError
from ..store.records import Record

CHECKPOINT_KIND = "reconcile.checkpoint"
VOID_KIND = "reconcile.void"

STATUS_VALID = "valid"
STATUS_VOIDED = "voided"


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """One advance of the reconciliation watermark."""

    checkpoint_id: str
    through_seq: int
    tick: int
    issuer: str
    digest: str
    policy_version: str

    def __post_init__(self) -> None:
        if not self.checkpoint_id:
            raise ValidationError("a checkpoint needs an id")
        if self.through_seq < 1:
            raise ValidationError("a checkpoint must cover at least one record", through_seq=self.through_seq)
        if self.tick < 0:
            raise ValidationError("checkpoint tick must not be negative", tick=self.tick)
        if not self.issuer:
            raise ValidationError("a checkpoint needs an issuer")
        if not self.digest:
            raise ValidationError("a checkpoint needs a view digest")

    def describe(self, status: str = STATUS_VALID, voided_tick: int | None = None) -> dict[str, Any]:
        document: dict[str, Any] = {
            "checkpoint_id": self.checkpoint_id,
            "through_seq": self.through_seq,
            "tick": self.tick,
            "issuer": self.issuer,
            "digest": self.digest,
            "policy_version": self.policy_version,
            "status": status,
        }
        if voided_tick is not None:
            document["voided_tick"] = voided_tick
        return document


@dataclass(frozen=True, slots=True)
class CheckpointStatus:
    """A checkpoint together with the void that may have retired it."""

    checkpoint: Checkpoint
    void: "Void | None" = None

    @property
    def valid(self) -> bool:
        return self.void is None

    def describe(self) -> dict[str, Any]:
        return self.checkpoint.describe(
            status=STATUS_VALID if self.valid else STATUS_VOIDED,
            voided_tick=None if self.void is None else self.void.tick,
        )


@dataclass(frozen=True, slots=True)
class Void:
    """The append only retirement of one checkpoint after a rollback."""

    void_id: str
    checkpoint_id: str
    rollback_to_seq: int
    tick: int
    issuer: str
    scope_by_kind: Mapping[str, str]
    affected_seqs: tuple[int, ...]
    reason: str
    policy_version: str

    def __post_init__(self) -> None:
        if not self.void_id:
            raise ValidationError("a void needs an id")
        if not self.checkpoint_id:
            raise ValidationError("a void must name the checkpoint it retires")
        if self.rollback_to_seq < 0:
            raise ValidationError("rollback point must not be negative", rollback_to_seq=self.rollback_to_seq)
        if self.tick < 0:
            raise ValidationError("void tick must not be negative", tick=self.tick)
        if not self.issuer:
            raise ValidationError("a void needs an issuer")

    def describe(self) -> dict[str, Any]:
        return {
            "void_id": self.void_id,
            "checkpoint_id": self.checkpoint_id,
            "rollback_to_seq": self.rollback_to_seq,
            "tick": self.tick,
            "issuer": self.issuer,
            "scope_by_kind": dict(self.scope_by_kind),
            "affected_seqs": list(self.affected_seqs),
            "reason": self.reason,
            "policy_version": self.policy_version,
        }


def checkpoint_payload(checkpoint: Checkpoint) -> dict[str, Any]:
    return {"active": True, **checkpoint.describe()}


def checkpoint_from_record(record: Record) -> Checkpoint:
    payload = record.payload
    return Checkpoint(
        checkpoint_id=str(payload["checkpoint_id"]),
        through_seq=int(payload["through_seq"]),
        tick=int(payload.get("tick", record.tick)),
        issuer=str(payload.get("issuer", record.origin)),
        digest=str(payload["digest"]),
        policy_version=str(payload.get("policy_version", "1")),
    )


def void_payload(void: Void) -> dict[str, Any]:
    return {"active": True, **void.describe()}


def void_from_record(record: Record) -> Void:
    payload = record.payload
    raw_scope = payload.get("scope_by_kind") or {}
    raw_seqs = payload.get("affected_seqs") or []
    return Void(
        void_id=str(payload["void_id"]),
        checkpoint_id=str(payload["checkpoint_id"]),
        rollback_to_seq=int(payload["rollback_to_seq"]),
        tick=int(payload.get("tick", record.tick)),
        issuer=str(payload.get("issuer", record.origin)),
        scope_by_kind=dict(raw_scope),
        affected_seqs=tuple(int(seq) for seq in raw_seqs),
        reason=str(payload.get("reason", "rollback")),
        policy_version=str(payload.get("policy_version", "1")),
    )
