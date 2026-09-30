"""Reads and writes the reconciliation boundary on top of the record stream.

The ledger never mutates history:

* ``reconcile`` appends a ``reconcile.checkpoint`` and advances the boundary;
* ``void_after`` (called by a rollback) appends one ``reconcile.void`` per
  checkpoint whose range the rollback reached, scoping the retired records with
  the frozen per-kind policy;
* reads are bounded by the *currently valid* boundary, while ``as_of`` reports
  exactly the boundary that was in force at the tick asked about.

Concurrency is modelled the same way the rest of the store models it: every
append is serialised by the repository's single append order, and every
idempotent action carries a caller supplied key.  Repeating a reconcile or a
void with the same key returns the record already produced instead of writing a
second one.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ..errors import ValidationError
from ..ids import SequenceIds
from ..store.records import Record
from ..store.repository import RecordRepository
from .checkpoint import (
    CHECKPOINT_KIND,
    VOID_KIND,
    Checkpoint,
    CheckpointStatus,
    Void,
    checkpoint_from_record,
    checkpoint_payload,
    void_from_record,
    void_payload,
)
from .digest import digest_slice
from .policy import VOID_SEGMENT, VoidPolicy
from .watermark import BoundaryError, ReconcileWatermark

IDEMPOTENCY_FIELD = "idem_key"

_METADATA_KINDS = frozenset({CHECKPOINT_KIND, VOID_KIND, "action.writeoff", "action.void"})


@dataclass(frozen=True, slots=True)
class ReconcileResult:
    """What one reconcile call produced, idempotent repeats included."""

    checkpoint: Checkpoint
    record: Record
    idempotent: bool
    watermark: ReconcileWatermark

    def describe(self) -> dict[str, Any]:
        return {
            "checkpoint": self.checkpoint.describe(),
            "seq": self.record.seq,
            "idempotent": self.idempotent,
            "watermark": self.watermark.describe(),
        }


class ReconciliationLedger:
    """Owns the checkpoint/void view of one :class:`RecordRepository`."""

    origin = "reconcile"

    __slots__ = ("_store", "_ids", "_policy")

    def __init__(
        self,
        store: RecordRepository,
        ids: SequenceIds,
        *,
        policy: VoidPolicy | None = None,
    ) -> None:
        self._store = store
        self._ids = ids
        self._policy = policy or VoidPolicy()

    @property
    def policy(self) -> VoidPolicy:
        return self._policy

    # ----- history reconstructed purely from the append only stream -----

    def _journal(self) -> tuple[Record, ...]:
        return self._store.committed_records()

    def checkpoints(self) -> tuple[CheckpointStatus, ...]:
        """Return every checkpoint with the void (if any) that retired it."""

        statuses: dict[str, CheckpointStatus] = {}
        order: list[str] = []
        for record in self._journal():
            if record.kind == CHECKPOINT_KIND:
                checkpoint = checkpoint_from_record(record)
                statuses[checkpoint.checkpoint_id] = CheckpointStatus(checkpoint)
                order.append(checkpoint.checkpoint_id)
            elif record.kind == VOID_KIND:
                void = void_from_record(record)
                if void.checkpoint_id not in statuses:
                    raise ValidationError(
                        "a void names a checkpoint the stream does not contain",
                        checkpoint_id=void.checkpoint_id,
                        void_seq=record.seq,
                    )
                previous = statuses[void.checkpoint_id]
                # Voids are append-only and idempotent: a repeat void for the
                # same checkpoint keeps the first retirement on record.
                if previous.void is None:
                    statuses[void.checkpoint_id] = CheckpointStatus(previous.checkpoint, void)
        return tuple(statuses[name] for name in order)

    def voids(self) -> tuple[Void, ...]:
        return tuple(void_from_record(record) for record in self._journal() if record.kind == VOID_KIND)

    def watermark(self) -> ReconcileWatermark:
        from .watermark import watermark_from

        return watermark_from(list(self.checkpoints()), now=self._store.watermark().tick)

    def checkpoint_at(self, tick: int) -> ReconcileWatermark:
        """Return the boundary that was in force as of ``tick``.

        Only checkpoints and voids with ``record.tick <= tick`` count, so a
        later rollback never rewrites the answer an as-of query gives.
        """

        if tick < 0:
            raise ValidationError("as-of tick must not be negative", tick=tick)
        statuses: list[CheckpointStatus] = []
        for record in self._journal():
            if record.tick > tick:
                break
            if record.kind == CHECKPOINT_KIND:
                statuses.append(CheckpointStatus(checkpoint_from_record(record)))
            elif record.kind == VOID_KIND:
                void = void_from_record(record)
                statuses = [
                    entry if entry.checkpoint.checkpoint_id != void.checkpoint_id else CheckpointStatus(entry.checkpoint, void)
                    for entry in statuses
                ]
        from .watermark import watermark_from

        return watermark_from(statuses, now=tick)

    # ----- writes -----

    def _find_idempotent(self, kind: str, key: str) -> Record | None:
        for record in self._journal():
            if record.kind == kind and record.payload.get(IDEMPOTENCY_FIELD) == key:
                return record
        return None

    def _checkpoint_record(self, checkpoint_id: str) -> Record:
        for record in self._journal():
            if record.kind == CHECKPOINT_KIND and record.payload.get("checkpoint_id") == checkpoint_id:
                return record
        raise ValidationError("checkpoint record missing from the stream", checkpoint_id=checkpoint_id)

    # The reconciliation boundary vouches for the domain slice.  Its own
    # checkpoint/void records are metadata appended *about* that slice; they
    # sit in the journal (so the history of the boundary is itself durable and
    # auditable) but they do not extend the range the next checkpoint has to
    # cover.  ``_domain_tip`` is therefore the highest non-metadata sequence.
    def _domain_tip(self) -> int:
        tip = 0
        for record in self._journal():
            if record.kind not in _METADATA_KINDS:
                tip = max(tip, record.seq)
        return tip

    def _valid_checkpoint_covering(self, tip: int) -> Checkpoint | None:
        for entry in reversed(self.checkpoints()):
            if entry.valid and entry.checkpoint.through_seq >= tip:
                return entry.checkpoint
        return None

    def reconcile(
        self,
        *,
        issuer: str = "console",
        idem_key: str | None = None,
    ) -> ReconcileResult:
        """Checkpoint everything committed up to now and move the boundary.

        Committed but unreconciled records are invisible externally; after this
        returns they are vouched for.  Repeating the call with the same
        ``idem_key`` yields the original checkpoint instead of a new one.
        """

        if idem_key is not None:
            existing = self._find_idempotent(CHECKPOINT_KIND, idem_key)
            if existing is not None:
                checkpoint = checkpoint_from_record(existing)
                return ReconcileResult(
                    checkpoint=checkpoint,
                    record=existing,
                    idempotent=True,
                    watermark=self.watermark(),
                )

        tip = self._domain_tip()
        current = self.watermark()
        if tip == 0:
            raise BoundaryError("there are no committed records to reconcile", committed=0)
        # A valid checkpoint already at the domain tip is a no-op only when its
        # digest still matches the slice.  After a rollback the same tip names
        # a different slice (tombstones were appended), so a fresh checkpoint
        # with a new digest is required instead.
        covering = self._valid_checkpoint_covering(tip)
        if covering is not None and covering.digest == digest_slice(
            self._store.committed_records(), through_seq=tip
        ):
            return ReconcileResult(
                checkpoint=covering,
                record=self._checkpoint_record(covering.checkpoint_id),
                idempotent=True,
                watermark=current,
            )

        through = tip
        digest = digest_slice(self._store.committed_records(), through_seq=through)
        checkpoint = Checkpoint(
            checkpoint_id=self._ids.next("rcpt"),
            through_seq=through,
            tick=self._store.watermark().tick,
            issuer=issuer,
            digest=digest,
            policy_version=self._policy.version,
        )
        payload = checkpoint_payload(checkpoint)
        if idem_key is not None:
            payload[IDEMPOTENCY_FIELD] = idem_key
        record = self._store.publish(CHECKPOINT_KIND, self.origin, 0, payload)
        result = ReconcileResult(
            checkpoint=checkpoint, record=record, idempotent=False, watermark=self.watermark()
        )
        return result

    def void_after(
        self,
        rollback_to_seq: int,
        *,
        tombstoned_seqs: Sequence[int],
        affected_kinds: Mapping[str, str],
        issuer: str = "console",
        reason: str = "rollback",
        idem_key: str | None = None,
    ) -> tuple[Void, ...]:
        """Void every checkpoint whose reconciled range the rollback reached.

        ``tombstoned_seqs`` are the records the rollback actually neutralised;
        ``affected_kinds`` maps each record kind touched to its frozen scope.
        ``segment`` kinds void the whole reconciled segment for that kind,
        ``affected`` kinds only the neutralised seqs.  One void record per
        retired checkpoint is appended; surviving checkpoints are untouched.
        """

        if rollback_to_seq < 0:
            raise ValidationError("rollback point must not be negative", rollback_to_seq=rollback_to_seq)

        if idem_key is not None:
            already = tuple(
                void_from_record(record)
                for record in self._journal()
                if record.kind == VOID_KIND and record.payload.get(IDEMPOTENCY_FIELD) == idem_key
            )
            if already:
                return already

        tombstoned = frozenset(tombstoned_seqs)
        produced: list[Void] = []
        # Checkpoints strictly after the rollback point are all retired; a
        # checkpoint ending exactly at the point still stands.
        for entry in self.checkpoints():
            if not entry.valid:
                continue
            checkpoint = entry.checkpoint
            if checkpoint.through_seq <= rollback_to_seq:
                continue
            segment_kinds = frozenset(
                kind for kind, scope in affected_kinds.items() if scope == VOID_SEGMENT
            )
            affected: list[int] = []
            for record in self._store.committed_records():
                if record.seq <= rollback_to_seq or record.seq > checkpoint.through_seq:
                    continue
                if record.kind in segment_kinds:
                    affected.append(record.seq)
                elif record.seq in tombstoned:
                    affected.append(record.seq)
            scope_by_kind = {
                kind: self._policy.scope_for(kind) for kind in sorted(set(affected_kinds))
            }
            void = Void(
                void_id=self._ids.next("void"),
                checkpoint_id=checkpoint.checkpoint_id,
                rollback_to_seq=rollback_to_seq,
                tick=self._store.watermark().tick,
                issuer=issuer,
                scope_by_kind=scope_by_kind,
                affected_seqs=tuple(affected),
                reason=reason,
                policy_version=self._policy.version,
            )
            payload = void_payload(void)
            if idem_key is not None:
                payload[IDEMPOTENCY_FIELD] = idem_key
            self._store.publish(VOID_KIND, self.origin, 0, payload)
            produced.append(void)
        return tuple(produced)

    # ----- reads bounded by the boundary -----

    def readable_records(self) -> tuple[Record, ...]:
        """Return exactly the records an external reader may see now.

        Committed, non-tombstoned, and inside the currently valid boundary.
        """

        boundary = self.watermark()
        from ..store.tombstone import effective_records

        bounded = tuple(
            record
            for record in self._store.committed_records()
            if record.seq <= boundary.through_seq
        )
        return effective_records(bounded)

    def readable_state(self) -> dict[str, Any]:
        from ..store.replay import reduce_state

        return reduce_state(self.readable_records())

    def state_as_of(self, tick: int) -> dict[str, Any]:
        """The effective state at ``tick`` under the boundary in force then."""

        from ..store.replay import reduce_state
        from ..store.tombstone import effective_records

        boundary = self.checkpoint_at(tick)
        within = tuple(
            record
            for record in self._store.committed_records()
            if record.tick <= tick and record.seq <= boundary.through_seq
        )
        return reduce_state(effective_records(within))

    # ----- console / audit description -----

    def describe(self) -> dict[str, Any]:
        watermark = self.watermark()
        checkpoints = self.checkpoints()
        pending = self._pending_domain_records(watermark.through_seq)
        return {
            "watermark": watermark.describe(),
            "policy": self._policy.describe(),
            "checkpoints": [entry.describe() for entry in checkpoints],
            "voids": [void.describe() for void in self.voids()],
            "committed_seq": self._store.watermark().committed_seq,
            "domain_tip": self._domain_tip(),
            "pending_seq": pending,
        }

    def _pending_domain_records(self, through_seq: int) -> int:
        """Count committed domain records past the valid boundary."""

        return sum(
            1
            for record in self._journal()
            if record.kind not in _METADATA_KINDS and record.seq > through_seq
        )
