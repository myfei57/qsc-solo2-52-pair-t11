"""Application service for reconciliation, write-off, void and rollback.

Concurrency: every mutating method takes one reentrant lock around the whole
"read current ledger -> decide -> append" section.  Idempotency is enforced
both by a client supplied ``idem_key`` (duplicate concurrent triggers return
the first result) and by the semantic identity of the action (target + action
+ rollback point), so retries never double apply.

Nothing here rewrites history.  A void appends ``recon.void``; a rollback
tombstones the business records and appends ``recon.void`` plus
``recon.rollback``; a publication seals a self contained conclusion.
"""

from __future__ import annotations

import threading
from typing import Any, Mapping

from ..errors import RecordError, ValidationError
from ..event.bus import Event
from ..event.topics import topic_for
from ..service_base import PlantService
from ..store.records import Record
from ..store.tombstone import effective_records, hidden_sequences
from .cascade import explicit_void_targets, rollback_cascade
from .events import ACTION_RECONCILE, ACTION_SETTLE
from .ledger import CLOSED, RECONCILED, SETTLED, VOIDED, ReconView, build_view
from .policy import MARK_KIND, PUBLISH_KIND, ROLLBACK_KIND, VOID_KIND, is_accountable
from .publication import PublishedConclusion, row_document, rows_digest

ORIGIN = "recon"
RECON_GROUP = "recon"


class ReconService(PlantService):
    """Owns the reconciliation ledger and its mutating commands."""

    origin = ORIGIN
    line = "batch"
    subject = ""

    def __init__(self, context, machine=None) -> None:
        super().__init__(context, machine)
        self._lock = threading.RLock()

    # ------------------------------------------------------------------ reads

    def _committed(self) -> tuple[Record, ...]:
        return self.store.committed_records()

    def view(self, *, as_of_seq: int | None = None, as_of_tick: int | None = None) -> ReconView:
        return build_view(self._committed(), as_of_seq=as_of_seq, as_of_tick=as_of_tick)

    def watermark(self) -> int:
        return self.view().watermark_seq

    def status(self) -> dict[str, Any]:
        view = self.view()
        return {
            "watermark_seq": view.watermark_seq,
            "counts": view.counts(),
            "external_rows": len(view.external),
            "internal_rows": len(view.internal),
            "conclusions": len(view.conclusions),
        }

    def external(self, *, as_of_seq: int | None = None, as_of_tick: int | None = None) -> dict[str, Any]:
        """The externally readable ledger: reconciled rows, as of a version."""

        view = self.view(as_of_seq=as_of_seq, as_of_tick=as_of_tick)
        return {
            "watermark_seq": view.watermark_seq,
            "as_of_seq": view.cutoff_seq,
            "as_of_tick": view.cutoff_tick,
            "rows": [row_document(record) for record in view.external],
            "digest": rows_digest(view.external),
        }

    def internal(self) -> dict[str, Any]:
        """The full internal ledger, voids and all, with reconciliation standing."""

        view = self.view()
        return {
            "watermark_seq": view.watermark_seq,
            "entries": [entry.describe() for entry in view.entries],
            "rows": [row_document(record) for record in view.internal],
        }

    def history(self, *, tick: int | None = None, seq: int | None = None) -> dict[str, Any]:
        """Return the result that was effective at the requested version."""

        if tick is None and seq is None:
            raise ValidationError("history needs a tick or a seq")
        view = self.view(as_of_tick=tick, as_of_seq=seq)
        return {
            "as_of_tick": tick,
            "as_of_seq": view.cutoff_seq,
            "watermark_seq": view.watermark_seq,
            "rows": [row_document(record) for record in view.external],
            "digest": rows_digest(view.external),
        }

    def conclusion(self, ref: str) -> dict[str, Any]:
        """Read one sealed conclusion back; sealed conclusions never change."""

        for conclusion in reversed(self.view().conclusions):
            if conclusion.conclusion_id == ref or conclusion.ref == ref or conclusion.digest == ref:
                return conclusion.describe()
        from ..errors import NotFoundError

        raise NotFoundError("published conclusion does not exist", ref=ref)

    def verify(self) -> dict[str, Any]:
        """Cross check external view, internal ledger and the audit stream."""

        view = self.view()
        external_digest = rows_digest(view.external)
        internal_closed = tuple(
            view.records_by_seq[entry.seq] for entry in view.entries if entry.status in CLOSED
        )
        # 1. external set is exactly the closed internal rows, in order.
        external_matches_internal = tuple(view.external) == tuple(internal_closed)
        # 2. audit = the append only journal: every control event must still
        #    be on the stream (a void is appended, never deleted), and every
        #    void has to reference a mark that came before it.
        committed = self._committed()
        hidden = hidden_sequences(committed)
        control_seqs = tuple(event.seq for event in view.events)
        marks = {event.target_seq: event.seq for event in view.events if event.is_mark}
        voids = tuple(event for event in view.events if event.is_void)
        live_targets = set(view.records_by_seq)
        # Every void either reverses an earlier mark, or explicitly kills a
        # still-live ledger row; it can never dangle on a deleted record.
        voids_grounded = all(
            (
                (event.target_seq in marks and marks[event.target_seq] < event.seq)
                or event.target_seq in live_targets
            )
            for event in voids
        )
        events_match_audit = bool(not (set(control_seqs) & hidden) and voids_grounded)
        audited = len(control_seqs)
        events = view.events
        # 3. every sealed conclusion re-digests to its stored digest.
        conclusions = []
        conclusions_ok = True
        for conclusion in view.conclusions:
            ok = rows_digest(conclusion.rows) == conclusion.digest
            conclusions_ok = conclusions_ok and ok
            conclusions.append({"ref": conclusion.ref, "digest": conclusion.digest, "ok": ok})
        return {
            "ok": bool(external_matches_internal and events_match_audit and conclusions_ok),
            "watermark_seq": view.watermark_seq,
            "external_digest": external_digest,
            "external_rows": len(view.external),
            "external_matches_internal": bool(external_matches_internal),
            "recon_events": len(events),
            "recon_events_audited": audited,
            "events_match_audit": bool(events_match_audit),
            "conclusions_ok": bool(conclusions_ok),
            "conclusions": conclusions,
        }

    # --------------------------------------------------------------- mutation

    def _accountable(self, seq: int, view: ReconView) -> Record:
        record = view.records_by_seq.get(seq)
        if record is None:
            raise RecordError("reconciliation target is not a live ledger record", target_seq=seq)
        if not is_accountable(record):
            raise ValidationError("record type cannot be reconciled", kind=record.kind, seq=seq)
        return record

    def mark(
        self,
        seq: int,
        *,
        action: str = ACTION_RECONCILE,
        issuer: str = ORIGIN,
        idem_key: str | None = None,
        note: str = "",
    ) -> dict[str, Any]:
        """Reconcile (``reconcile``) or write off (``settle``) one record."""

        if action not in (ACTION_RECONCILE, ACTION_SETTLE):
            raise ValidationError("unknown reconciliation action", action=action)
        key = idem_key or f"{action}:{seq}"
        with self._lock:
            existing = self._find_event(MARK_KIND, key)
            if existing is not None:
                return self._idempotent(existing)
            view = self.view()
            self._accountable(seq, view)
            status = view.status_of(seq)
            if status == VOIDED:
                raise RecordError("a voided record cannot be reconciled again", target_seq=seq)
            if status == SETTLED or (status == RECONCILED and action == ACTION_RECONCILE):
                return self._already(seq, action, status)
            payload = {
                "active": True,
                "action": action,
                "target_seq": seq,
                "idem_key": key,
                "issuer": issuer,
                "note": note,
            }
            return self._commit(self.store.publish(MARK_KIND, ORIGIN, 0, payload), name=f"recon.{action}")

    def reconcile(self, seq: int, *, idem_key: str | None = None, note: str = "") -> dict[str, Any]:
        return self.mark(seq, action=ACTION_RECONCILE, idem_key=idem_key, note=note)

    def settle(self, seq: int, *, idem_key: str | None = None, note: str = "") -> dict[str, Any]:
        return self.mark(seq, action=ACTION_SETTLE, idem_key=idem_key, note=note)

    def reconcile_through(
        self,
        seq: int,
        *,
        action: str = ACTION_RECONCILE,
        idem_key: str | None = None,
    ) -> dict[str, Any]:
        """Close every open ledger row up to and including ``seq``."""

        key = idem_key or f"{action}_through:{seq}"
        applied: list[dict[str, Any]] = []
        with self._lock:
            if self._find_event(MARK_KIND, key) is not None:
                return {"idem_key": key, "idempotent": True, "applied": applied}
            view = self.view()
            for entry in view.entries:
                if entry.seq > seq:
                    break
                if entry.status in CLOSED:
                    continue
                applied.append(
                    self.mark(entry.seq, action=action, idem_key=f"{key}:{entry.seq}")
                )
            # Record the range operation itself for idempotent retries.
            record = self.store.publish(
                MARK_KIND,
                ORIGIN,
                0,
                {
                    "active": True,
                    "action": action,
                    "target_seq": seq,
                    "idem_key": key,
                    "range": True,
                },
            )
            self._emit(f"recon.{action}_through", {"target_seq": seq, "count": len(applied)})
            return {"idem_key": key, "idempotent": False, "applied": applied, "seq": record.seq}

    def void(
        self,
        seq: int,
        *,
        reason: str,
        idem_key: str | None = None,
        issuer: str = ORIGIN,
    ) -> dict[str, Any]:
        """Explicitly void one record; spread is fixed by the record type."""

        key = idem_key or f"void:{seq}"
        with self._lock:
            existing = self._find_event(VOID_KIND, key)
            if existing is not None:
                return self._idempotent(existing)
            view = self.view()
            anchor = view.records_by_seq.get(seq)
            if anchor is None or not is_accountable(anchor):
                raise RecordError("void target is not a live ledger record", target_seq=seq)
            surviving = effective_records(self._committed())
            targets = explicit_void_targets(surviving, seq)
            return self._apply_void(targets, reason=reason, cause="void", idem_prefix=key)

    def rollback(self, point_seq: int, *, reason: str = "operator rollback") -> dict[str, Any]:
        """Roll the business ledger back, cascading voids by record type."""

        key = f"rollback:{point_seq}"
        with self._lock:
            existing = self._find_event(ROLLBACK_KIND, key)
            if existing is not None:
                return self._idempotent(existing)
            committed = self._committed()
            if point_seq < 0 or point_seq > self.store.last_seq():
                raise ValidationError("rollback point does not exist", point=point_seq)
            cascaded = rollback_cascade(committed, point_seq)
            tombstones = self.store.rollback_after(
                point_seq,
                reason=reason,
                origin=ORIGIN,
                generation=0,
                exclude_groups=(RECON_GROUP,),
            )
            target_rows = tuple((seq, policy) for seq, policy, _ in cascaded)
            self._apply_void(
                target_rows,
                reason=reason,
                cause=f"rollback:{point_seq}",
                idem_prefix=f"rollback:{point_seq}",
            )
            record = self.store.publish(
                ROLLBACK_KIND,
                ORIGIN,
                0,
                {
                    "active": True,
                    "target_seq": point_seq,
                    "idem_key": key,
                    "reason": reason,
                    "tombstones": [tomb.seq for tomb in tombstones],
                    "voided": [seq for seq, _, _ in cascaded],
                },
            )
            self._emit("recon.rollback", {"point_seq": point_seq, "voided": len(cascaded)})
            return {
                "point_seq": point_seq,
                "seq": record.seq,
                "idempotent": False,
                "tombstones": [tomb.seq for tomb in tombstones],
                "voided": [
                    {"target_seq": seq, "policy": policy, "reason": why}
                    for seq, policy, why in cascaded
                ],
                "watermark_seq": self.view().watermark_seq,
            }

    def publish_conclusion(self, *, issuer: str = ORIGIN, note: str = "") -> dict[str, Any]:
        """Seal the currently visible rows into an immutable conclusion."""

        with self._lock:
            view = self.view()
            rows = view.external
            digest = rows_digest(rows)
            for conclusion in view.conclusions:
                if conclusion.digest == digest and conclusion.watermark_seq == view.watermark_seq:
                    return {"idempotent": True, "conclusion": conclusion.describe()}
            conclusion_id = self.context.ids.next("concl")
            record = self.store.publish(
                PUBLISH_KIND,
                ORIGIN,
                0,
                {
                    "active": True,
                    "target_seq": view.watermark_seq,
                    "idem_key": f"publish:{conclusion_id}",
                    "conclusion_id": conclusion_id,
                    "watermark_seq": view.watermark_seq,
                    "issuer": issuer,
                    "note": note,
                    "digest": digest,
                    "rows": [row_document(row) for row in rows],
                },
            )
            self._emit(
                "recon.publish",
                {"conclusion_id": conclusion_id, "watermark_seq": view.watermark_seq, "digest": digest},
            )
            conclusion = PublishedConclusion(
                conclusion_id=conclusion_id,
                seq=record.seq,
                tick=record.tick,
                watermark_seq=view.watermark_seq,
                issuer=issuer,
                rows=rows,
                digest=digest,
            )
            return {"idempotent": False, "conclusion": conclusion.describe()}

    # ------------------------------------------------------------- internals

    def _apply_void(self, targets: tuple[tuple[int, str], ...], *, reason: str, cause: str, idem_prefix: str) -> dict[str, Any]:
        if not targets:
            return {"idempotent": True, "voided": []}
        view = self.view()
        current = view.status_map()
        applied: list[dict[str, Any]] = []
        for index, (target_seq, policy) in enumerate(targets):
            key = f"{idem_prefix}:{target_seq}"
            if self._find_event(VOID_KIND, key) is not None:
                continue
            status = current.get(target_seq)
            if status == VOIDED:
                continue
            record = self.store.publish(
                VOID_KIND,
                ORIGIN,
                0,
                {
                    "active": True,
                    "action": "void",
                    "target_seq": target_seq,
                    "idem_key": key,
                    "policy": policy,
                    "reason": reason,
                    "cause": cause,
                    "index": index,
                },
            )
            applied.append({"target_seq": target_seq, "policy": policy, "seq": record.seq})
        if applied:
            self._emit(
                "recon.void",
                {"cause": cause, "targets": [target_seq for target_seq, _ in targets]},
            )
        return {
            "idempotent": not applied,
            "voided": applied,
            "watermark_seq": self.view().watermark_seq,
        }

    def _find_event(self, kind: str, idem_key: str):
        from .events import event_from_record

        for record in self._committed():
            if record.is_tombstone or record.kind != kind:
                continue
            if str(record.payload.get("idem_key")) == idem_key:
                event = event_from_record(record)
                return {"seq": record.seq, "event": event}
        return None

    def _commit(self, record: Record, *, name: str) -> dict[str, Any]:
        payload = record.payload
        self._emit(name, {"target_seq": payload["target_seq"], "action": payload.get("action")})
        return {
            "idempotent": False,
            "seq": record.seq,
            "target_seq": int(payload["target_seq"]),
            "watermark_seq": self.view().watermark_seq,
        }

    def _idempotent(self, found: Mapping[str, Any]) -> dict[str, Any]:
        event = found["event"]
        return {
            "idempotent": True,
            "seq": found["seq"],
            "target_seq": event.target_seq if event is not None else None,
            "watermark_seq": self.view().watermark_seq,
        }

    def _already(self, seq: int, action: str, status: str) -> dict[str, Any]:
        return {
            "idempotent": True,
            "already": True,
            "target_seq": seq,
            "action": action,
            "status": status,
            "watermark_seq": self.view().watermark_seq,
        }

    def _emit(self, name: str, payload: Mapping[str, Any]) -> None:
        self.context.bus.publish(
            Event(
                topic=topic_for(ORIGIN),
                name=name,
                origin=ORIGIN,
                tick=self.context.clock.now(),
                payload=dict(payload),
            )
        )
