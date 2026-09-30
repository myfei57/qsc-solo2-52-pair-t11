"""The read side: project the surviving stream onto the reconciliation ledger.

Everything here is a pure fold over records, so a restart and a live run reach
the same answer.  The fold only ever reads events; a void or a rollback adds
events, it never removes one, which is what keeps history immutable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from ..store.records import Record
from .events import ACTION_SETTLE, ReconEvent, event_from_record
from .policy import is_accountable, policy_for_kind
from .publication import PublishedConclusion, rows_digest

UNMARKED = "unmarked"
RECONCILED = "reconciled"
SETTLED = "settled"
VOIDED = "voided"

CLOSED = frozenset({RECONCILED, SETTLED})
LIVE = frozenset({UNMARKED, RECONCILED, SETTLED})


@dataclass(frozen=True, slots=True)
class EntryStatus:
    """The reconciliation standing of one surviving accountable record."""

    seq: int
    kind: str
    tick: int
    status: str
    policy: str
    marked_seq: int | None
    voided_seq: int | None
    reasons: tuple[str, ...]

    @property
    def externally_visible(self) -> bool:
        return self.status in CLOSED

    def describe(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "kind": self.kind,
            "tick": self.tick,
            "status": self.status,
            "policy": self.policy,
            "marked_seq": self.marked_seq,
            "voided_seq": self.voided_seq,
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True, slots=True)
class ReconView:
    """Everything a reader can ask the reconciliation ledger at one moment."""

    cutoff_seq: int
    cutoff_tick: int | None
    watermark_seq: int
    entries: tuple[EntryStatus, ...]
    external: tuple[Record, ...]
    internal: tuple[Record, ...]
    events: tuple[ReconEvent, ...]
    conclusions: tuple[PublishedConclusion, ...]
    records_by_seq: Mapping[int, Record]

    def status_map(self) -> dict[int, EntryStatus]:
        return {entry.seq: entry for entry in self.entries}

    def external_rows(self) -> tuple[Record, ...]:
        return self.external

    def internal_rows(self) -> tuple[Record, ...]:
        return self.internal

    def status_of(self, seq: int) -> str | None:
        entry = self.status_map().get(seq)
        return None if entry is None else entry.status

    def counts(self) -> dict[str, int]:
        counts = {UNMARKED: 0, RECONCILED: 0, SETTLED: 0, VOIDED: 0}
        for entry in self.entries:
            counts[entry.status] += 1
        return counts

    def describe(self) -> dict[str, Any]:
        return {
            "watermark_seq": self.watermark_seq,
            "cutoff_seq": self.cutoff_seq,
            "cutoff_tick": self.cutoff_tick,
            "counts": self.counts(),
            "external_rows": len(self.external),
            "internal_rows": len(self.internal),
            "entries": [entry.describe() for entry in self.entries],
            "conclusions": [conclusion.describe() for conclusion in self.conclusions],
        }


def _hidden_at(window: list[Record]) -> frozenset[int]:
    """Return the sequences hidden within an already version filtered window."""

    return frozenset(rec.tombstone_of for rec in window if rec.tombstone_of is not None)


def _conclusion(event: ReconEvent) -> PublishedConclusion:
    payload = event.payload
    rows = tuple(Record.from_document(doc) for doc in payload.get("rows", ()))
    return PublishedConclusion(
        conclusion_id=str(payload.get("conclusion_id", event.idem_key)),
        seq=event.seq,
        tick=event.tick,
        watermark_seq=int(payload.get("watermark_seq", 0)),
        issuer=event.origin,
        rows=rows,
        digest=str(payload.get("digest", rows_digest(rows))),
        payload=payload,
    )


def build_view(
    records: Iterable[Record],
    *,
    as_of_seq: int | None = None,
    as_of_tick: int | None = None,
) -> ReconView:
    """Fold the stream into a reconciliation view.

    When an ``as_of`` version is given, tombstones appended *after* that
    version are ignored too, so a later rollback never rewrites the answer the
    ledger gave at the time.  Reconciliation control records are never
    tombstoned (they survive by construction), but the window filter applies
    to them the same way.
    """

    all_records = list(records)
    if as_of_tick is not None:
        # Tick is the wall clock version.
        window = [rec for rec in all_records if rec.tick <= as_of_tick]
    elif as_of_seq is not None:
        # Strict stream prefix: exactly the journal that existed at that seq,
        # so a later tombstone, void or mark cannot rewrite a past answer.
        window = [rec for rec in all_records if rec.seq <= as_of_seq]
    else:
        window = list(all_records)
    hidden = _hidden_at(window)
    surviving = [rec for rec in window if not rec.is_tombstone and rec.seq not in hidden]
    window_by_seq = {rec.seq: rec for rec in surviving}
    cutoff_seq = max((rec.seq for rec in window), default=0)

    events = tuple(event for rec in window if (event := event_from_record(rec)) is not None)
    accountable = [rec for rec in window if is_accountable(rec)]

    marked_at: dict[int, int] = {}
    marked_action: dict[int, str] = {}
    voided_at: dict[int, int] = {}
    reasons: dict[int, list[str]] = {}

    conclusions = [_conclusion(event) for event in events if event.is_publish]

    for event in events:
        if event.is_publish or event.is_rollback:
            continue
        target = event.target_seq
        if target not in window_by_seq or not is_accountable(window_by_seq[target]):
            continue
        if event.is_mark:
            marked_at[target] = event.seq
            marked_action[target] = event.action
            voided_at.pop(target, None)
            reasons.pop(target, None)
        elif event.is_void:
            voided_at[target] = event.seq
            reasons.setdefault(target, []).append(f"{event.kind}@{event.seq}:{event.reason}".rstrip(":"))

    entries: list[EntryStatus] = []
    by_seq: dict[int, Record] = {}
    for record in accountable:
        by_seq[record.seq] = record
        marked_seq = marked_at.get(record.seq)
        voided_seq = voided_at.get(record.seq)
        if voided_seq is not None and (marked_seq is None or voided_seq > marked_seq):
            status = VOIDED
        elif marked_seq is None:
            status = UNMARKED
        elif marked_action.get(record.seq) == ACTION_SETTLE:
            status = SETTLED
        else:
            status = RECONCILED
        entries.append(
            EntryStatus(
                seq=record.seq,
                kind=record.kind,
                tick=record.tick,
                status=status,
                policy=policy_for_kind(record.kind),
                marked_seq=marked_seq,
                voided_seq=voided_seq,
                reasons=tuple(reasons.get(record.seq, ())),
            )
        )

    entries.sort(key=lambda entry: entry.seq)

    # Watermark = longest fully closed (reconciled/settled), live prefix.
    watermark_seq = 0
    for entry in entries:
        if entry.status in CLOSED:
            watermark_seq = entry.seq
        else:
            break

    external = tuple(by_seq[entry.seq] for entry in entries if entry.status in CLOSED)
    internal = tuple(by_seq[entry.seq] for entry in entries if entry.status in LIVE)

    return ReconView(
        cutoff_seq=cutoff_seq,
        cutoff_tick=as_of_tick,
        watermark_seq=watermark_seq,
        entries=tuple(entries),
        external=external,
        internal=internal,
        events=events,
        conclusions=tuple(conclusions),
        records_by_seq=by_seq,
    )
