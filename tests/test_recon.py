"""Reconciliation watermark, write-off, void, rollback and conclusions."""

from __future__ import annotations

import concurrent.futures

from mixplant.recon import (
    MARK_KIND,
    POLICY_RECORD,
    POLICY_SEGMENT,
    RECONCILED,
    SETTLED,
    UNMARKED,
    VOIDED,
    build_view,
    is_accountable,
    policy_for_kind,
)
from mixplant.recon.cascade import explicit_void_targets, rollback_cascade
from mixplant.store.repository import RecordRepository
from mixplant.clock import ManualClock
from mixplant.ids import SequenceIds
from mixplant.runtime import PlantRuntime
from tests.support import batch_and_weigh, run

PUBLISH_KIND = "recon.publish"
ROLLBACK_KIND = "recon.rollback"
VOID_KIND = "recon.void"


def _store() -> RecordRepository:
    return RecordRepository.in_memory(ManualClock(), SequenceIds())


def _publish(store: RecordRepository, kind: str):
    return store.publish(kind, "agg", 0, {"active": True})


def _mark(store: RecordRepository, target, action: str = "reconcile") -> None:
    target_seq = target.seq if hasattr(target, "seq") else target
    store.publish(
        MARK_KIND,
        "recon",
        0,
        {"active": True, "action": action, "target_seq": target_seq,
         "idem_key": f"{action}:{target_seq}"},
    )


# --------------------------------------------------------------- pure policy

def test_policy_is_fixed_by_record_type() -> None:
    assert policy_for_kind("agg.weigh") == POLICY_RECORD
    assert policy_for_kind("agg.persist") == POLICY_RECORD
    assert policy_for_kind("agg.batch") == POLICY_SEGMENT
    assert policy_for_kind("audit.rollup") == POLICY_SEGMENT
    assert policy_for_kind("recipe.published") == POLICY_SEGMENT
    # every accountable kind has a definite, stable policy
    for kind in ("agg.weigh", "agg.drop", "mix.timer", "chute.door", "agg.batch",
                 "audit.rollup", "recipe.published", "version.baseline"):
        assert policy_for_kind(kind) in (POLICY_RECORD, POLICY_SEGMENT)


def test_control_records_are_not_accountable() -> None:
    store = _store()
    business = store.publish("agg.weigh", "agg", 0, {"active": True})
    mark = store.publish(MARK_KIND, "recon", 0, {"active": True, "target_seq": business.seq})
    tomb = store._append_tombstone(business.seq, "r", "recon", 0)
    assert is_accountable(business)
    assert not is_accountable(mark)
    assert not is_accountable(tomb)


# --------------------------------------------------------- watermark / reads

def test_only_reconciled_rows_are_externally_visible(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    assert runtime.external_ledger()["rows"] == []
    assert runtime.recon.watermark() == 0

    run(runtime, "recon.reconcile_through", {})
    external = runtime.external_ledger()
    assert external["rows"]
    assert runtime.recon.watermark() > 0
    # every external row is reconciled and carries a digest
    internal = runtime.internal_ledger()
    closed = [e for e in internal["entries"] if e["status"] in (RECONCILED, SETTLED)]
    assert len(external["rows"]) == len(closed)
    assert external["digest"]


def test_unmarked_gap_holds_the_reconciliation_watermark() -> None:
    store = _store()
    a = _publish(store, "agg.weigh")
    _publish(store, "agg.persist")
    _mark(store, a.seq)  # only the first row is reconciled
    view = build_view(store.committed_records())
    assert view.watermark_seq == a.seq
    assert [e.status for e in view.entries] == [RECONCILED, UNMARKED]
    assert len(view.external) == 1


# ------------------------------------------------------------- idempotency

def test_settle_and_reconcile_are_idempotent(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    seq = runtime.external_ledger()["rows"][0]["seq"]
    first = run(runtime, "recon.settle", {"seq": seq})
    second = run(runtime, "recon.settle", {"seq": seq})
    assert first["idempotent"] is False
    assert second["idempotent"] is True
    marks = [e for e in runtime.recon.view().events if e.is_mark and e.target_seq == seq]
    assert len(marks) == 2  # reconcile then settle; the duplicate settle added nothing


def test_concurrent_write_off_applies_once(runtime: PlantRuntime) -> None:
    seq = runtime.store.publish("agg.weigh", "agg", 0, {"active": True}).seq

    def fire(_: int) -> dict:
        return runtime.recon.settle(seq, idem_key="settle-once")

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        results = list(pool.map(fire, range(32)))
    applied = [r for r in results if not r["idempotent"]]
    assert len(applied) == 1
    marks = [e for e in runtime.recon.view().events if e.is_mark and e.target_seq == seq]
    assert len(marks) == 1


# -------------------------------------------------------------------- voids

def test_record_void_removes_only_the_affected_record() -> None:
    store = _store()
    a = _publish(store, "agg.weigh")
    b = _publish(store, "agg.persist")
    _mark(store, a.seq)
    _mark(store, b.seq)
    assert explicit_void_targets(store.committed_records(), a.seq) == ((a.seq, POLICY_RECORD),)


def test_segment_void_removes_the_whole_reconciled_segment() -> None:
    store = _store()
    head = _publish(store, "agg.weigh")
    seg = _publish(store, "agg.batch")
    after = _publish(store, "agg.persist")
    next_seg = _publish(store, "audit.rollup")
    for s in (head, seg, after, next_seg):
        _mark(store, s)
    targets = dict(explicit_void_targets(store.committed_records(), seg.seq))
    # the anchor and the reconciled rows up to the next segment, but not beyond
    assert targets[seg.seq] == POLICY_SEGMENT
    assert after.seq in targets
    assert next_seg.seq not in targets
    assert head.seq not in targets


def test_void_is_appended_never_a_delete(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    seq = runtime.external_ledger()["rows"][0]["seq"]
    run(runtime, "recon.void", {"seq": seq, "reason": "restated"})
    kinds = [r.kind for r in runtime.store.committed_records()]
    assert VOID_KIND in kinds
    entry = runtime.recon.view().status_of(seq)
    assert entry == VOIDED
    # the original record is still physically present
    assert any(r.seq == seq for r in runtime.store.committed_records())


def test_void_idempotent_under_concurrency(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    seq = runtime.external_ledger()["rows"][0]["seq"]

    def fire(_: int) -> dict:
        return runtime.recon.void(seq, reason="x", idem_key="void-once")

    with concurrent.futures.ThreadPoolExecutor(8) as pool:
        results = list(pool.map(fire, range(32)))
    applied = sum(1 for r in results if not r["idempotent"])
    assert applied == 1
    voids = [e for e in runtime.recon.view().events if e.is_void and e.target_seq == seq]
    assert len(voids) == 1


# ----------------------------------------------------------------- rollback

def test_rollback_cascades_record_vs_segment() -> None:
    store = _store()
    seg1 = _publish(store, "agg.batch")
    body1 = _publish(store, "agg.weigh")
    gap = _publish(store, "agg.persist")
    seg2 = _publish(store, "audit.rollup")
    body2 = _publish(store, "agg.drop")
    for s in (seg1, body1, gap, seg2, body2):
        _mark(store, s)

    # Point at seg2 removes body2 (its anchor seg2 survives): seg2 is
    # invalidated as a *segment*; body2 as a *record*.  seg1's body is intact.
    cut = dict((seq, policy) for seq, policy, _ in rollback_cascade(store.committed_records(), seg2.seq))
    assert cut.get(body2.seq) == POLICY_RECORD
    assert cut.get(seg2.seq) == POLICY_SEGMENT
    assert seg1.seq not in cut
    assert body1.seq not in cut

    # Point at body1 removes gap while seg1's anchor survives: seg1 is
    # invalidated as a *segment* (its body was cut).  seg2/body2 are removed
    # outright (their anchors no longer exist), not as segment casualties.
    rows = rollback_cascade(store.committed_records(), body1.seq)
    reasons = {seq: why for seq, _, why in rows}
    policies = dict((seq, pol) for seq, pol, _ in rows)
    assert policies.get(seg1.seq) == POLICY_SEGMENT
    assert reasons[seg1.seq].endswith("segment_cut")
    assert policies.get(gap.seq) == POLICY_RECORD
    assert reasons.get(seg2.seq, "").endswith("anchor_removed")
    assert policies.get(seg2.seq) == POLICY_SEGMENT


def test_rollback_is_idempotent(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    point = runtime.recon.watermark() - 3
    first = runtime.rollback(point, reason="x")
    second = runtime.rollback(point, reason="x")
    assert first["idempotent"] is False
    assert second["idempotent"] is True
    rollback_events = [e for e in runtime.recon.view().events if e.is_rollback]
    assert len(rollback_events) == 1


def test_rollback_preserves_reconciled_prefix_for_record_types(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    before = len(runtime.external_ledger()["rows"])
    point = runtime.recon.watermark() - 2
    runtime.rollback(point, reason="correction")
    after = len(runtime.external_ledger()["rows"])
    assert after < before
    assert runtime.recon.watermark() <= point


def test_rollback_keeps_the_recon_control_stream(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    point = runtime.recon.watermark() - 2
    runtime.rollback(point, reason="correction")
    groups = {r.group for r in runtime.store.committed_records() if r.kind in
              (MARK_KIND, VOID_KIND, ROLLBACK_KIND)}
    assert groups  # control events survive
    # the void appended for the cut rows is itself not tombstoned
    voids = [r for r in runtime.store.committed_records() if r.kind == VOID_KIND]
    assert voids
    assert all(not r.tombstone_of for r in voids)


# ------------------------------------------------------------- conclusions

def test_published_conclusion_is_immutable_after_rollback(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    published = run(runtime, "recon.publish", {})
    ref = published["conclusion"]["ref"]
    sealed_rows = published["conclusion"]["rows"]
    sealed_digest = published["conclusion"]["digest"]

    runtime.rollback(runtime.recon.watermark() - 3, reason="late correction")
    stored = runtime.recon.conclusion(ref)
    assert stored["rows"] == sealed_rows
    assert stored["digest"] == sealed_digest
    assert len(runtime.external_ledger()["rows"]) < sealed_rows


def test_conclusion_publication_is_idempotent_for_identical_state(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    first = run(runtime, "recon.publish", {})
    second = run(runtime, "recon.publish", {})
    assert first["idempotent"] is False
    assert second["idempotent"] is True


# ------------------------------------------------------------------ history

def test_history_returns_the_version_effective_then(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    published_seq = run(runtime, "recon.publish", {})["conclusion"]["seq"]
    sealed = len(runtime.external_ledger()["rows"])

    runtime.rollback(runtime.recon.watermark() - 3, reason="correction")
    now_rows = len(runtime.external_ledger()["rows"])
    assert now_rows < sealed

    then = runtime.recon_history({"seq": published_seq})
    assert len(then["rows"]) == sealed


# ----------------------------------------------------------------- verify

def test_external_internal_audit_consistency_verifies(runtime: PlantRuntime) -> None:
    batch_and_weigh(runtime)
    run(runtime, "recon.reconcile_through", {})
    run(runtime, "recon.publish", {})
    report = runtime.recon_verify()
    assert report["ok"] is True
    assert report["external_matches_internal"] is True
    assert report["events_match_audit"] is True
    assert report["conclusions_ok"] is True
    assert all(c["ok"] for c in report["conclusions"])


# ----------------------------------------------------------------- restart

def test_reconciliation_actions_are_mirrored_into_audit_and_restart(tmp_path) -> None:
    from mixplant.config import StationConfig

    config = StationConfig(data_dir=tmp_path / "state")
    first = PlantRuntime(config)
    first.bootstrap()
    batch_and_weigh(first)
    run(first, "recon.reconcile_through", {})
    run(first, "recon.publish", {})

    def mirrors(rt: PlantRuntime):
        return [
            r
            for r in rt.store.committed_records()
            if r.kind == "audit.event" and r.payload.get("origin") == "recon"
        ]

    assert {r.payload.get("name") for r in mirrors(first)} >= {
        "recon.reconcile",
        "recon.reconcile_through",
        "recon.publish",
    }
    count = len(mirrors(first))
    first.close()

    second = PlantRuntime(config)
    second.bootstrap()
    assert len(mirrors(second)) == count
    assert second.recon_verify()["ok"] is True
    second.close()


def test_reconciliation_rebuilds_after_restart(tmp_path) -> None:
    from mixplant.config import StationConfig

    config = StationConfig(data_dir=tmp_path / "state")
    first = PlantRuntime(config)
    first.bootstrap()
    batch_and_weigh(first)
    run(first, "recon.reconcile_through", {})
    published = run(first, "recon.publish", {})
    first.rollback(first.recon.watermark() - 2, reason="correction")
    watermark = first.recon.watermark()
    first.close()

    second = PlantRuntime(config)
    second.bootstrap()
    assert second.recon.watermark() == watermark
    assert second.recon_verify()["ok"] is True
    stored = second.recon.conclusion(published["conclusion"]["ref"])
    assert stored["digest"] == published["conclusion"]["digest"]
    second.close()
