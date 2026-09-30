"""End to end reconciliation behaviour through the runtime and console."""

from __future__ import annotations

import json

import pytest

from mixplant.config import StationConfig
from mixplant.console.pages import PageStore
from mixplant.console.router import Request
from mixplant.console.server import ConsoleServer
from mixplant.runtime import PlantRuntime
from tests.support import publish_and_select, run


def _server(runtime: PlantRuntime) -> ConsoleServer:
    return ConsoleServer(runtime, pages=PageStore(runtime.config.web_dir))


def test_external_query_only_shows_reconciled_records(runtime: PlantRuntime) -> None:
    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "visible"})
    assert runtime.external_query()["matched"] == 0
    run(runtime, "reconcile.take", {})
    names = [
        row["payload"].get("name")
        for row in runtime.external_query()["rows"]
        if row["kind"] == "audit.entry"
    ]
    assert "visible" in names

    # a later, unreconciled event stays hidden
    run(runtime, "audit.note", {"name": "pending"})
    names_after = [
        row["payload"].get("name")
        for row in runtime.external_query()["rows"]
        if row["kind"] == "audit.entry"
    ]
    assert "pending" not in names_after


def test_rollback_retreats_the_boundary_and_keeps_history(runtime: PlantRuntime) -> None:
    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "first"})
    first = run(runtime, "reconcile.take", {"idem_key": "c1"})
    first_through = first["watermark"]["through_seq"]
    run(runtime, "audit.note", {"name": "second"})
    run(runtime, "reconcile.take", {"idem_key": "c2"})

    outcome = runtime.rollback(first_through, reason="correction", idem_key="rb")
    statuses = [entry["status"] for entry in runtime.reconcile_status()["checkpoints"]]
    assert statuses == ["valid", "voided"]
    assert runtime.reconciliation.watermark().through_seq == first_through
    assert outcome["voids"]
    # void and checkpoint records are append-only facts
    kinds = [record.kind for record in runtime.store.committed_records()]
    assert kinds.count("reconcile.checkpoint") == 2
    assert kinds.count("reconcile.void") == 1


def test_repeat_rollback_is_idempotent(runtime: PlantRuntime) -> None:
    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "first"})
    through = run(runtime, "reconcile.take", {})["watermark"]["through_seq"]
    run(runtime, "audit.note", {"name": "second"})
    run(runtime, "reconcile.take", {})
    first = runtime.rollback(through, reason="rb", idem_key="rb")
    second = runtime.rollback(through, reason="rb", idem_key="rb")
    assert first["voids"]
    assert second["voids"] == []


def test_as_of_history_returns_the_then_valid_version(runtime: PlantRuntime) -> None:
    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "first"})
    checkpoint = run(runtime, "reconcile.take", {})
    valid_tick = checkpoint["checkpoint"]["tick"]
    run(runtime, "clock.advance", {"ticks": 3})
    run(runtime, "audit.note", {"name": "second"})
    run(runtime, "reconcile.take", {})
    run(runtime, "clock.advance", {"ticks": 3})
    runtime.rollback(checkpoint["watermark"]["through_seq"], reason="rb")
    as_of = runtime.history_as_of(valid_tick)
    assert as_of["reconcile_watermark"]["through_seq"] == checkpoint["watermark"]["through_seq"]


def test_consistency_anchor_matches_the_stream_and_its_faces(runtime: PlantRuntime) -> None:
    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "anchored"})
    run(runtime, "reconcile.take", {})
    anchor = runtime.consistency_anchor()
    assert anchor["anchor_matches_stream"] is True
    assert anchor["state_faces_identical"] is True
    assert anchor["anchor_consistent_across_faces"] is True
    checkpoint_ids = {face["anchor"]["checkpoint_id"] for face in anchor["faces"].values()}
    assert len(checkpoint_ids) == 1


def test_watermark_is_on_health_state_and_console_routes(runtime: PlantRuntime) -> None:
    server = _server(runtime)
    health = json.loads(server.handle(Request(method="GET", path="/api/health")).text)
    state = json.loads(server.handle(Request(method="GET", path="/api/state")).text)
    assert "reconcile_watermark" in health
    assert "reconcile_watermark" in state

    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "x"})
    post = server.handle(Request(method="POST", path="/api/reconcile/take", body={"idem_key": "http"}))
    assert post.status == 200
    post_again = server.handle(
        Request(method="POST", path="/api/reconcile/take", body={"idem_key": "http"})
    )
    assert json.loads(post_again.text)["result"]["idempotent"] is True

    reconcile_view = json.loads(server.handle(Request(method="GET", path="/api/reconcile")).text)
    assert reconcile_view["policy"]["rules"]["agg.weigh"] == "segment"
    anchor_view = json.loads(server.handle(Request(method="GET", path="/api/reconcile/anchor")).text)
    assert anchor_view["anchor_matches_stream"] is True
    external = json.loads(server.handle(Request(method="GET", path="/api/external")).text)
    assert external["reconcile_watermark"]["checkpoint_id"] == reconcile_view["watermark"]["checkpoint_id"]


def test_writeoff_and_void_commands(runtime: PlantRuntime) -> None:
    publish_and_select(runtime)
    run(runtime, "audit.note", {"name": "target"})
    run(runtime, "reconcile.take", {})
    target = [
        record for record in runtime.store.committed_records() if record.kind == "audit.entry"
    ][-1].seq
    run(runtime, "action.writeoff", {"target_seq": target, "idem_key": "w"})
    run(runtime, "action.writeoff", {"target_seq": target, "idem_key": "w"})
    run(runtime, "action.void", {"target_seq": target, "idem_key": "v", "reason": "fraud"})
    status = runtime.actions.status()
    assert not status["active_writeoffs"]
    assert status["superseded_writeoffs"]


def test_reconciled_boundary_survives_restart(tmp_path) -> None:
    config = StationConfig(data_dir=tmp_path / "state")
    first = PlantRuntime(config)
    first.bootstrap()
    publish_and_select(first)
    run(first, "audit.note", {"name": "first"})
    first_through = run(first, "reconcile.take", {"idem_key": "c1"})["watermark"]["through_seq"]
    run(first, "audit.note", {"name": "second"})
    run(first, "reconcile.take", {"idem_key": "c2"})
    first.rollback(first_through, reason="rb", idem_key="rb")
    first.close()

    second = PlantRuntime(config)
    second.bootstrap()
    assert second.reconciliation.watermark().through_seq == first_through
    assert [entry.valid for entry in second.reconciliation.checkpoints()] == [True, False]
    # new checkpoint ids never reuse a retired id
    restarted = run(second, "reconcile.take", {})
    assert restarted["checkpoint"]["checkpoint_id"] == "rcpt-000003"
    second.close()
