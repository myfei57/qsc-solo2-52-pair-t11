"""The single status document the console renders."""

from __future__ import annotations

from typing import Any, Mapping


def compose_plant_status(
    *,
    station: str,
    now: int,
    watermark: Mapping[str, Any],
    subsystems: Mapping[str, Any],
    lines: Mapping[str, Any],
    versioned: Mapping[str, Any],
    thresholds: Mapping[str, Any],
    interlocks: Mapping[str, Any],
    batches: list[dict[str, Any]],
    alarms: list[dict[str, Any]],
    facts: Mapping[str, bool],
    config: Mapping[str, Any],
    reconcile_watermark: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Fold every subsystem view into the document the console serves."""

    return {
        "station": station,
        "tick": now,
        "watermark": dict(watermark),
        "reconcile_watermark": dict(reconcile_watermark or {}),
        "subsystems": dict(subsystems),
        "lines": dict(lines),
        "versioned": dict(versioned),
        "thresholds": dict(thresholds),
        "interlocks": dict(interlocks),
        "batches": list(batches),
        "alarms": list(alarms),
        "facts": dict(facts),
        "config": dict(config),
    }
