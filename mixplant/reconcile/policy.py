"""The void scope policy, frozen per record type.

A rollback never decides "whole segment or just the affected records" on the
spot; that decision is fixed per record *kind* at build time.  The mapping is
deliberately a module level constant: changing it requires shipping a new
version of the station software, which is exactly the regulator-facing
guarantee the policy has to make.  Any kind not registered is rejected rather
than guessed at, so a new record type cannot silently pick a loose default.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from ..errors import ValidationError

VOID_SEGMENT = "segment"
"""Every reconciled record of the kind inside the affected range is voided."""

VOID_AFFECTED = "affected"
"""Only records the rollback actually neutralised are voided; the rest stand."""

# Batch composition kinds: every downstream total rests on them, so one
# neutralised reading reopens the run and the segment rechecks as a unit.
_SEGMENT_KINDS = frozenset(
    {
        "agg.weigh",
        "agg.persist",
        "agg.report",
        "agg.check",
        "agg.batch",
        "agg.bin",
        "agg.hopper",
        "agg.calibrate",
        "moist.comp",
        "moist.mode",
        "mix.cycle",
        "mix.feed",
        "mix.mixer",
        "mix.report",
        "mix.schedule",
        "mix.stop",
        "mix.timer",
        "cement.flow",
        "cement.gate",
        "cement.silo",
        "water.dispense",
        "water.prime",
        "water.tank",
        "water.valve",
        "pump.pressure",
        "pump.run",
        "pump.start",
        "pump.stroke",
        "sampler.draw",
        "sampler.latch",
        "sampler.batchset",
    }
)

# Independent entries, confirmations, ledgers and infrastructure records stand
# on their own; a rollback only retires the specific ones it touched.
_AFFECTED_KINDS = frozenset(
    {
        "agg.release",
        "agg.drop",
        "audit.entry",
        "audit.event",
        "audit.rollup",
        "chute.confirm",
        "chute.door",
        "chute.load",
        "chute.manifest",
        "chute.open",
        "chute.queue",
        "sampler.record",
        "version.confirmation",
        "version.baseline",
        "recipe.published",
        "recipe.selected",
        "recipe.version",
        "station.boot",
        "store.clock",
        "store.tombstone",
    }
)

_KIND_TO_POLICY: Mapping[str, str] = MappingProxyType(
    {kind: VOID_SEGMENT for kind in sorted(_SEGMENT_KINDS)}
    | {kind: VOID_AFFECTED for kind in sorted(_AFFECTED_KINDS)}
)


class VoidPolicy:
    """Answers the void scope question for one record kind.

    The table itself is fixed; a policy instance only pins which *version* of
    that table an operation used, so a journal line forever records whether
    "segment" or "affected" was the rule at the time.
    """

    __slots__ = ("_version",)

    def __init__(self, version: str = "1") -> None:
        if not version:
            raise ValidationError("void policy version must not be empty")
        self._version = str(version)

    @property
    def version(self) -> str:
        return self._version

    def scope_for(self, kind: str) -> str:
        """Return the frozen scope for ``kind`` or refuse an unregistered kind."""

        try:
            return _KIND_TO_POLICY[kind]
        except KeyError as exc:
            raise ValidationError(
                "no void policy is registered for this record kind",
                kind=kind,
                policy_version=self._version,
            ) from exc

    def is_segment(self, kind: str) -> bool:
        return self.scope_for(kind) == VOID_SEGMENT

    def describe(self) -> dict[str, object]:
        return {"version": self._version, "rules": {kind: scope for kind, scope in _KIND_TO_POLICY.items()}}


_DEFAULT = VoidPolicy()


def policy_for(kind: str, *, policy: VoidPolicy | None = None) -> str:
    """Convenience lookup against the frozen table."""

    return (policy or _DEFAULT).scope_for(kind)


def policy_table(policy: VoidPolicy | None = None) -> Mapping[str, str]:
    """Expose the frozen table for audit read-back."""

    chosen = policy or _DEFAULT
    return MappingProxyType({kind: chosen.scope_for(kind) for kind in _KIND_TO_POLICY})
