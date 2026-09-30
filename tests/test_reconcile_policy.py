"""The void scope policy is frozen per record kind."""

from __future__ import annotations

import pytest

from mixplant.errors import ValidationError
from mixplant.reconcile.policy import (
    VOID_AFFECTED,
    VOID_SEGMENT,
    VoidPolicy,
    policy_for,
    policy_table,
)


def test_segment_kinds_are_pinned() -> None:
    assert policy_for("agg.weigh") == VOID_SEGMENT
    assert policy_for("agg.persist") == VOID_SEGMENT
    assert policy_for("agg.check") == VOID_SEGMENT


def test_affected_kinds_are_pinned() -> None:
    assert policy_for("audit.entry") == VOID_AFFECTED
    assert policy_for("chute.confirm") == VOID_AFFECTED
    assert policy_for("version.confirmation") == VOID_AFFECTED


def test_an_unregistered_kind_is_refused_not_guessed() -> None:
    with pytest.raises(ValidationError):
        policy_for("not.a_kind")
    with pytest.raises(ValidationError):
        VoidPolicy().scope_for("mystery.record")


def test_the_table_is_exposed_for_audit_readback() -> None:
    policy = VoidPolicy(version="42")
    assert policy.version == "42"
    table = policy_table(policy)
    assert table["agg.weigh"] == VOID_SEGMENT
    assert table["audit.entry"] == VOID_AFFECTED
    described = policy.describe()
    assert described["version"] == "42"
    assert described["rules"]["agg.weigh"] == VOID_SEGMENT


def test_policy_version_must_not_be_empty() -> None:
    with pytest.raises(ValidationError):
        VoidPolicy(version="")
