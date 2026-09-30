"""Reconciliation watermark, write-off, void and immutable conclusions.

The regulatory rule implemented here is:

* a record only counts for the outside world once it was reconciled;
* reconciliation advances a watermark over the surviving ledger;
* a rollback after the watermark invalidates every reconciliation that came
  after the rollback point, by appending void events (never by deleting);
* whether a void removes a whole segment or only the affected records is
  fixed by record type (see :mod:`mixplant.recon.policy`);
* write-off, void and rollback are idempotent and concurrency safe;
* published conclusions are immutable history.
"""

from __future__ import annotations

from .ledger import RECONCILED, SETTLED, UNMARKED, VOIDED, ReconView, build_view
from .policy import (
    MARK_KIND,
    POLICY_RECORD,
    POLICY_SEGMENT,
    PUBLISH_KIND,
    RECON_KINDS,
    ROLLBACK_KIND,
    VOID_KIND,
    is_accountable,
    policy_for_kind,
)
from .publication import canonical_digest, rows_digest
from .service import ReconService

__all__ = [
    "MARK_KIND",
    "VOID_KIND",
    "ROLLBACK_KIND",
    "PUBLISH_KIND",
    "RECON_KINDS",
    "RECONCILED",
    "SETTLED",
    "UNMARKED",
    "VOIDED",
    "POLICY_RECORD",
    "POLICY_SEGMENT",
    "ReconService",
    "ReconView",
    "build_view",
    "canonical_digest",
    "is_accountable",
    "policy_for_kind",
    "rows_digest",
]
