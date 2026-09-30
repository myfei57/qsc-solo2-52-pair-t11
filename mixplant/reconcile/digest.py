"""Deterministic anchor digests for the three read faces.

The regulator has to be able to verify that the external query, the internal
ledger and the audit log say the same thing at the same point.  Rather than
compare three systems after the fact, every checkpoint records one digest of
the canonical, watermark bounded record slice.  Each reader projects that same
slice its own way; the digest is computed over the slice, not the projection,
so "the views agree" is a statement about one shared anchor.

The digest is intentionally sha256 over a canonical json encoding with no
randomness, so two stations (or a restart) produce the identical string for the
identical slice.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable, Sequence

from ..store.records import Record


def canonical_document(value: Any) -> str:
    """Encode ``value`` the same way on every machine."""

    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def record_anchor(record: Record) -> dict[str, Any]:
    """Return the identity fields a digest is taken over."""

    return {
        "seq": record.seq,
        "kind": record.kind,
        "origin": record.origin,
        "generation": record.generation,
        "tick": record.tick,
        "payload": dict(record.payload),
        "tombstone_of": record.tombstone_of,
    }


def digest_slice(records: Iterable[Record], *, through_seq: int) -> str:
    """Digest every record with ``seq <= through_seq`` in write order.

    Tombstones are included deliberately: the anchor covers the stream as it
    stood, including the rollback markers, so a reader can prove it replayed the
    exact same history.
    """

    anchors = [record_anchor(record) for record in records if record.seq <= through_seq]
    body = canonical_document({"through_seq": through_seq, "records": anchors})
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def digest_value(value: Any) -> str:
    """Digest an arbitrary read projection (a view, a ledger, an audit list)."""

    return hashlib.sha256(canonical_document(value).encode("utf-8")).hexdigest()


def anchor_projection(*, external: Any, ledger: Any, audit: Any) -> dict[str, str]:
    """Return the three projection digests a reconciliation compares."""

    return {
        "external": digest_value(external),
        "ledger": digest_value(ledger),
        "audit": digest_value(audit),
    }


def projections_agree(digests: dict[str, str]) -> bool:
    """True when every face reports the same projection digest."""

    values: Sequence[str] = tuple(digests.values())
    return bool(values) and len(set(values)) == 1
