"""Canonical digests and immutable published conclusions.

A published conclusion seals the rows that were effective *at a known sequence
and tick*.  Its digest is computed from a canonical encoding of exactly those
rows, which is the value the console and the audit stream read back and cross
check.  Publications are append only; republishing never rewrites one.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any, Mapping

from ..store.records import Record


def canonical_digest(payload: Any) -> str:
    """Return the stable sha256 digest of a json encodable payload."""

    encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def row_document(record: Record) -> dict[str, Any]:
    """Return the canonical shape of one externally visible row."""

    return {
        "seq": record.seq,
        "kind": record.kind,
        "origin": record.origin,
        "generation": record.generation,
        "tick": record.tick,
        "payload": dict(record.payload),
    }


def rows_digest(records: tuple[Record, ...]) -> str:
    """Digest an ordered set of rows, independent of later appends."""

    return canonical_digest([row_document(record) for record in records])


@dataclass(frozen=True, slots=True)
class PublishedConclusion:
    """A sealed, externally readable snapshot of the reconciled ledger."""

    conclusion_id: str
    seq: int
    tick: int
    watermark_seq: int
    issuer: str
    rows: tuple[Record, ...]
    digest: str
    payload: Mapping[str, Any] = field(default_factory=lambda: MappingProxyType({}))

    @property
    def ref(self) -> str:
        return self.digest[:12]

    def describe(self) -> dict[str, Any]:
        return {
            "conclusion_id": self.conclusion_id,
            "seq": self.seq,
            "tick": self.tick,
            "watermark_seq": self.watermark_seq,
            "issuer": self.issuer,
            "rows": len(self.rows),
            "digest": self.digest,
            "ref": self.ref,
            "payload": dict(self.payload),
        }
