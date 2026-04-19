"""In-memory ticket ledger for demo / tests. Replace with DB in production."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from threading import Lock
from typing import Any


@dataclass
class TicketRecord:
    ticket_id: str
    site_id: str
    ts: datetime
    net_volume_bbl: float
    manifest_ref: str
    extra: dict[str, Any] = field(default_factory=dict)


class TicketLedger:
    def __init__(self) -> None:
        self._lock = Lock()
        self._by_id: dict[str, TicketRecord] = {}
        self._ordered: list[TicketRecord] = []

    def upsert(self, rec: TicketRecord) -> None:
        with self._lock:
            self._by_id[rec.ticket_id] = rec
            self._ordered = sorted(self._by_id.values(), key=lambda r: r.ts)

    def get(self, ticket_id: str) -> TicketRecord | None:
        with self._lock:
            return self._by_id.get(ticket_id)

    def all_sorted(self) -> list[TicketRecord]:
        with self._lock:
            return list(self._ordered)

    def clear(self) -> None:
        with self._lock:
            self._by_id.clear()
            self._ordered.clear()


ledger = TicketLedger()
