"""Ticket matching: duplicates, sequence gaps, manifest consistency."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class MatchResult:
    status: str
    reasons: list[str]
    details: dict[str, Any]


def _parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def detect_duplicate_ticket_ids(df: pd.DataFrame) -> pd.DataFrame:
    dup = df.duplicated(subset=["ticket_id"], keep=False)
    return df.loc[dup].sort_values(["ticket_id", "timestamp"])


def inter_arrival_gaps(
    df: pd.DataFrame,
    site_col: str = "site_id",
    time_col: str = "timestamp",
    gap_hours: float = 36.0,
) -> pd.DataFrame:
    """Flags large gaps between consecutive tickets at the same site."""

    d = df.copy()
    d[time_col] = pd.to_datetime(d[time_col], utc=True, errors="coerce")
    d = d.dropna(subset=[time_col]).sort_values([site_col, time_col])
    d["prev_ts"] = d.groupby(site_col)[time_col].shift(1)
    d["gap_hours"] = (d[time_col] - d["prev_ts"]).dt.total_seconds() / 3600.0
    big = d["gap_hours"] >= gap_hours
    return d.loc[big & d["prev_ts"].notna()]


def manifest_mismatch_rate(df: pd.DataFrame, spread_ratio_threshold: float = 0.18) -> pd.DataFrame:
    """Flags manifests where ticket-level volumes diverge materially for the same document."""

    vol = pd.to_numeric(df["net_volume_bbl"], errors="coerce")
    tmp = df.assign(_vol=vol)
    g = (
        tmp.groupby("manifest_ref", dropna=False)
        .agg(
            vol_min=("_vol", "min"),
            vol_max=("_vol", "max"),
            vol_mean=("_vol", "mean"),
            count=("ticket_id", "count"),
        )
        .reset_index()
    )
    denom = g["vol_mean"].replace(0, np.nan)
    g["spread_ratio"] = (g["vol_max"] - g["vol_min"]) / denom
    g["suspicious_manifest"] = (g["count"] > 1) & (g["spread_ratio"] >= spread_ratio_threshold)
    return g.loc[g["suspicious_manifest"]]


def match_incoming_ticket(
    *,
    ticket_id: str,
    site_id: str,
    timestamp: str | datetime,
    net_volume_bbl: float,
    manifest_ref: str,
    history: pd.DataFrame,
    gap_hours: float = 36.0,
) -> MatchResult:
    """Real-time style check against in-memory or provided history."""

    reasons: list[str] = []
    ts = _parse_ts(timestamp)

    if not history.empty and "ticket_id" in history.columns:
        if ticket_id in set(history["ticket_id"].astype(str)):
            reasons.append("DUPLICATE_TICKET_ID")

    if not history.empty and {"site_id", "timestamp"}.issubset(history.columns):
        site_hist = history.loc[history["site_id"].astype(str) == str(site_id)].copy()
        if not site_hist.empty:
            site_hist["timestamp"] = pd.to_datetime(site_hist["timestamp"], utc=True, errors="coerce")
            last = site_hist["timestamp"].max()
            if pd.notna(last) and ts > last:
                gap = (ts - last.to_pydatetime()).total_seconds() / 3600.0
                if gap >= gap_hours:
                    reasons.append("SEQUENCE_GAP_LARGE")

    if not history.empty and "manifest_ref" in history.columns:
        same_man = history.loc[history["manifest_ref"].astype(str) == str(manifest_ref)]
        if len(same_man) > 0 and "net_volume_bbl" in same_man.columns:
            prev = pd.to_numeric(same_man["net_volume_bbl"], errors="coerce")
            if prev.notna().any():
                ref = float(prev.iloc[-1])
                if abs(float(net_volume_bbl) - ref) > max(0.5, 0.05 * abs(ref)):
                    reasons.append("MANIFEST_VOLUME_DRIFT")

    status = "CLEAR" if not reasons else "REVIEW"
    return MatchResult(status=status, reasons=reasons, details={"evaluated_at": ts.isoformat()})
