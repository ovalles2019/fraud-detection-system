"""REST API for fraud analytics and ticket matching."""

from __future__ import annotations

import hashlib
import io
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from flask import Blueprint, jsonify, request

from app.data_store import TicketRecord, ledger
from app.state import store
from core.outliers import add_zscore_outliers, summarize_outlier_flags
from core.reconciliation import expected_daily_counts
from core.tickets import (
    detect_duplicate_ticket_ids,
    inter_arrival_gaps,
    manifest_mismatch_rate,
    match_incoming_ticket,
)

bp = Blueprint("api", __name__, url_prefix="")

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "sample_tickets.csv"


def _df_to_records(df: pd.DataFrame, limit: int | None = 500) -> list[dict]:
    if df.empty:
        return []
    view = df.head(limit) if limit is not None else df
    tmp = view.copy()
    records = []
    for row in tmp.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if pd.isna(v):
                clean[k] = None
            elif hasattr(v, "isoformat") and not isinstance(v, bool):
                try:
                    clean[k] = v.isoformat()
                except (AttributeError, ValueError):
                    clean[k] = str(v)
            else:
                clean[k] = v
        records.append(clean)
    return records


def _stable_id(*parts: str) -> str:
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()[:16]


def _run_pipeline(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"summary": {"rows": 0}, "findings": []}

    need_cols = {"ticket_id", "site_id", "timestamp", "net_volume_bbl", "ticket_amount_usd", "manifest_ref"}
    missing = need_cols - set(df.columns)
    if missing:
        raise ValueError(f"Dataset missing columns: {sorted(missing)}")

    flagged = add_zscore_outliers(
        df,
        group_col="site_id",
        value_cols=["net_volume_bbl", "ticket_amount_usd"],
        z_threshold=3.0,
    )
    flagged = summarize_outlier_flags(flagged, ["net_volume_bbl", "ticket_amount_usd"])

    dupes = detect_duplicate_ticket_ids(flagged)
    gaps = inter_arrival_gaps(flagged, gap_hours=36.0)
    daily = expected_daily_counts(flagged)
    missing_days = daily.loc[daily["missing_activity_flag"].fillna(False)]
    manifest = manifest_mismatch_rate(flagged)

    findings: list[dict] = []

    outliers = flagged.loc[flagged["any_outlier"]]
    for _, r in outliers.iterrows():
        reasons = []
        if bool(r.get("net_volume_bbl_outlier")):
            reasons.append("VOLUME_Z_OUTLIER")
        if bool(r.get("ticket_amount_usd_outlier")):
            reasons.append("AMOUNT_Z_OUTLIER")
        findings.append(
            {
                "id": _stable_id("OUT", str(r["ticket_id"])),
                "type": "STATISTICAL_OUTLIER",
                "severity": "MEDIUM",
                "site_id": str(r["site_id"]),
                "ticket_id": str(r["ticket_id"]),
                "reasons": reasons,
                "evidence": {
                    "net_volume_bbl": float(r["net_volume_bbl"]),
                    "net_volume_bbl_z": float(r.get("net_volume_bbl_z", 0.0)),
                    "ticket_amount_usd": float(r["ticket_amount_usd"]),
                    "ticket_amount_usd_z": float(r.get("ticket_amount_usd_z", 0.0)),
                },
            }
        )

    for _, r in dupes.iterrows():
        findings.append(
            {
                "id": _stable_id("DUP", str(r["ticket_id"]), str(r["timestamp"])),
                "type": "DUPLICATE_TICKET",
                "severity": "HIGH",
                "site_id": str(r["site_id"]),
                "ticket_id": str(r["ticket_id"]),
                "reasons": ["DUPLICATE_TICKET_ID"],
                "evidence": {"timestamp": str(r["timestamp"])},
            }
        )

    for _, r in gaps.iterrows():
        findings.append(
            {
                "id": _stable_id("GAP", str(r["site_id"]), str(r["timestamp"])),
                "type": "SEQUENCE_GAP",
                "severity": "LOW",
                "site_id": str(r["site_id"]),
                "ticket_id": str(r.get("ticket_id", "")),
                "reasons": ["LARGE_INTER_ARRIVAL_GAP"],
                "evidence": {"gap_hours": float(r["gap_hours"]), "prev_ts": str(r["prev_ts"])},
            }
        )

    for _, r in missing_days.iterrows():
        findings.append(
            {
                "id": _stable_id("MISS", str(r["site_id"]), str(r["day"])),
                "type": "MISSING_OR_LOW_ACTIVITY",
                "severity": "MEDIUM",
                "site_id": str(r["site_id"]),
                "ticket_id": None,
                "reasons": ["DAILY_VOLUME_SHORTFALL"],
                "evidence": {
                    "day": str(r["day"]),
                    "observed": int(r["observed"]),
                    "expected_median": float(r["expected_median"]),
                    "shortfall_ratio": float(r["shortfall_ratio"]) if pd.notna(r["shortfall_ratio"]) else None,
                },
            }
        )

    for _, r in manifest.iterrows():
        findings.append(
            {
                "id": _stable_id("MNF", str(r["manifest_ref"])),
                "type": "MANIFEST_INCONSISTENCY",
                "severity": "MEDIUM",
                "site_id": None,
                "ticket_id": None,
                "reasons": ["MANIFEST_VOLUME_MISMATCH"],
                "evidence": {
                    "manifest_ref": str(r["manifest_ref"]),
                    "count": int(r["count"]),
                    "spread_ratio": float(r["spread_ratio"]) if pd.notna(r["spread_ratio"]) else None,
                },
            }
        )

    summary = {
        "rows": int(len(flagged)),
        "sites": int(flagged["site_id"].nunique()),
        "outlier_rows": int(outliers.shape[0]),
        "duplicate_rows": int(dupes.shape[0]),
        "gap_rows": int(gaps.shape[0]),
        "missing_day_rows": int(missing_days.shape[0]),
        "manifest_rows": int(manifest.shape[0]),
        "finding_count": len(findings),
    }

    return {
        "summary": summary,
        "findings": findings,
        "samples": {
            "outliers": _df_to_records(outliers, limit=25),
            "duplicates": _df_to_records(dupes, limit=25),
            "gaps": _df_to_records(gaps, limit=25),
            "missing_days": _df_to_records(missing_days, limit=25),
        },
    }


@bp.get("/health")
def health():
    return jsonify({"status": "ok", "time": datetime.now(timezone.utc).isoformat()})


@bp.get("/api/v1/meta/sites")
def sites():
    df = store.snapshot()
    if df.empty or "site_id" not in df.columns:
        sites_list = [f"SITE-{i:02d}" for i in range(1, 13)]
    else:
        sites_list = sorted(df["site_id"].dropna().astype(str).unique().tolist())
    return jsonify({"collection_sites": sites_list, "count": len(sites_list)})


@bp.post("/api/v1/dataset/load-sample")
def load_sample():
    if not DATA_PATH.exists():
        return jsonify({"error": f"Missing sample file: {DATA_PATH}. Run scripts/generate_sample_data.py"}), 400
    df = pd.read_csv(DATA_PATH)
    store.replace(df, source=str(DATA_PATH))
    return jsonify({"loaded": True, "rows": len(df), "source": str(DATA_PATH)})


@bp.post("/api/v1/dataset/upload")
def upload():
    if "file" in request.files:
        raw = request.files["file"].read()
        df = pd.read_csv(io.BytesIO(raw))
    else:
        payload = request.get_json(silent=True) or {}
        records = payload.get("records")
        if not isinstance(records, list):
            return jsonify({"error": "Send multipart file `file` or JSON {records: [...]}"}), 400
        df = pd.DataFrame(records)
    store.replace(df, source="upload")
    return jsonify({"loaded": True, "rows": len(df), "source": "upload"})


@bp.get("/api/v1/dataset/status")
def dataset_status():
    df = store.snapshot()
    return jsonify({"rows": len(df), "columns": list(df.columns), "source": store.source})


@bp.post("/api/v1/analysis/run")
def analysis_run():
    df = store.snapshot()
    try:
        result = _run_pipeline(df)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(result)


@bp.post("/api/v1/tickets/match")
def tickets_match():
    body = request.get_json(silent=True) or {}
    required = ["ticket_id", "site_id", "timestamp", "net_volume_bbl", "manifest_ref"]
    for k in required:
        if k not in body:
            return jsonify({"error": f"Missing field: {k}"}), 400

    hist = store.snapshot()
    res = match_incoming_ticket(
        ticket_id=str(body["ticket_id"]),
        site_id=str(body["site_id"]),
        timestamp=body["timestamp"],
        net_volume_bbl=float(body["net_volume_bbl"]),
        manifest_ref=str(body["manifest_ref"]),
        history=hist,
        gap_hours=float(body.get("gap_hours", 36.0)),
    )
    return jsonify({"status": res.status, "reasons": res.reasons, "details": res.details})


@bp.post("/api/v1/tickets/ingest")
def tickets_ingest():
    body = request.get_json(silent=True) or {}
    required = ["ticket_id", "site_id", "timestamp", "net_volume_bbl", "manifest_ref"]
    for k in required:
        if k not in body:
            return jsonify({"error": f"Missing field: {k}"}), 400

    ts = pd.to_datetime(body["timestamp"], utc=True, errors="coerce")
    if pd.isna(ts):
        return jsonify({"error": "Invalid timestamp"}), 400

    rec = TicketRecord(
        ticket_id=str(body["ticket_id"]),
        site_id=str(body["site_id"]),
        ts=ts.to_pydatetime(),
        net_volume_bbl=float(body["net_volume_bbl"]),
        manifest_ref=str(body["manifest_ref"]),
        extra={k: v for k, v in body.items() if k not in required},
    )
    ledger.upsert(rec)

    row = {
        "ticket_id": rec.ticket_id,
        "site_id": rec.site_id,
        "timestamp": rec.ts.isoformat(),
        "net_volume_bbl": rec.net_volume_bbl,
        "ticket_amount_usd": float(body.get("ticket_amount_usd", 0.0)),
        "manifest_ref": rec.manifest_ref,
    }
    df = store.snapshot()
    store.replace(pd.concat([df, pd.DataFrame([row])], ignore_index=True), source=store.source or "ingest")
    return jsonify({"ingested": True, "ticket_id": rec.ticket_id})


@bp.get("/api/v1/investigations")
def investigations():
    df = store.snapshot()
    if df.empty:
        return jsonify({"investigations": [], "note": "Load a dataset first."})
    try:
        pipeline = _run_pipeline(df)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    investigations_out = []
    for f in pipeline["findings"]:
        investigations_out.append(
            {
                "investigation_id": f"INV-{f['id']}",
                "finding": f,
                "workflow_state": "NEW",
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return jsonify({"investigations": investigations_out, "summary": pipeline["summary"]})


@bp.post("/api/v1/investigations/<inv_id>/transition")
def investigation_transition(inv_id: str):
    body = request.get_json(silent=True) or {}
    state = body.get("workflow_state", "REVIEWED")
    return jsonify({"investigation_id": inv_id, "workflow_state": state})


@bp.post("/api/v1/ledger/reset")
def ledger_reset():
    ledger.clear()
    return jsonify({"cleared": True})
