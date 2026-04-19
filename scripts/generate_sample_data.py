"""Generate synthetic transport tickets for 12 collection sites."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "sample_tickets.csv"


def main() -> None:
    rng = np.random.default_rng(42)
    sites = [f"SITE-{i:02d}" for i in range(1, 13)]
    rows: list[dict] = []
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)

    for day in range(90):
        d = base + timedelta(days=day)
        for site in sites:
            n = int(rng.integers(4, 12))
            manifest = f"MNF-{site}-{d.date().isoformat()}"
            manifest_base_vol = float(rng.normal(120, 14))
            for _ in range(n):
                ts = d + timedelta(minutes=int(rng.integers(0, 24 * 60)))
                vol = manifest_base_vol + float(rng.normal(0, 2.2))
                amt = float(rng.normal(3500 + (manifest_base_vol - 120.0) * 6.0, 380))
                tid = f"TKT-{site}-{d.date().isoformat()}-{len(rows):05d}"
                rows.append(
                    {
                        "ticket_id": tid,
                        "site_id": site,
                        "timestamp": ts.isoformat(),
                        "net_volume_bbl": round(vol, 3),
                        "ticket_amount_usd": round(max(0, amt), 2),
                        "manifest_ref": manifest,
                    }
                )

    df = pd.DataFrame(rows)

    # Inject anomalies for demos
    idxs = rng.choice(df.index, size=8, replace=False)
    df.loc[idxs[0], "net_volume_bbl"] = 600.0
    df.loc[idxs[1], "ticket_amount_usd"] = 12000.0
    dup_idx = int(idxs[2])
    dup = df.loc[[dup_idx]].copy()
    dup["timestamp"] = (pd.to_datetime(dup["timestamp"], utc=True) + pd.Timedelta(hours=2)).map(
        lambda x: x.isoformat()
    )
    df = pd.concat([df, dup], ignore_index=True)

    # Sparse day for one site (simulates missing hauls / incomplete uploads)
    sparse_site = "SITE-07"
    sparse_day = (base + timedelta(days=40)).date().isoformat()
    day_token = sparse_day
    df = df[~((df["site_id"] == sparse_site) & (df["timestamp"].str.slice(0, 10) == day_token))]

    # Manifest volume conflict on a busy manifest
    top_manifest = df.groupby("manifest_ref").size().sort_values(ascending=False).index[0]
    mrows = df.index[df["manifest_ref"] == top_manifest].tolist()
    if len(mrows) >= 2:
        base_vol = float(df.loc[mrows[0], "net_volume_bbl"])
        df.loc[mrows[1], "net_volume_bbl"] = round(base_vol * 2.4, 3)

    # Large inter-arrival gap at SITE-03: clear a 40h window so the next ticket is genuinely >36h after its predecessor
    site_gap = "SITE-03"
    s3 = df.loc[df["site_id"] == site_gap].sort_values("timestamp").reset_index(drop=False)
    if len(s3) >= 5:
        pos = min(120, len(s3) - 1)
        t_lo = pd.to_datetime(s3.loc[pos - 1, "timestamp"], utc=True)
        t_target = t_lo + pd.Timedelta(hours=40)
        move_idx = int(s3.loc[pos, "index"])
        ts_all = pd.to_datetime(df["timestamp"], utc=True)
        drop_mask = (df["site_id"] == site_gap) & (ts_all > t_lo) & (ts_all < t_target)
        if move_idx in df.index:
            drop_mask.loc[move_idx] = False
        df = df.loc[~drop_mask].copy()
        df.loc[move_idx, "timestamp"] = t_target.isoformat()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"Wrote {len(df)} rows to {OUT}")


if __name__ == "__main__":
    main()
