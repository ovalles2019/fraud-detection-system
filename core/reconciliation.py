"""Missing-record and coverage checks across sites and days."""

from __future__ import annotations

import numpy as np
import pandas as pd


def expected_daily_counts(
    df: pd.DataFrame,
    site_col: str = "site_id",
    day_col: str = "day",
    count_col: str = "ticket_id",
) -> pd.DataFrame:
    """Rolling median of daily ticket counts per site; flags unusually low days."""

    d = df.copy()
    if day_col not in d.columns and "timestamp" in d.columns:
        d[day_col] = pd.to_datetime(d["timestamp"], utc=True, errors="coerce").dt.date.astype(str)

    daily = d.groupby([site_col, day_col], dropna=False)[count_col].count().reset_index(name="observed")
    daily = daily.sort_values([site_col, day_col])

    def _roll(g: pd.DataFrame) -> pd.DataFrame:
        gg = g.copy()
        gg["expected_median"] = gg["observed"].rolling(window=7, min_periods=3).median()
        gg["shortfall_ratio"] = np.where(
            gg["expected_median"] > 0,
            gg["observed"] / gg["expected_median"],
            np.nan,
        )
        gg["missing_activity_flag"] = (gg["expected_median"] > 0) & (gg["shortfall_ratio"] < 0.55)
        return gg

    parts: list[pd.DataFrame] = []
    for _, g in daily.groupby(site_col, sort=False):
        parts.append(_roll(g))
    return pd.concat(parts, ignore_index=True) if parts else daily
