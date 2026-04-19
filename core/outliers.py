"""Statistical outlier detection on transport ticket fields."""

from __future__ import annotations

import numpy as np
import pandas as pd


def add_zscore_outliers(
    df: pd.DataFrame,
    group_col: str,
    value_cols: list[str],
    z_threshold: float = 3.0,
) -> pd.DataFrame:
    """Per-group z-score; flags rows where any value column exceeds threshold."""

    out = df.copy()
    for col in value_cols:
        s = pd.to_numeric(out[col], errors="coerce")
        gmean = out.groupby(group_col, sort=False)[col].transform(lambda x: pd.to_numeric(x, errors="coerce").mean())
        gstd = out.groupby(group_col, sort=False)[col].transform(lambda x: pd.to_numeric(x, errors="coerce").std(ddof=0))
        z = (s - gmean) / gstd.replace(0, np.nan)
        z = z.fillna(0.0)
        out[f"{col}_z"] = z
        out[f"{col}_outlier"] = z.abs() >= z_threshold
    return out


def summarize_outlier_flags(df: pd.DataFrame, value_cols: list[str]) -> pd.DataFrame:
    cols = [f"{c}_outlier" for c in value_cols if f"{c}_outlier" in df.columns]
    if not cols:
        return df.assign(any_outlier=False)
    any_flag = df[cols].any(axis=1)
    return df.assign(any_outlier=any_flag)
