"""clean.py

Data cleaning utilities: normalization, interpolation, resampling and smoothing.

Functions are intentionally small and testable.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Iterable, Optional


def cast_numeric(df: pd.DataFrame, cols: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """Attempt to cast columns to numeric, leaving others intact.

    Non-convertible values become NaN.
    """
    if cols is None:
        cols = [c for c in df.columns if c not in ("Time", "timestamp", "NE", "TP")]
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def resample_group(df: pd.DataFrame, group_cols: Iterable[str], freq: str = "15T") -> pd.DataFrame:
    """Resample time series per group (e.g., per TP) to a regular grid and interpolate.

    - Assumes there is a 'timestamp' column.
    - Sets timestamp as index for resampling then restores columns.
    """
    df = df.copy()
    if "timestamp" not in df.columns:
        raise ValueError("timestamp column required")
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    groups = []
    for name, g in df.groupby(list(group_cols)):
        g = g.set_index("timestamp").sort_index()
        # reindex with regular frequency across span
        full_idx = pd.date_range(start=g.index.min(), end=g.index.max(), freq=freq)
        g = g.reindex(full_idx)
        # forward/backward fill small gaps then linear interpolate
        g = g.ffill(limit=2).bfill(limit=2)
        g = g.interpolate(method="time", limit=4)
        # add back group keys as columns
        if isinstance(name, tuple):
            for k, v in zip(group_cols, name):
                g[k] = v
        else:
            g[list(group_cols)[0]] = name
        groups.append(g)
    if not groups:
        return pd.DataFrame()
    out = pd.concat(groups)
    out = out.reset_index().rename(columns={"index": "timestamp"})
    return out


def moving_average_smooth(series: pd.Series, window: int = 3) -> pd.Series:
    """Return a simple centered moving average; min_periods=1 to keep values early in series."""
    return series.rolling(window=window, center=True, min_periods=1).mean()


def normalize_zscore(series: pd.Series) -> pd.Series:
    """Z-score normalization robust to NaNs."""
    return (series - series.mean(skipna=True)) / series.std(skipna=True)


if __name__ == "__main__":
    print("clean.py module — import into your scripts")