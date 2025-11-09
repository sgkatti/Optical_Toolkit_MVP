"""predict.py

Simple prediction helpers using linear trend extrapolation.

This is intentionally lightweight: uses numpy linear regression over a recent window
to forecast a few future timestamps. It's not a full statistical model (ARIMA/Prophet)
but is sufficient for MVP and easy to run in constrained environments.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Tuple


def forecast_linear_trend(
    series: pd.Series,
    timestamps: pd.Series,
    periods: int = 24,
    freq: str = "15T",
    window: int = 96,
) -> pd.DataFrame:
    """Forecast future values using linear regression on recent window.

    Args:
        series: numeric series of historic values (aligned with timestamps)
        timestamps: pandas Series of datetime64[ns]
        periods: number of future points to predict
        freq: frequency string for future timestamps
        window: number of recent historic points to fit the linear model

    Returns:
        DataFrame with columns ['timestamp', 'forecast'] containing the forecasted points.
    """
    # align and drop NaNs
    df = pd.DataFrame({"ts": pd.to_datetime(timestamps), "y": pd.to_numeric(series, errors="coerce")}).dropna()
    if df.shape[0] < 3:
        return pd.DataFrame()
    # use last `window` points
    df = df.sort_values("ts").iloc[-window:]
    # convert timestamps to numeric (seconds since epoch)
    x = df["ts"].astype("int64") // 10 ** 9
    y = df["y"].values
    # linear fit
    try:
        coef = np.polyfit(x, y, 1)
    except Exception:
        return pd.DataFrame()
    # prepare future timestamps
    last_ts = df["ts"].max()
    future_idx = pd.date_range(start=last_ts + pd.Timedelta(freq), periods=periods, freq=freq)
    x_future = future_idx.astype("int64") // 10 ** 9
    y_future = np.polyval(coef, x_future)
    return pd.DataFrame({"timestamp": future_idx, "forecast": y_future})


def forecast_kpi_for_span(df: pd.DataFrame, span: str, kpi_col: str, periods: int = 24, freq: str = "15T") -> pd.DataFrame:
    """Helper: filter df for span and forecast kpi_col.

    Returns forecast dataframe or empty df if not enough data.
    """
    if span not in df["TP"].astype(str).values:
        # fallback to NE or other
        if span not in df.get("NE", pd.Series(dtype=str)).astype(str).values:
            return pd.DataFrame()
    # filter by TP membership
    mask = df["TP"].astype(str) == str(span)
    sub = df.loc[mask, ["timestamp", kpi_col]].dropna()
    if sub.empty:
        return pd.DataFrame()
    return forecast_linear_trend(sub[kpi_col], sub["timestamp"], periods=periods, freq=freq)


if __name__ == "__main__":
    print("predict module: use forecast_kpi_for_span from your pipeline")
