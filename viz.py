"""viz.py

Visualization helpers: generate time-series plots with anomalies overlay and forecast line
and a small HTML dashboard with embedded PNGs and an anomalies table.
"""
from __future__ import annotations

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
from typing import Optional
from predict import forecast_linear_trend


def plot_kpi_with_anomalies_and_forecast(
    df: pd.DataFrame,
    anomalies: pd.DataFrame,
    span: str,
    kpi_col: str,
    out_dir: str,
    forecast_periods: int = 24,
    freq: str = "15T",
) -> Optional[str]:
    """Create a plot for one span/kpi showing historic values, anomalies and forecast.

    Returns path to saved PNG or None if not enough data.
    """
    sub = df[df["TP"].astype(str) == str(span)][["timestamp", kpi_col]].dropna()
    if sub.empty or len(sub) < 3:
        return None
    sub = sub.sort_values("timestamp")
    plt.figure(figsize=(12, 4))
    plt.plot(sub["timestamp"], sub[kpi_col], label="historic", color="tab:blue")
    # overlay anomalies for this span & kpi
    if anomalies is not None and not anomalies.empty:
        a = anomalies[(anomalies["span"] == span) & (anomalies["kpi"] == kpi_col.lower() or anomalies["col"] == kpi_col)]
        if a is not None and not a.empty:
            # anomalies may not have timestamps for every row; ensure datetime
            a_ts = pd.to_datetime(a["timestamp"], errors="coerce")
            plt.scatter(a_ts, a.get("value", []), color="red", label="anomalies", zorder=5)

    # forecast
    fc = forecast_linear_trend(sub[kpi_col], sub["timestamp"], periods=forecast_periods, freq=freq)
    if not fc.empty:
        plt.plot(fc["timestamp"], fc["forecast"], label="forecast", linestyle="--", color="tab:orange")
    plt.title(f"{span} — {kpi_col}")
    plt.xlabel("time")
    plt.ylabel(kpi_col)
    plt.legend()
    plt.tight_layout()
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{span}_{kpi_col}.png")
    plt.savefig(out_path)
    plt.close()
    return out_path


def build_dashboard_html(image_paths: list, anomalies_df: pd.DataFrame, out_path: str):
    """Write a simple HTML file embedding images and a small anomalies table.

    images: list of (title, path) or str paths.
    """
    rows = []
    for p in image_paths:
        title = os.path.basename(p)
        rows.append(f"<h3>{title}</h3><img src=\"{os.path.basename(p)}\" style=\"max-width:100%;height:auto\">")

    # small anomalies table (top 200)
    table_html = "<p>No anomalies detected.</p>"
    if anomalies_df is not None and not anomalies_df.empty:
        small = anomalies_df.head(200).copy()
        small["timestamp"] = small["timestamp"].astype(str)
        table_html = small.to_html(index=False, classes="anomaly-table")

    html = f"""
    <html>
    <head>
      <meta charset='utf-8'/>
      <title>Optical Telemetry Dashboard</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 16px; }}
        .anomaly-table {{ border-collapse: collapse; width: 100%; }}
        .anomaly-table th, .anomaly-table td {{ border: 1px solid #ddd; padding: 8px; }}
      </style>
    </head>
    <body>
      <h1>Optical Telemetry — Dashboard</h1>
      {''.join(rows)}
      <h2>Recent anomalies (top 200)</h2>
      {table_html}
    </body>
    </html>
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    # copy images to same folder
    dashboard_dir = os.path.dirname(out_path)
    for p in image_paths:
        try:
            import shutil

            shutil.copy(p, os.path.join(dashboard_dir, os.path.basename(p)))
        except Exception:
            pass
    with open(out_path, "w", encoding="utf8") as fh:
        fh.write(html)


if __name__ == "__main__":
    print("viz module: use plot_kpi_with_anomalies_and_forecast and build_dashboard_html")
