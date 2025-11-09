"""main.py

CLI entrypoint for the Optical Telemetry Analyzer MVP v1.0.

Usage (examples):
  python main.py --input-dir ./ --output-dir ./output

Outputs:
 - JSON summary of dataset and KPIs
 - CSV debug files

TODO: add config file support, Prometheus pushgateway exporter, and unit tests.
"""
from __future__ import annotations

import argparse
import logging
import os
import json
from pathlib import Path

import pandas as pd

from ingest import find_csvs_in_dir, load_csv_files
from clean import cast_numeric, resample_group, moving_average_smooth
from analyze import infer_kpis_present, compute_time_range, detect_missing_timestamps, summarize_by_span
from alerts import build_thresholds, evaluate_thresholds
from anomaly import compute_baselines, detect_anomalies_baseline
from export import write_json_summary, write_csv, df_to_prometheus_lines
from viz import plot_kpi_with_anomalies_and_forecast, build_dashboard_html


LOG = logging.getLogger("opt_telemetry")


def setup_logging(out_dir: Path, level: str = "INFO"):
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / "analyzer.log"
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(str(log_path))],
    )


def run(args: argparse.Namespace) -> int:
    out_dir = Path(args.output_dir)
    setup_logging(out_dir, level=args.log_level)
    LOG.info("Starting Optical Telemetry Analyzer MVP")

    if args.input_files:
        paths = args.input_files
    else:
        paths = find_csvs_in_dir(args.input_dir)
    LOG.info("Found %d CSV files", len(paths))
    df = load_csv_files(paths)
    LOG.info("Rows loaded: %d", len(df))

    # Basic cleaning
    df = cast_numeric(df)

    # Infer KPIs
    kpis = infer_kpis_present(df)
    LOG.info("KPIs detected: %s", kpis)

    # Compute basic time range
    time_summary = compute_time_range(df)

    # Missing timestamps per TP/NE
    group_by = ["TP"] if "TP" in df.columns else ["NE"]
    missing = detect_missing_timestamps(df, group_by=group_by)

    # Summarize per span
    span_summary = summarize_by_span(df, span_key=group_by[0])

    # Alerts
    thresholds = build_thresholds()
    alerts = evaluate_thresholds(df, thresholds)

    # Baseline-based anomaly detection (Q-factor drop, CD drift, optional OSNR low)
    baselines = compute_baselines(df, span_key=group_by[0])
    anomalies_df = detect_anomalies_baseline(
        df, baselines, span_key=group_by[0], q_drop_db=1.0, cd_drift=1000.0, osnr_min=15.0
    )

    # Exports
    out_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "time_summary": time_summary,
        "kpis": kpis,
        "missing_per_group": missing.to_dict(orient="records") if not missing.empty else [],
        "alerts_count": len(alerts),
    }
    write_json_summary(summary, str(out_dir / "summary.json"))
    write_csv(span_summary, str(out_dir / "span_summary.csv"))
    # Alerts to CSV
    alerts_df = pd.DataFrame(alerts)
    if not alerts_df.empty:
        write_csv(alerts_df, str(out_dir / "alerts.csv"))

    # write anomalies
    if not anomalies_df.empty:
        write_csv(anomalies_df, str(out_dir / "anomalies.csv"))

    # Build dashboard: pick top N spans with anomalies or a default small set
    dashboard_dir = out_dir / "dashboard"
    dashboard_dir.mkdir(parents=True, exist_ok=True)
    image_paths = []
    try:
        # select spans to visualize: those that appear in anomalies, top 6 unique
        spans_to_plot = []
        if not anomalies_df.empty:
            spans_to_plot = list(pd.unique(anomalies_df["span"]))[:6]
        # fallback: first 6 spans in span_summary
        if not spans_to_plot:
            if not span_summary.empty and "TP" in span_summary.columns:
                spans_to_plot = list(span_summary["TP"].astype(str).head(6))
            else:
                spans_to_plot = list(pd.unique(df["TP"].astype(str)))[:6]
        # kpis to plot: take detected KPIs mapped to column names (use ESNR-AVG, QFACTOR-AVG, CDR, PREFEC-AVG if present)
        kpi_cols = [c for c in ["QFACTOR-AVG", "ESNR-AVG", "CDR", "PREFEC-AVG"] if c in df.columns]
        for span in spans_to_plot:
            for kpi in kpi_cols:
                p = plot_kpi_with_anomalies_and_forecast(df, anomalies_df, span, kpi, str(dashboard_dir), forecast_periods=96, freq="15T")
                if p:
                    image_paths.append(p)
        # build HTML
        build_dashboard_html(image_paths, anomalies_df, str(dashboard_dir / "dashboard.html"))
    except Exception as e:
        LOG.exception("dashboard generation failed: %s", e)

    # Optional Prometheus text file
    prom_lines = df_to_prometheus_lines(df.head(1000))
    if prom_lines:
        with open(out_dir / "prom_metrics.txt", "w") as fh:
            fh.write(prom_lines)

    LOG.info("Outputs written to %s", str(out_dir))
    return 0


def cli():
    p = argparse.ArgumentParser(description="Optical Telemetry Analyzer MVP v1.0")
    p.add_argument("--input-dir", default=".", help="Directory with CSV files")
    p.add_argument("--input-files", nargs="*", help="Explicit CSV paths (overrides input-dir)")
    p.add_argument("--output-dir", default="./output", help="Output directory")
    p.add_argument("--log-level", default="INFO", help="Logging level")
    args = p.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    cli()