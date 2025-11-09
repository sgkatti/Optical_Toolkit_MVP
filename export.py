"""export.py

Small helpers to export JSON summaries and CSV debugging files.
"""
from __future__ import annotations

import json
from typing import Any, Dict
import pandas as pd


def write_json_summary(obj: Dict[str, Any], out_path: str) -> None:
    with open(out_path, "w", encoding="utf8") as fh:
        json.dump(obj, fh, indent=2, default=str)


def write_csv(df: pd.DataFrame, out_path: str) -> None:
    df.to_csv(out_path, index=False)


def df_to_prometheus_lines(df: pd.DataFrame, metric_prefix: str = "opt_telemetry") -> str:
    """Optional exporter: produce a naive Prometheus text exposition for simple metrics.

    This is intentionally minimal — a production export would use client libraries.
    """
    lines = []
    for idx, row in df.iterrows():
        # example: opt_osnr{span="...",ne="..."} 15.2
        # pick a few numeric columns
        for col in df.select_dtypes(include=["number"]).columns:
            metric = f"{metric_prefix}_{col}"
            labels = []
            if "TP" in row.index:
                labels.append(f'tp="{row.get("TP")}"')
            if "NE" in row.index:
                labels.append(f'ne="{row.get("NE")}"')
            label_str = "{" + ",".join(labels) + "}" if labels else ""
            val = row[col]
            if pd.isna(val):
                continue
            lines.append(f"{metric}{label_str} {float(val)}")
    return "\n".join(lines)


if __name__ == "__main__":
    print("export helpers")