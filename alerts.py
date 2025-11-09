"""alerts.py

Simple alerting logic for telemetry KPIs.

This module contains threshold checks and a tiny rules engine for generating alert records.
"""
from __future__ import annotations

from typing import Dict, Any, List
import pandas as pd


def build_thresholds() -> Dict[str, Dict[str, float]]:
    """Return a sensible default thresholds dict.

    These are conservative examples; tune per network.
    """
    return {
        "osnr": {"min": 15.0},
        "pre_fec_ber": {"max": 1e-3},
        "qfactor": {"min_drop_db": 1.0},  # drop compared to baseline handled externally
        "cd": {"max_drift": 1000.0},
    }


def evaluate_thresholds(df: pd.DataFrame, thresholds: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    """Return list of alert dicts for rows breaching thresholds.

    Uses column names present in DataFrame; expects numeric columns or will coerce errors to NaN.
    """
    alerts = []
    # common KPI column names (a simpler mapping than analyze.KPI_COLUMN_MAP)
    mapping = {
        "osnr": ["ESNR-AVG", "ESNR", "OSNR-AVG", "OSNR"],
        "pre_fec_ber": ["PREFEC-AVG", "PRE-FEC", "PRE-FEC-AVG"],
        "qfactor": ["QFACTOR-AVG", "QFACTOR"],
        "cd": ["CDR", "CDR-AVG", "CD"],
    }
    cols_present = set(df.columns)
    for k, cond in thresholds.items():
        # find a column to check
        col = None
        for v in mapping.get(k, []):
            if v in cols_present:
                col = v
                break
        if col is None:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        if "min" in cond:
            mask = series < cond["min"]
            for idx in df[mask].index:
                alerts.append({"row_index": int(idx), "kpi": k, "col": col, "value": float(series.loc[idx]), "reason": "below_min"})
        if "max" in cond:
            mask = series > cond["max"]
            for idx in df[mask].index:
                alerts.append({"row_index": int(idx), "kpi": k, "col": col, "value": float(series.loc[idx]), "reason": "above_max"})
    return alerts


if __name__ == "__main__":
    print("alerts module — import in main")