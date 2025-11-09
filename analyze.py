"""analyze.py

Functions to extract KPIs, compute time ranges, detect gaps and identify simple anomalies.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import Dict, Any, List


KPI_COLUMN_MAP = {
    # map canonical KPI names to likely CSV columns
    "osnr": ["ESNR-AVG", "ESNR_AVG", "OSNR", "OSNR-AVG"],
    "pre_fec_ber": ["PREFEC-AVG", "PRE-FEC", "PRE-FEC-AVG"],
    "post_fec_ber": ["POST-FEC", "POSTFEC"],
    "qfactor": ["QFACTOR-AVG", "QFACTOR", "QFACTOR_AVG"],
    "cd": ["CDR", "CDR-AVG", "CD"],
    "rx_power": ["OPR-AVG", "TOPR-AVG", "TOPT-AVG", "TOPRL-AVG"],
}


def infer_kpis_present(df: pd.DataFrame) -> List[str]:
    found = []
    cols = set(df.columns)
    for k, variants in KPI_COLUMN_MAP.items():
        for v in variants:
            if v in cols:
                found.append(k)
                break
    return found


def compute_time_range(df: pd.DataFrame) -> Dict[str, Any]:
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    ts = ts.dropna()
    if ts.empty:
        return {"start": None, "end": None, "count": 0}
    return {"start": ts.min().isoformat(), "end": ts.max().isoformat(), "count": int(len(ts))}


def detect_missing_timestamps(df: pd.DataFrame, group_by: List[str], freq: str = "15T") -> pd.DataFrame:
    """Return a DataFrame listing groups with their missing timestamps counts.

    Output columns: group keys..., expected_count, observed_count, missing_count
    """
    out_rows = []
    for name, g in df.groupby(group_by):
        ts = pd.to_datetime(g["timestamp"].dropna()).sort_values()
        if ts.empty:
            continue
        expected_idx = pd.date_range(start=ts.min(), end=ts.max(), freq=freq)
        missing = len(expected_idx) - len(ts.unique())
        row = {
            "group": name,
            "expected_count": len(expected_idx),
            "observed_count": len(ts.unique()),
            "missing_count": int(missing),
        }
        out_rows.append(row)
    return pd.DataFrame(out_rows)


def detect_anomalies_simple(df: pd.DataFrame, thresholds: Dict[str, Any]) -> pd.DataFrame:
    """Return rows that breach simple threshold conditions.

    thresholds example: {"osnr": {"min": 15}, "pre_fec_ber": {"max": 1e-3}}
    Uses KPI_COLUMN_MAP to find actual columns.
    """
    res = []
    # find column names
    col_map = {}
    for k, variants in KPI_COLUMN_MAP.items():
        for v in variants:
            if v in df.columns:
                col_map[k] = v
                break
    for k, cond in thresholds.items():
        col = col_map.get(k)
        if not col:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if "min" in cond:
            mask = s < cond["min"]
            for idx in df[mask].index:
                res.append({"index": int(idx), "kpi": k, "col": col, "value": float(s.loc[idx]), "type": "below_min"})
        if "max" in cond:
            mask = s > cond["max"]
            for idx in df[mask].index:
                res.append({"index": int(idx), "kpi": k, "col": col, "value": float(s.loc[idx]), "type": "above_max"})
    return pd.DataFrame(res)


def summarize_by_span(df: pd.DataFrame, span_key: str = "TP") -> pd.DataFrame:
    """Return summary statistics (mean/min/max/std) per span (TP).

    Includes found KPIs.
    """
    kpis = infer_kpis_present(df)
    agg_map = {}
    col_lookup = {}
    for k in kpis:
        variants = KPI_COLUMN_MAP[k]
        for v in variants:
            if v in df.columns:
                col_lookup[k] = v
                agg_map[v] = ["mean", "min", "max", "std"]
                break
    if not agg_map:
        return pd.DataFrame()
    grouped = df.groupby(span_key).agg(agg_map)
    # flatten columns
    grouped.columns = [f"{c[0]}_{c[1]}" for c in grouped.columns.to_list()]
    return grouped.reset_index()


if __name__ == "__main__":
    print("analysis helpers")