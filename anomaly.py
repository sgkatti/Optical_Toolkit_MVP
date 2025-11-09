"""anomaly.py

Baseline-based anomaly detection helpers.

Functions:
- compute_baselines: compute per-span baseline stats (median, mean, std) for KPIs
- detect_anomalies_baseline: flag per-row anomalies relative to baseline (Q-factor drop, CD drift, OSNR drop)

TODO: make thresholds configurable per-span and persist baselines to disk.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
from typing import List, Dict


DEFAULT_KPIS = ["QFACTOR-AVG", "ESNR-AVG", "CDR", "PREFEC-AVG"]


def _find_present_kpis(df: pd.DataFrame, candidates: List[str]) -> List[str]:
    return [c for c in candidates if c in df.columns]


def compute_baselines(df: pd.DataFrame, span_key: str = "TP", kpis: List[str] | None = None) -> pd.DataFrame:
    """Compute median/mean/std per span for KPI columns.

    Returns a DataFrame indexed by span_key with columns like '<KPI>_median'.
    """
    if kpis is None:
        kpis = DEFAULT_KPIS
    kpis_present = _find_present_kpis(df, kpis)
    if not kpis_present:
        return pd.DataFrame()
    agg = {}
    for k in kpis_present:
        agg[k] = ["median", "mean", "std", "count"]
    # cast numeric safely
    df2 = df.copy()
    for k in kpis_present:
        df2[k] = pd.to_numeric(df2[k], errors="coerce")
    grouped = df2.groupby(span_key).agg(agg)
    # flatten
    grouped.columns = [f"{c[0]}_{c[1]}" for c in grouped.columns]
    return grouped


def detect_anomalies_baseline(
    df: pd.DataFrame,
    baselines: pd.DataFrame,
    span_key: str = "TP",
    q_drop_db: float = 1.0,
    cd_drift: float = 1000.0,
    osnr_min: float | None = None,
) -> pd.DataFrame:
    """Detect anomalies relative to provided baselines.

    Produces a DataFrame with anomaly records: timestamp, span, kpi, value, baseline, reason.
    Rules implemented:
    - Q-factor drop: row Q < baseline_median - q_drop_db
    - CD drift: abs(row CD - baseline_median) > cd_drift
    - OSNR min (optional): row OSNR < osnr_min
    """
    recs = []
    if baselines.empty:
        return pd.DataFrame()
    # find columns
    qcol = "QFACTOR-AVG" if "QFACTOR-AVG" in df.columns else None
    cdcol = "CDR" if "CDR" in df.columns else None
    osnrcol = "ESNR-AVG" if "ESNR-AVG" in df.columns else None

    df2 = df.copy()
    df2[span_key] = df2[span_key].astype(str)
    # numeric cast
    if qcol:
        df2[qcol] = pd.to_numeric(df2[qcol], errors="coerce")
    if cdcol:
        df2[cdcol] = pd.to_numeric(df2[cdcol], errors="coerce")
    if osnrcol:
        df2[osnrcol] = pd.to_numeric(df2[osnrcol], errors="coerce")

    # iterate rows (vectorized join is nicer, but keep clear logic)
    # create a baseline lookup dict
    baseline_lookup = {}
    for span, row in baselines.iterrows():
        baseline_lookup[str(span)] = row.to_dict()

    for idx, row in df2.iterrows():
        span = str(row.get(span_key))
        base = baseline_lookup.get(span)
        if base is None:
            continue
        ts = row.get("timestamp")
        # Q-factor drop
        if qcol and pd.notna(row.get(qcol)) and f"{qcol}_median" in base:
            baseline_q = base.get(f"{qcol}_median")
            if pd.notna(baseline_q) and row[qcol] < (baseline_q - q_drop_db):
                recs.append({
                    "timestamp": ts,
                    "span": span,
                    "kpi": "qfactor",
                    "col": qcol,
                    "value": float(row[qcol]),
                    "baseline": float(baseline_q),
                    "reason": f"q_drop<{q_drop_db}dB_vs_baseline",
                })
        # CD drift
        if cdcol and pd.notna(row.get(cdcol)) and f"{cdcol}_median" in base:
            baseline_cd = base.get(f"{cdcol}_median")
            if pd.notna(baseline_cd) and abs(row[cdcol] - baseline_cd) > cd_drift:
                recs.append({
                    "timestamp": ts,
                    "span": span,
                    "kpi": "cd",
                    "col": cdcol,
                    "value": float(row[cdcol]),
                    "baseline": float(baseline_cd),
                    "reason": f"cd_drift>{cd_drift}",
                })
        # OSNR min
        if osnrcol and osnr_min is not None and pd.notna(row.get(osnrcol)):
            if row[osnrcol] < osnr_min:
                recs.append({
                    "timestamp": ts,
                    "span": span,
                    "kpi": "osnr",
                    "col": osnrcol,
                    "value": float(row[osnrcol]),
                    "baseline": float(base.get(f"{osnrcol}_median", np.nan)),
                    "reason": f"osnr_below_min<{osnr_min}",
                })

    if not recs:
        return pd.DataFrame()
    out = pd.DataFrame(recs)
    return out


if __name__ == "__main__":
    print("anomaly helper module")
