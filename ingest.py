"""ingest.py

Functions to ingest telemetry CSV files into pandas DataFrames.

Assumptions:
- Files use a header row. Time column appears as 'Time'.
- Missing values encoded as 'NS', '-99.95', '-99.9' or empty strings and should map to NaN.

TODO: add streaming/iterator ingestion for very large files.
"""
from __future__ import annotations

import os
from typing import List
import pandas as pd


def _clean_na_values(df: pd.DataFrame) -> pd.DataFrame:
    # Replace common vendor 'no sample' encodings with NaN
    na_values = ["NS", "-99.95", "-99.9", "-40.0", ""]
    return df.replace(na_values, pd.NA)


def load_csv_files(paths: List[str]) -> pd.DataFrame:
    """Load one or more CSV files and return a concatenated DataFrame.

    Args:
        paths: list of file paths to CSV files.

    Returns:
        pandas.DataFrame with a parsed DatetimeIndex column named 'timestamp'.
    """
    dfs = []
    for p in paths:
        if not os.path.exists(p):
            raise FileNotFoundError(p)
        # read with low_memory False to avoid dtype warnings
        df = pd.read_csv(p, low_memory=False)
        df.columns = [c.strip() for c in df.columns]
        # Normalise Time column
        if "Time" in df.columns:
            # Parse time - many vendor formats; try common one
            df["timestamp"] = pd.to_datetime(df["Time"], errors="coerce", utc=True)
        else:
            df["timestamp"] = pd.NaT
        df = _clean_na_values(df)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    combined = pd.concat(dfs, ignore_index=True)
    # Prefer to have timestamp as timezone-naive UTC for analysis
    combined["timestamp"] = pd.to_datetime(combined["timestamp"], utc=True).dt.tz_convert(None)
    return combined


def find_csvs_in_dir(directory: str) -> List[str]:
    """Return CSV file paths in a directory (non-recursive).

    Simple helper used by CLI and tests.
    """
    files = []
    for entry in os.listdir(directory):
        if entry.lower().endswith(".csv"):
            files.append(os.path.join(directory, entry))
    return sorted(files)


if __name__ == "__main__":
    # quick smoke test (manual) when invoked directly
    import sys
    if len(sys.argv) < 2:
        print("usage: python ingest.py <csv-or-dir>")
        raise SystemExit(1)
    arg = sys.argv[1]
    paths = [arg] if arg.lower().endswith(".csv") else find_csvs_in_dir(arg)
    df = load_csv_files(paths)
    print("Loaded rows:", len(df))