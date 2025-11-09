# Optical Telemetry Analyzer MVP v1.0

This repository contains a minimal MVP to ingest, clean, analyze and export optical performance monitoring (PM) telemetry.

What is included
- `ingest.py` - CSV ingestion helpers
- `clean.py` - cleaning, casting and resampling utilities
- `analyze.py` - KPI inference, time-range and missing timestamp detection, simple summaries
- `alerts.py` - threshold evaluation and alert records
- `export.py` - JSON/CSV/Prometheus text export helpers
- `main.py` - CLI entrypoint to run the basic pipeline
- `requirements.txt` - minimal Python dependencies

Quickstart (Codespaces or local)

1. Create a virtualenv and install dependencies (optional but recommended):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Run the analyzer pointing at the directory that contains the CSVs (the repo root contains some sample CSVs):

```bash
python main.py --input-dir . --output-dir ./output
```

3. Outputs
- `output/summary.json` — high-level summary for dashboards
- `output/span_summary.csv` — per-span aggregated KPIs
- `output/alerts.csv` — rows that breached simple thresholds (if any)
- `output/prom_metrics.txt` — optional Prometheus exposition for a small sample
- `output/analyzer.log` — logs

How to run in Codespaces
- Open the repository in Codespaces. Use the terminal to run the Quickstart steps above. Codespaces already includes Python and should allow installing the pip requirements.

Next steps (v1.1 roadmap — see ROADMAP section in docs)
- Add unit tests and CI
- Add Prometheus pushgateway or client-based exporter
- Add anomaly detection using rolling baselines and ML models
- Add web dashboard exporter (JSON API) and Grafana dashboards

TODOs are sprinkled across code files for future enhancements.

License: internal example code
# Optical_Toolkit_MVP