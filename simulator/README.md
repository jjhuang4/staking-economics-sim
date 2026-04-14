# Simulator

This folder contains the custom staking simulator and analytics app for this repository.

## Purpose

- Run lightweight staking-economics experiments without depending on the CADLabs model.
- Pull read-only Hoodi execution and beacon data through environment-configured endpoints.
- Generate local plots, summaries, test outputs, and a live Streamlit dashboard for quick iteration.

## Main Files

- `simulation.py` runs scenario simulations and plotting.
- `network.py` defines network-level behavior and epoch/slot mechanics.
- `validator.py` defines validator state and economics objects.
- `behavior.py` contains validator decision logic.
- `fetch_hoodi.py` fetches external chain data using Hoodi RPC and Beacon API endpoints.
- `live_dashboard.py` is the localhost Streamlit frontend for live pool and validator monitoring.
- `live_dashboard_data.py` prepares the live Hoodi snapshot, history, and recommendation data for the dashboard.
- `test.py` is a smoke test for the container.

## Notes

- This is the fast, custom experimentation environment.
- It is intentionally separate from `../cadlabs`, which has its own model, dependencies, and workflow.
- Container outputs land in `../shared/output` and `../shared/data`.
- The dashboard is meant for read-only Hoodi analytics and stores per-epoch history in SQLite under `../shared/data` by default.
