# Simulator

This folder contains the custom staking simulator and analytics app for this repository.

## Purpose

- Run lightweight staking-economics experiments and a simulation-first dashboard from this repo.
- Expose the Ethereum economic-model assumptions in a transparent Streamlit frontend.
- Generate local plots, summaries, exports, and notebook-style visualizations for quick iteration.

## Main Files

- `simulation.py` runs scenario simulations and plotting.
- `network.py` defines network-level behavior and epoch/slot mechanics.
- `validator.py` defines validator state and economics objects.
- `behavior.py` contains validator decision logic.
- `fetch_hoodi.py` exports a local simulation snapshot JSON for the dashboard defaults and summaries.
- `live_dashboard.py` is the localhost Streamlit frontend for the simulation-only staking economics lab.
- `live_dashboard_data.py` runs the economic-model experiment templates, builds the dashboard frames, and serializes snapshot exports.
- `equivocation_attack.py` models a high-level equivocation attack tab that shows slashable validators, safety thresholds, and post-slash recovery.
- `test.py` is a smoke test for the container.

## Notes

- This is the fast, custom experimentation environment.
- It is intentionally separate from `../cadlabs`, which has its own model, dependencies, and workflow.
- Container outputs land in `../shared/output` and `../shared/data`.
- The dashboard no longer auto-refreshes or reads live beacon data on a slot interval.
- The simulation frontend is driven by the `ethereum-economic-model` submodule and the experiment patterns in its notebooks.
- Starting-state inputs and validator-environment assumptions are surfaced directly in the UI so runs are easier to inspect and reproduce.
- The dashboard also includes a custom equivocation-attack view focused on accountable safety and slashing response, intended for analysis rather than operational guidance.
