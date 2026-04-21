"""Export a local staking-economics simulation snapshot to JSON.

This script keeps the old filename for compatibility, but it no longer fetches
live Hoodi or Beacon data. It now runs the local economic model with the same
defaults used by the Streamlit dashboard and writes a JSON summary to disk.
"""

from __future__ import annotations

import os

try:
    from .live_dashboard_data import export_dashboard_snapshot, load_dashboard_assumptions_from_env
except ImportError:
    from live_dashboard_data import export_dashboard_snapshot, load_dashboard_assumptions_from_env


def main() -> None:
    data_dir = os.getenv("SIM_DATA_DIR", "shared/data")
    output_path = os.getenv(
        "SIM_SNAPSHOT_PATH",
        os.path.join(data_dir, "simulation_snapshot.json"),
    )

    assumptions = load_dashboard_assumptions_from_env()
    written_path = export_dashboard_snapshot(output_path, assumptions)
    print(f"Wrote simulation snapshot to {written_path}")


if __name__ == "__main__":
    main()
