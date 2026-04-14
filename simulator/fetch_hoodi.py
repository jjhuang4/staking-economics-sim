import json
import os
from datetime import datetime, timezone

import requests

from pool_tracker.config import resolve_env_value


def fetch_json(session: requests.Session, url: str):
    response = session.get(url, timeout=15)
    response.raise_for_status()
    return response.json()


def main():
    execution_rpc = resolve_env_value("HOODI_EXECUTION_RPC_URL", "EXECUTION_RPC_URL")
    beacon_api = resolve_env_value("HOODI_BEACON_API_URL", "BEACON_API_URL")
    data_dir = os.getenv("SIM_DATA_DIR", "/app/shared/data")
    os.makedirs(data_dir, exist_ok=True)

    snapshot = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "execution_rpc_configured": bool(execution_rpc),
        "beacon_api_configured": bool(beacon_api),
        "sources": {},
    }

    with requests.Session() as session:
        if execution_rpc:
            payload = {
                "jsonrpc": "2.0",
                "method": "eth_blockNumber",
                "params": [],
                "id": 1,
            }
            response = session.post(execution_rpc, json=payload, timeout=15)
            response.raise_for_status()
            snapshot["sources"]["execution_rpc"] = response.json()

        if beacon_api:
            snapshot["sources"]["beacon_genesis"] = fetch_json(
                session, f"{beacon_api.rstrip('/')}/eth/v1/beacon/genesis"
            )
            snapshot["sources"]["beacon_finality_checkpoints"] = fetch_json(
                session, f"{beacon_api.rstrip('/')}/eth/v1/beacon/states/head/finality_checkpoints"
            )

    output_path = os.path.join(data_dir, "hoodi_snapshot.json")
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(snapshot, fh, indent=2)

    print(f"Wrote Hoodi snapshot to {output_path}")


if __name__ == "__main__":
    main()
