# Staking Economics Workspace

Sources used:

https://ethereum-economic-model.cadlabs.org/experiments/notebooks/2_validator_revenue_and_profit_yields.html

https://ethereum-economic-model.cadlabs.org/ASSUMPTIONS.html

https://ethereum-economic-model.cadlabs.org/docs/model_specification/mathematical_specification.html

This repository is organized as two separate Docker Compose services:

- `simulator/` is the modern Python environment for Hoodi analytics, plots, and custom staking-simulator logic.
- `cadlabs/` is a separate container for the CADLabs Ethereum Economic Model and must keep its own virtualenv, requirements flow, and notebook-oriented workflow.

## Layout

```text
repo-root/
  compose.yaml
  .env.example
  README.md

  simulator/
    Dockerfile
    requirements.txt
    test.py
    fetch_hoodi.py
    live_dashboard.py
    live_dashboard_data.py
    output/

  cadlabs/
    Dockerfile
    entrypoint.sh

  shared/
    output/
    data/
```

`shared/` is the host-mounted exchange area for CSV, JSON, and plots produced by either container.

## Build And Run

From the repo root, with `.env` and `pool_config.yaml` already in place:

```bash
./build.sh
docker compose up --build simulator
```

Open:

```text
http://localhost:8501
```

Stop the live instance with `Ctrl+C`, then:

```bash
docker compose down
```

## Services

Bootstrap the whole workspace, including the first CADLabs clone/install flow:

```bash
./build.sh
```

### `simulator`

Launch the live Streamlit dashboard on localhost:

```bash
docker compose up --build simulator
docker compose up --build simulator
```

Then open:

```text
http://localhost:8501
```

Run the simulator smoke test explicitly:

```bash
docker compose run --rm simulator python -m simulator.test
```

Fetch optional Hoodi read-only data:

```bash
docker compose run --rm simulator python simulator/fetch_hoodi.py
```

Outputs are written to `/app/shared/output` and `/app/shared/data` inside the container, which map to `shared/output` and `shared/data` on the host.
The simulator service also mounts the repo into `/app`, so the dashboard sees local code and pool config changes immediately.

### `cadlabs`

The CADLabs container clones the model repository at runtime, creates its own virtual environment, and installs the model using its own requirements workflow:

```bash
docker compose run --rm cadlabs
```

You can override the upstream repo and branch through environment variables in `.env`.

## Environment

Copy `.env.example` to `.env` and set only the endpoints you need:

```bash
cp .env.example .env
```

- `HOODI_EXECUTION_RPC_URL` is required for the live dashboard and optional for the basic simulator smoke test.
- `HOODI_BEACON_API_URL` is required for the live dashboard and optional for the basic simulator smoke test.
- `BEACON_CHAIN_KEY` is now optional and only useful for any future BeaconCha experiments. The default dashboard flow no longer depends on BeaconCha premium entity endpoints.
- `SIM_POOL_CONFIG_PATH` points the dashboard at a YAML or JSON pool definition.
- `SIM_TRACKER_DB_PATH` controls where live per-epoch snapshots are stored.
- `SIM_DASHBOARD_REFRESH_SECONDS` sets the default refresh interval shown in the UI.
- `CADLABS_REPO_URL` and `CADLABS_REPO_REF` control which CADLabs model checkout the container installs.

## Compose

Start the localhost dashboard service:

```bash
docker compose up --build simulator
```

The Compose file intentionally keeps separate images and Dockerfiles for `simulator` and `cadlabs` so the environments stay independent even though the simulator container now mounts the repo for local dashboard development.

## Streamlit Dashboard

The simulator service now includes a Streamlit webapp that layers a live UI over the existing `pool_tracker` backend for Ethereum Hoodi.

### What It Does

- reads validator balances and status from the Hoodi Beacon API
- aggregates validator-centric balances into pool-level NAV and PnL
- stores per-epoch validator and pool snapshots in SQLite
