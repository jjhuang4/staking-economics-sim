# Staking Economics Workspace

This repository is organized as two separate Docker Compose services:

- `simulator/` is the modern Python environment for Hoodi analytics, plots, and custom staking-simulator logic.
- `cadlabs/` is a separate container for the CADLabs Ethereum Economic Model and must keep its own virtualenv, requirements flow, and notebook-oriented workflow.

Codex instructions for this repo:

- Keep the `simulator` and `cadlabs` environments separate.
- Do not merge CADLabs dependencies into the simulator image unless a later task explicitly requires it.
- Do not require Kurtosis, a local Hoodi node, or a wallet for read-only analytics.
- Support optional Hoodi execution RPC and Beacon API endpoints through environment variables.
- Use Docker Compose as the primary orchestration entrypoint because each service has its own build context and Dockerfile.

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

### Hoodi Highlights

- Hoodi is the Ethereum validator and staking-oriented testnet that replaces Holesky for this workflow.
- Hoodi Beacon is designed for protocol and validator testing with a permissionless validator set that more closely resembles mainnet than Beacon Sepolia.
- Alchemy lists Hoodi execution as chain ID `560048` with typical block times around `12-15s`.

### What It Shows

- current Beacon head or finalized epoch
- active validator count and validator status mix
- pool NAV, net rewards, penalties, share price, and cumulative PnL
- validator balance history and per-validator epoch-over-epoch gain or loss when a prior snapshot exists
- modeled next actions from `simulator/behavior.py`, including `add_to_stake`, `withdraw`, `wait`, and a theoretical `nothing_at_stake_attack`

### Live Data Limits

- Individual validator gain or loss is shown only as a prior-epoch balance delta inferred from standard Beacon balance reads.
- Reward source decomposition is not shown because the dashboard sticks to standard Beacon and execution reads available from the configured provider.
- The `nothing_at_stake_attack` row is displayed as a theoretical game-theory option only, with a slash-adjusted negative expected value and no operational guidance.

## Pool Tracker

The repo also includes a standalone Python package named `pool_tracker` for read-only Hoodi pool accounting. It is separate from both the custom simulator and the CADLabs model.

### What It Does

- reads validator balances and status from the Hoodi Beacon API
- aggregates validator-centric balances into pool-level NAV and PnL
- stores per-epoch validator and pool snapshots in SQLite
- exposes a CLI for syncing one epoch or a range of epochs
- leaves pool discovery and validator discovery manual in v1

### Environment Variables

Set these for `pool_tracker`:

```text
EXECUTION_RPC_URL=https://eth-hoodi.g.alchemy.com/v2/your-key
BEACON_API_URL=https://eth-hoodibeacon.g.alchemy.com/v2/your-key
```

The loader also accepts the simulator's `HOODI_EXECUTION_RPC_URL` and `HOODI_BEACON_API_URL` names for convenience.

Optional:

```text
POOL_TRACKER_DB_PATH=pool_tracker.db
```

### Example Pool Config

```yaml
pool_id: hoodi-pool-1
name: Hoodi Pool 1
fee_rate: 0.10
slash_pass_through: 1.0
validator_indices: [123, 456, 789]
contract_addresses:
  - "0x1111111111111111111111111111111111111111"
```

### CLI

Sync a single epoch:

```bash
python -m pool_tracker.cli sync-epoch --pool-config pool_config.yaml --epoch 12345
```

Sync a range:

```bash
python -m pool_tracker.cli sync-range --pool-config pool_config.yaml --start-epoch 12340 --end-epoch 12345
```

Read back a stored snapshot:

```bash
python -m pool_tracker.cli show-snapshot --pool-config pool_config.yaml --epoch 12345
```

### Unit Conventions

- Beacon validator balances are stored and computed in `gwei`.
- Execution-layer pool flows are stored in `wei`.
- Snapshot share price is expressed in `gwei`.
- Wei is converted to gwei with integer floor semantics when units are combined.

### V1 Limitations

- Hoodi only.
- Pools, validator indices, contract addresses, and event signatures are manually configured.
- No auto-discovery of pools or validators.
- No real-time subscriptions or dashboards.
- Exact reward decomposition is not attempted unless a provider exposes those endpoints.
- If execution block-range mapping is not configured, `sync_epoch()` will still work and will set execution net flows to zero.
- Share accounting is based on manual flow inputs and validator balance aggregation, not live share token supply discovery.
