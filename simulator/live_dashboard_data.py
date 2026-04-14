"""Backend helpers for the live Hoodi Streamlit dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import os
from statistics import mean

from pool_tracker.config import ConfigError, Settings, load_pool_config, resolve_env_value
from pool_tracker.execution_client import ExecutionClient
from pool_tracker.models import Pool, PoolSnapshot, ValidatorSnapshot
from pool_tracker.storage import SQLiteStorage
from pool_tracker.tracker import PoolTracker, SLOTS_PER_EPOCH
from pool_tracker.beacon_client import BeaconClient

try:
    from .behavior import ActionRecommendation, Behavior, PoolBehaviorContext
except ImportError:
    from behavior import ActionRecommendation, Behavior, PoolBehaviorContext

DEFAULT_HISTORY_EPOCHS = 32
PLACEHOLDER_VALIDATOR_INDICES = [123, 456, 789]
PLACEHOLDER_CONTRACT_ADDRESSES = ["0x1111111111111111111111111111111111111111"]


@dataclass(frozen=True)
class DashboardRuntimeConfig:
    """Runtime settings for the Streamlit dashboard."""

    pool_config_path: str
    db_path: str
    execution_rpc_url: str
    beacon_api_url: str
    history_epochs: int = DEFAULT_HISTORY_EPOCHS
    state_id: str = "head"


@dataclass(frozen=True)
class ValidatorDelta:
    """Current validator values plus an optional prior-epoch delta."""

    validator_index: int
    balance_gwei: int
    effective_balance_gwei: int
    status: str
    delta_gwei: int | None


@dataclass(frozen=True)
class LiveDashboardSnapshot:
    """All data needed to render the Hoodi dashboard."""

    refreshed_at: datetime
    pool: Pool
    current_epoch: int
    head_slot: int
    finalized_epoch: int
    chain_id: int
    execution_block_number: int
    pool_snapshot: PoolSnapshot
    pool_history: list[PoolSnapshot]
    current_validator_snapshots: list[ValidatorSnapshot]
    validator_history: dict[int, list[ValidatorSnapshot]]
    validator_deltas: list[ValidatorDelta]
    status_counts: dict[str, int]
    action_recommendations: list[ActionRecommendation]
    notes: list[str]


def _resolve_env(primary_name: str, fallback_name: str) -> str:
    return resolve_env_value(primary_name, fallback_name)


def load_dashboard_runtime_config(
    pool_config_path: str,
    db_path: str | None = None,
    execution_rpc_url: str | None = None,
    beacon_api_url: str | None = None,
    history_epochs: int = DEFAULT_HISTORY_EPOCHS,
    state_id: str = "head",
) -> DashboardRuntimeConfig:
    """Resolve dashboard runtime settings from args and environment variables."""

    resolved_execution_rpc_url = (execution_rpc_url or "").strip() or _resolve_env(
        "HOODI_EXECUTION_RPC_URL", "EXECUTION_RPC_URL"
    )
    resolved_beacon_api_url = (beacon_api_url or "").strip() or _resolve_env(
        "HOODI_BEACON_API_URL", "BEACON_API_URL"
    )
    resolved_db_path = (db_path or "").strip() or os.getenv(
        "SIM_TRACKER_DB_PATH",
        os.path.join(os.getenv("SIM_DATA_DIR", "shared/data"), "pool_tracker_live.db"),
    )

    if not pool_config_path.strip():
        raise ConfigError("A pool config path is required.")
    if not resolved_execution_rpc_url:
        raise ConfigError("HOODI_EXECUTION_RPC_URL or EXECUTION_RPC_URL is required for the dashboard.")
    if not resolved_beacon_api_url:
        raise ConfigError("HOODI_BEACON_API_URL or BEACON_API_URL is required for the dashboard.")
    if history_epochs < 1:
        raise ConfigError("history_epochs must be at least 1.")
    if state_id not in {"head", "finalized"}:
        raise ConfigError("state_id must be 'head' or 'finalized'.")

    return DashboardRuntimeConfig(
        pool_config_path=pool_config_path.strip(),
        db_path=resolved_db_path,
        execution_rpc_url=resolved_execution_rpc_url,
        beacon_api_url=resolved_beacon_api_url,
        history_epochs=history_epochs,
        state_id=state_id,
    )


def _build_tracker(runtime_config: DashboardRuntimeConfig) -> PoolTracker:
    settings = Settings(
        execution_rpc_url=runtime_config.execution_rpc_url,
        beacon_api_url=runtime_config.beacon_api_url,
        db_path=runtime_config.db_path,
        network="hoodi",
    )
    pool = load_pool_config(runtime_config.pool_config_path)
    beacon_client = BeaconClient(runtime_config.beacon_api_url)
    execution_client = ExecutionClient(runtime_config.execution_rpc_url)
    storage = SQLiteStorage(runtime_config.db_path)
    return PoolTracker(
        settings=settings,
        pool=pool,
        beacon_client=beacon_client,
        execution_client=execution_client,
        storage=storage,
    )


def _build_status_counts(snapshots: list[ValidatorSnapshot]) -> dict[str, int]:
    status_counts: dict[str, int] = {}
    for snapshot in snapshots:
        normalized_status = snapshot.status.strip().lower()
        status_counts[normalized_status] = status_counts.get(normalized_status, 0) + 1
    return status_counts


def _build_validator_deltas(
    current_snapshots: list[ValidatorSnapshot],
    prior_snapshots: list[ValidatorSnapshot],
) -> list[ValidatorDelta]:
    prior_map = {snapshot.validator_index: snapshot for snapshot in prior_snapshots}
    deltas: list[ValidatorDelta] = []
    for snapshot in current_snapshots:
        prior_snapshot = prior_map.get(snapshot.validator_index)
        delta_gwei = None
        if prior_snapshot is not None:
            delta_gwei = snapshot.balance_gwei - prior_snapshot.balance_gwei
        deltas.append(
            ValidatorDelta(
                validator_index=snapshot.validator_index,
                balance_gwei=snapshot.balance_gwei,
                effective_balance_gwei=snapshot.effective_balance_gwei,
                status=snapshot.status,
                delta_gwei=delta_gwei,
            )
        )
    return deltas


def _build_behavior_context(
    pool_snapshot: PoolSnapshot,
    pool_history: list[PoolSnapshot],
    current_validator_snapshots: list[ValidatorSnapshot],
) -> PoolBehaviorContext:
    active_validator_count = sum(
        1 for snapshot in current_validator_snapshots if snapshot.status.lower().startswith("active")
    )
    slashed_validator_count = sum(
        1 for snapshot in current_validator_snapshots if "slashed" in snapshot.status.lower()
    )
    trailing_rewards = [
        snapshot.net_rewards_gwei for snapshot in pool_history[-min(len(pool_history), 4) :]
    ] or [pool_snapshot.net_rewards_gwei]
    average_epoch_reward_gwei = int(mean(trailing_rewards))

    return PoolBehaviorContext(
        epoch=pool_snapshot.epoch,
        active_validator_count=active_validator_count,
        total_validator_count=len(current_validator_snapshots),
        total_balance_gwei=pool_snapshot.total_validator_balance_gwei,
        average_epoch_reward_gwei=average_epoch_reward_gwei,
        current_epoch_reward_gwei=pool_snapshot.net_rewards_gwei,
        current_epoch_penalty_gwei=pool_snapshot.penalties_gwei + pool_snapshot.slashing_losses_gwei,
        share_price_gwei=pool_snapshot.share_price_gwei,
        cumulative_pnl_gwei=pool_snapshot.cumulative_pnl_gwei,
        slashed_validator_count=slashed_validator_count,
    )


def fetch_live_dashboard_snapshot(runtime_config: DashboardRuntimeConfig) -> LiveDashboardSnapshot:
    """Fetch, persist, and return the latest live dashboard state."""

    tracker = _build_tracker(runtime_config)
    storage = tracker.storage
    try:
        head_slot = tracker.beacon_client.get_head_slot(block_id=runtime_config.state_id)
        current_epoch = head_slot // SLOTS_PER_EPOCH
        finalized_epoch = tracker.beacon_client.get_finalized_epoch()
        chain_id = tracker.execution_client.get_chain_id()
        execution_block_number = tracker.execution_client.get_latest_block_number()
        pool_snapshot = tracker.sync_epoch(current_epoch, state_id=runtime_config.state_id)

        current_validator_snapshots = storage.get_validator_snapshots_for_epoch(
            current_epoch,
            tracker.pool.validator_indices,
        )
        prior_validator_snapshots = (
            storage.get_validator_snapshots_for_epoch(current_epoch - 1, tracker.pool.validator_indices)
            if current_epoch > 0
            else []
        )
        pool_history = storage.list_pool_snapshots(
            tracker.pool.pool_id,
            limit=runtime_config.history_epochs,
        )
        validator_history = {
            validator_index: storage.list_validator_snapshots(
                validator_index,
                limit=runtime_config.history_epochs,
            )
            for validator_index in tracker.pool.validator_indices
        }

        status_counts = _build_status_counts(current_validator_snapshots)
        validator_deltas = _build_validator_deltas(current_validator_snapshots, prior_validator_snapshots)
        action_recommendations = Behavior.recommend_pool_actions(
            _build_behavior_context(pool_snapshot, pool_history, current_validator_snapshots)
        )

        notes = [
            "Validator gains and losses are inferred from prior-epoch balance deltas because standard Hoodi Beacon reads do not expose a full reward breakdown here.",
            "Pool user-flow accounting remains zero until manual event specs and epoch-to-block mapping are configured.",
        ]
        if tracker.pool.validator_indices == PLACEHOLDER_VALIDATOR_INDICES and [
            address.lower() for address in tracker.pool.contract_addresses
        ] == [address.lower() for address in PLACEHOLDER_CONTRACT_ADDRESSES]:
            notes.append(
                "The current pool configuration still uses placeholder validator indices and contract addresses. Replace them in pool_config.yaml to track a real Hoodi pool."
            )
        if not prior_validator_snapshots:
            notes.append(
                "No prior epoch snapshot is stored yet, so per-validator gain or loss will appear as unavailable until the next epoch."
            )

        return LiveDashboardSnapshot(
            refreshed_at=datetime.now(tz=UTC),
            pool=tracker.pool,
            current_epoch=current_epoch,
            head_slot=head_slot,
            finalized_epoch=finalized_epoch,
            chain_id=chain_id,
            execution_block_number=execution_block_number,
            pool_snapshot=pool_snapshot,
            pool_history=pool_history,
            current_validator_snapshots=current_validator_snapshots,
            validator_history=validator_history,
            validator_deltas=validator_deltas,
            status_counts=status_counts,
            action_recommendations=action_recommendations,
            notes=notes,
        )
    finally:
        storage.close()
