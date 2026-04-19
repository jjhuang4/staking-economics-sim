"""Backend helpers for the live Hoodi Streamlit dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha1
import math
import os
from statistics import mean
from typing import Any

from pool_tracker.accounting import compute_fee_gwei
from api_layer.beacon import BeaconClient
from pool_tracker.config import ConfigError, resolve_env_value
from pool_tracker.execution_client import ExecutionClient
from pool_tracker.models import Pool, PoolSnapshot, ValidatorActivity, ValidatorActivitySummary, ValidatorSnapshot
from pool_tracker.storage import SQLiteStorage
from pool_tracker.tracker import SLOTS_PER_EPOCH

try:
    from .behavior import ActionRecommendation, Behavior, PoolBehaviorContext
except ImportError:
    from behavior import ActionRecommendation, Behavior, PoolBehaviorContext

DEFAULT_HISTORY_EPOCHS = 3
DEFAULT_ACTIVITY_LOOKBACK_EPOCHS = 16
DEFAULT_LEADERBOARD_LIMIT = 100
MAX_VALIDATOR_HISTORY_SERIES = 24
MAX_SLOT_HISTORY_EPOCHS = 3
MAX_SLOT_SNAPSHOTS_PER_REFRESH = 12
MAX_ACTIVITY_SLOTS_PER_REFRESH = 96

BASE_METHODS_NOTES = [
    "Live validator balances and statuses are read from the configured Hoodi Beacon API.",
    "The leaderboard is derived from finalized beacon blocks by summing actual deposit and withdrawal operations per validator over the selected lookback window.",
    "Slot-level NAV is directly observed from slot state snapshots; slot-level rewards, penalties, fees, and cumulative PnL are derived locally from consecutive slot-to-slot balance deltas for the tracked validator basket.",
    "Modeled Next Moves comes from simulator/behavior.py. It is a repo-local mixed-strategy or game-theory heuristic, not a live chain signal and not the CADLabs simulator.",
    "Behavior forecast markers extend the latest scenario-adjusted slot by one step and translate each modeled net delta into a fee-on-gains or penalty-on-loss point for charting transparency.",
    "The dedicated CADLabs-style tab locally replicates the published CADLabs validator revenue and profit yield equations from the live validator basket, but it is still not a direct cadCAD execution of the separate cadlabs/ container.",
]


@dataclass(frozen=True)
class DashboardRuntimeConfig:
    """Runtime settings for the Streamlit dashboard."""

    db_path: str
    execution_rpc_url: str
    beacon_api_url: str
    history_epochs: int = DEFAULT_HISTORY_EPOCHS
    activity_lookback_epochs: int = DEFAULT_ACTIVITY_LOOKBACK_EPOCHS
    leaderboard_limit: int = DEFAULT_LEADERBOARD_LIMIT
    fee_rate: float = 0.10
    slash_pass_through: float = 1.0
    modeled_slashed_validators: int = 0
    modeled_slash_fraction: float = 0.0
    state_id: str = "head"


@dataclass(frozen=True)
class ValidatorDelta:
    """Current validator values plus an optional prior-slot delta."""

    validator_index: int
    balance_gwei: int
    effective_balance_gwei: int
    status: str
    delta_gwei: int | None


@dataclass(frozen=True)
class SlashSettings:
    """User-adjustable slash settings applied to the aggregate validator basket."""

    slash_pass_through: float
    modeled_slashed_validators: int
    modeled_slash_fraction: float


@dataclass(frozen=True)
class ValidatorLeaderboardRow:
    """Leaderboard row combining recent flow activity with current validator state."""

    validator_index: int
    public_key: str
    status: str
    balance_gwei: int
    effective_balance_gwei: int
    deposit_gwei: int
    withdrawal_gwei: int
    proposer_slashings: int
    attester_slashings: int
    epoch_delta_gwei: int | None

    @property
    def total_activity_gwei(self) -> int:
        return self.deposit_gwei + self.withdrawal_gwei

    @property
    def net_flow_gwei(self) -> int:
        return self.deposit_gwei - self.withdrawal_gwei

    @property
    def total_slashings(self) -> int:
        return self.proposer_slashings + self.attester_slashings


@dataclass(frozen=True)
class BehaviorProjection:
    """Projected next-slot point for one modeled action."""

    action: str
    projection_slot: int
    projection_epoch: int
    expected_delta_gwei: int
    projected_nav_gwei: int
    projected_cumulative_pnl_gwei: int
    projected_net_rewards_gwei: int
    projected_penalties_gwei: int
    projected_fees_gwei: int
    projected_share_price_gwei: float


@dataclass(frozen=True)
class LiveDashboardSnapshot:
    """All data needed to render the validator-flow dashboard."""

    refreshed_at: datetime
    pool: Pool
    current_epoch: int
    head_slot: int
    finalized_slot: int
    finalized_epoch: int
    chain_id: int
    execution_block_number: int
    pool_snapshot: PoolSnapshot
    adjusted_pool_snapshot: PoolSnapshot
    pool_history: list[PoolSnapshot]
    adjusted_pool_history: list[PoolSnapshot]
    current_validator_snapshots: list[ValidatorSnapshot]
    validator_history: dict[int, list[ValidatorSnapshot]]
    history_chart_validator_indices: list[int]
    validator_deltas: list[ValidatorDelta]
    status_counts: dict[str, int]
    action_recommendations: list[ActionRecommendation]
    behavior_projections: list[BehaviorProjection]
    notes: list[str]
    methodology_notes: list[str]
    leaderboard_rows: list[ValidatorLeaderboardRow]
    activity_window_start_slot: int
    activity_window_end_slot: int
    history_window_start_slot: int
    history_window_end_slot: int
    total_deposit_gwei: int
    total_withdrawal_gwei: int
    total_observed_slashings: int
    slash_settings: SlashSettings


def _resolve_env(primary_name: str, fallback_name: str) -> str:
    return resolve_env_value(primary_name, fallback_name)


def _slot_window_size(history_epochs: int) -> int:
    return min(max(history_epochs, 1), MAX_SLOT_HISTORY_EPOCHS) * SLOTS_PER_EPOCH


def load_dashboard_runtime_config(
    *,
    db_path: str | None = None,
    execution_rpc_url: str | None = None,
    beacon_api_url: str | None = None,
    history_epochs: int = DEFAULT_HISTORY_EPOCHS,
    activity_lookback_epochs: int = DEFAULT_ACTIVITY_LOOKBACK_EPOCHS,
    leaderboard_limit: int = DEFAULT_LEADERBOARD_LIMIT,
    fee_rate: float = 0.10,
    slash_pass_through: float = 1.0,
    modeled_slashed_validators: int = 0,
    modeled_slash_fraction: float = 0.0,
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

    if not resolved_execution_rpc_url:
        raise ConfigError("HOODI_EXECUTION_RPC_URL or EXECUTION_RPC_URL is required for the dashboard.")
    if not resolved_beacon_api_url:
        raise ConfigError("HOODI_BEACON_API_URL or BEACON_API_URL is required for the dashboard.")
    if history_epochs < 1 or history_epochs > MAX_SLOT_HISTORY_EPOCHS:
        raise ConfigError(f"history_epochs must be between 1 and {MAX_SLOT_HISTORY_EPOCHS}.")
    if activity_lookback_epochs < 1:
        raise ConfigError("activity_lookback_epochs must be at least 1.")
    if leaderboard_limit < 1:
        raise ConfigError("leaderboard_limit must be at least 1.")
    if not 0.0 <= fee_rate <= 1.0:
        raise ConfigError("fee_rate must be between 0.0 and 1.0.")
    if slash_pass_through < 0.0:
        raise ConfigError("slash_pass_through must be non-negative.")
    if modeled_slashed_validators < 0:
        raise ConfigError("modeled_slashed_validators must be non-negative.")
    if modeled_slash_fraction < 0.0:
        raise ConfigError("modeled_slash_fraction must be non-negative.")
    if state_id not in {"head", "finalized"}:
        raise ConfigError("state_id must be 'head' or 'finalized'.")

    return DashboardRuntimeConfig(
        db_path=resolved_db_path,
        execution_rpc_url=resolved_execution_rpc_url,
        beacon_api_url=resolved_beacon_api_url,
        history_epochs=history_epochs,
        activity_lookback_epochs=activity_lookback_epochs,
        leaderboard_limit=leaderboard_limit,
        fee_rate=fee_rate,
        slash_pass_through=slash_pass_through,
        modeled_slashed_validators=modeled_slashed_validators,
        modeled_slash_fraction=modeled_slash_fraction,
        state_id=state_id,
    )

def _read_chain_state(runtime_config: DashboardRuntimeConfig) -> tuple[int, int, int, int, int]:
    beacon_client = BeaconClient(runtime_config.beacon_api_url)
    head_slot = beacon_client.get_head_slot(block_id=runtime_config.state_id)
    finalized_slot = beacon_client.get_head_slot(block_id="finalized")
    finalized_epoch = beacon_client.get_finalized_epoch()
    chain_id = 0
    execution_block_number = 0
    try:
        execution_client = ExecutionClient(runtime_config.execution_rpc_url)
        chain_id = execution_client.get_chain_id()
        execution_block_number = execution_client.get_latest_block_number()
    except Exception:
        # Execution RPC metadata is auxiliary for this dashboard, so keep serving
        # the Beacon-derived feed even when the execution endpoint is rate-limited.
        pass
    return head_slot, finalized_slot, finalized_epoch, chain_id, execution_block_number


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


def _pick_history_validator_indices(
    current_snapshots: list[ValidatorSnapshot],
    validator_deltas: list[ValidatorDelta],
    leaderboard_rows: list[ValidatorLeaderboardRow],
    *,
    limit: int = MAX_VALIDATOR_HISTORY_SERIES,
) -> list[int]:
    delta_map = {item.validator_index: abs(item.delta_gwei or 0) for item in validator_deltas}
    activity_map = {item.validator_index: item.total_activity_gwei for item in leaderboard_rows}
    prioritized = sorted(
        current_snapshots,
        key=lambda snapshot: (
            activity_map.get(snapshot.validator_index, 0),
            delta_map.get(snapshot.validator_index, 0),
            snapshot.balance_gwei,
            -snapshot.validator_index,
        ),
        reverse=True,
    )
    return [snapshot.validator_index for snapshot in prioritized[:limit]]


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
        snapshot.net_rewards_gwei for snapshot in pool_history[-min(len(pool_history), SLOTS_PER_EPOCH) :]
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


def _validator_metadata_by_pubkey(
    beacon_client: BeaconClient,
    pubkeys: list[str],
) -> dict[str, dict[str, Any]]:
    if not pubkeys:
        return {}
    validators = beacon_client.get_validators("head", pubkeys)
    metadata: dict[str, dict[str, Any]] = {}
    for index, item in validators.items():
        validator = item.get("validator", {})
        pubkey = str(validator.get("pubkey", "")).strip()
        if not pubkey:
            continue
        metadata[pubkey] = {
            "validator_index": index,
            "public_key": pubkey,
        }
    return metadata


def _validator_metadata_by_index(
    beacon_client: BeaconClient,
    validator_indices: list[int],
) -> dict[int, dict[str, Any]]:
    if not validator_indices:
        return {}
    validators = beacon_client.get_validators("head", validator_indices)
    metadata: dict[int, dict[str, Any]] = {}
    for index, item in validators.items():
        validator = item.get("validator", {})
        metadata[index] = {
            "validator_index": index,
            "public_key": str(validator.get("pubkey", "")).strip(),
        }
    return metadata


def _collect_attester_slashed_indices(attester_slashing: dict[str, Any]) -> list[int]:
    candidates: list[int] = []
    for key in ("attestation_1", "attestation_2"):
        attestation = attester_slashing.get(key, {})
        indices = attestation.get("attesting_indices", [])
        if isinstance(indices, list):
            for index in indices:
                try:
                    candidates.append(int(index))
                except (TypeError, ValueError):
                    continue
    return sorted(set(candidates))


def _parse_validator_activity(
    beacon_client: BeaconClient,
    block: dict[str, Any],
) -> list[ValidatorActivity]:
    message = block.get("message", {})
    body = message.get("body", {})
    slot = int(message.get("slot", 0))
    deposits = body.get("deposits", [])
    execution_payload = body.get("execution_payload", {})
    withdrawals = execution_payload.get("withdrawals", []) if isinstance(execution_payload, dict) else []
    proposer_slashings = body.get("proposer_slashings", [])
    attester_slashings = body.get("attester_slashings", [])

    deposit_pubkeys = []
    if isinstance(deposits, list):
        for deposit in deposits:
            pubkey = str(deposit.get("data", {}).get("pubkey", "")).strip()
            if pubkey:
                deposit_pubkeys.append(pubkey)

    withdrawal_indices = []
    if isinstance(withdrawals, list):
        for withdrawal in withdrawals:
            try:
                withdrawal_indices.append(int(withdrawal.get("validator_index")))
            except (TypeError, ValueError):
                continue

    proposer_indices = []
    if isinstance(proposer_slashings, list):
        for item in proposer_slashings:
            try:
                proposer_indices.append(int(item.get("signed_header_1", {}).get("message", {}).get("proposer_index")))
            except (TypeError, ValueError):
                continue

    attester_indices: list[int] = []
    if isinstance(attester_slashings, list):
        for item in attester_slashings:
            attester_indices.extend(_collect_attester_slashed_indices(item))

    pubkey_metadata = _validator_metadata_by_pubkey(beacon_client, sorted(set(deposit_pubkeys)))
    index_metadata = _validator_metadata_by_index(
        beacon_client,
        sorted(set(withdrawal_indices + proposer_indices + attester_indices)),
    )

    activities: dict[tuple[int, str], ValidatorActivity] = {}

    def ensure_activity(validator_index: int, public_key: str) -> ValidatorActivity:
        key = (validator_index, public_key)
        if key not in activities:
            activities[key] = ValidatorActivity(
                slot=slot,
                validator_index=validator_index,
                public_key=public_key,
            )
        return activities[key]

    for deposit in deposits if isinstance(deposits, list) else []:
        deposit_data = deposit.get("data", {})
        pubkey = str(deposit_data.get("pubkey", "")).strip()
        amount_gwei = int(deposit_data.get("amount", 0))
        metadata = pubkey_metadata.get(pubkey)
        if metadata is None:
            continue
        ensure_activity(metadata["validator_index"], metadata["public_key"]).deposit_gwei += amount_gwei

    for withdrawal in withdrawals if isinstance(withdrawals, list) else []:
        try:
            validator_index = int(withdrawal.get("validator_index"))
            amount_gwei = int(withdrawal.get("amount", 0))
        except (TypeError, ValueError):
            continue
        metadata = index_metadata.get(validator_index, {})
        pubkey = str(metadata.get("public_key", "")).strip()
        ensure_activity(validator_index, pubkey).withdrawal_gwei += amount_gwei

    for validator_index in proposer_indices:
        metadata = index_metadata.get(validator_index, {})
        pubkey = str(metadata.get("public_key", "")).strip()
        ensure_activity(validator_index, pubkey).proposer_slashings += 1

    for validator_index in attester_indices:
        metadata = index_metadata.get(validator_index, {})
        pubkey = str(metadata.get("public_key", "")).strip()
        ensure_activity(validator_index, pubkey).attester_slashings += 1

    return list(activities.values())


def _backfill_activity_window(
    runtime_config: DashboardRuntimeConfig,
    storage: SQLiteStorage,
    *,
    start_slot: int,
    end_slot: int,
) -> None:
    if end_slot < start_slot:
        return
    scanned_slots = storage.get_scanned_activity_slots(start_slot, end_slot)
    missing_slots = [slot for slot in range(start_slot, end_slot + 1) if slot not in scanned_slots]
    if len(missing_slots) > MAX_ACTIVITY_SLOTS_PER_REFRESH:
        missing_slots = missing_slots[-MAX_ACTIVITY_SLOTS_PER_REFRESH:]
    if not missing_slots:
        return

    beacon_client = BeaconClient(runtime_config.beacon_api_url)
    for slot in missing_slots:
        block = beacon_client.get_block(slot)
        with storage.transaction():
            if block is not None:
                for activity in _parse_validator_activity(beacon_client, block):
                    storage.upsert_validator_activity(activity, commit=False)
            storage.mark_activity_slot_scanned(slot, block_present=block is not None, commit=False)


def _build_activity_pool(runtime_config: DashboardRuntimeConfig) -> Pool:
    config_key = (
        f"lookback={runtime_config.activity_lookback_epochs}"
        f"|limit={runtime_config.leaderboard_limit}"
        f"|fee={runtime_config.fee_rate:.6f}"
        f"|state={runtime_config.state_id}"
    )
    digest = sha1(config_key.encode("utf-8")).hexdigest()[:12]
    return Pool(
        pool_id=f"hoodi-validator-activity-{digest}",
        name=f"Top {runtime_config.leaderboard_limit} Hoodi Validators By Deposit/Withdrawal Activity",
        fee_rate=runtime_config.fee_rate,
        slash_pass_through=runtime_config.slash_pass_through,
        validator_indices=[],
        contract_addresses=[],
    )


def _scenario_extra_slash_gwei(
    total_balance_gwei: int,
    validator_count: int,
    slash_settings: SlashSettings,
) -> int:
    if validator_count <= 0:
        return 0
    if slash_settings.modeled_slashed_validators <= 0 or slash_settings.modeled_slash_fraction <= 0:
        return 0
    affected_validators = min(validator_count, slash_settings.modeled_slashed_validators)
    average_balance_gwei = total_balance_gwei / validator_count
    modeled_loss = average_balance_gwei * affected_validators * slash_settings.modeled_slash_fraction
    return max(0, math.floor(modeled_loss))


def _has_slashed_transition(
    current_validator_snapshots: list[ValidatorSnapshot],
    prior_validator_snapshots: list[ValidatorSnapshot],
) -> bool:
    prior_status = {snapshot.validator_index: snapshot.status.lower() for snapshot in prior_validator_snapshots}
    for snapshot in current_validator_snapshots:
        current_status = snapshot.status.lower()
        if "slashed" in current_status and "slashed" not in prior_status.get(snapshot.validator_index, ""):
            return True
    return False


def _build_aggregate_pool_snapshot(
    *,
    pool: Pool,
    epoch: int,
    slot: int | None,
    current_validator_snapshots: list[ValidatorSnapshot],
    prior_validator_snapshots: list[ValidatorSnapshot],
    previous_snapshot: PoolSnapshot | None,
) -> PoolSnapshot:
    """Build a stable aggregate snapshot for the displayed leaderboard basket."""

    nav_gwei = sum(snapshot.balance_gwei for snapshot in current_validator_snapshots)
    if previous_snapshot is None:
        total_shares = float(nav_gwei) if nav_gwei > 0 else 0.0
        share_price_gwei = 1.0 if total_shares > 0 else 0.0
        return PoolSnapshot(
            pool_id=pool.pool_id,
            epoch=epoch,
            total_validator_balance_gwei=nav_gwei,
            gross_rewards_gwei=0,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=0,
            net_rewards_gwei=0,
            net_user_flow_wei=0,
            nav_gwei=nav_gwei,
            total_shares=total_shares,
            share_price_gwei=share_price_gwei,
            cumulative_pnl_gwei=0,
            slot=slot,
        )

    epoch_delta_gwei = nav_gwei - previous_snapshot.nav_gwei
    gross_rewards_gwei = max(epoch_delta_gwei, 0)
    penalties_gwei = max(-epoch_delta_gwei, 0)
    slashing_losses_gwei = 0
    if penalties_gwei > 0 and _has_slashed_transition(current_validator_snapshots, prior_validator_snapshots):
        slashing_losses_gwei = penalties_gwei
        penalties_gwei = 0
    fees_gwei = compute_fee_gwei(gross_rewards_gwei, pool.fee_rate)
    net_rewards_gwei = gross_rewards_gwei - fees_gwei - penalties_gwei - slashing_losses_gwei
    total_shares = previous_snapshot.total_shares
    share_price_gwei = nav_gwei / total_shares if total_shares > 0 else 0.0
    cumulative_pnl_gwei = previous_snapshot.cumulative_pnl_gwei + epoch_delta_gwei
    return PoolSnapshot(
        pool_id=pool.pool_id,
        epoch=epoch,
        total_validator_balance_gwei=nav_gwei,
        gross_rewards_gwei=gross_rewards_gwei,
        penalties_gwei=penalties_gwei,
        slashing_losses_gwei=slashing_losses_gwei,
        fees_gwei=fees_gwei,
        net_rewards_gwei=net_rewards_gwei,
        net_user_flow_wei=0,
        nav_gwei=nav_gwei,
        total_shares=total_shares,
        share_price_gwei=share_price_gwei,
        cumulative_pnl_gwei=cumulative_pnl_gwei,
        slot=slot,
    )


def _apply_slash_scenario(
    history: list[PoolSnapshot],
    *,
    validator_count: int,
    slash_settings: SlashSettings,
) -> list[PoolSnapshot]:
    adjusted: list[PoolSnapshot] = []
    cumulative_nav_adjustment = 0
    for snapshot in history:
        observed_user_loss_gwei = math.floor(snapshot.slashing_losses_gwei * slash_settings.slash_pass_through)
        reimbursement_gwei = snapshot.slashing_losses_gwei - observed_user_loss_gwei
        modeled_extra_gwei = _scenario_extra_slash_gwei(
            snapshot.total_validator_balance_gwei,
            validator_count,
            slash_settings,
        )
        cumulative_nav_adjustment += reimbursement_gwei - modeled_extra_gwei
        adjusted_nav_gwei = snapshot.nav_gwei + cumulative_nav_adjustment
        adjusted_net_rewards_gwei = (
            snapshot.gross_rewards_gwei
            - snapshot.fees_gwei
            - snapshot.penalties_gwei
            - observed_user_loss_gwei
            - modeled_extra_gwei
        )
        adjusted_cumulative_pnl_gwei = snapshot.cumulative_pnl_gwei + cumulative_nav_adjustment
        adjusted_share_price_gwei = (
            adjusted_nav_gwei / snapshot.total_shares if snapshot.total_shares > 0 else 0.0
        )
        adjusted.append(
            PoolSnapshot(
                pool_id=snapshot.pool_id,
                epoch=snapshot.epoch,
                total_validator_balance_gwei=snapshot.total_validator_balance_gwei,
                gross_rewards_gwei=snapshot.gross_rewards_gwei,
                penalties_gwei=snapshot.penalties_gwei,
                slashing_losses_gwei=observed_user_loss_gwei + modeled_extra_gwei,
                fees_gwei=snapshot.fees_gwei,
                net_rewards_gwei=adjusted_net_rewards_gwei,
                net_user_flow_wei=snapshot.net_user_flow_wei,
                nav_gwei=adjusted_nav_gwei,
                total_shares=snapshot.total_shares,
                share_price_gwei=adjusted_share_price_gwei,
                cumulative_pnl_gwei=adjusted_cumulative_pnl_gwei,
                slot=snapshot.slot,
            )
        )
    return adjusted


def _build_behavior_projections(
    pool_snapshot: PoolSnapshot,
    action_recommendations: list[ActionRecommendation],
    *,
    fee_rate: float,
) -> list[BehaviorProjection]:
    """Project the modeled next actions one slot ahead for chart overlays."""

    projections: list[BehaviorProjection] = []
    projection_slot = (pool_snapshot.slot + 1) if pool_snapshot.slot is not None else None
    projection_epoch = (
        projection_slot // SLOTS_PER_EPOCH if projection_slot is not None else pool_snapshot.epoch + 1
    )
    for recommendation in action_recommendations:
        expected_delta_gwei = recommendation.expected_delta_gwei
        projected_nav_gwei = max(0, pool_snapshot.nav_gwei + expected_delta_gwei)
        projected_cumulative_pnl_gwei = pool_snapshot.cumulative_pnl_gwei + expected_delta_gwei

        projected_penalties_gwei = 0
        projected_fees_gwei = 0
        projected_net_rewards_gwei = expected_delta_gwei
        if expected_delta_gwei >= 0:
            retained_rate = max(1.0 - fee_rate, 1e-9)
            gross_rewards_gwei = math.ceil(expected_delta_gwei / retained_rate)
            projected_fees_gwei = max(0, gross_rewards_gwei - expected_delta_gwei)
        else:
            projected_penalties_gwei = abs(expected_delta_gwei)

        projected_share_price_gwei = (
            projected_nav_gwei / pool_snapshot.total_shares if pool_snapshot.total_shares > 0 else 0.0
        )
        projections.append(
            BehaviorProjection(
                action=recommendation.action,
                projection_slot=projection_slot if projection_slot is not None else projection_epoch * SLOTS_PER_EPOCH,
                projection_epoch=projection_epoch,
                expected_delta_gwei=expected_delta_gwei,
                projected_nav_gwei=projected_nav_gwei,
                projected_cumulative_pnl_gwei=projected_cumulative_pnl_gwei,
                projected_net_rewards_gwei=projected_net_rewards_gwei,
                projected_penalties_gwei=projected_penalties_gwei,
                projected_fees_gwei=projected_fees_gwei,
                projected_share_price_gwei=projected_share_price_gwei,
            )
        )
    return projections


def _sync_slot_snapshot_window(
    runtime_config: DashboardRuntimeConfig,
    storage: SQLiteStorage,
    *,
    pool: Pool,
    validator_indices: list[int],
    start_slot: int,
    end_slot: int,
) -> list[ValidatorSnapshot]:
    """Backfill and refresh aggregate slot snapshots for the rolling chart window."""

    if end_slot < start_slot:
        return []
    beacon_client = BeaconClient(runtime_config.beacon_api_url)
    stored_slots = storage.get_pool_slot_snapshot_slots(pool.pool_id, start_slot, end_slot)
    slots_to_sync = sorted(set(range(start_slot, end_slot + 1)) - stored_slots)
    if len(slots_to_sync) > MAX_SLOT_SNAPSHOTS_PER_REFRESH:
        slots_to_sync = slots_to_sync[-MAX_SLOT_SNAPSHOTS_PER_REFRESH:]

    current_validator_snapshots: list[ValidatorSnapshot] = []
    for slot in slots_to_sync:
        epoch = slot // SLOTS_PER_EPOCH
        prior_snapshot = storage.get_latest_pool_slot_snapshot_before(pool.pool_id, slot)
        prior_validator_snapshots = storage.get_latest_validator_slot_snapshots_before(slot, validator_indices)
        try:
            fetched_validator_snapshots = beacon_client.build_validator_snapshots(
                epoch=epoch,
                state_id=str(slot),
                ids=validator_indices,
            )
        except Exception:
            if not prior_validator_snapshots:
                raise
            fetched_validator_snapshots = [
                ValidatorSnapshot(
                    validator_index=snapshot.validator_index,
                    epoch=epoch,
                    balance_gwei=snapshot.balance_gwei,
                    effective_balance_gwei=snapshot.effective_balance_gwei,
                    status=snapshot.status,
                )
                for snapshot in prior_validator_snapshots
            ]
        validator_snapshots = [
            ValidatorSnapshot(
                validator_index=snapshot.validator_index,
                epoch=epoch,
                balance_gwei=snapshot.balance_gwei,
                effective_balance_gwei=snapshot.effective_balance_gwei,
                status=snapshot.status,
                slot=slot,
            )
            for snapshot in fetched_validator_snapshots
        ]
        pool_snapshot = _build_aggregate_pool_snapshot(
            pool=pool,
            epoch=epoch,
            slot=slot,
            current_validator_snapshots=validator_snapshots,
            prior_validator_snapshots=prior_validator_snapshots,
            previous_snapshot=prior_snapshot,
        )
        with storage.transaction():
            for validator_snapshot in validator_snapshots:
                storage.upsert_validator_slot_snapshot(validator_snapshot, commit=False)
            storage.upsert_pool_slot_snapshot(pool_snapshot, commit=False)
        if slot == end_slot:
            current_validator_snapshots = validator_snapshots

    if not current_validator_snapshots:
        current_validator_snapshots = storage.get_validator_slot_snapshots_for_slot(end_slot, validator_indices)
    return current_validator_snapshots


def fetch_live_dashboard_snapshot(runtime_config: DashboardRuntimeConfig) -> LiveDashboardSnapshot:
    """Fetch, persist, and return the validator activity leaderboard dashboard state."""

    head_slot, finalized_slot, finalized_epoch, chain_id, execution_block_number = _read_chain_state(runtime_config)
    current_slot = head_slot
    current_epoch = current_slot // SLOTS_PER_EPOCH
    history_window_size = _slot_window_size(runtime_config.history_epochs)
    history_window_start_slot = max(0, current_slot - history_window_size + 1)
    history_window_end_slot = current_slot
    activity_window_start_slot = max(
        0,
        finalized_slot - (runtime_config.activity_lookback_epochs * SLOTS_PER_EPOCH) + 1,
    )
    activity_window_end_slot = finalized_slot

    storage = SQLiteStorage(runtime_config.db_path)
    try:
        _backfill_activity_window(
            runtime_config,
            storage,
            start_slot=activity_window_start_slot,
            end_slot=activity_window_end_slot,
        )
        activity_summaries = storage.list_validator_activity_summaries(
            activity_window_start_slot,
            activity_window_end_slot,
            limit=runtime_config.leaderboard_limit,
        )
        scanned_activity_slots = storage.get_scanned_activity_slots(
            activity_window_start_slot,
            activity_window_end_slot,
        )
        total_activity_window_slots = activity_window_end_slot - activity_window_start_slot + 1
        validator_indices = [row.validator_index for row in activity_summaries if row.validator_index >= 0]
        if not validator_indices:
            if len(scanned_activity_slots) < total_activity_window_slots:
                raise ConfigError(
                    "The finalized activity window is still warming from local cache. Wait for the next refresh so more slots can be scanned without overloading the provider."
                )
            raise ConfigError(
                "No validator deposit or withdrawal activity was found in the selected finalized-slot window. Increase the activity lookback and try again."
            )

        pool = _build_activity_pool(runtime_config)
        pool.validator_indices = list(validator_indices)
        current_validator_snapshots = _sync_slot_snapshot_window(
            runtime_config,
            storage,
            pool=pool,
            validator_indices=validator_indices,
            start_slot=history_window_start_slot,
            end_slot=history_window_end_slot,
        )
        prior_validator_snapshots = storage.get_latest_validator_slot_snapshots_before(
            current_slot,
            validator_indices,
        )
        pool_snapshot = storage.get_pool_slot_snapshot(pool.pool_id, current_slot)
        if pool_snapshot is None:
            raise ConfigError("Unable to build the current slot snapshot for the tracked validator basket.")

        validator_deltas = _build_validator_deltas(current_validator_snapshots, prior_validator_snapshots)
        delta_map = {item.validator_index: item.delta_gwei for item in validator_deltas}
        current_snapshot_map = {item.validator_index: item for item in current_validator_snapshots}
        leaderboard_rows = [
            ValidatorLeaderboardRow(
                validator_index=item.validator_index,
                public_key=item.public_key,
                status=current_snapshot_map.get(item.validator_index, ValidatorSnapshot(item.validator_index, current_epoch, 0, 0, "unknown")).status,
                balance_gwei=current_snapshot_map.get(item.validator_index, ValidatorSnapshot(item.validator_index, current_epoch, 0, 0, "unknown")).balance_gwei,
                effective_balance_gwei=current_snapshot_map.get(item.validator_index, ValidatorSnapshot(item.validator_index, current_epoch, 0, 0, "unknown")).effective_balance_gwei,
                deposit_gwei=item.deposit_gwei,
                withdrawal_gwei=item.withdrawal_gwei,
                proposer_slashings=item.proposer_slashings,
                attester_slashings=item.attester_slashings,
                epoch_delta_gwei=delta_map.get(item.validator_index),
            )
            for item in activity_summaries
        ]

        history_chart_validator_indices = _pick_history_validator_indices(
            current_validator_snapshots,
            validator_deltas,
            leaderboard_rows,
        )
        validator_history = {
            validator_index: storage.list_validator_slot_snapshots(
                validator_index,
                start_slot=history_window_start_slot,
                end_slot=history_window_end_slot,
            )
            for validator_index in history_chart_validator_indices
        }

        observed_history = storage.list_pool_slot_snapshots(
            pool.pool_id,
            start_slot=history_window_start_slot,
            end_slot=history_window_end_slot,
        )
        slash_settings = SlashSettings(
            slash_pass_through=runtime_config.slash_pass_through,
            modeled_slashed_validators=runtime_config.modeled_slashed_validators,
            modeled_slash_fraction=runtime_config.modeled_slash_fraction,
        )
        adjusted_history = _apply_slash_scenario(
            observed_history,
            validator_count=len(validator_indices),
            slash_settings=slash_settings,
        )
        pool_snapshot = observed_history[-1] if observed_history else pool_snapshot
        adjusted_snapshot = adjusted_history[-1] if adjusted_history else pool_snapshot
        action_recommendations = Behavior.recommend_pool_actions(
            _build_behavior_context(adjusted_snapshot, adjusted_history, current_validator_snapshots)
        )
        behavior_projections = _build_behavior_projections(
            adjusted_snapshot,
            action_recommendations,
            fee_rate=runtime_config.fee_rate,
        )

        total_deposit_gwei = sum(item.deposit_gwei for item in activity_summaries)
        total_withdrawal_gwei = sum(item.withdrawal_gwei for item in activity_summaries)
        total_observed_slashings = sum(item.total_slashings for item in leaderboard_rows)
        notes = [
            "The leaderboard ranks validators by total deposit plus withdrawal volume over the selected finalized-slot lookback window.",
            "Deposit and withdrawal activity comes from finalized beacon blocks exposed by the configured Hoodi Beacon endpoint, which keeps this flow on the Alchemy-backed free path instead of BeaconCha entity endpoints.",
            "Aggregate NAV is observed at slot granularity, while slot-level rewards, penalties, fees, and cumulative PnL are derived from consecutive slot-to-slot balance changes because the protocol settles ordinary accounting at epoch boundaries.",
            f"The rolling chart window covers up to {history_window_size} slots and advances one slot at a time as new slot states are read.",
        ]
        if len(scanned_activity_slots) < total_activity_window_slots:
            notes.append(
                f"The finalized activity lookback is still warming from local cache. {len(scanned_activity_slots)} of {total_activity_window_slots} slots have been scanned so far to keep API usage below provider limits."
            )
        if len(observed_history) <= 1:
            notes.append(
                "This validator basket was newly initialized on this refresh, so the first slot snapshot is normalized to a zero-PnL baseline instead of treating the full basket balance as reward."
            )
        if len(observed_history) < history_window_size:
            notes.append(
                f"The rolling slot window is still warming from local cache. {len(observed_history)} of {history_window_size} slot points are available right now to avoid overloading the provider."
            )
        if len(activity_summaries) < runtime_config.leaderboard_limit:
            notes.append(
                f"Only {len(activity_summaries)} validators had detectable deposit, withdrawal, or slashing activity in the selected window."
            )
        if len(current_validator_snapshots) > len(history_chart_validator_indices):
            notes.append(
                f"Validator slot history lines are limited to the {len(history_chart_validator_indices)} most active or most changed validators to keep the chart readable."
            )
        if runtime_config.modeled_slashed_validators > 0 and runtime_config.modeled_slash_fraction > 0:
            notes.append(
                "The scenario-adjusted charts include a hypothetical extra slashing stress based on the user-selected slash fraction and modeled slashed-validator count."
            )
        notes.append(
            "Because the leaderboard basket is refreshed from recent activity, membership can shift over time; the rolling slot history follows the live feed rather than freezing a fixed cohort."
        )

        return LiveDashboardSnapshot(
            refreshed_at=datetime.now(tz=UTC),
            pool=pool,
            current_epoch=current_epoch,
            head_slot=current_slot,
            finalized_slot=finalized_slot,
            finalized_epoch=finalized_epoch,
            chain_id=chain_id,
            execution_block_number=execution_block_number,
            pool_snapshot=pool_snapshot,
            adjusted_pool_snapshot=adjusted_snapshot,
            pool_history=observed_history,
            adjusted_pool_history=adjusted_history,
            current_validator_snapshots=current_validator_snapshots,
            validator_history=validator_history,
            history_chart_validator_indices=history_chart_validator_indices,
            validator_deltas=validator_deltas,
            status_counts=_build_status_counts(current_validator_snapshots),
            action_recommendations=action_recommendations,
            behavior_projections=behavior_projections,
            notes=notes,
            methodology_notes=list(BASE_METHODS_NOTES),
            leaderboard_rows=leaderboard_rows,
            activity_window_start_slot=activity_window_start_slot,
            activity_window_end_slot=activity_window_end_slot,
            history_window_start_slot=history_window_start_slot,
            history_window_end_slot=history_window_end_slot,
            total_deposit_gwei=total_deposit_gwei,
            total_withdrawal_gwei=total_withdrawal_gwei,
            total_observed_slashings=total_observed_slashings,
            slash_settings=slash_settings,
        )
    finally:
        storage.close()
