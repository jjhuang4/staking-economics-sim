"""CADLabs-style yield replication seeded from the live validator basket."""

from __future__ import annotations

from dataclasses import dataclass
import math

from pool_tracker.tracker import SLOTS_PER_EPOCH

try:
    from .live_dashboard_data import LiveDashboardSnapshot, ValidatorLeaderboardRow
except ImportError:
    from live_dashboard_data import LiveDashboardSnapshot, ValidatorLeaderboardRow

SECONDS_PER_SLOT = 12
EPOCHS_PER_YEAR = (365 * 24 * 60 * 60) / (SECONDS_PER_SLOT * SLOTS_PER_EPOCH)
DEFAULT_ETH_PRICE_USD = 2500.0
DEFAULT_MONTHLY_VALIDATOR_COST_USD = 15.0
DEFAULT_PROJECTION_EPOCHS = 64
DEFAULT_STAKE_SWEEP_POINTS = 17
DEFAULT_PRICE_SWEEP_POINTS = 17


@dataclass(frozen=True)
class CadLabsReplicationConfig:
    """User-facing controls for the simplified CADLabs-style tab."""

    eth_price_usd: float = DEFAULT_ETH_PRICE_USD
    monthly_validator_cost_usd: float = DEFAULT_MONTHLY_VALIDATOR_COST_USD
    projection_epochs: int = DEFAULT_PROJECTION_EPOCHS
    stake_sweep_points: int = DEFAULT_STAKE_SWEEP_POINTS
    price_sweep_points: int = DEFAULT_PRICE_SWEEP_POINTS


@dataclass(frozen=True)
class CadLabsTimeSeriesPoint:
    """One time-series point for an adoption scenario."""

    scenario: str
    epoch: int
    projected_validators: float
    projected_staked_eth: float
    revenue_yield_pct: float
    profit_yield_pct: float


@dataclass(frozen=True)
class CadLabsSweepPoint:
    """One sweep point for either ETH staked or ETH price."""

    series: str
    x_value: float
    revenue_yield_pct: float
    profit_yield_pct: float


@dataclass(frozen=True)
class CadLabsSurfacePoint:
    """One point on the simplified profit-yield surface."""

    staked_eth: float
    eth_price_usd: float
    profit_yield_pct: float


@dataclass(frozen=True)
class CadLabsCohortSummary:
    """Empirical validator cohort used as a stand-in for environment buckets."""

    cohort: str
    validator_count: int
    share_pct: float
    active_share_pct: float
    avg_balance_eth: float
    avg_slot_delta_eth: float
    total_activity_eth: float
    slash_rate_pct: float
    reward_multiplier: float
    drag_multiplier: float
    cost_multiplier: float
    revenue_yield_pct: float
    profit_yield_pct: float


@dataclass(frozen=True)
class CadLabsCohortTimeSeriesPoint:
    """Projected cohort yield path under the normal adoption scenario."""

    cohort: str
    epoch: int
    revenue_yield_pct: float
    profit_yield_pct: float


@dataclass(frozen=True)
class CadLabsReplicationSnapshot:
    """Computed outputs for the CADLabs-style dashboard tab."""

    tracked_validators: int
    active_share_pct: float
    current_staked_eth: float
    average_balance_eth: float
    inferred_adoption_validators_per_epoch: float
    annualized_revenue_yield_pct: float
    annualized_net_yield_pct: float
    annualized_profit_yield_pct: float
    annualized_cost_yield_pct: float
    time_series: list[CadLabsTimeSeriesPoint]
    stake_sweep: list[CadLabsSweepPoint]
    price_sweep: list[CadLabsSweepPoint]
    profit_surface: list[CadLabsSurfacePoint]
    cohorts: list[CadLabsCohortSummary]
    cohort_time_series: list[CadLabsCohortTimeSeriesPoint]
    notes: list[str]


def _gwei_to_eth(value: int | float) -> float:
    return float(value) / 1_000_000_000


def _epochs_covered(snapshot: LiveDashboardSnapshot) -> float:
    slots_observed = snapshot.history_window_end_slot - snapshot.history_window_start_slot + 1
    return max(slots_observed / SLOTS_PER_EPOCH, 1.0 / SLOTS_PER_EPOCH)


def _annualized_yield_pct(per_epoch_eth: float, staked_eth: float) -> float:
    if staked_eth <= 0:
        return 0.0
    return (per_epoch_eth * EPOCHS_PER_YEAR / staked_eth) * 100.0


def _validator_cost_usd_per_epoch(
    monthly_validator_cost_usd: float,
    validator_count: float,
) -> float:
    if validator_count <= 0 or monthly_validator_cost_usd <= 0:
        return 0.0
    return (monthly_validator_cost_usd * max(validator_count, 0.0) * 12.0) / EPOCHS_PER_YEAR


def _cost_yield_pct(cost_usd_per_epoch: float, staked_eth: float, eth_price_usd: float) -> float:
    if staked_eth <= 0 or eth_price_usd <= 0:
        return 0.0
    return (cost_usd_per_epoch * EPOCHS_PER_YEAR / (staked_eth * eth_price_usd)) * 100.0


def _stake_sensitivity_factor(current_staked_eth: float, projected_staked_eth: float) -> float:
    if current_staked_eth <= 0 or projected_staked_eth <= 0:
        return 0.0
    return math.sqrt(current_staked_eth / projected_staked_eth)


def _activity_window_epochs(snapshot: LiveDashboardSnapshot) -> float:
    slots_observed = snapshot.activity_window_end_slot - snapshot.activity_window_start_slot + 1
    return max(slots_observed / SLOTS_PER_EPOCH, 1.0)


def _inferred_adoption_rate(snapshot: LiveDashboardSnapshot) -> float:
    deposit_eth = _gwei_to_eth(snapshot.total_deposit_gwei)
    return max(deposit_eth / 32.0 / _activity_window_epochs(snapshot), 0.0)


def _classify_validator_cohort(row: ValidatorLeaderboardRow) -> str:
    status = row.status.lower()
    if row.total_slashings > 0 or "slashed" in status:
        return "Slashed / impaired"
    if "withdraw" in status or "exit" in status or row.withdrawal_gwei > row.deposit_gwei:
        return "Exiting / withdrawing"
    if row.deposit_gwei > row.withdrawal_gwei:
        return "Accumulating"
    return "Stable active"


def _build_cadlabs_cohorts(
    snapshot: LiveDashboardSnapshot,
    *,
    base_revenue_yield_pct: float,
    base_net_yield_pct: float,
    base_cost_yield_pct: float,
    projection_epochs: int,
    inferred_adoption_rate: float,
) -> tuple[list[CadLabsCohortSummary], list[CadLabsCohortTimeSeriesPoint]]:
    rows = snapshot.leaderboard_rows
    if not rows:
        return [], []

    grouped: dict[str, list[ValidatorLeaderboardRow]] = {}
    for row in rows:
        grouped.setdefault(_classify_validator_cohort(row), []).append(row)

    total_count = len(rows)
    global_positive_delta = sum(max(row.epoch_delta_gwei or 0, 0) for row in rows) / max(total_count, 1)
    global_negative_delta = sum(
        max(-(row.epoch_delta_gwei or 0), 0) + (row.total_slashings * 1_000_000_000) for row in rows
    ) / max(total_count, 1)
    global_activity = sum(row.total_activity_gwei for row in rows) / max(total_count, 1)
    base_drag_yield_pct = max(base_revenue_yield_pct - base_net_yield_pct, 0.0)
    epsilon = 1.0

    summaries: list[CadLabsCohortSummary] = []
    time_series: list[CadLabsCohortTimeSeriesPoint] = []

    current_staked_eth = sum(_gwei_to_eth(row.balance_gwei) for row in rows)
    average_balance_eth = current_staked_eth / max(total_count, 1)
    normal_adoption_multiplier = 1.0

    for cohort_name, cohort_rows in grouped.items():
        cohort_count = len(cohort_rows)
        cohort_share = cohort_count / max(total_count, 1)
        active_count = sum(1 for row in cohort_rows if row.status.lower().startswith("active"))
        active_share = active_count / max(cohort_count, 1)
        avg_balance_eth = sum(_gwei_to_eth(row.balance_gwei) for row in cohort_rows) / max(cohort_count, 1)
        avg_slot_delta_eth = sum(_gwei_to_eth(row.epoch_delta_gwei or 0) for row in cohort_rows) / max(
            cohort_count, 1
        )
        total_activity_eth = sum(_gwei_to_eth(row.total_activity_gwei) for row in cohort_rows)
        slash_count = sum(
            1 for row in cohort_rows if row.total_slashings > 0 or "slashed" in row.status.lower()
        )
        slash_rate = slash_count / max(cohort_count, 1)

        positive_delta = sum(max(row.epoch_delta_gwei or 0, 0) for row in cohort_rows) / max(cohort_count, 1)
        negative_delta = sum(
            max(-(row.epoch_delta_gwei or 0), 0) + (row.total_slashings * 1_000_000_000)
            for row in cohort_rows
        ) / max(cohort_count, 1)
        average_activity = sum(row.total_activity_gwei for row in cohort_rows) / max(cohort_count, 1)

        reward_multiplier = (positive_delta + epsilon) / (global_positive_delta + epsilon)
        reward_multiplier *= max(active_share, 0.45)
        reward_multiplier = min(max(reward_multiplier, 0.25), 1.75)

        drag_multiplier = (negative_delta + epsilon) / (global_negative_delta + epsilon)
        drag_multiplier = max(drag_multiplier, 1.0 + slash_rate)
        drag_multiplier = min(max(drag_multiplier, 0.50), 2.50)

        cost_multiplier = 0.80 + 0.35 * ((average_activity + epsilon) / (global_activity + epsilon))
        cost_multiplier += 0.50 * slash_rate
        cost_multiplier = min(max(cost_multiplier, 0.60), 2.25)

        cohort_revenue_yield_pct = base_revenue_yield_pct * reward_multiplier
        cohort_profit_yield_pct = (
            cohort_revenue_yield_pct
            - (base_drag_yield_pct * drag_multiplier)
            - (base_cost_yield_pct * cost_multiplier)
        )

        summaries.append(
            CadLabsCohortSummary(
                cohort=cohort_name,
                validator_count=cohort_count,
                share_pct=cohort_share * 100.0,
                active_share_pct=active_share * 100.0,
                avg_balance_eth=avg_balance_eth,
                avg_slot_delta_eth=avg_slot_delta_eth,
                total_activity_eth=total_activity_eth,
                slash_rate_pct=slash_rate * 100.0,
                reward_multiplier=reward_multiplier,
                drag_multiplier=drag_multiplier,
                cost_multiplier=cost_multiplier,
                revenue_yield_pct=cohort_revenue_yield_pct,
                profit_yield_pct=cohort_profit_yield_pct,
            )
        )

        for epoch_offset in range(projection_epochs + 1):
            projected_validators = max(
                float(total_count) + (inferred_adoption_rate * normal_adoption_multiplier * epoch_offset),
                1.0,
            )
            projected_staked_eth = projected_validators * max(average_balance_eth, 0.001)
            stake_factor = _stake_sensitivity_factor(current_staked_eth, projected_staked_eth)
            projected_revenue_yield_pct = cohort_revenue_yield_pct * stake_factor
            projected_profit_yield_pct = (
                projected_revenue_yield_pct
                - (base_drag_yield_pct * drag_multiplier * stake_factor)
                - (base_cost_yield_pct * cost_multiplier)
            )
            time_series.append(
                CadLabsCohortTimeSeriesPoint(
                    cohort=cohort_name,
                    epoch=snapshot.current_epoch + epoch_offset,
                    revenue_yield_pct=projected_revenue_yield_pct,
                    profit_yield_pct=projected_profit_yield_pct,
                )
            )

    summaries.sort(key=lambda item: item.validator_count, reverse=True)
    return summaries, time_series


def build_cadlabs_replication(
    snapshot: LiveDashboardSnapshot,
    config: CadLabsReplicationConfig,
) -> CadLabsReplicationSnapshot:
    """Replicate the CADLabs revenue and profit yield notebook with local assumptions."""

    tracked_validators = len(snapshot.current_validator_snapshots)
    current_staked_eth = _gwei_to_eth(snapshot.adjusted_pool_snapshot.nav_gwei)
    average_balance_eth = current_staked_eth / max(tracked_validators, 1)
    active_count = sum(
        1 for validator in snapshot.current_validator_snapshots if validator.status.lower().startswith("active")
    )
    active_share_pct = (active_count / max(tracked_validators, 1)) * 100.0

    epochs_covered = _epochs_covered(snapshot)
    gross_rewards_eth_per_epoch = (
        sum(_gwei_to_eth(item.gross_rewards_gwei) for item in snapshot.adjusted_pool_history) / epochs_covered
    )
    net_rewards_eth_per_epoch = (
        sum(_gwei_to_eth(item.net_rewards_gwei) for item in snapshot.adjusted_pool_history) / epochs_covered
    )

    annualized_revenue_yield_pct = _annualized_yield_pct(gross_rewards_eth_per_epoch, current_staked_eth)
    annualized_net_yield_pct = _annualized_yield_pct(net_rewards_eth_per_epoch, current_staked_eth)
    cost_usd_per_epoch = _validator_cost_usd_per_epoch(
        config.monthly_validator_cost_usd,
        tracked_validators,
    )
    annualized_cost_yield_pct = _cost_yield_pct(cost_usd_per_epoch, current_staked_eth, config.eth_price_usd)
    annualized_profit_yield_pct = annualized_net_yield_pct - annualized_cost_yield_pct
    inferred_adoption_rate = _inferred_adoption_rate(snapshot)

    time_series: list[CadLabsTimeSeriesPoint] = []
    scenario_multipliers = {
        "Low adoption": 0.5,
        "Normal adoption": 1.0,
        "High adoption": 1.5,
    }
    for scenario_name, multiplier in scenario_multipliers.items():
        for epoch_offset in range(config.projection_epochs + 1):
            projected_validators = max(tracked_validators + (inferred_adoption_rate * multiplier * epoch_offset), 1.0)
            projected_staked_eth = projected_validators * max(average_balance_eth, 0.001)
            stake_factor = _stake_sensitivity_factor(current_staked_eth, projected_staked_eth)
            projected_revenue_yield_pct = annualized_revenue_yield_pct * stake_factor
            projected_net_yield_pct = annualized_net_yield_pct * stake_factor
            projected_cost_yield_pct = _cost_yield_pct(
                _validator_cost_usd_per_epoch(config.monthly_validator_cost_usd, projected_validators),
                projected_staked_eth,
                config.eth_price_usd,
            )
            time_series.append(
                CadLabsTimeSeriesPoint(
                    scenario=scenario_name,
                    epoch=snapshot.current_epoch + epoch_offset,
                    projected_validators=projected_validators,
                    projected_staked_eth=projected_staked_eth,
                    revenue_yield_pct=projected_revenue_yield_pct,
                    profit_yield_pct=projected_net_yield_pct - projected_cost_yield_pct,
                )
            )

    stake_sweep: list[CadLabsSweepPoint] = []
    if current_staked_eth > 0:
        for index in range(config.stake_sweep_points):
            fraction = index / max(config.stake_sweep_points - 1, 1)
            projected_staked_eth = current_staked_eth * (0.5 + fraction)
            projected_validators = projected_staked_eth / max(average_balance_eth, 0.001)
            stake_factor = _stake_sensitivity_factor(current_staked_eth, projected_staked_eth)
            projected_revenue_yield_pct = annualized_revenue_yield_pct * stake_factor
            projected_net_yield_pct = annualized_net_yield_pct * stake_factor
            projected_cost_yield_pct = _cost_yield_pct(
                _validator_cost_usd_per_epoch(config.monthly_validator_cost_usd, projected_validators),
                projected_staked_eth,
                config.eth_price_usd,
            )
            stake_sweep.append(
                CadLabsSweepPoint(
                    series="ETH staked sweep",
                    x_value=projected_staked_eth,
                    revenue_yield_pct=projected_revenue_yield_pct,
                    profit_yield_pct=projected_net_yield_pct - projected_cost_yield_pct,
                )
            )

    price_sweep: list[CadLabsSweepPoint] = []
    price_min = max(config.eth_price_usd * 0.4, 250.0)
    price_max = max(config.eth_price_usd * 1.8, price_min + 1.0)
    for index in range(config.price_sweep_points):
        fraction = index / max(config.price_sweep_points - 1, 1)
        eth_price_usd = price_min + ((price_max - price_min) * fraction)
        projected_cost_yield_pct = _cost_yield_pct(cost_usd_per_epoch, current_staked_eth, eth_price_usd)
        price_sweep.append(
            CadLabsSweepPoint(
                series="ETH price sweep",
                x_value=eth_price_usd,
                revenue_yield_pct=annualized_revenue_yield_pct,
                profit_yield_pct=annualized_net_yield_pct - projected_cost_yield_pct,
            )
        )

    profit_surface: list[CadLabsSurfacePoint] = []
    for stake_point in stake_sweep:
        projected_validators = stake_point.x_value / max(average_balance_eth, 0.001)
        for price_point in price_sweep:
            projected_cost_yield_pct = _cost_yield_pct(
                _validator_cost_usd_per_epoch(config.monthly_validator_cost_usd, projected_validators),
                stake_point.x_value,
                price_point.x_value,
            )
            stake_factor = _stake_sensitivity_factor(current_staked_eth, stake_point.x_value)
            projected_net_yield_pct = annualized_net_yield_pct * stake_factor
            profit_surface.append(
                CadLabsSurfacePoint(
                    staked_eth=stake_point.x_value,
                    eth_price_usd=price_point.x_value,
                    profit_yield_pct=projected_net_yield_pct - projected_cost_yield_pct,
                )
            )

    cohorts, cohort_time_series = _build_cadlabs_cohorts(
        snapshot,
        base_revenue_yield_pct=annualized_revenue_yield_pct,
        base_net_yield_pct=annualized_net_yield_pct,
        base_cost_yield_pct=annualized_cost_yield_pct,
        projection_epochs=config.projection_epochs,
        inferred_adoption_rate=inferred_adoption_rate,
    )

    notes = [
        "This tab is a local CADLabs-style replication, not a direct cadCAD run inside the separate cadlabs container.",
        "Revenue, profit, and annualized yield follow the published CADLabs notebook formulas, while the adoption scenarios are seeded from observed validator deposits in the live leaderboard basket.",
        "ETH-staked sensitivity is approximated with an inverse-square-root dilution curve so higher projected stake lowers annualized yield, which keeps the direction of the notebook-style scenarios without claiming protocol-exact issuance.",
        "Validator-environment lines are represented by empirical cohorts inferred from live flow, status, and slashing data: stable active, accumulating, exiting/withdrawing, and slashed/impaired.",
    ]
    if abs(snapshot.slash_settings.slash_pass_through - 1.0) > 1e-9 or snapshot.slash_settings.modeled_slashed_validators > 0:
        notes.append(
            "The underlying basket uses the current scenario-adjusted slash settings from the main dashboard, so this tab reflects those user-selected losses rather than raw observed chain results alone."
        )

    return CadLabsReplicationSnapshot(
        tracked_validators=tracked_validators,
        active_share_pct=active_share_pct,
        current_staked_eth=current_staked_eth,
        average_balance_eth=average_balance_eth,
        inferred_adoption_validators_per_epoch=inferred_adoption_rate,
        annualized_revenue_yield_pct=annualized_revenue_yield_pct,
        annualized_net_yield_pct=annualized_net_yield_pct,
        annualized_profit_yield_pct=annualized_profit_yield_pct,
        annualized_cost_yield_pct=annualized_cost_yield_pct,
        time_series=time_series,
        stake_sweep=stake_sweep,
        price_sweep=price_sweep,
        profit_surface=profit_surface,
        cohorts=cohorts,
        cohort_time_series=cohort_time_series,
        notes=notes,
    )
