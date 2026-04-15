from __future__ import annotations

from datetime import UTC, datetime

from pool_tracker.models import Pool, PoolSnapshot, ValidatorSnapshot
from simulator.cadlabs_replication import CadLabsReplicationConfig, build_cadlabs_replication
from simulator.live_dashboard_data import (
    BehaviorProjection,
    LiveDashboardSnapshot,
    SlashSettings,
    ValidatorDelta,
    ValidatorLeaderboardRow,
)


def _build_snapshot() -> LiveDashboardSnapshot:
    pool = Pool(
        pool_id="pool-1",
        name="Pool 1",
        fee_rate=0.10,
        slash_pass_through=1.0,
        validator_indices=[1, 2, 3, 4],
        contract_addresses=[],
    )
    pool_history = [
        PoolSnapshot(
            pool_id="pool-1",
            epoch=100,
            total_validator_balance_gwei=128_000_000_000,
            gross_rewards_gwei=640_000_000,
            penalties_gwei=40_000_000,
            slashing_losses_gwei=0,
            fees_gwei=60_000_000,
            net_rewards_gwei=540_000_000,
            net_user_flow_wei=0,
            nav_gwei=128_000_000_000,
            total_shares=128_000_000_000.0,
            share_price_gwei=1.0,
            cumulative_pnl_gwei=540_000_000,
            slot=3200,
        ),
        PoolSnapshot(
            pool_id="pool-1",
            epoch=100,
            total_validator_balance_gwei=128_540_000_000,
            gross_rewards_gwei=720_000_000,
            penalties_gwei=20_000_000,
            slashing_losses_gwei=10_000_000,
            fees_gwei=72_000_000,
            net_rewards_gwei=618_000_000,
            net_user_flow_wei=0,
            nav_gwei=128_540_000_000,
            total_shares=128_000_000_000.0,
            share_price_gwei=1.00421875,
            cumulative_pnl_gwei=1_158_000_000,
            slot=3231,
        ),
    ]
    validator_snapshots = [
        ValidatorSnapshot(validator_index=1, epoch=100, balance_gwei=32_200_000_000, effective_balance_gwei=32_000_000_000, status="active_ongoing", slot=3231),
        ValidatorSnapshot(validator_index=2, epoch=100, balance_gwei=32_180_000_000, effective_balance_gwei=32_000_000_000, status="active_ongoing", slot=3231),
        ValidatorSnapshot(validator_index=3, epoch=100, balance_gwei=32_120_000_000, effective_balance_gwei=32_000_000_000, status="withdrawal_possible", slot=3231),
        ValidatorSnapshot(validator_index=4, epoch=100, balance_gwei=32_040_000_000, effective_balance_gwei=31_000_000_000, status="active_slashed", slot=3231),
    ]
    leaderboard_rows = [
        ValidatorLeaderboardRow(
            validator_index=1,
            public_key="0x01",
            status="active_ongoing",
            balance_gwei=32_200_000_000,
            effective_balance_gwei=32_000_000_000,
            deposit_gwei=96_000_000_000,
            withdrawal_gwei=0,
            proposer_slashings=0,
            attester_slashings=0,
            epoch_delta_gwei=14_000_000,
        ),
        ValidatorLeaderboardRow(
            validator_index=2,
            public_key="0x02",
            status="active_ongoing",
            balance_gwei=32_180_000_000,
            effective_balance_gwei=32_000_000_000,
            deposit_gwei=32_000_000_000,
            withdrawal_gwei=0,
            proposer_slashings=0,
            attester_slashings=0,
            epoch_delta_gwei=8_000_000,
        ),
        ValidatorLeaderboardRow(
            validator_index=3,
            public_key="0x03",
            status="withdrawal_possible",
            balance_gwei=32_120_000_000,
            effective_balance_gwei=32_000_000_000,
            deposit_gwei=0,
            withdrawal_gwei=32_000_000_000,
            proposer_slashings=0,
            attester_slashings=0,
            epoch_delta_gwei=-6_000_000,
        ),
        ValidatorLeaderboardRow(
            validator_index=4,
            public_key="0x04",
            status="active_slashed",
            balance_gwei=32_040_000_000,
            effective_balance_gwei=31_000_000_000,
            deposit_gwei=0,
            withdrawal_gwei=0,
            proposer_slashings=1,
            attester_slashings=0,
            epoch_delta_gwei=-18_000_000,
        ),
    ]

    return LiveDashboardSnapshot(
        refreshed_at=datetime.now(tz=UTC),
        pool=pool,
        current_epoch=100,
        head_slot=3231,
        finalized_slot=3231,
        finalized_epoch=100,
        chain_id=560048,
        execution_block_number=123,
        pool_snapshot=pool_history[-1],
        adjusted_pool_snapshot=pool_history[-1],
        pool_history=pool_history,
        adjusted_pool_history=pool_history,
        current_validator_snapshots=validator_snapshots,
        validator_history={},
        history_chart_validator_indices=[],
        validator_deltas=[
            ValidatorDelta(validator_index=1, balance_gwei=32_200_000_000, effective_balance_gwei=32_000_000_000, status="active_ongoing", delta_gwei=14_000_000),
            ValidatorDelta(validator_index=2, balance_gwei=32_180_000_000, effective_balance_gwei=32_000_000_000, status="active_ongoing", delta_gwei=8_000_000),
            ValidatorDelta(validator_index=3, balance_gwei=32_120_000_000, effective_balance_gwei=32_000_000_000, status="withdrawal_possible", delta_gwei=-6_000_000),
            ValidatorDelta(validator_index=4, balance_gwei=32_040_000_000, effective_balance_gwei=31_000_000_000, status="active_slashed", delta_gwei=-18_000_000),
        ],
        status_counts={"active_ongoing": 2, "withdrawal_possible": 1, "active_slashed": 1},
        action_recommendations=[],
        behavior_projections=[
            BehaviorProjection(
                action="wait",
                projection_slot=3232,
                projection_epoch=101,
                expected_delta_gwei=0,
                projected_nav_gwei=128_540_000_000,
                projected_cumulative_pnl_gwei=1_158_000_000,
                projected_net_rewards_gwei=0,
                projected_penalties_gwei=0,
                projected_fees_gwei=0,
                projected_share_price_gwei=1.00421875,
            )
        ],
        notes=[],
        methodology_notes=[],
        leaderboard_rows=leaderboard_rows,
        activity_window_start_slot=3200,
        activity_window_end_slot=3231,
        history_window_start_slot=3200,
        history_window_end_slot=3231,
        total_deposit_gwei=128_000_000_000,
        total_withdrawal_gwei=32_000_000_000,
        total_observed_slashings=1,
        slash_settings=SlashSettings(
            slash_pass_through=1.0,
            modeled_slashed_validators=0,
            modeled_slash_fraction=0.0,
        ),
    )


def test_cadlabs_replication_infers_adoption_and_keeps_revenue_yield_price_neutral():
    snapshot = _build_snapshot()

    low_price = build_cadlabs_replication(
        snapshot,
        CadLabsReplicationConfig(eth_price_usd=1_500.0, monthly_validator_cost_usd=15.0, projection_epochs=8),
    )
    high_price = build_cadlabs_replication(
        snapshot,
        CadLabsReplicationConfig(eth_price_usd=3_000.0, monthly_validator_cost_usd=15.0, projection_epochs=8),
    )

    assert low_price.inferred_adoption_validators_per_epoch == 4.0
    assert low_price.annualized_revenue_yield_pct == high_price.annualized_revenue_yield_pct
    assert high_price.annualized_profit_yield_pct > low_price.annualized_profit_yield_pct
    assert len(low_price.time_series) == 3 * 9


def test_cadlabs_replication_exposes_slashed_cohort():
    snapshot = _build_snapshot()

    replication = build_cadlabs_replication(
        snapshot,
        CadLabsReplicationConfig(eth_price_usd=2_500.0, monthly_validator_cost_usd=15.0, projection_epochs=8),
    )

    cohort_names = {item.cohort for item in replication.cohorts}

    assert "Slashed / impaired" in cohort_names
    assert "Accumulating" in cohort_names
    assert "Exiting / withdrawing" in cohort_names
    assert replication.notes
