from __future__ import annotations

from simulator.behavior import ActionRecommendation
from simulator.live_dashboard_data import _build_behavior_projections
from pool_tracker.models import PoolSnapshot


def test_build_behavior_projections_splits_positive_delta_into_rewards_and_fees():
    pool_snapshot = PoolSnapshot(
        pool_id="pool-1",
        epoch=100,
        total_validator_balance_gwei=100,
        gross_rewards_gwei=0,
        penalties_gwei=0,
        slashing_losses_gwei=0,
        fees_gwei=0,
        net_rewards_gwei=0,
        net_user_flow_wei=0,
        nav_gwei=100,
        total_shares=100.0,
        share_price_gwei=1.0,
        cumulative_pnl_gwei=5,
        slot=3200,
    )
    recommendations = [
        ActionRecommendation(
            action="add_to_stake",
            expected_delta_gwei=9,
            confidence=0.8,
            risk_level="low",
            rationale="test",
        )
    ]

    projections = _build_behavior_projections(pool_snapshot, recommendations, fee_rate=0.10)

    assert len(projections) == 1
    projection = projections[0]
    assert projection.projection_slot == 3201
    assert projection.projection_epoch == 100
    assert projection.projected_nav_gwei == 109
    assert projection.projected_cumulative_pnl_gwei == 14
    assert projection.projected_net_rewards_gwei == 9
    assert projection.projected_penalties_gwei == 0
    assert projection.projected_fees_gwei == 1
    assert projection.projected_share_price_gwei == 1.09


def test_build_behavior_projections_routes_negative_delta_to_penalties():
    pool_snapshot = PoolSnapshot(
        pool_id="pool-1",
        epoch=100,
        total_validator_balance_gwei=100,
        gross_rewards_gwei=0,
        penalties_gwei=0,
        slashing_losses_gwei=0,
        fees_gwei=0,
        net_rewards_gwei=0,
        net_user_flow_wei=0,
        nav_gwei=100,
        total_shares=100.0,
        share_price_gwei=1.0,
        cumulative_pnl_gwei=5,
        slot=3200,
    )
    recommendations = [
        ActionRecommendation(
            action="nothing_at_stake_attack",
            expected_delta_gwei=-12,
            confidence=0.8,
            risk_level="extreme",
            rationale="test",
        )
    ]

    projections = _build_behavior_projections(pool_snapshot, recommendations, fee_rate=0.10)

    assert len(projections) == 1
    projection = projections[0]
    assert projection.projection_slot == 3201
    assert projection.projection_epoch == 100
    assert projection.projected_nav_gwei == 88
    assert projection.projected_cumulative_pnl_gwei == -7
    assert projection.projected_net_rewards_gwei == -12
    assert projection.projected_penalties_gwei == 12
    assert projection.projected_fees_gwei == 0
    assert projection.projected_share_price_gwei == 0.88
