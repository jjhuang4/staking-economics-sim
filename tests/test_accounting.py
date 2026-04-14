from __future__ import annotations

from pool_tracker.accounting import (
    build_pool_snapshot,
    compute_fee_gwei,
    compute_total_shares,
)
from pool_tracker.models import Pool, PoolSnapshot


def build_pool() -> Pool:
    return Pool(
        pool_id="pool-1",
        name="Pool 1",
        fee_rate=0.10,
        slash_pass_through=1.0,
        validator_indices=[1, 2],
        contract_addresses=["0x1111111111111111111111111111111111111111"],
    )


def test_fee_only_applies_on_positive_rewards():
    assert compute_fee_gwei(100, 0.10) == 10
    assert compute_fee_gwei(0, 0.10) == 0
    assert compute_fee_gwei(-100, 0.10) == 0


def test_negative_epoch_delta_becomes_penalty():
    snapshot = build_pool_snapshot(
        pool=build_pool(),
        epoch=2,
        current_balances={1: 90},
        prior_balances={1: 100},
        flows=[],
        previous_snapshot=PoolSnapshot(
            pool_id="pool-1",
            epoch=1,
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
            cumulative_pnl_gwei=0,
        ),
        cumulative_net_user_flow_wei=100_000_000_000,
    )
    assert snapshot.gross_rewards_gwei == 0
    assert snapshot.penalties_gwei == 10


def test_cumulative_pnl_math():
    snapshot = build_pool_snapshot(
        pool=build_pool(),
        epoch=1,
        current_balances={1: 1_200},
        prior_balances={},
        flows=[],
        previous_snapshot=None,
        cumulative_net_user_flow_wei=1_000 * 1_000_000_000,
    )
    assert snapshot.cumulative_pnl_gwei == 200


def test_initial_share_logic():
    snapshot = build_pool_snapshot(
        pool=build_pool(),
        epoch=0,
        current_balances={1: 500},
        prior_balances={},
        flows=[],
        previous_snapshot=None,
        cumulative_net_user_flow_wei=500 * 1_000_000_000,
    )
    assert snapshot.total_shares == 500.0
    assert snapshot.share_price_gwei == 1.0


def test_share_mint_burn_logic_from_net_flows():
    assert compute_total_shares(100.0, 20 * 1_000_000_000, 2.0) == 110.0
    assert compute_total_shares(100.0, -20 * 1_000_000_000, 2.0) == 90.0
