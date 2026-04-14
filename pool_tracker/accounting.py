"""Pure accounting helpers for pool snapshots."""

from __future__ import annotations

import math
from typing import Iterable

from .models import Pool, PoolFlow, PoolSnapshot, ValidatorSnapshot

GWEI_PER_ETH_WEI = 1_000_000_000


def wei_to_gwei_floor(amount_wei: int) -> int:
    """Convert wei to gwei using floor semantics."""

    return math.floor(amount_wei / GWEI_PER_ETH_WEI)


def compute_total_validator_balance_gwei(balance_map: dict[int, int]) -> int:
    """Sum validator balances expressed in gwei."""

    return sum(int(balance) for balance in balance_map.values())


def compute_epoch_delta_gwei(current_balances: dict[int, int], prior_balances: dict[int, int]) -> int:
    """Compute the total epoch-over-epoch balance delta in gwei."""

    return compute_total_validator_balance_gwei(current_balances) - compute_total_validator_balance_gwei(
        prior_balances
    )


def compute_fee_gwei(gross_rewards_gwei: int, fee_rate: float) -> int:
    """Compute pool fees on positive gross rewards."""

    if gross_rewards_gwei <= 0:
        return 0
    return math.floor(gross_rewards_gwei * fee_rate)


def compute_net_user_flow_wei(flows: list[PoolFlow]) -> int:
    """Aggregate net user capital flow in wei."""

    net_flow = 0
    for flow in flows:
        if flow.flow_type == "deposit":
            net_flow += flow.amount_wei
        elif flow.flow_type == "withdraw":
            net_flow -= flow.amount_wei
    return net_flow


def compute_total_shares(
    previous_total_shares: float,
    net_user_flow_wei: int,
    previous_share_price_gwei: float,
) -> float:
    """Update total shares from net user flows using the prior share price."""

    if previous_share_price_gwei <= 0:
        return max(previous_total_shares, 0.0)
    net_user_flow_gwei = wei_to_gwei_floor(net_user_flow_wei)
    share_delta = net_user_flow_gwei / previous_share_price_gwei
    return max(previous_total_shares + share_delta, 0.0)


def _status_lookup(snapshots: Iterable[ValidatorSnapshot]) -> dict[int, str]:
    return {snapshot.validator_index: snapshot.status.lower() for snapshot in snapshots}


def _has_slashed_transition(
    current_validator_snapshots: list[ValidatorSnapshot],
    prior_validator_snapshots: list[ValidatorSnapshot],
) -> bool:
    current_status = _status_lookup(current_validator_snapshots)
    prior_status = _status_lookup(prior_validator_snapshots)
    for validator_index, status in current_status.items():
        if "slashed" in status and "slashed" not in prior_status.get(validator_index, ""):
            return True
    return False


def build_pool_snapshot(
    *,
    pool: Pool,
    epoch: int,
    current_balances: dict[int, int],
    prior_balances: dict[int, int],
    flows: list[PoolFlow],
    previous_snapshot: PoolSnapshot | None,
    cumulative_net_user_flow_wei: int,
    current_validator_snapshots: list[ValidatorSnapshot] | None = None,
    prior_validator_snapshots: list[ValidatorSnapshot] | None = None,
) -> PoolSnapshot:
    """Build a per-epoch PoolSnapshot from normalized validator and flow data."""

    total_validator_balance_gwei = compute_total_validator_balance_gwei(current_balances)
    epoch_delta_gwei = compute_epoch_delta_gwei(current_balances, prior_balances)

    gross_rewards_gwei = epoch_delta_gwei if epoch_delta_gwei >= 0 else 0
    penalties_gwei = -epoch_delta_gwei if epoch_delta_gwei < 0 else 0

    slashing_losses_gwei = 0
    if penalties_gwei > 0 and current_validator_snapshots and prior_validator_snapshots:
        if _has_slashed_transition(current_validator_snapshots, prior_validator_snapshots):
            slashing_losses_gwei = penalties_gwei
            penalties_gwei = 0

    fees_gwei = compute_fee_gwei(gross_rewards_gwei, pool.fee_rate)
    net_rewards_gwei = gross_rewards_gwei - fees_gwei - penalties_gwei - slashing_losses_gwei
    net_user_flow_wei = compute_net_user_flow_wei(flows)
    nav_gwei = total_validator_balance_gwei
    cumulative_net_user_deposits_gwei = wei_to_gwei_floor(cumulative_net_user_flow_wei)

    if previous_snapshot is None:
        total_shares = float(max(cumulative_net_user_deposits_gwei, 0))
        share_price_gwei = 1.0 if total_shares > 0 else 0.0
    else:
        total_shares = compute_total_shares(
            previous_snapshot.total_shares,
            net_user_flow_wei,
            previous_snapshot.share_price_gwei,
        )
        share_price_gwei = nav_gwei / total_shares if total_shares > 0 else 0.0

    cumulative_pnl_gwei = nav_gwei - cumulative_net_user_deposits_gwei

    return PoolSnapshot(
        pool_id=pool.pool_id,
        epoch=epoch,
        total_validator_balance_gwei=total_validator_balance_gwei,
        gross_rewards_gwei=gross_rewards_gwei,
        penalties_gwei=penalties_gwei,
        slashing_losses_gwei=slashing_losses_gwei,
        fees_gwei=fees_gwei,
        net_rewards_gwei=net_rewards_gwei,
        net_user_flow_wei=net_user_flow_wei,
        nav_gwei=nav_gwei,
        total_shares=total_shares,
        share_price_gwei=share_price_gwei,
        cumulative_pnl_gwei=cumulative_pnl_gwei,
    )
