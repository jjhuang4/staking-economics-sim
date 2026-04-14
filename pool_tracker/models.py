"""Dataclasses used throughout the pool tracker package."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass
class Pool:
    pool_id: str
    name: str
    fee_rate: float
    slash_pass_through: float
    validator_indices: List[int]
    contract_addresses: List[str]


@dataclass
class ValidatorSnapshot:
    validator_index: int
    epoch: int
    balance_gwei: int
    effective_balance_gwei: int
    status: str


@dataclass
class RewardBreakdown:
    validator_index: int
    epoch: int
    source_reward_gwei: int = 0
    target_reward_gwei: int = 0
    head_reward_gwei: int = 0
    proposal_reward_gwei: int = 0
    inactivity_penalty_gwei: int = 0
    slashing_penalty_gwei: int = 0


@dataclass
class PoolFlow:
    block_number: int
    tx_hash: str
    log_index: int
    timestamp: datetime
    flow_type: str
    amount_wei: int
    actor: Optional[str] = None


@dataclass
class PoolSnapshot:
    pool_id: str
    epoch: int
    total_validator_balance_gwei: int
    gross_rewards_gwei: int
    penalties_gwei: int
    slashing_losses_gwei: int
    fees_gwei: int
    net_rewards_gwei: int
    net_user_flow_wei: int
    nav_gwei: int
    total_shares: float
    share_price_gwei: float
    cumulative_pnl_gwei: int
