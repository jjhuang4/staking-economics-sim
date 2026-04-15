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
    slot: int | None = None


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
    slot: int | None = None


@dataclass
class EntitySummary:
    entity: str
    validator_count: int
    sub_entity_count: int = 0
    beaconscore: float | None = None
    net_share: float | None = None
    apr: float | None = None
    apy: float | None = None


@dataclass
class EntityValidator:
    entity: str
    validator_index: int
    public_key: str
    status: str
    balance_gwei: int
    effective_balance_gwei: int
    finality: str = "unknown"
    online: bool | None = None


@dataclass
class ValidatorRewardSnapshot:
    validator_index: int
    public_key: str
    epoch: int
    total_wei: int
    total_reward_wei: int
    total_penalty_wei: int
    total_missed_wei: int
    realized_loss_wei: int
    attestations_source_reward_wei: int = 0
    attestations_target_reward_wei: int = 0
    attestations_head_reward_wei: int = 0
    attestations_source_penalty_wei: int = 0
    attestations_target_penalty_wei: int = 0
    sync_reward_wei: int = 0
    sync_penalty_wei: int = 0
    slashing_reward_wei: int = 0
    slashing_penalty_wei: int = 0
    proposal_reward_cl_wei: int = 0
    proposal_reward_el_wei: int = 0
    proposal_missed_reward_cl_wei: int = 0
    proposal_missed_reward_el_wei: int = 0
    finality: str = "unknown"


@dataclass
class EntityValidatorSnapshot:
    entity: str
    snapshot_epoch: int
    reward_epoch: int
    validator_index: int
    public_key: str
    status: str
    balance_gwei: int
    effective_balance_gwei: int
    cumulative_reward_wei: int
    cumulative_penalty_wei: int
    cumulative_loss_wei: int
    tracking_start_epoch: int
    finality: str = "unknown"
    online: bool | None = None


@dataclass
class ValidatorActivity:
    slot: int
    validator_index: int
    public_key: str
    deposit_gwei: int = 0
    withdrawal_gwei: int = 0
    proposer_slashings: int = 0
    attester_slashings: int = 0


@dataclass
class ValidatorActivitySummary:
    validator_index: int
    public_key: str
    deposit_gwei: int
    withdrawal_gwei: int
    proposer_slashings: int = 0
    attester_slashings: int = 0

    @property
    def total_activity_gwei(self) -> int:
        return self.deposit_gwei + self.withdrawal_gwei

    @property
    def net_flow_gwei(self) -> int:
        return self.deposit_gwei - self.withdrawal_gwei

    @property
    def total_slashings(self) -> int:
        return self.proposer_slashings + self.attester_slashings
