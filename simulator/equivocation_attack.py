"""High-level equivocation attack and accountable-safety visualizations."""

from __future__ import annotations

from dataclasses import dataclass
import math

import pandas as pd


@dataclass(frozen=True)
class EquivocationAttackConfig:
    """Inputs for the local equivocation-attack simulation."""

    total_validators: int
    attacker_fraction: float = 0.34
    honest_partition_fraction: float = 0.50
    finality_threshold: float = 2 / 3
    epochs: int = 8
    slash_detection_delay_epochs: int = 1
    slash_detection_fraction_per_epoch: float = 1.0
    slash_fraction_of_balance: float = 1.0
    validator_balance_eth: float = 32.0
    attack_sweep_max_fraction: float = 0.50
    attack_sweep_points: int = 26


@dataclass(frozen=True)
class EquivocationAttackSnapshot:
    """Computed outputs for the dashboard tab."""

    config: EquivocationAttackConfig
    summary: dict[str, float | int | bool | None]
    epoch_frame: pd.DataFrame
    sweep_frame: pd.DataFrame
    notes: tuple[str, ...]


def _accountable_safety_bound_fraction(finality_threshold: float) -> float:
    """Intersection bound for two conflicting supermajorities."""

    return max(0.0, min(1.0, (2.0 * finality_threshold) - 1.0))


def _branch_vote_shares(
    *,
    honest_validators: int,
    attacker_validators: int,
    honest_partition_fraction: float,
) -> tuple[float, float]:
    total_validators = honest_validators + attacker_validators
    if total_validators <= 0:
        return 0.0, 0.0

    honest_branch_a = honest_validators * honest_partition_fraction
    honest_branch_b = honest_validators - honest_branch_a
    branch_a_share = (honest_branch_a + attacker_validators) / total_validators
    branch_b_share = (honest_branch_b + attacker_validators) / total_validators
    return branch_a_share, branch_b_share


def _minimum_slashed_to_restore_safety(
    *,
    attacker_validators: int,
    total_validators: int,
    accountable_safety_bound_fraction: float,
) -> int:
    """Minimum number of attacker validators that must be removed to drop below the bound."""

    if (
        attacker_validators <= 0
        or total_validators <= 0
        or accountable_safety_bound_fraction <= 0
    ):
        return 0

    if accountable_safety_bound_fraction >= 1.0:
        return attacker_validators

    numerator = attacker_validators - (accountable_safety_bound_fraction * total_validators)
    if numerator < 0:
        return 0

    denominator = 1.0 - accountable_safety_bound_fraction
    required = math.floor(numerator / denominator) + 1
    return max(0, min(attacker_validators, required))


def _build_epoch_frame(config: EquivocationAttackConfig) -> pd.DataFrame:
    accountable_safety_bound_fraction = _accountable_safety_bound_fraction(
        config.finality_threshold
    )
    honest_validators = max(
        config.total_validators - round(config.total_validators * config.attacker_fraction),
        0,
    )
    attacker_validators = max(config.total_validators - honest_validators, 0)

    rows: list[dict[str, float | int | bool]] = []
    cumulative_slashed_validators = 0
    cumulative_burned_stake_eth = 0.0

    for epoch in range(max(config.epochs, 1) + 1):
        active_total = honest_validators + attacker_validators
        attacker_share = (
            attacker_validators / active_total if active_total > 0 else 0.0
        )
        branch_a_share, branch_b_share = _branch_vote_shares(
            honest_validators=honest_validators,
            attacker_validators=attacker_validators,
            honest_partition_fraction=config.honest_partition_fraction,
        )
        accountable_bound_validators = math.ceil(
            active_total * accountable_safety_bound_fraction
        )
        conflicting_finalization_possible = (
            attacker_validators > 0
            and branch_a_share >= config.finality_threshold
            and branch_b_share >= config.finality_threshold
        )
        minimum_slashed_to_restore_safety = _minimum_slashed_to_restore_safety(
            attacker_validators=attacker_validators,
            total_validators=active_total,
            accountable_safety_bound_fraction=accountable_safety_bound_fraction,
        )

        slashed_this_epoch = 0
        if epoch >= config.slash_detection_delay_epochs and attacker_validators > 0:
            slashed_this_epoch = math.ceil(
                attacker_validators * config.slash_detection_fraction_per_epoch
            )
            slashed_this_epoch = max(0, min(attacker_validators, slashed_this_epoch))

        post_attacker_validators = attacker_validators - slashed_this_epoch
        post_active_total = honest_validators + post_attacker_validators
        post_attacker_share = (
            post_attacker_validators / post_active_total if post_active_total > 0 else 0.0
        )
        post_branch_a_share, post_branch_b_share = _branch_vote_shares(
            honest_validators=honest_validators,
            attacker_validators=post_attacker_validators,
            honest_partition_fraction=config.honest_partition_fraction,
        )

        cumulative_slashed_validators += slashed_this_epoch
        cumulative_burned_stake_eth += (
            slashed_this_epoch
            * config.validator_balance_eth
            * config.slash_fraction_of_balance
        )

        rows.append(
            {
                "epoch": epoch,
                "active_validators_before_slash": active_total,
                "attacker_validators_before_slash": attacker_validators,
                "attacker_share_pct_before_slash": attacker_share * 100.0,
                "branch_a_vote_share_pct_before_slash": branch_a_share * 100.0,
                "branch_b_vote_share_pct_before_slash": branch_b_share * 100.0,
                "conflicting_finalization_possible": conflicting_finalization_possible,
                "accountable_safety_bound_validators": accountable_bound_validators,
                "accountable_safety_bound_pct": accountable_safety_bound_fraction * 100.0,
                "slashable_validators": attacker_validators,
                "minimum_slashed_to_restore_safety": minimum_slashed_to_restore_safety,
                "slashed_this_epoch": slashed_this_epoch,
                "cumulative_slashed_validators": cumulative_slashed_validators,
                "cumulative_burned_stake_eth": cumulative_burned_stake_eth,
                "attacker_validators_after_slash": post_attacker_validators,
                "attacker_share_pct_after_slash": post_attacker_share * 100.0,
                "branch_a_vote_share_pct_after_slash": post_branch_a_share * 100.0,
                "branch_b_vote_share_pct_after_slash": post_branch_b_share * 100.0,
                "safety_restored_after_slash": post_attacker_share
                < (accountable_safety_bound_fraction - 1e-12),
            }
        )

        attacker_validators = post_attacker_validators

    return pd.DataFrame(rows)


def _build_sweep_frame(config: EquivocationAttackConfig) -> pd.DataFrame:
    accountable_safety_bound_fraction = _accountable_safety_bound_fraction(
        config.finality_threshold
    )
    rows: list[dict[str, float | int | bool | str]] = []

    for index in range(max(config.attack_sweep_points, 2)):
        fraction = (
            config.attack_sweep_max_fraction * index / (max(config.attack_sweep_points, 2) - 1)
        )
        attacker_validators = round(config.total_validators * fraction)
        honest_validators = max(config.total_validators - attacker_validators, 0)
        branch_a_share, branch_b_share = _branch_vote_shares(
            honest_validators=honest_validators,
            attacker_validators=attacker_validators,
            honest_partition_fraction=config.honest_partition_fraction,
        )
        conflicting_finalization_possible = (
            attacker_validators > 0
            and branch_a_share >= config.finality_threshold
            and branch_b_share >= config.finality_threshold
        )
        accountable_bound_validators = math.ceil(
            config.total_validators * accountable_safety_bound_fraction
        )
        minimum_slashed_to_restore_safety = _minimum_slashed_to_restore_safety(
            attacker_validators=attacker_validators,
            total_validators=config.total_validators,
            accountable_safety_bound_fraction=accountable_safety_bound_fraction,
        )
        post_attackers = attacker_validators - minimum_slashed_to_restore_safety
        post_total = config.total_validators - minimum_slashed_to_restore_safety
        post_attacker_share = post_attackers / post_total if post_total > 0 else 0.0

        if not conflicting_finalization_possible:
            classification = "Below conflict threshold"
        elif minimum_slashed_to_restore_safety == 0:
            classification = "Conflict feasible"
        else:
            classification = "Conflict feasible, slashing restores safety"

        rows.append(
            {
                "attacker_share_pct": fraction * 100.0,
                "attacker_validators": attacker_validators,
                "branch_a_vote_share_pct": branch_a_share * 100.0,
                "branch_b_vote_share_pct": branch_b_share * 100.0,
                "conflicting_finalization_possible": conflicting_finalization_possible,
                "accountable_safety_bound_validators": accountable_bound_validators,
                "slashable_validators": attacker_validators,
                "minimum_slashed_to_restore_safety": minimum_slashed_to_restore_safety,
                "minimum_slashed_to_restore_safety_pct": (
                    minimum_slashed_to_restore_safety / config.total_validators * 100.0
                ),
                "post_response_attacker_share_pct": post_attacker_share * 100.0,
                "classification": classification,
            }
        )

    return pd.DataFrame(rows)


def build_equivocation_attack_snapshot(
    config: EquivocationAttackConfig,
) -> EquivocationAttackSnapshot:
    """Build a high-level view of equivocation risk, slashing, and safety restoration."""

    epoch_frame = _build_epoch_frame(config)
    sweep_frame = _build_sweep_frame(config)
    first_row = epoch_frame.iloc[0]
    restored_epochs = epoch_frame.loc[epoch_frame["safety_restored_after_slash"], "epoch"]
    first_restored_epoch = (
        int(restored_epochs.iloc[0]) if not restored_epochs.empty else None
    )
    final_row = epoch_frame.iloc[-1]

    summary = {
        "initial_attacker_validators": int(first_row["attacker_validators_before_slash"]),
        "initial_attacker_share_pct": float(first_row["attacker_share_pct_before_slash"]),
        "initial_conflicting_finalization_possible": bool(
            first_row["conflicting_finalization_possible"]
        ),
        "accountable_safety_bound_validators": int(
            first_row["accountable_safety_bound_validators"]
        ),
        "accountable_safety_bound_pct": float(first_row["accountable_safety_bound_pct"]),
        "initial_minimum_slashed_to_restore_safety": int(
            first_row["minimum_slashed_to_restore_safety"]
        ),
        "final_cumulative_slashed_validators": int(
            final_row["cumulative_slashed_validators"]
        ),
        "final_cumulative_burned_stake_eth": float(
            final_row["cumulative_burned_stake_eth"]
        ),
        "final_attacker_share_pct": float(final_row["attacker_share_pct_after_slash"]),
        "first_restored_epoch": first_restored_epoch,
    }

    notes = (
        "This tab is a high-level accountable-safety model, not an operational attack simulator.",
        "It assumes a partitioned honest set and an equivocating attacker cohort that signs both branches. That framing is used to visualize when conflicting finalization becomes feasible around the one-third slashable-stake threshold.",
        "The accountable-safety bound is shown as the minimum slashable stake implied by the supermajority intersection: for a 2/3 finality threshold, at least 1/3 of stake must be slashable for conflicting finalization to occur.",
        "Slashing response is modeled as progressively removing equivocators after a configurable detection delay. The charts highlight how many validators are slashable and how quickly the attacker share falls back below the accountable-safety bound.",
    )

    return EquivocationAttackSnapshot(
        config=config,
        summary=summary,
        epoch_frame=epoch_frame,
        sweep_frame=sweep_frame,
        notes=notes,
    )
