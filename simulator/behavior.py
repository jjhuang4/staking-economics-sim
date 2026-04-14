"""Game-theory-based validator behavior helpers."""

from __future__ import annotations

from dataclasses import dataclass

try:
    import nashpy as nash
    import numpy as np
except ImportError:
    nash = None
    np = None

try:
    from .validator import ValidatorState
except ImportError:
    from validator import ValidatorState


@dataclass(frozen=True)
class PoolBehaviorContext:
    """Inputs used to score pool-level next actions from live Hoodi data."""

    epoch: int
    active_validator_count: int
    total_validator_count: int
    total_balance_gwei: int
    average_epoch_reward_gwei: int
    current_epoch_reward_gwei: int
    current_epoch_penalty_gwei: int
    share_price_gwei: float
    cumulative_pnl_gwei: int
    slashed_validator_count: int = 0


@dataclass(frozen=True)
class ActionRecommendation:
    """A frontend-friendly action recommendation and its modeled payoff."""

    action: str
    expected_delta_gwei: int
    confidence: float
    risk_level: str
    rationale: str
    category: str = "pool"
    caution: str | None = None


class Behavior:
    """Encapsulate validator and pool-level strategy decisions."""

    def __init__(self, validator_state: ValidatorState):
        self.validator_state = validator_state

    def decide_action(self, current_epoch: int) -> str:
        """Decide a validator action for the current epoch."""

        _ = current_epoch
        if not self.validator_state.is_active():
            return "remain_idle"

        action_probabilities = self.compute_action_probabilities()
        actions = ["propose_block", "attest", "remain_idle"]
        if np is None:
            return actions[max(range(len(actions)), key=lambda index: action_probabilities[index])]
        return str(np.random.choice(actions, p=action_probabilities))

    def compute_action_probabilities(self) -> list[float] | np.ndarray:
        """Compute simple mixed-strategy probabilities for simulation actions."""

        if nash is None or np is None:
            return [0.1, 0.6, 0.3]

        reward_coop = 10
        penalty_defect = -5
        suckers_payoff = -10

        row_payoffs = np.array(
            [
                [reward_coop, suckers_payoff],
                [penalty_defect, 0],
            ]
        )
        column_payoffs = np.array(
            [
                [reward_coop, penalty_defect],
                [suckers_payoff, 0],
            ]
        )

        game = nash.Game(row_payoffs, column_payoffs)
        equilibria = list(game.support_enumeration())
        if equilibria:
            mixed_strategy = equilibria[0][0]
            prob_attest = float(mixed_strategy[0])
            prob_idle = float(mixed_strategy[1])
            prob_propose = 0.1
            total = prob_attest + prob_idle + prob_propose
            return np.array([prob_propose / total, prob_attest / total, prob_idle / total])
        return np.array([0.1, 0.6, 0.3])

    @staticmethod
    def _cooperate_vs_deviate_mix(context: PoolBehaviorContext) -> tuple[float, float]:
        """Estimate cooperative versus deviant strategy weights from live pool data."""

        participation_rate = (
            context.active_validator_count / context.total_validator_count
            if context.total_validator_count > 0
            else 0.0
        )
        reward_signal = max(context.current_epoch_reward_gwei, context.average_epoch_reward_gwei, 0)
        penalty_signal = max(context.current_epoch_penalty_gwei, 0)
        slash_cost = max(
            int(context.total_balance_gwei * 0.01),
            penalty_signal + (context.slashed_validator_count * 1_000_000_000),
            1,
        )

        cooperate_payoff = max(int(reward_signal * max(participation_rate, 0.25)) - penalty_signal, 1)
        tempted_deviation_payoff = int(reward_signal * 0.20) - slash_cost
        mutual_deviation_payoff = -max(
            int(context.total_balance_gwei * max(1.0 - participation_rate, 0.05) * 0.002),
            penalty_signal + 1,
        )

        if nash is not None and np is not None:
            row_payoffs = np.array(
                [
                    [cooperate_payoff, max(cooperate_payoff // 2, 1)],
                    [tempted_deviation_payoff, mutual_deviation_payoff],
                ]
            )
            column_payoffs = np.array(
                [
                    [cooperate_payoff, tempted_deviation_payoff],
                    [max(cooperate_payoff // 2, 1), mutual_deviation_payoff],
                ]
            )
            game = nash.Game(row_payoffs, column_payoffs)
            equilibria = list(game.support_enumeration())
            if equilibria:
                cooperate_probability = float(equilibria[0][0][0])
                deviate_probability = float(equilibria[0][0][1])
                return cooperate_probability, deviate_probability

        if cooperate_payoff >= abs(tempted_deviation_payoff):
            return 0.8, 0.2
        return 0.35, 0.65

    @classmethod
    def recommend_pool_actions(cls, context: PoolBehaviorContext) -> list[ActionRecommendation]:
        """Produce modeled next actions from the current pool state."""

        participation_rate = (
            context.active_validator_count / context.total_validator_count
            if context.total_validator_count > 0
            else 0.0
        )
        cooperative_weight, deviation_weight = cls._cooperate_vs_deviate_mix(context)
        reward_signal = max(context.current_epoch_reward_gwei, context.average_epoch_reward_gwei, 0)
        penalty_signal = max(context.current_epoch_penalty_gwei, 0)
        slash_drag = max(
            context.slashed_validator_count * 1_000_000_000,
            int(context.total_balance_gwei * max(1.0 - participation_rate, 0.0) * 0.001),
        )

        add_to_stake_delta = int(
            reward_signal * (0.65 + (0.35 * cooperative_weight)) - (penalty_signal * 0.15)
        )
        wait_delta = int(
            reward_signal * max(cooperative_weight, 0.25) - penalty_signal - (slash_drag * 0.10)
        )
        withdraw_delta = int((penalty_signal * 0.80) + (slash_drag * 0.40) - (reward_signal * 0.50))
        nothing_at_stake_delta = int(
            reward_signal * deviation_weight * 0.25
            - max(int(context.total_balance_gwei * 0.01), penalty_signal + slash_drag + 1)
        )

        confidence = min(0.95, 0.45 + (participation_rate * 0.40))
        defensive_confidence = min(0.95, 0.40 + ((1.0 - participation_rate) * 0.35))

        recommendations = [
            ActionRecommendation(
                action="add_to_stake",
                expected_delta_gwei=add_to_stake_delta,
                confidence=confidence,
                risk_level="medium" if participation_rate < 0.75 else "low",
                rationale=(
                    "Positive reward momentum and healthy participation suggest that adding stake "
                    "has the strongest modeled upside for the next epoch."
                ),
            ),
            ActionRecommendation(
                action="wait",
                expected_delta_gwei=wait_delta,
                confidence=confidence,
                risk_level="low" if penalty_signal == 0 else "medium",
                rationale=(
                    "Waiting keeps exposure unchanged while preserving upside from the current "
                    "validator reward trend."
                ),
            ),
            ActionRecommendation(
                action="withdraw",
                expected_delta_gwei=withdraw_delta,
                confidence=defensive_confidence,
                risk_level="medium",
                rationale=(
                    "Withdrawing mainly protects capital when recent penalties or degraded "
                    "participation make the next epoch look defensive."
                ),
            ),
            ActionRecommendation(
                action="nothing_at_stake_attack",
                expected_delta_gwei=nothing_at_stake_delta,
                confidence=0.85,
                risk_level="extreme",
                rationale=(
                    "This theoretical equivocation strategy is modeled as slash-dominated, so its "
                    "expected value remains deeply negative even if short-term duplication looks tempting."
                ),
                caution="Displayed for modeling only. The dashboard does not provide operational attack guidance.",
            ),
        ]
        return sorted(recommendations, key=lambda item: item.expected_delta_gwei, reverse=True)
