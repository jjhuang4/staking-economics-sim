import nashpy as nash
import numpy as np
from validator import ValidatorState, ValidatorEconomics

"""
Relies on implementation of Validator and ValidatorEconomics classes
Controls validator behavior and penalty/rewards using game theory
"""

class Behavior:
    def __init__(self, validator_state: ValidatorState):
        self.validator_state = validator_state

    def decide_action(self, current_epoch: int) -> str:
        """
        Decide on an action based on game theory analysis.
        Actions: "propose_block", "attest", "remain_idle", "exit"
        """
        if not self.validator_state.is_active():
            return "remain_idle"
        
        # Use game theory to decide probabilistically
        action_probabilities = self.compute_action_probabilities()
        actions = ["propose_block", "attest", "remain_idle"]
        chosen_action = np.random.choice(actions, p=action_probabilities)
        return chosen_action
    
    def compute_action_probabilities(self) -> np.ndarray:
        """
        Compute probabilities for actions using Nash equilibrium from a simple game.
        Model as a prisoner's dilemma: cooperate (attest) vs defect (remain_idle).
        For simplicity, focus on attest vs remain_idle, with propose_block as a third option.
        """
        # Define payoff matrix for two players (validators)
        # Rows: Player 1 actions (attest, remain_idle)
        # Columns: Player 2 actions
        # Payoffs: (reward for both, penalty for defection)
        reward_coop = 10  # Both attest
        penalty_defect = -5  # One defects
        suckers_payoff = -10  # Defect when other cooperates
        
        payoff_matrix = np.array([
            [[reward_coop, reward_coop], [suckers_payoff, penalty_defect]],  # Attest
            [[penalty_defect, suckers_payoff], [0, 0]]  # Remain idle
        ])
        
        # For simplicity, assume symmetric game and compute mixed strategy
        # Use nashpy to find equilibria
        game = nash.Game(payoff_matrix)
        equilibria = list(game.support_enumeration())
        
        if equilibria:
            # Take the first equilibrium, get mixed strategy for player 1
            mixed_strategy = equilibria[0][0]  # Probabilities for [attest, remain_idle]
            prob_attest = mixed_strategy[0]
            prob_idle = mixed_strategy[1]
            prob_propose = 0.1  # Small probability for propose_block
            # Normalize
            total = prob_attest + prob_idle + prob_propose
            return np.array([prob_propose / total, prob_attest / total, prob_idle / total])
        else:
            # Default probabilities if no equilibrium
            return np.array([0.1, 0.6, 0.3])  # propose, attest, idle