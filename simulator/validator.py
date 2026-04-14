from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any

"""
Python dataclasses implement __init__, __repr__, __eq__, and other methods automatically, which makes it easier for us
"""

@dataclass
class ValidatorState:
    """
    Class to maintain state of the validator
    Think of this also as a 'schema' for an individual validator at a point in time
    """
    validator_uid: str
    effective_stake: float
    uptime: float = 1.0
    
    status: str = "active"  # active, slashed, exited
    slash_count: int = 0
    proposed_blocks: int = 0
    attestations_made: int = 0
    
    total_epochs_active: int = 0
    last_epoch_active: Optional[int] = None  # TODO: make separate Epoch object, replace this line with that later on
    total_slots_active: int = 0
    
    def update_state(self, epoch: int, action: str, reward: float = 0.0, penalty: float = 0.0):
        """
        Update validator state based on action taken in the epoch.
        """
        self.total_epochs_active += 1
        self.last_epoch_active = epoch
        if action == "propose_block":
            self.proposed_blocks += 1
        elif action == "attest":
            self.attestations_made += 1
        if self.economics:
            if reward > 0:
                self.economics.add_reward(epoch, reward)
            if penalty > 0:
                self.economics.add_penalty(epoch, penalty)
    
    def get_balance(self) -> float:
        """
        Get the current balance based on economics.
        """
        if self.economics:
            return self.effective_stake + self.economics.get_net_rewards()
        return self.effective_stake
    
    def is_active(self) -> bool:
        """
        Check if validator is active.
        """
        return self.status == "active"

@dataclass
class ValidatorEconomics:
    """
    Class to maintain economic data of the validator
    """
    validator_uid: str
    total_rewards: float = 0.0
    total_penalties: float = 0.0
    
    reward_history: List[Dict[str, Any]] = field(default_factory=list)  # List of dicts with epoch, amount, type (reward/penalty)
    
    def add_reward(self, epoch: int, amount: float):
        self.total_rewards += amount
        self.reward_history.append({"epoch": epoch, "amount": amount})
    def add_penalty(self, epoch: int, amount: float):
        self.total_penalties += amount
        self.reward_history.append({"epoch": epoch, "amount": -amount})

    def get_net_rewards(self) -> float:
        return self.total_rewards - self.total_penalties
    def get_history(self) -> List[Dict[str, Any]]:
        return self.reward_history

        
    
 
    
