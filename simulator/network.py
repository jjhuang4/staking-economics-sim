import random


# Stores all the tunable parameters for the network so we can easily swap configs between simulation runs.
class NetworkConfig:
    def __init__(
        self,
        base_reward: float = 1.0,
        slash_fraction: float = 0.05,
        finality_threshold: float = 0.67,
        proposer_reward_multiplier: float = 2.0,
        missed_attestation_penalty: float = 0.5,
        inactivity_leak_rate: float = 0.01,
        slots_per_epoch: int = 32,
        inactivity_threshold: float = 0.67,
    ):
        # How much each attester earns when a block is finalized.
        self.base_reward = base_reward

        # What fraction of stake a validator loses when slashed, e.g. 0.05 means 5%.
        self.slash_fraction = slash_fraction

        # Minimum fraction of total stake that must vote yes for a block to be finalized.
        self.finality_threshold = finality_threshold

        # The proposer earns base_reward times this number as a bonus on top of attester rewards.
        self.proposer_reward_multiplier = proposer_reward_multiplier

        # How much stake a validator loses each slot they fail to attest.
        self.missed_attestation_penalty = missed_attestation_penalty

        # Extra drain rate applied to offline validators when overall participation is too low.
        self.inactivity_leak_rate = inactivity_leak_rate

        # How many slots make up one epoch, same as Ethereum's 32-slot design.
        self.slots_per_epoch = slots_per_epoch

        # Participation must stay above this threshold or the inactivity leak kicks in.
        self.inactivity_threshold = inactivity_threshold


# Runs the PoS protocol: selects proposers, collects votes, finalizes blocks, and enforces penalties.
class Network:
    def __init__(self, config: NetworkConfig = None):
        # Use the provided config or fall back to defaults if none is given.
        self.config = config if config is not None else NetworkConfig()

        # List of all validators participating in this network.
        self.validators = []

        # Tracks which validator proposed in each slot so we can catch double-proposals.
        self.slot_history = {}

        # Flag that turns on when participation drops too low, triggering extra penalties.
        self.inactivity_leak_active = False

        # Stores the result dict from every slot so the simulation can read it later.
        self.slot_log = []

    def register(self, validator):
        # Add a validator to the network so it participates in future slots.
        self.validators.append(validator)

    def get_validators(self):
        # Return the full validator list so the simulation can snapshot their balances.
        return self.validators

    def select_proposer(self):
        # Pick one validator to propose this slot, weighted by stake so richer validators propose more often.
        weights = [max(v.stake, 0) for v in self.validators]

        # If every validator has been slashed to zero, just pick one at random.
        if sum(weights) == 0:
            return random.choice(self.validators)

        return random.choices(self.validators, weights=weights, k=1)[0]

    def run_slot(self, slot_number: int) -> dict:
        # Run one full slot: propose a block, check for cheating, collect votes, and pay out rewards.
        cfg = self.config
        total_stake = sum(v.stake for v in self.validators)

        # Step 1: pick a proposer and have them produce a block for this slot.
        proposer = self.select_proposer()
        block = proposer.propose_block(slot_number)

        # Step 2: if a different validator already proposed for this slot, slash the current one for equivocation.
        slashing_occurred = False
        if slot_number in self.slot_history and self.slot_history[slot_number] != proposer.id:
            proposer.slash(cfg.slash_fraction)
            slashing_occurred = True

        # Record this proposer so future slots can detect any double-proposal attempts.
        self.slot_history[slot_number] = proposer.id

        # Step 3: ask every validator whether they vote for this block and tally up their stake.
        voters = []
        non_voters = []
        vote_stake = 0.0

        for v in self.validators:
            if v.attest(block):
                voters.append(v)
                vote_stake += v.stake
            else:
                non_voters.append(v)

        # Step 4: finalize the block only if enough stake voted yes, then compute the participation rate.
        finalized = (vote_stake > cfg.finality_threshold * total_stake) if total_stake > 0 else False
        participation_rate = vote_stake / total_stake if total_stake > 0 else 0.0

        # Step 5a: pay the proposer a bonus and reward every attester, but only if the block was finalized.
        if finalized:
            proposer.reward(cfg.base_reward * cfg.proposer_reward_multiplier)
            for v in voters:
                v.reward(cfg.base_reward)

        # Step 5b: deduct a missed-attestation penalty from every validator who did not vote.
        for v in non_voters:
            v.reward(-cfg.missed_attestation_penalty)

        # Step 6: if participation is too low, drain non-voters proportional to their stake each slot.
        self.inactivity_leak_active = participation_rate < cfg.inactivity_threshold
        if self.inactivity_leak_active:
            for v in non_voters:
                v.reward(-cfg.inactivity_leak_rate * v.stake)

        # Step 7: package all slot stats into a dict and save it for the simulation to read.
        result = {
            "slot": slot_number,
            "proposer_id": proposer.id,
            "finalized": finalized,
            "num_votes": len(voters),
            "participation_rate": participation_rate,
            "slashing_occurred": slashing_occurred,
            "inactivity_leak_active": self.inactivity_leak_active,
        }
        self.slot_log.append(result)
        return result

    def end_of_epoch(self, epoch_number: int) -> dict:
        # Summarize the last epoch's slots into aggregate stats for the simulation to record.
        slots_per_epoch = self.config.slots_per_epoch
        recent_slots = self.slot_log[-slots_per_epoch:]

        avg_participation = (
            sum(s["participation_rate"] for s in recent_slots) / len(recent_slots)
            if recent_slots else 0.0
        )
        total_slashings = sum(1 for s in recent_slots if s["slashing_occurred"])
        blocks_finalized = sum(1 for s in recent_slots if s["finalized"])
        inactivity_episodes = sum(1 for s in recent_slots if s["inactivity_leak_active"])

        return {
            "epoch": epoch_number,
            "avg_participation": avg_participation,
            "total_slashings": total_slashings,
            "blocks_finalized": blocks_finalized,
            "inactivity_leak_episodes": inactivity_episodes,
        }