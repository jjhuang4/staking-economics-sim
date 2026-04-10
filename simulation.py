import os
import random
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from network import Network, NetworkConfig


class Simulation:
    def __init__(self, network, slots_per_epoch=32, num_epochs=50):
        self.network = network
        self.slots_per_epoch = slots_per_epoch
        self.num_epochs = num_epochs
        # Each slot appends one row per validator here.
        self.records = []
        # Each epoch appends one aggregate row here.
        self.epoch_records = []

    def run(self):
        slot = 0
        for epoch in range(self.num_epochs):
            for _ in range(self.slots_per_epoch):
                self.network.run_slot(slot)
                # Save every validator's current state so we can plot it later.
                for v in self.network.validators:
                    self.records.append({
                        "slot": slot,
                        "epoch": epoch,
                        "validator_id": v.id,
                        "behavior": v.behavior_type,
                        "balance": v.stake,
                        "cumulative_reward": v.total_rewards,
                        "slashed": v.slash_count,
                    })
                slot += 1
            # After finishing all slots in an epoch, grab the network-level summary.
            self.epoch_records.append(self.network.end_of_epoch(epoch))

    def get_records(self):
        # Wrap the raw list in a DataFrame so callers can filter and group easily.
        return pd.DataFrame(self.records)

    def get_epoch_records(self):
        # Same as above but for the per-epoch aggregate data.
        return pd.DataFrame(self.epoch_records)

    def detect_convergence(self, window=5, tolerance=0.01):
        # Look for the first epoch where reward stopped meaningfully changing.
        df = pd.DataFrame(self.records)
        epoch_means = df.groupby("epoch")["cumulative_reward"].mean()
        for i in range(window, len(epoch_means)):
            recent = epoch_means.iloc[i - window:i]
            # Standard deviation below tolerance means the curve has flattened out.
            if recent.std() < tolerance:
                return int(epoch_means.index[i])
        return None

    def profitability_summary(self):
        # Print a quick table showing which behaviors ended up profitable and when they broke even.
        df = pd.DataFrame(self.records)
        print("\n=== Profitability Summary ===")
        print(f"{'Behavior':<20} {'Final avg reward':>18} {'Profitable?':>12} {'Break-even epoch':>18}")
        print("-" * 72)

        for behavior, group in df.groupby("behavior"):
            # Average cumulative reward across all validators of this type at the last epoch.
            final_reward = group[group["epoch"] == group["epoch"].max()]["cumulative_reward"].mean()
            profitable = "Yes" if final_reward > 0 else "No"

            # Walk epoch by epoch and find the first time this behavior type went positive.
            epoch_avg = group.groupby("epoch")["cumulative_reward"].mean()
            breakeven = None
            for epoch, val in epoch_avg.items():
                if val >= 0:
                    breakeven = epoch
                    break

            breakeven_str = str(breakeven) if breakeven is not None else "Never"
            print(f"{behavior:<20} {final_reward:>18.4f} {profitable:>12} {breakeven_str:>18}")

        convergence_epoch = self.detect_convergence()
        if convergence_epoch is not None:
            print(f"\nConvergence detected at epoch {convergence_epoch}.")
        else:
            print("\nNo convergence detected within the simulation run.")

    def plot(self, title="PoS Simulation Results", save_path="output/rewards.png"):
        # Make sure the output folder exists before trying to save anything.
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        df = pd.DataFrame(self.records)
        edf = pd.DataFrame(self.epoch_records)

        fig, axes = plt.subplots(1, 3, figsize=(18, 5))

        # Panel 1: how average cumulative reward evolves per behavior over time.
        ax = axes[0]
        for behavior, group in df.groupby("behavior"):
            avg = group.groupby("slot")["cumulative_reward"].mean()
            ax.plot(avg.index, avg.values, label=behavior)
        # Drop a vertical line where the rewards stopped changing significantly.
        convergence_epoch = self.detect_convergence()
        if convergence_epoch is not None:
            convergence_slot = convergence_epoch * self.slots_per_epoch
            ax.axvline(convergence_slot, color="gray", linestyle=":", linewidth=1.2,
                       label=f"Convergence (epoch {convergence_epoch})")
        ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
        ax.set_xlabel("Slot")
        ax.set_ylabel("Average cumulative reward")
        ax.set_title("Rewards by behavior profile")
        ax.legend()

        # Panel 2: how many times each behavior type got slashed over the run.
        ax2 = axes[1]
        for behavior, group in df.groupby("behavior"):
            epoch_slashes = group.groupby("epoch")["slashed"].max()
            ax2.plot(epoch_slashes.index, epoch_slashes.values, label=behavior)
        ax2.set_xlabel("Epoch")
        ax2.set_ylabel("Cumulative slash count")
        ax2.set_title("Slashing events by behavior profile")
        ax2.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        ax2.legend()

        # Panel 3: overall network participation and how often blocks actually finalized.
        ax3 = axes[2]
        ax3.plot(edf["epoch"], edf["avg_participation"], label="Participation rate")
        ax3.plot(edf["epoch"], edf["blocks_finalized"] / self.slots_per_epoch,
                 label="Finalization rate", linestyle="--")
        ax3.set_xlabel("Epoch")
        ax3.set_ylabel("Rate")
        ax3.set_title("Network health over time")
        ax3.legend()

        plt.suptitle(title)
        plt.tight_layout()
        plt.savefig(save_path)
        plt.show()
        print(f"Saved to {save_path}")


def run_scenario(validators_fn, config, slots_per_epoch=32, num_epochs=50, seed=42):
    # Seed random so repeated runs with the same config produce the same results.
    random.seed(seed)
    net = Network(config=config)
    for v in validators_fn():
        net.register(v)
    sim = Simulation(net, slots_per_epoch=slots_per_epoch, num_epochs=num_epochs)
    sim.run()
    return sim


def compare_scenarios(scenarios: dict, slots_per_epoch=32, num_epochs=50, seed=42,
                      save_path="output/comparison.png"):
    # Run every scenario and put all reward curves on one plot for easy comparison.
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    fig, axes = plt.subplots(1, 2, figsize=(14, 5))

    for label, (validators_fn, config) in scenarios.items():
        sim = run_scenario(validators_fn, config,
                           slots_per_epoch=slots_per_epoch,
                           num_epochs=num_epochs, seed=seed)
        df = sim.get_records()
        edf = sim.get_epoch_records()

        for behavior, group in df.groupby("behavior"):
            avg = group.groupby("slot")["cumulative_reward"].mean()
            axes[0].plot(avg.index, avg.values, label=f"{label} — {behavior}")

        axes[1].plot(edf["epoch"], edf["avg_participation"], label=label)

        print(f"\n--- Scenario: {label} ---")
        sim.profitability_summary()

        # Also save a separate detailed plot for each individual scenario.
        individual_path = save_path.replace(".png", f"_{label.replace(' ', '_')}.png")
        sim.plot(title=label, save_path=individual_path)

    axes[0].axhline(0, color="black", linewidth=0.8, linestyle="--")
    axes[0].set_xlabel("Slot")
    axes[0].set_ylabel("Average cumulative reward")
    axes[0].set_title("Reward comparison across scenarios")
    axes[0].legend(fontsize=7)

    axes[1].set_xlabel("Epoch")
    axes[1].set_ylabel("Participation rate")
    axes[1].set_title("Network participation across scenarios")
    axes[1].legend(fontsize=7)

    plt.tight_layout()
    plt.savefig(save_path)
    plt.show()
    print(f"\nComparison plot saved to {save_path}")