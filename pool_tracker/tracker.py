"""Pool tracking orchestration."""

from __future__ import annotations

from typing import Any

from .accounting import build_pool_snapshot
from .beacon_client import BeaconClient
from .config import Settings, load_pool_config, load_settings
from .execution_client import ExecutionClient
from .models import Pool, PoolFlow, PoolSnapshot
from .registry import PoolRegistry
from .storage import SQLiteStorage

SLOTS_PER_EPOCH = 32


class PoolTracker:
    """Coordinate Hoodi Beacon reads, execution reads, accounting, and storage."""

    def __init__(
        self,
        settings: Settings,
        pool: Pool,
        beacon_client: BeaconClient,
        execution_client: ExecutionClient,
        storage: SQLiteStorage,
    ) -> None:
        self.settings = settings
        self.pool = pool
        self.beacon_client = beacon_client
        self.execution_client = execution_client
        self.storage = storage
        self.registry = PoolRegistry(pool)
        self.event_specs: dict[str, dict[str, Any]] = {}

    @classmethod
    def from_config(cls, pool_config_path: str, db_path: str | None = None) -> "PoolTracker":
        """Build a tracker from env settings and a pool config file."""

        settings = load_settings()
        if db_path is not None:
            settings.db_path = db_path
        pool = load_pool_config(pool_config_path)
        beacon_client = BeaconClient(settings.beacon_api_url)
        execution_client = ExecutionClient(settings.execution_rpc_url)
        storage = SQLiteStorage(settings.db_path)
        return cls(settings, pool, beacon_client, execution_client, storage)

    @staticmethod
    def epoch_to_state_id(epoch: int) -> str:
        """Convert an epoch into a numeric slot string."""

        return str(epoch * SLOTS_PER_EPOCH)

    def resolve_epoch_block_range(self, epoch: int) -> tuple[int, int] | None:
        """Hook for future epoch-to-execution block mapping."""

        _ = epoch
        return None

    def fetch_pool_flows(self, epoch: int) -> list[PoolFlow]:
        """Fetch and decode pool-related execution flows for an epoch."""

        block_range = self.resolve_epoch_block_range(epoch)
        if block_range is None or not self.event_specs:
            return []

        from_block, to_block = block_range
        decoded_flows: list[PoolFlow] = []
        for contract_address in self.registry.get_contract_addresses():
            for log in self.execution_client.get_logs(
                address=contract_address,
                topics=None,
                from_block=from_block,
                to_block=to_block,
            ):
                flow = self.execution_client.decode_pool_flow(log, self.event_specs)
                if flow is not None:
                    decoded_flows.append(flow)
        return decoded_flows

    def sync_epoch(self, epoch: int, state_id: str | None = None) -> PoolSnapshot:
        """Fetch, compute, persist, and return a pool snapshot for a single epoch."""

        validator_indices = self.registry.get_validator_indices()
        resolved_state_id = state_id or self.epoch_to_state_id(epoch)
        current_validator_snapshots = self.beacon_client.build_validator_snapshots(
            epoch=epoch,
            state_id=resolved_state_id,
            ids=validator_indices,
        )
        for snapshot in current_validator_snapshots:
            self.storage.upsert_validator_snapshot(snapshot)

        prior_validator_snapshots = (
            self.storage.get_validator_snapshots_for_epoch(epoch - 1, validator_indices)
            if epoch > 0
            else []
        )
        current_balances = {
            snapshot.validator_index: snapshot.balance_gwei for snapshot in current_validator_snapshots
        }
        prior_balances = {
            snapshot.validator_index: snapshot.balance_gwei for snapshot in prior_validator_snapshots
        }

        flows = self.fetch_pool_flows(epoch)
        for flow in flows:
            self.storage.upsert_pool_flow(flow)

        previous_snapshot = self.storage.get_pool_snapshot(self.pool.pool_id, epoch - 1) if epoch > 0 else None
        cumulative_before_wei = (
            self.storage.get_cumulative_net_user_flow_wei(self.pool.pool_id, epoch - 1)
            if epoch > 0
            else 0
        )
        current_epoch_net_flow_wei = sum(
            flow.amount_wei if flow.flow_type == "deposit" else -flow.amount_wei
            for flow in flows
            if flow.flow_type in {"deposit", "withdraw"}
        )
        cumulative_through_epoch_wei = cumulative_before_wei + current_epoch_net_flow_wei

        pool_snapshot = build_pool_snapshot(
            pool=self.pool,
            epoch=epoch,
            current_balances=current_balances,
            prior_balances=prior_balances,
            flows=flows,
            previous_snapshot=previous_snapshot,
            cumulative_net_user_flow_wei=cumulative_through_epoch_wei,
            current_validator_snapshots=current_validator_snapshots,
            prior_validator_snapshots=prior_validator_snapshots,
        )
        self.storage.upsert_pool_snapshot(pool_snapshot)
        return pool_snapshot

    def sync_range(self, start_epoch: int, end_epoch: int) -> list[PoolSnapshot]:
        """Sync an inclusive epoch range."""

        if end_epoch < start_epoch:
            raise ValueError("end_epoch must be greater than or equal to start_epoch.")
        snapshots: list[PoolSnapshot] = []
        for epoch in range(start_epoch, end_epoch + 1):
            snapshots.append(self.sync_epoch(epoch))
        return snapshots
