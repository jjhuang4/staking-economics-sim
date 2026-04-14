from __future__ import annotations

from pool_tracker.beacon_client import BeaconClient
from pool_tracker.config import Settings
from pool_tracker.models import Pool, ValidatorSnapshot
from pool_tracker.storage import SQLiteStorage
from pool_tracker.tracker import PoolTracker


class FakeBeaconClient(BeaconClient):
    def __init__(self, snapshots_by_epoch):
        self.snapshots_by_epoch = snapshots_by_epoch

    def build_validator_snapshots(self, epoch: int, state_id: str, ids: list[int]):
        return self.snapshots_by_epoch[epoch]


class FakeExecutionClient:
    def get_logs(self, address: str, topics, from_block: int, to_block: int):
        return []

    def get_block_timestamp(self, block_number: int) -> int:
        return 0

    def decode_pool_flow(self, log, event_specs):
        return None


def build_pool() -> Pool:
    return Pool(
        pool_id="hoodi-pool-1",
        name="Hoodi Pool 1",
        fee_rate=0.10,
        slash_pass_through=1.0,
        validator_indices=[123, 456],
        contract_addresses=["0x1111111111111111111111111111111111111111"],
    )


def build_tracker(tmp_path, snapshots_by_epoch) -> PoolTracker:
    settings = Settings(
        execution_rpc_url="https://execution.example",
        beacon_api_url="https://beacon.example",
        db_path=str(tmp_path / "tracker.db"),
    )
    storage = SQLiteStorage(settings.db_path)
    return PoolTracker(
        settings=settings,
        pool=build_pool(),
        beacon_client=FakeBeaconClient(snapshots_by_epoch),
        execution_client=FakeExecutionClient(),
        storage=storage,
    )


def test_mocked_end_to_end_epoch_sync(tmp_path):
    tracker = build_tracker(
        tmp_path,
        {
            10: [
                ValidatorSnapshot(123, 10, 32_000_000_100, 32_000_000_000, "active_ongoing"),
                ValidatorSnapshot(456, 10, 32_000_000_200, 32_000_000_000, "active_ongoing"),
            ]
        },
    )
    snapshot = tracker.sync_epoch(10)
    assert snapshot.pool_id == "hoodi-pool-1"
    assert snapshot.total_validator_balance_gwei == 64_000_000_300
    assert tracker.storage.get_pool_snapshot("hoodi-pool-1", 10) == snapshot


def test_previous_epoch_comparison(tmp_path):
    tracker = build_tracker(
        tmp_path,
        {
            9: [
                ValidatorSnapshot(123, 9, 32_000_000_000, 32_000_000_000, "active_ongoing"),
                ValidatorSnapshot(456, 9, 32_000_000_000, 32_000_000_000, "active_ongoing"),
            ],
            10: [
                ValidatorSnapshot(123, 10, 32_000_000_100, 32_000_000_000, "active_ongoing"),
                ValidatorSnapshot(456, 10, 32_000_000_150, 32_000_000_000, "active_ongoing"),
            ],
        },
    )
    tracker.sync_epoch(9)
    snapshot = tracker.sync_epoch(10)
    assert snapshot.gross_rewards_gwei == 250
    assert snapshot.penalties_gwei == 0


def test_slashed_status_transition_handled_without_crash(tmp_path):
    tracker = build_tracker(
        tmp_path,
        {
            1: [
                ValidatorSnapshot(123, 1, 32_000_000_000, 32_000_000_000, "active_ongoing"),
                ValidatorSnapshot(456, 1, 32_000_000_000, 32_000_000_000, "active_ongoing"),
            ],
            2: [
                ValidatorSnapshot(123, 2, 31_900_000_000, 31_000_000_000, "active_slashed"),
                ValidatorSnapshot(456, 2, 32_000_000_000, 32_000_000_000, "active_ongoing"),
            ],
        },
    )
    tracker.sync_epoch(1)
    snapshot = tracker.sync_epoch(2)
    assert snapshot.slashing_losses_gwei > 0
    assert snapshot.penalties_gwei == 0
