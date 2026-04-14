from __future__ import annotations

from datetime import UTC, datetime

from pool_tracker.models import PoolFlow, PoolSnapshot, ValidatorSnapshot
from pool_tracker.storage import SQLiteStorage


def test_table_creation(tmp_path):
    db_path = tmp_path / "tracker.db"
    storage = SQLiteStorage(str(db_path))
    tables = {
        row[0]
        for row in storage.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    assert {"validator_snapshots", "pool_flows", "pool_snapshots"} <= tables


def test_upsert_and_fetch_snapshot(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    snapshot = PoolSnapshot(
        pool_id="pool-1",
        epoch=1,
        total_validator_balance_gwei=10,
        gross_rewards_gwei=2,
        penalties_gwei=0,
        slashing_losses_gwei=0,
        fees_gwei=0,
        net_rewards_gwei=2,
        net_user_flow_wei=0,
        nav_gwei=10,
        total_shares=10.0,
        share_price_gwei=1.0,
        cumulative_pnl_gwei=0,
    )
    storage.upsert_pool_snapshot(snapshot)
    fetched = storage.get_pool_snapshot("pool-1", 1)
    assert fetched == snapshot


def test_dedupe_on_tx_hash_and_log_index(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    flow = PoolFlow(
        block_number=1,
        tx_hash="0xabc",
        log_index=0,
        timestamp=datetime.now(tz=UTC),
        flow_type="deposit",
        amount_wei=100,
        actor="0x1111111111111111111111111111111111111111",
    )
    storage.upsert_pool_flow(flow)
    storage.upsert_pool_flow(flow)
    count = storage.connection.execute("SELECT COUNT(*) FROM pool_flows").fetchone()[0]
    assert count == 1


def test_list_snapshot_history_returns_ascending_epochs(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    storage.upsert_pool_snapshot(
        PoolSnapshot(
            pool_id="pool-1",
            epoch=2,
            total_validator_balance_gwei=20,
            gross_rewards_gwei=1,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=0,
            net_rewards_gwei=1,
            net_user_flow_wei=0,
            nav_gwei=20,
            total_shares=20.0,
            share_price_gwei=1.0,
            cumulative_pnl_gwei=0,
        )
    )
    storage.upsert_pool_snapshot(
        PoolSnapshot(
            pool_id="pool-1",
            epoch=1,
            total_validator_balance_gwei=10,
            gross_rewards_gwei=1,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=0,
            net_rewards_gwei=1,
            net_user_flow_wei=0,
            nav_gwei=10,
            total_shares=10.0,
            share_price_gwei=1.0,
            cumulative_pnl_gwei=0,
        )
    )
    storage.upsert_validator_snapshot(
        ValidatorSnapshot(
            validator_index=123,
            epoch=1,
            balance_gwei=32_000_000_000,
            effective_balance_gwei=32_000_000_000,
            status="active_ongoing",
        )
    )
    storage.upsert_validator_snapshot(
        ValidatorSnapshot(
            validator_index=123,
            epoch=2,
            balance_gwei=32_000_000_100,
            effective_balance_gwei=32_000_000_000,
            status="active_ongoing",
        )
    )

    pool_history = storage.list_pool_snapshots("pool-1")
    validator_history = storage.list_validator_snapshots(123)

    assert [snapshot.epoch for snapshot in pool_history] == [1, 2]
    assert [snapshot.epoch for snapshot in validator_history] == [1, 2]
