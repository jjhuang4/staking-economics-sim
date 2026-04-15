from __future__ import annotations

from datetime import UTC, datetime

from pool_tracker.models import (
    EntitySummary,
    EntityValidatorSnapshot,
    PoolFlow,
    PoolSnapshot,
    ValidatorActivity,
    ValidatorRewardSnapshot,
    ValidatorSnapshot,
)
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
    assert {
        "validator_snapshots",
        "validator_slot_snapshots",
        "pool_flows",
        "pool_snapshots",
        "pool_slot_snapshots",
        "validator_activity_slots",
        "validator_activity_scanned_slots",
    } <= tables


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


def test_upsert_and_fetch_slot_snapshots(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    validator_snapshot = ValidatorSnapshot(
        validator_index=123,
        epoch=10,
        balance_gwei=32_000_000_123,
        effective_balance_gwei=32_000_000_000,
        status="active_ongoing",
        slot=321,
    )
    pool_snapshot = PoolSnapshot(
        pool_id="pool-1",
        epoch=10,
        total_validator_balance_gwei=32_000_000_123,
        gross_rewards_gwei=123,
        penalties_gwei=0,
        slashing_losses_gwei=0,
        fees_gwei=12,
        net_rewards_gwei=111,
        net_user_flow_wei=0,
        nav_gwei=32_000_000_123,
        total_shares=32_000_000_000.0,
        share_price_gwei=1.0000000038,
        cumulative_pnl_gwei=123,
        slot=321,
    )

    storage.upsert_validator_slot_snapshot(validator_snapshot)
    storage.upsert_pool_slot_snapshot(pool_snapshot)

    fetched_pool_snapshot = storage.get_pool_slot_snapshot("pool-1", 321)
    fetched_validator_snapshots = storage.get_validator_slot_snapshots_for_slot(321, [123])

    assert fetched_pool_snapshot == pool_snapshot
    assert fetched_validator_snapshots == [validator_snapshot]


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


def test_latest_snapshots_before_epoch_are_selected_per_pool_and_validator(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    storage.upsert_pool_snapshot(
        PoolSnapshot(
            pool_id="pool-1",
            epoch=1,
            total_validator_balance_gwei=10,
            gross_rewards_gwei=0,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=0,
            net_rewards_gwei=0,
            net_user_flow_wei=0,
            nav_gwei=10,
            total_shares=10.0,
            share_price_gwei=1.0,
            cumulative_pnl_gwei=0,
        )
    )
    storage.upsert_pool_snapshot(
        PoolSnapshot(
            pool_id="pool-1",
            epoch=3,
            total_validator_balance_gwei=15,
            gross_rewards_gwei=5,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=1,
            net_rewards_gwei=4,
            net_user_flow_wei=0,
            nav_gwei=15,
            total_shares=10.0,
            share_price_gwei=1.5,
            cumulative_pnl_gwei=5,
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
            epoch=3,
            balance_gwei=32_000_000_500,
            effective_balance_gwei=32_000_000_000,
            status="active_ongoing",
        )
    )

    latest_pool_snapshot = storage.get_latest_pool_snapshot_before("pool-1", 4)
    latest_validator_snapshots = storage.get_latest_validator_snapshots_before(4, [123])
    prior_to_epoch_three = storage.get_latest_pool_snapshot_before("pool-1", 3)

    assert latest_pool_snapshot is not None
    assert latest_pool_snapshot.epoch == 3
    assert prior_to_epoch_three is not None
    assert prior_to_epoch_three.epoch == 1
    assert [snapshot.epoch for snapshot in latest_validator_snapshots] == [3]


def test_latest_slot_snapshots_before_slot_are_selected_per_pool_and_validator(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    storage.upsert_pool_slot_snapshot(
        PoolSnapshot(
            pool_id="pool-1",
            epoch=10,
            total_validator_balance_gwei=10,
            gross_rewards_gwei=0,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=0,
            net_rewards_gwei=0,
            net_user_flow_wei=0,
            nav_gwei=10,
            total_shares=10.0,
            share_price_gwei=1.0,
            cumulative_pnl_gwei=0,
            slot=320,
        )
    )
    storage.upsert_pool_slot_snapshot(
        PoolSnapshot(
            pool_id="pool-1",
            epoch=10,
            total_validator_balance_gwei=15,
            gross_rewards_gwei=5,
            penalties_gwei=0,
            slashing_losses_gwei=0,
            fees_gwei=1,
            net_rewards_gwei=4,
            net_user_flow_wei=0,
            nav_gwei=15,
            total_shares=10.0,
            share_price_gwei=1.5,
            cumulative_pnl_gwei=5,
            slot=322,
        )
    )
    storage.upsert_validator_slot_snapshot(
        ValidatorSnapshot(
            validator_index=123,
            epoch=10,
            balance_gwei=32_000_000_000,
            effective_balance_gwei=32_000_000_000,
            status="active_ongoing",
            slot=320,
        )
    )
    storage.upsert_validator_slot_snapshot(
        ValidatorSnapshot(
            validator_index=123,
            epoch=10,
            balance_gwei=32_000_000_500,
            effective_balance_gwei=32_000_000_000,
            status="active_ongoing",
            slot=322,
        )
    )

    latest_pool_snapshot = storage.get_latest_pool_slot_snapshot_before("pool-1", 323)
    latest_validator_snapshots = storage.get_latest_validator_slot_snapshots_before(323, [123])
    pool_history = storage.list_pool_slot_snapshots("pool-1", start_slot=320, end_slot=322)
    validator_history = storage.list_validator_slot_snapshots(123, start_slot=320, end_slot=322)

    assert latest_pool_snapshot is not None
    assert latest_pool_snapshot.slot == 322
    assert [snapshot.slot for snapshot in latest_validator_snapshots] == [322]
    assert [snapshot.slot for snapshot in pool_history] == [320, 322]
    assert [snapshot.slot for snapshot in validator_history] == [320, 322]


def test_entity_and_reward_tracking_tables_round_trip(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    with storage.transaction():
        storage.upsert_entity_snapshot(
            EntitySummary(
                entity="Lido",
                validator_count=12,
                sub_entity_count=2,
                beaconscore=0.8,
                net_share=0.2,
            ),
            snapshot_epoch=77,
            commit=False,
        )
        storage.upsert_validator_reward_snapshot(
            ValidatorRewardSnapshot(
                validator_index=123,
                public_key="0xabc123",
                epoch=77,
                total_wei=-50,
                total_reward_wei=20,
                total_penalty_wei=70,
                total_missed_wei=5,
                realized_loss_wei=50,
            ),
            commit=False,
        )
        storage.upsert_entity_reward_sync_state(
            "Lido",
            tracking_start_epoch=77,
            latest_reward_epoch=77,
            commit=False,
        )
        storage.upsert_entity_validator_snapshot(
            EntityValidatorSnapshot(
                entity="Lido",
                snapshot_epoch=78,
                reward_epoch=77,
                validator_index=123,
                public_key="0xabc123",
                status="active_ongoing",
                balance_gwei=32_000_000_123,
                effective_balance_gwei=32_000_000_000,
                cumulative_reward_wei=20,
                cumulative_penalty_wei=70,
                cumulative_loss_wei=50,
                tracking_start_epoch=77,
                finality="finalized",
                online=True,
            ),
            commit=False,
        )

    latest_entities = storage.list_latest_entity_snapshots()
    reward_state = storage.get_entity_reward_sync_state("Lido")
    reward_totals = storage.get_validator_reward_totals([123])
    mapping_rows = storage.list_latest_entity_validator_snapshots("Lido")

    assert latest_entities[0].entity == "Lido"
    assert reward_state == (77, 77)
    assert reward_totals[123]["cumulative_penalty_wei"] == 70
    assert mapping_rows[0].tracking_start_epoch == 77
    assert mapping_rows[0].online is True


def test_validator_activity_round_trip_and_aggregation(tmp_path):
    storage = SQLiteStorage(str(tmp_path / "tracker.db"))
    with storage.transaction():
        storage.upsert_validator_activity(
            ValidatorActivity(
                slot=100,
                validator_index=123,
                public_key="0xabc123",
                deposit_gwei=32_000_000_000,
                withdrawal_gwei=0,
                proposer_slashings=1,
            ),
            commit=False,
        )
        storage.upsert_validator_activity(
            ValidatorActivity(
                slot=101,
                validator_index=123,
                public_key="0xabc123",
                deposit_gwei=0,
                withdrawal_gwei=1_000_000_000,
                attester_slashings=2,
            ),
            commit=False,
        )
        storage.mark_activity_slot_scanned(100, block_present=True, commit=False)
        storage.mark_activity_slot_scanned(101, block_present=False, commit=False)

    scanned = storage.get_scanned_activity_slots(100, 101)
    summaries = storage.list_validator_activity_summaries(100, 101, limit=10)

    assert scanned == {100, 101}
    assert len(summaries) == 1
    assert summaries[0].validator_index == 123
    assert summaries[0].deposit_gwei == 32_000_000_000
    assert summaries[0].withdrawal_gwei == 1_000_000_000
    assert summaries[0].total_slashings == 3
