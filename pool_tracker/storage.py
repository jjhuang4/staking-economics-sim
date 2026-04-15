"""SQLite persistence for pool tracker snapshots."""

from __future__ import annotations

from contextlib import contextmanager
import sqlite3
from typing import Iterator

from .models import (
    EntitySummary,
    EntityValidatorSnapshot,
    PoolFlow,
    PoolSnapshot,
    ValidatorActivity,
    ValidatorActivitySummary,
    ValidatorRewardSnapshot,
    ValidatorSnapshot,
)


class SQLiteStorage:
    """Persist pool tracker state in SQLite."""

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self._create_tables()

    def _create_tables(self) -> None:
        cursor = self.connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS validator_snapshots (
              validator_index INTEGER NOT NULL,
              epoch INTEGER NOT NULL,
              balance_gwei INTEGER NOT NULL,
              effective_balance_gwei INTEGER NOT NULL,
              status TEXT NOT NULL,
              PRIMARY KEY (validator_index, epoch)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pool_flows (
              tx_hash TEXT NOT NULL,
              log_index INTEGER NOT NULL,
              block_number INTEGER NOT NULL,
              timestamp TEXT NOT NULL,
              flow_type TEXT NOT NULL,
              amount_wei INTEGER NOT NULL,
              actor TEXT,
              PRIMARY KEY (tx_hash, log_index)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pool_snapshots (
              pool_id TEXT NOT NULL,
              epoch INTEGER NOT NULL,
              total_validator_balance_gwei INTEGER NOT NULL,
              gross_rewards_gwei INTEGER NOT NULL,
              penalties_gwei INTEGER NOT NULL,
              slashing_losses_gwei INTEGER NOT NULL,
              fees_gwei INTEGER NOT NULL,
              net_rewards_gwei INTEGER NOT NULL,
              net_user_flow_wei INTEGER NOT NULL,
              nav_gwei INTEGER NOT NULL,
              total_shares REAL NOT NULL,
              share_price_gwei REAL NOT NULL,
              cumulative_pnl_gwei INTEGER NOT NULL,
              PRIMARY KEY (pool_id, epoch)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS validator_slot_snapshots (
              validator_index INTEGER NOT NULL,
              slot INTEGER NOT NULL,
              epoch INTEGER NOT NULL,
              balance_gwei INTEGER NOT NULL,
              effective_balance_gwei INTEGER NOT NULL,
              status TEXT NOT NULL,
              PRIMARY KEY (validator_index, slot)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pool_slot_snapshots (
              pool_id TEXT NOT NULL,
              slot INTEGER NOT NULL,
              epoch INTEGER NOT NULL,
              total_validator_balance_gwei INTEGER NOT NULL,
              gross_rewards_gwei INTEGER NOT NULL,
              penalties_gwei INTEGER NOT NULL,
              slashing_losses_gwei INTEGER NOT NULL,
              fees_gwei INTEGER NOT NULL,
              net_rewards_gwei INTEGER NOT NULL,
              net_user_flow_wei INTEGER NOT NULL,
              nav_gwei INTEGER NOT NULL,
              total_shares REAL NOT NULL,
              share_price_gwei REAL NOT NULL,
              cumulative_pnl_gwei INTEGER NOT NULL,
              PRIMARY KEY (pool_id, slot)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_snapshots (
              entity TEXT NOT NULL,
              snapshot_epoch INTEGER NOT NULL,
              validator_count INTEGER NOT NULL,
              sub_entity_count INTEGER NOT NULL,
              beaconscore REAL,
              net_share REAL,
              apr REAL,
              apy REAL,
              PRIMARY KEY (entity, snapshot_epoch)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS validator_reward_snapshots (
              validator_index INTEGER NOT NULL,
              epoch INTEGER NOT NULL,
              public_key TEXT NOT NULL,
              total_wei INTEGER NOT NULL,
              total_reward_wei INTEGER NOT NULL,
              total_penalty_wei INTEGER NOT NULL,
              total_missed_wei INTEGER NOT NULL,
              realized_loss_wei INTEGER NOT NULL,
              attestations_source_reward_wei INTEGER NOT NULL,
              attestations_target_reward_wei INTEGER NOT NULL,
              attestations_head_reward_wei INTEGER NOT NULL,
              attestations_source_penalty_wei INTEGER NOT NULL,
              attestations_target_penalty_wei INTEGER NOT NULL,
              sync_reward_wei INTEGER NOT NULL,
              sync_penalty_wei INTEGER NOT NULL,
              slashing_reward_wei INTEGER NOT NULL,
              slashing_penalty_wei INTEGER NOT NULL,
              proposal_reward_cl_wei INTEGER NOT NULL,
              proposal_reward_el_wei INTEGER NOT NULL,
              proposal_missed_reward_cl_wei INTEGER NOT NULL,
              proposal_missed_reward_el_wei INTEGER NOT NULL,
              finality TEXT NOT NULL,
              PRIMARY KEY (validator_index, epoch)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_validator_snapshots (
              entity TEXT NOT NULL,
              snapshot_epoch INTEGER NOT NULL,
              reward_epoch INTEGER NOT NULL,
              validator_index INTEGER NOT NULL,
              public_key TEXT NOT NULL,
              status TEXT NOT NULL,
              balance_gwei INTEGER NOT NULL,
              effective_balance_gwei INTEGER NOT NULL,
              cumulative_reward_wei INTEGER NOT NULL,
              cumulative_penalty_wei INTEGER NOT NULL,
              cumulative_loss_wei INTEGER NOT NULL,
              tracking_start_epoch INTEGER NOT NULL,
              finality TEXT NOT NULL,
              online INTEGER,
              PRIMARY KEY (entity, snapshot_epoch, validator_index)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS entity_reward_sync_state (
              entity TEXT NOT NULL PRIMARY KEY,
              tracking_start_epoch INTEGER NOT NULL,
              latest_reward_epoch INTEGER NOT NULL
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS validator_activity_slots (
              slot INTEGER NOT NULL,
              validator_index INTEGER NOT NULL,
              public_key TEXT NOT NULL,
              deposit_gwei INTEGER NOT NULL,
              withdrawal_gwei INTEGER NOT NULL,
              proposer_slashings INTEGER NOT NULL,
              attester_slashings INTEGER NOT NULL,
              PRIMARY KEY (slot, validator_index, public_key)
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS validator_activity_scanned_slots (
              slot INTEGER NOT NULL PRIMARY KEY,
              block_present INTEGER NOT NULL
            );
            """
        )
        self.connection.commit()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Execute a block atomically."""

        self.connection.execute("BEGIN")
        try:
            yield
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def upsert_validator_snapshot(self, snapshot: ValidatorSnapshot, *, commit: bool = True) -> None:
        """Insert or update a validator snapshot."""

        self.connection.execute(
            """
            INSERT INTO validator_snapshots (
              validator_index,
              epoch,
              balance_gwei,
              effective_balance_gwei,
              status
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(validator_index, epoch) DO UPDATE SET
              balance_gwei = excluded.balance_gwei,
              effective_balance_gwei = excluded.effective_balance_gwei,
              status = excluded.status
            """,
            (
                snapshot.validator_index,
                snapshot.epoch,
                snapshot.balance_gwei,
                snapshot.effective_balance_gwei,
                snapshot.status,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_pool_flow(self, flow: PoolFlow, *, commit: bool = True) -> None:
        """Insert or update a decoded pool flow."""

        self.connection.execute(
            """
            INSERT INTO pool_flows (
              tx_hash,
              log_index,
              block_number,
              timestamp,
              flow_type,
              amount_wei,
              actor
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(tx_hash, log_index) DO UPDATE SET
              block_number = excluded.block_number,
              timestamp = excluded.timestamp,
              flow_type = excluded.flow_type,
              amount_wei = excluded.amount_wei,
              actor = excluded.actor
            """,
            (
                flow.tx_hash,
                flow.log_index,
                flow.block_number,
                flow.timestamp.isoformat(),
                flow.flow_type,
                flow.amount_wei,
                flow.actor,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_pool_snapshot(self, snapshot: PoolSnapshot, *, commit: bool = True) -> None:
        """Insert or update a pool snapshot."""

        self.connection.execute(
            """
            INSERT INTO pool_snapshots (
              pool_id,
              epoch,
              total_validator_balance_gwei,
              gross_rewards_gwei,
              penalties_gwei,
              slashing_losses_gwei,
              fees_gwei,
              net_rewards_gwei,
              net_user_flow_wei,
              nav_gwei,
              total_shares,
              share_price_gwei,
              cumulative_pnl_gwei
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pool_id, epoch) DO UPDATE SET
              total_validator_balance_gwei = excluded.total_validator_balance_gwei,
              gross_rewards_gwei = excluded.gross_rewards_gwei,
              penalties_gwei = excluded.penalties_gwei,
              slashing_losses_gwei = excluded.slashing_losses_gwei,
              fees_gwei = excluded.fees_gwei,
              net_rewards_gwei = excluded.net_rewards_gwei,
              net_user_flow_wei = excluded.net_user_flow_wei,
              nav_gwei = excluded.nav_gwei,
              total_shares = excluded.total_shares,
              share_price_gwei = excluded.share_price_gwei,
              cumulative_pnl_gwei = excluded.cumulative_pnl_gwei
            """,
            (
                snapshot.pool_id,
                snapshot.epoch,
                snapshot.total_validator_balance_gwei,
                snapshot.gross_rewards_gwei,
                snapshot.penalties_gwei,
                snapshot.slashing_losses_gwei,
                snapshot.fees_gwei,
                snapshot.net_rewards_gwei,
                snapshot.net_user_flow_wei,
                snapshot.nav_gwei,
                snapshot.total_shares,
                snapshot.share_price_gwei,
                snapshot.cumulative_pnl_gwei,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_validator_slot_snapshot(self, snapshot: ValidatorSnapshot, *, commit: bool = True) -> None:
        """Insert or update a validator slot snapshot."""

        if snapshot.slot is None:
            raise ValueError("validator slot snapshot requires a slot value")
        self.connection.execute(
            """
            INSERT INTO validator_slot_snapshots (
              validator_index,
              slot,
              epoch,
              balance_gwei,
              effective_balance_gwei,
              status
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(validator_index, slot) DO UPDATE SET
              epoch = excluded.epoch,
              balance_gwei = excluded.balance_gwei,
              effective_balance_gwei = excluded.effective_balance_gwei,
              status = excluded.status
            """,
            (
                snapshot.validator_index,
                snapshot.slot,
                snapshot.epoch,
                snapshot.balance_gwei,
                snapshot.effective_balance_gwei,
                snapshot.status,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_pool_slot_snapshot(self, snapshot: PoolSnapshot, *, commit: bool = True) -> None:
        """Insert or update a pool slot snapshot."""

        if snapshot.slot is None:
            raise ValueError("pool slot snapshot requires a slot value")
        self.connection.execute(
            """
            INSERT INTO pool_slot_snapshots (
              pool_id,
              slot,
              epoch,
              total_validator_balance_gwei,
              gross_rewards_gwei,
              penalties_gwei,
              slashing_losses_gwei,
              fees_gwei,
              net_rewards_gwei,
              net_user_flow_wei,
              nav_gwei,
              total_shares,
              share_price_gwei,
              cumulative_pnl_gwei
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pool_id, slot) DO UPDATE SET
              epoch = excluded.epoch,
              total_validator_balance_gwei = excluded.total_validator_balance_gwei,
              gross_rewards_gwei = excluded.gross_rewards_gwei,
              penalties_gwei = excluded.penalties_gwei,
              slashing_losses_gwei = excluded.slashing_losses_gwei,
              fees_gwei = excluded.fees_gwei,
              net_rewards_gwei = excluded.net_rewards_gwei,
              net_user_flow_wei = excluded.net_user_flow_wei,
              nav_gwei = excluded.nav_gwei,
              total_shares = excluded.total_shares,
              share_price_gwei = excluded.share_price_gwei,
              cumulative_pnl_gwei = excluded.cumulative_pnl_gwei
            """,
            (
                snapshot.pool_id,
                snapshot.slot,
                snapshot.epoch,
                snapshot.total_validator_balance_gwei,
                snapshot.gross_rewards_gwei,
                snapshot.penalties_gwei,
                snapshot.slashing_losses_gwei,
                snapshot.fees_gwei,
                snapshot.net_rewards_gwei,
                snapshot.net_user_flow_wei,
                snapshot.nav_gwei,
                snapshot.total_shares,
                snapshot.share_price_gwei,
                snapshot.cumulative_pnl_gwei,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_entity_snapshot(
        self,
        snapshot: EntitySummary,
        snapshot_epoch: int,
        *,
        commit: bool = True,
    ) -> None:
        """Insert or update a top-entity summary for a given epoch."""

        self.connection.execute(
            """
            INSERT INTO entity_snapshots (
              entity,
              snapshot_epoch,
              validator_count,
              sub_entity_count,
              beaconscore,
              net_share,
              apr,
              apy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity, snapshot_epoch) DO UPDATE SET
              validator_count = excluded.validator_count,
              sub_entity_count = excluded.sub_entity_count,
              beaconscore = excluded.beaconscore,
              net_share = excluded.net_share,
              apr = excluded.apr,
              apy = excluded.apy
            """,
            (
                snapshot.entity,
                snapshot_epoch,
                snapshot.validator_count,
                snapshot.sub_entity_count,
                snapshot.beaconscore,
                snapshot.net_share,
                snapshot.apr,
                snapshot.apy,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_validator_reward_snapshot(
        self,
        snapshot: ValidatorRewardSnapshot,
        *,
        commit: bool = True,
    ) -> None:
        """Insert or update a per-validator reward breakdown row."""

        self.connection.execute(
            """
            INSERT INTO validator_reward_snapshots (
              validator_index,
              epoch,
              public_key,
              total_wei,
              total_reward_wei,
              total_penalty_wei,
              total_missed_wei,
              realized_loss_wei,
              attestations_source_reward_wei,
              attestations_target_reward_wei,
              attestations_head_reward_wei,
              attestations_source_penalty_wei,
              attestations_target_penalty_wei,
              sync_reward_wei,
              sync_penalty_wei,
              slashing_reward_wei,
              slashing_penalty_wei,
              proposal_reward_cl_wei,
              proposal_reward_el_wei,
              proposal_missed_reward_cl_wei,
              proposal_missed_reward_el_wei,
              finality
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(validator_index, epoch) DO UPDATE SET
              public_key = excluded.public_key,
              total_wei = excluded.total_wei,
              total_reward_wei = excluded.total_reward_wei,
              total_penalty_wei = excluded.total_penalty_wei,
              total_missed_wei = excluded.total_missed_wei,
              realized_loss_wei = excluded.realized_loss_wei,
              attestations_source_reward_wei = excluded.attestations_source_reward_wei,
              attestations_target_reward_wei = excluded.attestations_target_reward_wei,
              attestations_head_reward_wei = excluded.attestations_head_reward_wei,
              attestations_source_penalty_wei = excluded.attestations_source_penalty_wei,
              attestations_target_penalty_wei = excluded.attestations_target_penalty_wei,
              sync_reward_wei = excluded.sync_reward_wei,
              sync_penalty_wei = excluded.sync_penalty_wei,
              slashing_reward_wei = excluded.slashing_reward_wei,
              slashing_penalty_wei = excluded.slashing_penalty_wei,
              proposal_reward_cl_wei = excluded.proposal_reward_cl_wei,
              proposal_reward_el_wei = excluded.proposal_reward_el_wei,
              proposal_missed_reward_cl_wei = excluded.proposal_missed_reward_cl_wei,
              proposal_missed_reward_el_wei = excluded.proposal_missed_reward_el_wei,
              finality = excluded.finality
            """,
            (
                snapshot.validator_index,
                snapshot.epoch,
                snapshot.public_key,
                snapshot.total_wei,
                snapshot.total_reward_wei,
                snapshot.total_penalty_wei,
                snapshot.total_missed_wei,
                snapshot.realized_loss_wei,
                snapshot.attestations_source_reward_wei,
                snapshot.attestations_target_reward_wei,
                snapshot.attestations_head_reward_wei,
                snapshot.attestations_source_penalty_wei,
                snapshot.attestations_target_penalty_wei,
                snapshot.sync_reward_wei,
                snapshot.sync_penalty_wei,
                snapshot.slashing_reward_wei,
                snapshot.slashing_penalty_wei,
                snapshot.proposal_reward_cl_wei,
                snapshot.proposal_reward_el_wei,
                snapshot.proposal_missed_reward_cl_wei,
                snapshot.proposal_missed_reward_el_wei,
                snapshot.finality,
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_entity_validator_snapshot(
        self,
        snapshot: EntityValidatorSnapshot,
        *,
        commit: bool = True,
    ) -> None:
        """Insert or update the latest entity-to-validator mapping row."""

        self.connection.execute(
            """
            INSERT INTO entity_validator_snapshots (
              entity,
              snapshot_epoch,
              reward_epoch,
              validator_index,
              public_key,
              status,
              balance_gwei,
              effective_balance_gwei,
              cumulative_reward_wei,
              cumulative_penalty_wei,
              cumulative_loss_wei,
              tracking_start_epoch,
              finality,
              online
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(entity, snapshot_epoch, validator_index) DO UPDATE SET
              reward_epoch = excluded.reward_epoch,
              public_key = excluded.public_key,
              status = excluded.status,
              balance_gwei = excluded.balance_gwei,
              effective_balance_gwei = excluded.effective_balance_gwei,
              cumulative_reward_wei = excluded.cumulative_reward_wei,
              cumulative_penalty_wei = excluded.cumulative_penalty_wei,
              cumulative_loss_wei = excluded.cumulative_loss_wei,
              tracking_start_epoch = excluded.tracking_start_epoch,
              finality = excluded.finality,
              online = excluded.online
            """,
            (
                snapshot.entity,
                snapshot.snapshot_epoch,
                snapshot.reward_epoch,
                snapshot.validator_index,
                snapshot.public_key,
                snapshot.status,
                snapshot.balance_gwei,
                snapshot.effective_balance_gwei,
                snapshot.cumulative_reward_wei,
                snapshot.cumulative_penalty_wei,
                snapshot.cumulative_loss_wei,
                snapshot.tracking_start_epoch,
                snapshot.finality,
                None if snapshot.online is None else int(snapshot.online),
            ),
        )
        if commit:
            self.connection.commit()

    def upsert_entity_reward_sync_state(
        self,
        entity: str,
        *,
        tracking_start_epoch: int,
        latest_reward_epoch: int,
        commit: bool = True,
    ) -> None:
        """Persist the local reward backfill checkpoint for an entity."""

        self.connection.execute(
            """
            INSERT INTO entity_reward_sync_state (
              entity,
              tracking_start_epoch,
              latest_reward_epoch
            ) VALUES (?, ?, ?)
            ON CONFLICT(entity) DO UPDATE SET
              tracking_start_epoch = MIN(entity_reward_sync_state.tracking_start_epoch, excluded.tracking_start_epoch),
              latest_reward_epoch = excluded.latest_reward_epoch
            """,
            (entity, tracking_start_epoch, latest_reward_epoch),
        )
        if commit:
            self.connection.commit()

    def upsert_validator_activity(
        self,
        activity: ValidatorActivity,
        *,
        commit: bool = True,
    ) -> None:
        """Insert or update finalized per-slot validator activity."""

        self.connection.execute(
            """
            INSERT INTO validator_activity_slots (
              slot,
              validator_index,
              public_key,
              deposit_gwei,
              withdrawal_gwei,
              proposer_slashings,
              attester_slashings
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(slot, validator_index, public_key) DO UPDATE SET
              deposit_gwei = excluded.deposit_gwei,
              withdrawal_gwei = excluded.withdrawal_gwei,
              proposer_slashings = excluded.proposer_slashings,
              attester_slashings = excluded.attester_slashings
            """,
            (
                activity.slot,
                activity.validator_index,
                activity.public_key,
                activity.deposit_gwei,
                activity.withdrawal_gwei,
                activity.proposer_slashings,
                activity.attester_slashings,
            ),
        )
        if commit:
            self.connection.commit()

    def mark_activity_slot_scanned(
        self,
        slot: int,
        *,
        block_present: bool,
        commit: bool = True,
    ) -> None:
        """Mark a finalized slot as already scanned for validator activity."""

        self.connection.execute(
            """
            INSERT INTO validator_activity_scanned_slots (
              slot,
              block_present
            ) VALUES (?, ?)
            ON CONFLICT(slot) DO UPDATE SET
              block_present = excluded.block_present
            """,
            (slot, int(block_present)),
        )
        if commit:
            self.connection.commit()

    def get_validator_snapshot(self, epoch: int, validator_index: int) -> ValidatorSnapshot | None:
        """Fetch a single validator snapshot."""

        row = self.connection.execute(
            """
            SELECT validator_index, epoch, balance_gwei, effective_balance_gwei, status
            FROM validator_snapshots
            WHERE epoch = ? AND validator_index = ?
            """,
            (epoch, validator_index),
        ).fetchone()
        return self._row_to_validator_snapshot(row) if row else None

    def get_validator_snapshots_for_epoch(
        self,
        epoch: int,
        validator_indices: list[int],
    ) -> list[ValidatorSnapshot]:
        """Fetch validator snapshots for a specific epoch."""

        if not validator_indices:
            return []
        placeholders = ",".join("?" for _ in validator_indices)
        rows = self.connection.execute(
            f"""
            SELECT validator_index, epoch, balance_gwei, effective_balance_gwei, status
            FROM validator_snapshots
            WHERE epoch = ? AND validator_index IN ({placeholders})
            ORDER BY validator_index ASC
            """,
            (epoch, *validator_indices),
        ).fetchall()
        return [self._row_to_validator_snapshot(row) for row in rows]

    def get_validator_slot_snapshots_for_slot(
        self,
        slot: int,
        validator_indices: list[int],
    ) -> list[ValidatorSnapshot]:
        """Fetch validator slot snapshots for a specific slot."""

        if not validator_indices:
            return []
        placeholders = ",".join("?" for _ in validator_indices)
        rows = self.connection.execute(
            f"""
            SELECT validator_index, slot, epoch, balance_gwei, effective_balance_gwei, status
            FROM validator_slot_snapshots
            WHERE slot = ? AND validator_index IN ({placeholders})
            ORDER BY validator_index ASC
            """,
            (slot, *validator_indices),
        ).fetchall()
        return [self._row_to_validator_snapshot(row) for row in rows]

    def get_latest_pool_snapshot(self, pool_id: str) -> PoolSnapshot | None:
        """Fetch the latest stored pool snapshot."""

        row = self.connection.execute(
            """
            SELECT *
            FROM pool_snapshots
            WHERE pool_id = ?
            ORDER BY epoch DESC
            LIMIT 1
            """,
            (pool_id,),
        ).fetchone()
        return self._row_to_pool_snapshot(row) if row else None

    def get_pool_snapshot(self, pool_id: str, epoch: int) -> PoolSnapshot | None:
        """Fetch a pool snapshot for a specific epoch."""

        row = self.connection.execute(
            """
            SELECT *
            FROM pool_snapshots
            WHERE pool_id = ? AND epoch = ?
            """,
            (pool_id, epoch),
        ).fetchone()
        return self._row_to_pool_snapshot(row) if row else None

    def get_latest_pool_snapshot_before(self, pool_id: str, epoch: int) -> PoolSnapshot | None:
        """Fetch the latest stored pool snapshot before a target epoch."""

        row = self.connection.execute(
            """
            SELECT *
            FROM pool_snapshots
            WHERE pool_id = ? AND epoch < ?
            ORDER BY epoch DESC
            LIMIT 1
            """,
            (pool_id, epoch),
        ).fetchone()
        return self._row_to_pool_snapshot(row) if row else None

    def get_pool_slot_snapshot(self, pool_id: str, slot: int) -> PoolSnapshot | None:
        """Fetch a pool slot snapshot for a specific slot."""

        row = self.connection.execute(
            """
            SELECT *
            FROM pool_slot_snapshots
            WHERE pool_id = ? AND slot = ?
            """,
            (pool_id, slot),
        ).fetchone()
        return self._row_to_pool_snapshot(row) if row else None

    def get_latest_pool_slot_snapshot_before(self, pool_id: str, slot: int) -> PoolSnapshot | None:
        """Fetch the latest stored pool slot snapshot before a target slot."""

        row = self.connection.execute(
            """
            SELECT *
            FROM pool_slot_snapshots
            WHERE pool_id = ? AND slot < ?
            ORDER BY slot DESC
            LIMIT 1
            """,
            (pool_id, slot),
        ).fetchone()
        return self._row_to_pool_snapshot(row) if row else None

    def get_pool_slot_snapshot_slots(self, pool_id: str, start_slot: int, end_slot: int) -> set[int]:
        """Return the subset of slot snapshots already stored for a pool over a range."""

        rows = self.connection.execute(
            """
            SELECT slot
            FROM pool_slot_snapshots
            WHERE pool_id = ? AND slot BETWEEN ? AND ?
            """,
            (pool_id, start_slot, end_slot),
        ).fetchall()
        return {int(row["slot"]) for row in rows}

    def list_pool_snapshots(self, pool_id: str, limit: int | None = None) -> list[PoolSnapshot]:
        """List stored pool snapshots in ascending epoch order."""

        query = """
            SELECT *
            FROM pool_snapshots
            WHERE pool_id = ?
            ORDER BY epoch DESC
        """
        params: list[object] = [pool_id]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self.connection.execute(query, tuple(params)).fetchall()
        snapshots = [self._row_to_pool_snapshot(row) for row in rows]
        snapshots.reverse()
        return snapshots

    def list_pool_slot_snapshots(
        self,
        pool_id: str,
        *,
        start_slot: int,
        end_slot: int,
    ) -> list[PoolSnapshot]:
        """List stored pool slot snapshots in ascending slot order."""

        rows = self.connection.execute(
            """
            SELECT *
            FROM pool_slot_snapshots
            WHERE pool_id = ? AND slot BETWEEN ? AND ?
            ORDER BY slot ASC
            """,
            (pool_id, start_slot, end_slot),
        ).fetchall()
        return [self._row_to_pool_snapshot(row) for row in rows]

    def list_validator_snapshots(
        self,
        validator_index: int,
        limit: int | None = None,
    ) -> list[ValidatorSnapshot]:
        """List stored snapshots for a validator in ascending epoch order."""

        query = """
            SELECT validator_index, epoch, balance_gwei, effective_balance_gwei, status
            FROM validator_snapshots
            WHERE validator_index = ?
            ORDER BY epoch DESC
        """
        params: list[object] = [validator_index]
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)
        rows = self.connection.execute(query, tuple(params)).fetchall()
        snapshots = [self._row_to_validator_snapshot(row) for row in rows]
        snapshots.reverse()
        return snapshots

    def list_validator_slot_snapshots(
        self,
        validator_index: int,
        *,
        start_slot: int,
        end_slot: int,
    ) -> list[ValidatorSnapshot]:
        """List stored slot snapshots for a validator in ascending slot order."""

        rows = self.connection.execute(
            """
            SELECT validator_index, slot, epoch, balance_gwei, effective_balance_gwei, status
            FROM validator_slot_snapshots
            WHERE validator_index = ? AND slot BETWEEN ? AND ?
            ORDER BY slot ASC
            """,
            (validator_index, start_slot, end_slot),
        ).fetchall()
        return [self._row_to_validator_snapshot(row) for row in rows]

    def get_latest_validator_snapshots_before(
        self,
        epoch: int,
        validator_indices: list[int],
    ) -> list[ValidatorSnapshot]:
        """Fetch the latest stored snapshot before a target epoch for each validator."""

        if not validator_indices:
            return []
        placeholders = ",".join("?" for _ in validator_indices)
        rows = self.connection.execute(
            f"""
            SELECT current.validator_index, current.epoch, current.balance_gwei, current.effective_balance_gwei, current.status
            FROM validator_snapshots AS current
            INNER JOIN (
              SELECT validator_index, MAX(epoch) AS epoch
              FROM validator_snapshots
              WHERE epoch < ? AND validator_index IN ({placeholders})
              GROUP BY validator_index
            ) AS latest
              ON latest.validator_index = current.validator_index
             AND latest.epoch = current.epoch
            ORDER BY current.validator_index ASC
            """,
            (epoch, *validator_indices),
        ).fetchall()
        return [self._row_to_validator_snapshot(row) for row in rows]

    def get_latest_validator_slot_snapshots_before(
        self,
        slot: int,
        validator_indices: list[int],
    ) -> list[ValidatorSnapshot]:
        """Fetch the latest stored slot snapshot before a target slot for each validator."""

        if not validator_indices:
            return []
        placeholders = ",".join("?" for _ in validator_indices)
        rows = self.connection.execute(
            f"""
            SELECT current.validator_index, current.slot, current.epoch, current.balance_gwei, current.effective_balance_gwei, current.status
            FROM validator_slot_snapshots AS current
            INNER JOIN (
              SELECT validator_index, MAX(slot) AS slot
              FROM validator_slot_snapshots
              WHERE slot < ? AND validator_index IN ({placeholders})
              GROUP BY validator_index
            ) AS latest
              ON latest.validator_index = current.validator_index
             AND latest.slot = current.slot
            ORDER BY current.validator_index ASC
            """,
            (slot, *validator_indices),
        ).fetchall()
        return [self._row_to_validator_snapshot(row) for row in rows]

    def list_latest_entity_snapshots(self, limit: int = 100) -> list[EntitySummary]:
        """List the latest stored entity leaderboard snapshot."""

        latest_epoch_row = self.connection.execute(
            "SELECT MAX(snapshot_epoch) AS latest_epoch FROM entity_snapshots"
        ).fetchone()
        if not latest_epoch_row or latest_epoch_row["latest_epoch"] is None:
            return []
        rows = self.connection.execute(
            """
            SELECT entity, validator_count, sub_entity_count, beaconscore, net_share, apr, apy
            FROM entity_snapshots
            WHERE snapshot_epoch = ?
            ORDER BY validator_count DESC, entity ASC
            LIMIT ?
            """,
            (int(latest_epoch_row["latest_epoch"]), limit),
        ).fetchall()
        return [self._row_to_entity_summary(row) for row in rows]

    def get_latest_entity_snapshot(self, entity: str) -> EntitySummary | None:
        """Fetch the latest stored summary for a specific entity."""

        row = self.connection.execute(
            """
            SELECT entity, validator_count, sub_entity_count, beaconscore, net_share, apr, apy
            FROM entity_snapshots
            WHERE entity = ?
            ORDER BY snapshot_epoch DESC
            LIMIT 1
            """,
            (entity,),
        ).fetchone()
        return self._row_to_entity_summary(row) if row else None

    def get_entity_reward_sync_state(self, entity: str) -> tuple[int, int] | None:
        """Return the first and latest locally synced reward epochs for an entity."""

        row = self.connection.execute(
            """
            SELECT tracking_start_epoch, latest_reward_epoch
            FROM entity_reward_sync_state
            WHERE entity = ?
            """,
            (entity,),
        ).fetchone()
        if not row:
            return None
        return int(row["tracking_start_epoch"]), int(row["latest_reward_epoch"])

    def get_validator_reward_totals(self, validator_indices: list[int]) -> dict[int, dict[str, int]]:
        """Aggregate locally tracked validator rewards across synced epochs."""

        if not validator_indices:
            return {}
        placeholders = ",".join("?" for _ in validator_indices)
        rows = self.connection.execute(
            f"""
            SELECT
              validator_index,
              COALESCE(SUM(total_reward_wei), 0) AS cumulative_reward_wei,
              COALESCE(SUM(total_penalty_wei), 0) AS cumulative_penalty_wei,
              COALESCE(SUM(realized_loss_wei), 0) AS cumulative_loss_wei
            FROM validator_reward_snapshots
            WHERE validator_index IN ({placeholders})
            GROUP BY validator_index
            """,
            tuple(validator_indices),
        ).fetchall()
        return {
            int(row["validator_index"]): {
                "cumulative_reward_wei": int(row["cumulative_reward_wei"]),
                "cumulative_penalty_wei": int(row["cumulative_penalty_wei"]),
                "cumulative_loss_wei": int(row["cumulative_loss_wei"]),
            }
            for row in rows
        }

    def list_latest_entity_validator_snapshots(self, entity: str) -> list[EntityValidatorSnapshot]:
        """Fetch the latest stored mapping snapshot for an entity."""

        latest_epoch_row = self.connection.execute(
            """
            SELECT MAX(snapshot_epoch) AS latest_epoch
            FROM entity_validator_snapshots
            WHERE entity = ?
            """,
            (entity,),
        ).fetchone()
        if not latest_epoch_row or latest_epoch_row["latest_epoch"] is None:
            return []
        rows = self.connection.execute(
            """
            SELECT *
            FROM entity_validator_snapshots
            WHERE entity = ? AND snapshot_epoch = ?
            ORDER BY cumulative_reward_wei DESC, validator_index ASC
            """,
            (entity, int(latest_epoch_row["latest_epoch"])),
        ).fetchall()
        return [self._row_to_entity_validator_snapshot(row) for row in rows]

    def get_scanned_activity_slots(self, start_slot: int, end_slot: int) -> set[int]:
        """Return the subset of finalized slots already scanned for activity."""

        rows = self.connection.execute(
            """
            SELECT slot
            FROM validator_activity_scanned_slots
            WHERE slot BETWEEN ? AND ?
            """,
            (start_slot, end_slot),
        ).fetchall()
        return {int(row["slot"]) for row in rows}

    def list_validator_activity_summaries(
        self,
        start_slot: int,
        end_slot: int,
        *,
        limit: int = 100,
    ) -> list[ValidatorActivitySummary]:
        """Aggregate validator deposit, withdrawal, and slashing activity over a slot range."""

        rows = self.connection.execute(
            """
            SELECT
              validator_index,
              public_key,
              COALESCE(SUM(deposit_gwei), 0) AS deposit_gwei,
              COALESCE(SUM(withdrawal_gwei), 0) AS withdrawal_gwei,
              COALESCE(SUM(proposer_slashings), 0) AS proposer_slashings,
              COALESCE(SUM(attester_slashings), 0) AS attester_slashings
            FROM validator_activity_slots
            WHERE slot BETWEEN ? AND ?
            GROUP BY validator_index, public_key
            HAVING (COALESCE(SUM(deposit_gwei), 0) + COALESCE(SUM(withdrawal_gwei), 0)) > 0
                OR (COALESCE(SUM(proposer_slashings), 0) + COALESCE(SUM(attester_slashings), 0)) > 0
            ORDER BY
              (COALESCE(SUM(deposit_gwei), 0) + COALESCE(SUM(withdrawal_gwei), 0)) DESC,
              validator_index ASC
            LIMIT ?
            """,
            (start_slot, end_slot, limit),
        ).fetchall()
        return [
            ValidatorActivitySummary(
                validator_index=int(row["validator_index"]),
                public_key=str(row["public_key"]),
                deposit_gwei=int(row["deposit_gwei"]),
                withdrawal_gwei=int(row["withdrawal_gwei"]),
                proposer_slashings=int(row["proposer_slashings"]),
                attester_slashings=int(row["attester_slashings"]),
            )
            for row in rows
        ]

    def get_cumulative_net_user_flow_wei(self, pool_id: str, through_epoch: int) -> int:
        """Return cumulative net user flows by summing prior pool snapshots."""

        row = self.connection.execute(
            """
            SELECT COALESCE(SUM(net_user_flow_wei), 0) AS cumulative_flow
            FROM pool_snapshots
            WHERE pool_id = ? AND epoch <= ?
            """,
            (pool_id, through_epoch),
        ).fetchone()
        return int(row["cumulative_flow"]) if row else 0

    @staticmethod
    def _row_to_validator_snapshot(row: sqlite3.Row) -> ValidatorSnapshot:
        return ValidatorSnapshot(
            validator_index=int(row["validator_index"]),
            epoch=int(row["epoch"]),
            balance_gwei=int(row["balance_gwei"]),
            effective_balance_gwei=int(row["effective_balance_gwei"]),
            status=str(row["status"]),
            slot=int(row["slot"]) if "slot" in row.keys() and row["slot"] is not None else None,
        )

    @staticmethod
    def _row_to_pool_snapshot(row: sqlite3.Row) -> PoolSnapshot:
        return PoolSnapshot(
            pool_id=str(row["pool_id"]),
            epoch=int(row["epoch"]),
            total_validator_balance_gwei=int(row["total_validator_balance_gwei"]),
            gross_rewards_gwei=int(row["gross_rewards_gwei"]),
            penalties_gwei=int(row["penalties_gwei"]),
            slashing_losses_gwei=int(row["slashing_losses_gwei"]),
            fees_gwei=int(row["fees_gwei"]),
            net_rewards_gwei=int(row["net_rewards_gwei"]),
            net_user_flow_wei=int(row["net_user_flow_wei"]),
            nav_gwei=int(row["nav_gwei"]),
            total_shares=float(row["total_shares"]),
            share_price_gwei=float(row["share_price_gwei"]),
            cumulative_pnl_gwei=int(row["cumulative_pnl_gwei"]),
            slot=int(row["slot"]) if "slot" in row.keys() and row["slot"] is not None else None,
        )

    @staticmethod
    def _row_to_entity_summary(row: sqlite3.Row) -> EntitySummary:
        return EntitySummary(
            entity=str(row["entity"]),
            validator_count=int(row["validator_count"]),
            sub_entity_count=int(row["sub_entity_count"]),
            beaconscore=float(row["beaconscore"]) if row["beaconscore"] is not None else None,
            net_share=float(row["net_share"]) if row["net_share"] is not None else None,
            apr=float(row["apr"]) if row["apr"] is not None else None,
            apy=float(row["apy"]) if row["apy"] is not None else None,
        )

    @staticmethod
    def _row_to_entity_validator_snapshot(row: sqlite3.Row) -> EntityValidatorSnapshot:
        return EntityValidatorSnapshot(
            entity=str(row["entity"]),
            snapshot_epoch=int(row["snapshot_epoch"]),
            reward_epoch=int(row["reward_epoch"]),
            validator_index=int(row["validator_index"]),
            public_key=str(row["public_key"]),
            status=str(row["status"]),
            balance_gwei=int(row["balance_gwei"]),
            effective_balance_gwei=int(row["effective_balance_gwei"]),
            cumulative_reward_wei=int(row["cumulative_reward_wei"]),
            cumulative_penalty_wei=int(row["cumulative_penalty_wei"]),
            cumulative_loss_wei=int(row["cumulative_loss_wei"]),
            tracking_start_epoch=int(row["tracking_start_epoch"]),
            finality=str(row["finality"]),
            online=bool(row["online"]) if row["online"] is not None else None,
        )

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        self.connection.close()
