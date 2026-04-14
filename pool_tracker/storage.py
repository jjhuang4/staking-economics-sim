"""SQLite persistence for pool tracker snapshots."""

from __future__ import annotations

import sqlite3

from .models import PoolFlow, PoolSnapshot, ValidatorSnapshot


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
        self.connection.commit()

    def upsert_validator_snapshot(self, snapshot: ValidatorSnapshot) -> None:
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
        self.connection.commit()

    def upsert_pool_flow(self, flow: PoolFlow) -> None:
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
        self.connection.commit()

    def upsert_pool_snapshot(self, snapshot: PoolSnapshot) -> None:
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
        )

    def close(self) -> None:
        """Close the underlying SQLite connection."""

        self.connection.close()
