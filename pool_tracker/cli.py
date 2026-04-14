"""Typer CLI for the pool tracker package."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import typer

from .tracker import PoolTracker

app = typer.Typer(add_completion=False, no_args_is_help=True)


def _json_default(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return asdict(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _echo_json(payload: Any) -> None:
    typer.echo(json.dumps(payload, default=_json_default, indent=2, sort_keys=True))


@app.command("sync-epoch")
def sync_epoch(pool_config: Path = typer.Option(..., exists=True), epoch: int = typer.Option(...)) -> None:
    """Sync a single epoch and print the resulting snapshot JSON."""

    tracker = PoolTracker.from_config(str(pool_config))
    snapshot = tracker.sync_epoch(epoch)
    _echo_json(asdict(snapshot))


@app.command("sync-range")
def sync_range(
    pool_config: Path = typer.Option(..., exists=True),
    start_epoch: int = typer.Option(..., "--start-epoch"),
    end_epoch: int = typer.Option(..., "--end-epoch"),
) -> None:
    """Sync an inclusive epoch range and print snapshot JSON."""

    tracker = PoolTracker.from_config(str(pool_config))
    snapshots = tracker.sync_range(start_epoch, end_epoch)
    _echo_json([asdict(snapshot) for snapshot in snapshots])


@app.command("show-snapshot")
def show_snapshot(pool_config: Path = typer.Option(..., exists=True), epoch: int = typer.Option(...)) -> None:
    """Read a stored snapshot from SQLite and print it as JSON."""

    tracker = PoolTracker.from_config(str(pool_config))
    snapshot = tracker.storage.get_pool_snapshot(tracker.pool.pool_id, epoch)
    if snapshot is None:
        raise typer.Exit(code=1)
    _echo_json(asdict(snapshot))


if __name__ == "__main__":
    app()
