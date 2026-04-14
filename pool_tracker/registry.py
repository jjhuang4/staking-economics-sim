"""Thin pool registry wrapper."""

from __future__ import annotations

from .models import Pool


class PoolRegistry:
    """Expose the configured pool's validators and contracts."""

    def __init__(self, pool: Pool) -> None:
        self.pool = pool

    def get_validator_indices(self) -> list[int]:
        """Return configured validator indices."""

        return list(self.pool.validator_indices)

    def get_contract_addresses(self) -> list[str]:
        """Return configured execution-layer contract addresses."""

        return list(self.pool.contract_addresses)
