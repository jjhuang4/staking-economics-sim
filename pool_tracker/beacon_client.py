"""Hoodi Beacon API client."""

from __future__ import annotations

from typing import Any

import requests

from .models import ValidatorSnapshot


class BeaconClient:
    """Thin client around Beacon API validator endpoints."""

    def __init__(
        self,
        base_url: str,
        session: requests.Session | None = None,
        timeout: int = 30,
    ) -> None:
        normalized_base_url = base_url.rstrip("/")
        if normalized_base_url.endswith("/eth/v1"):
            normalized_base_url = normalized_base_url[: -len("/eth/v1")]
        self.base_url = normalized_base_url
        self.session = session or requests.Session()
        self.timeout = timeout

    def _get(self, path: str, params: list[tuple[str, str]] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            hint = ""
            if getattr(response, "status_code", None) == 404:
                hint = " Verify that BEACON_API_URL points to the provider base URL before /eth/v1."
            raise RuntimeError(f"Beacon API request failed for {url}: {exc}.{hint}".strip()) from exc
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"Beacon API returned an invalid payload for {url}.")
        return payload

    @staticmethod
    def _build_id_params(ids: list[int]) -> list[tuple[str, str]]:
        return [("id", str(validator_id)) for validator_id in ids]

    def get_validators(self, state_id: str, ids: list[int]) -> dict[int, dict[str, Any]]:
        """Fetch validator metadata and status for the provided ids."""

        if not ids:
            return {}
        payload = self._get(
            f"/eth/v1/beacon/states/{state_id}/validators",
            params=self._build_id_params(ids),
        )
        validators: dict[int, dict[str, Any]] = {}
        for item in payload.get("data", []):
            if not isinstance(item, dict):
                continue
            validator_data = item.get("validator", {})
            index = int(item["index"])
            validators[index] = {
                "status": str(item.get("status", "unknown")),
                "balance_gwei": int(item.get("balance", "0")),
                "effective_balance_gwei": int(validator_data.get("effective_balance", "0")),
                "validator": validator_data,
            }
        return validators

    def get_header(self, block_id: str = "head") -> dict[str, Any]:
        """Fetch a Beacon block header payload."""

        payload = self._get(f"/eth/v1/beacon/headers/{block_id}")
        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Beacon header response did not include a data object.")
        return data

    def get_head_slot(self, block_id: str = "head") -> int:
        """Fetch the slot number for a Beacon header reference like head or finalized."""

        header = self.get_header(block_id=block_id)
        header_container = header.get("header", {})
        message = header_container.get("message", {})
        slot_value = message.get("slot")
        if slot_value is None:
            raise RuntimeError("Beacon header response did not include a slot.")
        return int(slot_value)

    def get_finalized_epoch(self, state_id: str = "head") -> int:
        """Fetch the finalized epoch from Beacon finality checkpoints."""

        payload = self._get(f"/eth/v1/beacon/states/{state_id}/finality_checkpoints")
        data = payload.get("data", {})
        finalized = data.get("finalized", {})
        epoch_value = finalized.get("epoch")
        if epoch_value is None:
            raise RuntimeError("Beacon finality checkpoints response did not include a finalized epoch.")
        return int(epoch_value)

    def get_validator_balances(self, state_id: str, ids: list[int]) -> dict[int, int]:
        """Fetch validator balances in gwei for the provided ids."""

        if not ids:
            return {}
        payload = self._get(
            f"/eth/v1/beacon/states/{state_id}/validator_balances",
            params=self._build_id_params(ids),
        )
        balances: dict[int, int] = {}
        for item in payload.get("data", []):
            if not isinstance(item, dict):
                continue
            balances[int(item["index"])] = int(item.get("balance", "0"))
        return balances

    def build_validator_snapshots(
        self,
        epoch: int,
        state_id: str,
        ids: list[int],
    ) -> list[ValidatorSnapshot]:
        """Build normalized validator snapshots for a given Beacon state."""

        validator_data = self.get_validators(state_id, ids)
        balance_data = self.get_validator_balances(state_id, ids)
        snapshots: list[ValidatorSnapshot] = []
        for validator_index in ids:
            metadata = validator_data.get(validator_index, {})
            balance_gwei = balance_data.get(
                validator_index,
                int(metadata.get("balance_gwei", 0)),
            )
            effective_balance_gwei = int(metadata.get("effective_balance_gwei", 0))
            status = str(metadata.get("status", "unknown"))
            snapshots.append(
                ValidatorSnapshot(
                    validator_index=validator_index,
                    epoch=epoch,
                    balance_gwei=balance_gwei,
                    effective_balance_gwei=effective_balance_gwei,
                    status=status,
                )
            )
        return snapshots
