"""Hoodi Beacon API client."""

from __future__ import annotations

import time
from typing import Any

import requests

from .models import ValidatorSnapshot


class BeaconClient:
    """Thin client around Beacon API validator endpoints."""

    MAX_REQUEST_ATTEMPTS = 3

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

    def _get(
        self,
        path: str,
        params: list[tuple[str, str]] | None = None,
        *,
        allow_404: bool = False,
    ) -> dict[str, Any] | None:
        url = f"{self.base_url}{path}"
        response: Any = None
        for attempt in range(1, self.MAX_REQUEST_ATTEMPTS + 1):
            response = self.session.get(url, params=params, timeout=self.timeout)
            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                status_code = getattr(response, "status_code", None)
                if allow_404 and status_code == 404:
                    return None
                if status_code == 429 and attempt < self.MAX_REQUEST_ATTEMPTS:
                    retry_after = getattr(response, "headers", {}).get("Retry-After")
                    try:
                        delay_seconds = max(float(retry_after), 0.5) if retry_after is not None else 0.5 * (2 ** (attempt - 1))
                    except (TypeError, ValueError):
                        delay_seconds = 0.5 * (2 ** (attempt - 1))
                    time.sleep(delay_seconds)
                    continue

                hint = ""
                if status_code == 404:
                    hint = " Verify that BEACON_API_URL points to the provider base URL before /eth/v1."
                if status_code == 429:
                    hint = " Provider rate limit hit. Reduce refresh pressure or wait for the local cache to warm."
                raise RuntimeError(f"Beacon API request failed for {url}: {exc}.{hint}".strip()) from exc

            payload = response.json()
            if not isinstance(payload, dict):
                raise RuntimeError(f"Beacon API returned an invalid payload for {url}.")
            return payload
        raise RuntimeError(f"Beacon API request failed for {url}: exceeded retry budget.")

    @staticmethod
    def _build_id_params(ids: list[int | str]) -> list[tuple[str, str]]:
        return [("id", str(validator_id)) for validator_id in ids]

    def get_validators(self, state_id: str, ids: list[int | str]) -> dict[int, dict[str, Any]]:
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

    def resolve_state_id(self, state_id: str) -> str:
        """Resolve historical numeric slot references to state roots for provider compatibility."""

        normalized_state_id = str(state_id).strip()
        if normalized_state_id.isdigit():
            try:
                header = self.get_header(block_id=normalized_state_id)
            except RuntimeError:
                return normalized_state_id
            header_container = header.get("header", {})
            message = header_container.get("message", {})
            state_root = message.get("state_root")
            if state_root is None:
                raise RuntimeError(f"Beacon header response for slot {state_id} did not include a state_root.")
            return str(state_root)
        return normalized_state_id

    def get_block(self, block_id: str | int) -> dict[str, Any] | None:
        """Fetch a full Beacon block payload, returning None for skipped slots."""

        payload = self._get(f"/eth/v2/beacon/blocks/{block_id}", allow_404=True)
        if payload is None:
            return None
        data = payload.get("data")
        if not isinstance(data, dict):
            raise RuntimeError("Beacon block response did not include a data object.")
        return data

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

        resolved_state_id = self.resolve_state_id(state_id)
        validator_data = self.get_validators(resolved_state_id, ids)
        snapshots: list[ValidatorSnapshot] = []
        for validator_index in ids:
            metadata = validator_data.get(validator_index, {})
            balance_gwei = int(metadata.get("balance_gwei", 0))
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
