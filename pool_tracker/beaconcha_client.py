"""BeaconCha v2 client helpers for Hoodi entity and reward data."""

from __future__ import annotations

from typing import Any, Iterable

import requests

from .models import EntitySummary, EntityValidator, ValidatorRewardSnapshot

BEACONCHA_DEFAULT_BASE_URL = "https://beaconcha.in"
BEACONCHA_MAX_PAGE_SIZE = 10


class BeaconChaError(RuntimeError):
    """Base exception for BeaconCha API failures."""


class BeaconChaPermissionError(BeaconChaError):
    """Raised when the current subscription tier cannot access an endpoint."""


class BeaconChaFeatureUnavailableError(BeaconChaError):
    """Raised when BeaconCha reports that a selector or feature is unavailable."""


class BeaconChaClient:
    """Thin client around BeaconCha v2 entity and validator analytics endpoints."""

    def __init__(
        self,
        api_key: str,
        base_url: str = BEACONCHA_DEFAULT_BASE_URL,
        session: requests.Session | None = None,
        timeout: int = 30,
        chain: str = "hoodi",
    ) -> None:
        if not api_key.strip():
            raise ValueError("A BeaconCha API key is required.")
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout
        self.chain = chain

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _as_int(value: Any, default: int = 0) -> int:
        if value is None or value == "":
            return default
        return int(value)

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None or value == "":
            return None
        return float(value)

    @staticmethod
    def _get_value(payload: dict[str, Any], *keys: str) -> Any:
        for key in keys:
            if key in payload:
                return payload[key]
        return None

    @staticmethod
    def _nested_int(payload: dict[str, Any], *path: str) -> int:
        current: Any = payload
        for key in path:
            if not isinstance(current, dict):
                return 0
            current = current.get(key)
        return BeaconChaClient._as_int(current)

    @staticmethod
    def _extract_error(payload: dict[str, Any]) -> str | None:
        for key in ("error", "message", "detail"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return None

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        body = dict(payload)
        body["chain"] = self.chain
        response = self.session.post(
            f"{self.base_url}{path}",
            headers=self._headers(),
            json=body,
            timeout=self.timeout,
        )
        try:
            decoded = response.json()
        except ValueError:
            decoded = {}

        if response.status_code == 403:
            message = self._extract_error(decoded) or "BeaconCha denied access to this endpoint."
            raise BeaconChaPermissionError(message)
        if response.status_code == 400:
            message = self._extract_error(decoded) or "BeaconCha rejected the request."
            raise BeaconChaFeatureUnavailableError(message)
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            message = self._extract_error(decoded) or str(exc)
            raise BeaconChaError(message) from exc

        if not isinstance(decoded, dict):
            raise BeaconChaError(f"BeaconCha returned an invalid payload for {path}.")
        return decoded

    @staticmethod
    def _next_cursor(payload: dict[str, Any]) -> str:
        paging = payload.get("paging")
        if isinstance(paging, dict):
            cursor = paging.get("next_cursor") or paging.get("cursor") or ""
            return str(cursor or "")
        cursor = payload.get("next_cursor") or payload.get("cursor") or ""
        return str(cursor or "")

    def list_top_entities(
        self,
        limit: int = 100,
        evaluation_window: str = "24h",
    ) -> list[EntitySummary]:
        """List the largest entities on Hoodi by validator count."""

        entities: list[EntitySummary] = []
        cursor = ""
        while len(entities) < limit:
            page_size = min(BEACONCHA_MAX_PAGE_SIZE, limit - len(entities))
            payload = self._post(
                "/api/v2/ethereum/entities",
                {
                    "range": {"evaluation_window": evaluation_window},
                    "cursor": cursor,
                    "page_size": page_size,
                    "sort_by": "validator_count",
                    "sort_order": "desc",
                },
            )
            batch = payload.get("data", [])
            if not isinstance(batch, list) or not batch:
                break
            for item in batch:
                if not isinstance(item, dict):
                    continue
                entity_name = str(self._get_value(item, "entity", "name") or "").strip()
                if not entity_name:
                    continue
                entities.append(
                    EntitySummary(
                        entity=entity_name,
                        validator_count=self._as_int(item.get("validator_count")),
                        sub_entity_count=self._as_int(item.get("sub_entity_count")),
                        beaconscore=self._as_float(item.get("beaconscore")),
                        net_share=self._as_float(item.get("net_share")),
                        apr=self._as_float(self._get_value(item, "apr", "apr_mean")),
                        apy=self._as_float(self._get_value(item, "apy", "apy_mean")),
                    )
                )
                if len(entities) >= limit:
                    break

            cursor = self._next_cursor(payload)
            if not cursor:
                break
        return entities[:limit]

    def list_validators_by_entity(
        self,
        entity: str,
        *,
        sub_entity: str | None = None,
    ) -> list[EntityValidator]:
        """List validators mapped to an entity when the selector is available."""

        validators: list[EntityValidator] = []
        cursor = ""
        while True:
            selector: dict[str, Any] = {"entity": entity}
            if sub_entity:
                selector["sub_entity"] = sub_entity
            payload = self._post(
                "/api/v2/ethereum/validators",
                {
                    "validator": selector,
                    "cursor": cursor,
                    "page_size": BEACONCHA_MAX_PAGE_SIZE,
                },
            )
            batch = payload.get("data", [])
            if not isinstance(batch, list) or not batch:
                break
            for item in batch:
                if not isinstance(item, dict):
                    continue
                nested_validator = item.get("validator")
                validator_payload = nested_validator if isinstance(nested_validator, dict) else item
                validator_index = self._as_int(
                    self._get_value(validator_payload, "validator_index", "index"),
                    default=-1,
                )
                if validator_index < 0:
                    continue
                public_key = str(
                    self._get_value(validator_payload, "public_key", "pubkey", "validator_pubkey") or ""
                ).strip()
                validators.append(
                    EntityValidator(
                        entity=entity,
                        validator_index=validator_index,
                        public_key=public_key,
                        status=str(self._get_value(item, "status", "validator_status") or "unknown"),
                        balance_gwei=self._as_int(self._get_value(item, "balance", "current_balance")),
                        effective_balance_gwei=self._as_int(
                            self._get_value(
                                validator_payload,
                                "effective_balance",
                                "effective_balance_gwei",
                            )
                        ),
                        finality=str(item.get("finality") or "unknown"),
                        online=item.get("online"),
                    )
                )
            cursor = self._next_cursor(payload)
            if not cursor:
                break
        return validators

    def get_validator_rewards(
        self,
        validator_indices: Iterable[int],
        epoch: int,
    ) -> list[ValidatorRewardSnapshot]:
        """Fetch finalized reward/penalty rows for a specific epoch."""

        identifiers = [str(index) for index in validator_indices]
        rewards: list[ValidatorRewardSnapshot] = []
        for start in range(0, len(identifiers), BEACONCHA_MAX_PAGE_SIZE):
            chunk = identifiers[start : start + BEACONCHA_MAX_PAGE_SIZE]
            if not chunk:
                continue
            payload = self._post(
                "/api/v2/ethereum/validators/rewards-list",
                {
                    "validator_identifiers": chunk,
                    "epoch": epoch,
                },
            )
            batch = payload.get("data", [])
            if not isinstance(batch, list):
                continue
            for item in batch:
                if not isinstance(item, dict):
                    continue
                validator_index = self._as_int(self._get_value(item, "validator_index", "index"), default=-1)
                if validator_index < 0:
                    continue
                total_wei = self._as_int(item.get("total"))
                rewards.append(
                    ValidatorRewardSnapshot(
                        validator_index=validator_index,
                        public_key=str(self._get_value(item, "public_key", "pubkey") or "").strip(),
                        epoch=self._as_int(item.get("epoch"), default=epoch),
                        total_wei=total_wei,
                        total_reward_wei=self._as_int(item.get("total_reward")),
                        total_penalty_wei=self._as_int(item.get("total_penalty")),
                        total_missed_wei=self._as_int(item.get("total_missed")),
                        realized_loss_wei=max(-total_wei, 0),
                        attestations_source_reward_wei=self._nested_int(
                            item, "attestations", "source", "reward"
                        ),
                        attestations_target_reward_wei=self._nested_int(
                            item, "attestations", "target", "reward"
                        ),
                        attestations_head_reward_wei=self._nested_int(item, "attestations", "head", "reward"),
                        attestations_source_penalty_wei=self._nested_int(
                            item, "attestations", "source", "penalty"
                        ),
                        attestations_target_penalty_wei=self._nested_int(
                            item, "attestations", "target", "penalty"
                        ),
                        sync_reward_wei=self._nested_int(item, "sync", "reward"),
                        sync_penalty_wei=self._nested_int(item, "sync", "penalty"),
                        slashing_reward_wei=self._nested_int(item, "slashing", "reward"),
                        slashing_penalty_wei=self._nested_int(item, "slashing", "penalty"),
                        proposal_reward_cl_wei=self._nested_int(item, "proposal", "reward_cl"),
                        proposal_reward_el_wei=self._nested_int(item, "proposal", "reward_el"),
                        proposal_missed_reward_cl_wei=self._nested_int(
                            item, "proposal", "missed_reward_cl"
                        ),
                        proposal_missed_reward_el_wei=self._nested_int(
                            item, "proposal", "missed_reward_el"
                        ),
                        finality=str(item.get("finality") or "unknown"),
                    )
                )
        return rewards
