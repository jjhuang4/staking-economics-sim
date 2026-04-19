"""Shared API layer support for Alchemy-backed provider calls."""

from __future__ import annotations

from typing import Any

import requests


class AlchemyError(RuntimeError):
    """Base exception for Alchemy API failures."""


class AlchemyClient:
    """Thin wrapper for Alchemy provider HTTP endpoints."""

    def __init__(
        self,
        api_key: str,
        base_url: str = "https://eth-mainnet.g.alchemy.com/v2",
        session: requests.Session | None = None,
        timeout: int = 30,
    ) -> None:
        if not api_key.strip():
            raise ValueError("An Alchemy API key is required.")
        self.api_key = api_key.strip()
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
        }

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}/{self.api_key}/{path.lstrip('/')}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        self._raise_for_status(response)
        return response.json()

    def _post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        url = f"{self.base_url}/{self.api_key}/{path.lstrip('/')}"
        response = self.session.post(url, headers=self._headers(), json=payload, timeout=self.timeout)
        self._raise_for_status(response)
        return response.json()

    @staticmethod
    def _raise_for_status(response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise AlchemyError(f"Alchemy API request failed: {exc}") from exc
