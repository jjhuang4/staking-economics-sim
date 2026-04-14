"""Configuration loading for pool tracker."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .models import Pool


class ConfigError(ValueError):
    """Raised when settings or pool configuration are invalid."""


@dataclass
class Settings:
    execution_rpc_url: str
    beacon_api_url: str
    db_path: str = "pool_tracker.db"
    network: str = "hoodi"


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, value = stripped.partition("=")
        if not separator:
            continue
        parsed_value = value.strip()
        if len(parsed_value) >= 2 and parsed_value[0] == parsed_value[-1] and parsed_value[0] in {'"', "'"}:
            parsed_value = parsed_value[1:-1]
        values[key.strip()] = parsed_value
    return values


def _load_repo_env_file_values() -> dict[str, str]:
    repo_root = Path(__file__).resolve().parent.parent
    return _read_env_file(repo_root / ".env")


def resolve_env_value(*keys: str, default: str = "") -> str:
    """Resolve a setting from process env first, then the repo-level `.env` file."""

    env_file_values = _load_repo_env_file_values()
    for key in keys:
        process_value = os.getenv(key, "").strip()
        if process_value:
            return process_value
        file_value = env_file_values.get(key, "").strip()
        if file_value:
            return file_value
    return default


def _load_structured_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    content = path.read_text(encoding="utf-8")
    if suffix == ".json":
        data = json.loads(content)
    elif suffix in {".yaml", ".yml"}:
        data = yaml.safe_load(content)
    else:
        raise ConfigError(f"Unsupported pool config format: {path.suffix}")
    if not isinstance(data, dict):
        raise ConfigError("Pool config must deserialize to a mapping.")
    return data


def _require_non_empty_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Config field '{key}' must be a non-empty string.")
    return value.strip()


def _require_float(data: dict[str, Any], key: str) -> float:
    value = data.get(key)
    if not isinstance(value, (int, float)):
        raise ConfigError(f"Config field '{key}' must be numeric.")
    return float(value)


def _require_int_list(data: dict[str, Any], key: str) -> list[int]:
    value = data.get(key)
    if not isinstance(value, list) or not all(isinstance(item, int) for item in value):
        raise ConfigError(f"Config field '{key}' must be a list of integers.")
    return list(value)


def _require_str_list(data: dict[str, Any], key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not all(isinstance(item, str) and item.strip() for item in value):
        raise ConfigError(f"Config field '{key}' must be a list of strings.")
    return [item.strip() for item in value]


def load_pool_config(path: str) -> Pool:
    """Load a Pool definition from YAML or JSON."""

    config_path = Path(path)
    if not config_path.exists():
        raise ConfigError(f"Pool config file not found: {path}")

    data = _load_structured_file(config_path)
    fee_rate = _require_float(data, "fee_rate")
    slash_pass_through = _require_float(data, "slash_pass_through")
    if not 0.0 <= fee_rate <= 1.0:
        raise ConfigError("fee_rate must be between 0.0 and 1.0.")
    if slash_pass_through < 0.0:
        raise ConfigError("slash_pass_through must be non-negative.")

    return Pool(
        pool_id=_require_non_empty_str(data, "pool_id"),
        name=_require_non_empty_str(data, "name"),
        fee_rate=fee_rate,
        slash_pass_through=slash_pass_through,
        validator_indices=_require_int_list(data, "validator_indices"),
        contract_addresses=_require_str_list(data, "contract_addresses"),
    )


def load_settings() -> Settings:
    """Load Settings from environment variables."""

    execution_rpc_url = resolve_env_value("EXECUTION_RPC_URL", "HOODI_EXECUTION_RPC_URL")
    beacon_api_url = resolve_env_value("BEACON_API_URL", "HOODI_BEACON_API_URL")
    db_path = resolve_env_value("POOL_TRACKER_DB_PATH", default="pool_tracker.db") or "pool_tracker.db"
    network = resolve_env_value("POOL_TRACKER_NETWORK", default="hoodi") or "hoodi"

    if not execution_rpc_url:
        raise ConfigError("EXECUTION_RPC_URL is required.")
    if not beacon_api_url:
        raise ConfigError("BEACON_API_URL is required.")
    if network.lower() != "hoodi":
        raise ConfigError("pool_tracker v1 only supports Hoodi.")

    return Settings(
        execution_rpc_url=execution_rpc_url,
        beacon_api_url=beacon_api_url,
        db_path=db_path,
        network=network.lower(),
    )
