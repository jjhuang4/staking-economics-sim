"""Simulation-only backend helpers for the Streamlit dashboard."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from functools import lru_cache
import copy
import importlib
import json
import logging
import os
from pathlib import Path
import sys
from typing import Any

import numpy as np
import pandas as pd
import requests


ROOT_DIR = Path(__file__).resolve().parents[1]
ETHEREUM_MODEL_DIR = ROOT_DIR / "ethereum-economic-model"
DEFAULT_BEACON_VALIDATOR_COUNT = 156_250
DEFAULT_BEACON_TOTAL_BALANCE_GWEI = int(5_000_000e9)
DEFAULT_ETH_SUPPLY_WEI = int(116_250_000e18)
SCENARIO_LABELS = {
    0: "Normal adoption",
    1: "Low adoption",
    2: "High adoption",
}


class _OfflineResponse:
    """Small response stub used to short-circuit live HTTP imports."""

    def __init__(self, payload: dict[str, Any]):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


@contextmanager
def _offline_import_requests():
    """Temporarily replace live network reads with deterministic local defaults."""

    original_get = requests.get
    original_post = requests.post

    def fake_get(url: str, *args: Any, **kwargs: Any) -> _OfflineResponse:
        if "beaconcha.in/api/v1/epoch/" in url:
            return _OfflineResponse(
                {
                    "data": {
                        "validatorscount": DEFAULT_BEACON_VALIDATOR_COUNT,
                        "totalvalidatorbalance": DEFAULT_BEACON_TOTAL_BALANCE_GWEI,
                    }
                }
            )
        if "api.etherscan.io/api?module=stats&action=ethsupply" in url:
            return _OfflineResponse({"result": str(DEFAULT_ETH_SUPPLY_WEI)})
        return original_get(url, *args, **kwargs)

    def fake_post(url: str, *args: Any, **kwargs: Any) -> _OfflineResponse:
        if "gateway.thegraph.com" in url:
            return _OfflineResponse({"data": {}})
        return original_post(url, *args, **kwargs)

    requests.get = fake_get
    requests.post = fake_post
    try:
        yield
    finally:
        requests.get = original_get
        requests.post = original_post


@contextmanager
def _cadlabs_log_quietly(level: int = logging.WARNING):
    """Reduce radCAD experiment log noise while the dashboard computes."""

    root_logger = logging.getLogger()
    prior_level = root_logger.level
    root_logger.setLevel(level)
    try:
        yield
    finally:
        root_logger.setLevel(prior_level)


@dataclass(frozen=True)
class ValidatorEnvironmentAssumption:
    """Static validator-environment metadata taken from the model."""

    environment_key: str
    environment: str
    share_pct: float
    hardware_cost_usd_per_epoch: float
    cloud_cost_usd_per_epoch: float
    third_party_cost_pct_of_rewards: float


@dataclass(frozen=True)
class ModelBundle:
    """Imported economic-model modules and extracted defaults."""

    constants: Any
    stage_all: Any
    run_experiment: Any
    time_domain_template: Any
    stake_sweep_template: Any
    price_sweep_template: Any
    validator_environments: tuple[ValidatorEnvironmentAssumption, ...]
    defaults: dict[str, Any]


@dataclass(frozen=True)
class DashboardAssumptions:
    """User-facing dashboard assumptions and starting state."""

    simulation_time_months: int
    epochs_per_timestep: int
    initial_validator_count: int
    initial_eth_staked: float
    initial_eth_supply: float
    initial_eth_price_usd: float
    validator_adoption_per_epoch: float
    low_adoption_multiplier: float
    high_adoption_multiplier: float
    validator_uptime: float
    slashing_events_per_1000_epochs: int
    mev_per_block_eth: float
    base_fee_gwei: float
    priority_fee_gwei: float
    gas_target_per_block: float
    stake_sweep_points: int
    price_sweep_points: int
    stake_sweep_max_pct_of_supply: float
    stake_price_low_multiplier: float
    stake_price_high_multiplier: float


@dataclass(frozen=True)
class SimulationDashboardSnapshot:
    """All precomputed frames and summary metadata for the dashboard."""

    generated_at: datetime
    assumptions: DashboardAssumptions
    model_defaults: dict[str, Any]
    overview_metrics: dict[str, float]
    time_series_frame: pd.DataFrame
    scenario_summary_frame: pd.DataFrame
    stake_sweep_frame: pd.DataFrame
    price_sweep_frame: pd.DataFrame
    surface_frame: pd.DataFrame
    environment_time_series_frame: pd.DataFrame
    environment_summary_frame: pd.DataFrame
    starting_state_frame: pd.DataFrame
    model_controls_frame: pd.DataFrame
    validator_environment_assumptions_frame: pd.DataFrame
    notes: tuple[str, ...]
    warnings: tuple[str, ...]


def _friendly_label(value: str) -> str:
    return value.replace("_", " ").title()


def _constant_process(value: float | int):
    return lambda _run, _timestep, _value=value: _value


@lru_cache(maxsize=1)
def _load_model_bundle() -> ModelBundle:
    """Import the economic model once, while shielding imports from live HTTP calls."""

    ethereum_model_path = str(ETHEREUM_MODEL_DIR)
    if ethereum_model_path not in sys.path:
        sys.path.insert(0, ethereum_model_path)

    with _offline_import_requests():
        constants = importlib.import_module("model.constants")
        model_types = importlib.import_module("model.types")
        system_parameters = importlib.import_module("model.system_parameters")
        state_variables = importlib.import_module("model.state_variables")
        time_domain_template = importlib.import_module("experiments.templates.time_domain_analysis")
        stake_sweep_template = importlib.import_module("experiments.templates.eth_staked_sweep_analysis")
        price_sweep_template = importlib.import_module("experiments.templates.eth_price_sweep_analysis")
        run_module = importlib.import_module("experiments.run")

    validator_environments = tuple(
        ValidatorEnvironmentAssumption(
            environment_key=environment.type,
            environment=_friendly_label(environment.type),
            share_pct=float(environment.percentage_distribution) * 100.0,
            hardware_cost_usd_per_epoch=float(environment.hardware_costs_per_epoch),
            cloud_cost_usd_per_epoch=float(environment.cloud_costs_per_epoch),
            third_party_cost_pct_of_rewards=float(environment.third_party_costs_per_epoch) * 100.0,
        )
        for environment in system_parameters.validator_environments
    )

    defaults = {
        "simulation_time_months": 36,
        "epochs_per_timestep": int(constants.epochs_per_day),
        "initial_validator_count": int(state_variables.initial_state["number_of_active_validators"]),
        "initial_eth_staked": float(state_variables.initial_state["eth_staked"]),
        "initial_eth_supply": float(state_variables.initial_state["eth_supply"]),
        "initial_eth_price_usd": float(state_variables.initial_state["eth_price"]),
        "validator_adoption_per_epoch": float(
            system_parameters.parameters["validator_process"][0](None, None)
        ),
        "validator_uptime": float(
            system_parameters.parameters["validator_uptime_process"][0](None, None)
        ),
        "slashing_events_per_1000_epochs": int(
            system_parameters.parameters["slashing_events_per_1000_epochs"][0]
        ),
        "mev_per_block_eth": float(system_parameters.parameters["mev_per_block"][0]),
        "base_fee_gwei": float(system_parameters.parameters["base_fee_process"][0](None, None)),
        "priority_fee_gwei": float(
            system_parameters.parameters["priority_fee_process"][0](None, None)
        ),
        "gas_target_per_block": float(
            system_parameters.parameters["gas_target_process"][0](None, None)
        ),
        "max_validator_count": system_parameters.parameters["MAX_VALIDATOR_COUNT"][0],
    }

    return ModelBundle(
        constants=constants,
        stage_all=model_types.Stage.ALL,
        run_experiment=run_module.run,
        time_domain_template=time_domain_template.experiment.simulations[0],
        stake_sweep_template=stake_sweep_template.experiment.simulations[0],
        price_sweep_template=price_sweep_template.experiment.simulations[0],
        validator_environments=validator_environments,
        defaults=defaults,
    )


def default_dashboard_assumptions() -> DashboardAssumptions:
    """Build the default dashboard assumptions from model defaults."""

    defaults = _load_model_bundle().defaults
    return DashboardAssumptions(
        simulation_time_months=int(defaults["simulation_time_months"]),
        epochs_per_timestep=int(defaults["epochs_per_timestep"]),
        initial_validator_count=int(defaults["initial_validator_count"]),
        initial_eth_staked=float(defaults["initial_eth_staked"]),
        initial_eth_supply=float(defaults["initial_eth_supply"]),
        initial_eth_price_usd=float(defaults["initial_eth_price_usd"]),
        validator_adoption_per_epoch=float(defaults["validator_adoption_per_epoch"]),
        low_adoption_multiplier=0.5,
        high_adoption_multiplier=1.5,
        validator_uptime=float(defaults["validator_uptime"]),
        slashing_events_per_1000_epochs=int(defaults["slashing_events_per_1000_epochs"]),
        mev_per_block_eth=float(defaults["mev_per_block_eth"]),
        base_fee_gwei=float(defaults["base_fee_gwei"]),
        priority_fee_gwei=float(defaults["priority_fee_gwei"]),
        gas_target_per_block=float(defaults["gas_target_per_block"]),
        stake_sweep_points=25,
        price_sweep_points=25,
        stake_sweep_max_pct_of_supply=0.30,
        stake_price_low_multiplier=0.75,
        stake_price_high_multiplier=1.25,
    )


def load_dashboard_assumptions_from_env() -> DashboardAssumptions:
    """Read optional dashboard assumptions from environment variables."""

    defaults = default_dashboard_assumptions()

    def read_int(name: str, value: int) -> int:
        return int(os.getenv(name, str(value)))

    def read_float(name: str, value: float) -> float:
        return float(os.getenv(name, str(value)))

    return DashboardAssumptions(
        simulation_time_months=read_int("SIM_TIME_MONTHS", defaults.simulation_time_months),
        epochs_per_timestep=read_int("SIM_EPOCHS_PER_TIMESTEP", defaults.epochs_per_timestep),
        initial_validator_count=read_int(
            "SIM_INITIAL_VALIDATOR_COUNT",
            defaults.initial_validator_count,
        ),
        initial_eth_staked=read_float("SIM_INITIAL_ETH_STAKED", defaults.initial_eth_staked),
        initial_eth_supply=read_float("SIM_INITIAL_ETH_SUPPLY", defaults.initial_eth_supply),
        initial_eth_price_usd=read_float(
            "SIM_INITIAL_ETH_PRICE_USD",
            defaults.initial_eth_price_usd,
        ),
        validator_adoption_per_epoch=read_float(
            "SIM_VALIDATOR_ADOPTION_PER_EPOCH",
            defaults.validator_adoption_per_epoch,
        ),
        low_adoption_multiplier=read_float(
            "SIM_LOW_ADOPTION_MULTIPLIER",
            defaults.low_adoption_multiplier,
        ),
        high_adoption_multiplier=read_float(
            "SIM_HIGH_ADOPTION_MULTIPLIER",
            defaults.high_adoption_multiplier,
        ),
        validator_uptime=read_float("SIM_VALIDATOR_UPTIME", defaults.validator_uptime),
        slashing_events_per_1000_epochs=read_int(
            "SIM_SLASHING_EVENTS_PER_1000_EPOCHS",
            defaults.slashing_events_per_1000_epochs,
        ),
        mev_per_block_eth=read_float("SIM_MEV_PER_BLOCK_ETH", defaults.mev_per_block_eth),
        base_fee_gwei=read_float("SIM_BASE_FEE_GWEI", defaults.base_fee_gwei),
        priority_fee_gwei=read_float("SIM_PRIORITY_FEE_GWEI", defaults.priority_fee_gwei),
        gas_target_per_block=read_float("SIM_GAS_TARGET_PER_BLOCK", defaults.gas_target_per_block),
        stake_sweep_points=read_int("SIM_STAKE_SWEEP_POINTS", defaults.stake_sweep_points),
        price_sweep_points=read_int("SIM_PRICE_SWEEP_POINTS", defaults.price_sweep_points),
        stake_sweep_max_pct_of_supply=read_float(
            "SIM_STAKE_SWEEP_MAX_PCT_OF_SUPPLY",
            defaults.stake_sweep_max_pct_of_supply,
        ),
        stake_price_low_multiplier=read_float(
            "SIM_STAKE_PRICE_LOW_MULTIPLIER",
            defaults.stake_price_low_multiplier,
        ),
        stake_price_high_multiplier=read_float(
            "SIM_STAKE_PRICE_HIGH_MULTIPLIER",
            defaults.stake_price_high_multiplier,
        ),
    )


def _phase_space_dt(bundle: ModelBundle) -> int:
    return int(bundle.constants.epochs_per_year)


def _time_domain_timesteps(assumptions: DashboardAssumptions, bundle: ModelBundle) -> int:
    epochs = assumptions.simulation_time_months * bundle.constants.epochs_per_month
    return max(int(epochs // assumptions.epochs_per_timestep), 1)


def _awake_validator_count(assumptions: DashboardAssumptions, bundle: ModelBundle) -> int:
    cap = bundle.defaults.get("max_validator_count")
    if cap is None:
        return assumptions.initial_validator_count
    return min(assumptions.initial_validator_count, int(cap))


def _shared_initial_state(assumptions: DashboardAssumptions, bundle: ModelBundle) -> dict[str, Any]:
    return {
        "eth_price": float(assumptions.initial_eth_price_usd),
        "eth_staked": float(assumptions.initial_eth_staked),
        "eth_supply": float(assumptions.initial_eth_supply),
        "number_of_active_validators": int(assumptions.initial_validator_count),
        "number_of_awake_validators": int(_awake_validator_count(assumptions, bundle)),
    }


def _run_simulation(bundle: ModelBundle, simulation: Any) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    with _cadlabs_log_quietly():
        frame, exceptions = bundle.run_experiment(simulation)
    actual_exceptions = [item for item in exceptions if item.get("exception") is not None]
    if actual_exceptions:
        raise RuntimeError(f"Simulation failed: {actual_exceptions[0]}")
    return frame, exceptions


def _latest_substep_frame(frame: pd.DataFrame) -> pd.DataFrame:
    latest_substep = int(frame["substep"].max())
    latest = frame.loc[frame["substep"] == latest_substep].copy()
    if "timestamp" in latest.columns:
        latest["timestamp"] = pd.to_datetime(latest["timestamp"])
    return latest.sort_values(["subset", "run", "timestep"]).reset_index(drop=True)


def _stake_sweep_values(assumptions: DashboardAssumptions) -> np.ndarray:
    upper = max(
        assumptions.initial_eth_staked * 1.05,
        assumptions.initial_eth_supply * assumptions.stake_sweep_max_pct_of_supply,
    )
    if upper <= assumptions.initial_eth_staked:
        upper = assumptions.initial_eth_staked * 1.25
    return np.linspace(
        assumptions.initial_eth_staked,
        upper,
        max(assumptions.stake_sweep_points, 2),
    )


def _price_sweep_values(assumptions: DashboardAssumptions) -> np.ndarray:
    lower = max(250.0, assumptions.initial_eth_price_usd * 0.4)
    upper = max(lower + 1.0, assumptions.initial_eth_price_usd * 1.8)
    return np.linspace(lower, upper, max(assumptions.price_sweep_points, 2))


def _build_time_series_results(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    simulation = copy.deepcopy(bundle.time_domain_template)
    simulation.timesteps = _time_domain_timesteps(assumptions, bundle)
    simulation.runs = 1
    simulation.model.params.update(
        {
            "dt": [assumptions.epochs_per_timestep],
            "stage": [bundle.stage_all],
            "validator_process": [
                _constant_process(assumptions.validator_adoption_per_epoch),
                _constant_process(
                    assumptions.validator_adoption_per_epoch * assumptions.low_adoption_multiplier
                ),
                _constant_process(
                    assumptions.validator_adoption_per_epoch * assumptions.high_adoption_multiplier
                ),
            ],
            "eth_price_process": [_constant_process(assumptions.initial_eth_price_usd)],
            "validator_uptime_process": [_constant_process(assumptions.validator_uptime)],
            "slashing_events_per_1000_epochs": [
                assumptions.slashing_events_per_1000_epochs
            ],
            "mev_per_block": [assumptions.mev_per_block_eth],
            "base_fee_process": [_constant_process(assumptions.base_fee_gwei)],
            "priority_fee_process": [_constant_process(assumptions.priority_fee_gwei)],
            "gas_target_process": [_constant_process(assumptions.gas_target_per_block)],
        }
    )
    simulation.model.initial_state.update(_shared_initial_state(assumptions, bundle))

    raw_frame, _exceptions = _run_simulation(bundle, simulation)
    latest = _latest_substep_frame(raw_frame)
    latest["scenario"] = latest["subset"].map(SCENARIO_LABELS)

    summary = (
        latest.sort_values(["subset", "timestep"])
        .groupby(["subset", "scenario"], as_index=False)
        .tail(1)
        .rename(
            columns={
                "number_of_active_validators": "final_validator_count",
                "eth_staked": "final_eth_staked",
                "total_revenue_yields_pct": "final_revenue_yield_pct",
                "total_profit_yields_pct": "final_profit_yield_pct",
                "cumulative_profit_yields_pct": "final_cumulative_profit_yield_pct",
            }
        )
        [
            [
                "subset",
                "scenario",
                "final_validator_count",
                "final_eth_staked",
                "eth_price",
                "final_revenue_yield_pct",
                "final_profit_yield_pct",
                "final_cumulative_profit_yield_pct",
            ]
        ]
        .sort_values("subset")
        .reset_index(drop=True)
    )

    base_scenario = latest.loc[latest["subset"] == 0].copy()
    environment_time_series_rows: list[dict[str, Any]] = []
    environment_summary_rows: list[dict[str, Any]] = []

    if not base_scenario.empty:
        final_base_row = base_scenario.iloc[-1]
        for environment in bundle.validator_environments:
            key = environment.environment_key
            for _, row in base_scenario.iterrows():
                environment_time_series_rows.append(
                    {
                        "timestamp": row.get("timestamp"),
                        "timestep": row["timestep"],
                        "environment": environment.environment,
                        "validator_count": row[f"{key}_validator_count"],
                        "revenue_yield_pct": row[f"{key}_revenue_yields_pct"],
                        "profit_yield_pct": row[f"{key}_profit_yields_pct"],
                    }
                )

            environment_summary_rows.append(
                {
                    "environment": environment.environment,
                    "share_pct": environment.share_pct,
                    "final_validator_count": final_base_row[f"{key}_validator_count"],
                    "final_revenue_yield_pct": final_base_row[f"{key}_revenue_yields_pct"],
                    "final_profit_yield_pct": final_base_row[f"{key}_profit_yields_pct"],
                    "final_cost_usd_per_epoch": final_base_row[f"{key}_costs"],
                }
            )

    environment_time_series = pd.DataFrame(environment_time_series_rows)
    environment_summary = pd.DataFrame(environment_summary_rows)
    if not environment_time_series.empty and "timestamp" in environment_time_series.columns:
        environment_time_series["timestamp"] = pd.to_datetime(environment_time_series["timestamp"])

    return raw_frame, latest, summary, environment_time_series, environment_summary


def _build_stake_sweep_frame(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> pd.DataFrame:
    simulation = copy.deepcopy(bundle.stake_sweep_template)
    sweep_values = _stake_sweep_values(assumptions)
    price_levels = [
        assumptions.initial_eth_price_usd * assumptions.stake_price_low_multiplier,
        assumptions.initial_eth_price_usd,
        assumptions.initial_eth_price_usd * assumptions.stake_price_high_multiplier,
    ]
    price_labels = {
        0: "Lower price",
        1: "Starting price",
        2: "Higher price",
    }

    simulation.runs = len(sweep_values)
    simulation.timesteps = 1
    simulation.model.params.update(
        {
            "dt": [_phase_space_dt(bundle)],
            "stage": [bundle.stage_all],
            "eth_staked_process": [
                lambda run, _timestep, values=sweep_values: float(values[run - 1])
            ],
            "eth_price_process": [_constant_process(level) for level in price_levels],
            "validator_uptime_process": [_constant_process(assumptions.validator_uptime)],
            "slashing_events_per_1000_epochs": [
                assumptions.slashing_events_per_1000_epochs
            ],
            "mev_per_block": [assumptions.mev_per_block_eth],
            "base_fee_process": [_constant_process(assumptions.base_fee_gwei)],
            "priority_fee_process": [_constant_process(assumptions.priority_fee_gwei)],
            "gas_target_process": [_constant_process(assumptions.gas_target_per_block)],
        }
    )
    simulation.model.initial_state.update(_shared_initial_state(assumptions, bundle))

    raw_frame, _exceptions = _run_simulation(bundle, simulation)
    latest = _latest_substep_frame(raw_frame)
    latest["price_scenario"] = latest["subset"].map(price_labels)
    latest["eth_price"] = latest["eth_price"].astype(float)
    latest["eth_staked"] = latest["eth_staked"].astype(float)
    return latest[
        [
            "price_scenario",
            "eth_price",
            "eth_staked",
            "total_revenue_yields_pct",
            "total_profit_yields_pct",
        ]
    ].sort_values(["price_scenario", "eth_staked"]).reset_index(drop=True)


def _build_price_sweep_frame(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> pd.DataFrame:
    simulation = copy.deepcopy(bundle.price_sweep_template)
    sweep_values = _price_sweep_values(assumptions)

    simulation.runs = len(sweep_values)
    simulation.timesteps = 1
    simulation.model.params.update(
        {
            "dt": [_phase_space_dt(bundle)],
            "stage": [bundle.stage_all],
            "eth_price_process": [
                lambda run, _timestep, values=sweep_values: float(values[run - 1])
            ],
            "eth_staked_process": [_constant_process(assumptions.initial_eth_staked)],
            "validator_uptime_process": [_constant_process(assumptions.validator_uptime)],
            "slashing_events_per_1000_epochs": [
                assumptions.slashing_events_per_1000_epochs
            ],
            "mev_per_block": [assumptions.mev_per_block_eth],
            "base_fee_process": [_constant_process(assumptions.base_fee_gwei)],
            "priority_fee_process": [_constant_process(assumptions.priority_fee_gwei)],
            "gas_target_process": [_constant_process(assumptions.gas_target_per_block)],
        }
    )
    simulation.model.initial_state.update(_shared_initial_state(assumptions, bundle))

    raw_frame, _exceptions = _run_simulation(bundle, simulation)
    latest = _latest_substep_frame(raw_frame)
    latest["eth_price"] = latest["eth_price"].astype(float)
    return latest[
        [
            "eth_price",
            "eth_staked",
            "total_revenue_yields_pct",
            "total_profit_yields_pct",
        ]
    ].sort_values("eth_price").reset_index(drop=True)


def _build_surface_frame(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> pd.DataFrame:
    simulation = copy.deepcopy(bundle.price_sweep_template)
    stake_values = _stake_sweep_values(assumptions)
    price_values = _price_sweep_values(assumptions)
    sweep_pairs = [(float(price), float(stake)) for price in price_values for stake in stake_values]

    simulation.runs = len(sweep_pairs)
    simulation.timesteps = 1
    simulation.model.params.update(
        {
            "dt": [_phase_space_dt(bundle)],
            "stage": [bundle.stage_all],
            "eth_price_process": [
                lambda run, _timestep, pairs=sweep_pairs: pairs[run - 1][0]
            ],
            "eth_staked_process": [
                lambda run, _timestep, pairs=sweep_pairs: pairs[run - 1][1]
            ],
            "validator_uptime_process": [_constant_process(assumptions.validator_uptime)],
            "slashing_events_per_1000_epochs": [
                assumptions.slashing_events_per_1000_epochs
            ],
            "mev_per_block": [assumptions.mev_per_block_eth],
            "base_fee_process": [_constant_process(assumptions.base_fee_gwei)],
            "priority_fee_process": [_constant_process(assumptions.priority_fee_gwei)],
            "gas_target_process": [_constant_process(assumptions.gas_target_per_block)],
        }
    )
    simulation.model.initial_state.update(_shared_initial_state(assumptions, bundle))

    raw_frame, _exceptions = _run_simulation(bundle, simulation)
    latest = _latest_substep_frame(raw_frame)
    latest["eth_price"] = latest["eth_price"].astype(float)
    latest["eth_staked"] = latest["eth_staked"].astype(float)
    return latest[
        [
            "eth_price",
            "eth_staked",
            "total_revenue_yields_pct",
            "total_profit_yields_pct",
        ]
    ].sort_values(["eth_price", "eth_staked"]).reset_index(drop=True)


def _build_starting_state_frame(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> pd.DataFrame:
    defaults = bundle.defaults
    return pd.DataFrame(
        [
            {
                "parameter": "Active validators",
                "dashboard_value": assumptions.initial_validator_count,
                "model_default": defaults["initial_validator_count"],
                "units": "validators",
                "model_field": "initial_state.number_of_active_validators",
            },
            {
                "parameter": "ETH staked",
                "dashboard_value": assumptions.initial_eth_staked,
                "model_default": defaults["initial_eth_staked"],
                "units": "ETH",
                "model_field": "initial_state.eth_staked",
            },
            {
                "parameter": "ETH supply",
                "dashboard_value": assumptions.initial_eth_supply,
                "model_default": defaults["initial_eth_supply"],
                "units": "ETH",
                "model_field": "initial_state.eth_supply",
            },
            {
                "parameter": "ETH price",
                "dashboard_value": assumptions.initial_eth_price_usd,
                "model_default": defaults["initial_eth_price_usd"],
                "units": "USD/ETH",
                "model_field": "initial_state.eth_price",
            },
            {
                "parameter": "Awake validators",
                "dashboard_value": _awake_validator_count(assumptions, bundle),
                "model_default": min(
                    defaults["initial_validator_count"],
                    defaults["max_validator_count"] or defaults["initial_validator_count"],
                ),
                "units": "validators",
                "model_field": "initial_state.number_of_awake_validators",
            },
        ]
    )


def _build_model_controls_frame(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> pd.DataFrame:
    defaults = bundle.defaults
    return pd.DataFrame(
        [
            {
                "parameter": "Simulation horizon",
                "dashboard_value": assumptions.simulation_time_months,
                "model_default": defaults["simulation_time_months"],
                "units": "months",
                "model_field": "template override",
            },
            {
                "parameter": "Epochs per timestep",
                "dashboard_value": assumptions.epochs_per_timestep,
                "model_default": defaults["epochs_per_timestep"],
                "units": "epochs",
                "model_field": "params.dt",
            },
            {
                "parameter": "Validator adoption",
                "dashboard_value": assumptions.validator_adoption_per_epoch,
                "model_default": defaults["validator_adoption_per_epoch"],
                "units": "validators/epoch",
                "model_field": "params.validator_process",
            },
            {
                "parameter": "Low adoption multiplier",
                "dashboard_value": assumptions.low_adoption_multiplier,
                "model_default": 0.5,
                "units": "x",
                "model_field": "dashboard scenario",
            },
            {
                "parameter": "High adoption multiplier",
                "dashboard_value": assumptions.high_adoption_multiplier,
                "model_default": 1.5,
                "units": "x",
                "model_field": "dashboard scenario",
            },
            {
                "parameter": "Validator uptime",
                "dashboard_value": assumptions.validator_uptime,
                "model_default": defaults["validator_uptime"],
                "units": "share",
                "model_field": "params.validator_uptime_process",
            },
            {
                "parameter": "Slashing events",
                "dashboard_value": assumptions.slashing_events_per_1000_epochs,
                "model_default": defaults["slashing_events_per_1000_epochs"],
                "units": "events / 1000 epochs",
                "model_field": "params.slashing_events_per_1000_epochs",
            },
            {
                "parameter": "MEV per block",
                "dashboard_value": assumptions.mev_per_block_eth,
                "model_default": defaults["mev_per_block_eth"],
                "units": "ETH",
                "model_field": "params.mev_per_block",
            },
            {
                "parameter": "Base fee",
                "dashboard_value": assumptions.base_fee_gwei,
                "model_default": defaults["base_fee_gwei"],
                "units": "gwei/gas",
                "model_field": "params.base_fee_process",
            },
            {
                "parameter": "Priority fee",
                "dashboard_value": assumptions.priority_fee_gwei,
                "model_default": defaults["priority_fee_gwei"],
                "units": "gwei/gas",
                "model_field": "params.priority_fee_process",
            },
            {
                "parameter": "Gas target",
                "dashboard_value": assumptions.gas_target_per_block,
                "model_default": defaults["gas_target_per_block"],
                "units": "gas/block",
                "model_field": "params.gas_target_process",
            },
        ]
    )


def _build_validator_environment_assumptions_frame(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "environment": environment.environment,
                "share_pct": environment.share_pct,
                "starting_validator_count": assumptions.initial_validator_count
                * (environment.share_pct / 100.0),
                "hardware_cost_usd_per_epoch": environment.hardware_cost_usd_per_epoch,
                "cloud_cost_usd_per_epoch": environment.cloud_cost_usd_per_epoch,
                "third_party_cost_pct_of_rewards": environment.third_party_cost_pct_of_rewards,
            }
            for environment in bundle.validator_environments
        ]
    )


def _build_warnings(
    assumptions: DashboardAssumptions,
    bundle: ModelBundle,
) -> tuple[str, ...]:
    warnings: list[str] = []
    average_stake = assumptions.initial_eth_staked / max(assumptions.initial_validator_count, 1)
    if assumptions.initial_eth_staked > assumptions.initial_eth_supply:
        warnings.append("Starting ETH staked exceeds starting ETH supply.")
    if not 30.0 <= average_stake <= 34.0:
        warnings.append(
            f"Starting ETH staked implies {average_stake:.2f} ETH per active validator, which is far from the usual 32 ETH anchor."
        )
    if assumptions.low_adoption_multiplier >= 1.0:
        warnings.append("Low adoption multiplier is at or above 1.0, so the low scenario is no longer lower than the base case.")
    if assumptions.high_adoption_multiplier <= 1.0:
        warnings.append("High adoption multiplier is at or below 1.0, so the high scenario is no longer above the base case.")
    if bundle.defaults.get("max_validator_count") is not None and _awake_validator_count(
        assumptions,
        bundle,
    ) < assumptions.initial_validator_count:
        warnings.append("The active-validator cap reduced the starting awake-validator count below the entered active-validator count.")
    return tuple(warnings)


def build_dashboard_snapshot(
    assumptions: DashboardAssumptions | None = None,
) -> SimulationDashboardSnapshot:
    """Run the economic model and assemble all dashboard views."""

    assumptions = assumptions or default_dashboard_assumptions()
    bundle = _load_model_bundle()

    _raw_time_series, time_series_frame, scenario_summary_frame, environment_time_series_frame, environment_summary_frame = _build_time_series_results(
        assumptions,
        bundle,
    )
    stake_sweep_frame = _build_stake_sweep_frame(assumptions, bundle)
    price_sweep_frame = _build_price_sweep_frame(assumptions, bundle)
    surface_frame = _build_surface_frame(assumptions, bundle)

    normal_scenario = scenario_summary_frame.loc[scenario_summary_frame["subset"] == 0]
    if normal_scenario.empty:
        raise RuntimeError("The normal-adoption scenario did not produce a summary row.")
    normal_row = normal_scenario.iloc[0]

    overview_metrics = {
        "starting_validator_count": float(assumptions.initial_validator_count),
        "starting_eth_staked": float(assumptions.initial_eth_staked),
        "final_validator_count": float(normal_row["final_validator_count"]),
        "final_eth_staked": float(normal_row["final_eth_staked"]),
        "final_revenue_yield_pct": float(normal_row["final_revenue_yield_pct"]),
        "final_profit_yield_pct": float(normal_row["final_profit_yield_pct"]),
        "final_cumulative_profit_yield_pct": float(
            normal_row["final_cumulative_profit_yield_pct"]
        ),
    }

    notes = (
        # "This dashboard now runs only local simulations from the ethereum-economic-model submodule. It does not auto-refresh and it no longer reads slot-by-slot live chain data.",
        "The time-series tab mirrors the notebook-style adoption and validator-yield analyses with normal, low, and high validator-adoption scenarios.",
        "The phase-space tabs use annualized single-step sweeps seeded from the starting state, similar to the ETH staked, ETH price, and yield-surface notebook analyses."
        # "Model imports normally try to hydrate defaults from live APIs. The dashboard now wraps those imports with deterministic fallback values so the app stays local and reproducible.",
        # "Validator-environment outputs come directly from the model's configured environment distribution and per-environment cost assumptions.",
    )

    return SimulationDashboardSnapshot(
        generated_at=datetime.now(tz=timezone.utc),
        assumptions=assumptions,
        model_defaults=dict(bundle.defaults),
        overview_metrics=overview_metrics,
        time_series_frame=time_series_frame,
        scenario_summary_frame=scenario_summary_frame,
        stake_sweep_frame=stake_sweep_frame,
        price_sweep_frame=price_sweep_frame,
        surface_frame=surface_frame,
        environment_time_series_frame=environment_time_series_frame,
        environment_summary_frame=environment_summary_frame,
        starting_state_frame=_build_starting_state_frame(assumptions, bundle),
        model_controls_frame=_build_model_controls_frame(assumptions, bundle),
        validator_environment_assumptions_frame=_build_validator_environment_assumptions_frame(
            assumptions,
            bundle,
        ),
        notes=notes,
        warnings=_build_warnings(assumptions, bundle),
    )


def _frame_to_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    normalized = frame.copy()
    for column in normalized.columns:
        if pd.api.types.is_datetime64_any_dtype(normalized[column]):
            normalized[column] = normalized[column].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    normalized = normalized.replace({np.nan: None})
    return normalized.to_dict(orient="records")


def snapshot_to_json_payload(snapshot: SimulationDashboardSnapshot) -> dict[str, Any]:
    """Serialize a dashboard snapshot into a machine-readable payload."""

    return {
        "generated_at": snapshot.generated_at.isoformat(),
        "assumptions": asdict(snapshot.assumptions),
        "model_defaults": snapshot.model_defaults,
        "overview_metrics": snapshot.overview_metrics,
        "starting_state": _frame_to_records(snapshot.starting_state_frame),
        "model_controls": _frame_to_records(snapshot.model_controls_frame),
        "scenario_summary": _frame_to_records(snapshot.scenario_summary_frame),
        "validator_environment_assumptions": _frame_to_records(
            snapshot.validator_environment_assumptions_frame
        ),
        "validator_environment_summary": _frame_to_records(snapshot.environment_summary_frame),
        "notes": list(snapshot.notes),
        "warnings": list(snapshot.warnings),
    }


def export_dashboard_snapshot(
    output_path: str | os.PathLike[str],
    assumptions: DashboardAssumptions | None = None,
) -> Path:
    """Run the dashboard snapshot pipeline and write a JSON export."""

    snapshot = build_dashboard_snapshot(assumptions or load_dashboard_assumptions_from_env())
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(snapshot_to_json_payload(snapshot), indent=2),
        encoding="utf-8",
    )
    return path
