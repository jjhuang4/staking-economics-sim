"""Streamlit dashboard for the CADLabs-style staking economics simulator."""

from __future__ import annotations

from dataclasses import asdict
import json

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from .live_dashboard_data import (
        DashboardAssumptions,
        SimulationDashboardSnapshot,
        build_dashboard_snapshot,
        default_dashboard_assumptions,
        snapshot_to_json_payload,
    )
    from .equivocation_attack import (
        EquivocationAttackConfig,
        EquivocationAttackSnapshot,
        build_equivocation_attack_snapshot,
    )
except ImportError:
    from live_dashboard_data import (
        DashboardAssumptions,
        SimulationDashboardSnapshot,
        build_dashboard_snapshot,
        default_dashboard_assumptions,
        snapshot_to_json_payload,
    )
    from equivocation_attack import (
        EquivocationAttackConfig,
        EquivocationAttackSnapshot,
        build_equivocation_attack_snapshot,
    )


SCENARIO_COLORS = {
    "Normal adoption": "#4db4ff",
    "Low adoption": "#5be7c4",
    "High adoption": "#ffbe55",
}
METRIC_COLORS = {
    "Revenue yield": "#4db4ff",
    "Profit yield": "#ff7c6b",
    "Spread": "#ffd166",
}
ENVIRONMENT_COLORS = [
    "#4db4ff",
    "#5be7c4",
    "#ffbe55",
    "#ff7c6b",
    "#c08bff",
    "#7fb3ff",
    "#8bd450",
]
ATTACK_COLORS = {
    "Branch A vote share": "#4db4ff",
    "Branch B vote share": "#ffbe55",
    "Attacker share before slashing": "#ff7c6b",
    "Attacker share after slashing": "#5be7c4",
    "Cumulative slashed validators": "#c08bff",
    "Slashed this epoch": "#ffd166",
    "Slashable attackers": "#4db4ff",
    "Minimum slashed to restore safety": "#ff7c6b",
}


def render_theme() -> None:
    """Apply a black-forward simulation-lab visual style."""

    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(255, 190, 92, 0.14), transparent 26%),
                radial-gradient(circle at top right, rgba(77, 180, 255, 0.14), transparent 24%),
                linear-gradient(180deg, #020202 0%, #06080b 48%, #0a0d11 100%);
            color: #f3f3f3;
        }
        .stSidebar {
            background: linear-gradient(180deg, #040404 0%, #0b0f14 100%);
        }
        .stSidebar [data-testid="stMarkdownContainer"],
        .stSidebar label,
        .stSidebar div,
        .stSidebar p,
        .stSidebar span {
            color: #f5f5f5;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            color: #faf7ef;
        }
        p, li, label, div, span {
            color: #f0f0f0;
        }
        div[data-testid="stMetric"] {
            background: rgba(9, 9, 9, 0.92);
            border: 1px solid rgba(255, 255, 255, 0.12);
            border-radius: 18px;
            padding: 0.85rem 1rem;
            box-shadow: 0 18px 36px rgba(0, 0, 0, 0.34);
        }
        div[data-testid="stMetricLabel"],
        div[data-testid="stMetricValue"],
        div[data-testid="stMetricDelta"] {
            color: #f8f8f8;
        }
        .model-chip {
            display: inline-block;
            padding: 0.2rem 0.6rem;
            margin-right: 0.45rem;
            margin-bottom: 0.45rem;
            border-radius: 999px;
            border: 1px solid rgba(255, 255, 255, 0.12);
            background: rgba(10, 10, 10, 0.82);
            color: #f3f3f3;
            font-size: 0.9rem;
        }
        .note-card {
            background: rgba(8, 8, 8, 0.88);
            border-left: 4px solid #c8922d;
            border-radius: 14px;
            padding: 0.8rem 0.95rem;
            margin-bottom: 0.65rem;
            color: #f3f3f3;
        }
        div[data-testid="stDataFrame"] {
            background: rgba(8, 8, 8, 0.82);
            border: 1px solid rgba(255, 255, 255, 0.1);
            border-radius: 18px;
            padding: 0.2rem;
        }
        div[data-baseweb="tab-list"] {
            background: rgba(255, 255, 255, 0.04);
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 999px;
            padding: 0.2rem;
        }
        button[data-baseweb="tab"] {
            color: #dcdcdc;
        }
        button[data-baseweb="tab"][aria-selected="true"] {
            background: rgba(255, 255, 255, 0.08);
            color: #ffffff;
            border-radius: 999px;
        }
        [data-baseweb="input"] input,
        [data-baseweb="base-input"] input,
        textarea {
            background: #090909 !important;
            color: #f3f3f3 !important;
        }
        [data-baseweb="select"] > div {
            background: #090909 !important;
            color: #f3f3f3 !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def load_snapshot(config_payload: dict[str, float | int]) -> SimulationDashboardSnapshot:
    """Cache the expensive simulation bundle between UI reruns."""

    return build_dashboard_snapshot(DashboardAssumptions(**config_payload))


@st.cache_data(show_spinner=False)
def load_equivocation_snapshot(
    config_payload: dict[str, float | int],
) -> EquivocationAttackSnapshot:
    """Cache the custom equivocation-attack simulation between reruns."""

    return build_equivocation_attack_snapshot(EquivocationAttackConfig(**config_payload))


def _configure_figure(
    figure: go.Figure,
    *,
    title: str,
    xaxis_title: str,
    yaxis_title: str,
) -> None:
    figure.update_layout(
        title=title,
        template="plotly_dark",
        paper_bgcolor="#050505",
        plot_bgcolor="#050505",
        font=dict(color="#f3f3f3"),
        margin=dict(l=24, r=18, t=62, b=20),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0.0,
            title_text="",
            bgcolor="rgba(0,0,0,0)",
        ),
        hovermode="x unified",
        xaxis=dict(
            title=xaxis_title,
            gridcolor="rgba(255, 255, 255, 0.12)",
            linecolor="rgba(255, 255, 255, 0.18)",
            zerolinecolor="rgba(255, 255, 255, 0.14)",
        ),
        yaxis=dict(
            title=yaxis_title,
            gridcolor="rgba(255, 255, 255, 0.12)",
            linecolor="rgba(255, 255, 255, 0.18)",
            zerolinecolor="rgba(255, 255, 255, 0.14)",
        ),
    )


def _line_chart(
    frame: pd.DataFrame,
    *,
    x: str,
    y: str,
    color: str,
    title: str,
    yaxis_title: str,
    color_discrete_map: dict[str, str] | None = None,
) -> go.Figure:
    figure = px.line(
        frame,
        x=x,
        y=y,
        color=color,
        markers=True,
        color_discrete_map=color_discrete_map,
    )
    _configure_figure(
        figure,
        title=title,
        xaxis_title="Time" if x == "timestamp" else x.replace("_", " ").title(),
        yaxis_title=yaxis_title,
    )
    return figure


def _metric_columns(snapshot: SimulationDashboardSnapshot) -> None:
    metrics = snapshot.overview_metrics
    top = st.columns(6)
    top[0].metric("Start validators", f"{metrics['starting_validator_count']:,.0f}")
    top[1].metric("Final validators", f"{metrics['final_validator_count']:,.0f}")
    top[2].metric("Start staked", f"{metrics['starting_eth_staked']:,.0f} ETH")
    top[3].metric("Final staked", f"{metrics['final_eth_staked']:,.0f} ETH")
    top[4].metric("Final revenue yield", f"{metrics['final_revenue_yield_pct']:.2f}%")
    top[5].metric("Final profit yield", f"{metrics['final_profit_yield_pct']:.2f}%")


def render_sidebar(current: DashboardAssumptions) -> DashboardAssumptions:
    """Collect simulation controls without auto-refresh behaviour."""

    st.sidebar.title("Simulation Controls")
    # st.sidebar.caption(
    #     "This dashboard is now simulation-only. It does not poll beacon APIs, refresh on a slot cadence, or depend on live Hoodi data."
    # )

    with st.sidebar.form("simulation_controls"):
        st.markdown("### Starting State")
        initial_validator_count = int(
            st.number_input(
                "Active validators",
                min_value=1,
                value=int(current.initial_validator_count),
                step=1_000,
            )
        )
        initial_eth_staked = float(
            st.number_input(
                "ETH staked",
                min_value=1.0,
                value=float(current.initial_eth_staked),
                step=100_000.0,
            )
        )
        initial_eth_supply = float(
            st.number_input(
                "ETH supply",
                min_value=1.0,
                value=float(current.initial_eth_supply),
                step=500_000.0,
            )
        )
        initial_eth_price_usd = float(
            st.number_input(
                "ETH price (USD)",
                min_value=1.0,
                value=float(current.initial_eth_price_usd),
                step=100.0,
            )
        )

        st.markdown("### Network Assumptions")
        validator_adoption_per_epoch = float(
            st.number_input(
                "New validators per epoch",
                min_value=0.0,
                value=float(current.validator_adoption_per_epoch),
                step=0.25,
            )
        )
        validator_uptime = float(
            st.slider(
                "Validator uptime",
                min_value=float(2 / 3),
                max_value=1.0,
                value=float(current.validator_uptime),
                step=0.005,
            )
        )
        slashing_events_per_1000_epochs = int(
            st.slider(
                "Slashing events per 1,000 epochs",
                min_value=0,
                max_value=25,
                value=int(current.slashing_events_per_1000_epochs),
                step=1,
            )
        )
        mev_per_block_eth = float(
            st.number_input(
                "MEV per block (ETH)",
                min_value=0.0,
                value=float(current.mev_per_block_eth),
                step=0.01,
            )
        )
        base_fee_gwei = float(
            st.number_input(
                "Base fee (gwei / gas)",
                min_value=0.0,
                value=float(current.base_fee_gwei),
                step=1.0,
            )
        )
        priority_fee_gwei = float(
            st.number_input(
                "Priority fee (gwei / gas)",
                min_value=0.0,
                value=float(current.priority_fee_gwei),
                step=0.5,
            )
        )
        gas_target_per_block = float(
            st.number_input(
                "Gas target per block",
                min_value=1.0,
                value=float(current.gas_target_per_block),
                step=1_000_000.0,
            )
        )

        st.markdown("### Scenario Shape")
        simulation_time_months = int(
            st.slider(
                "Time horizon (months)",
                min_value=6,
                max_value=60,
                value=int(current.simulation_time_months),
                step=6,
            )
        )
        low_adoption_multiplier = float(
            st.slider(
                "Low adoption multiplier",
                min_value=0.1,
                max_value=1.0,
                value=float(current.low_adoption_multiplier),
                step=0.05,
            )
        )
        high_adoption_multiplier = float(
            st.slider(
                "High adoption multiplier",
                min_value=1.0,
                max_value=3.0,
                value=float(current.high_adoption_multiplier),
                step=0.05,
            )
        )
        epochs_per_timestep = int(
            st.select_slider(
                "Epochs per timestep",
                options=[32, 64, 128, 225, 450],
                value=int(current.epochs_per_timestep),
            )
        )

        st.markdown("### Sweep Resolution")
        stake_sweep_points = int(
            st.slider(
                "Stake sweep points",
                min_value=10,
                max_value=40,
                value=int(current.stake_sweep_points),
                step=5,
            )
        )
        price_sweep_points = int(
            st.slider(
                "Price sweep points",
                min_value=10,
                max_value=40,
                value=int(current.price_sweep_points),
                step=5,
            )
        )
        stake_sweep_max_pct_of_supply = float(
            st.slider(
                "Max stake sweep share of supply",
                min_value=0.10,
                max_value=0.60,
                value=float(current.stake_sweep_max_pct_of_supply),
                step=0.05,
            )
        )

        submitted = st.form_submit_button("Run Simulation", use_container_width=True)

    if submitted:
        return DashboardAssumptions(
            simulation_time_months=simulation_time_months,
            epochs_per_timestep=epochs_per_timestep,
            initial_validator_count=initial_validator_count,
            initial_eth_staked=initial_eth_staked,
            initial_eth_supply=initial_eth_supply,
            initial_eth_price_usd=initial_eth_price_usd,
            validator_adoption_per_epoch=validator_adoption_per_epoch,
            low_adoption_multiplier=low_adoption_multiplier,
            high_adoption_multiplier=high_adoption_multiplier,
            validator_uptime=validator_uptime,
            slashing_events_per_1000_epochs=slashing_events_per_1000_epochs,
            mev_per_block_eth=mev_per_block_eth,
            base_fee_gwei=base_fee_gwei,
            priority_fee_gwei=priority_fee_gwei,
            gas_target_per_block=gas_target_per_block,
            stake_sweep_points=stake_sweep_points,
            price_sweep_points=price_sweep_points,
            stake_sweep_max_pct_of_supply=stake_sweep_max_pct_of_supply,
            stake_price_low_multiplier=current.stake_price_low_multiplier,
            stake_price_high_multiplier=current.stake_price_high_multiplier,
        )

    return current


def render_overview_tab(snapshot: SimulationDashboardSnapshot) -> None:
    """Render summary metrics, assumptions, and export controls."""

    _metric_columns(snapshot)
    # st.markdown(
    #     """
    #     <div class="model-chip">Simulation-only dashboard</div>
    #     <div class="model-chip">CADLabs experiment patterns</div>
    #     <div class="model-chip">Transparent starting state</div>
    #     <div class="model-chip">No live slot refresh</div>
    #     """,
    #     unsafe_allow_html=True,
    # )

    left, right = st.columns([1.15, 0.85])

    left.subheader("Scenario Summary")
    left.dataframe(
        snapshot.scenario_summary_frame.rename(
            columns={
                "scenario": "Scenario",
                "final_validator_count": "Final validators",
                "final_eth_staked": "Final ETH staked",
                "eth_price": "ETH price",
                "final_revenue_yield_pct": "Revenue yield",
                "final_profit_yield_pct": "Profit yield",
                "final_cumulative_profit_yield_pct": "Cumulative profit yield",
            }
        ),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Final validators": st.column_config.NumberColumn(format="%.0f"),
            "Final ETH staked": st.column_config.NumberColumn(format="%.0f ETH"),
            "ETH price": st.column_config.NumberColumn(format="$%.0f"),
            "Revenue yield": st.column_config.NumberColumn(format="%.2f%%"),
            "Profit yield": st.column_config.NumberColumn(format="%.2f%%"),
            "Cumulative profit yield": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )

    right.subheader("Export")
    payload = snapshot_to_json_payload(snapshot)
    right.download_button(
        "Download snapshot JSON",
        data=json.dumps(payload, indent=2),
        file_name="staking_economics_snapshot.json",
        mime="application/json",
        use_container_width=True,
    )
    right.caption(
        f"Generated at {snapshot.generated_at.isoformat()} from local model assumptions only."
    )
    with right.expander("Machine-readable payload", expanded=False):
        st.json(payload)

    state_col, controls_col = st.columns(2)
    state_col.subheader("Starting State")
    state_col.dataframe(
        snapshot.starting_state_frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "dashboard_value": st.column_config.NumberColumn("Dashboard value", format="%.4f"),
            "model_default": st.column_config.NumberColumn("Model default", format="%.4f"),
        },
    )

    controls_col.subheader("Model Controls")
    controls_col.dataframe(
        snapshot.model_controls_frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "dashboard_value": st.column_config.NumberColumn("Dashboard value", format="%.4f"),
            "model_default": st.column_config.NumberColumn("Model default", format="%.4f"),
        },
    )

    st.subheader("Notes")
    for note in snapshot.notes:
        st.markdown(f'<div class="note-card">{note}</div>', unsafe_allow_html=True)


def render_time_series_tab(snapshot: SimulationDashboardSnapshot) -> None:
    """Render notebook-style time-domain charts."""

    frame = snapshot.time_series_frame.copy()
    x_axis = "timestamp" if "timestamp" in frame.columns else "timestep"

    top_left, top_right = st.columns(2)
    top_left.plotly_chart(
        _line_chart(
            frame,
            x=x_axis,
            y="number_of_active_validators",
            color="scenario",
            title="Active Validators Over Time",
            yaxis_title="Validators",
            color_discrete_map=SCENARIO_COLORS,
        ),
        use_container_width=True,
    )
    top_right.plotly_chart(
        _line_chart(
            frame,
            x=x_axis,
            y="eth_staked",
            color="scenario",
            title="ETH Staked Over Time",
            yaxis_title="ETH",
            color_discrete_map=SCENARIO_COLORS,
        ),
        use_container_width=True,
    )

    middle_left, middle_right = st.columns(2)
    middle_left.plotly_chart(
        _line_chart(
            frame,
            x=x_axis,
            y="total_revenue_yields_pct",
            color="scenario",
            title="Annualized Revenue Yield",
            yaxis_title="Yield (%)",
            color_discrete_map=SCENARIO_COLORS,
        ),
        use_container_width=True,
    )
    middle_right.plotly_chart(
        _line_chart(
            frame,
            x=x_axis,
            y="total_profit_yields_pct",
            color="scenario",
            title="Annualized Profit Yield",
            yaxis_title="Yield (%)",
            color_discrete_map=SCENARIO_COLORS,
        ),
        use_container_width=True,
    )

    lower_left, lower_right = st.columns(2)
    lower_left.plotly_chart(
        _line_chart(
            frame,
            x=x_axis,
            y="cumulative_profit_yields_pct",
            color="scenario",
            title="Cumulative Profit Yield",
            yaxis_title="Yield (%)",
            color_discrete_map=SCENARIO_COLORS,
        ),
        use_container_width=True,
    )
    lower_right.plotly_chart(
        _line_chart(
            frame,
            x=x_axis,
            y="revenue_profit_yield_spread_pct",
            color="scenario",
            title="Revenue-Profit Yield Spread",
            yaxis_title="Spread (%)",
            color_discrete_map=SCENARIO_COLORS,
        ),
        use_container_width=True,
    )


def render_phase_space_tab(snapshot: SimulationDashboardSnapshot) -> None:
    """Render ETH staked, ETH price, and surface analyses."""

    stake_frame = snapshot.stake_sweep_frame.copy()
    stake_melt = stake_frame.melt(
        id_vars=["price_scenario", "eth_price", "eth_staked"],
        value_vars=["total_revenue_yields_pct", "total_profit_yields_pct"],
        var_name="metric",
        value_name="yield_pct",
    )
    stake_melt["metric"] = stake_melt["metric"].map(
        {
            "total_revenue_yields_pct": "Revenue yield",
            "total_profit_yields_pct": "Profit yield",
        }
    )
    stake_figure = px.line(
        stake_melt,
        x="eth_staked",
        y="yield_pct",
        color="price_scenario",
        line_dash="metric",
        markers=True,
        color_discrete_map={
            "Lower price": "#2f8f83",
            "Starting price": "#1b5e87",
            "Higher price": "#d58b1f",
        },
    )
    _configure_figure(
        stake_figure,
        title="Yield Sensitivity to ETH Staked",
        xaxis_title="ETH staked",
        yaxis_title="Yield (%)",
    )

    price_frame = snapshot.price_sweep_frame.copy()
    price_melt = price_frame.melt(
        id_vars=["eth_price", "eth_staked"],
        value_vars=["total_revenue_yields_pct", "total_profit_yields_pct"],
        var_name="metric",
        value_name="yield_pct",
    )
    price_melt["metric"] = price_melt["metric"].map(
        {
            "total_revenue_yields_pct": "Revenue yield",
            "total_profit_yields_pct": "Profit yield",
        }
    )
    price_figure = px.line(
        price_melt,
        x="eth_price",
        y="yield_pct",
        color="metric",
        markers=True,
        color_discrete_map=METRIC_COLORS,
    )
    _configure_figure(
        price_figure,
        title="Yield Sensitivity to ETH Price",
        xaxis_title="ETH price (USD)",
        yaxis_title="Yield (%)",
    )

    top_left, top_right = st.columns(2)
    top_left.plotly_chart(stake_figure, use_container_width=True)
    top_right.plotly_chart(price_figure, use_container_width=True)

    surface_grid = snapshot.surface_frame.pivot_table(
        index="eth_price",
        columns="eth_staked",
        values="total_profit_yields_pct",
    )
    surface_figure = go.Figure(
        data=go.Heatmap(
            x=surface_grid.columns.tolist(),
            y=surface_grid.index.tolist(),
            z=surface_grid.to_numpy(),
            colorscale=[
                [0.0, "#173247"],
                [0.35, "#1b6f8a"],
                [0.7, "#ddb25d"],
                [1.0, "#cb5d3e"],
            ],
            colorbar=dict(title="Profit yield (%)"),
        )
    )
    _configure_figure(
        surface_figure,
        title="Profit Yield Surface",
        xaxis_title="ETH staked",
        yaxis_title="ETH price (USD)",
    )
    st.plotly_chart(surface_figure, use_container_width=True)


def render_validator_environment_tab(snapshot: SimulationDashboardSnapshot) -> None:
    """Render validator-environment compositions and outcomes."""

    if snapshot.environment_time_series_frame.empty:
        st.warning("Validator-environment outputs were not available for this run.")
        return

    frame = snapshot.environment_time_series_frame.copy()
    frame["environment"] = pd.Categorical(
        frame["environment"],
        categories=snapshot.validator_environment_assumptions_frame["environment"].tolist(),
        ordered=True,
    )

    profit_figure = px.line(
        frame,
        x="timestamp",
        y="profit_yield_pct",
        color="environment",
        markers=True,
        color_discrete_sequence=ENVIRONMENT_COLORS,
    )
    _configure_figure(
        profit_figure,
        title="Profit Yield by Validator Environment",
        xaxis_title="Time",
        yaxis_title="Profit yield (%)",
    )

    composition_figure = px.area(
        frame,
        x="timestamp",
        y="validator_count",
        color="environment",
        color_discrete_sequence=ENVIRONMENT_COLORS,
    )
    _configure_figure(
        composition_figure,
        title="Validator Environment Composition",
        xaxis_title="Time",
        yaxis_title="Validators",
    )

    top_left, top_right = st.columns(2)
    top_left.plotly_chart(profit_figure, use_container_width=True)
    top_right.plotly_chart(composition_figure, use_container_width=True)

    bottom_left, bottom_right = st.columns(2)
    bottom_left.subheader("Final Environment Outcomes")
    bottom_left.dataframe(
        snapshot.environment_summary_frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "share_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "final_validator_count": st.column_config.NumberColumn(format="%.0f"),
            "final_revenue_yield_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "final_profit_yield_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "final_cost_usd_per_epoch": st.column_config.NumberColumn(format="$%.4f"),
        },
    )

    bottom_right.subheader("Environment Assumptions")
    bottom_right.dataframe(
        snapshot.validator_environment_assumptions_frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "share_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "starting_validator_count": st.column_config.NumberColumn(format="%.0f"),
            "hardware_cost_usd_per_epoch": st.column_config.NumberColumn(format="$%.4f"),
            "cloud_cost_usd_per_epoch": st.column_config.NumberColumn(format="$%.4f"),
            "third_party_cost_pct_of_rewards": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )


def render_equivocation_attack_tab(snapshot: SimulationDashboardSnapshot) -> None:
    """Render a custom equivocation-attack and accountable-safety view."""

    st.caption(
        "This tab uses the repo's own high-level safety model to visualize a balanced-partition equivocation scenario. "
        "It focuses on slashable-validator counts, conflicting-finalization feasibility, and how quickly slashing pushes the attacker share back below the accountable-safety bound."
    )

    controls_top = st.columns(4)
    total_validators = int(
        controls_top[0].number_input(
            "Total validators",
            min_value=1,
            value=int(snapshot.assumptions.initial_validator_count),
            step=1_000,
            key="attack_total_validators",
        )
    )
    attacker_fraction_pct = float(
        controls_top[1].slider(
            "Equivocating attacker share (%)",
            min_value=0.0,
            max_value=50.0,
            value=34.0,
            step=1.0,
            key="attack_attacker_fraction_pct",
        )
    )
    honest_partition_fraction_pct = float(
        controls_top[2].slider(
            "Honest stake on branch A (%)",
            min_value=10.0,
            max_value=50.0,
            value=50.0,
            step=1.0,
            key="attack_honest_partition_pct",
            help="50/50 is the most adversarial partition for showing when equivocators can push both branches over the 2/3 line.",
        )
    )
    epochs = int(
        controls_top[3].slider(
            "Attack horizon (epochs)",
            min_value=1,
            max_value=16,
            value=8,
            step=1,
            key="attack_epochs",
        )
    )

    controls_bottom = st.columns(3)
    slash_detection_delay_epochs = int(
        controls_bottom[0].slider(
            "Detection delay (epochs)",
            min_value=0,
            max_value=8,
            value=1,
            step=1,
            key="attack_detection_delay_epochs",
        )
    )
    slash_detection_fraction_pct = float(
        controls_bottom[1].slider(
            "Attackers slashed per epoch (%)",
            min_value=5.0,
            max_value=100.0,
            value=100.0,
            step=5.0,
            key="attack_detection_fraction_pct",
        )
    )
    slash_fraction_of_balance_pct = float(
        controls_bottom[2].slider(
            "Burned balance per slashed validator (%)",
            min_value=5.0,
            max_value=100.0,
            value=100.0,
            step=5.0,
            key="attack_burn_fraction_pct",
        )
    )

    attack_snapshot = load_equivocation_snapshot(
        {
            "total_validators": total_validators,
            "attacker_fraction": attacker_fraction_pct / 100.0,
            "honest_partition_fraction": honest_partition_fraction_pct / 100.0,
            "epochs": epochs,
            "slash_detection_delay_epochs": slash_detection_delay_epochs,
            "slash_detection_fraction_per_epoch": slash_detection_fraction_pct / 100.0,
            "slash_fraction_of_balance": slash_fraction_of_balance_pct / 100.0,
        }
    )

    summary = attack_snapshot.summary
    if summary["initial_conflicting_finalization_possible"]:
        st.warning(
            "With these assumptions, both branches can clear the 2/3 finality threshold before slashing lands. "
            "The accountable-safety question becomes how much slashable stake exists and how quickly detection removes it."
        )
    else:
        st.info(
            "With these assumptions, equivocators remain below the conflicting-finalization threshold. "
            "The one-third accountable-safety bound still appears below so you can see the safety margin."
        )

    metrics = st.columns(6)
    metrics[0].metric(
        "Initial attackers",
        f"{summary['initial_attacker_validators']:,.0f}",
        f"{summary['initial_attacker_share_pct']:.1f}%",
    )
    metrics[1].metric(
        "Accountable-safety bound",
        f"{summary['accountable_safety_bound_validators']:,.0f}",
        f"{summary['accountable_safety_bound_pct']:.1f}%",
    )
    metrics[2].metric(
        "Conflict feasible",
        "Yes" if summary["initial_conflicting_finalization_possible"] else "No",
    )
    metrics[3].metric(
        "Min slashed to restore safety",
        f"{summary['initial_minimum_slashed_to_restore_safety']:,.0f}",
    )
    metrics[4].metric(
        "Cumulative slashed",
        f"{summary['final_cumulative_slashed_validators']:,.0f}",
    )
    metrics[5].metric(
        "Safety restored by epoch",
        "Not restored"
        if summary["first_restored_epoch"] is None
        else f"{summary['first_restored_epoch']}",
    )

    frame = attack_snapshot.epoch_frame.copy()
    sweep_frame = attack_snapshot.sweep_frame.copy()
    accountable_bound_pct = float(summary["accountable_safety_bound_pct"])
    finality_threshold_pct = attack_snapshot.config.finality_threshold * 100.0

    vote_share_figure = go.Figure()
    vote_share_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["branch_a_vote_share_pct_before_slash"],
            mode="lines+markers",
            name="Branch A vote share",
            line=dict(color=ATTACK_COLORS["Branch A vote share"], width=2.5),
        )
    )
    vote_share_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["branch_b_vote_share_pct_before_slash"],
            mode="lines+markers",
            name="Branch B vote share",
            line=dict(color=ATTACK_COLORS["Branch B vote share"], width=2.5),
        )
    )
    vote_share_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["branch_a_vote_share_pct_after_slash"],
            mode="lines",
            name="Branch A after slashing",
            line=dict(color=ATTACK_COLORS["Branch A vote share"], width=2, dash="dot"),
        )
    )
    vote_share_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["branch_b_vote_share_pct_after_slash"],
            mode="lines",
            name="Branch B after slashing",
            line=dict(color=ATTACK_COLORS["Branch B vote share"], width=2, dash="dot"),
        )
    )
    vote_share_figure.add_hline(
        y=finality_threshold_pct,
        line_dash="dash",
        line_color="#f3f3f3",
        annotation_text="2/3 finality threshold",
        annotation_position="top left",
    )
    _configure_figure(
        vote_share_figure,
        title="Branch Vote Shares During Equivocation",
        xaxis_title="Epoch",
        yaxis_title="Vote share (%)",
    )

    attacker_share_figure = go.Figure()
    attacker_share_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["attacker_share_pct_before_slash"],
            mode="lines+markers",
            name="Attacker share before slashing",
            line=dict(color=ATTACK_COLORS["Attacker share before slashing"], width=2.5),
        )
    )
    attacker_share_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["attacker_share_pct_after_slash"],
            mode="lines+markers",
            name="Attacker share after slashing",
            line=dict(color=ATTACK_COLORS["Attacker share after slashing"], width=2.5),
        )
    )
    attacker_share_figure.add_hline(
        y=accountable_bound_pct,
        line_dash="dash",
        line_color="#f3f3f3",
        annotation_text="Accountable-safety bound",
        annotation_position="top left",
    )
    _configure_figure(
        attacker_share_figure,
        title="Attacker Share Versus The Accountable-Safety Bound",
        xaxis_title="Epoch",
        yaxis_title="Attacker share (%)",
    )

    slashing_figure = go.Figure()
    slashing_figure.add_trace(
        go.Bar(
            x=frame["epoch"],
            y=frame["slashed_this_epoch"],
            name="Slashed this epoch",
            marker_color=ATTACK_COLORS["Slashed this epoch"],
            opacity=0.72,
        )
    )
    slashing_figure.add_trace(
        go.Scatter(
            x=frame["epoch"],
            y=frame["cumulative_slashed_validators"],
            mode="lines+markers",
            name="Cumulative slashed validators",
            line=dict(color=ATTACK_COLORS["Cumulative slashed validators"], width=2.5),
        )
    )
    _configure_figure(
        slashing_figure,
        title="Slashing Response Over Time",
        xaxis_title="Epoch",
        yaxis_title="Validators",
    )

    sweep_figure = go.Figure()
    sweep_figure.add_trace(
        go.Scatter(
            x=sweep_frame["attacker_share_pct"],
            y=sweep_frame["slashable_validators"],
            mode="lines+markers",
            name="Slashable attackers",
            line=dict(color=ATTACK_COLORS["Slashable attackers"], width=2.5),
        )
    )
    sweep_figure.add_trace(
        go.Scatter(
            x=sweep_frame["attacker_share_pct"],
            y=sweep_frame["minimum_slashed_to_restore_safety"],
            mode="lines+markers",
            name="Minimum slashed to restore safety",
            line=dict(
                color=ATTACK_COLORS["Minimum slashed to restore safety"],
                width=2.5,
            ),
        )
    )
    sweep_figure.add_vline(
        x=summary["initial_attacker_share_pct"],
        line_dash="dash",
        line_color="#f3f3f3",
        annotation_text="Current attacker share",
        annotation_position="top left",
    )
    _configure_figure(
        sweep_figure,
        title="How Much Slashing Restores Safety?",
        xaxis_title="Attacker share (%)",
        yaxis_title="Validators",
    )

    top_left, top_right = st.columns(2)
    top_left.plotly_chart(vote_share_figure, use_container_width=True)
    top_right.plotly_chart(attacker_share_figure, use_container_width=True)

    bottom_left, bottom_right = st.columns(2)
    bottom_left.plotly_chart(slashing_figure, use_container_width=True)
    bottom_right.plotly_chart(sweep_figure, use_container_width=True)

    st.subheader("Epoch-by-Epoch Attack State")
    st.dataframe(
        frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "attacker_share_pct_before_slash": st.column_config.NumberColumn(format="%.2f%%"),
            "branch_a_vote_share_pct_before_slash": st.column_config.NumberColumn(format="%.2f%%"),
            "branch_b_vote_share_pct_before_slash": st.column_config.NumberColumn(format="%.2f%%"),
            "accountable_safety_bound_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "attacker_share_pct_after_slash": st.column_config.NumberColumn(format="%.2f%%"),
            "branch_a_vote_share_pct_after_slash": st.column_config.NumberColumn(format="%.2f%%"),
            "branch_b_vote_share_pct_after_slash": st.column_config.NumberColumn(format="%.2f%%"),
            "cumulative_burned_stake_eth": st.column_config.NumberColumn(format="%.2f ETH"),
        },
    )

    with st.expander("Equivocation Modeling Notes", expanded=False):
        for note in attack_snapshot.notes:
            st.write(f"- {note}")


def main() -> None:
    """Run the Streamlit dashboard."""

    st.set_page_config(
        page_title="Ethereum Staking Economics Lab",
        layout="wide",
    )
    render_theme()

    if "dashboard_assumptions" not in st.session_state:
        st.session_state.dashboard_assumptions = default_dashboard_assumptions()

    if st.sidebar.button("Reset To Model Defaults", use_container_width=True):
        st.session_state.dashboard_assumptions = default_dashboard_assumptions()
        st.rerun()

    updated_assumptions = render_sidebar(st.session_state.dashboard_assumptions)
    st.session_state.dashboard_assumptions = updated_assumptions

    with st.spinner("Running staking-economics simulations..."):
        snapshot = load_snapshot(asdict(st.session_state.dashboard_assumptions))

    st.title("Ethereum Staking Economics Lab")
    st.caption(
        "A local simulation dashboard built from the CADLabs-style economic model experiments. "
    )
    
    for warning in snapshot.warnings:
        st.warning(warning)

    overview_tab, time_tab, phase_tab, environment_tab, attack_tab = st.tabs(
        [
            "Overview",
            "Time Series",
            "Phase Space",
            "Validator Environments",
            "Equivocation Attack",
        ]
    )

    with overview_tab:
        render_overview_tab(snapshot)

    with time_tab:
        render_time_series_tab(snapshot)

    with phase_tab:
        render_phase_space_tab(snapshot)

    with environment_tab:
        render_validator_environment_tab(snapshot)

    with attack_tab:
        render_equivocation_attack_tab(snapshot)


if __name__ == "__main__":
    main()
