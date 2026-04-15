"""Streamlit dashboard for live Hoodi validator-flow monitoring."""

from __future__ import annotations

from dataclasses import asdict
import json
import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

try:
    from .live_dashboard_data import (
        DEFAULT_ACTIVITY_LOOKBACK_EPOCHS,
        DEFAULT_HISTORY_EPOCHS,
        DashboardRuntimeConfig,
        LiveDashboardSnapshot,
        fetch_live_dashboard_snapshot,
        load_dashboard_runtime_config,
    )
    from .cadlabs_replication import (
        CadLabsReplicationConfig,
        CadLabsReplicationSnapshot,
        build_cadlabs_replication,
    )
except ImportError:
    from live_dashboard_data import (
        DEFAULT_ACTIVITY_LOOKBACK_EPOCHS,
        DEFAULT_HISTORY_EPOCHS,
        DashboardRuntimeConfig,
        LiveDashboardSnapshot,
        fetch_live_dashboard_snapshot,
        load_dashboard_runtime_config,
    )
    from cadlabs_replication import (
        CadLabsReplicationConfig,
        CadLabsReplicationSnapshot,
        build_cadlabs_replication,
    )

GWEI_PER_ETH = 1_000_000_000
ACTION_LABELS = {
    "add_to_stake": "add_to_stake",
    "wait": "wait",
    "withdraw": "withdraw",
    "nothing_at_stake_attack": "nothing-at-stake-attack",
}
ACTION_COLORS = {
    "add_to_stake": "#ecc170",
    "wait": "#7ba8ff",
    "withdraw": "#ff9f68",
    "nothing_at_stake_attack": "#ff6b7a",
}
SERIES_COLORS = {
    "observed_nav": "#7ba8ff",
    "scenario_nav": "#ecc170",
    "observed_cumulative_pnl": "#63d0c5",
    "scenario_cumulative_pnl": "#ff9f68",
    "observed_net_rewards": "#7dd3a8",
    "scenario_net_rewards": "#4ad3a7",
    "observed_penalties": "#f6a04d",
    "scenario_penalties": "#ff6b6b",
    "observed_fees": "#d5b77a",
    "scenario_fees": "#ecc170",
    "observed_share_price": "#7ba8ff",
    "scenario_share_price": "#ecc170",
}
EPOCH_REGIME_COLORS = [
    "rgba(123, 168, 255, 0.08)",
    "rgba(236, 193, 112, 0.08)",
    "rgba(99, 208, 197, 0.08)",
    "rgba(255, 159, 104, 0.08)",
]


def gwei_to_eth(value: int | float) -> float:
    """Convert gwei to ETH for display."""

    return float(value) / GWEI_PER_ETH


def render_theme() -> None:
    """Inject dashboard styling."""

    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at 10% 20%, rgba(255, 190, 92, 0.18), transparent 28%),
                radial-gradient(circle at 80% 12%, rgba(71, 126, 255, 0.16), transparent 26%),
                linear-gradient(180deg, #07111d 0%, #0c1929 48%, #132131 100%);
            color: #f8f6f1;
        }
        .stApp [data-testid="stAppViewContainer"] {
            color: #f8f6f1;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            letter-spacing: 0.02em;
            color: #fffaf0;
        }
        p, li, label, div, span {
            color: #edf3fb;
        }
        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(14, 24, 40, 0.95), rgba(10, 17, 29, 0.96));
            border: 1px solid rgba(236, 193, 112, 0.18);
            border-radius: 18px;
            padding: 0.75rem 1rem;
            box-shadow: 0 14px 28px rgba(0, 0, 0, 0.24);
        }
        .action-card {
            background: linear-gradient(145deg, rgba(14, 24, 43, 0.94), rgba(10, 18, 34, 0.96));
            border: 1px solid rgba(236, 193, 112, 0.16);
            border-radius: 18px;
            padding: 1rem 1rem 0.85rem 1rem;
            margin-bottom: 1rem;
            box-shadow: 0 16px 32px rgba(0, 0, 0, 0.28);
        }
        .action-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.05rem;
            color: #fffaf0;
            margin-bottom: 0.35rem;
        }
        .risk-pill {
            display: inline-block;
            font-size: 0.78rem;
            padding: 0.15rem 0.55rem;
            border-radius: 999px;
            margin-bottom: 0.65rem;
            color: white;
        }
        .risk-low { background: #1f6f50; }
        .risk-medium { background: #9b6a11; }
        .risk-extreme { background: #8f1d1d; }
        .muted-note {
            color: #b9cae5;
            font-size: 0.92rem;
        }
        .pill {
            display: inline-block;
            padding: 0.18rem 0.55rem;
            border-radius: 999px;
            margin-right: 0.4rem;
            margin-bottom: 0.4rem;
            background: rgba(236, 193, 112, 0.14);
            border: 1px solid rgba(236, 193, 112, 0.24);
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> tuple[str, int, int, int, float, float, int, int, float, bool, str]:
    """Collect dashboard controls from the sidebar."""

    st.sidebar.header("Feed Controls")
    refresh_default = int(os.getenv("SIM_DASHBOARD_REFRESH_SECONDS", "36"))
    refresh_default = max(12, min(360, round(refresh_default / 12) * 12))
    db_path = st.sidebar.text_input(
        "SQLite path",
        value=os.getenv(
            "SIM_TRACKER_DB_PATH",
            os.path.join(os.getenv("SIM_DATA_DIR", "shared/data"), "pool_tracker_live.db"),
        ),
        help="The dashboard caches finalized validator activity and per-epoch aggregate snapshots here.",
    )
    refresh_seconds = int(
        st.sidebar.slider(
            "Refresh interval (seconds)",
            min_value=12,
            max_value=360,
            value=refresh_default,
            step=12,
            help="Slot cadence is 12 seconds, so refresh controls move in slot-sized increments.",
        )
    )
    history_epochs = int(
        st.sidebar.slider(
            "Slot history window (epochs)",
            min_value=1,
            max_value=3,
            value=DEFAULT_HISTORY_EPOCHS,
            step=1,
            help="Each epoch contributes 32 slots. A 3-epoch window shows 96 slot points.",
        )
    )
    activity_lookback_epochs = int(
        st.sidebar.slider(
            "Activity lookback (epochs)",
            min_value=1,
            max_value=128,
            value=DEFAULT_ACTIVITY_LOOKBACK_EPOCHS,
            step=1,
            help="Top validators are ranked by deposit plus withdrawal volume across this finalized-slot window.",
        )
    )
    leaderboard_limit = int(
        st.sidebar.slider(
            "Leaderboard size",
            min_value=10,
            max_value=200,
            value=100,
            step=10,
        )
    )
    state_id = st.sidebar.selectbox(
        "Beacon state",
        options=["head", "finalized"],
        index=0,
        help="Use head for the freshest aggregate balances or finalized for a fully checkpointed view.",
    )

    st.sidebar.header("Fees")
    fee_rate = float(
        st.sidebar.slider(
            "Reward fee rate",
            min_value=0.0,
            max_value=0.50,
            value=0.10,
            step=0.01,
            help="Applied to positive gross rewards in the synthetic validator basket.",
        )
    )

    st.sidebar.header("Slashing")
    slash_pass_through = float(
        st.sidebar.slider(
            "Observed slash pass-through",
            min_value=0.0,
            max_value=1.0,
            value=1.0,
            step=0.05,
            help="How much of observed on-chain slashing loss should be treated as user-borne in the aggregate PnL view.",
        )
    )
    modeled_slashed_validators = int(
        st.sidebar.slider(
            "Modeled slashed validators",
            min_value=0,
            max_value=10,
            value=0,
            step=1,
            help="Adds a hypothetical slash stress to the aggregate portfolio on top of observed chain events.",
        )
    )
    modeled_slash_fraction = float(
        st.sidebar.slider(
            "Modeled slash fraction",
            min_value=0.0,
            max_value=0.10,
            value=0.0,
            step=0.005,
            help="Scenario-only slash fraction applied to the modeled slashed-validator count.",
        )
    )

    auto_refresh = st.sidebar.toggle("Auto refresh", value=True)
    st.sidebar.button("Refresh now", type="primary")
    st.sidebar.caption("Local host URL: http://localhost:8501")
    return (
        db_path,
        refresh_seconds,
        history_epochs,
        activity_lookback_epochs,
        fee_rate,
        slash_pass_through,
        leaderboard_limit,
        modeled_slashed_validators,
        modeled_slash_fraction,
        auto_refresh,
        state_id,
    )


def build_pool_history_frame(snapshot: LiveDashboardSnapshot) -> pd.DataFrame:
    """Convert stored pool snapshots into a chart-friendly DataFrame."""

    rows = []
    for observed, adjusted in zip(snapshot.pool_history, snapshot.adjusted_pool_history):
        rows.append(
            {
                "slot": observed.slot,
                "epoch": observed.epoch,
                "slot_in_epoch": (observed.slot or 0) % 32,
                "observed_nav_eth": gwei_to_eth(observed.nav_gwei),
                "scenario_nav_eth": gwei_to_eth(adjusted.nav_gwei),
                "observed_cumulative_pnl_eth": gwei_to_eth(observed.cumulative_pnl_gwei),
                "scenario_cumulative_pnl_eth": gwei_to_eth(adjusted.cumulative_pnl_gwei),
                "observed_net_rewards_eth": gwei_to_eth(observed.net_rewards_gwei),
                "scenario_net_rewards_eth": gwei_to_eth(adjusted.net_rewards_gwei),
                "observed_penalties_eth": gwei_to_eth(observed.penalties_gwei + observed.slashing_losses_gwei),
                "scenario_penalties_eth": gwei_to_eth(adjusted.penalties_gwei + adjusted.slashing_losses_gwei),
                "observed_fees_eth": gwei_to_eth(observed.fees_gwei),
                "scenario_fees_eth": gwei_to_eth(adjusted.fees_gwei),
                "observed_share_price_gwei": observed.share_price_gwei,
                "scenario_share_price_gwei": adjusted.share_price_gwei,
            }
        )
    return pd.DataFrame(rows)


def build_behavior_projection_frame(snapshot: LiveDashboardSnapshot) -> pd.DataFrame:
    """Build chart-friendly next-slot behavior projection points."""

    rows = []
    for projection in snapshot.behavior_projections:
        rows.append(
            {
                "slot": projection.projection_slot,
                "epoch": projection.projection_epoch,
                "action": projection.action,
                "action_label": ACTION_LABELS.get(projection.action, projection.action),
                "action_color": ACTION_COLORS.get(projection.action, "#ecc170"),
                "expected_delta_eth": gwei_to_eth(projection.expected_delta_gwei),
                "projected_nav_eth": gwei_to_eth(projection.projected_nav_gwei),
                "projected_cumulative_pnl_eth": gwei_to_eth(projection.projected_cumulative_pnl_gwei),
                "projected_net_rewards_eth": gwei_to_eth(projection.projected_net_rewards_gwei),
                "projected_penalties_eth": gwei_to_eth(projection.projected_penalties_gwei),
                "projected_fees_eth": gwei_to_eth(projection.projected_fees_gwei),
                "projected_share_price_gwei": projection.projected_share_price_gwei,
            }
        )
    return pd.DataFrame(rows)


def build_leaderboard_frame(snapshot: LiveDashboardSnapshot) -> pd.DataFrame:
    """Build the top-validator activity table."""

    rows = []
    for item in snapshot.leaderboard_rows:
        rows.append(
            {
                "validator_index": item.validator_index,
                "pubkey": item.public_key[:10] + "..." + item.public_key[-8:] if item.public_key else "",
                "status": item.status,
                "balance_eth": gwei_to_eth(item.balance_gwei),
                "effective_balance_eth": gwei_to_eth(item.effective_balance_gwei),
                "deposit_eth": gwei_to_eth(item.deposit_gwei),
                "withdrawal_eth": gwei_to_eth(item.withdrawal_gwei),
                "total_activity_eth": gwei_to_eth(item.total_activity_gwei),
                "net_flow_eth": gwei_to_eth(item.net_flow_gwei),
                "slot_delta_eth": gwei_to_eth(item.epoch_delta_gwei) if item.epoch_delta_gwei is not None else None,
                "proposer_slashings": item.proposer_slashings,
                "attester_slashings": item.attester_slashings,
                "total_slashings": item.total_slashings,
            }
        )
    return pd.DataFrame(rows)


def build_validator_history_frame(snapshot: LiveDashboardSnapshot) -> pd.DataFrame:
    """Build a long-form validator history frame for line charts."""

    rows = []
    for validator_index, history in snapshot.validator_history.items():
        for item in history:
            rows.append(
                {
                    "slot": item.slot,
                    "epoch": item.epoch,
                    "validator_index": str(validator_index),
                    "balance_eth": gwei_to_eth(item.balance_gwei),
                    "status": item.status,
                }
            )
    return pd.DataFrame(rows)


def build_cadlabs_time_series_frame(replication: CadLabsReplicationSnapshot) -> pd.DataFrame:
    """Build a DataFrame for the simplified CADLabs-style time-series charts."""

    return pd.DataFrame(
        [
            {
                "scenario": item.scenario,
                "epoch": item.epoch,
                "projected_validators": item.projected_validators,
                "projected_staked_eth": item.projected_staked_eth,
                "revenue_yield_pct": item.revenue_yield_pct,
                "profit_yield_pct": item.profit_yield_pct,
            }
            for item in replication.time_series
        ]
    )


def build_cadlabs_sweep_frame(replication: CadLabsReplicationSnapshot, series_name: str) -> pd.DataFrame:
    """Build a sweep DataFrame for stake or ETH-price sensitivity charts."""

    points = replication.stake_sweep if series_name == "stake" else replication.price_sweep
    x_label = "staked_eth" if series_name == "stake" else "eth_price_usd"
    return pd.DataFrame(
        [
            {
                x_label: item.x_value,
                "revenue_yield_pct": item.revenue_yield_pct,
                "profit_yield_pct": item.profit_yield_pct,
            }
            for item in points
        ]
    )


def build_cadlabs_surface_frame(replication: CadLabsReplicationSnapshot) -> pd.DataFrame:
    """Build a DataFrame for the profit-yield heatmap."""

    return pd.DataFrame(
        [
            {
                "staked_eth": item.staked_eth,
                "eth_price_usd": item.eth_price_usd,
                "profit_yield_pct": item.profit_yield_pct,
            }
            for item in replication.profit_surface
        ]
    )


def build_cadlabs_cohort_summary_frame(replication: CadLabsReplicationSnapshot) -> pd.DataFrame:
    """Build a table of empirical cohort assumptions and resulting yields."""

    return pd.DataFrame(
        [
            {
                "cohort": item.cohort,
                "validator_count": item.validator_count,
                "share_pct": item.share_pct,
                "active_share_pct": item.active_share_pct,
                "avg_balance_eth": item.avg_balance_eth,
                "avg_slot_delta_eth": item.avg_slot_delta_eth,
                "total_activity_eth": item.total_activity_eth,
                "slash_rate_pct": item.slash_rate_pct,
                "reward_multiplier": item.reward_multiplier,
                "drag_multiplier": item.drag_multiplier,
                "cost_multiplier": item.cost_multiplier,
                "revenue_yield_pct": item.revenue_yield_pct,
                "profit_yield_pct": item.profit_yield_pct,
            }
            for item in replication.cohorts
        ]
    )


def build_cadlabs_cohort_time_series_frame(replication: CadLabsReplicationSnapshot) -> pd.DataFrame:
    """Build a DataFrame for cohort-level profit and revenue yield paths."""

    return pd.DataFrame(
        [
            {
                "cohort": item.cohort,
                "epoch": item.epoch,
                "revenue_yield_pct": item.revenue_yield_pct,
                "profit_yield_pct": item.profit_yield_pct,
            }
            for item in replication.cohort_time_series
        ]
    )


def add_epoch_regime_shading(figure: go.Figure, frame: pd.DataFrame) -> None:
    """Shade slot ranges by epoch so shifting regimes stay visible."""

    if frame.empty or "slot" not in frame or "epoch" not in frame:
        return
    regimes = (
        frame.dropna(subset=["slot", "epoch"])
        .groupby("epoch", as_index=False)
        .agg(slot_start=("slot", "min"), slot_end=("slot", "max"))
    )
    for index, regime in regimes.iterrows():
        figure.add_vrect(
            x0=float(regime["slot_start"]) - 0.5,
            x1=float(regime["slot_end"]) + 0.5,
            fillcolor=EPOCH_REGIME_COLORS[index % len(EPOCH_REGIME_COLORS)],
            line_width=0,
            layer="below",
        )


def has_slash_adjustment(snapshot: LiveDashboardSnapshot) -> bool:
    """Return whether the scenario differs from observed chain results."""

    return (
        abs(snapshot.slash_settings.slash_pass_through - 1.0) > 1e-9
        or snapshot.slash_settings.modeled_slashed_validators > 0
        and snapshot.slash_settings.modeled_slash_fraction > 0
    )


def add_history_trace(
    figure: go.Figure,
    frame: pd.DataFrame,
    *,
    y_column: str,
    name: str,
    color: str,
    dash: str = "solid",
) -> None:
    """Add a consistent history line trace."""

    figure.add_trace(
        go.Scatter(
            x=frame["slot"],
            y=frame[y_column],
            mode="lines+markers",
            name=name,
            line=dict(color=color, width=2.5, dash=dash),
            marker=dict(size=7),
        )
    )


def add_projection_trace(
    figure: go.Figure,
    frame: pd.DataFrame,
    *,
    y_column: str,
    name: str,
    hover_label: str,
    symbol: str,
) -> None:
    """Overlay next-slot action markers on a chart."""

    if frame.empty:
        return
    figure.add_trace(
        go.Scatter(
            x=frame["slot"],
            y=frame[y_column],
            mode="markers",
            name=name,
            showlegend=False,
            marker=dict(
                size=13,
                symbol=symbol,
                color=frame["action_color"].tolist(),
                line=dict(color="rgba(7, 17, 29, 0.95)", width=1.5),
            ),
            customdata=frame[["action_label", "expected_delta_eth"]].to_numpy(),
            hovertemplate=(
                "%{customdata[0]}<br>"
                "Slot %{x}<br>"
                + hover_label
                + ": %{y:.6f}<br>"
                "Modeled delta: %{customdata[1]:.6f} ETH"
                "<extra></extra>"
            ),
        )
    )


def style_time_series_figure(
    figure: go.Figure,
    *,
    title: str,
    yaxis_title: str,
) -> None:
    """Apply consistent styling for dashboard time-series charts."""

    figure.update_layout(
        template="plotly_dark",
        title=title,
        hovermode="x unified",
        legend_title_text="",
        margin=dict(l=18, r=18, t=58, b=18),
        xaxis_title="Slot",
        yaxis_title=yaxis_title,
    )
    figure.update_xaxes(type="linear", tickmode="auto")


def render_action_cards(snapshot: LiveDashboardSnapshot) -> None:
    """Render modeled next-action cards."""

    st.subheader("Modeled Next Moves")
    st.caption("These are local repo models from `simulator/behavior.py`, not CADLabs outputs, and are plotted as next-slot scenarios.")
    best_action = snapshot.action_recommendations[0]
    st.success(
        f"Best modeled next move right now: `{best_action.action}` "
        f"({gwei_to_eth(best_action.expected_delta_gwei):.6f} ETH expected next-slot delta)."
    )

    left, right = st.columns(2)
    columns = [left, right]
    for index, recommendation in enumerate(snapshot.action_recommendations):
        risk_class = recommendation.risk_level.lower()
        column = columns[index % 2]
        caution = ""
        if recommendation.caution:
            caution = f"<p class='muted-note'>{recommendation.caution}</p>"
        column.markdown(
            f"""
            <div class="action-card">
                <div class="action-title">{recommendation.action}</div>
                <div class="risk-pill risk-{risk_class}">{recommendation.risk_level}</div>
                <p><strong>Modeled next-slot delta:</strong> {gwei_to_eth(recommendation.expected_delta_gwei):.6f} ETH</p>
                <p><strong>Confidence:</strong> {recommendation.confidence:.0%}</p>
                <p>{recommendation.rationale}</p>
                {caution}
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_pool_charts(snapshot: LiveDashboardSnapshot) -> None:
    """Render aggregate charts for the top-validator basket."""

    pool_history_frame = build_pool_history_frame(snapshot)
    behavior_projection_frame = build_behavior_projection_frame(snapshot)
    validator_history_frame = build_validator_history_frame(snapshot)
    adjusted = has_slash_adjustment(snapshot)

    if not pool_history_frame.empty:
        st.subheader("Aggregate Time Series")
        st.caption(
            f"Showing the latest {len(pool_history_frame)} stored slots for the current validator basket, "
            f"covering slots {snapshot.history_window_start_slot}-{snapshot.history_window_end_slot}. "
            f"Behavior markers are plotted one step ahead at slot {snapshot.head_slot + 1}: "
            "add_to_stake (gold), wait (blue), withdraw (orange), nothing-at-stake-attack (crimson)."
        )

        upper_left, upper_right = st.columns(2)
        nav_figure = go.Figure()
        if adjusted:
            add_history_trace(
                nav_figure,
                pool_history_frame,
                y_column="observed_nav_eth",
                name="Observed NAV",
                color=SERIES_COLORS["observed_nav"],
                dash="dot",
            )
        add_history_trace(
            nav_figure,
            pool_history_frame,
            y_column="scenario_nav_eth",
            name="Scenario NAV",
            color=SERIES_COLORS["scenario_nav"],
        )
        add_projection_trace(
            nav_figure,
            behavior_projection_frame,
            y_column="projected_nav_eth",
            name="Projected next-slot NAV",
            hover_label="Scenario NAV (ETH)",
            symbol="diamond",
        )
        add_epoch_regime_shading(nav_figure, pool_history_frame)
        style_time_series_figure(nav_figure, title="Slot-Level Aggregate NAV", yaxis_title="ETH")
        upper_left.plotly_chart(nav_figure, width="stretch")

        pnl_figure = go.Figure()
        if adjusted:
            add_history_trace(
                pnl_figure,
                pool_history_frame,
                y_column="observed_cumulative_pnl_eth",
                name="Observed cumulative PnL",
                color=SERIES_COLORS["observed_cumulative_pnl"],
                dash="dot",
            )
        add_history_trace(
            pnl_figure,
            pool_history_frame,
            y_column="scenario_cumulative_pnl_eth",
            name="Scenario cumulative PnL",
            color=SERIES_COLORS["scenario_cumulative_pnl"],
        )
        add_projection_trace(
            pnl_figure,
            behavior_projection_frame,
            y_column="projected_cumulative_pnl_eth",
            name="Projected next-slot cumulative PnL",
            hover_label="Scenario cumulative PnL (ETH)",
            symbol="diamond-open",
        )
        add_epoch_regime_shading(pnl_figure, pool_history_frame)
        style_time_series_figure(pnl_figure, title="Slot-Level Cumulative PnL", yaxis_title="ETH")
        upper_right.plotly_chart(pnl_figure, width="stretch")

        lower_left, lower_right = st.columns(2)
        reward_figure = go.Figure()
        if adjusted:
            add_history_trace(
                reward_figure,
                pool_history_frame,
                y_column="observed_net_rewards_eth",
                name="Observed net rewards",
                color=SERIES_COLORS["observed_net_rewards"],
                dash="dot",
            )
            add_history_trace(
                reward_figure,
                pool_history_frame,
                y_column="observed_penalties_eth",
                name="Observed penalties + slash losses",
                color=SERIES_COLORS["observed_penalties"],
                dash="dot",
            )
        add_history_trace(
            reward_figure,
            pool_history_frame,
            y_column="scenario_net_rewards_eth",
            name="Scenario net rewards",
            color=SERIES_COLORS["scenario_net_rewards"],
        )
        add_history_trace(
            reward_figure,
            pool_history_frame,
            y_column="scenario_penalties_eth",
            name="Scenario penalties + slash losses",
            color=SERIES_COLORS["scenario_penalties"],
        )
        add_history_trace(
            reward_figure,
            pool_history_frame,
            y_column="scenario_fees_eth",
            name="Derived fees",
            color=SERIES_COLORS["scenario_fees"],
        )
        add_projection_trace(
            reward_figure,
            behavior_projection_frame,
            y_column="projected_net_rewards_eth",
            name="Projected next-slot net rewards",
            hover_label="Projected net rewards (ETH)",
            symbol="circle",
        )
        add_projection_trace(
            reward_figure,
            behavior_projection_frame,
            y_column="projected_penalties_eth",
            name="Projected penalties",
            hover_label="Projected penalties (ETH)",
            symbol="x",
        )
        add_projection_trace(
            reward_figure,
            behavior_projection_frame,
            y_column="projected_fees_eth",
            name="Projected fees",
            hover_label="Projected fees (ETH)",
            symbol="square",
        )
        add_epoch_regime_shading(reward_figure, pool_history_frame)
        style_time_series_figure(
            reward_figure,
            title="Derived Slot-Level Rewards, Penalties, And Fees",
            yaxis_title="ETH",
        )
        lower_left.plotly_chart(reward_figure, width="stretch")

        share_figure = go.Figure()
        if adjusted:
            add_history_trace(
                share_figure,
                pool_history_frame,
                y_column="observed_share_price_gwei",
                name="Observed share price",
                color=SERIES_COLORS["observed_share_price"],
                dash="dot",
            )
        add_history_trace(
            share_figure,
            pool_history_frame,
            y_column="scenario_share_price_gwei",
            name="Scenario share price",
            color=SERIES_COLORS["scenario_share_price"],
        )
        add_projection_trace(
            share_figure,
            behavior_projection_frame,
            y_column="projected_share_price_gwei",
            name="Projected next-slot share price",
            hover_label="Projected share price (gwei)",
            symbol="star",
        )
        add_epoch_regime_shading(share_figure, pool_history_frame)
        style_time_series_figure(
            share_figure,
            title="Slot-Level Share Price",
            yaxis_title="gwei",
        )
        lower_right.plotly_chart(share_figure, width="stretch")

        st.subheader("Validator Flow And Balance History")
        activity_frame = build_leaderboard_frame(snapshot)
        if not activity_frame.empty:
            slash_figure = px.bar(
                activity_frame.head(20),
                x="validator_index",
                y=["deposit_eth", "withdrawal_eth"],
                barmode="group",
                title="Top 20 Validator Flows In Lookback Window",
                labels={"value": "ETH", "validator_index": "validator"},
                template="plotly_dark",
            )
            st.plotly_chart(slash_figure, width="stretch")

    if not validator_history_frame.empty:
        validator_figure = px.line(
            validator_history_frame,
            x="slot",
            y="balance_eth",
            color="validator_index",
            markers=True,
            title="Tracked Validator Slot Balance History",
            labels={"balance_eth": "ETH", "validator_index": "validator"},
            template="plotly_dark",
        )
        add_epoch_regime_shading(validator_figure, validator_history_frame)
        st.plotly_chart(validator_figure, width="stretch")


def render_cadlabs_replication_tab(snapshot: LiveDashboardSnapshot) -> None:
    """Render a simplified CADLabs-style yield lab from the live validator basket."""

    st.subheader("CADLabs-Style Validator Revenue And Profit Yields")
    st.caption(
        "This tab replicates the shape of CADLabs notebook-style validator revenue and profit yield experiments "
        "using the live validator basket from the main page. It uses published CADLabs formulas for revenue, "
        "profit, and annualized yields, but the adoption path and cohorting remain local empirical approximations."
    )

    controls_left, controls_middle, controls_right = st.columns(3)
    eth_price_usd = float(
        controls_left.number_input(
            "ETH price assumption (USD)",
            min_value=250.0,
            max_value=25_000.0,
            value=2500.0,
            step=250.0,
            help="Revenue yield is effectively price-neutral, while profit yield changes as fixed validator opex is diluted across staked ETH value.",
        )
    )
    monthly_validator_cost_usd = float(
        controls_middle.number_input(
            "Validator opex per month (USD)",
            min_value=0.0,
            max_value=500.0,
            value=15.0,
            step=1.0,
            help="Simplified fixed validator operating cost used to convert CADLabs-style revenue yield into profit yield.",
        )
    )
    projection_epochs = int(
        controls_right.slider(
            "Projection horizon (epochs)",
            min_value=8,
            max_value=256,
            value=64,
            step=8,
            help="Future epochs shown in the low, normal, and high adoption scenario projections.",
        )
    )

    replication = build_cadlabs_replication(
        snapshot,
        CadLabsReplicationConfig(
            eth_price_usd=eth_price_usd,
            monthly_validator_cost_usd=monthly_validator_cost_usd,
            projection_epochs=projection_epochs,
        ),
    )
    time_series_frame = build_cadlabs_time_series_frame(replication)
    stake_sweep_frame = build_cadlabs_sweep_frame(replication, "stake")
    price_sweep_frame = build_cadlabs_sweep_frame(replication, "price")
    surface_frame = build_cadlabs_surface_frame(replication)
    cohort_summary_frame = build_cadlabs_cohort_summary_frame(replication)
    cohort_time_series_frame = build_cadlabs_cohort_time_series_frame(replication)

    metrics = st.columns(5)
    metrics[0].metric("Tracked validators", replication.tracked_validators)
    metrics[1].metric("Active share", f"{replication.active_share_pct:.1f}%")
    metrics[2].metric("Revenue yield", f"{replication.annualized_revenue_yield_pct:.2f}%")
    metrics[3].metric("Profit yield", f"{replication.annualized_profit_yield_pct:.2f}%")
    metrics[4].metric(
        "Inferred adoption",
        f"{replication.inferred_adoption_validators_per_epoch:.2f} val/epoch",
    )

    st.markdown(
        f"""
        <p class="muted-note">
        Current sample stake: {replication.current_staked_eth:.4f} ETH | Average validator balance: {replication.average_balance_eth:.4f} ETH |
        Annualized net yield before opex: {replication.annualized_net_yield_pct:.2f}% | Annualized opex drag: {replication.annualized_cost_yield_pct:.2f}%
        </p>
        """,
        unsafe_allow_html=True,
    )

    if not time_series_frame.empty:
        top_left, top_right = st.columns(2)

        revenue_figure = px.line(
            time_series_frame,
            x="epoch",
            y="revenue_yield_pct",
            color="scenario",
            title="Revenue Yield Over Time",
            labels={
                "epoch": "Epoch",
                "revenue_yield_pct": "Annualized revenue yield (%)",
                "scenario": "Scenario",
            },
            template="plotly_dark",
        )
        revenue_figure.update_layout(margin=dict(l=18, r=18, t=58, b=18), hovermode="x unified")
        top_left.plotly_chart(revenue_figure, width="stretch")

        profit_figure = px.line(
            time_series_frame,
            x="epoch",
            y="profit_yield_pct",
            color="scenario",
            title="Profit Yield Over Time",
            labels={
                "epoch": "Epoch",
                "profit_yield_pct": "Annualized profit yield (%)",
                "scenario": "Scenario",
            },
            template="plotly_dark",
        )
        profit_figure.update_layout(margin=dict(l=18, r=18, t=58, b=18), hovermode="x unified")
        top_right.plotly_chart(profit_figure, width="stretch")

    middle_left, middle_right = st.columns(2)
    if not stake_sweep_frame.empty:
        stake_melt = stake_sweep_frame.melt(
            id_vars=["staked_eth"],
            value_vars=["revenue_yield_pct", "profit_yield_pct"],
            var_name="metric",
            value_name="yield_pct",
        )
        stake_figure = px.line(
            stake_melt,
            x="staked_eth",
            y="yield_pct",
            color="metric",
            title="Yield Sensitivity To ETH Staked",
            labels={
                "staked_eth": "Projected ETH staked",
                "yield_pct": "Annualized yield (%)",
                "metric": "Metric",
            },
            template="plotly_dark",
        )
        stake_figure.update_layout(margin=dict(l=18, r=18, t=58, b=18), hovermode="x unified")
        middle_left.plotly_chart(stake_figure, width="stretch")

    if not price_sweep_frame.empty:
        price_melt = price_sweep_frame.melt(
            id_vars=["eth_price_usd"],
            value_vars=["revenue_yield_pct", "profit_yield_pct"],
            var_name="metric",
            value_name="yield_pct",
        )
        price_figure = px.line(
            price_melt,
            x="eth_price_usd",
            y="yield_pct",
            color="metric",
            title="Yield Sensitivity To ETH Price",
            labels={
                "eth_price_usd": "ETH price (USD)",
                "yield_pct": "Annualized yield (%)",
                "metric": "Metric",
            },
            template="plotly_dark",
        )
        price_figure.update_layout(margin=dict(l=18, r=18, t=58, b=18), hovermode="x unified")
        middle_right.plotly_chart(price_figure, width="stretch")

    if not surface_frame.empty:
        st.subheader("Profit Yield Surface")
        surface_grid = surface_frame.pivot(
            index="eth_price_usd",
            columns="staked_eth",
            values="profit_yield_pct",
        )
        surface_figure = go.Figure(
            data=go.Heatmap(
                x=surface_grid.columns.tolist(),
                y=surface_grid.index.tolist(),
                z=surface_grid.to_numpy(),
                colorscale="Tealrose",
                colorbar=dict(title="Profit yield (%)"),
            )
        )
        surface_figure.update_layout(
            template="plotly_dark",
            title="Profit Yield Across ETH Price And ETH Staked",
            xaxis_title="Projected ETH staked",
            yaxis_title="ETH price (USD)",
            margin=dict(l=18, r=18, t=58, b=18),
        )
        st.plotly_chart(surface_figure, width="stretch")

    bottom_left, bottom_right = st.columns(2)
    if not cohort_time_series_frame.empty:
        cohort_figure = px.line(
            cohort_time_series_frame,
            x="epoch",
            y="profit_yield_pct",
            color="cohort",
            title="Profit Yield By Empirical Validator Cohort",
            labels={
                "epoch": "Epoch",
                "profit_yield_pct": "Annualized profit yield (%)",
                "cohort": "Cohort",
            },
            template="plotly_dark",
        )
        cohort_figure.update_layout(margin=dict(l=18, r=18, t=58, b=18), hovermode="x unified")
        bottom_left.plotly_chart(cohort_figure, width="stretch")

    if not cohort_summary_frame.empty:
        cohort_bar = px.bar(
            cohort_summary_frame,
            x="cohort",
            y="validator_count",
            color="profit_yield_pct",
            color_continuous_scale="Tealrose",
            title="Empirical Cohort Composition",
            labels={"validator_count": "Validators", "cohort": "Cohort"},
            template="plotly_dark",
        )
        cohort_bar.update_layout(margin=dict(l=18, r=18, t=58, b=18))
        bottom_right.plotly_chart(cohort_bar, width="stretch")

    st.subheader("Empirical Cohort Table")
    st.dataframe(
        cohort_summary_frame,
        width="stretch",
        hide_index=True,
        column_config={
            "share_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "active_share_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "avg_balance_eth": st.column_config.NumberColumn(format="%.6f ETH"),
            "avg_slot_delta_eth": st.column_config.NumberColumn(format="%.9f ETH"),
            "total_activity_eth": st.column_config.NumberColumn(format="%.6f ETH"),
            "slash_rate_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "reward_multiplier": st.column_config.NumberColumn(format="%.2f x"),
            "drag_multiplier": st.column_config.NumberColumn(format="%.2f x"),
            "cost_multiplier": st.column_config.NumberColumn(format="%.2f x"),
            "revenue_yield_pct": st.column_config.NumberColumn(format="%.2f%%"),
            "profit_yield_pct": st.column_config.NumberColumn(format="%.2f%%"),
        },
    )

    with st.expander("CADLabs-Style Assumptions", expanded=False):
        for note in replication.notes:
            st.write(f"- {note}")


def render_machine_readable_snapshot(snapshot: LiveDashboardSnapshot) -> None:
    """Render and expose the latest synthetic basket snapshot payload."""

    st.subheader("Snapshot Payload")
    payload = {
        "observed": asdict(snapshot.pool_snapshot),
        "scenario_adjusted": asdict(snapshot.adjusted_pool_snapshot),
        "behavior_projections": [asdict(item) for item in snapshot.behavior_projections],
        "activity_window_start_slot": snapshot.activity_window_start_slot,
        "activity_window_end_slot": snapshot.activity_window_end_slot,
        "history_window_start_slot": snapshot.history_window_start_slot,
        "history_window_end_slot": snapshot.history_window_end_slot,
    }
    st.json(payload)
    st.download_button(
        label="Download latest snapshot JSON",
        data=json.dumps(payload, indent=2),
        file_name=f"{snapshot.pool.pool_id}-slot-{snapshot.head_slot}.json",
        mime="application/json",
    )


def render_methodology(snapshot: LiveDashboardSnapshot) -> None:
    """Render provenance and modeling notes."""

    with st.expander("Methodology And Provenance", expanded=False):
        for note in snapshot.methodology_notes:
            st.write(f"- {note}")


def main() -> None:
    """Run the Streamlit dashboard."""

    st.set_page_config(
        page_title="Hoodi Validator Activity Leaderboard",
        layout="wide",
    )
    render_theme()
    (
        db_path,
        refresh_seconds,
        history_epochs,
        activity_lookback_epochs,
        fee_rate,
        slash_pass_through,
        leaderboard_limit,
        modeled_slashed_validators,
        modeled_slash_fraction,
        auto_refresh,
        state_id,
    ) = render_sidebar()

    if auto_refresh and st_autorefresh is not None:
        st_autorefresh(interval=refresh_seconds * 1_000, key="hoodi-validator-flow-feed")
    elif auto_refresh:
        st.sidebar.warning("Install `streamlit-autorefresh` to enable automatic refresh.")

    try:
        runtime_config = load_dashboard_runtime_config(
            db_path=db_path,
            history_epochs=history_epochs,
            activity_lookback_epochs=activity_lookback_epochs,
            leaderboard_limit=leaderboard_limit,
            fee_rate=fee_rate,
            slash_pass_through=slash_pass_through,
            modeled_slashed_validators=modeled_slashed_validators,
            modeled_slash_fraction=modeled_slash_fraction,
            state_id=state_id,
        )
        with st.spinner("Refreshing Hoodi validator activity leaderboard..."):
            snapshot = fetch_live_dashboard_snapshot(runtime_config)
    except Exception as exc:
        st.error(str(exc))
        if "429" in str(exc) or "Too Many Requests" in str(exc) or "rate limit" in str(exc).lower():
            st.info(
                "The provider rate-limited this refresh. The dashboard now relies on cached slot history and gradual backfill, so waiting for the next 12-second refresh or increasing the refresh interval should let it recover."
            )
            return
        st.info(
            "Set valid Hoodi execution and Beacon endpoints in `.env`, then restart the dashboard. "
            "The validator-flow leaderboard uses finalized beacon blocks plus current Beacon state from the configured provider."
        )
        return

    st.title("Hoodi Validator Activity Leaderboard")
    st.caption(
        "Top validators ranked by deposit and withdrawal activity over a finalized-slot window, with slot-level aggregate NAV observed directly and slot-level economics derived from consecutive state changes."
    )
    st.markdown(
        """
        <div class="pill">Alchemy-priority flow</div>
        <div class="pill">Top validator basket</div>
        <div class="pill">96-slot rolling window</div>
        <div class="pill">Observed + scenario slashing</div>
        """,
        unsafe_allow_html=True,
    )

    top_row = st.columns(6)
    top_row[0].metric("Current slot", snapshot.head_slot)
    top_row[1].metric("Current epoch", snapshot.current_epoch)
    top_row[2].metric("Tracked validators", len(snapshot.leaderboard_rows))
    top_row[3].metric("Scenario NAV", f"{gwei_to_eth(snapshot.adjusted_pool_snapshot.nav_gwei):.6f} ETH")
    top_row[4].metric("Derived slot rewards", f"{gwei_to_eth(snapshot.adjusted_pool_snapshot.net_rewards_gwei):.6f} ETH")
    top_row[5].metric("Observed slash ops", snapshot.total_observed_slashings)

    secondary_row = st.columns(6)
    secondary_row[0].metric("Deposits", f"{gwei_to_eth(snapshot.total_deposit_gwei):.6f} ETH")
    secondary_row[1].metric("Withdrawals", f"{gwei_to_eth(snapshot.total_withdrawal_gwei):.6f} ETH")
    secondary_row[2].metric("Reward fee", f"{snapshot.pool.fee_rate:.0%}")
    secondary_row[3].metric("Slash pass-through", f"{snapshot.slash_settings.slash_pass_through:.0%}")
    secondary_row[4].metric("Modeled slashed vals", snapshot.slash_settings.modeled_slashed_validators)
    secondary_row[5].metric("Modeled slash fraction", f"{snapshot.slash_settings.modeled_slash_fraction:.2%}")

    st.markdown(
        f"""
        <p class="muted-note">
        Refreshed at {snapshot.refreshed_at.isoformat()} | Head slot {snapshot.head_slot} | Finalized slot {snapshot.finalized_slot} | History window slots {snapshot.history_window_start_slot}-{snapshot.history_window_end_slot} | Activity window slots {snapshot.activity_window_start_slot}-{snapshot.activity_window_end_slot}
        </p>
        """,
        unsafe_allow_html=True,
    )

    live_tab, cadlabs_tab = st.tabs(
        ["Validator Flow Feed", "CADLabs-Style Yield Lab"]
    )

    with live_tab:
        render_pool_charts(snapshot)

        st.subheader("Top Validator Activity")
        leaderboard_frame = build_leaderboard_frame(snapshot)
        st.dataframe(
            leaderboard_frame,
            width="stretch",
            hide_index=True,
            column_config={
                "balance_eth": st.column_config.NumberColumn(format="%.6f ETH"),
                "effective_balance_eth": st.column_config.NumberColumn(format="%.6f ETH"),
                "deposit_eth": st.column_config.NumberColumn(format="%.6f ETH"),
                "withdrawal_eth": st.column_config.NumberColumn(format="%.6f ETH"),
                "total_activity_eth": st.column_config.NumberColumn(format="%.6f ETH"),
                "net_flow_eth": st.column_config.NumberColumn(format="%.6f ETH"),
                "slot_delta_eth": st.column_config.NumberColumn("Slot delta", format="%.9f ETH"),
            },
        )

        render_action_cards(snapshot)
        render_machine_readable_snapshot(snapshot)
        render_methodology(snapshot)

        with st.expander("Notes And Limitations", expanded=False):
            for note in snapshot.notes:
                st.write(f"- {note}")

    with cadlabs_tab:
        render_cadlabs_replication_tab(snapshot)


if __name__ == "__main__":
    main()
