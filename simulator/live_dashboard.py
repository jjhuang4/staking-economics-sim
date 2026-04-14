"""Streamlit dashboard for live Hoodi staking-pool monitoring."""

from __future__ import annotations

from dataclasses import asdict
import json
import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
except ImportError:
    st_autorefresh = None

from pool_tracker.models import PoolSnapshot

try:
    from .live_dashboard_data import (
        DEFAULT_HISTORY_EPOCHS,
        LiveDashboardSnapshot,
        fetch_live_dashboard_snapshot,
        load_dashboard_runtime_config,
    )
except ImportError:
    from live_dashboard_data import (
        DEFAULT_HISTORY_EPOCHS,
        LiveDashboardSnapshot,
        fetch_live_dashboard_snapshot,
        load_dashboard_runtime_config,
    )

GWEI_PER_ETH = 1_000_000_000
EXAMPLE_POOL_CONFIG = """pool_id: hoodi-pool-1
name: Hoodi Pool 1
fee_rate: 0.10
slash_pass_through: 1.0
validator_indices: [123, 456, 789]
contract_addresses:
  - "0x1111111111111111111111111111111111111111"
"""


def render_hoodi_highlights(chain_id: int) -> None:
    """Render a short Hoodi-specific feature summary."""

    st.info(
        "\n".join(
            [
                "**Hoodi Highlights**",
                f"- Chain ID: `{chain_id}`",
                "- Validator and staking-focused Ethereum testnet",
                "- Permissionless validator set intended to better mirror Beacon mainnet conditions",
                "- Positioned as the Holesky replacement, with typical block times around 12-15 seconds",
            ]
        )
    )


def gwei_to_eth(value: int | float) -> float:
    """Convert gwei to ETH for display."""

    return float(value) / GWEI_PER_ETH


def render_theme() -> None:
    """Inject lightweight dashboard styling."""

    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(44, 78, 122, 0.35), transparent 30%),
                radial-gradient(circle at bottom right, rgba(13, 31, 61, 0.50), transparent 35%),
                linear-gradient(180deg, #04070f 0%, #08101e 45%, #0c1730 100%);
            color: #f7fafc;
        }
        .stApp [data-testid="stAppViewContainer"] {
            color: #f7fafc;
        }
        h1, h2, h3 {
            font-family: Georgia, "Times New Roman", serif;
            letter-spacing: 0.02em;
            color: #f8fbff;
        }
        p, li, label, div, span {
            color: #e9f1ff;
        }
        div[data-testid="stMetric"] {
            background: linear-gradient(180deg, rgba(12, 20, 37, 0.94), rgba(9, 15, 30, 0.94));
            border: 1px solid rgba(142, 180, 255, 0.16);
            border-radius: 18px;
            padding: 0.75rem 1rem;
            box-shadow: 0 16px 32px rgba(0, 0, 0, 0.28);
        }
        .action-card {
            background: linear-gradient(145deg, rgba(14, 24, 43, 0.94), rgba(10, 18, 34, 0.96));
            border: 1px solid rgba(142, 180, 255, 0.16);
            border-radius: 18px;
            padding: 1rem 1rem 0.85rem 1rem;
            margin-bottom: 1rem;
            box-shadow: 0 16px 32px rgba(0, 0, 0, 0.28);
        }
        .action-title {
            font-family: Georgia, "Times New Roman", serif;
            font-size: 1.05rem;
            color: #f8fbff;
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
            color: #b5c7ea;
            font-size: 0.92rem;
        }
        div[data-baseweb="select"] > div,
        div[data-testid="stTextInputRootElement"] > div,
        div[data-testid="stNumberInputContainer"] input {
            background: rgba(11, 18, 33, 0.92);
            color: #f8fbff;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_sidebar() -> tuple[str, str, int, int, bool, str]:
    """Collect dashboard controls from the sidebar."""

    st.sidebar.header("Feed Controls")
    pool_config_path = st.sidebar.text_input(
        "Pool config path",
        value=os.getenv("SIM_POOL_CONFIG_PATH", "pool_config.yaml"),
        help="Path to the YAML or JSON file that defines validator indices and pool contract addresses.",
    )
    db_path = st.sidebar.text_input(
        "SQLite path",
        value=os.getenv(
            "SIM_TRACKER_DB_PATH",
            os.path.join(os.getenv("SIM_DATA_DIR", "shared/data"), "pool_tracker_live.db"),
        ),
        help="The dashboard stores per-epoch snapshots here so it can build history and deltas.",
    )
    refresh_seconds = int(
        st.sidebar.slider(
            "Refresh interval (seconds)",
            min_value=5,
            max_value=300,
            value=int(os.getenv("SIM_DASHBOARD_REFRESH_SECONDS", "30")),
            step=5,
        )
    )
    history_epochs = int(
        st.sidebar.slider(
            "History window (epochs)",
            min_value=4,
            max_value=256,
            value=DEFAULT_HISTORY_EPOCHS,
            step=4,
        )
    )
    state_id = st.sidebar.selectbox(
        "Beacon state",
        options=["head", "finalized"],
        index=0,
        help="Use head for the freshest view or finalized for a more stable checkpointed view.",
    )
    auto_refresh = st.sidebar.toggle("Auto refresh", value=True)
    st.sidebar.button("Refresh now", type="primary")
    st.sidebar.caption("Local host URL: http://localhost:8501")
    return pool_config_path, db_path, refresh_seconds, history_epochs, auto_refresh, state_id


def build_pool_history_frame(pool_history: list[PoolSnapshot]) -> pd.DataFrame:
    """Convert stored pool snapshots into a chart-friendly DataFrame."""

    rows = []
    for snapshot in pool_history:
        rows.append(
            {
                "epoch": snapshot.epoch,
                "nav_eth": gwei_to_eth(snapshot.nav_gwei),
                "gross_rewards_eth": gwei_to_eth(snapshot.gross_rewards_gwei),
                "penalties_eth": gwei_to_eth(snapshot.penalties_gwei + snapshot.slashing_losses_gwei),
                "net_rewards_eth": gwei_to_eth(snapshot.net_rewards_gwei),
                "share_price_gwei": snapshot.share_price_gwei,
                "cumulative_pnl_eth": gwei_to_eth(snapshot.cumulative_pnl_gwei),
            }
        )
    return pd.DataFrame(rows)


def build_validator_frame(snapshot: LiveDashboardSnapshot) -> pd.DataFrame:
    """Build the current validator table."""

    rows = []
    for item in snapshot.validator_deltas:
        rows.append(
            {
                "validator_index": item.validator_index,
                "status": item.status,
                "balance_eth": gwei_to_eth(item.balance_gwei),
                "effective_balance_eth": gwei_to_eth(item.effective_balance_gwei),
                "delta_gwei": item.delta_gwei,
                "delta_eth": gwei_to_eth(item.delta_gwei) if item.delta_gwei is not None else None,
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
                    "epoch": item.epoch,
                    "validator_index": str(validator_index),
                    "balance_eth": gwei_to_eth(item.balance_gwei),
                    "status": item.status,
                }
            )
    return pd.DataFrame(rows)


def render_action_cards(snapshot: LiveDashboardSnapshot) -> None:
    """Render modeled next-action cards."""

    st.subheader("Modeled Next Moves")
    best_action = snapshot.action_recommendations[0]
    st.success(
        f"Best modeled next move right now: `{best_action.action}` "
        f"({gwei_to_eth(best_action.expected_delta_gwei):.6f} ETH expected next-epoch delta)."
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
                <p><strong>Modeled next-epoch delta:</strong> {gwei_to_eth(recommendation.expected_delta_gwei):.6f} ETH</p>
                <p><strong>Confidence:</strong> {recommendation.confidence:.0%}</p>
                <p>{recommendation.rationale}</p>
                {caution}
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_pool_charts(snapshot: LiveDashboardSnapshot) -> None:
    """Render time-series charts for pool and validator data."""

    pool_history_frame = build_pool_history_frame(snapshot.pool_history)
    validator_history_frame = build_validator_history_frame(snapshot)
    chart_template = "plotly_dark"
    status_frame = pd.DataFrame(
        [
            {"status": status, "count": count}
            for status, count in sorted(snapshot.status_counts.items(), key=lambda item: item[0])
        ]
    )

    if not pool_history_frame.empty:
        chart_left, chart_right = st.columns(2)
        nav_figure = px.line(
            pool_history_frame,
            x="epoch",
            y=["nav_eth", "cumulative_pnl_eth"],
            markers=True,
            title="Pool NAV And Cumulative PnL",
            labels={"value": "ETH", "variable": "series"},
            template=chart_template,
        )
        chart_left.plotly_chart(nav_figure, use_container_width=True)

        reward_figure = px.bar(
            pool_history_frame,
            x="epoch",
            y=["gross_rewards_eth", "penalties_eth", "net_rewards_eth"],
            barmode="group",
            title="Per-Epoch Reward And Penalty Flow",
            labels={"value": "ETH", "variable": "series"},
            template=chart_template,
        )
        chart_right.plotly_chart(reward_figure, use_container_width=True)

        lower_left, lower_right = st.columns(2)
        share_price_figure = px.line(
            pool_history_frame,
            x="epoch",
            y="share_price_gwei",
            markers=True,
            title="Share Price (Gwei)",
            labels={"share_price_gwei": "gwei"},
            template=chart_template,
        )
        lower_left.plotly_chart(share_price_figure, use_container_width=True)

        if not status_frame.empty:
            status_figure = px.bar(
                status_frame,
                x="status",
                y="count",
                color="status",
                title="Current Validator Status Mix",
                template=chart_template,
            )
            lower_right.plotly_chart(status_figure, use_container_width=True)

    if not validator_history_frame.empty:
        validator_figure = px.line(
            validator_history_frame,
            x="epoch",
            y="balance_eth",
            color="validator_index",
            markers=True,
            title="Validator Balance History",
            labels={"balance_eth": "ETH", "validator_index": "validator"},
            template=chart_template,
        )
        st.plotly_chart(validator_figure, use_container_width=True)


def render_machine_readable_snapshot(snapshot: LiveDashboardSnapshot) -> None:
    """Render and expose the latest snapshot payload."""

    st.subheader("Snapshot Payload")
    payload = asdict(snapshot.pool_snapshot)
    st.json(payload)
    st.download_button(
        label="Download latest snapshot JSON",
        data=json.dumps(payload, indent=2),
        file_name=f"{snapshot.pool.pool_id}-epoch-{snapshot.current_epoch}.json",
        mime="application/json",
    )


def main() -> None:
    """Run the Streamlit dashboard."""

    st.set_page_config(
        page_title="Hoodi Pool Tracker",
        layout="wide",
    )
    render_theme()
    pool_config_path, db_path, refresh_seconds, history_epochs, auto_refresh, state_id = render_sidebar()

    if auto_refresh and st_autorefresh is not None:
        st_autorefresh(interval=refresh_seconds * 1_000, key="hoodi-pool-feed")
    elif auto_refresh:
        st.sidebar.warning("Install `streamlit-autorefresh` to enable automatic refresh.")

    st.title("Hoodi Pool Tracker")
    st.caption(
        "Live read-only pool monitoring for manually configured validator sets on Ethereum Hoodi."
    )

    if not Path(pool_config_path).exists():
        st.error(f"Pool config file not found: {pool_config_path}")
        st.code(EXAMPLE_POOL_CONFIG, language="yaml")
        st.stop()

    try:
        runtime_config = load_dashboard_runtime_config(
            pool_config_path=pool_config_path,
            db_path=db_path,
            history_epochs=history_epochs,
            state_id=state_id,
        )
        with st.spinner("Refreshing Hoodi validator and pool state..."):
            snapshot = fetch_live_dashboard_snapshot(runtime_config)
    except Exception as exc:
        st.error(str(exc))
        st.info(
            "Set valid Alchemy Hoodi execution and Beacon base URLs in `.env`, then restart the dashboard. "
            "The Beacon URL should be the provider base before `/eth/v1/...`."
        )
        st.stop()

    render_hoodi_highlights(snapshot.chain_id)

    top_row = st.columns(4)
    active_validator_count = sum(
        1 for item in snapshot.current_validator_snapshots if item.status.lower().startswith("active")
    )
    slot_label = "Head slot" if state_id == "head" else "Finalized slot"
    top_row[0].metric("Current epoch", snapshot.current_epoch)
    top_row[1].metric("Active validators", f"{active_validator_count}/{len(snapshot.current_validator_snapshots)}")
    top_row[2].metric("Pool NAV", f"{gwei_to_eth(snapshot.pool_snapshot.nav_gwei):.6f} ETH")
    top_row[3].metric("Net rewards", f"{gwei_to_eth(snapshot.pool_snapshot.net_rewards_gwei):.6f} ETH")

    secondary_row = st.columns(5)
    secondary_row[0].metric("Share price", f"{snapshot.pool_snapshot.share_price_gwei:.2f} gwei")
    secondary_row[1].metric("Cumulative PnL", f"{gwei_to_eth(snapshot.pool_snapshot.cumulative_pnl_gwei):.6f} ETH")
    secondary_row[2].metric("Chain ID", snapshot.chain_id)
    secondary_row[3].metric("Execution block", snapshot.execution_block_number)
    secondary_row[4].metric("Finalized epoch", snapshot.finalized_epoch)

    st.markdown(
        f"""
        <p class="muted-note">
        Refreshed at {snapshot.refreshed_at.isoformat()} | {slot_label} {snapshot.head_slot} | Beacon state {state_id}
        </p>
        """,
        unsafe_allow_html=True,
    )

    render_pool_charts(snapshot)

    st.subheader("Current Validator Snapshot")
    validator_frame = build_validator_frame(snapshot)
    st.dataframe(
        validator_frame,
        use_container_width=True,
        hide_index=True,
        column_config={
            "balance_eth": st.column_config.NumberColumn(format="%.6f ETH"),
            "effective_balance_eth": st.column_config.NumberColumn(format="%.6f ETH"),
            "delta_gwei": st.column_config.NumberColumn(format="%d"),
            "delta_eth": st.column_config.NumberColumn(format="%.9f ETH"),
        },
    )

    render_action_cards(snapshot)
    render_machine_readable_snapshot(snapshot)

    with st.expander("Notes And Limitations", expanded=False):
        for note in snapshot.notes:
            st.write(f"- {note}")


if __name__ == "__main__":
    main()
