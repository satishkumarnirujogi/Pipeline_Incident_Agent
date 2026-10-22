"""Incidents: what is happening, refreshed every 5 seconds. Select a row for its detail.

Pages: Incident detail, Approval inbox, KPIs, Demo (dev only).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ui import api_client as api

st.set_page_config(page_title="Pipeline Incident Agent", layout="wide")

BADGE = {
    "received": "⚪ received",
    "running": "🔵 running",
    "awaiting_approval": "🟠 awaiting approval",
    "resolved": "🟢 resolved",
    "escalated": "🟣 escalated",
    "failed": "🔴 failed",
}
DETAIL_PAGE = "pages/2_Incident_detail.py"

st.title("Incidents")
status = st.selectbox("Status", ["all", *BADGE], index=0)


@st.fragment(run_every=5)
def incident_table() -> None:
    try:
        rows = api.incidents(None if status == "all" else status)
    except api.ApiError as exc:
        st.error(str(exc))
        return
    pending = sum(r["status"] == "awaiting_approval" for r in rows)
    if pending:
        st.warning(f"{pending} incident(s) waiting for approval: open the Approval inbox.")
    if not rows:
        st.info("No incidents yet.")
        return
    df = pd.DataFrame(rows)
    df["received_at"] = pd.to_datetime(df["received_at"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    df["status"] = df["status"].map(lambda s: BADGE.get(s, s))
    df["mttr_min"] = (df["mttr_s"] / 60).round(1)
    df["cost_eur"] = df["cost_eur"].round(4)
    cols = ["received_at", "status", "dag_id", "task_id", "month", "root_cause"]
    picked = st.dataframe(
        df[[*cols, "mttr_min", "cost_eur", "id"]],
        hide_index=True,
        width="stretch",
        on_select="rerun",
        selection_mode="single-row",
        key="incidents",
    )
    st.caption("Select a row to open the incident.")
    selected = picked.selection.rows if picked else []
    if selected:
        st.session_state["incident_id"] = df.iloc[selected[0]]["id"]
        st.switch_page(DETAIL_PAGE)


incident_table()
