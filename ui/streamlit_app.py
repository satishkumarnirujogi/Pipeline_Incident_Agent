"""Incidents: what is happening, refreshed every 5 seconds. Pages: approval inbox."""

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
    df["status"] = df["status"].map(lambda s: BADGE.get(s, s))
    df["mttr_min"] = (df["mttr_s"] / 60).round(1)
    st.dataframe(
        df[["received_at", "status", "dag_id", "task_id", "month", "root_cause", "mttr_min", "id"]],
        hide_index=True,
        use_container_width=True,
    )


incident_table()
