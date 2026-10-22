"""Demo control (dev only): pick a scenario, inject its fault, then follow the incident on
the Incidents page and decide in the Approval inbox. Revert puts the month back clean.

Never use it while an evaluation is running: both work on the same warehouse.
"""

from __future__ import annotations

import streamlit as st

from ui import api_client as api

st.set_page_config(page_title="Demo", layout="wide")
st.title("Demo control")

try:
    scenarios = api.demo_scenarios()
except api.ApiError as exc:
    st.info(f"The demo is off: {exc}")
    st.stop()

by_id = {s["id"]: s for s in scenarios}
default = list(by_id).index("F04-v1") if "F04-v1" in by_id else 0
chosen = st.selectbox(
    "Scenario",
    list(by_id),
    index=default,
    format_func=lambda i: f"{i} · {by_id[i]['fault']} · {by_id[i]['month']}",
)
sc = by_id[chosen]
st.markdown(
    f"{sc['description']}  \nTriggers `{sc['dag_id']}` for {sc['month']} · expected outcome: "
    f"**{sc['expected']}**"
)
st.warning("Do not run a demo while an evaluation is running: they share the warehouse.")

inject, revert = st.columns(2)
for col, action, label, kind in (
    (inject, "inject", "Inject fault", "primary"),
    (revert, "revert", "Revert to clean", "secondary"),
):
    if col.button(label, type=kind, width="stretch"):  # type: ignore[arg-type]
        try:
            api.demo(action, chosen)
            st.toast(f"{label}: started")
        except api.ApiError as exc:
            st.error(str(exc))


@st.fragment(run_every=3)
def status() -> None:
    try:
        s = api.demo_status()
    except api.ApiError as exc:
        st.error(str(exc))
        return
    state = s.get("state", "idle")
    if state == "idle":
        st.caption("No demo action yet.")
        return
    text = f"**{s.get('action')} {s.get('scenario_id')}**: {s.get('message')}"
    {"running": st.info, "done": st.success, "failed": st.error}.get(state, st.write)(text)
    if state == "done" and s.get("action") == "inject":
        st.page_link("streamlit_app.py", label="Follow it on the Incidents page", icon="📋")
        st.page_link("pages/1_Approval_inbox.py", label="Approval inbox", icon="📥")


status()
