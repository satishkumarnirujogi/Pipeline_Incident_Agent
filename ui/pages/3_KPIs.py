"""KPIs: the headline metrics of the latest full-system evaluation, the per-fault table,
the ablation table and the trend across eval runs. Every number comes from the API, which
runs the same queries as the evaluation report (eval/queries.sql)."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ui import api_client as api

st.set_page_config(page_title="KPIs", layout="wide")
st.title("KPIs")

HEADLINE = [
    ("task_completion_rate", "Task completion", "{:.1%}"),
    ("root_cause_accuracy", "Root-cause accuracy", "{:.1%}"),
    ("tool_call_success_rate", "Tool-call success", "{:.1%}"),
    ("recovery_rate", "Recovery rate", "{:.1%}"),
    ("safety_violations", "Safety violations", "{:.0f}"),
    ("cost_per_completed_eur", "Cost per completed (EUR)", "{:.4f}"),
]


@st.cache_data(ttl=60, show_spinner="Loading metrics")
def load(eval_run_id: str | None) -> tuple[dict, list[dict], dict]:
    return api.metrics_latest(eval_run_id), api.metrics_runs(), api.metrics_ablations("test")


def _fmt(value: object, pattern: str) -> str:
    if value is None:
        return "—"
    return pattern.format(float(value))  # type: ignore[arg-type]


try:
    runs = api.metrics_runs()
except api.ApiError as exc:
    st.error(str(exc))
    st.stop()
if not runs:
    st.info("No evaluation run yet: run `make eval SPLIT=dev`.")
    st.stop()

choices = ["latest full-system run", *[r["eval_run_id"] for r in reversed(runs)]]
choice = st.selectbox("Evaluation run", choices)
try:
    latest, trend, ablations = load(None if choice == choices[0] else choice)
except api.ApiError as exc:
    st.error(str(exc))
    st.stop()

st.caption(
    f"`{latest['eval_run_id']}` · split {latest['split']} · config {latest['ablation']} · "
    f"chaos {'on' if latest['chaos'] else 'off'} · {latest['secondary']['scenarios']} scenarios "
    f"· git `{latest['git']}`"
)
cols = st.columns(len(HEADLINE))
for col, (key, label, pattern) in zip(cols, HEADLINE, strict=True):
    value = latest["headline"][key]
    col.metric(label, _fmt(value, pattern), help=f"target {latest['targets'].get(key)}")
if latest["headline"]["recovery_rate"] is None:
    st.caption("Recovery rate comes from chaos runs: see the ablation table below.")

sec = latest["secondary"]
st.markdown(
    f"Escalation precision **{sec['escalation_precision']}** · recall "
    f"**{sec['escalation_recall']}** · approval precision **{sec['approval_precision']}** · "
    f"MTTR median / p95 **{sec['mttr_median_min']} / {sec['mttr_p95_min']} min** · "
    f"mean steps **{sec['mean_steps']}**"
)

st.subheader("Per fault")
st.dataframe(pd.DataFrame(latest["per_fault"]), hide_index=True, width="stretch")

st.subheader("Ablations (test split)")
if ablations["rows"]:
    st.caption(
        "Several runs of a configuration show `mean (worst run)`. Recovery rate is measured "
        f"against the full system without chaos (`{ablations['baseline']}`)."
    )
    # cells mix numbers and "mean (worst run)" text: show them all as text
    table = pd.DataFrame(ablations["rows"]).fillna("—").astype(str)
    st.dataframe(table, hide_index=True, width="stretch")
else:
    st.info("No test-split runs yet.")

st.subheader("Trend across eval runs")
df = pd.DataFrame(trend)
df["started_at"] = pd.to_datetime(df["started_at"])
df["config"] = df["ablation"] + df["chaos"].map({True: " + chaos", False: ""})
for metric in ("task_completion_rate", "root_cause_accuracy", "cost_per_completed_eur"):
    df[metric] = df[metric].astype(float)
left, right = st.columns(2)
with left:
    st.markdown("**Task completion by run**")
    st.line_chart(df, x="started_at", y="task_completion_rate", color="config", height=260)
with right:
    st.markdown("**Cost per completed incident (EUR)**")
    st.line_chart(df, x="started_at", y="cost_per_completed_eur", color="config", height=260)
st.dataframe(
    df[["eval_run_id", "split", "config", "scenarios", "task_completion_rate"]],
    hide_index=True,
    width="stretch",
)
