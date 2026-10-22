"""Incident detail: the timeline of the run (supervisor decisions, tool calls, approvals),
the diagnosis with its evidence, the actions with their verification, the postmortem and
the link to the Langfuse trace. Refreshes every 5 seconds while the incident is open."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from ui import api_client as api

st.set_page_config(page_title="Incident detail", layout="wide")
st.title("Incident detail")

OPEN = {"received", "running", "awaiting_approval"}
ICON = {True: "✅", False: "❌", None: "•"}


def _pick() -> str | None:
    try:
        recent = api.incidents()
    except api.ApiError as exc:
        st.error(str(exc))
        return None
    if not recent:
        st.info("No incidents yet.")
        return None
    ids = [r["id"] for r in recent]
    wanted = st.query_params.get("id") or st.session_state.get("incident_id") or ids[0]
    labels = {
        r["id"]: f"{r['received_at'][:19]} · {r['dag_id']} · {r['status']} · {r['id'][:8]}"
        for r in recent
    }
    if wanted not in labels:
        labels[wanted] = wanted
        ids.insert(0, wanted)
    chosen = st.selectbox("Incident", ids, index=ids.index(wanted), format_func=lambda i: labels[i])
    st.session_state["incident_id"] = chosen
    st.query_params["id"] = chosen
    return chosen


def _detail(d: dict) -> str:
    keep = {k: v for k, v in (d or {}).items() if v not in (None, False, "", [], {})}
    return ", ".join(f"{k}={v}" for k, v in keep.items())


def _header(inc: dict, t: dict) -> None:
    a, b, c, e = st.columns(4)
    a.metric("Status", inc["status"])
    b.metric("Root cause", inc.get("root_cause") or "—")
    mttr = inc.get("mttr_s")
    c.metric("MTTR (min)", f"{mttr / 60:.1f}" if mttr else "—")
    cost = sum((ev["detail"].get("cost_eur") or 0) for ev in t["events"] if ev["kind"] == "llm")
    e.metric("LLM cost (EUR)", f"{cost:.4f}")
    st.caption(
        f"{inc['dag_id']}.{inc['task_id']} · month {inc.get('month')} · try "
        f"{inc['try_number']} · run `{inc['run_id']}`"
    )
    if inc.get("exception"):
        st.code(inc["exception"], language=None)
    if t.get("trace_url"):
        st.link_button("Open the Langfuse trace", t["trace_url"])


def _diagnosis(t: dict) -> None:
    dx = t.get("diagnosis")
    st.subheader("Diagnosis")
    if not dx:
        st.write("No diagnosis yet.")
        return
    st.markdown(
        f"**{dx['root_cause']}** · confidence {dx['confidence']} · recommendation "
        f"`{dx['recommendation']}` · failing component `{dx['failing_component']}`"
    )
    for item in dx.get("evidence", []):
        st.markdown(f"- {item}")


def _actions(t: dict) -> None:
    st.subheader("Actions")
    actions = t.get("actions") or []
    if not actions:
        st.write("No actions.")
        return
    for a in actions:
        verification = (a.get("verification") or {}).get("chain_state")
        st.markdown(
            f"{ICON[a.get('ok')]} `{a['tool']}` {a.get('approved') or ''}"
            + (f" · verification: **{verification}**" if verification else "")
        )
        st.json(a.get("args") or {}, expanded=False)


def _routing(t: dict) -> None:
    st.subheader("Supervisor decisions")
    routing = t.get("routing") or []
    if not routing:
        st.write("None recorded (single-agent run, or not started).")
        return
    st.dataframe(
        pd.DataFrame(routing)[["step", "rule", "next", "decided_by", "reason"]],
        hide_index=True,
        width="stretch",
    )


def _timeline(t: dict) -> None:
    st.subheader("Timeline")
    rows = [
        {
            "at": pd.to_datetime(ev["at"]).strftime("%H:%M:%S.%f")[:-3],
            "": ICON[ev["ok"]],
            "kind": ev["kind"],
            "who": ev["who"],
            "what": ev["what"],
            "detail": _detail(ev["detail"]),
        }
        for ev in t["events"]
    ]
    kinds = sorted({r["kind"] for r in rows})
    shown = st.multiselect("Show", kinds, default=kinds, key="timeline_kinds")
    st.dataframe(
        pd.DataFrame([r for r in rows if r["kind"] in shown]), hide_index=True, width="stretch"
    )


def _outcome(inc: dict) -> None:
    if inc.get("escalation"):
        e = inc["escalation"]
        st.subheader("Escalation")
        st.warning(f"**{e['suggested_owner']}**: {e['reason']}")
    if inc.get("postmortem"):
        st.subheader("Postmortem")
        with st.container(border=True):
            st.markdown(inc["postmortem"])


incident_id = _pick()


@st.fragment(run_every=5)
def detail() -> None:
    if not incident_id:
        return
    try:
        inc = api.incident(incident_id)
        t = api.timeline(incident_id)
    except api.ApiError as exc:
        st.error(str(exc))
        return
    _header(inc, t)
    if inc["status"] == "awaiting_approval":
        st.info("Waiting for a human decision.")
        st.page_link("pages/1_Approval_inbox.py", label="Open the Approval inbox", icon="📥")
    left, right = st.columns([3, 2])
    with left:
        _timeline(t)
        _routing(t)
    with right:
        _diagnosis(t)
        _actions(t)
    _outcome(inc)
    if inc["status"] not in OPEN:
        st.caption("Incident closed; the page no longer changes.")


detail()
