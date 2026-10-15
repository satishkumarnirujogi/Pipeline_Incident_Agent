"""Approval inbox: one card per pending tier-2 action. Approve, edit or reject (with a reason).

Each card shows what a human needs to decide fast: the action and arguments, the agent's
rationale, the diagnosis and its evidence, the dry-run preview and the rollback path.
"""

from __future__ import annotations

import json

import streamlit as st

from ui import api_client as api

st.set_page_config(page_title="Approval inbox", layout="wide")
st.title("Approval inbox")


def card(a: dict) -> None:
    diagnosis = a.get("diagnosis") or {}
    preview = (a.get("preview") or {}).get("data") or a.get("preview") or {}
    with st.container(border=True):
        st.subheader(f"{a['tool']}")
        st.caption(f"incident {a['incident_id']} · requested {a['requested_at']}")
        left, right = st.columns(2)
        with left:
            st.markdown("**Arguments**")
            st.json(a["args"])
            st.markdown("**Rationale**")
            st.write(a.get("rationale") or "—")
            st.markdown("**Rollback**")
            st.write(a.get("rollback") or "—")
        with right:
            st.markdown(f"**Diagnosis:** `{diagnosis.get('root_cause')}` "
                        f"(confidence {diagnosis.get('confidence')})")
            for item in diagnosis.get("evidence", []):
                st.markdown(f"- {item}")
            st.markdown("**Dry-run preview**")
            if isinstance(preview, dict) and preview.get("effect"):
                st.info(preview["effect"])
            st.json(preview, expanded=False)

        approve_col, edit_col, reject_col = st.columns(3)
        if approve_col.button("Approve", key=f"approve-{a['id']}", type="primary"):
            _submit(a["id"], "approve")
        with edit_col.popover("Edit"):
            edited = st.text_area("Arguments (JSON)", json.dumps(a["args"], indent=2),
                                  key=f"args-{a['id']}", height=200)
            if st.button("Approve edited", key=f"edit-{a['id']}"):
                try:
                    _submit(a["id"], "edit", args=json.loads(edited))
                except json.JSONDecodeError as exc:
                    st.error(f"not valid JSON: {exc}")
        with reject_col.popover("Reject"):
            comment = st.text_input("What is wrong? (required)", key=f"comment-{a['id']}")
            if st.button("Reject", key=f"reject-{a['id']}", disabled=not comment.strip()):
                _submit(a["id"], "reject", comment=comment)


def _submit(approval_id: str, decision: str, args: dict | None = None,
            comment: str | None = None) -> None:
    try:
        api.decide(approval_id, decision, args=args, comment=comment)
    except api.ApiError as exc:
        st.error(str(exc))  # e.g. an edit with invalid arguments, explained by the API
        return
    st.success(f"{decision} recorded; the agent resumes in the background")
    st.rerun()


@st.fragment(run_every=5)
def inbox() -> None:
    try:
        pending = api.approvals("pending")
    except api.ApiError as exc:
        st.error(str(exc))
        return
    if not pending:
        st.info("Nothing waiting for approval.")
        return
    for a in pending:
        card(a)


inbox()
