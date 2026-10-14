"""Streamlit UI. Talks only to the API, never to the database.

Incidents, approval inbox, timeline and KPI pages are added in Phase 10.
"""

import os

import httpx
import streamlit as st

API_URL = os.environ.get("API_URL", "http://api:8000")

st.set_page_config(page_title="Pipeline Incident Agent", layout="wide")
st.title("Pipeline Incident Agent")

try:
    resp = httpx.get(f"{API_URL}/health", timeout=5)
    st.json(resp.json())
    if resp.status_code == 200:
        st.success("API healthy")
    else:
        st.warning("API degraded")
except httpx.HTTPError as exc:
    st.error(f"API unreachable: {exc}")
