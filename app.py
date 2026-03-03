"""
Streamlit control panel for the lead pipeline.
"""
import streamlit as st

from lib.config import Settings
from lib.db import LeadDB

st.set_page_config(page_title="Lead Pipeline", layout="wide")

settings = Settings()

if settings.app_password:
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if not st.session_state.authenticated:
        pwd = st.text_input("Password", type="password")
        if pwd == settings.app_password:
            st.session_state.authenticated = True
            st.rerun()
        elif pwd:
            st.error("Incorrect password")
        st.stop()

db = LeadDB(settings.db_path)

with st.expander("What this app does", expanded=False):
    st.markdown(
        "This app is a **collection of tools** for ingesting, cleaning, enriching, and reviewing leads. "
        "Use whichever tools you need, in any order."
    )
    st.markdown("**Ingest** — Bring leads in from CSV or Attio lists.")
    st.markdown(
        "**Clean & Enrich** — Keyword clean, LLM scope review, URL enrichment, dedup against Attio, "
        "and review flagged or duplicate leads. Use any combination, in any order; many tools can also run on an uploaded CSV."
    )
    st.markdown(
        "**Campaign Lists** — Build named lists of leads, configure Attio field mapping, and sync updates to existing Attio records. "
        "Lists can be used when creating campaigns."
    )
    st.markdown(
        "**Campaigns** — Create JustCall dialer campaigns and add contacts (from a Campaign List or by filters). "
        "Optionally link campaigns to Attio."
    )
    st.markdown(
        "**Transcripts** — Transcribe JustCall recordings (Mistral), sync to Attio, link calls to People."
    )
    st.markdown("**Settings** — API config, database stats, export, purge.")
    with st.expander("Status reference (outcomes of using the tools)", expanded=False):
        st.caption(
            "**pending_review** — New or imported, not yet enriched. "
            "**enriched** — Enrichment done. "
            "**ready_for_attio** — Deduped, not a duplicate; can sync to Attio. "
            "**duplicate** — Matched an existing Attio company. "
            "**synced_to_attio** — Pushed to Attio; use for campaigns. "
            "**flagged_keyword** / **flagged_llm** — Flagged for review. "
            "**excluded** — Removed from the pipeline (keyword/LLM or manual)."
        )

st.divider()

tab_names = [
    "Ingest",
    "Clean & Enrich",
    "Campaign Lists",
    "Campaigns",
    "Transcripts",
    "Settings",
]
tabs = st.tabs(tab_names)

from tabs.ingest import render as render_ingest
from tabs.enrich import render as render_enrich
from tabs.campaign_lists import render as render_campaign_lists
from tabs.campaigns import render as render_campaigns
from tabs.transcripts import render as render_transcripts
from tabs.settings import render as render_settings

with tabs[0]:
    render_ingest(db, settings)
with tabs[1]:
    render_enrich(db, settings)
with tabs[2]:
    render_campaign_lists(db, settings)
with tabs[3]:
    render_campaigns(db, settings)
with tabs[4]:
    render_transcripts(db, settings)
with tabs[5]:
    render_settings(db, settings)
