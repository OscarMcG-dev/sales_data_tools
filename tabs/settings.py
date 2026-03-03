"""Settings tab: env status, DB stats, export."""
import streamlit as st
import pandas as pd
from io import StringIO

from lib.config import Settings
from lib.db import LeadDB
from lib.attio_client import export_attio_lookups
from lib.justcall_client import JustCallClient
from lib.cleaning import SCOPE_REVIEW_SYSTEM
import asyncio

EXPORT_ROW_LIMIT = 5000


def render(db: LeadDB, settings: Settings) -> None:
    st.header("Settings")
    st.markdown("API keys, database stats, connection tests, and data export.")

    with st.expander("Documentation", expanded=False):
        st.markdown(
            "Full reference in the repo: **docs/OVERVIEW.md** (architecture, integrations, technical details) "
            "and **docs/LEAD_PROCESSING_PROCESS.MD** (process and status reference)."
        )

    st.subheader("API configuration")
    st.caption("Keys are read from environment variables (e.g. `.env`).")
    config_rows = [
        ("DB_PATH", settings.db_path, "SQLite database path"),
        ("ATTIO_API_KEY", bool(settings.attio_api_key), "Attio CRM sync and dedup"),
        ("JUSTCALL_API_KEY", bool(settings.justcall_api_key), "JustCall campaigns"),
        ("JUSTCALL_API_SECRET", bool(settings.justcall_api_secret), "JustCall campaigns"),
        ("OPENROUTER_API_KEY", bool(settings.openrouter_api_key), "Enrichment (OpenRouter)"),
        ("MISTRAL_API_KEY", bool(settings.mistral_api_key), "Call transcripts"),
    ]
    for key, is_set, desc in config_rows:
        st.write(f"- **{key}**: {'✓ Set' if is_set else '— Not set'} — {desc}")

    st.subheader("LLM Config (read-only)")
    with st.expander("Model, temperature, and prompts", expanded=False):
        st.write("**Model:** ", settings.openrouter_model or "(default)")
        st.write("**Temperature:** ", getattr(settings, "llm_temperature", 0.0))
        st.caption("Scope review uses the prompt below; enrichment uses the extraction prompt. Override model via OPENROUTER_MODEL; enrichment prompt via data/state/prompts.json.")
        st.write("**Scope review system prompt:**")
        st.code(SCOPE_REVIEW_SYSTEM, language=None)
        try:
            from scraper.website_enricher import get_default_crawl_prompts
            prompts = get_default_crawl_prompts(settings)
            st.write("**Enrichment extraction prompt:**")
            st.code(prompts.get("extraction_system", ""), language=None)
        except Exception:
            st.caption("Enrichment prompt not loaded (website_enricher dependency).")
        st.caption("To change prompts: edit lib/cleaning.py (SCOPE_REVIEW_SYSTEM) or data/state/prompts.json (extraction_system).")

    st.subheader("Scraper & enrichment (optional)")
    with st.expander("Configurable limits", expanded=False):
        st.write(f"- **directory_delay**: {settings.directory_delay} s")
        st.write(f"- **directory_max_concurrent**: {settings.directory_max_concurrent}")
        st.write(f"- **max_concurrent_crawls**: {settings.max_concurrent_crawls}")
        st.write(f"- **page_timeout**: {settings.page_timeout} ms")
        st.write(f"- **max_crawl_subpages**: {settings.max_crawl_subpages}")
        st.caption("Override via environment variables.")

    st.subheader("Filters & push targets")
    with st.expander("Dedup and enrichment/cleaning config", expanded=False):
        try:
            from lib.filters_config import get_config_path, get_dedup_config, get_cleaning_config, get_enrichment_output_mapping
            st.caption(f"Config file: `{get_config_path()}`")
            dedup = get_dedup_config()
            st.write("**Dedup — company match rules:**")
            for r in dedup.get("company_rules") or []:
                st.code(f"lead[{r.get('lead_field')}] → Attio {r.get('attio_attribute')}", language=None)
            st.write("**Dedup — people match rules:**")
            for r in dedup.get("people_rules") or []:
                st.code(f"lead[{r.get('lead_field')}] → Attio {r.get('attio_attribute')}", language=None)
            cleaning = get_cleaning_config()
            st.write("**Keyword clean push target:**")
            st.json(cleaning.get("keyword_clean") or {})
            st.write("**LLM scope review push target:**")
            st.json(cleaning.get("llm_scope_review") or {})
            mapping = get_enrichment_output_mapping()
            st.write("**Enrichment → DB mapping:**")
            if mapping:
                st.json(mapping)
            else:
                st.caption("Using default mapping (see lib/enrich.py).")
        except Exception as e:
            st.caption(f"Config not loaded: {e}")

    st.subheader("Database")
    counts = db.count_by_status()
    total = sum(counts.values())
    st.metric("Total leads", total)
    for status, n in sorted(counts.items()):
        st.caption(f"{status}: {n}")
    by_source = db.count_by_source()
    for src, n in sorted(by_source.items()):
        st.caption(f"Source {src}: {n}")

    st.subheader("Purge / Reset leads")
    st.caption(
        "Remove leads from the SQLite pipeline (e.g. after testing). "
        "Campaign list memberships for those leads are removed. This cannot be undone."
    )
    st.error("**Destructive:** Purging is permanent. Type RESET below to confirm.")
    purge_scope = st.radio(
        "Purge scope",
        ["All leads", "By status", "By source"],
        key="purge_scope",
        horizontal=True,
    )
    purge_statuses: list[str] = []
    purge_source = None
    if purge_scope == "By status":
        status_options = list(counts.keys()) if counts else []
        purge_statuses = st.multiselect(
            "Statuses to purge",
            status_options,
            key="purge_statuses",
            help="Delete only leads with these statuses.",
        )
    elif purge_scope == "By source":
        source_options = list(by_source.keys()) if by_source else []
        purge_source = st.selectbox(
            "Source to purge",
            [None] + source_options,
            format_func=lambda x: "Select..." if x is None else x,
            key="purge_source",
        )

    confirm_text = st.text_input(
        "Type RESET to confirm",
        key="purge_confirm",
        placeholder="RESET",
        help="Required before purging. Case-insensitive.",
    )
    if st.button("Purge leads", type="primary", key="btn_purge"):
        if confirm_text.strip().upper() != "RESET":
            st.error("Type RESET (case-insensitive) to confirm.")
        else:
            if purge_scope == "All leads":
                deleted = db.purge_leads()
            elif purge_scope == "By status":
                deleted = sum(db.purge_leads(status=s) for s in purge_statuses) if purge_statuses else 0
            else:
                deleted = db.purge_leads(lead_source=purge_source) if purge_source else 0
            st.success(f"Purged {deleted} lead(s).")
            st.rerun()

    st.subheader("Custom Tags")
    st.caption("Manage custom tags used to label leads for Campaign Lists and tracking.")

    all_tags = db.get_all_tags()
    if all_tags:
        tag_rows = [{"Name": t["name"], "Leads": t["lead_count"], "Created": t["created_at"], "id": t["id"]} for t in all_tags]
        st.dataframe(
            [{"Name": r["Name"], "Leads": r["Leads"], "Created": r["Created"]} for r in tag_rows],
            width="stretch", hide_index=True,
        )
        tag_to_delete = st.selectbox(
            "Delete tag",
            options=["— select —"] + [t["name"] for t in all_tags],
            key="settings_delete_tag",
        )
        if tag_to_delete != "— select —" and st.button("Delete tag", key="btn_delete_tag"):
            tag_obj = next((t for t in all_tags if t["name"] == tag_to_delete), None)
            if tag_obj:
                db.delete_tag(tag_obj["id"])
                st.success(f"Deleted tag '{tag_to_delete}'.")
                st.rerun()
    else:
        st.caption("No custom tags yet. Tags are created from the Clean & Enrich tab.")

    new_tag = st.text_input("Create new tag", key="settings_new_tag", placeholder="e.g. q1-campaign")
    if st.button("Create tag", key="btn_create_tag"):
        if not new_tag.strip():
            st.warning("Enter a tag name.")
        else:
            try:
                db.create_tag(new_tag.strip())
                st.success(f"Created tag '{new_tag.strip()}'.")
                st.rerun()
            except Exception as e:
                st.error(f"Could not create tag: {e}")

    st.subheader("Connection tests")
    if st.button("Test Attio connection"):
        if settings.attio_api_key:
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                d, p = loop.run_until_complete(export_attio_lookups(settings.attio_api_key))
                st.success(f"Attio OK: {len(d)} domains, {len(p)} phones")
            except Exception as e:
                st.error(str(e))
        else:
            st.warning("ATTIO_API_KEY not set.")

    if st.button("Test JustCall connection"):
        jc = JustCallClient()
        if jc.is_configured():
            try:
                jc.list_campaigns()
                st.success("JustCall OK")
            except Exception as e:
                st.error(str(e))
        else:
            st.warning("JustCall not configured.")

    st.subheader("Export")
    st.caption(f"Export is limited to the first **{EXPORT_ROW_LIMIT}** rows.")
    leads = db.get_leads(limit=EXPORT_ROW_LIMIT)
    if leads:
        df = pd.DataFrame(leads)
        buf = StringIO()
        df.to_csv(buf, index=False)
        st.download_button(
            "Download leads as CSV",
            buf.getvalue(),
            file_name="leads_export.csv",
            mime="text/csv",
            key="dl_csv",
        )
    else:
        st.caption("No leads to export.")
