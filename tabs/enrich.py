"""Clean & Enrich tab: tools for processing leads from DB or arbitrary CSVs."""
import asyncio
import io
import json
import threading
import time
import streamlit as st
import pandas as pd

# Thread-safe progress store so worker threads never touch Streamlit (avoids ScriptRunContext warning)
_enrich_progress_lock = threading.Lock()
_enrich_progress = {"running": False, "current": 0, "total": 1, "message": "", "log": []}
_enrich_csv_progress = {"running": False, "results": None, "error": None}
_scrape_list_progress_lock = threading.Lock()
_scrape_list_progress = {"running": False, "current": 0, "total": 1, "message": "", "log": [], "results": None, "error": None}

from lib.config import Settings
from lib.cleaning import keyword_clean, llm_scope_review
from lib.db import LeadDB
from lib.enrich import run_enrichment
from lib.attio_client import export_attio_lookups, classify_leads

STATUSES_CLEANING = ["pending_review", "enriched", "ready_for_attio"]
MATCH_FIELDS = ["name", "description"]
FLAGGED_STATUSES = ["flagged_keyword", "flagged_llm"]

CSV_REQUIRED_FIELDS = ["name"]
CSV_ENRICHMENT_FIELDS = [
    "name", "domains", "website_url", "office_phone", "office_email",
    "description", "street_address", "segment",
]


def render(db: LeadDB, settings: Settings) -> None:
    st.header("Clean & Enrich")
    st.markdown(
        "Tools for processing leads. Each tool works independently — "
        "run whichever ones you need, in any order. Tools can process leads "
        "from the pipeline **or** from an uploaded CSV."
    )

    counts = db.count_by_status()
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.metric("Pending review", counts.get("pending_review", 0))
    with c2:
        st.metric("Enriched", counts.get("enriched", 0))
    with c3:
        st.metric("Ready for Attio", counts.get("ready_for_attio", 0))
    with c4:
        st.metric("Duplicates", counts.get("duplicate", 0))
    with c5:
        n_flagged = counts.get("flagged_keyword", 0) + counts.get("flagged_llm", 0)
        st.metric("Flagged", n_flagged)
    with c6:
        st.metric("Excluded", counts.get("excluded", 0))

    if not settings.openrouter_api_key:
        st.warning("Set **OPENROUTER_API_KEY** for LLM scope review and enrichment.")
    if not settings.attio_api_key:
        st.info("Set **ATTIO_API_KEY** for dedup against Attio.")

    tool = st.selectbox(
        "Select tool",
        [
            "Keyword Cleaning",
            "LLM Scope Review",
            "Enrichment (Crawl from URL)",
            "Scrape from URL list",
            "Dedup Against Attio",
            "Review Flagged Leads",
            "Review Duplicates",
        ],
        key="enrich_tool_select",
    )
    _TOOL_DESCRIPTIONS = {
        "Keyword Cleaning": "Flag or exclude leads by keywords in name/description (case-insensitive).",
        "LLM Scope Review": "LLM classifies each lead as in-scope, out-of-scope, or needs review.",
        "Enrichment (Crawl from URL)": "Crawl a website URL and extract structured data via LLM (pipeline or CSV).",
        "Scrape from URL list": "Scrape multiple URLs from a list and extract data.",
        "Dedup Against Attio": "Match leads against existing Attio companies by domain/phone; mark duplicate or ready_for_attio.",
        "Review Flagged Leads": "Review and clear or exclude leads flagged by keyword or LLM.",
        "Review Duplicates": "View leads marked as duplicate of an existing Attio record.",
    }
    st.caption(_TOOL_DESCRIPTIONS.get(tool, ""))

    st.divider()

    if tool == "Keyword Cleaning":
        _render_keyword_cleaning(db)
    elif tool == "LLM Scope Review":
        _render_llm_scope(db, settings)
    elif tool == "Enrichment (Crawl from URL)":
        _render_enrichment(db, settings)
    elif tool == "Scrape from URL list":
        _render_scrape_url_list(settings)
    elif tool == "Dedup Against Attio":
        _render_dedup(db, settings)
    elif tool == "Review Flagged Leads":
        _render_flagged(db)
    elif tool == "Review Duplicates":
        _render_duplicates(db)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Columns to include in flagged/excluded lead CSV exports
_LEAD_CSV_COLUMNS = [
    "id", "name", "domains", "status", "flag_reason", "flag_source",
    "office_phone", "office_email", "description", "lead_source",
    "primary_location_locality", "primary_location_region",
]


def _download_leads_csv(leads: list, file_name: str, button_label: str) -> None:
    """Render a download button for a CSV of lead rows (subset of columns)."""
    if not leads:
        return
    rows = []
    for r in leads:
        row = {c: r.get(c) for c in _LEAD_CSV_COLUMNS if c in (r or {})}
        rows.append(row)
    df = pd.DataFrame(rows)
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    st.download_button(
        button_label,
        csv_buf.getvalue(),
        file_name=file_name,
        mime="text/csv",
        key=f"download_{file_name.replace('.', '_')}",
    )


# ---------------------------------------------------------------------------
# CSV input helper
# ---------------------------------------------------------------------------

def _render_csv_input(tool_key: str) -> tuple:
    """Render CSV upload + column mapping UI. Returns (dataframe_or_none, column_mapping_dict)."""
    with st.expander("Use CSV input instead of pipeline leads", expanded=False):
        uploaded = st.file_uploader("Upload CSV", type=["csv"], key=f"{tool_key}_csv_upload")
        if uploaded is None:
            return None, {}
        try:
            if hasattr(uploaded, "seek"):
                uploaded.seek(0)
            df = pd.read_csv(uploaded, dtype=str, encoding="utf-8")
        except Exception:
            if hasattr(uploaded, "seek"):
                uploaded.seek(0)
            try:
                df = pd.read_csv(uploaded, dtype=str, encoding="latin-1")
            except Exception as e:
                st.error(f"Could not read CSV: {e}")
                return None, {}

        st.caption(f"CSV has **{len(df)}** rows × **{len(df.columns)}** columns")
        skip = "— skip —"
        csv_cols = [skip] + list(df.columns)
        mapping = {}

        st.markdown("Map CSV columns to required fields:")
        cols_ui = st.columns(min(len(CSV_ENRICHMENT_FIELDS), 4))
        for i, field in enumerate(CSV_ENRICHMENT_FIELDS):
            with cols_ui[i % len(cols_ui)]:
                best_guess = 0
                for j, col in enumerate(df.columns):
                    if col.strip().lower().replace(" ", "_") == field:
                        best_guess = j + 1
                        break
                sel = st.selectbox(
                    field, csv_cols, index=best_guess,
                    key=f"{tool_key}_csv_map_{field}",
                )
                if sel != skip:
                    mapping[field] = sel

        has_name = "name" in mapping
        if not has_name:
            st.warning("You must map at least the **name** column.")
            return None, {}

        st.caption("Preview (first 5 rows, mapped):")
        preview_rows = []
        for _, row in df.head(5).iterrows():
            preview_rows.append({field: row.get(csv_col, "") for field, csv_col in mapping.items()})
        st.dataframe(pd.DataFrame(preview_rows), width="stretch", hide_index=True)

        return df, mapping

    return None, {}


def _csv_to_lead_dicts(df: pd.DataFrame, mapping: dict) -> list[dict]:
    """Convert mapped CSV dataframe rows to lead-like dicts."""
    leads = []
    for _, row in df.iterrows():
        lead = {}
        for field, csv_col in mapping.items():
            val = row.get(csv_col, "")
            if pd.isna(val):
                val = ""
            lead[field] = str(val).strip()
        if not lead.get("name"):
            continue
        lead["id"] = len(leads) + 1
        leads.append(lead)
    return leads


def _render_output_options(tool_key: str, results: list[dict], db: LeadDB) -> None:
    """Show output options: download CSV or tag leads."""
    if not results:
        return
    st.divider()
    st.subheader("Output")
    out_mode = st.radio(
        "Output mode",
        ["Download CSV", "Tag leads in pipeline"],
        key=f"{tool_key}_output_mode",
        horizontal=True,
    )
    if out_mode == "Download CSV":
        csv_buf = io.StringIO()
        pd.DataFrame(results).to_csv(csv_buf, index=False)
        st.download_button(
            "Download results CSV",
            csv_buf.getvalue(),
            file_name=f"{tool_key}_results.csv",
            mime="text/csv",
            key=f"{tool_key}_download_csv",
        )
    else:
        tag_name = st.text_input(
            "Custom tag name",
            key=f"{tool_key}_tag_name",
            placeholder="e.g. enriched-batch-march",
        )
        if st.button("Create tag and apply to results", key=f"{tool_key}_apply_tag"):
            if not tag_name.strip():
                st.warning("Enter a tag name.")
            else:
                tag_id = db.get_or_create_tag(tag_name.strip())
                lead_ids = [r.get("id") or r.get("lead_id") for r in results if r.get("id") or r.get("lead_id")]
                lead_ids = [lid for lid in lead_ids if isinstance(lid, int)]
                if lead_ids:
                    added = db.tag_leads(lead_ids, tag_id)
                    st.success(f"Tagged {added} leads with '{tag_name.strip()}'")
                else:
                    st.info("No pipeline lead IDs in results (CSV-only results can only be downloaded).")


# ---------------------------------------------------------------------------
# Keyword Cleaning
# ---------------------------------------------------------------------------

def _render_keyword_cleaning(db: LeadDB) -> None:
    st.subheader("Keyword Cleaning")
    st.markdown(
        "Flag or exclude leads whose **name** or **description** contains "
        "any of the keywords below (case-insensitive substring match)."
    )

    csv_df, csv_mapping = _render_csv_input("kw_clean")
    using_csv = csv_df is not None and csv_mapping

    keywords_text = st.text_area(
        "Keywords (one per line)",
        height=100,
        placeholder="e.g. real estate\nbookkeeping only",
        key="clean_keywords",
    )

    col_left, col_right = st.columns(2)
    with col_left:
        match_fields = st.multiselect(
            "Match fields",
            MATCH_FIELDS,
            default=MATCH_FIELDS,
            key="clean_match_fields",
            help="Which lead fields to search for the keywords (name and/or description).",
        )
        if not using_csv:
            statuses_scan = st.multiselect(
                "Statuses to scan",
                STATUSES_CLEANING,
                default=["pending_review", "enriched"],
                key="clean_statuses",
                help="Only leads with these statuses will be checked for keyword matches.",
            )
            clean_lead_source = st.selectbox(
                "Filter by source (optional)",
                [None, "directory", "b2b_data", "attio_export", "attio_list", "outscraper"],
                format_func=lambda x: "All sources" if x is None else x,
                key="clean_lead_source",
            )
            clean_limit = st.number_input(
                "Max leads to scan",
                min_value=1,
                max_value=5000,
                value=500,
                key="clean_limit",
                help="Cap how many leads to check in this run.",
            )
    with col_right:
        action_clean = st.radio(
            "Action",
            ["Flag only", "Exclude"] if not using_csv else ["Flag only"],
            key="clean_action",
            help="Flag keeps leads visible for manual review. Exclude removes them from the pipeline.",
        )

    action_value = "flag" if action_clean == "Flag only" else "exclude"

    if using_csv:
        if st.button("Run keyword clean on CSV", key="btn_keyword_clean_csv"):
            keywords = [k.strip() for k in keywords_text.strip().splitlines() if k.strip()]
            if not keywords:
                st.warning("Enter at least one keyword.")
            else:
                leads = _csv_to_lead_dicts(csv_df, csv_mapping)
                matches = []
                for lead in leads:
                    for field in (match_fields or MATCH_FIELDS):
                        value = (lead.get(field) or "").lower()
                        for kw in keywords:
                            if kw and kw.lower() in value:
                                matches.append({
                                    **lead,
                                    "matched_keyword": kw,
                                    "matched_field": field,
                                })
                                break
                        else:
                            continue
                        break
                if matches:
                    st.dataframe(
                        pd.DataFrame(matches),
                        width="stretch", hide_index=True,
                    )
                    st.caption(f"{len(matches)} rows matched.")
                    _render_output_options("kw_csv", matches, db)
                else:
                    st.caption("0 rows matched.")
    else:
        if st.button("Run keyword clean", key="btn_keyword_clean"):
            keywords = [k.strip() for k in keywords_text.strip().splitlines() if k.strip()]
            if not keywords:
                st.warning("Enter at least one keyword.")
            else:
                with st.spinner("Running keyword clean..."):
                    matches = keyword_clean(
                        db,
                        keywords=keywords,
                        match_fields=match_fields or list(MATCH_FIELDS),
                        statuses=statuses_scan or ["pending_review", "enriched"],
                        action=action_value,
                        lead_source=clean_lead_source,
                        limit=clean_limit,
                    )
                if matches:
                    st.dataframe(
                        [{"lead_id": m["lead_id"], "name": m["name"], "matched_keyword": m["matched_keyword"], "matched_field": m["matched_field"]} for m in matches],
                        width="stretch",
                    )
                    unique_leads = len({m["lead_id"] for m in matches})
                    st.caption(f"{unique_leads} leads matched.")
                    _render_output_options("kw_db", matches, db)
                else:
                    st.caption("0 leads matched.")


# ---------------------------------------------------------------------------
# LLM Scope Review
# ---------------------------------------------------------------------------

def _render_llm_scope(db: LeadDB, settings: Settings) -> None:
    st.subheader("LLM Scope Review")
    st.markdown(
        "Sends each lead's name and description to an LLM that classifies it as "
        "**in_scope**, **out_of_scope**, or **needs_review** based on your target segment."
    )

    if not settings.openrouter_api_key:
        st.warning("Set **OPENROUTER_API_KEY** to run LLM scope review.")
        return

    csv_df, csv_mapping = _render_csv_input("scope")
    using_csv = csv_df is not None and csv_mapping

    st.caption(f"Model: **{settings.openrouter_model or 'default'}**")

    # Show which lead columns are sent to the LLM for scope (visible without going to settings)
    with st.expander("Input columns used for scope", expanded=True):
        try:
            from lib.filters_config import get_cleaning_config
            cfg = get_cleaning_config()
            scope_cfg = (cfg or {}).get("llm_scope_review") or {}
            input_fields = scope_cfg.get("input_fields") or ["name", "description"]
        except Exception:
            input_fields = ["name", "description"]
        st.markdown("The following lead attributes are sent to the LLM for classification:")
        for col in input_fields:
            st.code(col, language=None)
        st.caption("Leads are classified as **in_scope**, **out_of_scope**, or **needs_review** based on these fields.")

    # Show and allow editing the scope prompt in the tool
    with st.expander("Scope classification prompt", expanded=True):
        from lib.cleaning import SCOPE_REVIEW_SYSTEM
        st.text_area(
            "System prompt for scope review (edit to change how leads are classified)",
            value=SCOPE_REVIEW_SYSTEM,
            height=220,
            key="scope_prompt_ta",
            help="This prompt is sent to the LLM. It must ask for JSON with 'label' (in_scope | out_of_scope | needs_review) and 'reason'.",
        )
        st.caption("Changes here apply to the next run. Leave as-is to use the default prompt.")

    col_left, col_right = st.columns(2)
    with col_left:
        scope_limit = st.number_input(
            "Batch size",
            min_value=1,
            max_value=500,
            value=50,
            key="scope_limit",
        )
        if not using_csv:
            scope_statuses = st.multiselect(
                "Statuses to scan",
                STATUSES_CLEANING,
                default=["enriched"],
                key="scope_statuses",
            )
            scope_lead_source = st.selectbox(
                "Filter by source (optional)",
                [None, "directory", "b2b_data", "attio_export", "attio_list", "outscraper"],
                format_func=lambda x: "All sources" if x is None else x,
                key="scope_lead_source",
            )
    with col_right:
        scope_action_radio = st.radio(
            "Action",
            ["Flag for review", "Exclude"] if not using_csv else ["Flag for review"],
            key="scope_action",
        )

    scope_action_value = "flag" if scope_action_radio == "Flag for review" else "exclude"

    if using_csv:
        if st.button("Run scope review on CSV", key="btn_scope_review_csv"):
            leads = _csv_to_lead_dicts(csv_df, csv_mapping)[:scope_limit]
            if not leads:
                st.warning("No valid rows in CSV.")
                return
            from openai import OpenAI
            from lib.cleaning import SCOPE_REVIEW_SYSTEM
            client = OpenAI(
                api_key=settings.openrouter_api_key,
                base_url=settings.openrouter_base_url,
            )
            results = []
            progress = st.progress(0.0)
            for i, lead in enumerate(leads):
                progress.progress((i + 1) / len(leads))
                name = lead.get("name", "")
                desc = lead.get("description", "")
                text = f"Name: {name}\nDescription: {desc}" if desc else f"Name: {name}"
                try:
                    response = client.chat.completions.create(
                        model=settings.openrouter_model,
                        messages=[
                            {"role": "system", "content": SCOPE_REVIEW_SYSTEM},
                            {"role": "user", "content": text},
                        ],
                        temperature=settings.llm_temperature,
                        max_tokens=300,
                    )
                    content = (response.choices[0].message.content or "").strip()
                    if "```" in content:
                        content = content.split("```")[1]
                        if content.startswith("json"):
                            content = content[4:].strip()
                    obj = json.loads(content)
                    label = (obj.get("label") or "in_scope").strip().lower()
                    reason = (obj.get("reason") or "")[:500]
                except Exception:
                    label = "needs_review"
                    reason = "LLM error"
                results.append({**lead, "label": label, "reason": reason})
            progress.empty()
            if results:
                st.dataframe(pd.DataFrame(results), width="stretch", hide_index=True)
                _render_output_options("scope_csv", results, db)
    else:
        if st.button("Run scope review", disabled=st.session_state.get("scope_review_running", False), key="btn_scope_review"):
            st.session_state.scope_review_running = True
            st.session_state.scope_review_results = None
            st.session_state.scope_review_error = None

            def run_scope_review():
                try:
                    from lib.cleaning import SCOPE_REVIEW_SYSTEM as _DEFAULT_SCOPE
                    custom_prompt = (st.session_state.get("scope_prompt_ta") or "").strip()
                    if custom_prompt and custom_prompt != (_DEFAULT_SCOPE or "").strip():
                        scope_system_prompt_arg = custom_prompt
                    else:
                        scope_system_prompt_arg = None
                    res = llm_scope_review(
                        db,
                        statuses=scope_statuses or ["enriched"],
                        limit=scope_limit,
                        action=scope_action_value,
                        settings=settings,
                        lead_source=scope_lead_source,
                        scope_system_prompt=scope_system_prompt_arg,
                    )
                    st.session_state.scope_review_results = res
                except Exception as e:
                    st.session_state.scope_review_error = str(e)
                finally:
                    st.session_state.scope_review_running = False

            threading.Thread(target=run_scope_review, daemon=True).start()
            st.rerun()

        if st.session_state.get("scope_review_running"):
            st.info("Running scope review...")
        if st.session_state.get("scope_review_error"):
            st.warning(st.session_state.scope_review_error)
        if st.session_state.get("scope_review_results") is not None and not st.session_state.get("scope_review_running"):
            results = st.session_state.scope_review_results
            if results:
                st.dataframe(
                    [{"lead_id": r["lead_id"], "name": r["name"], "label": r["label"], "reason": r["reason"]} for r in results],
                    width="stretch",
                )
                _render_output_options("scope_db", results, db)
            else:
                st.caption("No leads processed or all in scope.")
            st.session_state.scope_review_results = None


# ---------------------------------------------------------------------------
# Enrichment (Crawl from URL)
# ---------------------------------------------------------------------------

def _render_enrichment(db: LeadDB, settings: Settings) -> None:
    st.subheader("Enrichment (Crawl from URL)")
    st.markdown(
        "Crawl a website URL and extract structured data via LLM. "
        "Can enrich pipeline leads or process an uploaded CSV with URL + name columns. "
        "Configure crawl behavior, prompts, and where extracted data lands."
    )

    if not settings.openrouter_api_key:
        st.info("Set **OPENROUTER_API_KEY** for LLM extraction.")

    csv_df, csv_mapping = _render_csv_input("enrich")
    using_csv = csv_df is not None and csv_mapping

    # Crawl configuration
    with st.expander("Crawl & extraction configuration", expanded=False):
        st.markdown("**Crawl settings**")
        cfg_col1, cfg_col2 = st.columns(2)
        with cfg_col1:
            use_web_plugin = st.checkbox(
                "Use web search plugin (Phase 2b)",
                value=getattr(settings, "web_search_enabled", False),
                key="enrich_web_plugin",
                help="Falls back to web search when crawl finds no decision makers.",
            )
            max_subpages = st.number_input(
                "Max sub-pages to crawl",
                min_value=0,
                max_value=10,
                value=getattr(settings, "max_crawl_subpages", 3),
                key="enrich_max_subpages",
            )
            llm_link_triage = st.checkbox(
                "Use LLM for link triage",
                value=getattr(settings, "llm_link_triage", True),
                key="enrich_llm_triage",
                help="Use LLM to pick which sub-pages to crawl (vs keyword matching).",
            )
        with cfg_col2:
            enrich_model = st.text_input(
                "LLM model",
                value=settings.openrouter_model or "",
                key="enrich_model",
            )
            enrich_temperature = st.slider(
                "LLM temperature",
                min_value=0.0, max_value=1.0,
                value=getattr(settings, "llm_temperature", 0.0),
                key="enrich_temperature",
            )

        st.markdown("**Extraction prompt** (system prompt for LLM)")
        from scraper.website_enricher import get_default_crawl_prompts
        defaults = get_default_crawl_prompts(settings)
        custom_prompt = st.text_area(
            "Extraction system prompt",
            value=defaults["extraction_system"],
            height=200,
            key="enrich_custom_prompt",
            help="Modify the system prompt to change what data is extracted.",
        )

        st.markdown("**Output field mapping** — which extracted fields to keep")
        from lib.models import LLMEnrichmentResponse
        enrichment_fields = list(LLMEnrichmentResponse.model_fields.keys())
        selected_fields = st.multiselect(
            "Fields to extract",
            enrichment_fields,
            default=enrichment_fields,
            key="enrich_selected_fields",
        )

    # Show which lead columns/attributes are written by enrichment (visible without opening settings)
    with st.expander("Enriched fields (columns written to leads)", expanded=True):
        try:
            from lib.filters_config import get_enrichment_output_mapping
            from lib.enrich import _default_lead_update_keys
            mapping = get_enrichment_output_mapping()
            if not mapping:
                mapping = [{"enrichment_field": ef, "db_column": dc} for ef, dc in _default_lead_update_keys()]
            st.caption("Extraction output is written to these lead attributes:")
            for m in mapping:
                st.code("%s → %s" % (m.get("enrichment_field", ""), m.get("db_column", "")), language=None)
        except Exception:
            st.caption("Enrichment writes: description, edited_description, office_phone, office_email, decision_makers, and related fields.")

    if not using_csv:
        counts = db.count_by_status()
        st.metric("Pending review", counts.get("pending_review", 0))

        col_left, col_right = st.columns(2)
        with col_left:
            lead_source = st.selectbox(
                "Filter by source",
                [None, "directory", "b2b_data", "attio_export", "attio_list", "outscraper"],
                format_func=lambda x: "All" if x is None else x,
                key="enrich_source",
            )
        with col_right:
            limit = st.number_input(
                "Max leads to process",
                min_value=1,
                max_value=200,
                value=20,
                key="enrich_limit",
            )

        # Preview list of websites that will be crawled; allow editing URLs before run
        leads_for_preview = db.get_leads(status="pending_review", lead_source=lead_source, limit=limit)
        preview_rows = [
            {
                "id": l["id"],
                "name": (l.get("name") or "").strip(),
                "website_url": (l.get("website_url") or l.get("domains") or "").strip(),
                "lead_source": (l.get("lead_source") or "").strip(),
            }
            for l in leads_for_preview
        ]
        with st.expander("Websites to crawl (view or edit URLs before running)", expanded=True):
            if not preview_rows:
                st.caption("No leads with status **pending_review** match the current filter. Add leads or change filter/source.")
            else:
                preview_key = "enrich_url_editor_%s_%s" % (str(lead_source), limit)
                df_preview = pd.DataFrame(preview_rows)
                edited = st.data_editor(
                    df_preview,
                    key=preview_key,
                    column_config={
                        "id": st.column_config.NumberColumn("ID", disabled=True),
                        "name": st.column_config.TextColumn("Name", disabled=True),
                        "website_url": st.column_config.TextColumn(
                            "URL (editable)",
                            help="Website to crawl. Edit if needed, then click Save below before Run enrichment.",
                        ),
                        "lead_source": st.column_config.TextColumn("Source", disabled=True),
                    },
                    hide_index=True,
                    num_rows="fixed",
                )
                if st.button("Save URL changes to pipeline", key="enrich_save_urls"):
                    for _, row in edited.iterrows():
                        lead_id = int(row["id"]) if row["id"] is not None else None
                        if lead_id is None:
                            continue
                        new_url = (row["website_url"] or "").strip() if pd.notna(row.get("website_url")) else ""
                        orig = next((r for r in preview_rows if r["id"] == lead_id), None)
                        if orig and (orig.get("website_url") or "") != new_url:
                            db.update_lead(lead_id, {"website_url": new_url if new_url else None})
                    st.success("URL changes saved. Run enrichment to use the updated URLs.")
                    if preview_key in st.session_state:
                        del st.session_state[preview_key]
                    st.rerun()

        if st.button("Run enrichment", disabled=st.session_state.get("enrich_running", False)):
            st.session_state.enrich_running = True
            with _enrich_progress_lock:
                _enrich_progress["running"] = True
                _enrich_progress["current"] = 0
                _enrich_progress["total"] = 1
                _enrich_progress["message"] = "Starting..."
                _enrich_progress["log"] = []

            override_settings = _build_override_settings(
                settings, use_web_plugin, max_subpages, llm_link_triage,
                enrich_model, enrich_temperature, custom_prompt,
            )

            def run():
                try:
                    def progress(current: int, total: int, name: str):
                        with _enrich_progress_lock:
                            _enrich_progress["current"] = current
                            _enrich_progress["total"] = total
                            _enrich_progress["message"] = name or ""

                    def log_cb(level: str, msg: str):
                        with _enrich_progress_lock:
                            _enrich_progress.setdefault("log", []).append((level, msg))

                    n = run_enrichment(
                        db,
                        override_settings,
                        lead_source=lead_source,
                        limit=limit,
                        progress_callback=progress,
                        log_callback=log_cb,
                    )
                    with _enrich_progress_lock:
                        _enrich_progress["message"] = f"Done. Enriched {n} leads."
                finally:
                    with _enrich_progress_lock:
                        _enrich_progress["running"] = False

            threading.Thread(target=run, daemon=True).start()
            st.rerun()

        # Read progress from thread-safe store (main thread only touches Streamlit)
        with _enrich_progress_lock:
            running = _enrich_progress["running"]
            current = _enrich_progress["current"]
            total = _enrich_progress["total"]
            message = _enrich_progress["message"]
        if not running and st.session_state.get("enrich_running"):
            st.session_state.enrich_running = False
            st.session_state.enrich_message = message
        if st.session_state.get("enrich_running"):
            pct = current / total if total else 0
            st.progress(min(1.0, pct))
            st.caption(message)
            # Auto-refresh so progress bar updates while the scraper runs
            time.sleep(1.5)
            st.rerun()
        elif st.session_state.get("enrich_message"):
            st.success(st.session_state.enrich_message)

        # Expandable run log (per-lead progress and skip reasons)
        with _enrich_progress_lock:
            log_lines = list(_enrich_progress.get("log", []))
        if log_lines:
            with st.expander("Run log (per-lead progress and skip reasons)", expanded=True):
                for level, msg in log_lines:
                    if level == "skip":
                        st.markdown("- ⏭️ `%s`" % msg.replace("|", "\\|"))
                    else:
                        st.markdown("- %s" % msg.replace("|", "\\|"))

        # Tag enriched leads
        if st.session_state.get("enrich_message", "").startswith("Done"):
            _render_post_enrichment_tagging(db, "enrich_db")
    else:
        st.markdown("**CSV enrichment** — crawl URLs from CSV and extract data")
        url_col = csv_mapping.get("website_url") or csv_mapping.get("domains")
        name_col = csv_mapping.get("name")
        if not url_col:
            st.warning("Map **website_url** or **domains** column so we know which URLs to crawl.")
            return
        if not name_col:
            st.warning("Map **name** column.")
            return

        limit = st.number_input("Max rows to process", min_value=1, max_value=200, value=20, key="enrich_csv_limit")

        if st.button("Run enrichment on CSV", key="btn_enrich_csv",
                      disabled=st.session_state.get("enrich_csv_running", False)):
            st.session_state.enrich_csv_running = True
            st.session_state.enrich_csv_results = None
            st.session_state.enrich_csv_error = None
            with _enrich_progress_lock:
                _enrich_csv_progress["running"] = True
                _enrich_csv_progress["results"] = None
                _enrich_csv_progress["error"] = None

            rows = []
            for _, row in csv_df.head(limit).iterrows():
                url = str(row.get(url_col, "")).strip()
                name = str(row.get(name_col, "")).strip()
                if url and name:
                    rows.append({"url": url, "name": name, "original_row": row.to_dict()})

            override_settings = _build_override_settings(
                settings, use_web_plugin, max_subpages, llm_link_triage,
                enrich_model, enrich_temperature, custom_prompt,
            )

            def run_csv_enrich():
                import asyncio as _asyncio
                from scraper.website_enricher import WebsiteEnricher

                results = []
                try:
                    loop = _asyncio.new_event_loop()
                    _asyncio.set_event_loop(loop)

                    async def _do():
                        enricher = WebsiteEnricher(override_settings)
                        try:
                            await enricher.start_pool(size=min(4, getattr(override_settings, "max_concurrent_crawls", 4)))
                        except Exception:
                            pass
                        for i, item in enumerate(rows):
                            url = item["url"]
                            if not url.startswith("http"):
                                url = "https://" + url
                            try:
                                enrichment = await enricher.enrich(url, item["name"])
                                result_row = {**item["original_row"]}
                                if enrichment:
                                    for field in selected_fields:
                                        val = getattr(enrichment, field, None)
                                        if val is not None:
                                            if isinstance(val, list):
                                                serialized = []
                                                for item_val in val:
                                                    if hasattr(item_val, "model_dump"):
                                                        serialized.append(item_val.model_dump())
                                                    else:
                                                        serialized.append(item_val)
                                                result_row[f"enriched_{field}"] = json.dumps(serialized)
                                            elif hasattr(val, "model_dump"):
                                                result_row[f"enriched_{field}"] = json.dumps(val.model_dump())
                                            else:
                                                result_row[f"enriched_{field}"] = str(val)
                                result_row["_enrichment_status"] = "success" if enrichment else "no_data"
                                results.append(result_row)
                            except Exception as e:
                                result_row = {**item["original_row"], "_enrichment_status": f"error: {e}"}
                                results.append(result_row)
                            await _asyncio.sleep(2.0)
                        try:
                            await enricher.stop_pool()
                        except Exception:
                            pass

                    loop.run_until_complete(_do())
                except Exception as e:
                    with _enrich_progress_lock:
                        _enrich_csv_progress["error"] = str(e)
                finally:
                    with _enrich_progress_lock:
                        _enrich_csv_progress["results"] = results
                        _enrich_csv_progress["running"] = False

            threading.Thread(target=run_csv_enrich, daemon=True).start()
            st.rerun()

        # Sync CSV progress from thread-safe store to session_state (main thread only)
        with _enrich_progress_lock:
            csv_running = _enrich_csv_progress["running"]
            csv_results = _enrich_csv_progress["results"]
            csv_error = _enrich_csv_progress["error"]
        if not csv_running and st.session_state.get("enrich_csv_running"):
            st.session_state.enrich_csv_running = False
            if csv_results is not None:
                st.session_state.enrich_csv_results = csv_results
            if csv_error:
                st.session_state.enrich_csv_error = csv_error
        if st.session_state.get("enrich_csv_running"):
            st.info("Enriching CSV rows...")
            time.sleep(1.5)
            st.rerun()
        if st.session_state.get("enrich_csv_results") is not None:
            results = st.session_state.enrich_csv_results
            if results:
                df_out = pd.DataFrame(results)
                st.dataframe(df_out, width="stretch", hide_index=True)
                csv_buf = io.StringIO()
                df_out.to_csv(csv_buf, index=False)
                st.download_button(
                    "Download enriched CSV",
                    csv_buf.getvalue(),
                    file_name="enriched_output.csv",
                    mime="text/csv",
                    key="enrich_csv_download",
                )
            else:
                st.caption("No results.")
            st.session_state.enrich_csv_results = None


# ---------------------------------------------------------------------------
# Scrape from URL list (arbitrary URLs: paste or .txt, configurable output fields)
# ---------------------------------------------------------------------------

def _parse_url_list_from_text(text: str) -> list[str]:
    """Parse one-URL-per-line from pasted text or file content. Skip empty and # lines."""
    urls = []
    for line in (text or "").splitlines():
        url = line.strip()
        if url and not url.startswith("#"):
            urls.append(url)
    return urls


def _render_scrape_url_list(settings: Settings) -> None:
    st.subheader("Scrape from URL list")
    st.markdown(
        "Crawl an arbitrary list of URLs and extract structured data using the same scraper as pipeline enrichment. "
        "Paste URLs below or upload a .txt file (one URL per line). Choose which fields to include in the output."
    )

    if not settings.openrouter_api_key:
        st.info("Set **OPENROUTER_API_KEY** for LLM extraction.")

    # Input: paste or file
    input_mode = st.radio("URL input", ["Paste URLs", "Upload .txt file"], key="scrape_list_input_mode", horizontal=True)
    urls_parsed: list[str] = []
    if input_mode == "Upload .txt file":
        uploaded = st.file_uploader("Upload .txt (one URL per line)", type=["txt"], key="scrape_list_txt_upload")
        if uploaded:
            try:
                if hasattr(uploaded, "seek"):
                    uploaded.seek(0)
                raw = uploaded.read().decode("utf-8", errors="replace")
                urls_parsed = _parse_url_list_from_text(raw)
            except Exception as e:
                st.error("Could not read file: %s" % e)
            st.caption("Lines starting with # and blank lines are ignored.")
    else:
        paste_area = st.text_area(
            "Paste URLs (one per line)",
            height=180,
            placeholder="https://example.com\nhttps://another-site.com",
            key="scrape_list_paste",
        )
        urls_parsed = _parse_url_list_from_text(paste_area or "")
        st.caption("Lines starting with # and blank lines are ignored.")

    # Which fields the scraper fills (from schema) and which to include in output
    from lib.models import EnrichmentData
    scraper_fields = list(EnrichmentData.model_fields.keys())
    with st.expander("Fields filled by the scraper (select which to include in output)", expanded=True):
        st.caption("The scraper extracts these attributes per URL according to its config. Select which to include in the results table and CSV.")
        selected_output_fields = st.multiselect(
            "Include these fields in output",
            scraper_fields,
            default=scraper_fields,
            key="scrape_list_fields",
        )

    # Crawl config (same as enrichment)
    with st.expander("Crawl & extraction config", expanded=False):
        from scraper.website_enricher import get_default_crawl_prompts
        defaults = get_default_crawl_prompts(settings)
        use_web_plugin = st.checkbox("Use web search plugin (Phase 2b)", value=getattr(settings, "web_search_enabled", False), key="scrape_web_plugin")
        max_subpages = st.number_input("Max sub-pages to crawl", min_value=0, max_value=10, value=getattr(settings, "max_crawl_subpages", 3), key="scrape_max_subpages")
        llm_link_triage = st.checkbox("Use LLM for link triage", value=getattr(settings, "llm_link_triage", True), key="scrape_llm_triage")
        enrich_model = st.text_input("LLM model", value=settings.openrouter_model or "", key="scrape_model")
        enrich_temperature = st.slider("LLM temperature", 0.0, 1.0, value=getattr(settings, "llm_temperature", 0.0), key="scrape_temp")
        custom_prompt = st.text_area("Extraction system prompt", value=defaults["extraction_system"], height=120, key="scrape_prompt")

    if not urls_parsed:
        st.warning("Enter or upload at least one URL.")
        return

    limit = st.number_input("Max URLs to process", min_value=1, max_value=200, value=min(50, len(urls_parsed)), key="scrape_list_limit")
    urls_to_process = urls_parsed[:limit]
    st.caption("Will process **%d** URL(s)." % len(urls_to_process))

    if st.button("Run scrape", key="btn_scrape_list", disabled=st.session_state.get("scrape_list_running", False)):
        st.session_state.scrape_list_running = True
        with _scrape_list_progress_lock:
            _scrape_list_progress["running"] = True
            _scrape_list_progress["current"] = 0
            _scrape_list_progress["total"] = len(urls_to_process)
            _scrape_list_progress["message"] = "Starting..."
            _scrape_list_progress["log"] = []
            _scrape_list_progress["results"] = None
            _scrape_list_progress["error"] = None

        override_settings = _build_override_settings(
            settings, use_web_plugin, max_subpages, llm_link_triage,
            enrich_model, enrich_temperature, custom_prompt,
        )

        def run_scrape_list():
            import asyncio as _asyncio
            from scraper.website_enricher import WebsiteEnricher

            results = []
            try:
                loop = _asyncio.new_event_loop()
                _asyncio.set_event_loop(loop)

                async def _do():
                    enricher = WebsiteEnricher(override_settings)
                    try:
                        await enricher.start_pool(size=min(4, getattr(override_settings, "max_concurrent_crawls", 4)))
                    except Exception:
                        pass
                    for i, url in enumerate(urls_to_process):
                        with _scrape_list_progress_lock:
                            _scrape_list_progress["current"] = i + 1
                            _scrape_list_progress["total"] = len(urls_to_process)
                            _scrape_list_progress["message"] = url[:60] + ("..." if len(url) > 60 else "")
                            _scrape_list_progress.setdefault("log", []).append(("info", "[%d/%d] %s" % (i + 1, len(urls_to_process), url)))
                        if not url.startswith("http"):
                            url = "https://" + url
                        firm_name = "Unknown"
                        try:
                            enrichment = await enricher.enrich(url, firm_name)
                            row = {"url": url, "_status": "success" if enrichment else "no_data"}
                            if enrichment:
                                for field in selected_output_fields:
                                    val = getattr(enrichment, field, None)
                                    if val is not None:
                                        if isinstance(val, list):
                                            row[field] = json.dumps([x.model_dump() if hasattr(x, "model_dump") else x for x in val])
                                        elif hasattr(val, "model_dump"):
                                            row[field] = json.dumps(val.model_dump())
                                        else:
                                            row[field] = str(val)
                                    else:
                                        row[field] = ""
                            else:
                                for field in selected_output_fields:
                                    row[field] = ""
                            results.append(row)
                        except Exception as e:
                            with _scrape_list_progress_lock:
                                _scrape_list_progress.setdefault("log", []).append(("info", "  -> error: %s" % e))
                            results.append({"url": url, "_status": "error", "error": str(e), **{f: "" for f in selected_output_fields}})
                        await _asyncio.sleep(2.0)
                    try:
                        await enricher.stop_pool()
                    except Exception:
                        pass

                loop.run_until_complete(_do())
            except Exception as e:
                with _scrape_list_progress_lock:
                    _scrape_list_progress["error"] = str(e)
            finally:
                with _scrape_list_progress_lock:
                    _scrape_list_progress["results"] = results
                    _scrape_list_progress["running"] = False

        threading.Thread(target=run_scrape_list, daemon=True).start()
        st.rerun()

    # Progress and results
    with _scrape_list_progress_lock:
        sl_running = _scrape_list_progress["running"]
        sl_current = _scrape_list_progress["current"]
        sl_total = _scrape_list_progress["total"]
        sl_message = _scrape_list_progress["message"]
        sl_log = list(_scrape_list_progress.get("log", []))
        sl_results = _scrape_list_progress.get("results")
        sl_error = _scrape_list_progress.get("error")

    if st.session_state.get("scrape_list_running") and sl_running:
        st.progress(min(1.0, sl_current / sl_total) if sl_total else 0)
        st.caption(sl_message)
        time.sleep(1.5)
        st.rerun()
    if not sl_running and st.session_state.get("scrape_list_running"):
        st.session_state.scrape_list_running = False

    if sl_error:
        st.warning("Error: %s" % sl_error)
    if sl_results is not None:
        if sl_log:
            with st.expander("Run log", expanded=False):
                for level, msg in sl_log:
                    st.markdown("- %s" % msg.replace("|", "\\|"))
        if not sl_results:
            st.caption("No results (list was empty or all URLs failed before producing output).")
        else:
            # Build display table: url, status, then all result keys (order preserved from result row)
            all_keys = list(sl_results[0].keys())
            cols = [k for k in ["url", "_status"] if k in all_keys] + [k for k in all_keys if k not in ("url", "_status")]
            rows = []
            for r in sl_results:
                rows.append({c: r.get(c, "") for c in cols})
            df_out = pd.DataFrame(rows)
            df_out = df_out.rename(columns={"_status": "status"})
            st.dataframe(df_out, width="stretch", hide_index=True)
            csv_buf = io.StringIO()
            df_out.to_csv(csv_buf, index=False)
            st.download_button("Download results CSV", csv_buf.getvalue(), file_name="scrape_url_list_results.csv", mime="text/csv", key="scrape_list_download")


def _build_override_settings(
    base: Settings, web_plugin: bool, max_subpages: int,
    llm_triage: bool, model: str, temperature: float, prompt: str,
) -> Settings:
    """Build a settings copy with user overrides for this enrichment session."""
    import os
    from pathlib import Path

    override = base.model_copy()
    override.web_search_enabled = web_plugin
    override.max_crawl_subpages = max_subpages
    override.llm_link_triage = llm_triage
    if model:
        override.openrouter_model = model
    override.llm_temperature = temperature

    if prompt and prompt.strip():
        data_dir = Path(os.environ.get("DATA_DIR", "data"))
        prompts_path = data_dir / "state" / "prompts.json"
        prompts_path.parent.mkdir(parents=True, exist_ok=True)
        existing = {}
        if prompts_path.is_file():
            try:
                existing = json.loads(prompts_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        from scraper.website_enricher import get_default_crawl_prompts
        defaults = get_default_crawl_prompts(base)
        if prompt.strip() != defaults["extraction_system"].strip():
            existing["extraction_system"] = prompt
            prompts_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")

    return override


def _render_post_enrichment_tagging(db: LeadDB, key_prefix: str) -> None:
    """After enrichment, offer to tag the enriched leads."""
    with st.expander("Tag enriched leads", expanded=False):
        tag_name = st.text_input(
            "Custom tag for this enrichment batch",
            key=f"{key_prefix}_enrich_tag",
            placeholder="e.g. enriched-march-2026",
        )
        if st.button("Apply tag to recently enriched", key=f"{key_prefix}_apply_enrich_tag"):
            if not tag_name.strip():
                st.warning("Enter a tag name.")
            else:
                enriched = db.get_leads(status="enriched", limit=5000)
                if enriched:
                    tag_id = db.get_or_create_tag(tag_name.strip())
                    lead_ids = [l["id"] for l in enriched]
                    added = db.tag_leads(lead_ids, tag_id)
                    st.success(f"Tagged {added} enriched leads with '{tag_name.strip()}'")
                else:
                    st.info("No enriched leads to tag.")


# ---------------------------------------------------------------------------
# Dedup
# ---------------------------------------------------------------------------

def _render_dedup(db: LeadDB, settings: Settings) -> None:
    st.subheader("Dedup Against Attio")
    st.markdown(
        "Compares **enriched** leads against your existing Attio companies by "
        "**domain** then **phone number**. Matches are marked as "
        "`duplicate`; non-matches become `ready_for_attio`."
    )

    if not settings.attio_api_key:
        st.warning("Set **ATTIO_API_KEY** to run dedup.")
        return

    counts = db.count_by_status()
    dc1, dc2, dc3 = st.columns(3)
    with dc1:
        st.metric("Enriched", counts.get("enriched", 0))
    with dc2:
        st.metric("Ready for Attio", counts.get("ready_for_attio", 0))
    with dc3:
        st.metric("Duplicates", counts.get("duplicate", 0))

    with st.expander("Current dedup config (match rules)"):
        try:
            from lib.filters_config import get_dedup_config
            dedup = get_dedup_config()
            st.write("**Company:** ", ", ".join(f"{r.get('lead_field')} -> {r.get('attio_attribute')}" for r in (dedup.get("company_rules") or [])))
            st.write("**People:** ", ", ".join(f"{r.get('lead_field')} -> {r.get('attio_attribute')}" for r in (dedup.get("people_rules") or [])))
            st.caption("Edit in data/config/filters_config.json or Settings tab.")
        except Exception:
            st.caption("Using default: domains, office_phone.")

    _CACHE_TTL_SEC = 600

    col_left, col_right = st.columns(2)
    with col_left:
        if st.button("Run dedup",
                      disabled=st.session_state.get("dedup_running", False),
                      key="btn_dedup"):
            st.session_state.dedup_running = True
            st.session_state.dedup_result = None

            def run():
                try:
                    now = time.time()
                    cache = st.session_state.get("attio_lookup_cache")
                    cache_ts = st.session_state.get("attio_lookup_cache_ts", 0)
                    if cache and (now - cache_ts) < _CACHE_TTL_SEC:
                        domain_lookup, phone_lookup = cache
                    else:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        domain_lookup, phone_lookup = loop.run_until_complete(
                            export_attio_lookups(settings.attio_api_key)
                        )
                        st.session_state.attio_lookup_cache = (domain_lookup, phone_lookup)
                        st.session_state.attio_lookup_cache_ts = now
                    new_count, existing_count = classify_leads(
                        db, domain_lookup, phone_lookup,
                        api_key=settings.attio_api_key or None,
                    )
                    st.session_state.dedup_result = (new_count, existing_count)
                except Exception as e:
                    st.session_state.dedup_result = ("error", str(e))
                finally:
                    st.session_state.dedup_running = False

            threading.Thread(target=run, daemon=True).start()
            st.rerun()
    with col_right:
        if st.button("Refresh Attio cache", key="btn_refresh_attio_cache",
                      help="Clear cached Attio export so next dedup fetches fresh data."):
            st.session_state.pop("attio_lookup_cache", None)
            st.session_state.pop("attio_lookup_cache_ts", None)
            st.success("Cache cleared.")

    if st.session_state.get("dedup_running"):
        st.info("Running dedup...")
    result = st.session_state.get("dedup_result")
    if result:
        if result[0] == "error":
            st.error(result[1])
        else:
            st.success(f"New: {result[0]}, Existing (duplicate): {result[1]}")


# ---------------------------------------------------------------------------
# Flagged / Duplicates
# ---------------------------------------------------------------------------

def _render_flagged(db: LeadDB) -> None:
    st.subheader("Review Flagged Leads")
    st.markdown(
        "Leads flagged by keyword cleaning or LLM scope review. "
        "Exclude them or clear flags to return them to the pipeline."
    )
    counts = db.count_by_status()
    n_flagged_keyword = counts.get("flagged_keyword", 0)
    n_flagged_llm = counts.get("flagged_llm", 0)

    fc1, fc2 = st.columns(2)
    with fc1:
        st.metric("Flagged (keyword)", n_flagged_keyword)
    with fc2:
        st.metric("Flagged (LLM)", n_flagged_llm)

    flagged = db.get_leads_by_statuses(FLAGGED_STATUSES, limit=1000)
    if flagged:
        st.dataframe(
            [{"id": r["id"], "name": r["name"], "status": r["status"],
              "flag_reason": r.get("flag_reason"), "flag_source": r.get("flag_source")}
             for r in flagged],
            width="stretch",
        )
        flagged_ids = [r["id"] for r in flagged]
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Exclude all flagged", key="btn_exclude_flagged"):
                db.bulk_update_status(flagged_ids, "excluded")
                st.success("Flagged leads excluded.")
                st.rerun()
        with col2:
            if st.button("Clear all flags (return to enriched)", key="btn_clear_flags"):
                db.bulk_update_status(flagged_ids, "enriched", {"flag_reason": None, "flag_source": None})
                st.success("Flags cleared; leads set to enriched.")
                st.rerun()
        with col3:
            _download_leads_csv(flagged, "flagged_leads.csv", "Download flagged leads CSV")
    else:
        st.caption("No flagged leads.")

    st.subheader("Excluded from pipeline")
    st.markdown("Leads excluded from the pipeline (by keyword/LLM or manually).")
    excluded = db.get_leads(status="excluded", limit=5000)
    if excluded:
        st.dataframe(
            [{"id": r["id"], "name": r["name"], "flag_reason": r.get("flag_reason"), "flag_source": r.get("flag_source")}
             for r in excluded[:500]],
            width="stretch",
        )
        if len(excluded) > 500:
            st.caption(f"Showing first 500 of {len(excluded)} excluded leads.")
        _download_leads_csv(excluded, "excluded_leads.csv", "Download excluded leads CSV")
    else:
        st.caption("No excluded leads.")


def _render_duplicates(db: LeadDB) -> None:
    st.subheader("Review Duplicates")
    st.markdown(
        "Leads identified as duplicates of existing Attio records during dedup."
    )
    dupes = db.get_leads(status="duplicate", limit=100)
    if dupes:
        st.dataframe(
            [{"id": r["id"], "name": r["name"], "domains": r.get("domains", ""),
              "attio_record_id": r.get("attio_record_id", ""), "duplicate_of": r.get("duplicate_of", "")}
             for r in dupes],
            width="stretch",
        )
        st.caption(f"{len(dupes)} duplicate(s) shown (max 100).")
    else:
        st.caption("No duplicates yet.")
