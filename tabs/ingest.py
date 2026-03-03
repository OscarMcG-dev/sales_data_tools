"""Ingest tab: upload CSV, import from Attio list."""
import streamlit as st
import pandas as pd

from lib.config import Settings
from lib.db import LeadDB
from lib.csv_import import import_csv, preview_mapped_rows, DEFAULT_MAPS
from lib.attio_client import list_attio_lists, ingest_attio_list_into_db

# Target lead fields for custom CSV mapping, grouped
CUSTOM_REQUIRED_FIELDS = ["name", "domains", "office_phone", "office_email"]
CUSTOM_RECOMMENDED_FIELDS = ["description", "street_address", "segment"]
CUSTOM_OPTIONAL_FIELDS = [
    "linkedin", "facebook", "website_url", "listing_url",
    "dm_first_name", "dm_last_name", "dm_title",
]


def _csv_headers(uploaded_file) -> list[str]:
    """Read CSV column headers from uploaded file."""
    if hasattr(uploaded_file, "seek"):
        uploaded_file.seek(0)
    try:
        df = pd.read_csv(uploaded_file, nrows=0, encoding="utf-8")
    except Exception:
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(0)
        df = pd.read_csv(uploaded_file, nrows=0, encoding="latin-1")
    return df.columns.tolist()


def _auto_detect_mapping(headers: list[str], default_map: dict) -> dict[str, str]:
    """Return lead_field -> csv_column for preset; match by lowercase."""
    out = {}
    header_lower = {str(h).strip().lower(): h for h in headers}
    for csv_key, lead_field in default_map.items():
        if csv_key in header_lower:
            out[lead_field] = header_lower[csv_key]
    return out


def render(db: LeadDB, settings: Settings) -> None:
    st.header("Ingest")
    st.markdown(
        "Bring leads in from an Attio list or CSV file. "
        "Imported leads start as `pending_review`; use the Clean & Enrich tools to process them as needed."
    )
    counts = db.count_by_status()
    total = sum(counts.values())
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Pending review", counts.get("pending_review", 0))
    with col2:
        st.metric("Total leads", total)

    st.subheader("Import from Attio list")
    st.caption(
        "Pull a Company or People list from Attio into the pipeline as leads. "
        "Records already in the DB (by Attio record ID) are skipped."
    )
    if not settings.attio_api_key:
        st.warning("Set ATTIO_API_KEY to import from Attio lists.")
    else:
        try:
            attio_lists = list_attio_lists(settings.attio_api_key, include_counts=False)
        except Exception as e:
            st.caption(f"Could not load Attio lists: {e}")
            attio_lists = []
        allowed_objects = ("companies", "people")
        choices = [l for l in attio_lists if l.get("parent_object") in allowed_objects]
        if not choices:
            st.caption("No Company or People lists found in Attio (or API error).")
        else:
            def _format_list_label(c: dict) -> str:
                return f"{c['name']} ({c['parent_object']})"

            list_options = {_format_list_label(c): c["list_id"] for c in choices}
            selected_label = st.selectbox(
                "Attio list",
                options=list(list_options.keys()),
                key="ingest_attio_list",
            )

            selected_list_id = list_options.get(selected_label)
            selected_info = next((c for c in choices if c["list_id"] == selected_list_id), None) if selected_list_id else None

            # Show entry count for the selected list
            if selected_list_id:
                cache_key = f"attio_list_count_{selected_list_id}"
                if cache_key not in st.session_state:
                    try:
                        from lib.attio_client import get_attio_list_record_ids
                        record_ids = get_attio_list_record_ids(settings.attio_api_key, selected_list_id, limit=10000)
                        st.session_state[cache_key] = len(record_ids)
                    except Exception:
                        st.session_state[cache_key] = "?"
                entry_count = st.session_state[cache_key]
                st.caption(f"This list has **{entry_count}** records in Attio.")

            ingest_limit = st.number_input(
                "Max records to import",
                min_value=1,
                max_value=2000,
                value=500,
                key="ingest_attio_limit",
            )

            if st.button("Import list into pipeline", key="ingest_attio_btn"):
                if selected_list_id:
                    progress_bar = st.progress(0.0, text="Fetching list entries...")
                    status_msg = st.empty()
                    try:
                        def _progress(processed: int, total: int, message: str) -> None:
                            if total:
                                progress_bar.progress(min(1.0, processed / total), text=message)
                            status_msg.caption(message)
                        inserted, skipped, total_entries = ingest_attio_list_into_db(
                            settings.attio_api_key,
                            selected_list_id,
                            db,
                            list_info=selected_info,
                            limit=ingest_limit,
                            progress_callback=_progress,
                        )
                        progress_bar.empty()
                        status_msg.empty()
                        if total_entries == 0:
                            st.warning(
                                "This list has no entries (or the API returned none). "
                                "Check the list in Attio and that your token has list_entry:read scope."
                            )
                        else:
                            st.success(
                                f"Imported {inserted} leads, skipped {skipped} already present "
                                f"(list had {total_entries} entries)."
                            )
                            # Show imported data at a glance
                            imported = db.get_leads(
                                status="pending_review",
                                lead_source="attio_list",
                                limit=inserted or 50,
                            )
                            if imported:
                                with st.expander(f"Imported data preview ({len(imported)} leads)", expanded=True):
                                    preview_cols = [
                                        "id", "name", "domains", "office_phone", "office_email",
                                        "segment", "description", "primary_location_locality",
                                        "primary_location_region", "attio_record_id",
                                    ]
                                    df = pd.DataFrame(imported)
                                    show_cols = [c for c in preview_cols if c in df.columns]
                                    st.dataframe(df[show_cols], width="stretch", hide_index=True)
                    except Exception as e:
                        progress_bar.empty()
                        status_msg.empty()
                        st.error(str(e))

            # Always show existing attio_list leads for cross-reference
            existing_attio_leads = db.get_leads(lead_source="attio_list", limit=200)
            if existing_attio_leads:
                with st.expander(f"Existing Attio list leads in pipeline ({len(existing_attio_leads)})", expanded=False):
                    preview_cols = [
                        "id", "name", "domains", "office_phone", "office_email",
                        "segment", "status", "description", "attio_record_id",
                    ]
                    df = pd.DataFrame(existing_attio_leads)
                    show_cols = [c for c in preview_cols if c in df.columns]
                    st.dataframe(df[show_cols], width="stretch", hide_index=True)

    st.subheader("Upload CSV")
    lead_source = st.selectbox(
        "Lead source",
        ["directory", "b2b_data", "attio_export", "outscraper", "custom"],
        key="ingest_lead_source",
        help=(
            "**directory** / **b2b_data**: Company, website, phone, email, etc. "
            "**attio_export**: Attio export columns (name, company, domains, phone, email). "
            "**outscraper**: Outscraper-style (name, site, phone, email, full_address). "
            "**custom**: Any CSV — you choose which columns map to which lead fields."
        ),
    )
    uploaded = st.file_uploader("CSV file", type=["csv"], key="ingest_csv")

    if uploaded is not None:
        try:
            csv_headers_list = _csv_headers(uploaded)
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            csv_headers_list = []
        skip_option = "— skip —"
        column_options = [skip_option] + list(csv_headers_list)

        column_mapping: dict[str, str] = {}

        if lead_source == "custom":
            # Guided mapping: Required, Recommended, Optional
            st.caption("Map each lead field to a CSV column (or skip).")
            with st.expander("Column mapping", expanded=True):
                st.markdown("**Required** (at least one: name, or one of domains / office_phone / office_email)")
                for field in CUSTOM_REQUIRED_FIELDS:
                    col_key = f"custom_map_{field}"
                    sel = st.selectbox(
                        field,
                        options=column_options,
                        key=col_key,
                        index=0,
                    )
                    if sel != skip_option:
                        column_mapping[sel] = field

                st.markdown("**Recommended**")
                for field in CUSTOM_RECOMMENDED_FIELDS:
                    col_key = f"custom_map_{field}"
                    sel = st.selectbox(
                        field,
                        options=column_options,
                        key=col_key,
                        index=0,
                    )
                    if sel != skip_option:
                        column_mapping[sel] = field

                st.markdown("**Optional**")
                for field in CUSTOM_OPTIONAL_FIELDS:
                    col_key = f"custom_map_{field}"
                    sel = st.selectbox(
                        field,
                        options=column_options,
                        key=col_key,
                        index=0,
                    )
                    if sel != skip_option:
                        column_mapping[sel] = field

            # Validation: at least one of name, domains, office_phone, office_email
            mapped_required = any(
                f in column_mapping.values() for f in CUSTOM_REQUIRED_FIELDS
            )
            if not mapped_required:
                st.error(
                    "Map at least one required field: **name**, or one of **domains**, **office_phone**, **office_email**. "
                    "Otherwise we cannot create valid leads."
                )
        else:
            mapped_required = True  # Presets use default mapping; no extra validation
            # Preset: show auto-detected mapping in expander (editable)
            default_map = DEFAULT_MAPS.get(lead_source, DEFAULT_MAPS["directory"])
            auto = _auto_detect_mapping(csv_headers_list, default_map)
            # Unique lead fields from default_map, in stable order
            all_fields = list(dict.fromkeys(default_map.values()))

            with st.expander("Column mapping", expanded=True):
                for field in all_fields:
                    col_key = f"preset_map_{field}"
                    default_idx = 0
                    if field in auto:
                        try:
                            default_idx = column_options.index(auto[field])
                        except ValueError:
                            pass
                    sel = st.selectbox(
                        field,
                        options=column_options,
                        key=col_key,
                        index=default_idx,
                    )
                    if sel != skip_option:
                        column_mapping[sel] = field

        # Data preview: first 5 rows of mapped output
        if csv_headers_list and column_mapping:
            if hasattr(uploaded, "seek"):
                uploaded.seek(0)
            try:
                preview_df = pd.read_csv(uploaded, encoding="utf-8", dtype=str)
            except Exception:
                if hasattr(uploaded, "seek"):
                    uploaded.seek(0)
                preview_df = pd.read_csv(uploaded, encoding="latin-1", dtype=str)
            preview_leads = preview_mapped_rows(
                preview_df, lead_source, column_mapping=column_mapping, nrows=5
            )
            if preview_leads:
                preview_display = pd.DataFrame(preview_leads)
                st.caption("Preview (first 5 rows as they will be imported)")
                st.dataframe(preview_display, width="stretch")

        if st.button("Import CSV", key="ingest_csv_btn"):
            if lead_source == "custom" and not mapped_required:
                st.error("Fix the mapping: at least one required field must be mapped.")
            else:
                try:
                    mapping_to_use = column_mapping if column_mapping else None
                    count = import_csv(uploaded, db, lead_source, column_mapping=mapping_to_use)
                    st.success(f"Imported {count} rows.")
                except Exception as e:
                    st.error(str(e))
    else:
        st.caption("Upload a CSV to see column mapping and preview.")