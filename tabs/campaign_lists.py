"""Campaign Lists tab: named lead lists with configurable Attio field mapping, preview/diff, and sync."""
import io
import json
import threading
import streamlit as st

from lib.config import Settings
from lib.db import LeadDB
from lib.filters_config import get_attio_sync_mapping, get_attio_people_sync_mapping
from lib.attio_client import (
    fetch_attio_attributes,
    get_company_diff,
    get_person_diff,
    sync_campaign_list_to_attio,
)

FLAGGED_STATUSES = frozenset({"flagged_keyword", "flagged_llm", "excluded"})


def _get_selected_list(db: LeadDB) -> dict | None:
    list_id = st.session_state.get("cl_selected_list_id")
    if not list_id:
        return None
    return db.get_campaign_list(list_id)


def render(db: LeadDB, settings: Settings) -> None:
    st.header("Campaign Lists")
    st.markdown(
        "Build named lists of leads, configure which Attio fields to update, "
        "preview diffs, then sync to Attio. Sync updates **existing** Attio company/person records only. "
        "Use the **Campaigns** tab to create a JustCall campaign from this list or from ad-hoc filters."
    )

    # ── Section 1: Create / Select List ──
    st.subheader("Create / Select List")

    col_create, col_select = st.columns([1, 2])

    with col_create:
        new_name = st.text_input("New list name", key="cl_new_name")
        new_attio_object = st.radio(
            "Attio object",
            ["companies", "people"],
            format_func=lambda x: "Companies" if x == "companies" else "People",
            key="cl_new_attio_object",
            horizontal=True,
        )
        if st.button("Create list", key="cl_create_btn"):
            if not new_name.strip():
                st.warning("Enter a name.")
            else:
                mapping = get_attio_people_sync_mapping() if new_attio_object == "people" else get_attio_sync_mapping()
                new_id = db.create_campaign_list(
                    new_name.strip(),
                    field_mapping=mapping,
                    attio_object=new_attio_object,
                )
                st.session_state.cl_selected_list_id = new_id
                st.success(f"Created list: {new_name.strip()}")
                st.rerun()

    with col_select:
        all_lists = db.get_campaign_lists()
        if all_lists:
            obj_label = lambda cl: (cl.get("attio_object") or "companies").capitalize()
            options = {
                f"{cl['name']}  ({cl.get('member_count', 0)} leads, {obj_label(cl)}, {cl['attio_sync_status']})": cl["id"]
                for cl in all_lists
            }
            labels = list(options.keys())

            current_id = st.session_state.get("cl_selected_list_id")
            default_idx = 0
            if current_id:
                for i, cl in enumerate(all_lists):
                    if cl["id"] == current_id:
                        default_idx = i
                        break

            selected_label = st.selectbox(
                "Select list",
                labels,
                index=default_idx,
                key="cl_select_box",
            )
            st.session_state.cl_selected_list_id = options[selected_label]
        else:
            st.caption("No lists yet. Create one above.")

    selected = _get_selected_list(db)
    if not selected:
        return

    # Delete list action
    if st.button("Delete this list", key="cl_delete_btn", type="secondary"):
        db.delete_campaign_list(selected["id"])
        st.session_state.cl_selected_list_id = None
        st.rerun()

    st.divider()

    # ── Section 2: List Members ──
    st.subheader("List Members")

    member_ids = set(db.get_campaign_list_member_ids(selected["id"]))
    members = db.get_campaign_list_members(selected["id"]) if member_ids else []

    st.caption(f"{len(members)} leads in this list")

    if members:
        display_cols = ["id", "name", "domains", "office_phone", "segment", "status", "attio_record_id"]
        df_data = [{"Remove": False, **{c: lead.get(c, "") or "" for c in display_cols}} for lead in members]

        edited = st.data_editor(
            df_data,
            column_config={"Remove": st.column_config.CheckboxColumn("Remove", default=False)},
            disabled=display_cols,
            width="stretch",
            key=f"cl_members_editor_{selected['id']}",
        )
        col_rm, col_clear = st.columns(2)
        with col_rm:
            if st.button("Remove selected", key="cl_remove_selected"):
                to_remove = [row["id"] for row in edited if row.get("Remove")]
                if to_remove:
                    db.remove_from_campaign_list(selected["id"], to_remove)
                    st.rerun()
        with col_clear:
            if st.button("Clear all members", key="cl_clear_all"):
                db.clear_campaign_list(selected["id"])
                st.rerun()

    # Add leads
    with st.expander("Add leads", expanded=not members):
        status_options = ["enriched", "ready_for_attio", "synced_to_attio", "pending_review"]
        status_filter = st.multiselect(
            "Status",
            options=status_options,
            default=["ready_for_attio", "enriched"],
            key="cl_add_status",
        )
        sources = list(db.count_by_source().keys())
        source_options = ["All"] + sorted(sources)
        lead_source_filter = st.selectbox(
            "Lead source",
            options=source_options,
            key="cl_add_source",
        )
        exclude_oos = st.checkbox(
            "Exclude out-of-scope",
            value=True,
            key="cl_add_oos",
            help="Exclude leads marked out of scope by LLM scope review.",
        )
        exclude_flagged = st.checkbox(
            "Exclude flagged",
            value=True,
            key="cl_add_flagged",
            help="Exclude leads flagged by keyword cleaning or LLM (flagged_keyword, flagged_llm, excluded).",
        )

        # Custom tags filter
        all_tags = db.get_all_tags()
        if all_tags:
            with st.expander("Filter by custom tags", expanded=False):
                tag_options = {f"{t['name']} ({t['lead_count']} leads)": t["id"] for t in all_tags}
                selected_tags = st.multiselect(
                    "Include leads with any of these tags",
                    options=list(tag_options.keys()),
                    key="cl_add_tags",
                )
                selected_tag_ids = [tag_options[label] for label in selected_tags]
                tag_mode = st.radio(
                    "Tag filter mode",
                    ["Add to filters (AND)", "Use tags only (ignore status/source)"],
                    key="cl_tag_mode",
                    horizontal=True,
                )

        if st.button("Preview matching leads", key="cl_preview_btn"):
            lead_source = None if lead_source_filter == "All" else lead_source_filter

            use_tags_only = (all_tags and selected_tags and
                             tag_mode == "Use tags only (ignore status/source)")

            if use_tags_only:
                candidates = db.get_leads_by_tags(selected_tag_ids, limit=5000)
            else:
                candidates = db.get_leads_by_statuses(
                    status_filter,
                    lead_source=lead_source,
                    limit=5000,
                )
                if all_tags and selected_tags and selected_tag_ids:
                    tagged_leads = db.get_leads_by_tags(selected_tag_ids, limit=10000)
                    tagged_ids = {l["id"] for l in tagged_leads}
                    candidates = [l for l in candidates if l["id"] in tagged_ids]

            filtered = []
            for lead in candidates:
                if lead["id"] in member_ids:
                    continue
                if exclude_oos and lead.get("out_of_scope"):
                    continue
                if exclude_flagged and (lead.get("status") or "") in FLAGGED_STATUSES:
                    continue
                filtered.append(lead)
            st.session_state.cl_preview_leads = filtered
            st.session_state.cl_preview_count = len(filtered)

        if "cl_preview_count" in st.session_state:
            st.caption(f"Matching: {st.session_state.cl_preview_count} leads (not already in list)")

        if st.button("Add to list", key="cl_add_btn"):
            preview = st.session_state.get("cl_preview_leads", [])
            if not preview:
                st.warning("Preview leads first.")
            else:
                ids_to_add = [l["id"] for l in preview if l.get("id")]
                added = db.add_to_campaign_list(selected["id"], ids_to_add)
                st.success(f"Added {added} leads to list.")
                st.session_state.pop("cl_preview_leads", None)
                st.session_state.pop("cl_preview_count", None)
                st.rerun()

    st.divider()

    # ── Section 3: Attio Field Mapping ──
    st.subheader("Attio Field Mapping")

    attio_object = (selected.get("attio_object") or "companies").strip().lower()
    if attio_object not in ("companies", "people"):
        attio_object = "companies"

    # Allow switching Attio object; reload default mapping when switching
    new_attio_object = st.radio(
        "Sync to Attio object",
        ["companies", "people"],
        index=0 if attio_object == "companies" else 1,
        format_func=lambda x: "Companies" if x == "companies" else "People",
        key=f"cl_attio_object_{selected['id']}",
        horizontal=True,
    )
    if new_attio_object != attio_object:
        db.update_campaign_list(selected["id"], {"attio_object": new_attio_object})
        default_mapping = get_attio_people_sync_mapping() if new_attio_object == "people" else get_attio_sync_mapping()
        db.update_campaign_list(selected["id"], {"field_mapping": default_mapping})
        st.rerun()

    mapping = selected.get("field_mapping")
    if not mapping or not isinstance(mapping, list):
        mapping = get_attio_people_sync_mapping() if attio_object == "people" else get_attio_sync_mapping()
        db.update_campaign_list(selected["id"], {"field_mapping": mapping})
        st.rerun()

    mapping_display = []
    for m in mapping:
        mapping_display.append({
            "Enabled": m.get("enabled", True),
            "Lead Field": m["lead_field"],
            "Attio Attribute": m["attio_attribute"],
        })

    edited_mapping = st.data_editor(
        mapping_display,
        column_config={
            "Enabled": st.column_config.CheckboxColumn("Enabled", default=True),
            "Lead Field": st.column_config.TextColumn("Lead Field", disabled=True),
            "Attio Attribute": st.column_config.TextColumn("Attio Attribute"),
        },
        disabled=["Lead Field"],
        width="stretch",
        key=f"cl_mapping_editor_{selected['id']}",
    )

    col_save, col_reset, col_refresh = st.columns(3)

    with col_save:
        if st.button("Save mapping", key="cl_save_mapping"):
            new_mapping = []
            for row in edited_mapping:
                new_mapping.append({
                    "lead_field": row["Lead Field"],
                    "attio_attribute": row["Attio Attribute"],
                    "enabled": row["Enabled"],
                })
            db.update_campaign_list(selected["id"], {"field_mapping": new_mapping})
            st.success("Mapping saved.")
            st.rerun()

    with col_reset:
        if st.button("Reset to defaults", key="cl_reset_mapping"):
            default_mapping = get_attio_people_sync_mapping() if attio_object == "people" else get_attio_sync_mapping()
            db.update_campaign_list(selected["id"], {"field_mapping": default_mapping})
            st.success("Reset to default mapping.")
            st.rerun()

    with col_refresh:
        if st.button("Show Attio attributes", key="cl_refresh_attio"):
            if not settings.attio_api_key:
                st.warning("Set ATTIO_API_KEY first.")
            else:
                try:
                    attrs = fetch_attio_attributes(settings.attio_api_key, object_slug=attio_object)
                    writable = [a for a in attrs if a.get("is_writable")]
                    st.session_state.cl_attio_attrs = writable
                    st.session_state.cl_attio_attrs_object = attio_object
                except Exception as e:
                    st.error(f"Could not fetch: {e}")

    if "cl_attio_attrs" in st.session_state:
        obj_cap = (st.session_state.get("cl_attio_attrs_object") or attio_object).capitalize()
        st.caption(f"Writable Attio {obj_cap} attributes:")
        st.dataframe(
            [{"slug": a["slug"], "title": a["title"], "type": a["type"]}
             for a in st.session_state.cl_attio_attrs],
            width="stretch",
        )

    st.divider()

    # ── Section 4: Preview & Sync ──
    st.subheader("Preview & Sync to Attio")
    record_id_key = "attio_person_id" if attio_object == "people" else "attio_record_id"
    obj_name = "person" if attio_object == "people" else "company"
    st.caption(
        f"Sync pushes updated data to **existing** Attio {obj_name} records only. "
        f"Leads without an `{record_id_key}` are skipped — no new records are created in Attio."
    )

    if not members:
        st.caption("Add leads to the list first.")
        return

    if not settings.attio_api_key:
        st.warning("Set ATTIO_API_KEY to sync.")
        return

    updatable = [l for l in members if l.get(record_id_key)]
    skippable = [l for l in members if not l.get(record_id_key)]

    st.write(f"**{len(updatable)}** leads with Attio {obj_name} record (will update). "
             f"**{len(skippable)}** without (will be skipped).")

    if updatable:
        display_cols = ["name", "domains", record_id_key] if attio_object == "companies" else ["name", "office_email", "office_phone", record_id_key]
        st.dataframe(
            [{c: l.get(c, "") for c in display_cols} for l in updatable[:30]],
            width="stretch",
        )
        if len(updatable) > 30:
            st.caption(f"... and {len(updatable) - 30} more")

    if skippable:
        with st.expander(f"Leads without Attio {obj_name} record ({len(skippable)})"):
            st.dataframe(
                [{"id": l.get("id"), "name": l.get("name", ""), "status": l.get("status", "")}
                 for l in skippable[:30]],
                width="stretch",
            )

    enabled_mapping = [m for m in mapping if m.get("enabled", True)]

    def _get_diff(lead, api_key, field_mapping):
        if attio_object == "people":
            return get_person_diff(lead, api_key, field_mapping=field_mapping)
        return get_company_diff(lead, api_key, field_mapping=field_mapping)

    if updatable and st.button("Show diff (sample)", key="cl_show_diff"):
        sample = updatable[:5]
        for lead in sample:
            diff = _get_diff(lead, settings.attio_api_key, enabled_mapping)
            if not diff:
                st.caption(f"{lead.get('name', '')}: Could not fetch Attio record.")
                continue
            st.write(f"**{lead.get('name', '')}**")
            rows = []
            for field in diff["current"]:
                cur = (diff["current"].get(field) or "")[:200]
                new = (diff["new"].get(field) or "")[:200]
                rows.append({"field": field, "Attio (current)": cur, "Local (will send)": new})
            st.dataframe(rows, width="stretch", hide_index=True)
            st.divider()

    if st.button(
        "Confirm & Sync to Attio",
        key="cl_confirm_sync",
        disabled=st.session_state.get("cl_sync_running", False) or not updatable,
    ):
        st.session_state.cl_sync_running = True
        st.session_state.cl_sync_result = None

        list_id = selected["id"]
        api_key = settings.attio_api_key

        def run_sync():
            try:
                success, skipped, results = sync_campaign_list_to_attio(
                    db, list_id, api_key,
                    field_mapping=enabled_mapping,
                    attio_object=attio_object,
                )
                st.session_state.cl_sync_result = (success, skipped, results)
            except Exception as e:
                st.session_state.cl_sync_result = ("error", str(e), [])
            finally:
                st.session_state.cl_sync_running = False

        threading.Thread(target=run_sync, daemon=True).start()
        st.rerun()

    if st.session_state.get("cl_sync_running"):
        st.info("Syncing to Attio...")

    result = st.session_state.get("cl_sync_result")
    if result:
        st.session_state.cl_sync_result = None
        if result[0] == "error":
            st.error(result[1])
        else:
            success, skipped, results_log = result
            st.success(f"Updated: {success}, Skipped: {skipped}")
            if results_log:
                csv_buf = io.StringIO()
                csv_buf.write("id,name,action,record_id,reason,error\n")
                for r in results_log:
                    name = (r.get("name") or "").replace(",", " ")
                    csv_buf.write(
                        f"{r.get('id','')},{name},{r.get('action','')},{r.get('record_id','')},{r.get('reason','')},{r.get('error','')}\n"
                    )
                st.download_button(
                    "Download results CSV",
                    csv_buf.getvalue(),
                    file_name="campaign_list_sync_results.csv",
                    mime="text/csv",
                )
