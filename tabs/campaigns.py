"""Campaigns tab: create JustCall campaign, add leads, link to Attio. Configurable cohort, preview, existing campaigns with add/remove."""
import threading
import streamlit as st

from lib.config import Settings
from lib.db import LeadDB
from lib.justcall_client import JustCallClient, build_justcall_contact_from_lead
from lib.attio_client import link_campaign_to_attio


def _cohort_leads(
    db: LeadDB,
    statuses: list[str],
    lead_source: str | None,
    only_with_phone: bool,
    exclude_in_campaign: bool,
    exclude_campaign_id: str | None,
    limit: int = 500,
) -> list[dict]:
    """Return leads matching cohort filters. Post-filters for phone and justcall_campaign_id."""
    if not statuses:
        return []
    raw = db.get_leads_by_statuses(statuses, lead_source=lead_source, limit=limit)
    out = []
    for lead in raw:
        if only_with_phone and not lead.get("office_phone"):
            continue
        if exclude_in_campaign and lead.get("justcall_campaign_id"):
            continue
        if exclude_campaign_id and lead.get("justcall_campaign_id") == exclude_campaign_id:
            continue
        out.append(lead)
    return out


def render(db: LeadDB, settings: Settings) -> None:
    st.header("Campaigns")
    st.markdown(
        "Create and manage JustCall dialer campaigns from your leads. Create a campaign, add contacts, "
        "and optionally link it to Attio so campaign membership is visible in the CRM before the first call. "
        "Contacts can come from a **Campaign List** (built in the previous tab) or from ad-hoc status/source filters."
    )
    jc = JustCallClient()
    if not jc.is_configured():
        st.warning("Set JUSTCALL_API_KEY and JUSTCALL_API_SECRET.")

    # Define once so "Add contacts to campaign" (Campaign Detail) always has them
    status_options = ["synced_to_attio", "ready_for_attio"]
    sources = list(db.count_by_source().keys())
    source_options = ["All"] + (sorted(sources) if sources else [])

    # --- Section 1: Create New Campaign ---
    st.subheader("Create New Campaign")

    # Source selection: Campaign List or ad-hoc filters
    campaign_lists = db.get_campaign_lists()
    source_mode = st.radio(
        "Contact source",
        ["From Campaign List", "Ad-hoc filters"],
        key="campaign_source_mode",
        horizontal=True,
        help="Use a Campaign List you've already built, or pick leads ad-hoc by status/source.",
    )

    selected_cl_id: int | None = None

    if source_mode == "From Campaign List":
        if not campaign_lists:
            st.info("No campaign lists. Create one in the Campaign Lists tab first.")
        else:
            cl_options = {
                f"{cl['name']}  ({cl.get('member_count', 0)} leads)": cl["id"]
                for cl in campaign_lists
            }
            cl_label = st.selectbox(
                "Campaign list",
                list(cl_options.keys()),
                key="campaign_cl_select",
            )
            selected_cl_id = cl_options[cl_label]
    else:
        status_filter = st.multiselect(
            "Status",
            options=status_options,
            default=["synced_to_attio"],
            key="campaign_status_filter",
        )
        lead_source_filter = st.selectbox(
            "Lead source",
            options=source_options,
            key="campaign_lead_source_filter",
        )
        only_with_phone = st.checkbox(
            "Only leads with phone",
            value=True,
            key="campaign_only_phone",
        )
        exclude_in_campaign = st.checkbox(
            "Exclude leads already in a campaign",
            value=True,
            key="campaign_exclude_in_campaign",
        )

    campaign_name = st.text_input("Campaign name", key="campaign_name")
    campaign_type = st.selectbox(
        "Type",
        ["Autodial", "Predictive", "Dynamic"],
        key="campaign_type",
        help="JustCall dial mode: Autodial, Predictive, or Dynamic.",
    )
    country = st.text_input(
        "Country code",
        value="AU",
        key="campaign_country",
        help="ISO country code for the campaign (e.g. AU, US, GB).",
    )

    if st.button("Preview contacts", key="campaign_preview_btn"):
        if source_mode == "From Campaign List" and selected_cl_id:
            leads = db.get_campaign_list_members(selected_cl_id)
            leads = [l for l in leads if l.get("office_phone")]
        elif source_mode == "Ad-hoc filters":
            lead_source = None if lead_source_filter == "All" else lead_source_filter
            leads = _cohort_leads(
                db,
                status_filter,
                lead_source,
                only_with_phone,
                exclude_in_campaign,
                exclude_campaign_id=None,
                limit=500,
            )
        else:
            leads = []
        contacts = []
        for lead in leads:
            c = build_justcall_contact_from_lead(lead)
            if c:
                contacts.append(c)
        st.session_state.campaign_create_preview_count = len(contacts)
        st.session_state.campaign_create_preview_leads = leads
        st.session_state.campaign_create_preview_contacts = contacts
        st.session_state.campaign_create_preview_rows = [
            {
                "company": lead.get("name", ""),
                "phone": lead.get("office_phone", ""),
                "email": lead.get("office_email", ""),
                "lead_grade": lead.get("lead_grade", ""),
            }
            for lead in leads[:30]
        ]
        st.rerun()

    if "campaign_create_preview_count" in st.session_state:
        count = st.session_state.campaign_create_preview_count
        st.caption(f"Preview: {count} contacts")
        if count == 0:
            st.warning("No contacts match filters.")
        else:
            rows = st.session_state.get("campaign_create_preview_rows", [])
            if rows:
                st.dataframe(rows, width="stretch")
                if count > 30:
                    st.caption(f"... and {count - 30} more")

    preview_ready = st.session_state.get("campaign_create_preview_count", 0) > 0
    create_disabled = (
        st.session_state.get("campaign_running", False)
        or not campaign_name
        or not preview_ready
    )

    if st.button(
        "Create campaign",
        key="campaign_create_btn",
        disabled=create_disabled,
    ):
        if not jc.is_configured() or not campaign_name:
            st.error("JustCall must be configured and campaign name required.")
        else:
            leads = st.session_state.get("campaign_create_preview_leads", [])
            contacts = st.session_state.get("campaign_create_preview_contacts", [])
            if not contacts or not leads:
                st.warning("Preview again to get contacts.")
            else:
                st.session_state.campaign_running = True
                st.session_state.campaign_result = None
                cl_id_for_update = selected_cl_id

                def run():
                    try:
                        r = jc.create_campaign(
                            name=campaign_name,
                            campaign_type=campaign_type,
                            country_code=country,
                        )
                        data = r.get("data") or r
                        campaign_id = data.get("id") or data.get("campaign_id") or r.get("id")
                        if not campaign_id:
                            st.session_state.campaign_result = ("error", "No campaign id returned")
                            return
                        campaign_id = str(campaign_id)
                        if contacts:
                            jc.bulk_import_contacts(campaign_id, contacts)
                        company_ids = [l["attio_record_id"] for l in leads if l.get("attio_record_id")]
                        if settings.attio_api_key and company_ids:
                            link_campaign_to_attio(
                                campaign_name,
                                campaign_id,
                                company_ids,
                                settings.attio_api_key,
                            )
                        for lead in leads:
                            if lead.get("office_phone"):
                                db.update_lead(lead["id"], {"justcall_campaign_id": campaign_id})
                        if cl_id_for_update:
                            db.update_campaign_list(cl_id_for_update, {"justcall_campaign_id": campaign_id})
                        st.session_state.campaign_result = ("ok", campaign_id, len(contacts))
                    except Exception as e:
                        st.session_state.campaign_result = ("error", str(e))
                    finally:
                        st.session_state.campaign_running = False

                threading.Thread(target=run, daemon=True).start()
                st.rerun()

    if st.session_state.get("campaign_running"):
        st.info("Creating campaign...")
    res = st.session_state.get("campaign_result")
    if res:
        st.session_state.campaign_result = None
        if res[0] == "error":
            st.error(res[1])
        else:
            st.success(f"Campaign created. ID: {res[1]}. Contacts added: {res[2]}.")

    # --- Section 2: Existing Campaigns ---
    st.subheader("Existing Campaigns")
    if jc.is_configured():
        try:
            campaigns = jc.list_campaigns()
            counts = db.count_by_campaign()
            if campaigns:
                rows = []
                for c in campaigns:
                    cid = str(c.get("id", ""))
                    rows.append({
                        "id": cid,
                        "name": c.get("name", ""),
                        "local_lead_count": counts.get(cid, 0),
                    })
                st.dataframe(rows, width="stretch")
                options = ["—"] + [f"{r['name']} ({r['id']})" for r in rows]
                current_id = st.session_state.get("campaign_detail_id")
                default_idx = 0
                if current_id:
                    for i, r in enumerate(rows):
                        if r["id"] == current_id:
                            default_idx = i + 1
                            break
                idx = st.selectbox(
                    "Select campaign to manage",
                    range(len(options)),
                    format_func=lambda i: options[i],
                    index=default_idx,
                    key="campaign_select_idx",
                )
                if idx > 0:
                    st.session_state.campaign_detail_id = rows[idx - 1]["id"]
                else:
                    if "campaign_detail_id" in st.session_state:
                        del st.session_state.campaign_detail_id
            else:
                st.caption("No campaigns or API returned empty.")
        except Exception as e:
            st.caption(str(e))

    # --- Section 3: Campaign Detail ---
    detail_id = st.session_state.get("campaign_detail_id")
    if detail_id and jc.is_configured():
        st.subheader("Campaign Detail")
        # Ensure option lists for "Add contacts" (defensive: always set in this branch)
        status_options = ["synced_to_attio", "ready_for_attio"]
        _detail_sources = list(db.count_by_source().keys())
        source_options = ["All"] + (sorted(_detail_sources) if _detail_sources else [])
        try:
            meta = jc.get_campaign(detail_id)
            st.caption(f"JustCall: {meta.get('name', detail_id)}")
        except Exception as e:
            st.caption(f"Could not load campaign: {e}")
            meta = {}

        leads_in_campaign = db.get_leads_by_campaign(detail_id)
        with_attio = sum(1 for l in leads_in_campaign if l.get("attio_record_id"))
        st.metric("Leads in campaign (local)", len(leads_in_campaign))
        st.metric("With Attio record", with_attio)

        if leads_in_campaign:
            display_cols = ["id", "name", "office_phone", "lead_grade", "attio_record_id"]
            df_data = [{c: lead.get(c, "") or "" for c in display_cols} for lead in leads_in_campaign]
            df_data_with_remove = [{"Remove": False, **row} for row in df_data]

            st.data_editor(
                df_data_with_remove,
                column_config={"Remove": st.column_config.CheckboxColumn("Remove", default=False)},
                disabled=[c for c in display_cols],
                width="stretch",
                key="campaign_detail_editor",
            )
            if st.button("Remove selected", key="campaign_remove_btn"):
                if "campaign_detail_editor" in st.session_state:
                    edited = st.session_state.campaign_detail_editor
                    to_remove = [row["id"] for row in edited if row.get("Remove")]
                    for lead_id in to_remove:
                        db.update_lead(lead_id, {"justcall_campaign_id": None})
                    if to_remove:
                        st.caption(
                            "Cleared campaign link in DB for selected leads. "
                            "JustCall does not return contact IDs from bulk import; contacts may still appear in the campaign in JustCall and must be removed there manually if needed."
                        )
                        st.rerun()
                else:
                    st.caption("Select rows with Remove checkbox first.")
        else:
            st.caption("No local leads linked to this campaign.")

        # Add contacts to campaign
        st.caption("Add more contacts to this campaign")
        add_status_filter = st.multiselect(
            "Status",
            options=status_options,
            default=["synced_to_attio"],
            key="campaign_add_status_filter",
        )
        add_source_filter = st.selectbox(
            "Lead source",
            options=source_options,
            key="campaign_add_lead_source_filter",
        )
        add_only_phone = st.checkbox("Only leads with phone", value=True, key="campaign_add_only_phone")

        if st.button("Preview new contacts", key="campaign_add_preview_btn"):
            lead_source = None if add_source_filter == "All" else add_source_filter
            add_leads = _cohort_leads(
                db,
                add_status_filter,
                lead_source,
                add_only_phone,
                exclude_in_campaign=False,
                exclude_campaign_id=detail_id,
                limit=500,
            )
            add_contacts = []
            for lead in add_leads:
                c = build_justcall_contact_from_lead(lead)
                if c:
                    add_contacts.append(c)
            st.session_state.campaign_add_preview_count = len(add_contacts)
            st.session_state.campaign_add_preview_leads = add_leads
            st.session_state.campaign_add_preview_contacts = add_contacts
            st.rerun()

        if "campaign_add_preview_count" in st.session_state:
            add_count = st.session_state.campaign_add_preview_count
            st.caption(f"New contacts to add: {add_count}")
            if add_count > 0 and st.button(
                "Add to campaign",
                key="campaign_add_btn",
                disabled=st.session_state.get("campaign_add_running", False),
            ):
                st.session_state.campaign_add_running = True
                add_leads = st.session_state.get("campaign_add_preview_leads", [])
                add_contacts = st.session_state.get("campaign_add_preview_contacts", [])

                def run_add():
                    try:
                        if add_contacts:
                            jc.bulk_import_contacts(detail_id, add_contacts)
                        company_ids = [l["attio_record_id"] for l in add_leads if l.get("attio_record_id")]
                        if settings.attio_api_key and company_ids:
                            link_campaign_to_attio(
                                meta.get("name", "Campaign"),
                                detail_id,
                                company_ids,
                                settings.attio_api_key,
                            )
                        for lead in add_leads:
                            if lead.get("office_phone"):
                                db.update_lead(lead["id"], {"justcall_campaign_id": detail_id})
                        st.session_state.campaign_add_result = len(add_leads)
                    except Exception as e:
                        st.session_state.campaign_add_result = ("error", str(e))
                    finally:
                        st.session_state.campaign_add_running = False

                threading.Thread(target=run_add, daemon=True).start()
                st.rerun()

        if st.session_state.get("campaign_add_running"):
            st.info("Adding contacts...")
        add_res = st.session_state.get("campaign_add_result")
        if add_res is not None:
            st.session_state.campaign_add_result = None
            if isinstance(add_res, tuple):
                st.error(add_res[1])
            else:
                st.success(f"Added {add_res} leads to campaign.")
