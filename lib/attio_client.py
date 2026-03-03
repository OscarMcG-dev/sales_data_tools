"""
Attio API: dedup (export lookups, classify leads), sync (company/people), campaign link.
"""
import asyncio
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

import httpx

from lib.db import LeadDB
from lib.url_utils import extract_domain

logger = logging.getLogger(__name__)

ATTIO_API_BASE = "https://api.attio.com/v2"

# How to extract a scalar key from each Attio attribute value (for dedup lookups)
_ATTIO_VALUE_KEY: Dict[str, str] = {
    "domains": "domain",
    "office_phone": "original_phone_number",
    "email_addresses": "email_address",
}


def _extract_lookup_keys(values_list: List[Any], attio_attribute: str) -> List[str]:
    keys: List[str] = []
    key_name = _ATTIO_VALUE_KEY.get(attio_attribute, "value")
    for entry in values_list or []:
        if not isinstance(entry, dict):
            continue
        val = entry.get(key_name) or entry.get("value") or ""
        if isinstance(val, str) and val.strip():
            keys.append(val.strip().lower() if attio_attribute == "domains" else val.strip())
    return keys


async def export_attio_lookups(api_key: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Paginate through all Attio company records and build lookup dicts from config.
    Returns (domain_to_record_id, phone_to_record_id) for backward compatibility.
    Uses filters_config dedup.company_rules to decide which attributes to index.
    """
    try:
        from lib.filters_config import get_dedup_config
        company_rules = get_dedup_config().get("company_rules") or []
    except Exception:
        company_rules = [
            {"lead_field": "domains", "attio_attribute": "domains"},
            {"lead_field": "office_phone", "attio_attribute": "office_phone"},
        ]

    lookups: Dict[str, Dict[str, str]] = {r["lead_field"]: {} for r in company_rules}
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    offset = 0
    page_size = 50
    total_fetched = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            payload = {
                "sorts": [{"attribute": "created_at", "direction": "asc"}],
                "limit": page_size,
                "offset": offset,
            }
            resp = await client.post(
                f"{ATTIO_API_BASE}/objects/companies/records/query",
                headers=headers,
                json=payload,
            )
            if resp.status_code != 200:
                logger.error("Attio API error (%s): %s", resp.status_code, resp.text[:500])
                break
            data = resp.json()
            records = data.get("data", [])
            if not records:
                break
            for record in records:
                record_id = record.get("id", {}).get("record_id", "")
                values = record.get("values", {})
                for rule in company_rules:
                    lead_field = rule.get("lead_field")
                    attio_attr = rule.get("attio_attribute")
                    if not lead_field or not attio_attr or lead_field not in lookups:
                        continue
                    entries = values.get(attio_attr)
                    if isinstance(entries, list):
                        for key in _extract_lookup_keys(entries, attio_attr):
                            lookups[lead_field][key] = record_id
            total_fetched += len(records)
            logger.info("Fetched %s Attio records...", total_fetched)
            if not data.get("next_page_token") and len(records) < page_size:
                break
            offset += page_size

    domain_lookup = lookups.get("domains", {})
    phone_lookup = lookups.get("office_phone", {})
    logger.info(
        "Attio export complete: %s domains, %s phones across %s records",
        len(domain_lookup), len(phone_lookup), total_fetched,
    )
    return domain_lookup, phone_lookup


def classify_leads(
    db: LeadDB,
    domain_lookup: Dict[str, str],
    phone_lookup: Dict[str, str],
    status_filter: str = "enriched",
    api_key: Optional[str] = None,
) -> Tuple[int, int]:
    """
    Get leads with status=enriched, classify each as new or existing using config-driven rules.
    If api_key is set and no cache match: tries lookup_company_by_domain then lookup_person_by_email.
    Returns (new_count, existing_count).
    """
    try:
        from lib.filters_config import get_dedup_config
        company_rules = get_dedup_config().get("company_rules") or []
    except Exception:
        company_rules = [
            {"lead_field": "domains", "attio_attribute": "domains"},
            {"lead_field": "office_phone", "attio_attribute": "office_phone"},
        ]

    lookups: Dict[str, Dict[str, str]] = {
        "domains": domain_lookup,
        "office_phone": phone_lookup,
    }
    leads = db.get_leads(status=status_filter, limit=10_000)
    new_count = 0
    existing_count = 0

    for lead in leads:
        lead_id = lead.get("id")
        if not lead_id:
            continue
        matched_id = None
        matched_person_id: Optional[str] = None
        for rule in company_rules:
            lead_field = rule.get("lead_field")
            if not lead_field or lead_field not in lookups:
                continue
            raw = lead.get(lead_field)
            if raw is None or raw == "":
                continue
            key = (raw if isinstance(raw, str) else str(raw)).strip()
            if lead_field == "domains":
                key = key.lower()
            matched_id = lookups[lead_field].get(key)
            if matched_id:
                break

        if not matched_id and api_key:
            domain = (lead.get("domains") or "")
            if isinstance(domain, str) and domain.strip():
                matched_id = lookup_company_by_domain(domain.strip(), api_key)
            if not matched_id and lead.get("office_email"):
                person_id = lookup_person_by_email(lead.get("office_email"), api_key)
                if person_id:
                    matched_person_id = person_id
                    matched_id = person_id

        if matched_id:
            update: Dict[str, Any] = {
                "attio_status": "existing",
                "status": "duplicate",
                "duplicate_of": matched_id,
            }
            if matched_person_id:
                update["attio_person_id"] = matched_person_id
            else:
                update["attio_record_id"] = matched_id
            db.update_lead(lead_id, update)
            existing_count += 1
        else:
            db.update_lead(lead_id, {
                "attio_status": "new",
                "status": "ready_for_attio",
            })
            new_count += 1

    logger.info("Classification: %s new, %s existing", new_count, existing_count)
    return new_count, existing_count


def lookup_company_by_domain(domain: str, api_key: str) -> Optional[str]:
    """Query Attio companies by domain (exact match). Returns record_id if found, else None."""
    if not (domain and api_key):
        return None
    domain = (domain if isinstance(domain, str) else "").strip().lower()
    if not domain:
        return None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "filter": {"attribute": "domains", "op": "eq", "value": domain},
        "limit": 1,
    }
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(
            f"{ATTIO_API_BASE}/objects/companies/records/query",
            headers=headers,
            json=payload,
        )
    if resp.status_code != 200:
        logger.debug("Attio company lookup by domain %s: %s", domain[:20], resp.status_code)
        return None
    data = resp.json()
    records = data.get("data", [])
    if not records:
        return None
    return (records[0].get("id") or {}).get("record_id")


def lookup_person_by_email(email: str, api_key: str) -> Optional[str]:
    """Query Attio people by email (exact match). Returns person record_id if found, else None."""
    if not (email and api_key):
        return None
    email = (email if isinstance(email, str) else "").strip()
    if not email or "@" not in email:
        return None
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "filter": {"attribute": "email_addresses", "op": "eq", "value": email},
        "limit": 1,
    }
    with httpx.Client(timeout=15.0) as client:
        resp = client.post(
            f"{ATTIO_API_BASE}/objects/people/records/query",
            headers=headers,
            json=payload,
        )
    if resp.status_code != 200:
        logger.debug("Attio person lookup by email: %s", resp.status_code)
        return None
    data = resp.json()
    records = data.get("data", [])
    if not records:
        return None
    return (records[0].get("id") or {}).get("record_id")


def _build_company_values(
    lead: dict,
    field_mapping: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """
    Build Attio company values dict from a lead using the given field mapping.
    Only enabled fields are included. Handles special cases like edited_description
    fallback, primary_location assembly, and JSON columns.
    """
    if field_mapping is None:
        from lib.filters_config import get_attio_sync_mapping
        field_mapping = get_attio_sync_mapping()

    enabled = {
        m["lead_field"]: m["attio_attribute"]
        for m in field_mapping
        if m.get("enabled", True)
    }
    values: Dict[str, Any] = {}

    if "name" in enabled:
        name = (lead.get("name") or "").strip()
        if name:
            values[enabled["name"]] = [{"value": name}]

    if "domains" in enabled:
        domain = (lead.get("domains") or "").strip()
        if domain:
            values[enabled["domains"]] = [{"domain": domain}]

    if "description" in enabled:
        desc = (lead.get("edited_description") or "").strip() or (lead.get("description") or "").strip()
        if desc:
            values[enabled["description"]] = [{"value": desc}]

    if "office_phone" in enabled:
        phone = (lead.get("office_phone") or "").strip()
        if phone:
            values[enabled["office_phone"]] = [{"original_phone_number": phone}]

    if "office_email" in enabled:
        email = (lead.get("office_email") or "").strip()
        if email:
            values[enabled["office_email"]] = [{"email_address": email}]

    if "segment" in enabled:
        seg = (lead.get("segment") or "").strip()
        if seg:
            values[enabled["segment"]] = [{"value": seg}]

    if "primary_location" in enabled:
        loc_parts = {
            "line_1": (lead.get("primary_location_line_1") or "").strip(),
            "locality": (lead.get("primary_location_locality") or "").strip(),
            "region": (lead.get("primary_location_region") or "").strip(),
            "postcode": (lead.get("primary_location_postcode") or "").strip(),
            "country_code": "AU",
        }
        if any(loc_parts[k] for k in ("line_1", "locality", "region", "postcode")):
            values[enabled["primary_location"]] = [loc_parts]

    if "linkedin" in enabled:
        li = (lead.get("linkedin") or "").strip()
        if li:
            values[enabled["linkedin"]] = [{"value": li}]

    if "facebook" in enabled:
        fb = (lead.get("facebook") or "").strip()
        if fb:
            values[enabled["facebook"]] = [{"value": fb}]

    if "organisational_structure" in enabled:
        org = (lead.get("organisational_structure") or "").strip()
        if org:
            values[enabled["organisational_structure"]] = [{"value": org}]

    if "areas_of_accountancy" in enabled:
        raw_areas = lead.get("areas_of_accountancy")
        if isinstance(raw_areas, list):
            text = ", ".join(str(a) for a in raw_areas if a)
        elif isinstance(raw_areas, str):
            text = raw_areas.strip()
        else:
            text = ""
        if text:
            values[enabled["areas_of_accountancy"]] = [{"value": text}]

    return values


def _build_person_values(
    lead: dict,
    field_mapping: Optional[List[Dict[str, Any]]] = None,
    company_record_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build Attio people values dict from a lead using the given field mapping.
    Only enabled fields are included. name is split into first/last for Attio name attribute.
    """
    if field_mapping is None:
        from lib.filters_config import get_attio_people_sync_mapping
        field_mapping = get_attio_people_sync_mapping()

    enabled = {
        m["lead_field"]: m["attio_attribute"]
        for m in field_mapping
        if m.get("enabled", True)
    }
    values: Dict[str, Any] = {}

    if "name" in enabled:
        raw = (lead.get("name") or "").strip()
        if raw:
            parts = raw.split(None, 1)
            first_name = parts[0] if parts else ""
            last_name = parts[1] if len(parts) > 1 else ""
            values[enabled["name"]] = [{"first_name": first_name, "last_name": last_name}]

    if "office_email" in enabled:
        email = (lead.get("office_email") or "").strip()
        if email:
            values[enabled["office_email"]] = [{"email_address": email}]

    if "office_phone" in enabled:
        phone = (lead.get("office_phone") or "").strip()
        if phone:
            # Attio people phone_numbers use "phone_number" key
            values[enabled["office_phone"]] = [{"phone_number": phone}]

    if "description" in enabled:
        desc = (lead.get("edited_description") or "").strip() or (lead.get("description") or "").strip()
        if desc:
            values[enabled["description"]] = [{"value": desc}]

    if company_record_id:
        values["company"] = [{"referenced_record_id": company_record_id}]

    return values


def _decision_maker_to_person(dm: dict) -> dict:
    """Map a decision_makers list item to shape expected by sync_person_to_attio."""
    name = (dm.get("name") or "").strip()
    parts = name.split(None, 1)
    first_name = parts[0] if parts else ""
    last_name = parts[1] if len(parts) > 1 else ""
    phones = []
    for key in ("phone_office", "phone_mobile", "phone_direct"):
        p = (dm.get(key) or "").strip()
        if p:
            phones.append(p)
    return {
        "first_name": first_name,
        "last_name": last_name,
        "job_title": (dm.get("title") or dm.get("job_title") or "").strip() or None,
        "email_addresses": (dm.get("email") or "").strip() or None,
        "phone_numbers": phones if phones else None,
    }


def sync_people_for_company(lead: dict, company_record_id: str, api_key: str) -> Optional[str]:
    """
    Create Attio people records from lead decision_makers linked to the company.
    Returns the first person's record_id, or None if none created.
    """
    raw = lead.get("decision_makers")
    if not raw:
        return None
    if isinstance(raw, str):
        try:
            import json
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return None
    if not isinstance(raw, list):
        return None
    first_id: Optional[str] = None
    for dm in raw:
        if not isinstance(dm, dict):
            continue
        person = _decision_maker_to_person(dm)
        if not person.get("first_name") and not person.get("email_addresses"):
            continue
        try:
            record_id = sync_person_to_attio(person, company_record_id, api_key)
            if first_id is None:
                first_id = record_id
        except Exception as e:
            logger.warning("Sync person to Attio failed: %s", e)
    return first_id


def sync_person_to_attio(
    person: dict,
    company_record_id: str,
    api_key: str,
) -> str:
    """Create Attio person record linked to company. Returns record_id."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    values: Dict[str, Any] = {}
    if person.get("first_name"):
        values["name"] = [{
            "first_name": person["first_name"],
            "last_name": person.get("last_name") or "",
        }]
    if person.get("email_addresses"):
        values["email_addresses"] = [{"email_address": person["email_addresses"]}]
    if person.get("phone_numbers"):
        values["phone_numbers"] = [
            {"phone_number": p} for p in (person["phone_numbers"] if isinstance(person["phone_numbers"], list) else [person["phone_numbers"]])
        ]
    if person.get("job_title"):
        values["job_title"] = [{"value": person["job_title"]}]
    values["company"] = [{"referenced_record_id": company_record_id}]

    payload = {"data": {"values": values}}
    with httpx.Client(timeout=30.0) as client:
        resp = client.post(
            f"{ATTIO_API_BASE}/objects/people/records",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        record_id = data.get("data", {}).get("id", {}).get("record_id", "")
        if not record_id:
            raise ValueError("Attio did not return person record_id")
        return record_id


def update_attio_company(
    record_id: str,
    values: Dict[str, Any],
    api_key: str,
) -> Dict[str, Any]:
    """PATCH an existing Attio company record. Returns the updated record data."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"data": {"values": values}}
    with httpx.Client(timeout=30.0) as client:
        resp = client.patch(
            f"{ATTIO_API_BASE}/objects/companies/records/{record_id}",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        return resp.json().get("data", {})


def update_attio_person(
    record_id: str,
    values: Dict[str, Any],
    api_key: str,
) -> Dict[str, Any]:
    """PATCH an existing Attio person record. Returns the updated record data."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {"data": {"values": values}}
    with httpx.Client(timeout=30.0) as client:
        resp = client.patch(
            f"{ATTIO_API_BASE}/objects/people/records/{record_id}",
            headers=headers,
            json=payload,
        )
        resp.raise_for_status()
        return resp.json().get("data", {})


def fetch_attio_attributes(
    api_key: str,
    object_slug: str = "companies",
) -> List[Dict[str, Any]]:
    """
    Fetch attribute definitions from Attio for the given object.
    Returns list of {slug, title, type, is_writable}.
    Paginates to get all attributes.
    """
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    attributes: List[Dict[str, Any]] = []
    offset = 0
    page_size = 50

    with httpx.Client(timeout=30.0) as client:
        while True:
            resp = client.get(
                f"{ATTIO_API_BASE}/objects/{object_slug}/attributes",
                headers=headers,
                params={"offset": offset, "limit": page_size},
            )
            if resp.status_code != 200:
                logger.warning("Attio attributes fetch error %s: %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            items = data.get("data", [])
            for item in items:
                attr_type = item.get("type", "")
                is_writable = attr_type not in ("interaction",)
                attributes.append({
                    "slug": item.get("api_slug", ""),
                    "title": item.get("title", ""),
                    "type": attr_type,
                    "is_writable": is_writable,
                })
            if len(items) < page_size:
                break
            offset += page_size

    return attributes


def sync_campaign_list_to_attio(
    db: LeadDB,
    campaign_list_id: int,
    api_key: str,
    field_mapping: Optional[List[Dict[str, Any]]] = None,
    attio_object: str = "companies",
) -> Tuple[int, int, List[Dict[str, Any]]]:
    """
    Sync all leads in a campaign list to Attio (update existing records only).
    Returns (success_count, skip_count, results_log).
    - When attio_object is "companies": leads without attio_record_id are skipped; updates company records.
    - When attio_object is "people": leads without attio_person_id are skipped; updates person records.
    """
    members = db.get_campaign_list_members(campaign_list_id)
    if not members:
        return 0, 0, []

    is_people = attio_object == "people"
    id_key = "attio_person_id" if is_people else "attio_record_id"
    success = 0
    skipped = 0
    results: List[Dict[str, Any]] = []

    for lead in members:
        lead_id = lead.get("id")
        record_id = lead.get(id_key)
        if not record_id:
            skipped += 1
            results.append({
                "id": lead_id,
                "name": lead.get("name", ""),
                "action": "skipped",
                "reason": f"no {id_key}",
            })
            continue
        try:
            if is_people:
                values = _build_person_values(
                    lead,
                    field_mapping,
                    company_record_id=lead.get("attio_record_id"),
                )
                if not values:
                    skipped += 1
                    results.append({
                        "id": lead_id,
                        "name": lead.get("name", ""),
                        "action": "skipped",
                        "reason": "no fields to update",
                    })
                    continue
                update_attio_person(record_id, values, api_key)
                db.update_lead(lead_id, {"status": "synced_to_attio"})
            else:
                values = _build_company_values(lead, field_mapping)
                if not values:
                    skipped += 1
                    results.append({
                        "id": lead_id,
                        "name": lead.get("name", ""),
                        "action": "skipped",
                        "reason": "no fields to update",
                    })
                    continue
                update_attio_company(record_id, values, api_key)
                person_id = sync_people_for_company(lead, record_id, api_key)
                upd: Dict[str, Any] = {"status": "synced_to_attio"}
                if person_id:
                    upd["attio_person_id"] = person_id
                db.update_lead(lead_id, upd)
            success += 1
            results.append({
                "id": lead_id,
                "name": lead.get("name", ""),
                "action": "updated",
                "record_id": record_id,
            })
        except Exception as e:
            logger.warning("Sync failed for lead id=%s: %s", lead_id, e)
            results.append({
                "id": lead_id,
                "name": lead.get("name", ""),
                "action": "failed",
                "error": str(e),
            })

    if success > 0:
        db.update_campaign_list(campaign_list_id, {
            "attio_sync_status": "synced" if skipped == 0 else "partial",
        })

    return success, skipped, results


def link_campaign_to_attio(
    campaign_name: str,
    campaign_id: str,
    company_record_ids: List[str],
    api_key: str,
) -> None:
    """
    Proactive link: create a list in Attio (companies) and add company records
    so campaign membership is visible before first call.
    """
    if not company_record_ids:
        return
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=30.0) as client:
        list_payload = {
            "title": f"JustCall: {campaign_name}",
            "object": "companies",
        }
        resp = client.post(f"{ATTIO_API_BASE}/lists", headers=headers, json=list_payload)
        if resp.status_code not in (200, 201):
            logger.warning("Could not create Attio list: %s %s", resp.status_code, resp.text[:200])
            return
        list_data = resp.json()
        list_id = list_data.get("data", {}).get("id", {}).get("list_id")
        if not list_id:
            return
        for record_id in company_record_ids:
            try:
                client.post(
                    f"{ATTIO_API_BASE}/lists/{list_id}/entries",
                    headers=headers,
                    json={"data": {"record_id": record_id}},
                )
            except Exception as e:
                logger.debug("Add company to list failed: %s", e)


# --- Attio list ingestion (lists as staging input for enrichment) ---


def _first_text(values: list) -> Optional[str]:
    """Extract first text value from Attio values array."""
    if not values or not isinstance(values, list):
        return None
    v = values[0] if values else None
    if not isinstance(v, dict):
        return str(v).strip() or None
    return (v.get("value") or v.get("domain") or "").strip() or None


def _first_phone(values: list) -> Optional[str]:
    """Extract first phone from Attio values array."""
    if not values or not isinstance(values, list):
        return None
    v = values[0] if values else None
    if not isinstance(v, dict):
        return None
    return (v.get("original_phone_number") or v.get("phone_number") or v.get("value") or "").strip() or None


def _first_email(values: list) -> Optional[str]:
    if not values or not isinstance(values, list):
        return None
    v = values[0] if values else None
    if not isinstance(v, dict):
        return None
    return (v.get("email_address") or v.get("value") or "").strip() or None


def _primary_location_dict(values: list) -> Optional[dict]:
    if not values or not isinstance(values, list):
        return None
    v = values[0] if values else None
    if not isinstance(v, dict):
        return None
    return v


def list_attio_lists(api_key: str, include_counts: bool = False) -> List[Dict[str, Any]]:
    """List all Attio lists. Returns list of {list_id, name, parent_object, entry_count}.
    When include_counts=True, fetches entry count for each list (one extra API call per list)."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(f"{ATTIO_API_BASE}/lists", headers=headers)
        resp.raise_for_status()
        data = resp.json()
        out = []
        for item in data.get("data", []):
            id_block = item.get("id") or {}
            list_id = id_block.get("list_id")
            parent = item.get("parent_object") or item.get("parent_objects") or []
            if isinstance(parent, list) and parent:
                obj = parent[0] if isinstance(parent[0], str) else (parent[0].get("slug") or parent[0].get("api_slug") if isinstance(parent[0], dict) else str(parent[0]))
            elif isinstance(parent, str):
                obj = parent
            else:
                obj = None
            if list_id:
                entry = {
                    "list_id": list_id,
                    "name": item.get("name") or item.get("api_slug") or list_id,
                    "parent_object": obj,
                    "entry_count": None,
                }
                if include_counts:
                    try:
                        count_resp = client.post(
                            f"{ATTIO_API_BASE}/lists/{list_id}/entries/query",
                            headers=headers,
                            json={"limit": 1, "offset": 0},
                        )
                        if count_resp.status_code == 200:
                            count_data = count_resp.json()
                            total = count_data.get("total_count")
                            if total is not None:
                                entry["entry_count"] = total
                            else:
                                entries = count_data.get("data", [])
                                entry["entry_count"] = len(entries) if len(entries) == 0 else "1+"
                    except Exception as e:
                        logger.debug("Could not fetch count for list %s: %s", list_id, e)
                out.append(entry)
    return out


def get_attio_list_record_ids(api_key: str, list_id: str, limit: int = 500) -> List[str]:
    """Fetch all parent record IDs from a list (paginated)."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    record_ids: List[str] = []
    offset = 0
    page_size = 50
    with httpx.Client(timeout=30.0) as client:
        while True:
            payload = {"limit": page_size, "offset": offset}
            resp = client.post(
                f"{ATTIO_API_BASE}/lists/{list_id}/entries/query",
                headers=headers,
                json=payload,
            )
            if resp.status_code != 200:
                logger.warning("Attio list entries error %s: %s", resp.status_code, resp.text[:300])
                break
            data = resp.json()
            entries = data.get("data", [])
            for entry in entries:
                rid = entry.get("parent_record_id")
                if rid:
                    record_ids.append(rid)
            if len(entries) < page_size or len(record_ids) >= limit:
                break
            offset += page_size
    return record_ids[:limit]


def get_attio_record(api_key: str, object_slug: str, record_id: str) -> Optional[Dict[str, Any]]:
    """Fetch a single company or person record by id. Returns raw API record (id, values)."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    with httpx.Client(timeout=30.0) as client:
        resp = client.get(
            f"{ATTIO_API_BASE}/objects/{object_slug}/records/{record_id}",
            headers=headers,
        )
        if resp.status_code != 200:
            logger.debug("Attio get record %s %s: %s", object_slug, record_id, resp.status_code)
            return None
    return resp.json().get("data")


# Max IDs per query (Attio accepts record_id $in; keep batch size conservative)
_ATTIO_BATCH_QUERY_SIZE = 50


def get_attio_records_batch(
    api_key: str, object_slug: str, record_ids: List[str]
) -> List[Dict[str, Any]]:
    """
    Fetch multiple records by id in one query. Uses POST .../records/query with filter record_id $in.
    Returns list of raw API records (id, values). Skips empty record_ids.
    """
    if not record_ids or not api_key:
        return []
    ids = [r for r in record_ids if r and isinstance(r, str)]
    if not ids:
        return []
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "filter": {"record_id": {"$in": ids}},
        "limit": len(ids),
    }
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            f"{ATTIO_API_BASE}/objects/{object_slug}/records/query",
            headers=headers,
            json=payload,
        )
    if resp.status_code != 200:
        logger.warning(
            "Attio batch query %s failed %s: %s",
            object_slug,
            resp.status_code,
            resp.text[:300],
        )
        return []
    data = resp.json()
    return data.get("data") or []


def _attio_company_to_lead(record: Dict[str, Any]) -> Dict[str, Any]:
    """Map Attio company record to lead row for DB insert."""
    values = record.get("values") or {}
    record_id = (record.get("id") or {}).get("record_id", "")
    loc = _primary_location_dict(values.get("primary_location") or [])
    lead: Dict[str, Any] = {
        "name": _first_text(values.get("name") or []) or "Unknown",
        "domains": _first_text(values.get("domains") or []),
        "description": _first_text(values.get("description") or []),
        "office_phone": _first_phone(values.get("office_phone") or []),
        "office_email": _first_email(values.get("office_email") or []),
        "segment": _first_text(values.get("segment") or []),
        "linkedin": _first_text(values.get("linkedin") or []),
        "facebook": _first_text(values.get("facebook") or []),
        "lead_source": "attio_list",
        "status": "pending_review",
        "attio_status": "existing",
        "attio_record_id": record_id or None,
    }
    if loc:
        lead["primary_location_line_1"] = loc.get("line_1") or ""
        lead["primary_location_locality"] = loc.get("locality") or ""
        lead["primary_location_region"] = loc.get("region") or ""
        lead["primary_location_postcode"] = loc.get("postcode") or ""
    return lead


def _attio_person_to_lead(
    record: Dict[str, Any],
    company_record: Optional[Dict[str, Any]],
    api_key: str,
) -> Dict[str, Any]:
    """Map Attio person record (and optional linked company) to lead row."""
    values = record.get("values") or {}
    person_id = (record.get("id") or {}).get("record_id", "")
    name_arr = values.get("name") or []
    first_name = ""
    last_name = ""
    if name_arr and isinstance(name_arr[0], dict):
        first_name = (name_arr[0].get("first_name") or "").strip()
        last_name = (name_arr[0].get("last_name") or "").strip()
    full_name = f"{first_name} {last_name}".strip() or _first_text(values.get("name") or []) or "Unknown"
    company_name = "Unknown"
    domains = None
    company_record_id = None
    if company_record:
        cvals = company_record.get("values") or {}
        company_name = _first_text(cvals.get("name") or []) or "Unknown"
        domains = _first_text(cvals.get("domains") or [])
        company_record_id = (company_record.get("id") or {}).get("record_id")
    lead: Dict[str, Any] = {
        "name": company_name,
        "domains": domains,
        "office_phone": _first_phone(values.get("phone_numbers") or []),
        "office_email": _first_email(values.get("email_addresses") or []),
        "lead_source": "attio_list",
        "status": "pending_review",
        "attio_status": "existing",
        "attio_record_id": company_record_id,
        "attio_person_id": person_id,
    }
    if full_name and full_name != "Unknown":
        lead["description"] = f"Contact: {full_name}"
    return lead


def ingest_attio_list_into_db(
    api_key: str,
    list_id: str,
    db: LeadDB,
    list_info: Optional[Dict[str, Any]] = None,
    limit: int = 500,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Tuple[int, int, int]:
    """
    Ingest an Attio list into SQLite as leads (pending_review) for enrichment.
    Skips records already present (by attio_record_id for companies, attio_person_id for people).
    progress_callback(processed, total, message) is called periodically for UI updates.
    Returns (inserted_count, skip_duplicate_count, total_entries_fetched).
    """
    lists_by_id = {l["list_id"]: l for l in list_attio_lists(api_key)}
    info = list_info or lists_by_id.get(list_id)
    if not info:
        info = {"list_id": list_id, "name": list_id, "parent_object": "companies"}
    parent_object = info.get("parent_object") or "companies"
    if isinstance(parent_object, list):
        parent_object = parent_object[0] if parent_object else "companies"

    record_ids = get_attio_list_record_ids(api_key, list_id, limit=limit)
    total = len(record_ids)
    if progress_callback:
        progress_callback(0, total, "Fetching list entries..." if total else "No entries in list.")
    if not record_ids:
        return 0, 0, 0

    inserted = 0
    skipped = 0
    processed = 0

    if parent_object == "companies":
        for offset in range(0, len(record_ids), _ATTIO_BATCH_QUERY_SIZE):
            chunk = record_ids[offset : offset + _ATTIO_BATCH_QUERY_SIZE]
            records = get_attio_records_batch(api_key, parent_object, chunk)
            if progress_callback:
                processed += len(chunk)
                progress_callback(min(processed, total), total, f"Importing record {processed}/{total}...")
            for record in records:
                lead_row = _attio_company_to_lead(record)
                if lead_row.get("attio_record_id") and db.get_lead_by_attio_record_id(lead_row["attio_record_id"]):
                    skipped += 1
                    continue
                try:
                    db.insert_lead(lead_row)
                    inserted += 1
                except Exception as e:
                    logger.warning("Insert lead from Attio failed: %s", e)
    else:
        # People: batch fetch people, then batch fetch their companies
        for offset in range(0, len(record_ids), _ATTIO_BATCH_QUERY_SIZE):
            chunk = record_ids[offset : offset + _ATTIO_BATCH_QUERY_SIZE]
            people_records = get_attio_records_batch(api_key, parent_object, chunk)
            if progress_callback:
                processed += len(chunk)
                progress_callback(min(processed, total), total, f"Importing record {processed}/{total}...")
            company_ids: List[str] = []
            for rec in people_records:
                refs = (rec.get("values") or {}).get("company") or []
                if refs and isinstance(refs[0], dict):
                    ref_id = refs[0].get("referenced_record_id") or refs[0].get("record_id")
                    if ref_id:
                        company_ids.append(ref_id)
            company_map: Dict[str, Dict[str, Any]] = {}
            if company_ids:
                unique_ids = list(dict.fromkeys(company_ids))
                for c_offset in range(0, len(unique_ids), _ATTIO_BATCH_QUERY_SIZE):
                    c_chunk = unique_ids[c_offset : c_offset + _ATTIO_BATCH_QUERY_SIZE]
                    for c_rec in get_attio_records_batch(api_key, "companies", c_chunk):
                        rid = (c_rec.get("id") or {}).get("record_id")
                        if rid:
                            company_map[rid] = c_rec
            for record in people_records:
                company_record = None
                refs = (record.get("values") or {}).get("company") or []
                if refs and isinstance(refs[0], dict):
                    ref_id = refs[0].get("referenced_record_id") or refs[0].get("record_id")
                    if ref_id:
                        company_record = company_map.get(ref_id)
                lead_row = _attio_person_to_lead(record, company_record, api_key)
                if lead_row.get("attio_person_id") and db.get_lead_by_attio_person_id(lead_row["attio_person_id"]):
                    skipped += 1
                    continue
                try:
                    db.insert_lead(lead_row)
                    inserted += 1
                except Exception as e:
                    logger.warning("Insert lead from Attio failed: %s", e)

    if progress_callback:
        progress_callback(total, total, "Done.")
    return inserted, skipped, total


def get_company_diff(
    lead: dict,
    api_key: str,
    field_mapping: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Dict[str, str]]]:
    """
    For a lead with attio_record_id, fetch current Attio company and return
    {"current": {field: value}, "new": {field: value}} for diff display.
    Only includes fields that are enabled in the mapping. Returns None if fetch fails.
    """
    record_id = lead.get("attio_record_id")
    if not record_id or not api_key:
        return None
    record = get_attio_record(api_key, "companies", record_id)
    if not record:
        return None

    if field_mapping is None:
        from lib.filters_config import get_attio_sync_mapping
        field_mapping = get_attio_sync_mapping()

    enabled = [m for m in field_mapping if m.get("enabled", True)]
    values = record.get("values") or {}
    current_flat: Dict[str, str] = {}
    new_flat: Dict[str, str] = {}

    for m in enabled:
        lf = m["lead_field"]
        attr = m["attio_attribute"]

        if lf == "primary_location":
            loc = _primary_location_dict(values.get(attr) or [])
            cur_parts = []
            if loc:
                for k in ("line_1", "locality", "region", "postcode"):
                    p = (loc.get(k) or "").strip()
                    if p:
                        cur_parts.append(p)
            current_flat[lf] = ", ".join(cur_parts)
            new_parts = [
                (lead.get("primary_location_line_1") or "").strip(),
                (lead.get("primary_location_locality") or "").strip(),
                (lead.get("primary_location_region") or "").strip(),
                (lead.get("primary_location_postcode") or "").strip(),
            ]
            new_flat[lf] = ", ".join(p for p in new_parts if p)
        elif lf == "office_phone":
            current_flat[lf] = _first_phone(values.get(attr) or []) or ""
            new_flat[lf] = (lead.get(lf) or "").strip()
        elif lf == "office_email":
            current_flat[lf] = _first_email(values.get(attr) or []) or ""
            new_flat[lf] = (lead.get(lf) or "").strip()
        elif lf == "description":
            current_flat[lf] = _first_text(values.get(attr) or []) or ""
            new_flat[lf] = ((lead.get("edited_description") or "").strip()
                            or (lead.get("description") or "").strip())
        elif lf == "areas_of_accountancy":
            current_flat[lf] = _first_text(values.get(attr) or []) or ""
            raw_areas = lead.get(lf)
            if isinstance(raw_areas, list):
                new_flat[lf] = ", ".join(str(a) for a in raw_areas if a)
            else:
                new_flat[lf] = (str(raw_areas) if raw_areas else "").strip()
        else:
            current_flat[lf] = _first_text(values.get(attr) or []) or ""
            new_flat[lf] = (lead.get(lf) or "").strip() if isinstance(lead.get(lf), str) else str(lead.get(lf, ""))

    return {"current": current_flat, "new": new_flat}


def _person_name_from_attio(values: list) -> str:
    """Extract 'First Last' from Attio name attribute values."""
    if not values or not isinstance(values, list):
        return ""
    v = values[0] if values else None
    if not isinstance(v, dict):
        return ""
    first = (v.get("first_name") or "").strip()
    last = (v.get("last_name") or "").strip()
    return f"{first} {last}".strip()


def get_person_diff(
    lead: dict,
    api_key: str,
    field_mapping: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Dict[str, str]]]:
    """
    For a lead with attio_person_id, fetch current Attio person and return
    {"current": {field: value}, "new": {field: value}} for diff display.
    Only includes fields that are enabled in the mapping. Returns None if fetch fails.
    """
    record_id = lead.get("attio_person_id")
    if not record_id or not api_key:
        return None
    record = get_attio_record(api_key, "people", record_id)
    if not record:
        return None

    if field_mapping is None:
        from lib.filters_config import get_attio_people_sync_mapping
        field_mapping = get_attio_people_sync_mapping()

    enabled = [m for m in field_mapping if m.get("enabled", True)]
    values = record.get("values") or {}
    current_flat: Dict[str, str] = {}
    new_flat: Dict[str, str] = {}

    for m in enabled:
        lf = m["lead_field"]
        attr = m["attio_attribute"]

        if attr == "name":
            current_flat[lf] = _person_name_from_attio(values.get(attr) or [])
            raw = (lead.get(lf) or "").strip()
            new_flat[lf] = raw
        elif lf == "office_phone":
            current_flat[lf] = _first_phone(values.get(attr) or []) or ""
            new_flat[lf] = (lead.get(lf) or "").strip()
        elif lf == "office_email":
            current_flat[lf] = _first_email(values.get(attr) or []) or ""
            new_flat[lf] = (lead.get(lf) or "").strip()
        elif lf == "description":
            current_flat[lf] = _first_text(values.get(attr) or []) or ""
            new_flat[lf] = ((lead.get("edited_description") or "").strip()
                            or (lead.get("description") or "").strip())
        else:
            current_flat[lf] = _first_text(values.get(attr) or []) or ""
            new_flat[lf] = (lead.get(lf) or "").strip() if isinstance(lead.get(lf), str) else str(lead.get(lf, ""))

    return {"current": current_flat, "new": new_flat}
