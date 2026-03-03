"""
SQLite-aware enrichment: get pending_review leads, run WebsiteEnricher, update DB.

Building blocks: scope-only re-assessment via extract_from_text(..., schema="scope_only")
(no crawl). Future: "refresh from web" = crawl_single_url + extract_from_text(schema="enrichment").
"""
import asyncio
import json
import logging
from typing import Callable, List, Optional

from lib.config import Settings
from lib.db import LeadDB
from lib.models import EnrichmentData

logger = logging.getLogger(__name__)


async def reassess_scope_async(
    settings: Settings,
    description: str,
    name: str,
) -> dict:
    """
    Re-assess whether a lead is in scope using existing description text only (no crawl).

    Returns dict with keys: in_scope (bool), reason (str), segment (str).
    Can be wired into UI later as a button on selected leads.
    """
    from scraper.website_enricher import extract_from_text

    return await extract_from_text(
        text=description or "",
        name=name or "Unknown",
        schema="scope_only",
        settings=settings,
    )


def reassess_scope(
    settings: Settings,
    description: str,
    name: str,
) -> dict:
    """Sync wrapper for reassess_scope_async."""
    return asyncio.run(reassess_scope_async(settings, description, name))


def _enrichment_to_lead_update(enrichment: EnrichmentData) -> dict:
    """Map EnrichmentData to lead row update dict (for db.update_lead). Uses filters_config enrichment_output_mapping when present."""
    decision_makers = [
        {
            "name": dm.name,
            "title": dm.title,
            "summary": getattr(dm, "summary", "") or "",
            "phone_office": dm.phone_office,
            "phone_mobile": dm.phone_mobile,
            "phone_direct": dm.phone_direct,
            "email": dm.email,
            "linkedin": dm.linkedin,
        }
        for dm in (enrichment.decision_makers or [])
    ]
    default_update = {
        "description": enrichment.description or "",
        "edited_description": enrichment.edited_description or "",
        "office_phone": enrichment.office_phone,
        "office_email": enrichment.office_email,
        "decision_makers": json.dumps(decision_makers) if decision_makers else None,
        "associated_emails": json.dumps(enrichment.associated_emails) if enrichment.associated_emails else None,
        "associated_mobiles": json.dumps(enrichment.associated_mobiles) if enrichment.associated_mobiles else None,
        "associated_info": enrichment.associated_info or "",
        "organisational_structure": enrichment.organisational_structure or "",
        "linkedin": enrichment.linkedin,
        "facebook": enrichment.facebook,
        "confidence_score": enrichment.confidence_score,
        "out_of_scope": 1 if enrichment.out_of_scope else 0,
        "out_of_scope_reason": enrichment.out_of_scope_reason or "",
        "status": "enriched",
    }
    try:
        from lib.filters_config import get_enrichment_output_mapping
        mapping = get_enrichment_output_mapping()
    except Exception:
        mapping = []
    if not mapping:
        return default_update
    update = {"status": "enriched"}
    json_columns = {"decision_makers", "associated_emails", "associated_mobiles"}
    for m in mapping:
        ef = m.get("enrichment_field")
        col = m.get("db_column")
        if not ef or not col:
            continue
        val = getattr(enrichment, ef, None)
        if val is None and ef in default_update:
            val = default_update[ef]
        if col == "out_of_scope" and isinstance(val, bool):
            val = 1 if val else 0
        if col in json_columns and isinstance(val, (list, dict)):
            val = json.dumps(val) if val else None
        update[col] = val
    return update


def _default_lead_update_keys() -> List[tuple]:
    """Return (enrichment_field, db_column) pairs used when no mapping config is set."""
    return [
        ("description", "description"),
        ("edited_description", "edited_description"),
        ("office_phone", "office_phone"),
        ("office_email", "office_email"),
        ("decision_makers", "decision_makers"),
        ("associated_emails", "associated_emails"),
        ("associated_mobiles", "associated_mobiles"),
        ("associated_info", "associated_info"),
        ("organisational_structure", "organisational_structure"),
        ("linkedin", "linkedin"),
        ("facebook", "facebook"),
        ("confidence_score", "confidence_score"),
        ("out_of_scope", "out_of_scope"),
        ("out_of_scope_reason", "out_of_scope_reason"),
    ]


async def run_enrichment_async(
    db: LeadDB,
    settings: Settings,
    lead_source: Optional[str] = None,
    limit: int = 50,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    log_callback: Optional[Callable[[str, str], None]] = None,
) -> int:
    """
    Get leads with status=pending_review, enrich each: crawl+LLM when website URL is present (any source),
    else mark b2b_data as enriched without crawl. Other sources with no URL are skipped.
    Returns count of leads enriched.
    log_callback(level, message) is called for progress and skip reasons.
    """
    from scraper.website_enricher import WebsiteEnricher

    def log(level: str, msg: str) -> None:
        if level == "info":
            logger.info("%s", msg)
        else:
            logger.debug("%s: %s", level, msg)
        if log_callback:
            log_callback(level, msg)

    leads = db.get_leads(status="pending_review", lead_source=lead_source, limit=limit)
    if not leads:
        log("info", "No leads with status=pending_review (lead_source=%s). Nothing to enrich." % (lead_source or "any"))
        return 0

    log("info", "Found %d leads with status=pending_review (lead_source=%s). Starting crawler pool." % (len(leads), lead_source or "any"))
    enricher = WebsiteEnricher(settings)
    try:
        await enricher.start_pool(size=min(4, settings.max_concurrent_crawls))
    except Exception as e:
        logger.warning("Could not start crawler pool, continuing without: %s", e)
        log("info", "Crawler pool failed to start: %s. Continuing with single crawler." % e)

    enriched_count = 0
    total = len(leads)
    for i, lead in enumerate(leads):
        lead_id = lead.get("id")
        name = lead.get("name") or "Unknown"
        website_url = lead.get("website_url") or lead.get("domains") or ""
        src = lead.get("lead_source") or "(none)"
        if progress_callback:
            progress_callback(i + 1, total, name)

        if not website_url:
            log("skip", "Lead id=%s (%s): skipped — no website_url or domains." % (lead_id, name))
            if src == "b2b_data":
                db.update_lead(lead_id, {"status": "enriched"})
                enriched_count += 1
                log("info", "Marked b2b_data lead id=%s as enriched (no crawl)." % lead_id)
            continue

        if not website_url.startswith("http"):
            website_url = "https://" + website_url

        log("info", "Enriching %s/%s: %s (%s)" % (i + 1, total, name, website_url))
        try:
            enrichment = await enricher.enrich(website_url, name)
            if enrichment:
                update = _enrichment_to_lead_update(enrichment)
                db.update_lead(lead_id, update)
                enriched_count += 1
                log("info", "  -> enriched (id=%s)." % lead_id)
            else:
                db.update_lead(lead_id, {"status": "enriched"})
                enriched_count += 1
                log("info", "  -> marked enriched, no extraction (id=%s)." % lead_id)
        except Exception as e:
            logger.exception("Enrichment failed for lead id=%s: %s", lead_id, e)
            log("info", "  -> error: %s" % e)

        await asyncio.sleep(2.0)

    try:
        await enricher.stop_pool()
    except Exception:
        pass

    return enriched_count


def run_enrichment(
    db: LeadDB,
    settings: Settings,
    lead_source: Optional[str] = None,
    limit: int = 50,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
    log_callback: Optional[Callable[[str, str], None]] = None,
) -> int:
    """Sync wrapper for run_enrichment_async."""
    return asyncio.run(
        run_enrichment_async(db, settings, lead_source, limit, progress_callback, log_callback)
    )
