"""
Lead cleaning: keyword-based and LLM-based scope/review flagging.
Backend only; no UI. Uses flag_reason / flag_source and statuses flagged_keyword, flagged_llm, excluded.
"""
import json
import logging
from typing import Any, Optional

from lib.config import Settings
from lib.db import LeadDB

logger = logging.getLogger(__name__)

ALLOWED_MATCH_FIELDS = frozenset({"name", "description"})

SCOPE_REVIEW_SYSTEM = """You classify whether a lead (business) is in scope for our campaign.

Segment: Accounting firms in Australia (AU). We want firms that offer accounting services to businesses or individuals.

Exclude:
- Sole traders with no employees (single practitioner only).
- Non-accounting businesses (e.g. pure legal, real estate, insurance, financial planning only).
- Businesses that are clearly not in Australia.
- Listings that are aggregators or directories, not actual firms.

Needs review: Unclear from the name/description (e.g. could be accounting or could be bookkeeping-only; location unclear).

Respond with exactly one JSON object: {"label": "in_scope" | "out_of_scope" | "needs_review", "reason": "brief explanation"}."""


def keyword_clean(
    db: LeadDB,
    keywords: list[str],
    match_fields: list[str],
    statuses: list[str],
    action: str,
    lead_source: Optional[str] = None,
    limit: int = 5000,
) -> list[dict]:
    """
    Match leads by keywords (case-insensitive substring) in name/description, then flag or exclude.
    Always scoped: by statuses and optionally by lead_source and limit.

    keywords: list of strings to match.
    match_fields: subset of ["name", "description"].
    statuses: which lead statuses to scan (e.g. ["pending_review", "enriched"]).
    action: "flag" -> status=flagged_keyword; "exclude" -> status=excluded. Sets flag_reason=matched keyword, flag_source=keyword.
    lead_source: optional filter (e.g. "directory", "b2b_data"); None = all sources.
    limit: max leads to scan (default 5000).

    Returns list of {"lead_id": int, "name": str, "matched_keyword": str, "matched_field": str}.
    """
    if not keywords or not statuses:
        return []
    fields = [f for f in match_fields if f in ALLOWED_MATCH_FIELDS]
    if not fields:
        return []
    if action not in ("flag", "exclude"):
        return []

    status_to_set = "flagged_keyword" if action == "flag" else "excluded"
    leads = db.get_leads_by_statuses(statuses, lead_source=lead_source, limit=limit)
    matches: list[dict] = []

    for lead in leads:
        lead_id = lead.get("id")
        if lead_id is None:
            continue
        for field in fields:
            value = (lead.get(field) or "") if isinstance(lead.get(field), str) else ""
            value_lower = value.lower()
            for kw in keywords:
                if kw and kw.lower() in value_lower:
                    matches.append({
                        "lead_id": lead_id,
                        "name": (lead.get("name") or "").strip(),
                        "matched_keyword": kw,
                        "matched_field": field,
                    })
                    break
            else:
                continue
            break

    # Batch by (status, flag_reason, flag_source) then bulk update; or update per lead (each can have different reason).
    for m in matches:
        db.bulk_update_status(
            [m["lead_id"]],
            status_to_set,
            extra={"flag_reason": m["matched_keyword"], "flag_source": "keyword"},
        )

    return matches


def llm_scope_review(
    db: LeadDB,
    statuses: list[str],
    limit: int,
    action: str,
    settings: Settings,
    lead_source: Optional[str] = None,
    scope_system_prompt: Optional[str] = None,
) -> list[dict]:
    """
    Classify leads with an LLM (in_scope | out_of_scope | needs_review + reason), then flag or exclude.
    Always scoped: by statuses, limit, and optionally lead_source.

    statuses: which lead statuses to process.
    limit: max leads to process.
    action: "flag" -> out_of_scope and needs_review get status=flagged_llm; "exclude" -> out_of_scope get excluded, needs_review get flagged_llm.
    settings: used for OpenRouter (openrouter_api_key, openrouter_base_url, openrouter_model, llm_temperature).
    lead_source: optional filter (e.g. "directory", "b2b_data"); None = all sources.

    Returns list of {"lead_id": int, "name": str, "label": str, "reason": str} for processed leads.
    """
    if not statuses or action not in ("flag", "exclude"):
        return []
    if not settings.openrouter_api_key:
        logger.warning("OpenRouter API key not set; skipping llm_scope_review")
        return []

    from openai import OpenAI

    client = OpenAI(
        api_key=settings.openrouter_api_key,
        base_url=settings.openrouter_base_url,
    )
    leads = db.get_leads_by_statuses(statuses, lead_source=lead_source, limit=limit)
    results: list[dict] = []

    for lead in leads:
        lead_id = lead.get("id")
        if lead_id is None:
            continue
        name = (lead.get("name") or "").strip()
        desc = (lead.get("description") or lead.get("edited_description") or "").strip()
        text = f"Name: {name}\nDescription: {desc}" if desc else f"Name: {name}"
        system_prompt = (scope_system_prompt or SCOPE_REVIEW_SYSTEM).strip()

        try:
            response = client.chat.completions.create(
                model=settings.openrouter_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": text},
                ],
                temperature=settings.llm_temperature,
                max_tokens=300,
            )
        except Exception as e:
            logger.warning("LLM call failed for lead %s: %s", lead_id, e)
            continue

        content = (response.choices[0].message.content or "").strip()
        label = "in_scope"
        reason = ""
        try:
            # Allow markdown code block
            if "```" in content:
                content = content.split("```")[1]
                if content.startswith("json"):
                    content = content[4:].strip()
            obj = json.loads(content)
            label = (obj.get("label") or "in_scope").strip().lower()
            if label not in ("in_scope", "out_of_scope", "needs_review"):
                label = "needs_review"
            reason = (obj.get("reason") or "")[:500]
        except (json.JSONDecodeError, TypeError):
            label = "needs_review"
            reason = "LLM response parse error"

        results.append({"lead_id": lead_id, "name": name, "label": label, "reason": reason})

        if label == "in_scope":
            continue
        if action == "flag":
            status_to_set = "flagged_llm"
            db.bulk_update_status(
                [lead_id],
                status_to_set,
                extra={"flag_reason": reason, "flag_source": "llm"},
            )
        else:
            if label == "out_of_scope":
                db.bulk_update_status(
                    [lead_id],
                    "excluded",
                    extra={"flag_reason": reason, "flag_source": "llm"},
                )
            else:
                db.bulk_update_status(
                    [lead_id],
                    "flagged_llm",
                    extra={"flag_reason": reason, "flag_source": "llm"},
                )

    return results


if __name__ == "__main__":
    # Inline test: keyword matching against a mock list (no real DB required for basic logic check).
    mock_leads = [
        {"id": 1, "name": "Acme Tax Solutions", "description": "We do bookkeeping and payroll."},
        {"id": 2, "name": "Real Estate Partners", "description": "Property management and sales."},
        {"id": 3, "name": "Smith & Co", "description": "Accounting and audit services."},
    ]
    keywords = ["real estate", "bookkeeping"]
    match_fields = ["name", "description"]
    found = []
    for lead in mock_leads:
        for field in match_fields:
            if field not in ALLOWED_MATCH_FIELDS:
                continue
            value = (lead.get(field) or "").lower()
            for kw in keywords:
                if kw.lower() in value:
                    found.append({
                        "lead_id": lead["id"],
                        "matched_keyword": kw,
                        "matched_field": field,
                    })
                    break
            else:
                continue
            break
    assert len(found) == 2
    assert any(m["lead_id"] == 1 and m["matched_keyword"] == "bookkeeping" for m in found)
    assert any(m["lead_id"] == 2 and "real estate" in m["matched_keyword"].lower() for m in found)
    print("Keyword matching test passed:", found)
