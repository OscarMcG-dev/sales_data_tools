"""
CSV import into leads table with column mapping per lead_source.
"""
import json
import logging
from io import StringIO
from typing import Optional

import pandas as pd

from lib.db import LeadDB
from lib.phone_utils import normalize_to_e164
from lib.url_utils import extract_domain

logger = logging.getLogger(__name__)

# CSV column name (case-insensitive match) -> lead field name
B2B_COLUMN_MAP = {
    "company": "name",
    "website": "domains",
    "phone": "office_phone",
    "email": "office_email",
    "first name": "dm_first_name",
    "last name": "dm_last_name",
    "title": "dm_title",
    "linkedin": "linkedin",
    "description": "description",
    "address": "street_address",
}

OUTSCRAPER_COLUMN_MAP = {
    "name": "name",
    "site": "domains",
    "phone": "office_phone",
    "email": "office_email",
    "full_address": "street_address",
    "description": "description",
}

ATTIO_EXPORT_COLUMN_MAP = {
    "name": "name",
    "company": "name",
    "domains": "domains",
    "website": "domains",
    "phone": "office_phone",
    "email": "office_email",
    "description": "description",
}

DEFAULT_MAPS = {
    "b2b_data": B2B_COLUMN_MAP,
    "outscraper": OUTSCRAPER_COLUMN_MAP,
    "attio_export": ATTIO_EXPORT_COLUMN_MAP,
    "directory": B2B_COLUMN_MAP,
}


def _normalize_column_map(custom: Optional[dict]) -> dict:
    """Return map from lowercase CSV column name -> lead field name."""
    out = {}
    for csv_col, field in (custom or {}).items():
        out[str(csv_col).strip().lower()] = field
    return out


def _row_to_lead(
    row: dict,
    cols_lower_to_field: dict,
    lead_source: str,
) -> dict:
    """Map a CSV row to a lead dict for insert_lead."""
    lead = {
        "lead_source": lead_source,
        "status": "pending_review",
    }
    for col_lower, field in cols_lower_to_field.items():
        raw = row.get(col_lower)
        if raw is None:
            for k, v in row.items():
                if k and str(k).strip().lower() == col_lower:
                    raw = v
                    break
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        val = str(raw).strip()
        if val.lower() == "nan" or not val:
            continue

        if field == "office_phone":
            val = normalize_to_e164(val) or val
        elif field == "domains":
            val = extract_domain(val) or val

        if field in ("dm_first_name", "dm_last_name", "dm_title"):
            if "decision_makers" not in lead:
                lead["decision_makers"] = [{"name": "", "title": "", "email": "", "phone": ""}]
            dm = lead["decision_makers"][0]
            if field == "dm_first_name":
                dm["name"] = (dm.get("name") or "").strip() + " " + val
            elif field == "dm_last_name":
                dm["name"] = ((dm.get("name") or "").strip() + " " + val).strip()
            elif field == "dm_title":
                dm["title"] = val
            continue

        lead[field] = val

    if "decision_makers" in lead and isinstance(lead["decision_makers"], list):
        lead["decision_makers"] = json.dumps(lead["decision_makers"])

    if not lead.get("name"):
        lead["name"] = "Unknown"
    return lead


def import_csv(
    file,
    db: LeadDB,
    lead_source: str,
    column_mapping: Optional[dict] = None,
    encoding: str = "utf-8",
) -> int:
    """
    Read CSV from file-like object, map columns, insert leads.
    Returns count of rows imported.
    """
    default_map = DEFAULT_MAPS.get(lead_source, B2B_COLUMN_MAP)
    custom = _normalize_column_map(column_mapping) or default_map

    if hasattr(file, "seek"):
        file.seek(0)
    try:
        df = pd.read_csv(file, encoding=encoding, dtype=str)
    except Exception as e:
        logger.warning("Try latin-1 encoding: %s", e)
        if hasattr(file, "seek"):
            file.seek(0)
        df = pd.read_csv(file, encoding="latin-1", dtype=str)

    # Build map: CSV column name (as in df) -> lead field
    cols_lower_to_field = {}
    for csv_col in df.columns:
        col_lower = str(csv_col).strip().lower()
        if col_lower in custom:
            cols_lower_to_field[col_lower] = custom[col_lower]
        elif csv_col in custom:
            cols_lower_to_field[col_lower] = custom[csv_col]

    if not cols_lower_to_field:
        cols_lower_to_field = dict(default_map)

    count = 0
    for _, row in df.iterrows():
        row_dict = {str(k).strip().lower(): v for k, v in row.items()}
        lead = _row_to_lead(row_dict, cols_lower_to_field, lead_source)
        if not lead.get("name") or lead["name"] == "Unknown":
            if not any(lead.get(k) for k in ("domains", "office_phone", "office_email")):
                continue
        db.insert_lead(lead)
        count += 1

    logger.info("Imported %d rows from CSV (lead_source=%s)", count, lead_source)
    return count


def preview_mapped_rows(
    df: pd.DataFrame,
    lead_source: str,
    column_mapping: Optional[dict] = None,
    nrows: int = 5,
) -> list[dict]:
    """Build lead dicts for the first nrows of df using the same mapping as import_csv. For UI preview."""
    default_map = DEFAULT_MAPS.get(lead_source, B2B_COLUMN_MAP)
    custom = _normalize_column_map(column_mapping) or default_map
    cols_lower_to_field = {}
    for csv_col in df.columns:
        col_lower = str(csv_col).strip().lower()
        if col_lower in custom:
            cols_lower_to_field[col_lower] = custom[col_lower]
        elif csv_col in custom:
            cols_lower_to_field[col_lower] = custom[csv_col]
    if not cols_lower_to_field:
        cols_lower_to_field = dict(default_map)
    out = []
    for _, row in df.head(nrows).iterrows():
        row_dict = {str(k).strip().lower(): v for k, v in row.items()}
        lead = _row_to_lead(row_dict, cols_lower_to_field, lead_source)
        out.append(lead)
    return out
