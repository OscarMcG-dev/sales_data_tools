"""
Load filters and push-target config from data/config/filters_config.json.
Used by dedup (company/people match rules), cleaning (keyword, LLM scope), and enrichment output mapping.
Falls back to hardcoded defaults if file is missing or invalid.
"""
import json
import logging
import os
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

# Defaults used when config file is missing
DEFAULT_DEDUP = {
    "company_rules": [
        {"lead_field": "domains", "attio_attribute": "domains"},
        {"lead_field": "office_phone", "attio_attribute": "office_phone"},
    ],
    "people_rules": [
        {"lead_field": "office_email", "attio_attribute": "email_addresses"},
    ],
}

DEFAULT_KEYWORD_CLEAN = {
    "on_flag": {
        "status": "flagged_keyword",
        "flag_reason_column": "flag_reason",
        "flag_source_column": "flag_source",
        "flag_source_value": "keyword",
    },
    "on_exclude": {
        "status": "excluded",
        "flag_reason_column": "flag_reason",
        "flag_source_column": "flag_source",
        "flag_source_value": "keyword",
    },
}

DEFAULT_ATTIO_SYNC_MAPPING: List[Dict[str, Any]] = [
    {"lead_field": "name", "attio_attribute": "name", "enabled": True},
    {"lead_field": "domains", "attio_attribute": "domains", "enabled": True},
    {"lead_field": "description", "attio_attribute": "description", "enabled": True},
    {"lead_field": "office_phone", "attio_attribute": "office_phone", "enabled": True},
    {"lead_field": "office_email", "attio_attribute": "office_email", "enabled": False},
    {"lead_field": "segment", "attio_attribute": "segment", "enabled": True},
    {"lead_field": "primary_location", "attio_attribute": "primary_location", "enabled": True},
    {"lead_field": "linkedin", "attio_attribute": "linkedin", "enabled": False},
    {"lead_field": "facebook", "attio_attribute": "facebook", "enabled": False},
    {"lead_field": "organisational_structure", "attio_attribute": "organisational_structure", "enabled": False},
    {"lead_field": "areas_of_accountancy", "attio_attribute": "areas_of_accountancy", "enabled": False},
]

# Lead → Attio People attribute mapping (for campaign lists syncing to People object)
DEFAULT_ATTIO_PEOPLE_SYNC_MAPPING: List[Dict[str, Any]] = [
    {"lead_field": "name", "attio_attribute": "name", "enabled": True},
    {"lead_field": "office_email", "attio_attribute": "email_addresses", "enabled": True},
    {"lead_field": "office_phone", "attio_attribute": "phone_numbers", "enabled": True},
    {"lead_field": "description", "attio_attribute": "job_title", "enabled": True},
]

DEFAULT_LLM_SCOPE = {
    "input_fields": ["name", "description"],
    "edited_description_fallback": True,
    "on_out_of_scope": {
        "status": "excluded",
        "flag_reason_column": "flag_reason",
        "flag_source_column": "flag_source",
        "flag_source_value": "llm",
    },
    "on_needs_review": {
        "status": "flagged_llm",
        "flag_reason_column": "flag_reason",
        "flag_source_column": "flag_source",
        "flag_source_value": "llm",
    },
}

_CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "config", "filters_config.json",
)

_cached: Dict[str, Any] | None = None


def _load_raw() -> Dict[str, Any]:
    global _cached
    if _cached is not None:
        return _cached
    if not os.path.isfile(_CONFIG_PATH):
        logger.debug("No filters config at %s; using defaults", _CONFIG_PATH)
        _cached = {}
        return _cached
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            _cached = json.load(f)
        return _cached
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load filters config: %s", e)
        _cached = {}
        return _cached


def get_config_path() -> str:
    return _CONFIG_PATH


def get_dedup_config() -> Dict[str, Any]:
    raw = _load_raw()
    dedup = raw.get("dedup") or {}
    return {
        "company_rules": dedup.get("company_rules") or DEFAULT_DEDUP["company_rules"],
        "people_rules": dedup.get("people_rules") or DEFAULT_DEDUP["people_rules"],
    }


def get_cleaning_config() -> Dict[str, Any]:
    raw = _load_raw()
    return {
        "keyword_clean": raw.get("keyword_clean") or DEFAULT_KEYWORD_CLEAN,
        "llm_scope_review": raw.get("llm_scope_review") or DEFAULT_LLM_SCOPE,
    }


def get_attio_sync_mapping() -> List[Dict[str, Any]]:
    """Return the default Attio sync field mapping (companies), optionally overridden from config."""
    raw = _load_raw()
    mapping = raw.get("attio_sync_mapping")
    if mapping and isinstance(mapping, list):
        valid = [
            m for m in mapping
            if isinstance(m, dict) and m.get("lead_field") and m.get("attio_attribute")
        ]
        if valid:
            for m in valid:
                m.setdefault("enabled", True)
            return valid
    import copy
    return copy.deepcopy(DEFAULT_ATTIO_SYNC_MAPPING)


def get_attio_people_sync_mapping() -> List[Dict[str, Any]]:
    """Return the default Attio People sync field mapping, optionally overridden from config."""
    raw = _load_raw()
    mapping = raw.get("attio_people_sync_mapping")
    if mapping and isinstance(mapping, list):
        valid = [
            m for m in mapping
            if isinstance(m, dict) and m.get("lead_field") and m.get("attio_attribute")
        ]
        if valid:
            for m in valid:
                m.setdefault("enabled", True)
            return valid
    import copy
    return copy.deepcopy(DEFAULT_ATTIO_PEOPLE_SYNC_MAPPING)


def get_enrichment_output_mapping() -> List[Dict[str, str]]:
    raw = _load_raw()
    mapping = raw.get("enrichment_output_mapping")
    if not mapping or not isinstance(mapping, list):
        return []
    return [m for m in mapping if isinstance(m, dict) and m.get("enrichment_field") and m.get("db_column")]


def save_config(updates: Dict[str, Any]) -> bool:
    """Merge updates into config and write back to file. Returns True on success."""
    global _cached
    raw = _load_raw().copy() if _cached is not None else {}
    for key, value in updates.items():
        if value is not None:
            raw[key] = value
    try:
        os.makedirs(os.path.dirname(_CONFIG_PATH), exist_ok=True)
        with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(raw, f, indent=2)
        _cached = raw
        return True
    except OSError as e:
        logger.warning("Failed to save filters config: %s", e)
        return False
