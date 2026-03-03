"""Map accountantlist.com.au 'Areas of Accountancy' to Attio segment options."""
from typing import List, Optional

TAX_INDICATOR = "Tax Planning and Returns"
BOOKKEEPING_INDICATOR = "Bookkeeping"

SEGMENT_GENERAL = "General Accounting (Including Tax)"
SEGMENT_BOOKKEEPING = "Bookkeeping (No Income Tax)"
SEGMENT_OTHER_ACCOUNTING = "Other Accounting (No Tax)"

ALL_KNOWN_AREAS = {
    "Accounting System Set Ups",
    "Asset Protection",
    "Audit Services",
    "BAS Returns/GST",
    "Bookkeeping",
    "Business advisory",
    "Business Recovery",
    "Business Set Ups",
    "Financial Planning",
    "Forensic Accounting",
    "Insolvency",
    "Litigation Support",
    "Medical Profession Accounting",
    "Personal Administration",
    "Retirement Planning",
    "Tax Planning and Returns",
}


def map_areas_to_segment(areas: List[str]) -> str:
    """Map a list of 'Areas of Accountancy' strings to a single Attio segment."""
    if not areas:
        return SEGMENT_GENERAL

    normalized = {a.strip() for a in areas if a.strip()}

    if TAX_INDICATOR in normalized:
        return SEGMENT_GENERAL

    if BOOKKEEPING_INDICATOR in normalized:
        return SEGMENT_BOOKKEEPING

    if normalized:
        return SEGMENT_OTHER_ACCOUNTING

    return SEGMENT_GENERAL
