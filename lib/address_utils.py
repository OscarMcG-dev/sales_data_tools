"""Parse address strings into location components (for Attio primary_location)."""
import re
from typing import Optional

STATE_FULL_NAMES = {
    "VIC": "Victoria",
    "NSW": "New South Wales",
    "QLD": "Queensland",
    "SA": "South Australia",
    "WA": "Western Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory",
}


def parse_address(raw: Optional[str]) -> dict:
    """
    Parse an accountantlist.com.au-style address into line_1, locality, region, postcode.
    """
    result = {
        "line_1": None,
        "locality": None,
        "region": None,
        "postcode": None,
    }
    if not raw:
        return result

    match = re.search(r"[.\s,]?\s*(VIC|NSW|QLD|SA|WA|TAS|NT|ACT)\s+(\d{4})\s*$", raw)
    if match:
        result["region"] = STATE_FULL_NAMES.get(match.group(1), match.group(1))
        result["postcode"] = match.group(2)
        before_state = raw[: match.start()].rstrip(". ,")
        parts = re.split(r"\.\s*", before_state)
        parts = [p.strip(" ,") for p in parts if p.strip(" ,")]
        if len(parts) >= 2:
            result["locality"] = parts[-1]
            result["line_1"] = ", ".join(parts[:-1])
        elif parts:
            result["line_1"] = parts[0]
            words = parts[0].rsplit(" ", 1)
            if len(words) == 2:
                result["locality"] = words[-1]
                result["line_1"] = words[0]
    else:
        result["line_1"] = raw

    return result
