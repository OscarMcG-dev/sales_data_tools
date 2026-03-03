"""Phone number normalization utilities for AU numbers."""
import phonenumbers
from typing import Optional


def normalize_to_e164(phone: str, default_country: str = "AU") -> Optional[str]:
    """Parse and normalize a phone number to E.164 format."""
    if not phone:
        return None

    phone = phone.strip().replace(" ", "").replace("-", "").replace("(", "").replace(")", "")

    if len(phone) < 6:
        return None

    country_map = {"AU": "AU", "NZ": "NZ", "UK": "GB"}
    iso_code = country_map.get(default_country.upper(), default_country.upper())

    if phone.startswith("+"):
        try:
            parsed = phonenumbers.parse(phone, None)
            if str(parsed.country_code) in ("61", "64", "44"):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except Exception:
            pass
        return None

    try:
        parsed = phonenumbers.parse(phone, iso_code)
        if not parsed or not parsed.country_code:
            return None
        if str(parsed.country_code) not in ("61", "64", "44"):
            return None
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        pass

    return None


def classify_phone_type(phone: str) -> str:
    """Classify an E.164 phone as 'mobile' or 'office'."""
    if not phone:
        return "office"
    clean = phone.replace(" ", "").replace("-", "")
    for prefix in ("+614", "+642", "+447"):
        if clean.startswith(prefix):
            return "mobile"
    return "office"
