"""
JustCall Sales Dialer API client.
Adapted from scraper/justcall_api.py with list_campaigns and remove_contact.
"""
import logging
from typing import Any, Optional

import httpx

from lib.config import Settings

logger = logging.getLogger(__name__)

BULK_IMPORT_BATCH_SIZE = 500


def build_justcall_contact_from_lead(lead: dict) -> dict:
    """Build JustCall contact payload from a lead row (SQLite dict)."""
    first_name = (lead.get("name") or "Contact").split()[0] if lead.get("name") else "Contact"
    last_name = " ".join((lead.get("name") or "").split()[1:]) if lead.get("name") else ""
    phone = lead.get("office_phone")
    if not phone:
        return {}
    contact: dict[str, Any] = {
        "first_name": first_name,
        "last_name": last_name or "",
        "phone": phone,
    }
    if lead.get("office_email"):
        contact["email"] = lead["office_email"]
    if lead.get("name"):
        contact["company"] = lead["name"]
    custom = {}
    if lead.get("attio_record_id"):
        custom["attio_record_id"] = str(lead["attio_record_id"])[:200]
    if lead.get("lead_grade"):
        custom["lead_grade"] = str(lead["lead_grade"])[:200]
    if custom:
        contact["custom_fields"] = custom
    return contact


def grade_lead_from_row(lead: dict) -> str:
    """Grade A/B/C/D from lead dict."""
    has_phone = bool(lead.get("office_phone"))
    has_name = bool(lead.get("name"))
    has_title = bool(lead.get("decision_makers"))  # simplified
    has_email = bool(lead.get("office_email"))
    if not has_phone:
        return "D"
    if has_name and has_title and has_email:
        return "A"
    if has_name and (has_title or has_email):
        return "B"
    if has_name:
        return "C"
    return "D"


class JustCallClient:
    def __init__(self, api_key: str = "", api_secret: str = "", base_url: str = ""):
        settings = Settings()
        self.api_key = api_key or settings.justcall_api_key
        self.api_secret = api_secret or settings.justcall_api_secret
        self.base_url = (base_url or settings.justcall_base_url).rstrip("/")
        self._auth = f"{self.api_key}:{self.api_secret}"
        self._headers = {
            "Authorization": self._auth,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    def is_configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    def _request(
        self,
        method: str,
        path: str,
        json: Optional[dict] = None,
        timeout: float = 30.0,
    ) -> dict:
        url = f"{self.base_url}{path}"
        with httpx.Client(timeout=timeout) as client:
            resp = client.request(method, url, headers=self._headers, json=json)
            resp.raise_for_status()
            return resp.json() if resp.content else {}

    def list_campaigns(self) -> list:
        """List Sales Dialer campaigns."""
        data = self._request("GET", "/sales_dialer/campaigns")
        return data.get("data", []) if isinstance(data, dict) else []

    def create_campaign(
        self,
        name: str,
        campaign_type: str = "Autodial",
        default_number: Optional[str] = None,
        country_code: str = "AU",
        contact_dialing_order: str = "First in first out",
    ) -> dict:
        body: dict[str, Any] = {
            "name": name,
            "type": campaign_type,
            "country_code": country_code,
            "contact_dialing_order": contact_dialing_order,
        }
        if default_number:
            body["default_number"] = default_number
        return self._request("POST", "/sales_dialer/campaigns", json=body)

    def add_contact_to_campaign(
        self,
        campaign_id: str,
        first_name: str,
        last_name: str,
        phone: str,
        email: Optional[str] = None,
        company: Optional[str] = None,
        custom_fields: Optional[dict] = None,
    ) -> dict:
        body: dict[str, Any] = {
            "first_name": first_name or "",
            "last_name": last_name or "",
            "phone": phone,
        }
        if email:
            body["email"] = email
        if company:
            body["company"] = company
        if custom_fields:
            body["custom_fields"] = custom_fields
        return self._request(
            "POST",
            f"/sales_dialer/campaigns/{campaign_id}/contacts",
            json=body,
        )

    def bulk_import_contacts(
        self,
        campaign_id: str,
        contacts: list[dict],
        callback_url: Optional[str] = None,
    ) -> dict:
        body: dict[str, Any] = {
            "campaign_id": campaign_id,
            "contacts": contacts,
        }
        if callback_url:
            body["callback_url"] = callback_url
        return self._request(
            "POST",
            "/sales_dialer/contacts/bulk_import",
            json=body,
            timeout=60.0,
        )

    def get_campaign(self, campaign_id: str) -> dict:
        return self._request("GET", f"/sales_dialer/campaigns/{campaign_id}")

    def remove_contact(self, campaign_id: str, contact_id: str) -> dict:
        """Remove a contact from a campaign (if API supports it)."""
        return self._request(
            "DELETE",
            f"/sales_dialer/campaigns/{campaign_id}/contacts/{contact_id}",
        )
