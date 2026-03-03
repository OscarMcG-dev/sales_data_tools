"""Pydantic models aligned to the Attio Company and People schemas."""
from __future__ import annotations

import copy
from typing import Optional, List
from pydantic import BaseModel, Field


class DirectoryListing(BaseModel):
    """Raw data scraped from a single accountantlist.com.au detail page."""
    listing_url: str
    name: str
    phone: Optional[str] = None
    email: Optional[str] = None
    contact_name: Optional[str] = None
    website_url: Optional[str] = None
    street_address: Optional[str] = None
    areas_of_accountancy: List[str] = Field(default_factory=list)
    state: Optional[str] = None


class DecisionMaker(BaseModel):
    """A decision maker extracted via LLM enrichment."""
    name: Optional[str] = None
    title: Optional[str] = None
    summary: str = ""
    phone_office: Optional[str] = None
    phone_mobile: Optional[str] = None
    phone_direct: Optional[str] = None
    email: Optional[str] = None
    linkedin: Optional[str] = None


class EnrichmentData(BaseModel):
    """Data obtained by crawling the firm's own website (Phase 2)."""
    description: str = ""
    edited_description: str = ""
    office_phone: Optional[str] = None
    office_email: Optional[str] = None
    associated_emails: List[str] = Field(default_factory=list)
    associated_mobiles: List[str] = Field(default_factory=list)
    associated_info: str = ""
    organisational_structure: Optional[str] = None
    company_size: Optional[str] = None
    linkedin: Optional[str] = None
    facebook: Optional[str] = None
    decision_makers: List[DecisionMaker] = Field(default_factory=list)
    confidence_score: float = 0.0
    out_of_scope: bool = False
    out_of_scope_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# LLM structured-output schema models
# ---------------------------------------------------------------------------

class LLMDecisionMaker(BaseModel):
    """Schema for a single decision maker in the LLM response."""
    name: str = Field("", description="Full name of the decision maker")
    title: str = Field("", description="Job title (Partner, Director, Principal, etc.)")
    decision_maker_summary: str = Field(
        "",
        description=(
            "Factual bullet-point profile for a sales rep calling this person. "
            "Include: qualifications (CA, CPA, NTAA fellow), years in role/industry, "
            "specific responsibilities (e.g. 'runs SMSF division'), prior firms "
            "(e.g. 'ex-Deloitte'). No marketing language. Example: "
            "'CA, CPA. 15 yrs at firm. Heads tax compliance. Ex-PwC. NTAA fellow.'"
        ),
    )
    phone_office: str = Field("", description="Office phone in E.164 format (+61XXXXXXXXX) or empty string")
    phone_mobile: str = Field("", description="Mobile phone in E.164 format or empty string")
    phone_direct: str = Field("", description="Direct line in E.164 format or empty string")
    email: str = Field("", description="Email address or empty string")
    linkedin: str = Field("", description="Personal LinkedIn URL or empty string")


class LLMEnrichmentResponse(BaseModel):
    """Top-level schema the LLM must conform to."""
    description: str = Field("", description="Factual company description. What they do, where, how big, who they serve. No marketing spin.")
    edited_description: str = Field(
        "",
        description=(
            "Concise firmographic brief a sales rep reads while the phone rings. "
            "Use short bullet points separated by ' | '. Cover: "
            "location/suburb, core services (tax, SMSF, audit, bookkeeping), "
            "software stack (Xero, MYOB, QuickBooks), team size if stated, "
            "client types (SMB, individuals, specific industries), "
            "notable specializations. No adjectives like 'trusted' or 'expert'. "
            "Example: 'Dee Why NSW | Tax, SMSF, audit | Xero | ~5 staff | "
            "Serves medical & trades | NTAA, CAANZ members'"
        ),
    )
    office_phone: str = Field("", description="Main office phone in E.164 format (+61XXXXXXXXX) or empty string")
    office_email: str = Field("", description="Main office/reception email or empty string")
    associated_emails: List[str] = Field(default_factory=list, description="Other email addresses found on the site")
    associated_mobile_numbers: List[str] = Field(default_factory=list, description="Mobile numbers found, each in E.164 format")
    associated_info: str = Field("", description="Supplementary firmographic detail: professional memberships, software, etc.")
    organisational_structure: str = Field("", description="One of: 'solo practice', 'SMB', 'enterprise', 'franchise', or empty string")
    linkedin: str = Field("", description="Company LinkedIn URL or empty string")
    facebook: str = Field("", description="Company Facebook URL or empty string")
    decision_makers: List[LLMDecisionMaker] = Field(default_factory=list, description="Senior decision makers.")
    confidence_score: float = Field(0.0, description="0.0-1.0 confidence in the extraction quality")
    out_of_scope: bool = Field(False, description="True if the business is NOT an accounting firm")
    out_of_scope_reason: str = Field("", description="Why the firm is out of scope, or empty string")


class LLMWebSearchPerson(BaseModel):
    """A single person found via web search."""
    name: str = Field("", description="Full name exactly as found in the search result")
    title: str = Field("", description="Job title at the firm or empty string")
    qualifications: str = Field("", description="Professional qualifications or empty string")
    email: str = Field("", description="Email address if found, or empty string")
    phone: str = Field("", description="Phone number in E.164 format if found, or empty string")
    linkedin: str = Field("", description="LinkedIn profile URL or empty string")
    source: str = Field("", description="Where this person was found")


class LLMWebSearchResponse(BaseModel):
    """Top-level schema for the web search DM discovery call."""
    people: List[LLMWebSearchPerson] = Field(default_factory=list, description="Senior people associated with this firm")
    firm_linkedin: str = Field("", description="Company LinkedIn page URL if found, or empty string")
    firm_email: str = Field("", description="General firm email if found, or empty string")
    firm_phone: str = Field("", description="Firm phone in E.164 format if found, or empty string")
    brief: str = Field("", description="One-line factual summary from search results or empty string")


def _make_strict_schema(schema: dict) -> dict:
    """Transform Pydantic JSON Schema for OpenRouter strict mode."""
    schema = copy.deepcopy(schema)
    defs = schema.pop("$defs", {})

    def resolve_refs(node):
        if isinstance(node, dict):
            if "$ref" in node:
                ref_name = node["$ref"].split("/")[-1]
                return resolve_refs(copy.deepcopy(defs[ref_name]))
            return {k: resolve_refs(v) for k, v in node.items()}
        if isinstance(node, list):
            return [resolve_refs(item) for item in node]
        return node

    schema = resolve_refs(schema)

    def enforce_strict(node):
        if isinstance(node, dict):
            if node.get("type") == "object" and "properties" in node:
                node["additionalProperties"] = False
                node["required"] = list(node["properties"].keys())
            for v in node.values():
                enforce_strict(v)
        elif isinstance(node, list):
            for item in node:
                enforce_strict(item)

    enforce_strict(schema)
    schema.pop("title", None)
    return schema


def get_enrichment_json_schema() -> dict:
    """Return the strict JSON Schema for LLM structured output."""
    raw = LLMEnrichmentResponse.model_json_schema()
    return _make_strict_schema(raw)


def get_web_search_json_schema() -> dict:
    """Return the strict JSON Schema for the web search DM discovery call."""
    raw = LLMWebSearchResponse.model_json_schema()
    return _make_strict_schema(raw)


class LLMScopeOnlyResponse(BaseModel):
    """Schema for scope-only re-assessment: is this lead in scope (accounting firm)?"""
    in_scope: bool = Field(description="True if the business appears to be an accounting firm we target")
    reason: str = Field("", description="Brief reason for in_scope (e.g. 'Accounting firm, tax and SMSF') or out-of-scope (e.g. 'Not accounting')")
    segment: str = Field("", description="Optional segment label, e.g. 'SMB', 'Solo', or empty string")


def get_scope_only_json_schema() -> dict:
    """Return the strict JSON Schema for scope-only LLM calls."""
    raw = LLMScopeOnlyResponse.model_json_schema()
    return _make_strict_schema(raw)


class CompanyRecord(BaseModel):
    """Merged company record ready for Attio export."""
    domains: Optional[str] = None
    name: str
    description: str = ""
    primary_location_line_1: Optional[str] = None
    primary_location_locality: Optional[str] = None
    primary_location_region: Optional[str] = None
    primary_location_postcode: Optional[str] = None
    primary_location_country_code: str = "AU"
    segment: Optional[str] = None
    office_phone: Optional[str] = None
    office_email: Optional[str] = None
    associated_mobiles: List[str] = Field(default_factory=list)
    associated_emails_1: str = ""
    associated_location: str = ""
    associated_location_4: str = ""
    organisational_structure: Optional[str] = None
    company_size: Optional[str] = None
    linkedin: Optional[str] = None
    facebook: Optional[str] = None
    original_data_source_scrape: str = "accountantlist.com.au"
    dm_1_name_temp: Optional[str] = None
    attio_status: str = "new"
    attio_record_id: Optional[str] = None
    listing_url: str = ""


class PersonRecord(BaseModel):
    """Person record for Attio People import."""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email_addresses: Optional[str] = None
    job_title: Optional[str] = None
    phone_numbers: List[str] = Field(default_factory=list)
    linkedin: Optional[str] = None
    company_name: str = ""
    company_domain: Optional[str] = None
