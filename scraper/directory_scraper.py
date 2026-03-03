"""
Phase 1: Scrape accountantlist.com.au directory.
Writes listings to checkpoint and optionally to LeadDB (SQLite).
"""
import asyncio
import json
import logging
import re
from typing import List, Optional, Tuple
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

from lib.models import DirectoryListing
from lib.phone_utils import normalize_to_e164
from lib.url_utils import extract_domain
from lib.address_utils import parse_address
from lib.db import LeadDB
from scraper.checkpoint import Checkpoint
from scraper.segment_mapper import map_areas_to_segment

logger = logging.getLogger(__name__)

BASE_URL = "https://www.accountantlist.com.au"
STATES = ["VIC", "NSW", "QLD", "SA", "WA", "TAS", "NT", "ACT"]
LETTERS = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")


def build_index_urls() -> List[Tuple[str, str, str]]:
    """Generate all (state, letter, url) tuples for first pages."""
    urls = []
    for state in STATES:
        for letter in LETTERS:
            url = f"{BASE_URL}/accountants-in-{state}-beginning-with-{letter}.aspx"
            urls.append((state, letter, url))
    return urls


async def fetch(client: httpx.AsyncClient, url: str, retries: int = 3) -> Optional[str]:
    """Fetch a URL with retries, returning HTML text or None."""
    for attempt in range(retries):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=30.0)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"HTTP {resp.status_code} for {url}")
        except httpx.HTTPError as e:
            logger.warning(f"Attempt {attempt+1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt)
    return None


def parse_index_page(html: str) -> Tuple[List[str], int]:
    """Parse an index/listing page. Returns (list of detail page URLs, max page number)."""
    soup = BeautifulSoup(html, "html.parser")
    detail_urls = []
    max_page = 0

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.match(r"/\d+-.*\.aspx$", href):
            full_url = urljoin(BASE_URL, href)
            if full_url not in detail_urls:
                detail_urls.append(full_url)
        page_match = re.search(r"[?&]Page=(\d+)", href)
        if page_match:
            page_num = int(page_match.group(1))
            if page_num > max_page:
                max_page = page_num

    return detail_urls, max_page


def parse_detail_page(html: str, url: str) -> Optional[DirectoryListing]:
    """Parse a firm detail page into a DirectoryListing."""
    soup = BeautifulSoup(html, "html.parser")

    h1_tags = soup.find_all("h1")
    name = None
    for h1 in h1_tags:
        text = h1.get_text(strip=True)
        if text and text.lower() not in ("accountantlist", "accountant list"):
            name = text
            break

    if not name:
        return None

    listing = DirectoryListing(listing_url=url, name=name)

    body = soup.find("body") or soup
    text_content = body.get_text(separator="\n")
    lines = [line.strip() for line in text_content.split("\n") if line.strip()]

    LABELS = {"phone", "email", "contact name", "website",
              "street address", "areas of accountancy",
              "back to other accountants"}

    for i, line in enumerate(lines):
        line_lower = line.lower()

        if line_lower == "phone" and i + 1 < len(lines):
            raw_phone = lines[i + 1]
            listing.phone = raw_phone.split(",")[0].strip()

        elif line_lower == "email" and i + 1 < len(lines):
            candidate = lines[i + 1]
            if "@" in candidate:
                listing.email = candidate.strip()

        elif line_lower == "contact name" and i + 1 < len(lines):
            listing.contact_name = lines[i + 1].strip()

        elif line_lower == "website" and i + 1 < len(lines):
            candidate = lines[i + 1].strip()
            if candidate.startswith("http") or candidate.startswith("www"):
                listing.website_url = candidate

        elif line_lower == "street address" and i + 1 < len(lines):
            addr_parts = []
            for j in range(i + 1, min(i + 4, len(lines))):
                if lines[j].lower() in LABELS:
                    break
                addr_parts.append(lines[j])
            listing.street_address = " ".join(addr_parts).strip()

        elif line_lower == "areas of accountancy" and i + 1 < len(lines):
            raw_areas = lines[i + 1]
            listing.areas_of_accountancy = [a.strip() for a in raw_areas.split(",") if a.strip()]

    if not listing.website_url:
        for a in body.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "accountantlist.com.au" not in href:
                listing.website_url = href
                break

    if listing.street_address:
        state_match = re.search(r"\b(VIC|NSW|QLD|SA|WA|TAS|NT|ACT)\b", listing.street_address)
        if state_match:
            listing.state = state_match.group(1)

    return listing


def _listing_to_lead_row(listing: DirectoryListing, state: Optional[str] = None) -> dict:
    """Map a DirectoryListing to a lead row for LeadDB.insert_lead."""
    addr = parse_address(listing.street_address)
    domains = extract_domain(listing.website_url)
    areas = listing.areas_of_accountancy or []
    row = {
        "name": listing.name,
        "domains": domains,
        "listing_url": listing.listing_url,
        "website_url": listing.website_url or "",
        "lead_source": "directory",
        "status": "pending_review",
        "office_phone": normalize_to_e164(listing.phone) if listing.phone else None,
        "office_email": listing.email,
        "street_address": listing.street_address,
        "areas_of_accountancy": json.dumps(areas),
        "segment": map_areas_to_segment(areas),
        "primary_location_line_1": addr.get("line_1"),
        "primary_location_locality": addr.get("locality"),
        "primary_location_region": addr.get("region"),
        "primary_location_postcode": addr.get("postcode"),
    }
    return row


async def scrape_directory(
    checkpoint: Checkpoint,
    delay: float = 1.0,
    max_concurrent: int = 5,
    states: Optional[List[str]] = None,
    db: Optional[LeadDB] = None,
) -> List[DirectoryListing]:
    """
    Scrape the entire accountantlist.com.au directory.
    If db is provided, each new listing is also inserted into the leads table.
    """
    existing_listings = checkpoint.get_directory_listings()
    if existing_listings:
        listings = [DirectoryListing(**l) for l in existing_listings]
        logger.info(f"Resumed {len(listings)} listings from checkpoint")
    else:
        listings = []

    completed_urls = checkpoint.get_completed_detail_urls()
    logger.info(f"Already completed {len(completed_urls)} detail URLs")

    all_index_urls = build_index_urls()
    if states:
        allowed = {s.upper() for s in states}
        all_index_urls = [(s, l, u) for s, l, u in all_index_urls if s in allowed]

    logger.info(f"Will process {len(all_index_urls)} index page groups")

    semaphore = asyncio.Semaphore(max_concurrent)

    async with httpx.AsyncClient(
        headers={"User-Agent": "Mozilla/5.0 (compatible; AccountantListScraper/1.0)"},
        limits=httpx.Limits(max_connections=max_concurrent),
    ) as client:
        all_detail_urls: List[Tuple[str, str]] = []

        for state, letter, index_url in all_index_urls:
            async with semaphore:
                logger.info(f"Fetching index: {state}-{letter}")
                html = await fetch(client, index_url)
                if not html:
                    continue

                detail_urls, max_page = parse_index_page(html)
                for u in detail_urls:
                    all_detail_urls.append((u, state))

                for page_num in range(1, max_page + 1):
                    page_url = f"{index_url}?Page={page_num}"
                    await asyncio.sleep(delay)
                    html = await fetch(client, page_url)
                    if not html:
                        continue
                    page_detail_urls, _ = parse_index_page(html)
                    for u in page_detail_urls:
                        all_detail_urls.append((u, state))

                await asyncio.sleep(delay)

        seen = set()
        unique_detail_urls = []
        for url, state in all_detail_urls:
            if url not in seen:
                seen.add(url)
                unique_detail_urls.append((url, state))

        logger.info(f"Found {len(unique_detail_urls)} unique detail URLs "
                    f"({len(completed_urls)} already done)")

        new_count = 0
        for url, state in unique_detail_urls:
            if url in completed_urls:
                continue

            async with semaphore:
                html = await fetch(client, url)
                if not html:
                    checkpoint.mark_detail_url_done(url)
                    continue

                listing = parse_detail_page(html, url)
                if listing:
                    if not listing.state:
                        listing.state = state
                    listings.append(listing)
                    new_count += 1

                    if db is not None:
                        row = _listing_to_lead_row(listing, state)
                        db.insert_lead(row)

                checkpoint.mark_detail_url_done(url)

                if new_count % 50 == 0 and new_count > 0:
                    checkpoint.save_directory_listings(
                        [l.model_dump() for l in listings]
                    )
                    logger.info(f"Checkpoint saved at {len(listings)} listings")

                await asyncio.sleep(delay)

        checkpoint.save_directory_listings([l.model_dump() for l in listings])
        logger.info(f"Directory scrape complete: {len(listings)} total listings "
                    f"({new_count} new)")

    return listings
