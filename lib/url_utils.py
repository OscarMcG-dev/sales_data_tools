"""URL/domain utilities."""
from typing import Optional
from urllib.parse import urlparse


def extract_domain(url: Optional[str]) -> Optional[str]:
    """Extract the bare domain from a URL (no www prefix)."""
    if not url:
        return None
    if not url.startswith("http"):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc.lower()
        if domain.startswith("www."):
            domain = domain[4:]
        return domain if domain else None
    except Exception:
        return None
