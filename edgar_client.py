"""
edgar_client.py

SEC EDGAR API client for the US intelligence monitor.

Replaces the Companies House API used in the UK version. Key differences:
  - Change detection via 8-K Item 5.02 filings, not officer-list diffing
  - No persistent officer ID; officers matched by normalised name
  - Covers SEC-registered (public) companies only
  - 4-business-day filing lag vs CH's near-real-time updates
  - Rate limit: 10 req/s per SEC fair-use policy (vs CH 500/5min)
"""

import logging
import re
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import requests

EDGAR_BASE        = "https://data.sec.gov"
SEC_ARCHIVES      = "https://www.sec.gov/Archives/edgar/data"
EDGAR_EFTS        = "https://efts.sec.gov/LATEST/search-index"
EDGAR_MAX_RETRIES = 3

# SEC fair-use: identify your organisation in User-Agent.
# Set via EDGAR_USER_AGENT env var or pass to build_edgar_session().
DEFAULT_USER_AGENT = "InsureMonitorUS contact@example.com"

# 8-K items that signal officer/director changes
OFFICER_CHANGE_ITEMS = {"5.02"}

# Patterns for parsing 8-K 5.02 sections
_APPOINT_RE = re.compile(
    r"(?:appointed|elected|named|designated)\s+(?:as\s+)?([A-Z][^,\n]{2,80}?)(?:,|\s+of\b|\s+effective|\s+to\b)",
    re.IGNORECASE,
)
_RESIGN_RE = re.compile(
    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)"
    r"(?:\s*,\s*[^,\n]{1,60})?"
    r"\s+(?:resigned|notified.*?of.*?resignation|stepped down|will resign|departure)",
    re.IGNORECASE,
)
_NAME_TITLE_RE = re.compile(
    r"([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:\s+[A-Z][a-z]+)+)"   # name
    r"\s*(?:,\s*age\s*\d+\s*,)?"
    r"\s+(?:has been|was|will be)\s+(?:appointed|elected|named)\s+(?:as\s+)?"
    r"([A-Z][^,\n]{5,80}?)(?:,|\.|effective|\s+of\b)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# SESSION & RATE LIMITING
# ---------------------------------------------------------------------------

class RateLimiter:
    def __init__(self, requests_per_second: float) -> None:
        self._min_interval = 1.0 / requests_per_second
        self._last_call: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        gap = now - self._last_call
        if gap < self._min_interval:
            time.sleep(self._min_interval - gap)
        self._last_call = time.monotonic()


def build_edgar_session(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    """Build a requests.Session with the SEC-required User-Agent header."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": user_agent,
        "Accept":     "application/json",
    })
    return s


def _edgar_get(
    session: requests.Session,
    url: str,
    params: Optional[Dict],
    limiter: RateLimiter,
    logger: logging.Logger,
    accept: str = "application/json",
) -> Optional[requests.Response]:
    """Resilient GET with rate limiting, retries, and 429 handling."""
    session.headers["Accept"] = accept
    for attempt in range(1, EDGAR_MAX_RETRIES + 1):
        limiter.wait()
        try:
            resp = session.get(url, params=params or {}, timeout=(5, 30))
        except requests.RequestException as exc:
            logger.error("  EDGAR network error (attempt %d/%d): %s", attempt, EDGAR_MAX_RETRIES, exc)
            if attempt < EDGAR_MAX_RETRIES:
                time.sleep(2 ** attempt)
                continue
            return None
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            logger.warning("  EDGAR 429 — sleeping %ds", wait)
            time.sleep(wait)
            continue
        if resp.status_code == 404:
            return resp
        if resp.status_code >= 500:
            if attempt < EDGAR_MAX_RETRIES:
                time.sleep(5 * attempt)
                continue
        return resp
    return None


# ---------------------------------------------------------------------------
# COMPANY SUBMISSIONS
# ---------------------------------------------------------------------------

def _pad_cik(cik: str) -> str:
    """Zero-pad a CIK to 10 digits as required by the EDGAR submissions endpoint."""
    return cik.strip().lstrip("0").zfill(10)


def fetch_company_submissions(
    session: requests.Session,
    cik: str,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[Dict]:
    """Fetch company metadata and recent filing list from EDGAR submissions JSON."""
    padded = _pad_cik(cik)
    url = f"{EDGAR_BASE}/submissions/CIK{padded}.json"
    resp = _edgar_get(session, url, None, limiter, logger)
    if resp is None:
        return None
    if resp.status_code == 404:
        logger.warning("  EDGAR: CIK %s not found (404)", cik)
        return None
    if resp.status_code != 200:
        logger.error("  EDGAR submissions HTTP %d for CIK %s", resp.status_code, cik)
        return None
    return resp.json()


def get_company_name(submissions: Dict) -> str:
    return submissions.get("name", "")


def get_company_sic(submissions: Dict) -> str:
    return submissions.get("sic", "")


# ---------------------------------------------------------------------------
# 8-K OFFICER CHANGE DETECTION
# ---------------------------------------------------------------------------

def fetch_new_officer_8ks(
    session: requests.Session,
    cik: str,
    last_checked: Optional[str],
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[List[Dict]]:
    """
    Return a list of new 8-K Item 5.02 filings for this CIK since last_checked.

    Each entry contains:
        accession_number, filing_date, items, primary_document, filing_url
    Returns None on API error. Returns [] if no new filings.
    """
    subs = fetch_company_submissions(session, cik, limiter, logger)
    if subs is None:
        return None

    recent = subs.get("filings", {}).get("recent", {})
    forms        = recent.get("form", [])
    dates        = recent.get("filingDate", [])
    items_list   = recent.get("items", [])
    acc_numbers  = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])

    cutoff = last_checked[:10] if last_checked else None
    new_filings: List[Dict] = []

    for form, filing_date, items, acc, primary_doc in zip(
        forms, dates, items_list, acc_numbers, primary_docs
    ):
        if form != "8-K":
            continue
        # items is a string like "5.02" or "5.02 9.01" or "2.02 5.02"
        if not any(oi in (items or "") for oi in OFFICER_CHANGE_ITEMS):
            continue
        if cutoff and filing_date <= cutoff:
            break  # filings are newest-first; once we're past cutoff, stop

        cik_num = str(int(cik.lstrip("0") or "0"))
        acc_clean = acc.replace("-", "")
        filing_url = f"https://www.sec.gov/Archives/edgar/data/{cik_num}/{acc_clean}/{primary_doc}"

        new_filings.append({
            "accession_number": acc,
            "filing_date":      filing_date,
            "items":            items or "",
            "primary_document": primary_doc,
            "filing_url":       filing_url,
        })

    return new_filings


# ---------------------------------------------------------------------------
# 8-K DOCUMENT PARSING
# ---------------------------------------------------------------------------

def fetch_filing_text(
    session: requests.Session,
    filing_url: str,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[str]:
    """Fetch an 8-K document and return its text content, HTML tags stripped."""
    resp = _edgar_get(session, filing_url, None, limiter, logger, accept="text/html,text/plain,*/*")
    if resp is None or resp.status_code != 200:
        logger.warning("  Could not fetch 8-K document: %s (HTTP %s)",
                       filing_url, resp.status_code if resp else "N/A")
        return None
    text = resp.text
    # Strip HTML tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_item_502_section(text: str) -> str:
    """Pull out the Item 5.02 section from an 8-K document."""
    # Find the section starting at "5.02" or "Item 5.02"
    match = re.search(r"Item\s+5\.02\b", text, re.IGNORECASE)
    if not match:
        match = re.search(r"\b5\.02\b", text, re.IGNORECASE)
    if not match:
        return text  # return full text if section not found

    start = match.start()
    # Find the next Item section (e.g., "Item 7.01", "Item 9.01")
    next_item = re.search(r"Item\s+\d+\.\d+\b", text[start + 20:], re.IGNORECASE)
    end = start + 20 + next_item.start() if next_item else min(start + 3000, len(text))
    return text[start:end]


def parse_8k_officer_changes(
    text: str,
    filing_date: str,
    cik: str,
    company_name: str,
) -> List[Dict]:
    """
    Parse an 8-K document text to extract officer appointment/resignation events.

    Uses pattern matching on the Item 5.02 section. Returns a list of change dicts
    compatible with the officer_changes format used in intelligence_monitor.py.
    Falls back to a minimal "manual review" entry if parsing fails.
    """
    section = _extract_item_502_section(text)
    changes: List[Dict] = []

    # Try named-appointment pattern: "John Smith was appointed as Chief Executive Officer"
    for m in _NAME_TITLE_RE.finditer(section):
        name_raw = m.group(1).strip()
        role_raw = m.group(2).strip().rstrip(".,;")
        changes.append({
            "change_type":  "New Appointment",
            "company_number": cik,
            "company_name": company_name,
            "officer_name": name_raw,
            "officer_role": role_raw,
            "appointed_on": filing_date,
            "resigned_on":  "",
        })

    # Try resignation pattern
    for m in _RESIGN_RE.finditer(section):
        name_raw = m.group(1).strip()
        # Try to find their role nearby
        role_context = section[max(0, m.start() - 200):m.end() + 200]
        role_match = re.search(
            r"(?:as|role of|position of|her|his)\s+(Chief\s+\w+\s*Officer|C[A-Z]O|Director|President|[A-Z][a-z]+\s+(?:Officer|Director|President|Vice President))",
            role_context, re.IGNORECASE
        )
        role = role_match.group(1).strip() if role_match else "Director/Officer"
        changes.append({
            "change_type":  "Resignation",
            "company_number": cik,
            "company_name": company_name,
            "officer_name": name_raw,
            "officer_role": role,
            "appointed_on": "",
            "resigned_on":  filing_date,
        })

    # If nothing parsed, create a placeholder so the filing is still surfaced
    if not changes:
        changes.append({
            "change_type":  "Officer Change (see filing)",
            "company_number": cik,
            "company_name": company_name,
            "officer_name": "(see SEC filing)",
            "officer_role": "Director/Officer",
            "appointed_on": filing_date,
            "resigned_on":  "",
        })

    return changes


# ---------------------------------------------------------------------------
# OFFICER CAREER HISTORY (EDGAR name search)
# ---------------------------------------------------------------------------

def fetch_officer_history_by_name(
    session: requests.Session,
    officer_name: str,
    watched_ciks: set,
    limiter: RateLimiter,
    logger: logging.Logger,
    lookback_years: int = 7,
) -> List[Dict]:
    """
    Search EDGAR 8-K filings mentioning this officer's name to build a career history.

    Searches full-text of 8-K filings across all SEC filers, then cross-references
    against the watched CIK list to flag watchlist connections.

    Returns a list of dicts compatible with director_intelligence.store_director_profile()
    appointments format.
    """
    from_date = (date.today() - timedelta(days=lookback_years * 365)).isoformat()

    # Quote the name for exact phrase matching
    quoted_name = f'"{officer_name}"'
    params = {
        "q":         quoted_name,
        "forms":     "8-K",
        "dateRange": "custom",
        "startdt":   from_date,
        "enddt":     date.today().isoformat(),
    }

    limiter.wait()
    try:
        resp = session.get(EDGAR_EFTS, params=params, timeout=(5, 30))
    except requests.RequestException as exc:
        logger.warning("  EDGAR name search failed for %s: %s", officer_name, exc)
        return []

    if resp.status_code != 200:
        logger.warning("  EDGAR name search HTTP %d for %s", resp.status_code, officer_name)
        return []

    hits = resp.json().get("hits", {}).get("hits", [])
    appointments: List[Dict] = []
    seen_ciks: set = set()

    for hit in hits[:20]:  # cap to avoid excessive follow-up fetches
        source  = hit.get("_source", {})
        hit_cik = str(source.get("entity_id", "")).lstrip("0")
        if not hit_cik or hit_cik in seen_ciks:
            continue
        seen_ciks.add(hit_cik)

        padded = hit_cik.zfill(8)
        appointments.append({
            "company_number": padded,
            "company_name":   source.get("display_names", [hit_cik])[0] if source.get("display_names") else hit_cik,
            "role":           "Director/Officer",  # role not available from search index
            "appointed_on":   source.get("file_date", ""),
            "resigned_on":    "",
        })

    return appointments


# ---------------------------------------------------------------------------
# CURRENT OFFICERS FROM PROXY/10-K (best-effort; used for context only)
# ---------------------------------------------------------------------------

def fetch_current_officers(
    session: requests.Session,
    cik: str,
    submissions: Dict,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> List[Dict]:
    """
    Attempt to retrieve current officers from the most recent DEF 14A or 10-K.

    Returns a best-effort list of {officer_name, officer_role, appointed_on, resigned_on}.
    Returns [] if unavailable — the 8-K change-detection approach doesn't strictly need this.
    """
    recent = submissions.get("filings", {}).get("recent", {})
    forms        = recent.get("form", [])
    acc_numbers  = recent.get("accessionNumber", [])
    primary_docs = recent.get("primaryDocument", [])
    dates        = recent.get("filingDate", [])

    # Find most recent DEF 14A (proxy) or 10-K
    target_form = None
    target_acc  = None
    target_doc  = None
    target_date = None

    for form, acc, doc, dt in zip(forms, acc_numbers, primary_docs, dates):
        if form in ("DEF 14A", "10-K") and target_form is None:
            target_form = form
            target_acc  = acc
            target_doc  = doc
            target_date = dt
            break

    if not target_acc:
        return []

    cik_num   = str(int(cik.lstrip("0") or "0"))
    acc_clean = target_acc.replace("-", "")
    doc_url   = f"{SEC_ARCHIVES}/{cik_num}/{acc_clean}/{target_doc}"

    text = fetch_filing_text(session, doc_url, limiter, logger)
    if not text:
        return []

    # Simple extraction: look for common officer title patterns
    officers: List[Dict] = []
    seen_names: set = set()

    pattern = re.compile(
        r"([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?(?:\s+[A-Z][a-z]+)+)"
        r"\s+(?:serves?|is|has served|has been)\s+as\s+(?:(?:our|the)\s+)?"
        r"((?:Chief|Executive|Senior|Vice|Managing|Non-Executive|Independent|Lead)\s+"
        r"(?:Executive\s+)?(?:Officer|Director|President|Chairman|Operating|Financial|"
        r"Technology|Information|Risk|Underwriting|Actuar\w+)[^,\n]{0,50})",
        re.IGNORECASE
    )

    for m in pattern.finditer(text):
        name = m.group(1).strip()
        role = m.group(2).strip().rstrip(".,;")
        if name not in seen_names:
            seen_names.add(name)
            officers.append({
                "officer_name": name,
                "officer_role": role,
                "appointed_on": "",
                "resigned_on":  "",
                "appointments_url": "",
            })

    logger.info("  Parsed %d officer(s) from %s (%s)", len(officers), target_form, target_date)
    return officers[:30]  # cap to avoid noise
