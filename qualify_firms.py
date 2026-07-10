"""
qualify_firms.py

Scores pending insurance firms for NY/Northeast relevance before promotion
to firms.csv.  Runs on demand; takes ~2-3 minutes for 64 firms.

Signals checked per firm:
  1. Principal office address from EDGAR submissions
     → hq_city, hq_state, northeast_hq
  2. Adzuna job postings (US-wide, no location filter)
     → count results in Northeast states
     → subset with tech-titled roles (engineers, data, cloud, cyber, etc.)

Verdict:
  Recommend — NE HQ  OR  ≥3 tech jobs in NE
  Review    — Partial signal (some NE jobs but HQ elsewhere, or 1-2 tech roles)
  Skip      — No detectable Northeast presence

Output: input/pending_firms_qualified.csv — sorted Recommend → Review → Skip
        Columns match firms.csv plus scoring fields; paste Recommend rows straight in.

Usage:
    python qualify_firms.py
    python qualify_firms.py --skip-adzuna   # EDGAR address only (faster, no job data)
"""

import argparse
import csv
import logging
import os
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

from edgar_client import RateLimiter, build_edgar_session, fetch_company_submissions

load_dotenv()

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
SCRIPT_DIR    = Path(__file__).resolve().parent
INPUT_DIR     = SCRIPT_DIR / "input"
PENDING_CSV   = INPUT_DIR / "pending_firms.csv"
QUALIFIED_CSV = INPUT_DIR / "pending_firms_qualified.csv"

# ---------------------------------------------------------------------------
# GEOGRAPHY
# ---------------------------------------------------------------------------
# EDGAR stateOrCountry codes for Northeast + mid-Atlantic
NE_STATE_CODES = {"NY", "NJ", "CT", "MA", "PA", "RI", "VT", "NH", "ME", "DC"}

# State names as they appear in Adzuna location.area arrays
NE_AREA_NAMES = {
    "New York", "New York State",
    "New Jersey",
    "Connecticut",
    "Massachusetts",
    "Pennsylvania",
    "Rhode Island",
    "Vermont",
    "New Hampshire",
    "Maine",
    "Washington DC", "District of Columbia",
}

# ---------------------------------------------------------------------------
# TECH ROLE DETECTION
# ---------------------------------------------------------------------------
TECH_TITLE_KEYWORDS = [
    "engineer", "developer", "architect", "software", "data ", " data",
    "cloud", "devops", "platform", "analytics", "technology", "technologist",
    "cyber", "security", "infrastructure", "machine learning", "artificial intelligence",
    "digital", "sre", "site reliability", "database", "network", "systems",
    "full stack", "fullstack", "backend", "frontend", "api ", "python",
    "java ", "golang", "automation", "devsecops", "mlops",
]

# ---------------------------------------------------------------------------
# ADZUNA
# ---------------------------------------------------------------------------
ADZUNA_BASE              = "https://api.adzuna.com/v1/api/jobs/us/search/1"
ADZUNA_RPS               = 1      # conservative; Adzuna throttles at ~1/s per key
ADZUNA_RESULTS_PER_PAGE  = 50
COMPANY_MATCH_THRESHOLD  = 80     # stricter than monitor — false positives affect verdict

# Name cleaning — mirrors intelligence_monitor.py
_STRIP_PARENS    = re.compile(r"\([^)]*\)")
_STRIP_FOR_SEARCH = re.compile(
    r"\b(limited|ltd|llp|plc|lp|inc|the|reinsurance|underwriters|underwriting|"
    r"syndicate|syndicates|managing|agency|agent|holdings|usa|"
    r"mutual|assurance|society|association|of|and|corp|corporation)\b",
    re.IGNORECASE,
)
_STRIP_LEGAL_ONLY = re.compile(
    r"\b(ltd|llp|plc|lp|inc|corp|corporation)\b",
    re.IGNORECASE,
)
_WS = re.compile(r"\s+")


def _make_search_name(registered_name: str) -> str:
    name = _STRIP_PARENS.sub(" ", registered_name)
    name = _WS.sub(" ", _STRIP_FOR_SEARCH.sub(" ", name)).strip()
    words = [w for w in name.split() if len(w) > 1]
    return " ".join(words[:2]) if words else registered_name


def _make_validation_name(registered_name: str) -> str:
    name = _STRIP_PARENS.sub(" ", registered_name)
    return _WS.sub(" ", _STRIP_LEGAL_ONLY.sub(" ", name)).strip().lower()


# ---------------------------------------------------------------------------
# OUTPUT COLUMNS
# ---------------------------------------------------------------------------
QUALIFY_COLUMNS = [
    "Company Name", "CIK", "NAIC Code", "Ticker", "Company Type",
    "monitoring_status", "notes",
    "hq_city", "hq_state", "northeast_hq",
    "northeast_jobs", "tech_jobs_ne",
    "verdict", "qualification_notes",
]

VERDICT_ORDER = {"Recommend": 0, "Review": 1, "Skip": 2}


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("qualify_firms")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)-8s] %(message)s",
                            datefmt="%H:%M:%S")
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ---------------------------------------------------------------------------
# EDGAR ADDRESS
# ---------------------------------------------------------------------------
def get_edgar_address(
    cik: str,
    session: requests.Session,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Tuple[str, str]:
    """Return (city, state_code) from EDGAR principal business address."""
    subs = fetch_company_submissions(session, cik, limiter, logger)
    if not subs:
        return "", ""
    addr  = subs.get("addresses", {}).get("business", {})
    city  = (addr.get("city") or "").strip().title()
    state = (addr.get("stateOrCountry") or "").strip().upper()
    return city, state


# ---------------------------------------------------------------------------
# ADZUNA SEARCH
# ---------------------------------------------------------------------------
def search_adzuna(
    firm_name: str,
    app_id: str,
    app_key: str,
    session: requests.Session,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> List[Dict]:
    """Search Adzuna US-wide for this firm. Returns company-validated job results."""
    search_name = _make_search_name(firm_name)
    validation  = _make_validation_name(firm_name)

    limiter.wait()
    try:
        resp = session.get(
            ADZUNA_BASE,
            params={
                "app_id":           app_id,
                "app_key":          app_key,
                "results_per_page": ADZUNA_RESULTS_PER_PAGE,
                "what":             search_name,
                "content-type":     "application/json",
            },
            timeout=(5, 20),
        )
    except requests.RequestException as exc:
        logger.warning("  Adzuna error: %s", exc)
        return []

    if not resp.ok:
        logger.warning("  Adzuna HTTP %d", resp.status_code)
        return []

    validated = []
    for job in resp.json().get("results", []):
        company = (job.get("company", {}).get("display_name") or "").lower()
        if fuzz.token_set_ratio(validation, company) >= COMPANY_MATCH_THRESHOLD:
            validated.append(job)

    logger.debug("  Adzuna '%s': %d raw → %d validated",
                 search_name, len(resp.json().get("results", [])), len(validated))
    return validated


# ---------------------------------------------------------------------------
# LOCATION & ROLE ANALYSIS
# ---------------------------------------------------------------------------
def is_northeast(job: Dict) -> bool:
    areas = job.get("location", {}).get("area", [])
    return any(a in NE_AREA_NAMES for a in areas)


def is_tech_role(job: Dict) -> bool:
    title = (job.get("title") or "").lower()
    return any(kw in title for kw in TECH_TITLE_KEYWORDS)


# ---------------------------------------------------------------------------
# VERDICT
# ---------------------------------------------------------------------------
def score_firm(
    hq_state: str,
    ne_jobs: int,
    tech_ne_jobs: int,
) -> Tuple[str, str]:
    """Return (verdict, notes)."""
    ne_hq = hq_state in NE_STATE_CODES

    if ne_hq and tech_ne_jobs >= 1:
        return "Recommend", f"NE HQ ({hq_state}) + {tech_ne_jobs} tech role(s) in NE"
    if ne_hq:
        return "Recommend", f"NE HQ ({hq_state}); no NE tech postings on Adzuna"
    if tech_ne_jobs >= 3:
        return "Recommend", (
            f"{tech_ne_jobs} tech role(s) in NE "
            f"(HQ {hq_state if hq_state else 'unknown'})"
        )
    if tech_ne_jobs >= 1:
        return "Review", (
            f"{tech_ne_jobs} tech role(s) in NE; "
            f"HQ {hq_state if hq_state else 'unknown'}"
        )
    if ne_jobs >= 2:
        return "Review", (
            f"{ne_jobs} job(s) in NE (no tech titles); "
            f"HQ {hq_state if hq_state else 'unknown'}"
        )
    if ne_jobs == 1:
        return "Review", f"1 job in NE; HQ {hq_state if hq_state else 'unknown'}"
    if hq_state and hq_state not in NE_STATE_CODES:
        return "Skip", f"HQ {hq_state}; no NE jobs found"
    return "Skip", "No NE address or jobs detected"


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Qualify pending firms for NE relevance")
    parser.add_argument("--skip-adzuna", action="store_true",
                        help="Only check EDGAR address; skip Adzuna job search")
    args = parser.parse_args()

    logger = setup_logging()

    if not PENDING_CSV.exists():
        logger.error("Not found: %s", PENDING_CSV)
        sys.exit(1)

    with PENDING_CSV.open(encoding="utf-8-sig") as fh:
        firms = list(csv.DictReader(fh))
    logger.info("Qualifying %d firms from %s", len(firms), PENDING_CSV.name)
    if args.skip_adzuna:
        logger.info("--skip-adzuna: job search disabled")

    edgar_ua      = os.getenv("EDGAR_USER_AGENT", "InsureMonitorUS qualify@example.com")
    adzuna_app_id = os.getenv("ADZUNA_APP_ID", "").strip()
    adzuna_app_key = os.getenv("ADZUNA_APP_KEY", "").strip()

    if not args.skip_adzuna and (not adzuna_app_id or not adzuna_app_key):
        logger.error("ADZUNA_APP_ID / ADZUNA_APP_KEY not set — use --skip-adzuna or add to .env")
        sys.exit(1)

    edgar_session  = build_edgar_session(edgar_ua)
    edgar_limiter  = RateLimiter(5)
    adzuna_session = requests.Session()
    adzuna_session.headers["User-Agent"] = edgar_ua
    adzuna_limiter = RateLimiter(ADZUNA_RPS)

    results = []
    counts  = {"Recommend": 0, "Review": 0, "Skip": 0}

    for i, firm in enumerate(firms, 1):
        name = firm.get("Company Name", "")
        cik  = firm.get("CIK", "")
        logger.info("[%d/%d] %s", i, len(firms), name)

        city, state = get_edgar_address(cik, edgar_session, edgar_limiter, logger)
        ne_hq = state in NE_STATE_CODES
        logger.info("  HQ: %s%s%s%s",
                    city or "?", ", " if city and state else "",
                    state or "?",
                    "  [NE]" if ne_hq else "")

        if args.skip_adzuna:
            ne_jobs = tech_ne_jobs = 0
        else:
            jobs         = search_adzuna(name, adzuna_app_id, adzuna_app_key,
                                         adzuna_session, adzuna_limiter, logger)
            ne_jobs      = sum(1 for j in jobs if is_northeast(j))
            tech_ne_jobs = sum(1 for j in jobs if is_northeast(j) and is_tech_role(j))
            if jobs:
                logger.info("  Jobs: %d validated | %d in NE | %d tech in NE",
                            len(jobs), ne_jobs, tech_ne_jobs)
            else:
                logger.info("  Jobs: none found on Adzuna")

        verdict, notes = score_firm(state, ne_jobs, tech_ne_jobs)
        counts[verdict] += 1
        logger.info("  => %-10s %s", verdict, notes)

        row = dict(firm)
        row["monitoring_status"]   = "active"
        row["hq_city"]             = city
        row["hq_state"]            = state
        row["northeast_hq"]        = "Y" if ne_hq else "N"
        row["northeast_jobs"]      = ne_jobs
        row["tech_jobs_ne"]        = tech_ne_jobs
        row["verdict"]             = verdict
        row["qualification_notes"] = notes
        results.append(row)

    results.sort(key=lambda r: (VERDICT_ORDER[r["verdict"]], r["Company Name"]))

    with QUALIFIED_CSV.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=QUALIFY_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    logger.info("")
    logger.info("=" * 50)
    logger.info("Written: %s", QUALIFIED_CSV.name)
    logger.info("  Recommend : %d", counts["Recommend"])
    logger.info("  Review    : %d", counts["Review"])
    logger.info("  Skip      : %d", counts["Skip"])
    logger.info("=" * 50)
    logger.info("Next step: review Recommend rows and copy into input/firms.csv")


if __name__ == "__main__":
    main()
