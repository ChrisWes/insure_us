"""
intelligence_monitor.py  (US/NYC edition)

Unified daily intelligence monitor for the US insurance market.
For each target firm, checks:
  1. Officer changes via SEC EDGAR 8-K Item 5.02 filings
  2. Relevant job postings (Adzuna US)
  3. Industry news (NewsAPI)

Writes dated digest CSVs and sends one consolidated email structured
per company, with a signal strength indicator for prioritisation.

Key differences from UK edition:
  - Companies House API replaced with SEC EDGAR
  - Firm identifier: CIK (SEC Central Index Key) instead of CH number
  - Officer change detection: new 8-K Item 5.02 filings (not officer-list diffing)
  - No persistent officer ID; director intelligence keyed by normalised name
  - Private MGAs (no CIK) receive job + news monitoring only
"""

import csv
import logging
import os
import re
import smtplib
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import anthropic
import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

from director_intelligence import (
    build_director_intelligence,
    flatten_for_csv,
    get_director_profile,
    init_director_db,
    is_profile_stale,
    load_clients,
    make_officer_key,
    store_director_profile,
)
from edgar_client import (
    RateLimiter,
    build_edgar_session,
    fetch_company_submissions,
    fetch_current_officers,
    fetch_filing_text,
    fetch_new_officer_8ks,
    fetch_officer_history_by_name,
    get_company_name,
    get_company_sic,
    parse_8k_officer_changes,
)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
FIRMS_NAME_COLUMN   = "Company Name"
FIRMS_NUMBER_COLUMN = "CIK"               # primary identifier (10-digit, zero-padded)
FIRMS_TYPE_COLUMN   = "Company Type"
FIRMS_STATUS_COLUMN = "monitoring_status"

SENIOR_ROLES = {
    "director", "chief executive", "ceo", "cfo", "cto", "cio", "cdo", "coo",
    "chief financial", "chief operating", "chief technology", "chief information",
    "chief digital", "chief data", "chief underwriting", "chief risk", "chief actuary",
    "president", "chairman", "chair", "managing director",
}

# Adzuna — US region
ADZUNA_BASE_URL         = "https://api.adzuna.com/v1/api/jobs/us/search"
ADZUNA_WHERE            = "New York, NY"
ADZUNA_RESULTS_PER_PAGE = 50
ADZUNA_MAX_PER_MINUTE   = 100
ADZUNA_MAX_PAGES        = 10
COMPANY_MATCH_THRESHOLD = 80
SHORT_LIVED_DAYS        = 30
MULTI_POSTING_THRESHOLD = 3

ROLE_KEYWORDS = [
    "transformation", "programme manager", "program manager", "project manager",
    "business analyst", "PMO", "change manager", "data manager",
    "digital", "technology lead", "IT director",
    "chief information", "chief technology", "chief data",
]

# NewsAPI
NEWS_API_BASE       = "https://newsapi.org/v2/everything"
NEWS_DAILY_BUDGET   = 95
NEWS_PAGE_SIZE      = 100
NEWS_LOOKBACK_DAYS  = 30
NEWS_SLEEP_SECONDS  = 1.0

NEWS_KEYWORDS = [
    "technology", "digital", "transformation", "system", "platform",
    "software", "data", "cyber", "acquisition", "merger", "partnership",
    "investment", "regulatory", "compliance", "appointed", "restructure",
]

# Signal scoring thresholds
SIGNAL_HIGH   = 5
SIGNAL_MEDIUM = 2

LLM_MODEL = "claude-haiku-4-5-20251001"

# EDGAR rate limit: 10 req/s per SEC fair-use policy
EDGAR_REQUESTS_PER_SECOND = 8   # stay comfortably under 10

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
LOGS_DIR   = SCRIPT_DIR / "logs"

FIRMS_CSV        = INPUT_DIR / "firms.csv"
CLIENTS_CSV      = INPUT_DIR / "clients.csv"
OFFICER_DB_PATH  = DATA_DIR / "officer_baseline.db"
JOBS_DB_PATH     = DATA_DIR / "jobs_baseline.db"
NEWS_DB_PATH     = DATA_DIR / "news_baseline.db"
DIRECTOR_DB_PATH = DATA_DIR / "director_intelligence.db"

VERSION = (SCRIPT_DIR / "VERSION").read_text().strip()
TODAY   = date.today().isoformat()

OFFICER_DIGEST = OUTPUT_DIR / f"officer_changes_{TODAY}.csv"
JOBS_DIGEST    = OUTPUT_DIR / f"job_changes_{TODAY}.csv"
NEWS_DIGEST    = OUTPUT_DIR / f"news_changes_{TODAY}.csv"
LOG_FILE       = LOGS_DIR   / f"intelligence_monitor_{TODAY}.log"

OFFICER_COLUMNS = [
    "change_type", "company_number", "company_name",
    "officer_name", "officer_role", "appointed_on", "resigned_on", "date_detected",
    "digital_background", "digital_roles", "client_connections",
    "watchlist_connections", "concurrent_watchlist",
    "filing_url", "llm_commentary",
]
JOBS_COLUMNS = [
    "change_type", "company_number", "company_name",
    "job_title", "job_location", "salary_min", "salary_max",
    "posted_date", "date_detected", "company_type", "monitoring_status",
]
NEWS_COLUMNS = [
    "change_type", "company_number", "company_name",
    "article_title", "article_source", "article_published",
    "article_url", "date_detected",
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("intelligence_monitor_us")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch_handler = logging.StreamHandler(sys.stdout)
    ch_handler.setLevel(logging.INFO)
    ch_handler.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch_handler)
    return logger


# ---------------------------------------------------------------------------
# OFFICER DATABASE (keyed by CIK; uses 8-K filings instead of officer lists)
# ---------------------------------------------------------------------------
def init_officer_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS firms (
            cik            TEXT PRIMARY KEY,
            company_name   TEXT,
            last_checked   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS officer_8k_filings (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            cik              TEXT NOT NULL,
            accession_number TEXT UNIQUE,
            filing_date      DATE,
            items            TEXT,
            officer_name     TEXT,
            officer_role     TEXT,
            change_type      TEXT,
            filing_url       TEXT,
            first_seen       TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS officer_enrichment (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            officer_name   TEXT NOT NULL,
            cik            TEXT NOT NULL,
            career_summary TEXT,
            fetched_at     TIMESTAMP,
            UNIQUE(officer_name, cik)
        );
    """)
    conn.commit()


def officer_is_first_run(conn: sqlite3.Connection, cik: str) -> bool:
    return conn.execute(
        "SELECT last_checked FROM firms WHERE cik = ?", (cik,)
    ).fetchone() is None


def get_processed_accession_numbers(conn: sqlite3.Connection, cik: str) -> set:
    rows = conn.execute(
        "SELECT accession_number FROM officer_8k_filings WHERE cik = ?", (cik,)
    ).fetchall()
    return {r[0] for r in rows}


def store_8k_changes(conn, cik, filing, changes, ts):
    for c in changes:
        conn.execute(
            """INSERT OR IGNORE INTO officer_8k_filings
               (cik, accession_number, filing_date, items, officer_name,
                officer_role, change_type, filing_url, first_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                cik, filing["accession_number"], filing["filing_date"],
                filing["items"], c.get("officer_name"), c.get("officer_role"),
                c.get("change_type"), filing.get("filing_url"), ts,
            ),
        )


def upsert_officer_firm(conn, cik, name, ts):
    conn.execute(
        """INSERT INTO firms (cik, company_name, last_checked) VALUES (?, ?, ?)
           ON CONFLICT(cik) DO UPDATE SET
               company_name = excluded.company_name, last_checked = excluded.last_checked""",
        (cik, name, ts),
    )


def get_officer_enrichment(conn, officer_name, cik) -> Optional[str]:
    row = conn.execute(
        "SELECT career_summary FROM officer_enrichment WHERE officer_name=? AND cik=?",
        (officer_name, cik),
    ).fetchone()
    return row[0] if row else None


def upsert_officer_enrichment(conn, officer_name, cik, career_summary, ts):
    conn.execute(
        """INSERT INTO officer_enrichment (officer_name, cik, career_summary, fetched_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(officer_name, cik) DO UPDATE SET
               career_summary=excluded.career_summary, fetched_at=excluded.fetched_at""",
        (officer_name, cik, career_summary, ts),
    )


# ---------------------------------------------------------------------------
# JOBS DATABASE  (identical to UK version; Adzuna schema unchanged)
# ---------------------------------------------------------------------------
def init_jobs_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS processed_firms (
            cik            TEXT PRIMARY KEY,
            company_name   TEXT,
            last_checked   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS job_postings (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            cik            TEXT NOT NULL,
            company_name   TEXT,
            job_id         TEXT NOT NULL,
            job_title      TEXT,
            job_location   TEXT,
            salary_min     REAL,
            salary_max     REAL,
            posted_date    DATE,
            first_seen     TIMESTAMP,
            last_seen      TIMESTAMP,
            is_active      INTEGER NOT NULL DEFAULT 1,
            UNIQUE(cik, job_id)
        );
    """)
    conn.commit()


def jobs_is_first_run(conn, cik):
    return conn.execute(
        "SELECT last_checked FROM processed_firms WHERE cik = ?", (cik,)
    ).fetchone() is None


def get_active_postings(conn, cik) -> Dict:
    rows = conn.execute(
        "SELECT job_id, job_title, job_location, salary_min, salary_max, posted_date, first_seen "
        "FROM job_postings WHERE cik = ? AND is_active = 1", (cik,)
    ).fetchall()
    return {r[0]: dict(r) for r in rows}


def upsert_posting(conn, cik, company_name, job, ts):
    conn.execute(
        """INSERT INTO job_postings
           (cik, company_name, job_id, job_title, job_location,
            salary_min, salary_max, posted_date, first_seen, last_seen, is_active)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
           ON CONFLICT(cik, job_id) DO UPDATE SET last_seen=excluded.last_seen, is_active=1""",
        (cik, company_name, job["job_id"], job["job_title"], job["job_location"],
         job.get("salary_min"), job.get("salary_max"), job.get("posted_date"), ts, ts),
    )


def mark_posting_disappeared(conn, cik, job_id):
    conn.execute("UPDATE job_postings SET is_active=0 WHERE cik=? AND job_id=?", (cik, job_id))


def upsert_jobs_firm(conn, cik, name, ts):
    conn.execute(
        """INSERT INTO processed_firms (cik, company_name, last_checked) VALUES (?, ?, ?)
           ON CONFLICT(cik) DO UPDATE SET
               company_name=excluded.company_name, last_checked=excluded.last_checked""",
        (cik, name, ts),
    )


# ---------------------------------------------------------------------------
# NEWS DATABASE  (identical to UK version)
# ---------------------------------------------------------------------------
def init_news_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS news_processed_firms (
            cik            TEXT PRIMARY KEY,
            company_name   TEXT,
            last_checked   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS news_articles (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            cik               TEXT NOT NULL,
            company_name      TEXT,
            article_url       TEXT NOT NULL UNIQUE,
            article_title     TEXT,
            article_source    TEXT,
            article_published DATE,
            first_seen        TIMESTAMP,
            is_active         INTEGER NOT NULL DEFAULT 1
        );
    """)
    conn.commit()


def news_is_first_run(conn, cik):
    return conn.execute(
        "SELECT last_checked FROM news_processed_firms WHERE cik=?", (cik,)
    ).fetchone() is None


def get_baseline_articles(conn, cik) -> Dict:
    rows = conn.execute(
        "SELECT article_url, article_title, article_source, article_published, first_seen "
        "FROM news_articles WHERE cik=? AND is_active=1", (cik,)
    ).fetchall()
    return {r[0]: dict(r) for r in rows}


def upsert_article(conn, cik, company_name, article, ts):
    conn.execute(
        """INSERT INTO news_articles
           (cik, company_name, article_url, article_title, article_source,
            article_published, first_seen, is_active)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1)
           ON CONFLICT(article_url) DO UPDATE SET is_active=1""",
        (cik, company_name, article["article_url"], article["article_title"],
         article["article_source"], article.get("article_published"), ts),
    )


def upsert_news_firm(conn, cik, name, ts):
    conn.execute(
        """INSERT INTO news_processed_firms (cik, company_name, last_checked) VALUES (?, ?, ?)
           ON CONFLICT(cik) DO UPDATE SET
               company_name=excluded.company_name, last_checked=excluded.last_checked""",
        (cik, name, ts),
    )


# ---------------------------------------------------------------------------
# ADZUNA  (US endpoint; identical logic to UK version)
# ---------------------------------------------------------------------------
_STRIP_FOR_ADZUNA = re.compile(
    r"\b(limited|ltd|llp|plc|lp|inc|the|insurance|reinsurance|underwriters|underwriting|"
    r"syndicate|syndicates|managing|agency|agent|group|holdings|usa|us|services|"
    r"financial|life|general|mutual|assurance|society|association|of|and|corp|corporation)\b",
    re.IGNORECASE,
)
_STRIP_LEGAL_ONLY = re.compile(r"\b(limited|ltd|llp|plc|lp|inc|corp|corporation)\b", re.IGNORECASE)
_WS = re.compile(r"\s+")


def make_search_name(registered_name: str) -> str:
    cleaned = _WS.sub(" ", _STRIP_FOR_ADZUNA.sub(" ", registered_name)).strip()
    words = [w for w in cleaned.split() if len(w) > 1]
    return " ".join(words[:2]) if words else registered_name


def make_validation_name(registered_name: str) -> str:
    cleaned = _WS.sub(" ", _STRIP_LEGAL_ONLY.sub(" ", registered_name)).strip()
    return cleaned.lower()


def matches_keywords(title: str, description: str) -> bool:
    text = f"{title} {description}".lower()
    return any(kw.lower() in text for kw in ROLE_KEYWORDS)


def fetch_adzuna_jobs(
    session: requests.Session,
    app_id: str, app_key: str,
    firm_name: str,
    limiter: "AdzunaLimiter",
    logger: logging.Logger,
) -> Optional[List[Dict]]:
    search_name     = make_search_name(firm_name)
    validation_name = make_validation_name(firm_name)
    relevant, page  = [], 1
    retry_counts: Dict = {}

    while True:
        url = f"{ADZUNA_BASE_URL}/{page}"
        limiter.wait()
        try:
            resp = session.get(
                url,
                params={
                    "app_id": app_id, "app_key": app_key,
                    "what": search_name, "where": ADZUNA_WHERE,
                    "results_per_page": ADZUNA_RESULTS_PER_PAGE,
                },
                timeout=(5, 20),
            )
        except requests.RequestException as exc:
            logger.error("  Adzuna request failed (page %d): %s", page, exc)
            return None

        if resp.status_code == 400:
            if page == 1:
                logger.error("  Adzuna 400 for '%s'", search_name)
                return None
            break

        if resp.status_code in (500, 502, 503, 504):
            key = (firm_name, page)
            retries = retry_counts.get(key, 0)
            if retries < 3:
                retry_counts[key] = retries + 1
                time.sleep(10 * (retries + 1))
                continue
            return None

        if resp.status_code != 200:
            logger.error("  Adzuna HTTP %d for %s", resp.status_code, firm_name)
            return None

        data = resp.json()
        for job in data.get("results", []):
            company_display = job.get("company", {}).get("display_name", "")
            if fuzz.token_sort_ratio(validation_name, company_display.lower()) < COMPANY_MATCH_THRESHOLD:
                continue
            title = job.get("title", "")
            if not matches_keywords(title, job.get("description", "")):
                continue
            created = job.get("created", "")
            relevant.append({
                "job_id":       str(job.get("id", "")),
                "job_title":    title,
                "job_location": job.get("location", {}).get("display_name", ""),
                "salary_min":   job.get("salary_min"),
                "salary_max":   job.get("salary_max"),
                "posted_date":  created[:10] if created else "",
            })

        total = data.get("count", 0)
        fetched = (page - 1) * ADZUNA_RESULTS_PER_PAGE + len(data.get("results", []))
        if not data.get("results") or fetched >= total or page >= ADZUNA_MAX_PAGES:
            break
        page += 1

    return relevant


class AdzunaLimiter:
    def __init__(self):
        self._min_interval = 60.0 / ADZUNA_MAX_PER_MINUTE
        self._last_call = 0.0

    def wait(self):
        now = time.monotonic()
        gap = now - self._last_call
        if gap < self._min_interval:
            time.sleep(self._min_interval - gap)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# NEWS API  (identical to UK version)
# ---------------------------------------------------------------------------
def article_matches_keywords(title: str, description: str) -> bool:
    text = f"{title} {description}".lower()
    return any(kw.lower() in text for kw in NEWS_KEYWORDS)


def fetch_newsapi(
    session: requests.Session,
    api_key: str,
    firm_name: str,
    logger: logging.Logger,
) -> Tuple[Optional[List[Dict]], int]:
    from_date = (date.today() - timedelta(days=NEWS_LOOKBACK_DAYS)).isoformat()
    relevant: List[Dict] = []
    page, pages_fetched = 1, 0

    while True:
        time.sleep(NEWS_SLEEP_SECONDS)
        try:
            resp = session.get(
                NEWS_API_BASE,
                params={
                    "apiKey": api_key, "q": f'"{firm_name}"', "language": "en",
                    "from": from_date, "sortBy": "publishedAt",
                    "pageSize": NEWS_PAGE_SIZE, "page": page,
                },
                timeout=(5, 20),
            )
        except requests.RequestException as exc:
            logger.error("  NewsAPI request failed (page %d): %s", page, exc)
            return None, pages_fetched

        pages_fetched += 1
        if resp.status_code == 426:
            logger.error("  NewsAPI: free-tier upgrade required")
            return None, pages_fetched
        if resp.status_code == 429:
            logger.warning("  NewsAPI 429 — sleeping 60s")
            time.sleep(60)
            pages_fetched -= 1
            continue
        if resp.status_code != 200:
            logger.error("  NewsAPI HTTP %d for '%s'", resp.status_code, firm_name)
            return None, pages_fetched

        data = resp.json()
        if data.get("status") != "ok":
            logger.error("  NewsAPI error: %s", data.get("message", "unknown"))
            return None, pages_fetched

        for art in data.get("articles", []):
            title = art.get("title") or ""
            desc  = art.get("description") or ""
            if not article_matches_keywords(title, desc):
                continue
            url = art.get("url", "")
            if not url:
                continue
            relevant.append({
                "article_url":       url,
                "article_title":     title,
                "article_source":    (art.get("source") or {}).get("name", ""),
                "article_published": (art.get("publishedAt") or "")[:10],
            })

        fetched_so_far = page * NEWS_PAGE_SIZE
        if not data.get("articles") or fetched_so_far >= data.get("totalResults", 0):
            break
        page += 1

    return relevant, pages_fetched


# ---------------------------------------------------------------------------
# CHANGE DETECTION
# ---------------------------------------------------------------------------
def is_senior(role: str) -> bool:
    return any(sr in role.lower() for sr in SENIOR_ROLES)


def detect_job_changes(cik, company_name, baseline, current_jobs, firm_meta, logger) -> List[Dict]:
    changes = []
    current_ids = {job["job_id"] for job in current_jobs}
    firm_type = firm_meta.get(FIRMS_TYPE_COLUMN, "")
    status    = firm_meta.get(FIRMS_STATUS_COLUMN, "")

    new_count = 0
    for job in current_jobs:
        if job["job_id"] not in baseline:
            new_count += 1
            logger.info("  JOB NEW: %s | %s", job["job_title"], job["job_location"])
            changes.append({
                "change_type": "New Posting", "company_number": cik,
                "company_name": company_name, "job_title": job["job_title"],
                "job_location": job["job_location"],
                "salary_min": job.get("salary_min") or "",
                "salary_max": job.get("salary_max") or "",
                "posted_date": job["posted_date"], "date_detected": TODAY,
                "company_type": firm_type, "monitoring_status": status,
            })

    if new_count >= MULTI_POSTING_THRESHOLD:
        logger.warning("  ** %d new relevant postings — strong signal **", new_count)

    for job_id, b in baseline.items():
        if job_id in current_ids:
            continue
        days_active = (datetime.utcnow() - datetime.fromisoformat(b["first_seen"])).days
        change_type = "Disappeared - Short-lived" if days_active < SHORT_LIVED_DAYS else "Disappeared"
        logger.info("  JOB %s: %s (%d days)", change_type.upper(), b["job_title"], days_active)
        changes.append({
            "change_type": change_type, "company_number": cik,
            "company_name": company_name, "job_title": b["job_title"],
            "job_location": b.get("job_location", ""),
            "salary_min": b.get("salary_min") or "", "salary_max": b.get("salary_max") or "",
            "posted_date": b.get("posted_date", ""), "date_detected": TODAY,
            "company_type": firm_type, "monitoring_status": status,
        })

    return changes


def detect_news_changes(cik, company_name, baseline_urls, current_articles, first_run, logger) -> List[Dict]:
    if first_run:
        return []
    changes = []
    for art in current_articles:
        if art["article_url"] in baseline_urls:
            continue
        logger.info("  NEWS NEW: %s | %s", art["article_source"], art["article_title"][:60])
        changes.append({
            "change_type": "New Article", "company_number": cik, "company_name": company_name,
            "article_title": art["article_title"], "article_source": art["article_source"],
            "article_published": art["article_published"], "article_url": art["article_url"],
            "date_detected": TODAY,
        })
    return changes


def signal_score(
    officer_changes: List[Dict],
    job_changes: List[Dict],
    news_changes: Optional[List[Dict]] = None,
) -> Tuple[str, int]:
    score = 0
    for c in officer_changes:
        if c["change_type"] in ("New Appointment", "Resignation"):
            score += 3 if is_senior(c.get("officer_role", "")) else 1
    new_jobs = [j for j in job_changes if j["change_type"] == "New Posting"]
    score += len(new_jobs)
    if len(new_jobs) >= MULTI_POSTING_THRESHOLD:
        score += 2
    new_articles = [n for n in (news_changes or []) if n["change_type"] == "New Article"]
    score += min(2, len(new_articles))
    label = "HIGH" if score >= SIGNAL_HIGH else ("MEDIUM" if score >= SIGNAL_MEDIUM else "LOW")
    return label, score


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------
def write_digest(path, columns, rows, empty_label, logger):
    OUTPUT_DIR.mkdir(exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow({col: "" for col in columns} | {
                "change_type": empty_label, "date_detected": TODAY,
            })
    logger.info("Digest written -> %s", path.name)


# ---------------------------------------------------------------------------
# CROSS-FIRM APPOINTMENT DETECTION
# ---------------------------------------------------------------------------
def collect_cross_firm_appointments(all_officer_changes: List[Dict]) -> List[Dict]:
    by_officer: Dict[str, List[Dict]] = defaultdict(list)
    for c in all_officer_changes:
        if c["change_type"] == "New Appointment":
            by_officer[c["officer_name"].lower()].append(c)

    alerts = []
    for _, changes in by_officer.items():
        if len(changes) < 2:
            continue
        roles = list({c["officer_role"] for c in changes})
        alerts.append({
            "officer_name":  changes[0]["officer_name"],
            "role":          roles[0] if len(roles) == 1 else " / ".join(roles),
            "firm_count":    len(changes),
            "firms":         [{"company_name": c["company_name"], "company_number": c["company_number"]} for c in changes],
            "llm_commentary": "",
        })

    alerts.sort(key=lambda a: a["firm_count"], reverse=True)
    return alerts


def generate_cross_firm_commentary(officer_name, role, firms, logger) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""
    firm_list = "\n".join(f"  - {f['company_name']}" for f in firms)
    prompt = (
        f"Director: {officer_name} ({role.title()})\n"
        f"Simultaneously appointed to {len(firms)} US insurance sector companies:\n"
        f"{firm_list}\n\n"
        "In 2-3 sentences: what does this simultaneous multi-company appointment "
        "signal about group strategy, and what is the BD opportunity for an insurance broker?"
    )
    system = (
        "You are a US insurance market intelligence analyst helping an insurance broker "
        "identify business development opportunities. Be specific and concise."
    )
    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=LLM_MODEL, max_tokens=150, system=system,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        logger.warning("Cross-firm LLM failed for %s: %s", officer_name, exc)
        return ""


# ---------------------------------------------------------------------------
# LLM COMMENTARY
# ---------------------------------------------------------------------------
def generate_llm_commentary(name: str, firm_type: str, activity: Dict, logger: logging.Logger) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""

    lines = [
        f"Firm: {name}" + (f" [{firm_type}]" if firm_type else ""),
        f"Relevant live job postings: {activity['active_job_count']}",
        "",
    ]

    o_changes = activity["officer_changes"]
    if o_changes:
        lines.append("OFFICER CHANGES (from SEC 8-K filings):")
        for c in o_changes:
            senior = " [SENIOR]" if is_senior(c.get("officer_role", "")) else ""
            lines.append(f"  {c['change_type']}: {c['officer_name']} as {c['officer_role']}{senior}")
            intel = c.get("director_intel")
            if intel:
                if intel.get("career_summary"):
                    lines.append(f"    Career: {intel['career_summary']}")
                if intel.get("digital_background"):
                    dig = "; ".join(intel["digital_roles"][:3])
                    lines.append(f"    Digital transformation background: {dig}")
                for cc in intel.get("client_connections", [])[:3]:
                    status = "current" if cc["is_current"] else "former"
                    lines.append(f"    CLIENT CONNECTION: {cc['officer_role'].title()} at {cc['company_name']} ({status})")
                for cc in intel.get("concurrent_watchlist", [])[:3]:
                    lines.append(f"    CONCURRENT POSITION: Also active as {cc['officer_role'].title()} at {cc['company_name']}")
            elif c.get("career_summary"):
                lines.append(f"    Background: {c['career_summary']}")

    j_changes = [j for j in activity["job_changes"] if j["change_type"] == "New Posting"]
    if j_changes:
        lines.append("NEW JOB POSTINGS:")
        for j in j_changes:
            lines.append(f"  {j['job_title']} | {j['job_location']}")

    n_changes = [n for n in activity["news_changes"] if n["change_type"] == "New Article"]
    if n_changes:
        lines.append("RECENT NEWS:")
        for n in n_changes:
            lines.append(f'  "{n["article_title"]}" ({n["article_source"]}, {n["article_published"]})')

    system = (
        "You are a US insurance market intelligence analyst helping an insurance broker or MGA partner "
        "identify business development opportunities. Given signals about a US insurance sector firm, "
        "provide a brief assessment in 2-3 sentences. Focus on what the signals suggest about the firm's "
        "direction and any BD opportunity. If a newly appointed director has a digital transformation "
        "background or connections to existing client firms, treat these as high-priority signals. "
        "Be specific and concise."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=LLM_MODEL, max_tokens=200, system=system,
            messages=[{"role": "user", "content": "\n".join(lines)}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        logger.warning("LLM commentary failed for %s: %s", name, exc)
        return ""


# ---------------------------------------------------------------------------
# EMAIL
# ---------------------------------------------------------------------------
def send_email(
    officer_changes: List[Dict],
    job_changes: List[Dict],
    news_changes: List[Dict],
    company_activity: Dict[str, Dict],
    cross_firm_alerts: List[Dict],
    stats: Dict,
    logger: logging.Logger,
) -> None:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    if not smtp_host:
        logger.info("SMTP_HOST not set — skipping email")
        return

    smtp_port     = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    email_from    = os.getenv("EMAIL_FROM", smtp_username).strip()
    email_to_raw  = os.getenv("EMAIL_TO", "").strip()
    if not email_to_raw:
        logger.warning("EMAIL_TO not set — skipping email")
        return

    recipients    = [a.strip() for a in email_to_raw.split(",") if a.strip()]
    total_changes = (
        stats["officer_new"] + stats["officer_resigned"]
        + stats["job_new"] + stats["job_disappeared"]
        + stats["news_new"]
    )

    if total_changes:
        subject = (
            f"US | Intelligence Monitor v{VERSION}: {total_changes} signal(s) — {TODAY}  "
            f"({stats['officer_new']} appointments, {stats['officer_resigned']} resignations, "
            f"{stats['job_new']} new postings, {stats['news_new']} news articles)"
        )
    else:
        subject = f"US | Intelligence Monitor v{VERSION}: No changes — {TODAY}"

    lines = [
        f"US Intelligence Monitor v{VERSION} — {TODAY}",
        "=" * 60,
        f"Firms checked        : {stats['firms_checked']}",
        f"  - Public (EDGAR)   : {stats['edgar_firms']}",
        f"  - Private (no CIK) : {stats['private_firms']}",
        f"Officer changes      : {stats['officer_new']} new appointments, {stats['officer_resigned']} resignations",
        f"Job posting changes  : {stats['job_new']} new, {stats['job_disappeared']} disappeared",
        f"News articles        : {stats['news_new']} new",
        f"API errors (EDGAR)   : {stats['edgar_errors']}",
        f"API errors (Adzuna)  : {stats['adzuna_errors']}",
        "",
    ]

    if cross_firm_alerts:
        lines.append("CROSS-FIRM APPOINTMENT ALERTS")
        lines.append("=" * 60)
        lines.append("(Same director appointed at multiple monitored firms simultaneously)")
        lines.append("")
        for alert in cross_firm_alerts:
            firms_str = ", ".join(f["company_name"] for f in alert["firms"])
            lines.append(f"  {alert['officer_name']}  |  {alert['role'].title()}  |  {alert['firm_count']} firms")
            lines.append(f"  Firms: {firms_str}")
            if alert.get("llm_commentary"):
                lines.append(f"  Assessment: {alert['llm_commentary']}")
            lines.append("")

    if total_changes and company_activity:
        lines.append("COMPANIES WITH ACTIVITY")
        lines.append("=" * 60)

        sorted_companies = sorted(company_activity.items(), key=lambda x: x[1]["score"], reverse=True)

        for cik, activity in sorted_companies:
            o_changes  = activity["officer_changes"]
            j_changes  = activity["job_changes"]
            signal     = activity["signal"]
            name       = activity["name"]
            firm_type  = activity.get("firm_type", "")

            header = f"[ {signal} ]  {name}"
            if firm_type:
                header += f"  [{firm_type}]"
            lines.append("")
            lines.append(header)
            lines.append("-" * 60)

            if o_changes:
                lines.append("  Officers  (from SEC 8-K filings):")
                for c in o_changes:
                    ct     = c["change_type"].upper()
                    senior = "  [SENIOR]" if is_senior(c.get("officer_role", "")) else ""
                    lines.append(f"    {ct:<30}  {c['officer_name']}  |  {c['officer_role']}{senior}")
                    if c.get("filing_url"):
                        lines.append(f"                                    SEC filing: {c['filing_url']}")
                    intel = c.get("director_intel")
                    if intel:
                        if intel.get("career_summary"):
                            lines.append(f"                                    {intel['career_summary']}")
                        if intel.get("digital_background"):
                            dig = "; ".join(intel["digital_roles"][:2])
                            lines.append(f"                                    *** DIGITAL BACKGROUND: {dig}")
                        for cc in intel.get("client_connections", [])[:3]:
                            status = "current" if cc["is_current"] else "former"
                            lines.append(f"                                    *** CLIENT: {cc['officer_role'].title()} at {cc['company_name']} ({status})")
                        for cc in intel.get("concurrent_watchlist", [])[:3]:
                            lines.append(f"                                    *** CONCURRENT: Also active at {cc['company_name']} ({cc['officer_role'].title()})")
                    elif c.get("career_summary"):
                        lines.append(f"                                    {c['career_summary']}")

            active_jobs = activity.get("active_job_count", 0)
            new_jobs    = [j for j in j_changes if j["change_type"] == "New Posting"]
            gone_jobs   = [j for j in j_changes if j["change_type"] != "New Posting"]

            if j_changes:
                lines.append(f"  Jobs  ({active_jobs} relevant posting(s) currently live):")
                for j in new_jobs:
                    salary = ""
                    if j.get("salary_min") or j.get("salary_max"):
                        salary = f"  ${j.get('salary_min','')}–{j.get('salary_max','')}"
                    lines.append(f"    NEW  {j['job_title']}  |  {j['job_location']}{salary}")
                for j in gone_jobs:
                    lines.append(f"    {j['change_type'].upper()}  {j['job_title']}")

            new_articles = [n for n in activity.get("news_changes", []) if n["change_type"] == "New Article"]
            if new_articles:
                lines.append(f"  News  ({len(new_articles)} new article(s)):")
                for n in new_articles:
                    lines.append(f'    - "{n["article_title"]}"  --  {n["article_source"]}, {n["article_published"]}')

            commentary = activity.get("llm_commentary", "")
            if commentary:
                lines.append(f"  Assessment: {commentary}")

        lines.append("")

    lines.append("Full digests attached.")
    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["From"]    = email_from
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for digest_path in (OFFICER_DIGEST, JOBS_DIGEST, NEWS_DIGEST):
        if digest_path.exists():
            with digest_path.open("rb") as fh:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(fh.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={digest_path.name}")
            msg.attach(part)

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
                if smtp_username:
                    server.login(smtp_username, smtp_password)
                server.sendmail(email_from, recipients, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.ehlo(); server.starttls(); server.ehlo()
                if smtp_username:
                    server.login(smtp_username, smtp_password)
                server.sendmail(email_from, recipients, msg.as_string())
        logger.info("Email sent to %s", ", ".join(recipients))
    except smtplib.SMTPException as exc:
        logger.error("Failed to send email: %s", exc)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("US Intelligence Monitor v%s", VERSION)
    logger.info("=" * 60)

    load_dotenv()

    app_id  = os.getenv("ADZUNA_APP_ID", "").strip()
    app_key = os.getenv("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        logger.error("ADZUNA_APP_ID / ADZUNA_APP_KEY not set. Exiting.")
        sys.exit(1)

    edgar_user_agent = os.getenv(
        "EDGAR_USER_AGENT",
        f"InsureMonitorUS {os.getenv('EMAIL_FROM', 'contact@example.com')}"
    )

    if not FIRMS_CSV.exists():
        logger.error("Firms CSV not found: %s", FIRMS_CSV)
        sys.exit(1)

    with FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        firms = list(csv.DictReader(fh))
    logger.info("Loaded %d firms from %s", len(firms), FIRMS_CSV.name)

    # Build watchlist lookups
    watchlist_numbers: Set[str] = set()
    watchlist_names:   Dict[str, str] = {}
    for row in firms:
        cik = row.get(FIRMS_NUMBER_COLUMN, "").strip().zfill(10)
        if cik.strip("0"):  # skip empty CIKs
            watchlist_numbers.add(cik)
            watchlist_names[cik] = row.get(FIRMS_NAME_COLUMN, "")

    # Load client list
    client_names   = load_clients(CLIENTS_CSV)
    client_numbers = set(client_names.keys())
    if client_names:
        logger.info("Loaded %d clients from %s", len(client_names), CLIENTS_CSV.name)
    else:
        logger.info("No clients.csv found — client connection matching disabled")

    DATA_DIR.mkdir(exist_ok=True)
    officer_conn = sqlite3.connect(OFFICER_DB_PATH)
    officer_conn.row_factory = sqlite3.Row
    init_officer_db(officer_conn)

    jobs_conn = sqlite3.connect(JOBS_DB_PATH)
    jobs_conn.row_factory = sqlite3.Row
    init_jobs_db(jobs_conn)

    director_conn = sqlite3.connect(DIRECTOR_DB_PATH)
    director_conn.row_factory = sqlite3.Row
    init_director_db(director_conn)

    edgar_session  = build_edgar_session(edgar_user_agent)
    adzuna_session = requests.Session()
    adzuna_session.headers.update({"Accept": "application/json"})

    edgar_limiter  = RateLimiter(EDGAR_REQUESTS_PER_SECOND)
    adzuna_limiter = AdzunaLimiter()

    all_officer_changes: List[Dict] = []
    all_job_changes:     List[Dict] = []
    company_activity:    Dict[str, Dict] = {}
    firm_buffer:         Dict[str, Dict] = {}

    total_firms   = len(firms)
    edgar_firms   = 0
    private_firms = 0
    edgar_errors  = 0
    adzuna_errors = 0
    officer_new   = 0
    officer_resigned = 0
    job_new       = 0
    job_disappeared = 0

    for idx, row in enumerate(firms, start=1):
        cik  = row.get(FIRMS_NUMBER_COLUMN, "").strip()
        name = (row.get(FIRMS_NAME_COLUMN, "") or "").strip() or cik
        if not name:
            continue

        # Zero-pad CIK if present
        has_cik = bool(cik and cik.strip("0"))
        if has_cik:
            cik = cik.zfill(10)
        else:
            # Private firm — no CIK; use name-slug as pseudo-key for jobs/news only
            cik = f"PRIVATE_{re.sub(r'[^a-z0-9]', '_', name.lower())[:30]}"
            private_firms += 1

        logger.info("[%d/%d] %s  (%s)", idx, total_firms, name, cik)
        ts = datetime.utcnow().isoformat()

        # ---- Officers (SEC EDGAR 8-K) — public companies only ----
        o_changes: List[Dict] = []

        if has_cik:
            edgar_firms += 1
            o_first = officer_is_first_run(officer_conn, cik)

            last_checked = None
            if not o_first:
                row_db = officer_conn.execute(
                    "SELECT last_checked FROM firms WHERE cik=?", (cik,)
                ).fetchone()
                last_checked = row_db[0] if row_db else None

            new_8ks = fetch_new_officer_8ks(edgar_session, cik, last_checked, edgar_limiter, logger)

            if new_8ks is None:
                edgar_errors += 1
                logger.error("  EDGAR error for %s", cik)
            elif o_first:
                logger.info("  Officers: first run, %d recent 8-K(s) found — baselined", len(new_8ks))
                # Store filings as processed without generating change alerts
                for filing in new_8ks:
                    text = fetch_filing_text(edgar_session, filing["filing_url"], edgar_limiter, logger)
                    if text:
                        changes = parse_8k_officer_changes(text, filing["filing_date"], cik, name)
                        store_8k_changes(officer_conn, cik, filing, changes, ts)
            else:
                for filing in new_8ks:
                    text = fetch_filing_text(edgar_session, filing["filing_url"], edgar_limiter, logger)
                    if not text:
                        # Surface the filing even if parsing fails
                        o_changes.append({
                            "change_type":  "Officer Change (see filing)",
                            "company_number": cik, "company_name": name,
                            "officer_name": "(see SEC filing)", "officer_role": "Director/Officer",
                            "appointed_on": filing["filing_date"], "resigned_on": "",
                            "date_detected": TODAY, "filing_url": filing.get("filing_url", ""),
                        })
                        continue
                    changes = parse_8k_officer_changes(text, filing["filing_date"], cik, name)
                    store_8k_changes(officer_conn, cik, filing, changes, ts)
                    for c in changes:
                        c["date_detected"] = TODAY
                        c["filing_url"]    = filing.get("filing_url", "")
                        senior = is_senior(c.get("officer_role", ""))
                        logger.log(
                            logging.WARNING if senior else logging.INFO,
                            "  OFFICER %s: %s | %s%s",
                            c["change_type"].upper(), c["officer_name"],
                            c["officer_role"], "  [SENIOR]" if senior else "",
                        )
                        o_changes.append(c)

            upsert_officer_firm(officer_conn, cik, name, ts)
            officer_conn.commit()

            # Director intelligence enrichment for new appointments
            for change in o_changes:
                if change["change_type"] != "New Appointment":
                    continue
                officer_name = change["officer_name"]
                if officer_name == "(see SEC filing)":
                    continue

                officer_key = make_officer_key(officer_name)
                profile = get_director_profile(director_conn, officer_key) if officer_key else None

                if profile is None or is_profile_stale(profile):
                    appts = fetch_officer_history_by_name(
                        edgar_session, officer_name, watchlist_numbers, edgar_limiter, logger
                    )
                    if officer_key and appts is not None:
                        profile = store_director_profile(
                            director_conn, officer_key, officer_name,
                            appts, watchlist_numbers, client_numbers, ts,
                        )

                if profile:
                    intel = build_director_intelligence(profile, cik, watchlist_names, client_names)
                    change["director_intel"] = intel
                    change["career_summary"] = intel["career_summary"]
                    change.update(flatten_for_csv(intel))

                    if intel["digital_background"]:
                        logger.info("  Director Intel [DIGITAL]: %s — %s",
                                    officer_name, "; ".join(intel["digital_roles"][:2]))
                    if intel["client_connections"]:
                        logger.warning("  Director Intel [CLIENT]: %s previously at %s",
                                       officer_name, ", ".join(c["company_name"] for c in intel["client_connections"]))
                    if intel["concurrent_watchlist"]:
                        logger.warning("  Director Intel [CONCURRENT]: %s also active at %s",
                                       officer_name, ", ".join(c["company_name"] for c in intel["concurrent_watchlist"]))

        else:
            logger.info("  Officers: private firm (no CIK) — skipping EDGAR")

        # ---- Jobs (Adzuna US) ----
        j_first = jobs_is_first_run(jobs_conn, cik)
        current_jobs = fetch_adzuna_jobs(adzuna_session, app_id, app_key, name, adzuna_limiter, logger)

        if current_jobs is None:
            adzuna_errors += 1
            j_changes: List[Dict] = []
        elif j_first:
            logger.info("  Jobs: first run (%d relevant postings)", len(current_jobs))
            for job in current_jobs:
                upsert_posting(jobs_conn, cik, name, job, ts)
            upsert_jobs_firm(jobs_conn, cik, name, ts)
            jobs_conn.commit()
            j_changes = []
        else:
            j_baseline = get_active_postings(jobs_conn, cik)
            j_changes = detect_job_changes(cik, name, j_baseline, current_jobs, row, logger)
            current_ids = {j["job_id"] for j in current_jobs}
            for job in current_jobs:
                upsert_posting(jobs_conn, cik, name, job, ts)
            for job_id in j_baseline:
                if job_id not in current_ids:
                    mark_posting_disappeared(jobs_conn, cik, job_id)
            upsert_jobs_firm(jobs_conn, cik, name, ts)
            jobs_conn.commit()
            if not j_changes:
                logger.info("  Jobs: no changes (%d relevant live)", len(current_jobs))

        # ---- Aggregate ----
        all_officer_changes.extend(o_changes)
        all_job_changes.extend(j_changes)

        for c in o_changes:
            if c["change_type"] == "New Appointment":
                officer_new += 1
            else:
                officer_resigned += 1
        for c in j_changes:
            if c["change_type"] == "New Posting":
                job_new += 1
            else:
                job_disappeared += 1

        firm_buffer[cik] = {
            "row":             row,
            "name":            name,
            "firm_type":       row.get(FIRMS_TYPE_COLUMN, ""),
            "o_changes":       o_changes,
            "j_changes":       j_changes,
            "active_job_count": len(current_jobs) if current_jobs is not None else 0,
        }

    officer_conn.close()
    jobs_conn.close()
    director_conn.close()

    # -------------------------------------------------------------------------
    # PASS 2 — NEWS (NewsAPI, priority-sorted, daily budget capped)
    # -------------------------------------------------------------------------
    news_api_key    = os.getenv("NEWS_API_KEY", "").strip()
    all_news_changes: List[Dict] = []
    news_calls_used = 0
    news_new        = 0

    if not news_api_key:
        logger.warning("NEWS_API_KEY not set — skipping news monitoring")
    else:
        news_conn = sqlite3.connect(NEWS_DB_PATH)
        news_conn.row_factory = sqlite3.Row
        init_news_db(news_conn)
        news_session = requests.Session()
        news_session.headers.update({"Accept": "application/json"})

        def _news_priority(cik: str) -> int:
            buf = firm_buffer.get(cik, {})
            has_signals = bool(buf.get("o_changes") or buf.get("j_changes"))
            return 0 if has_signals else 1

        sorted_ciks = sorted(firm_buffer.keys(), key=_news_priority)

        for pos, cik in enumerate(sorted_ciks):
            if news_calls_used >= NEWS_DAILY_BUDGET:
                logger.warning("News API budget reached — %d firm(s) skipped", len(sorted_ciks) - pos)
                break

            buf  = firm_buffer[cik]
            name = buf["name"]
            ts   = datetime.utcnow().isoformat()

            logger.info("[NEWS %d/%d] %s", pos + 1, len(sorted_ciks), name)
            n_first = news_is_first_run(news_conn, cik)
            articles, pages = fetch_newsapi(news_session, news_api_key, name, logger)
            news_calls_used += pages

            if articles is None:
                continue

            for art in articles:
                upsert_article(news_conn, cik, name, art, ts)
            upsert_news_firm(news_conn, cik, name, ts)
            news_conn.commit()

            if n_first:
                logger.info("  News: first run (%d relevant article(s) baselined)", len(articles))
                n_changes: List[Dict] = []
            else:
                baseline_urls = get_baseline_articles(news_conn, cik)
                n_changes = detect_news_changes(cik, name, baseline_urls, articles, n_first, logger)
                if not n_changes:
                    logger.info("  News: no new articles (%d relevant found)", len(articles))

            firm_buffer[cik]["n_changes"] = n_changes
            all_news_changes.extend(n_changes)
            news_new += len(n_changes)

        news_conn.close()

    # -------------------------------------------------------------------------
    # BUILD company_activity
    # -------------------------------------------------------------------------
    for cik, buf in firm_buffer.items():
        o_changes = buf["o_changes"]
        j_changes = buf["j_changes"]
        n_changes = buf.get("n_changes", [])
        if not (o_changes or j_changes or n_changes):
            continue
        signal, score = signal_score(o_changes, j_changes, n_changes)
        company_activity[cik] = {
            "name":             buf["name"],
            "firm_type":        buf.get("firm_type", ""),
            "officer_changes":  o_changes,
            "job_changes":      j_changes,
            "news_changes":     n_changes,
            "active_job_count": buf["active_job_count"],
            "signal":           signal,
            "score":            score,
        }

    # -------------------------------------------------------------------------
    # CROSS-FIRM ALERTS
    # -------------------------------------------------------------------------
    cross_firm_alerts = collect_cross_firm_appointments(all_officer_changes)
    if cross_firm_alerts:
        logger.info("Cross-firm appointments: %d director(s) across multiple firms", len(cross_firm_alerts))
        for alert in cross_firm_alerts:
            logger.warning("  CROSS-FIRM: %s at %d firms: %s",
                           alert["officer_name"], alert["firm_count"],
                           ", ".join(f["company_name"] for f in alert["firms"]))

    # -------------------------------------------------------------------------
    # LLM COMMENTARY
    # -------------------------------------------------------------------------
    if os.getenv("ANTHROPIC_API_KEY", "").strip():
        logger.info("Generating LLM commentary for %d firm(s)...", len(company_activity))
        for cik, activity in company_activity.items():
            activity["llm_commentary"] = generate_llm_commentary(
                activity["name"], activity.get("firm_type", ""), activity, logger
            )
        for alert in cross_firm_alerts:
            alert["llm_commentary"] = generate_cross_firm_commentary(
                alert["officer_name"], alert["role"], alert["firms"], logger
            )
    else:
        logger.info("ANTHROPIC_API_KEY not set — skipping LLM commentary")

    for change in all_officer_changes:
        cik = change["company_number"]
        change["llm_commentary"] = company_activity.get(cik, {}).get("llm_commentary", "")

    write_digest(OFFICER_DIGEST, OFFICER_COLUMNS, all_officer_changes, "No officer changes detected", logger)
    write_digest(JOBS_DIGEST, JOBS_COLUMNS, all_job_changes, "No job changes detected", logger)
    write_digest(NEWS_DIGEST, NEWS_COLUMNS, all_news_changes, "No news articles detected", logger)

    stats = {
        "firms_checked":   total_firms,
        "edgar_firms":     edgar_firms,
        "private_firms":   private_firms,
        "officer_new":     officer_new,
        "officer_resigned":officer_resigned,
        "job_new":         job_new,
        "job_disappeared": job_disappeared,
        "news_new":        news_new,
        "edgar_errors":    edgar_errors,
        "adzuna_errors":   adzuna_errors,
    }

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info("Firms checked       : %d (%d EDGAR, %d private)", total_firms, edgar_firms, private_firms)
    logger.info("New appointments    : %d", officer_new)
    logger.info("Resignations        : %d", officer_resigned)
    logger.info("New job postings    : %d", job_new)
    logger.info("Disappeared postings: %d", job_disappeared)
    logger.info("New news articles   : %d", news_new)
    logger.info("News API calls used : %d / %d", news_calls_used, NEWS_DAILY_BUDGET)
    logger.info("EDGAR errors        : %d", edgar_errors)
    logger.info("Adzuna errors       : %d", adzuna_errors)
    logger.info("=" * 60)

    send_email(all_officer_changes, all_job_changes, all_news_changes,
               company_activity, cross_firm_alerts, stats, logger)


if __name__ == "__main__":
    main()
