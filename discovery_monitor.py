"""
discovery_monitor.py  (US/NYC edition)

Scans US insurance and InsurTech trade press RSS feeds to identify firms that
may be new prospects, based on funding, acquisition, appointment, or
transformation events. Runs independently on a weekly schedule.

Produces:
  output/discovery_YYYY_WW.csv           -- all triggered articles
  output/discovery_new_firms_YYYY_WW.csv -- unknown firms only, for seed list review
  input/pending_firms.csv                -- EDGAR-confirmed new firms awaiting promotion
  Weekly email digest
"""

import csv
import logging
import os
import re
import smtplib
import sqlite3
import sys
import time
from datetime import date, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import feedparser
import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

from edgar_client import RateLimiter, build_edgar_session, lookup_firm_on_edgar

# ---------------------------------------------------------------------------
# FEEDS — US insurance and InsurTech trade press
# ---------------------------------------------------------------------------
FEEDS = [
    # US insurance trade press
    {"name": "Insurance Journal",       "url": "https://www.insurancejournal.com/feed/"},
    {"name": "Carrier Management",      "url": "https://carriermanagement.com/feed/"},
    {"name": "PropertyCasualty360",     "url": "https://www.propertycasualty360.com/feed/"},
    {"name": "Business Insurance",      "url": "https://www.businessinsurance.com/section/feed/RSS"},
    # InsurTech / innovation
    {"name": "Coverager",               "url": "https://coverager.com/feed/"},
    {"name": "Artemis",                 "url": "https://www.artemis.bm/news/feed/"},
    {"name": "Reinsurance News",        "url": "https://www.reinsurancene.ws/feed/"},
    # Broader tech / financial services
    {"name": "TechCrunch",             "url": "https://techcrunch.com/feed"},
    {"name": "FinTech Futures",         "url": "https://www.fintechfutures.com/feed/"},
    {"name": "CB Insights Fintech",     "url": "https://www.cbinsights.com/research/feed"},
]

# ---------------------------------------------------------------------------
# TRIGGER KEYWORDS
# ---------------------------------------------------------------------------
TRIGGER_KEYWORDS = [
    # Funding and capital events
    "raises", "funding round", "series A", "series B", "series C",
    "seed funding", "venture capital", "growth capital", "private equity",
    # M&A and structural events
    "acquires", "acquisition", "merger", "management buyout", "buys",
    # People and strategy signals
    "appoints", "appointed", "strategic review", "names new",
    # Sector-specific
    "insurtech", "insuretech", "MGA", "program business",
    # Transformation signals
    "digital transformation", "technology platform", "core system",
    "modernization", "modernisation", "legacy replacement", "cloud migration",
]

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
FUZZY_MATCH_THRESHOLD = 80
ARTICLE_LOOKBACK_DAYS = 7

FIRM_EXTRACTION_STOP_WORDS = frozenset({
    "The", "A", "An", "In", "At", "For", "On", "By", "To", "With", "Of",
    "And", "Or", "But", "New", "First", "Last", "Latest", "Week", "Annual",
    "US", "UK", "EU", "NYC", "New", "York", "America", "Global", "International",
    "North", "South", "East", "West", "American",
    "Q1", "Q2", "Q3", "Q4", "CEO", "CTO", "CFO", "CIO", "MD", "COO", "Chair",
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
    "Report", "Update", "News", "Review", "Analysis", "Comment", "Feature",
    "How", "Why", "What", "When", "Where", "Which", "Who",
})
_FEED_PUBLICATION_NAMES = frozenset(f["name"] for f in FEEDS)

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
LOGS_DIR   = SCRIPT_DIR / "logs"

FIRMS_CSV         = INPUT_DIR / "firms.csv"
PENDING_FIRMS_CSV = INPUT_DIR / "pending_firms.csv"
DISCOVERY_DB_PATH = DATA_DIR / "discovery_baseline.db"

TODAY    = date.today()
_ISO     = TODAY.isocalendar()
YEAR     = _ISO[0]
WEEK     = _ISO[1]
WEEK_STR = f"{YEAR}-W{WEEK:02d}"

DISCOVERY_DIGEST           = OUTPUT_DIR / f"discovery_{YEAR}_{WEEK:02d}.csv"
DISCOVERY_NEW_FIRMS_DIGEST = OUTPUT_DIR / f"discovery_new_firms_{YEAR}_{WEEK:02d}.csv"
LOG_FILE                   = LOGS_DIR   / f"discovery_{TODAY.isoformat()}.log"

DIGEST_COLUMNS = [
    "article_title", "article_url", "feed_name", "article_published",
    "trigger_keywords", "extracted_firm_name", "known_firm",
    "cik", "suggested_action",
]

# Columns for pending_firms.csv — mirrors firms.csv plus discovery metadata.
# To promote a firm: copy the first 7 columns into firms.csv and delete the row here.
PENDING_COLUMNS = [
    "Company Name", "CIK", "NAIC Code", "Ticker", "Company Type",
    "monitoring_status", "notes",
    "discovered_date", "discovery_source", "edgar_match_score",
]

# EDGAR rate limit — shared with intelligence_monitor but discovery runs separately
EDGAR_REQUESTS_PER_SECOND = 5  # conservative for discovery (no urgency)


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("discovery_monitor_us")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# ---------------------------------------------------------------------------
# DATABASE
# ---------------------------------------------------------------------------
def init_discovery_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS discovery_articles (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            feed_name           TEXT,
            article_url         TEXT UNIQUE,
            article_title       TEXT,
            article_description TEXT,
            article_published   DATE,
            trigger_keywords    TEXT,
            extracted_firm_name TEXT,
            known_firm          INTEGER,
            cik                 TEXT,
            first_seen          TIMESTAMP,
            included_in_digest  INTEGER DEFAULT 0
        );
    """)
    conn.commit()


def upsert_article(conn: sqlite3.Connection, article: Dict, ts: str) -> bool:
    cur = conn.execute(
        """INSERT OR IGNORE INTO discovery_articles
           (feed_name, article_url, article_title, article_description,
            article_published, trigger_keywords, extracted_firm_name,
            known_firm, cik, first_seen, included_in_digest)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)""",
        (
            article["feed_name"], article["article_url"], article["article_title"],
            article["article_description"], article["article_published"],
            article["trigger_keywords"], article["extracted_firm_name"],
            1 if article["known_firm"] else 0, article["cik"], ts,
        ),
    )
    return cur.rowcount > 0


def get_undigested_articles(conn: sqlite3.Connection) -> List[Dict]:
    rows = conn.execute(
        "SELECT * FROM discovery_articles WHERE included_in_digest = 0 "
        "ORDER BY article_published DESC"
    ).fetchall()
    return [dict(r) for r in rows]


def mark_articles_included(conn: sqlite3.Connection, ids: List[int]) -> None:
    if not ids:
        return
    placeholders = ",".join("?" * len(ids))
    conn.execute(
        f"UPDATE discovery_articles SET included_in_digest = 1 WHERE id IN ({placeholders})",
        ids,
    )


# ---------------------------------------------------------------------------
# PENDING FIRMS CSV
# ---------------------------------------------------------------------------

def load_pending_firms() -> Dict[str, Dict]:
    """Return existing pending_firms.csv keyed by CIK (to avoid duplicates)."""
    if not PENDING_FIRMS_CSV.exists():
        return {}
    pending: Dict[str, Dict] = {}
    with PENDING_FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            cik = row.get("CIK", "").strip()
            if cik:
                pending[cik] = row
    return pending


def load_monitored_ciks() -> set:
    """Return all CIKs already in firms.csv (to avoid re-surfacing known firms)."""
    if not FIRMS_CSV.exists():
        return set()
    ciks = set()
    with FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            cik = row.get("CIK", "").strip()
            if cik:
                ciks.add(cik)
    return ciks


def append_pending_firm(edgar_result: Dict, discovered_date: str,
                        source_article: str, logger: logging.Logger) -> bool:
    """Add a new firm to pending_firms.csv if not already present. Returns True if added."""
    cik = edgar_result["cik"]
    existing = load_pending_firms()
    monitored = load_monitored_ciks()

    if cik in monitored:
        logger.debug("  Pending: CIK %s already monitored — skipping", cik)
        return False
    if cik in existing:
        logger.debug("  Pending: CIK %s already pending — skipping", cik)
        return False

    write_header = not PENDING_FIRMS_CSV.exists()
    with PENDING_FIRMS_CSV.open("a", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=PENDING_COLUMNS, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({
            "Company Name":       edgar_result["company_name"],
            "CIK":                cik,
            "NAIC Code":          "",
            "Ticker":             "",
            "Company Type":       edgar_result["company_type"],
            "monitoring_status":  "pending",
            "notes":              edgar_result["sic_description"],
            "discovered_date":    discovered_date,
            "discovery_source":   source_article[:120],
            "edgar_match_score":  edgar_result["match_score"],
        })

    logger.info("  PENDING: added %s (CIK %s, SIC %s, score %d)",
                edgar_result["company_name"], cik,
                edgar_result["sic"], edgar_result["match_score"])
    return True


def load_all_pending() -> List[Dict]:
    """Return all rows from pending_firms.csv."""
    if not PENDING_FIRMS_CSV.exists():
        return []
    with PENDING_FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


# ---------------------------------------------------------------------------
# FEED FETCHING
# ---------------------------------------------------------------------------
def fetch_feed(name: str, url: str, session: requests.Session,
               logger: logging.Logger) -> list:
    try:
        resp = session.get(url, timeout=(5, 15))
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Feed failed [%s]: %s", name, exc)
        return []
    feed = feedparser.parse(resp.text)
    if feed.bozo and feed.entries:
        logger.warning("Feed parse warning [%s]: %s", name,
                       getattr(feed, "bozo_exception", "unknown"))
    logger.info("Fetched [%s]: %d entries", name, len(feed.entries))
    return feed.entries


def is_recent(entry) -> bool:
    parsed = entry.get("published_parsed")
    if not parsed:
        return True
    try:
        pub = date(*parsed[:3])
        return (TODAY - pub).days <= ARTICLE_LOOKBACK_DAYS
    except (TypeError, ValueError):
        return True


def get_entry_text(entry) -> Tuple[str, str]:
    title = (entry.get("title") or "").strip()
    desc  = (entry.get("summary") or entry.get("description") or "").strip()
    desc  = re.sub(r"<[^>]+>", " ", desc)
    desc  = re.sub(r"\s+", " ", desc).strip()
    return title, desc


def get_entry_date(entry) -> str:
    parsed = entry.get("published_parsed")
    if parsed:
        try:
            return date(*parsed[:3]).isoformat()
        except (TypeError, ValueError):
            pass
    return ""


def get_entry_url(entry) -> str:
    return (entry.get("link") or entry.get("id") or "").strip()


# ---------------------------------------------------------------------------
# KEYWORD MATCHING
# ---------------------------------------------------------------------------
def find_trigger_keywords(title: str, description: str) -> List[str]:
    text = f"{title} {description}".lower()
    return [kw for kw in TRIGGER_KEYWORDS if kw.lower() in text]


# ---------------------------------------------------------------------------
# FIRM NAME EXTRACTION
# ---------------------------------------------------------------------------
_TITLE_CASE_SEQ = re.compile(r"\b[A-Z][A-Za-z&]+(?:\s+[A-Z][A-Za-z&]+)+\b")


def extract_firm_name(title: str, logger: logging.Logger) -> Optional[str]:
    candidates = []
    for seq in _TITLE_CASE_SEQ.findall(title):
        words = seq.split()
        meaningful = [
            w for w in words
            if w not in FIRM_EXTRACTION_STOP_WORDS
            and w not in _FEED_PUBLICATION_NAMES
        ]
        if meaningful:
            if len(meaningful) == 1:
                logger.debug("  Low-confidence extraction: %r (only 1 meaningful word)", seq)
            candidates.append(seq)
    return candidates[0] if candidates else None


# ---------------------------------------------------------------------------
# KNOWN FIRM MATCHING
# ---------------------------------------------------------------------------
def match_known_firm(
    firm_name: Optional[str], firms: List[Dict]
) -> Tuple[bool, str, str]:
    """Returns (is_known, matched_company_name, cik)."""
    if not firm_name:
        return False, "", ""
    best_score, best_firm = 0, None
    for firm in firms:
        score = fuzz.token_set_ratio(
            firm_name.lower(), firm.get("Company Name", "").lower()
        )
        if score > best_score:
            best_score, best_firm = score, firm
    if best_score >= FUZZY_MATCH_THRESHOLD and best_firm:
        return True, best_firm["Company Name"], best_firm.get("CIK", "")
    return False, "", ""


def suggested_action(known: bool) -> str:
    return "Already monitored" if known else "Review for seed list"


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------
def write_discovery_digests(
    undigested: List[Dict], logger: logging.Logger
) -> Tuple[List[Dict], List[Dict]]:
    OUTPUT_DIR.mkdir(exist_ok=True)
    all_rows: List[Dict] = []
    new_rows: List[Dict] = []

    for rec in undigested:
        known_label = "Yes" if rec["known_firm"] else "No"
        row = {
            "article_title":       rec["article_title"],
            "article_url":         rec["article_url"],
            "feed_name":           rec["feed_name"],
            "article_published":   rec["article_published"],
            "trigger_keywords":    rec["trigger_keywords"],
            "extracted_firm_name": rec["extracted_firm_name"] or "",
            "known_firm":          known_label,
            "cik":                 rec["cik"] or "",
            "suggested_action":    suggested_action(bool(rec["known_firm"])),
        }
        all_rows.append(row)
        if not rec["known_firm"]:
            new_rows.append(row)

    with DISCOVERY_DIGEST.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=DIGEST_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        if all_rows:
            writer.writerows(all_rows)
        else:
            writer.writerow({c: "" for c in DIGEST_COLUMNS} | {
                "article_title": "No triggered articles this week",
                "article_published": TODAY.isoformat(),
            })
    logger.info("Digest written -> %s", DISCOVERY_DIGEST.name)

    with DISCOVERY_NEW_FIRMS_DIGEST.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=DIGEST_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        if new_rows:
            writer.writerows(new_rows)
        else:
            writer.writerow({c: "" for c in DIGEST_COLUMNS} | {
                "article_title": "No new firms identified this week",
                "article_published": TODAY.isoformat(),
            })
    logger.info("New firms digest written -> %s", DISCOVERY_NEW_FIRMS_DIGEST.name)

    return all_rows, new_rows


# ---------------------------------------------------------------------------
# EMAIL
# ---------------------------------------------------------------------------
def send_weekly_email(
    stats: Dict,
    all_rows: List[Dict],
    new_rows: List[Dict],
    pending_firms: List[Dict],
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

    recipients   = [a.strip() for a in email_to_raw.split(",") if a.strip()]
    known_rows   = [r for r in all_rows if r["known_firm"] == "Yes"]
    new_pending  = [p for p in pending_firms if p.get("discovered_date") == TODAY.isoformat()]
    subject      = (
        f"US | Discovery Report: {len(new_rows)} new firm(s), "
        f"{len(known_rows)} known firm event(s)"
        + (f", {len(new_pending)} auto-resolved" if new_pending else "")
        + f" — {WEEK_STR}"
    )

    lines = [
        f"US DISCOVERY REPORT — Week {WEEK}, {YEAR}",
        "=" * 60,
        f"NEW FIRMS IDENTIFIED      : {stats['new_firms']}",
        f"  - EDGAR auto-resolved   : {stats['edgar_resolved']}",
        f"  - Needs manual lookup   : {stats['new_firms'] - stats['edgar_resolved']}",
        f"KNOWN FIRMS WITH ACTIVITY : {stats['known_firms']}",
        f"FEEDS MONITORED           : {stats['feeds_fetched']}",
        f"FEEDS FAILED              : {stats['feeds_failed']}",
        f"ARTICLES REVIEWED         : {stats['articles_reviewed']}",
        f"ARTICLES TRIGGERED        : {stats['articles_triggered']}",
        f"TOTAL PENDING FIRMS       : {len(pending_firms)}",
        "",
    ]

    if new_pending:
        lines.append("AUTO-RESOLVED THIS WEEK (added to pending_firms.csv)")
        lines.append("-" * 60)
        lines.append("To add to monitoring: copy these rows into input/firms.csv")
        lines.append("")
        for p in new_pending:
            lines.append(
                f"  {p['Company Name']}  |  CIK: {p['CIK']}  |  {p['notes']}  "
                f"|  match score: {p['edgar_match_score']}"
            )
            lines.append(f"  Source: {p['discovery_source']}")
            lines.append("")

    if len(pending_firms) > len(new_pending):
        older = [p for p in pending_firms if p.get("discovered_date") != TODAY.isoformat()]
        lines.append(f"PREVIOUSLY PENDING ({len(older)} firm(s) still awaiting review)")
        lines.append("-" * 60)
        for p in older[:10]:
            lines.append(f"  {p['Company Name']}  |  CIK: {p['CIK']}  |  discovered {p['discovered_date']}")
        if len(older) > 10:
            lines.append(f"  ... and {len(older) - 10} more in pending_firms.csv")
        lines.append("")

    if new_rows:
        lines.append("NEW FIRMS (no EDGAR match — manual review)")
        lines.append("-" * 60)
        for r in new_rows:
            firm = r["extracted_firm_name"] or "(firm not identified)"
            lines.append(
                f"{firm}  —  {r['trigger_keywords']}  —  {r['feed_name']}  —  {r['article_published']}"
            )
            lines.append(f"  {r['article_title']}")
            lines.append(f"  {r['article_url']}")
            lines.append("")
    else:
        lines.append("No unresolved new firms this week.")
        lines.append("")

    if known_rows:
        lines.append("KNOWN FIRMS WITH ACTIVITY")
        lines.append("-" * 60)
        for r in known_rows:
            firm = r["extracted_firm_name"] or r["article_title"][:50]
            lines.append(
                f"{firm}  —  {r['trigger_keywords']}  —  {r['feed_name']}  —  {r['article_published']}"
            )
            lines.append(f"  {r['article_title']}")
            lines.append(f"  {r['article_url']}")
            lines.append("")

    lines.append("Full digests attached.")
    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["From"]    = email_from
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for path in (DISCOVERY_DIGEST, DISCOVERY_NEW_FIRMS_DIGEST, PENDING_FIRMS_CSV):
        if path.exists():
            with path.open("rb") as fh:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(fh.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={path.name}")
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
        logger.info("Weekly email sent to %s", ", ".join(recipients))
    except smtplib.SMTPException as exc:
        logger.error("Failed to send email: %s", exc)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("US Discovery Monitor — %s", WEEK_STR)
    logger.info("=" * 60)

    load_dotenv()

    if not FIRMS_CSV.exists():
        logger.error("Firms CSV not found: %s", FIRMS_CSV)
        sys.exit(1)

    with FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        firms = list(csv.DictReader(fh))
    logger.info("Loaded %d firms from %s", len(firms), FIRMS_CSV.name)

    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DISCOVERY_DB_PATH)
    conn.row_factory = sqlite3.Row
    init_discovery_db(conn)

    rss_session = requests.Session()
    rss_session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; insure-us-discovery/1.0)",
        "Accept":     "application/rss+xml, application/atom+xml, text/xml, */*",
    })

    edgar_user_agent = os.getenv(
        "EDGAR_USER_AGENT",
        f"InsureMonitorUS {os.getenv('EMAIL_FROM', 'contact@example.com')}"
    )
    edgar_session = build_edgar_session(edgar_user_agent)
    edgar_limiter = RateLimiter(EDGAR_REQUESTS_PER_SECOND)

    digest_already_produced = DISCOVERY_DIGEST.exists()
    if digest_already_produced:
        logger.info("Digest already produced for %s — will fetch and store but skip digest/email", WEEK_STR)

    feeds_fetched      = 0
    feeds_failed       = 0
    articles_reviewed  = 0
    articles_triggered = 0
    new_firm_count     = 0
    known_firm_count   = 0
    # Collect unique new firm names seen this run for EDGAR lookup
    new_firm_candidates: Dict[str, str] = {}   # firm_name -> source article title
    ts = datetime.now().isoformat()

    for feed in FEEDS:
        name    = feed["name"]
        url     = feed["url"]
        entries = fetch_feed(name, url, rss_session, logger)

        if not entries:
            feeds_failed += 1
            continue
        feeds_fetched += 1

        feed_triggered = 0
        for entry in entries:
            if not is_recent(entry):
                continue
            article_url = get_entry_url(entry)
            if not article_url:
                continue

            articles_reviewed += 1
            title, desc = get_entry_text(entry)
            keywords = find_trigger_keywords(title, desc)
            if not keywords:
                continue

            articles_triggered += 1
            feed_triggered += 1

            firm_name = extract_firm_name(title, logger)
            is_known, matched_name, cik = match_known_firm(firm_name, firms)

            if is_known:
                known_firm_count += 1
                logger.info("  TRIGGER [%s] known firm: %s — %s", name, matched_name, ", ".join(keywords))
            else:
                new_firm_count += 1
                logger.info("  TRIGGER [%s] new firm candidate: %s — %s",
                            name, firm_name or "(unknown)", ", ".join(keywords))
                if firm_name and firm_name not in new_firm_candidates:
                    new_firm_candidates[firm_name] = title

            article = {
                "feed_name":           name,
                "article_url":         article_url,
                "article_title":       title,
                "article_description": desc[:500],
                "article_published":   get_entry_date(entry),
                "trigger_keywords":    ", ".join(keywords),
                "extracted_firm_name": firm_name or "",
                "known_firm":          is_known,
                "cik":                 cik,
            }
            upsert_article(conn, article, ts)

        if feed_triggered:
            logger.info("  [%s] %d triggered article(s) this run", name, feed_triggered)

    conn.commit()

    # -------------------------------------------------------------------------
    # EDGAR AUTO-RESOLUTION: look up each new firm candidate by name
    # -------------------------------------------------------------------------
    edgar_resolved = 0
    if new_firm_candidates:
        logger.info("EDGAR lookup for %d new firm candidate(s)...", len(new_firm_candidates))
        for firm_name, source_title in new_firm_candidates.items():
            result = lookup_firm_on_edgar(firm_name, edgar_session, edgar_limiter, logger)
            if result:
                added = append_pending_firm(result, TODAY.isoformat(), source_title, logger)
                if added:
                    edgar_resolved += 1
            else:
                logger.debug("  No EDGAR match for '%s'", firm_name)
        logger.info("EDGAR resolved %d / %d new firm(s)", edgar_resolved, len(new_firm_candidates))
    else:
        logger.info("No new firm candidates to look up on EDGAR")

    pending_firms = load_all_pending()

    if not digest_already_produced:
        undigested = get_undigested_articles(conn)
        if undigested:
            all_rows, new_rows = write_discovery_digests(undigested, logger)
            mark_articles_included(conn, [r["id"] for r in undigested])
            conn.commit()
        else:
            all_rows, new_rows = [], []
            logger.info("No undigested articles — writing empty digests")
            write_discovery_digests([], logger)

        # Filter new_rows to only those without an EDGAR match (pending firms
        # are already captured in pending_firms.csv, no need to repeat them)
        pending_names = {p["Company Name"].lower() for p in pending_firms
                        if p.get("discovered_date") == TODAY.isoformat()}
        unresolved_new = [
            r for r in new_rows
            if (r.get("extracted_firm_name") or "").lower() not in pending_names
        ]

        stats = {
            "feeds_fetched":      feeds_fetched,
            "feeds_failed":       feeds_failed,
            "articles_reviewed":  articles_reviewed,
            "articles_triggered": articles_triggered,
            "new_firms":          len(new_rows),
            "edgar_resolved":     edgar_resolved,
            "known_firms":        len(all_rows) - len(new_rows),
        }
        send_weekly_email(stats, all_rows, unresolved_new, pending_firms, logger)
    else:
        stats = {
            "feeds_fetched":      feeds_fetched,
            "feeds_failed":       feeds_failed,
            "articles_reviewed":  articles_reviewed,
            "articles_triggered": articles_triggered,
            "new_firms":          new_firm_count,
            "edgar_resolved":     edgar_resolved,
            "known_firms":        known_firm_count,
        }

    conn.close()

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary — %s", WEEK_STR)
    logger.info("=" * 60)
    logger.info("Feeds fetched        : %d", stats["feeds_fetched"])
    logger.info("Feeds failed         : %d", stats["feeds_failed"])
    logger.info("Articles reviewed    : %d", stats["articles_reviewed"])
    logger.info("Articles triggered   : %d", stats["articles_triggered"])
    logger.info("New firm candidates  : %d", stats["new_firms"])
    logger.info("  EDGAR resolved     : %d", stats["edgar_resolved"])
    logger.info("Known firm events    : %d", stats["known_firms"])
    logger.info("Total pending firms  : %d", len(pending_firms))
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
