# app/personio_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin
import logging
import random
import re
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]


# ============================================================================
# URL HELPERS
# ============================================================================

def is_personio_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return "jobs.personio.de" in host or "jobs.personio.com" in host


def _base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _company_name_from_host(netloc: str) -> str:
    slug = netloc.split(".")[0]
    return " ".join(w.capitalize() for w in re.split(r"[-_]+", slug) if w)


def _job_id_from_path(path: str) -> str | None:
    """Extract numeric job ID from a Personio job path like /job/2599449."""
    parts = [p for p in path.strip("/").split("/") if p]
    for i, part in enumerate(parts):
        if part == "job" and i + 1 < len(parts):
            candidate = parts[i + 1].split("?")[0]
            if candidate.isdigit():
                return candidate
    return None


# ============================================================================
# DATE EXTRACTION FROM EMBEDDED RSC/SCRIPT DATA
# ============================================================================

def _extract_job_dates(html: str, job_ids: list[str]) -> dict[str, str]:
    """
    Personio SSR pages embed job data in Next.js RSC script blocks where JSON
    is escaped as \\\" (literal backslash + quote). Scan after each job ID
    occurrence for the nearest published_at value.
    """
    # Matches both escaped form \"published_at\":\"2026-...Z\" and plain form
    _DATE_RE = re.compile(r'published_at\\?"\\?\s*:\s*\\?"([^"\\]+)')

    dates: dict[str, str] = {}
    for job_id in job_ids:
        for m in re.finditer(re.escape(job_id), html):
            window = html[m.start(): m.start() + 1500]
            date_m = _DATE_RE.search(window)
            if date_m:
                dates[job_id] = date_m.group(1)
                break
    return dates


def _parse_published(published_at: str, since_dt: datetime | None) -> tuple[str, bool]:
    """
    Parse a published_at ISO string. Returns (date_str, skip).
    skip=True means the job is older than since_dt and should be excluded.
    """
    if not published_at:
        return "", False
    try:
        normalized = published_at.replace("Z", "+00:00") if published_at.endswith("Z") else published_at
        pub_dt = datetime.fromisoformat(normalized)
        if pub_dt.tzinfo is None:
            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
        if since_dt and pub_dt < since_dt:
            return published_at[:10], True
        return published_at[:10], False
    except Exception:
        return published_at[:10] if len(published_at) >= 10 else "", False


# ============================================================================
# GET JOB LINKS — static HTML board page
# ============================================================================

def get_links_from_personio(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch Personio job board via static HTML (Next.js SSR — no JS needed).

    Board layout:
      Departments:  section.jobs-group > h2.jobs-group-header
      Job links:    a.job-box[href="/job/{id}"]
      Title:        h3.jb-title
      Meta items:   span[class*='jobMetaText']  (location / schedule / contract)

    Date data is embedded as escaped JSON in RSC script blocks, containing
    published_at per job ID. Used for since_dt filtering.
    """
    base = _base_url(board_url)
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(board_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Personio board: HTTP {resp.status_code}")
            return []
    except Exception as e:
        logger.warning(f"   Personio board fetch failed: {e}")
        return []

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    raw_jobs: list[dict] = []

    # ── Layout A: department groups ────────────────────────────────────────
    groups = soup.select("section.jobs-group")
    if groups:
        for group in groups:
            dept_el = group.select_one("h2.jobs-group-header")
            department = dept_el.get_text(strip=True) if dept_el else ""

            for job_box in group.select("a.job-box[href]"):
                href = job_box.get("href", "")
                job_id = _job_id_from_path(href)
                if not job_id:
                    continue

                title_el = job_box.select_one("h3.jb-title") or job_box.select_one("h3")
                title = title_el.get_text(strip=True) if title_el else ""
                if not title:
                    continue

                meta_spans = job_box.select("span[class*='jobMetaText']")
                location = meta_spans[0].get_text(strip=True) if len(meta_spans) > 0 else ""
                schedule = meta_spans[1].get_text(strip=True) if len(meta_spans) > 1 else ""
                emp_type = meta_spans[2].get_text(strip=True) if len(meta_spans) > 2 else ""

                raw_jobs.append({
                    "job_id":     job_id,
                    "url":        urljoin(base, href),
                    "title":      title,
                    "department": department,
                    "location":   location,
                    "schedule":   schedule,
                    "type":       emp_type,
                })

    # ── Layout B: flat list fallback ───────────────────────────────────────
    if not raw_jobs:
        for job_box in soup.select("a.job-box[href]"):
            href = job_box.get("href", "")
            job_id = _job_id_from_path(href)
            if not job_id:
                continue

            title_el = job_box.select_one("h3.jb-title") or job_box.select_one("h3")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            meta_spans = job_box.select("span[class*='jobMetaText']")
            location = meta_spans[0].get_text(strip=True) if len(meta_spans) > 0 else ""

            raw_jobs.append({
                "job_id":   job_id,
                "url":      urljoin(base, href),
                "title":    title,
                "location": location,
            })

    if not raw_jobs:
        logger.warning("   Personio board: no jobs found in HTML")
        return []

    # ── Extract dates from embedded RSC data ───────────────────────────────
    job_ids = [j["job_id"] for j in raw_jobs]
    date_map = _extract_job_dates(html, job_ids)

    jobs: list[dict] = []
    for raw in raw_jobs:
        pub_str = date_map.get(raw["job_id"], "")
        published, skip = _parse_published(pub_str, since_dt)
        if skip:
            continue

        job: dict = {
            "url":       raw["url"],
            "jobId":     raw["job_id"],
            "title":     raw["title"],
            "published": published,
            "method":    "html",
        }
        for key in ("department", "location", "type"):
            if raw.get(key):
                job[key] = raw[key]

        jobs.append(job)

    logger.info(f"✅ Personio: {len(jobs)} jobs")
    return jobs


# ============================================================================
# JOB DETAIL — DOM scrape
# ============================================================================

def extract_job_with_personio(job_url: str) -> dict | None:
    """
    Scrape a Personio job detail page.

    Confirmed selectors:
      Title         h1.detail-title | h1.job-position-title | h1[class*='title']
      Published     extracted from embedded RSC JSON (published_at)
      Location      span[class*='locationText']
      Meta items    div.jb-attribute span[class*='jobMetaText']
      Sections      div.jb-description-item > h2.detail-block-title
                                             > div.rich-text-content
      Apply URL     a[class*='action-btn'][href*='apply'] | a[href*='?apply']
    """
    parsed = urlparse(job_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    job_id = _job_id_from_path(parsed.path)
    if not job_id:
        logger.warning(f"   Personio: cannot parse job URL: {job_url}")
        return None

    clean_url = f"{base}/job/{job_id}"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(clean_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Personio job: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   Personio job fetch failed: {e}")
        return None

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")

    # ── Title ──────────────────────────────────────────────────────────────
    title_el = (
        soup.select_one("h1.detail-title") or
        soup.select_one("h1.job-position-title") or
        soup.select_one("h1[class*='title']") or
        soup.select_one("h1")
    )
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title:
        return None

    job: dict = {
        "jobId": job_id,
        "url":   clean_url,
        "title": title,
        "company": _company_name_from_host(parsed.netloc),
    }

    # ── Published date from embedded RSC data ──────────────────────────────
    # On detail pages the job ID and published_at may be far apart; scan all
    date_m = re.search(r'published_at\\?"\\?\s*:\s*\\?"([^"\\]+)', html)
    if date_m:
        job["published"] = date_m.group(1)[:10]

    # ── Location ───────────────────────────────────────────────────────────
    loc_el = (
        soup.select_one("span[class*='locationText']") or
        soup.select_one("div[class*='jobMetaItemLocation'] span")
    )
    if loc_el:
        loc = loc_el.get_text(strip=True)
        if loc:
            job["location"] = loc

    # ── Meta attributes (schedule, employment type) ────────────────────────
    # The location item's parent div contains "jobMetaItemLocation" in its class;
    # exclude those so we only capture schedule and employment-type spans.
    all_meta = soup.select("div.jb-attribute span[class*='jobMetaText']")
    meta_spans = [
        s for s in all_meta
        if not s.find_parent(attrs={"class": re.compile(r"jobMetaItemLocation")})
    ]

    if len(meta_spans) >= 1:
        schedule = meta_spans[0].get_text(strip=True)
        if schedule:
            job["schedule"] = schedule
    if len(meta_spans) >= 2:
        emp_type = meta_spans[1].get_text(strip=True)
        if emp_type:
            job["type"] = emp_type

    # ── Job description sections ───────────────────────────────────────────
    # Keyword sets cover common English and German headers used by Personio clients.
    _DESC_KW = {"description", "role", "responsibilit", "about the job", "position",
                "stelle", "aufgabe", "tätigk", "über uns", "hallo", "schön", "wir sind",
                "die stelle", "deine aufgabe", "was dich erwartet", "what you"}
    _REQ_KW  = {"requirement", "qualification", "skill", "experience", "you bring", "you have",
                "anforderung", "qualifikation", "erfahrung", "kenntnisse", "mitbringst",
                "bringst du", "was du mitbringst", "das bringst", "profil"}
    _BEN_KW  = {"benefit", "we offer", "offer", "why join", "perks",
                "bieten", "angebot", "vorteile", "warum", "das bieten", "was wir bieten",
                "möchten wir", "das möchten"}

    for section in soup.select("div.jb-description-item"):
        header_el = section.select_one("h2.detail-block-title") or section.select_one("h2")
        content_el = section.select_one("div.rich-text-content") or section.select_one("div")
        if not content_el:
            continue

        header = header_el.get_text(strip=True).lower() if header_el else ""
        content_html = str(content_el)
        if len(content_html) < 30:
            continue

        if any(k in header for k in _REQ_KW):
            job.setdefault("requirements", content_html)
        elif any(k in header for k in _BEN_KW):
            job.setdefault("benefits", content_html)
        elif any(k in header for k in _DESC_KW) or not header:
            job.setdefault("description", content_html)
        else:
            # Unknown header — fold into description so content isn't lost
            job.setdefault("description", content_html)

    # ── Apply URL ──────────────────────────────────────────────────────────
    apply_el = (
        soup.select_one("a.action-btn-wrapper[href]") or
        soup.select_one("a[class*='action-btn'][href*='apply']") or
        soup.select_one("a[href*='?apply']")
    )
    if apply_el:
        href = apply_el.get("href", "")
        if href:
            job["apply_url"] = urljoin(base, href)

    # ── Company website ─────────────────────────────────────────────────────
    # Personio pages have no dedicated "company site" element, but boards
    # for EU (mostly German) companies link out to a legal notice page
    # (Impressum/Imprint) hosted on the company's real domain. Only trust
    # it when there's exactly one distinct external, non-social domain on
    # the page — ambiguous cases are left unset rather than guessed.
    _SOCIAL_HOSTS = ("facebook.", "linkedin.", "twitter.", "x.com", "instagram.",
                     "youtube.", "xing.", "tiktok.")
    external_domains = set()
    for a in soup.select("a[href^='http']"):
        href_host = urlparse(a["href"]).netloc.lower()
        if not href_host or "personio." in href_host:
            continue
        if any(s in href_host for s in _SOCIAL_HOSTS):
            continue
        external_domains.add(f"{urlparse(a['href']).scheme}://{href_host}")
    if len(external_domains) == 1:
        job["company_website"] = next(iter(external_domains))

    has_content = bool(job.get("description") or job.get("requirements") or job.get("location"))
    return job if has_content else None
