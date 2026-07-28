# app/breezy_scraper.py
from datetime import datetime
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

def is_breezy_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host.endswith(".breezy.hr") or host == "breezy.hr"


def _base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _company_name_from_host(netloc: str) -> str:
    slug = netloc.split(".")[0]
    return " ".join(w.capitalize() for w in re.split(r"[-_]+", slug) if w)


def _job_id_from_path(path: str) -> str | None:
    """Extract job ID from a Breezy path like /p/25c7b9d48869-qa-test-engineer."""
    parts = [p for p in path.strip("/").split("/") if p]
    for i, part in enumerate(parts):
        if part == "p" and i + 1 < len(parts):
            slug = parts[i + 1].split("?")[0]
            # Job ID is the leading hex segment before the first dash
            job_id = slug.split("-")[0]
            if re.match(r"^[0-9a-f]{10,}$", job_id):
                return job_id
    return None


_LABEL_MAP: dict[str, str] = {
    "%LABEL_POSITION_TYPE_FULL_TIME%":   "Full-Time",
    "%LABEL_POSITION_TYPE_PART_TIME%":   "Part-Time",
    "%LABEL_POSITION_TYPE_CONTRACT%":    "Contract",
    "%LABEL_POSITION_TYPE_FREELANCE%":   "Freelance",
    "%LABEL_POSITION_TYPE_INTERNSHIP%":  "Internship",
    "%LABEL_POSITION_TYPE_TEMPORARY%":   "Temporary",
    "%LABEL_POSITION_TYPE_VOLUNTEER%":   "Volunteer",
    "%LABEL_POSITION_TYPE_OTHER%":       "Other",
}


def _decode_label(text: str) -> str:
    return _LABEL_MAP.get(text, text)


def _is_job_path(href: str) -> bool:
    return bool(href) and re.match(r"^/p/[0-9a-f]", href)


# ============================================================================
# GET JOB LINKS — static HTML board page
# ============================================================================

def get_links_from_breezy(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch Breezy job board via static HTML.

    Board layout:
      Groups:      h2.group-header > span (location name)
      Job items:   li.position > ul.position-wrap > li.position-details > a[href^='/p/']
      Title:       h2 inside the anchor
      Meta items:  ul.meta > li.location > span
                             li.type > span
                             li.department > span
      Remote:      li containing i.fa-wifi (inside ul.meta)

    Breezy does not expose published dates on the board listing page,
    so since_dt filtering is not applied (all jobs are returned).
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
            logger.warning(f"   Breezy board: HTTP {resp.status_code}")
            return []
    except Exception as e:
        logger.warning(f"   Breezy board fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    jobs: list[dict] = []
    seen_ids: set[str] = set()

    for position in soup.select("li.position"):
        link_el = position.select_one("li.position-details > a[href]")
        if not link_el:
            continue

        href = link_el.get("href", "")
        if not _is_job_path(href):
            continue

        job_id = _job_id_from_path(href)
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title_el = link_el.select_one("h2")
        title = title_el.get_text(strip=True) if title_el else ""
        if not title:
            continue

        meta = link_el.select_one("ul.meta")
        location = ""
        emp_type = ""
        department = ""
        remote = False

        if meta:
            loc_el = meta.select_one("li.location span:not(.polygot)")
            if not loc_el:
                loc_el = meta.select_one("li.location span")
            if loc_el:
                location = loc_el.get_text(strip=True)

            type_el = meta.select_one("li.type span.polygot") or meta.select_one("li.type span")
            if type_el:
                emp_type = _decode_label(type_el.get_text(strip=True))

            dept_el = meta.select_one("li.department span")
            if dept_el:
                department = dept_el.get_text(strip=True)

            # Remote indicator: li containing fa-wifi icon
            if meta.select_one("i.fa-wifi"):
                remote = True

        job: dict = {
            "url":    urljoin(base, href),
            "jobId":  job_id,
            "title":  title,
            "method": "html",
        }
        if location:
            job["location"] = location
        if emp_type:
            job["type"] = emp_type
        if department:
            job["department"] = department
        if remote:
            job["remote"] = True

        jobs.append(job)

    logger.info(f"✅ Breezy: {len(jobs)} jobs")
    return jobs


# ============================================================================
# JOB DETAIL — DOM scrape
# ============================================================================

def extract_job_with_breezy(job_url: str) -> dict | None:
    """
    Scrape a Breezy job detail page.

    Confirmed selectors (breezy.hr):
      Title         h1.title | h1
      Location      li.location span
      Type          li.type span
      Department    li.department span
      Remote        i.fa-wifi present in meta section
      Description   div.description | section.description | div#details
      Apply URL     a[href$='/apply'] | a.apply
    """
    parsed = urlparse(job_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    job_id = _job_id_from_path(parsed.path)
    if not job_id:
        logger.warning(f"   Breezy: cannot parse job URL: {job_url}")
        return None

    # Normalize to canonical URL (strip query/fragment)
    path_parts = [p for p in parsed.path.strip("/").split("/") if p]
    try:
        p_idx = path_parts.index("p")
        slug = path_parts[p_idx + 1]
        clean_url = f"{base}/p/{slug}"
    except (ValueError, IndexError):
        clean_url = job_url

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(clean_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Breezy job: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   Breezy job fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Title ──────────────────────────────────────────────────────────────
    title_el = (
        soup.select_one("h1.title") or
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

    # ── Meta (location, type, department, remote) ──────────────────────────
    meta = soup.select_one("ul.meta") or soup.select_one("ul.position-meta")
    if meta:
        loc_spans = meta.select("li.location span")
        # First non-remote span is the location text
        for span in loc_spans:
            text = span.get_text(strip=True)
            if text and "remote" not in text.lower():
                job["location"] = text
                break

        type_el = meta.select_one("li.type span.polygot") or meta.select_one("li.type span")
        if type_el:
            emp_type = _decode_label(type_el.get_text(strip=True))
            if emp_type:
                job["type"] = emp_type

        dept_el = meta.select_one("li.department span")
        if dept_el:
            dept = dept_el.get_text(strip=True)
            if dept:
                job["department"] = dept

        if meta.select_one("i.fa-wifi"):
            job["remote"] = True

    # ── Description ────────────────────────────────────────────────────────
    desc_el = (
        soup.select_one("div.description") or
        soup.select_one("section.description") or
        soup.select_one("div#details") or
        soup.select_one("div.job-description") or
        soup.select_one("div[class*='description']")
    )
    if desc_el:
        desc_html = str(desc_el)
        if len(desc_html) > 50:
            job["description"] = desc_html

    # ── Apply URL ──────────────────────────────────────────────────────────
    apply_el = (
        soup.select_one(f"a[href$='/apply']") or
        soup.select_one("a.apply") or
        soup.select_one("a[href*='/apply']")
    )
    if apply_el:
        href = apply_el.get("href", "")
        if href:
            job["apply_url"] = urljoin(base, href)

    has_content = bool(job.get("description") or job.get("location") or job.get("department"))
    return job if has_content else None
