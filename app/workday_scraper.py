# app/workday_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
import html
import json
import logging
import random
import re
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_PAGE_SIZE = 20


# ============================================================================
# URL HELPERS
# ============================================================================

def is_workday_url(url: str) -> bool:
    return ".myworkdayjobs.com" in urlparse(url).netloc.lower()


def _parse_board_url(url: str) -> tuple[str, str, str]:
    """Return (netloc, company, board) from a Workday board URL.

    e.g. https://blackbaud.wd1.myworkdayjobs.com/externalcareers/
    → ('blackbaud.wd1.myworkdayjobs.com', 'blackbaud', 'externalcareers')
    """
    p = urlparse(url)
    netloc = p.netloc.lower()
    company = netloc.split(".")[0]
    parts = [seg for seg in p.path.strip("/").split("/") if seg]
    # skip locale prefix like "en-US"
    if parts and re.match(r"^[a-z]{2}-[A-Z]{2}$", parts[0]):
        parts = parts[1:]
    board = parts[0] if parts else ""
    return netloc, company, board


def _job_id_from_external_path(path: str) -> str:
    """Last path segment = JobTitle_RXXXXXXXX."""
    parts = [p for p in path.strip("/").split("/") if p]
    return parts[-1] if parts else path


def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def _api_headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


# ============================================================================
# GET JOB LINKS — Workday CXS JSON API
# ============================================================================

def get_links_from_workday(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch jobs from the internal Workday CXS JSON API.

    Every Workday board exposes:
      POST https://{host}/wday/cxs/{company}/{board}/jobs
    with {limit, offset, searchText, appliedFacets} body.

    The listing API does NOT include datePosted — date filtering is done by
    extract_job_with_workday() which reads the SSR LD+JSON on each job page.
    """
    netloc, company, board = _parse_board_url(board_url)
    api_url = f"https://{netloc}/wday/cxs/{company}/{board}/jobs"

    jobs: list[dict] = []
    seen_ids: set[str] = set()
    offset = 0
    total: int | None = None

    while True:
        payload = {
            "limit": _PAGE_SIZE,
            "offset": offset,
            "searchText": "",
            "appliedFacets": {},
        }
        try:
            resp = requests.post(api_url, json=payload, headers=_api_headers(), timeout=30)
            if resp.status_code != 200:
                logger.warning(f"   Workday API: HTTP {resp.status_code} for {api_url}")
                break
            data = resp.json()
        except Exception as e:
            logger.warning(f"   Workday API fetch failed: {e}")
            break

        if total is None:
            total = data.get("total", 0)

        postings = data.get("jobPostings", [])
        if not postings:
            break

        for item in postings:
            external_path = (item.get("externalPath") or "").strip()
            if not external_path:
                continue

            job_id = _job_id_from_external_path(external_path)
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            title = (item.get("title") or "").strip()
            if not title:
                continue

            job_url = f"https://{netloc}/en-US/{board}{external_path}"

            job: dict = {
                "jobId": job_id,
                "url": job_url,
                "title": title,
                "method": "workday_api",
            }

            location = (item.get("locationsText") or "").strip()
            if location:
                job["location"] = location

            bullet = item.get("bulletFields") or []
            if bullet:
                job["reqId"] = bullet[0]

            jobs.append(job)

        offset += len(postings)
        if total is not None and offset >= total:
            break

    logger.info(f"✅ Workday: {len(jobs)} jobs from {board_url}")
    return jobs


# ============================================================================
# JOB DETAIL — SSR page with LD+JSON (schema.org JobPosting)
# ============================================================================

def extract_job_with_workday(job_url: str) -> dict | None:
    """
    Scrape a Workday job detail page (SSR, no JS required).

    The SSR HTML embeds <script type="application/ld+json"> with a
    schema.org JobPosting including datePosted, employmentType,
    jobLocation, hiringOrganization.name, and full plain-text description.

    Apply URL is constructed as: /en-US/{board}/details/{jobId}/apply
    """
    try:
        resp = requests.get(job_url, headers=_headers(), timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Workday job: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   Workday job fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    ld_tag = soup.find("script", type="application/ld+json")
    if not ld_tag or not ld_tag.string:
        return None

    try:
        ld = json.loads(ld_tag.string)
    except Exception as e:
        logger.debug(f"   Workday LD+JSON parse error: {e}")
        return None

    title = (ld.get("title") or "").strip()
    if not title:
        return None

    # ── Extract board and jobId from URL ──────────────────────────────────
    p = urlparse(job_url)
    netloc = p.netloc.lower()
    path_parts = [seg for seg in p.path.strip("/").split("/") if seg]
    job_id = path_parts[-1] if path_parts else job_url

    # board is the segment before "job" in the path
    board = ""
    for i, part in enumerate(path_parts):
        if part == "job" and i > 0:
            board = path_parts[i - 1]
            break

    job: dict = {
        "jobId": job_id,
        "url": job_url,
        "title": title,
    }

    # ── datePosted ────────────────────────────────────────────────────────
    date_posted = (ld.get("datePosted") or "").strip()
    if date_posted:
        job["posted"] = date_posted[:10]

    # ── Employment type ───────────────────────────────────────────────────
    emp_type = (ld.get("employmentType") or "").strip()
    if emp_type:
        job["type"] = emp_type

    # ── Remote ────────────────────────────────────────────────────────────
    if ld.get("jobLocationType") == "TELECOMMUTE":
        job["workplace"] = "Remote"

    # ── Location ──────────────────────────────────────────────────────────
    job_loc = ld.get("jobLocation") or {}
    if isinstance(job_loc, list):
        job_loc = job_loc[0] if job_loc else {}
    addr = job_loc.get("address") or {}
    city = (addr.get("addressLocality") or "").strip()
    country = (addr.get("addressCountry") or "").strip()
    if city:
        job["location"] = city
    elif country:
        job["location"] = country

    # ── Company ───────────────────────────────────────────────────────────
    org = ld.get("hiringOrganization") or {}
    org_name = (org.get("name") or "").strip()
    # Strip Workday legal-entity prefix like "LE-6300 "
    org_name = re.sub(r"^[A-Z]{2}-\d+\s+", "", org_name)
    if org_name:
        job["company"] = org_name

    # ── Req ID ────────────────────────────────────────────────────────────
    identifier = ld.get("identifier") or {}
    req_id = (identifier.get("value") or "").strip()
    if req_id:
        job["reqId"] = req_id

    # ── Description ───────────────────────────────────────────────────────
    description = (ld.get("description") or "").strip()
    if description:
        job["description"] = html.unescape(description)

    # ── Apply URL ─────────────────────────────────────────────────────────
    if board:
        job["apply_url"] = f"https://{netloc}/en-US/{board}/details/{job_id}/apply"

    has_content = bool(job.get("description") or job.get("location"))
    return job if has_content else None
