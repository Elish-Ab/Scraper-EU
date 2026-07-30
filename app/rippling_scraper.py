# app/rippling_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
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

RIPPLING_HOST = "ats.rippling.com"


# ============================================================================
# URL HELPERS
# ============================================================================

def is_rippling_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host == RIPPLING_HOST


def _board_base(board_url: str) -> str:
    """Normalise board URL to the /jobs path (drop trailing slash / query)."""
    p = urlparse(board_url)
    path = p.path.rstrip("/")
    if not path.endswith("/jobs"):
        path = path + "/jobs"
    return f"{p.scheme}://{p.netloc}{path}"


def _slug_from_url(url: str) -> str | None:
    """Extract company slug from e.g. https://ats.rippling.com/prisma-careers/jobs"""
    parts = [p for p in urlparse(url).path.strip("/").split("/") if p]
    return parts[0] if parts else None


def _company_name_from_slug(slug: str) -> str:
    slug = re.sub(r"-careers$", "", slug, flags=re.IGNORECASE)
    return " ".join(w.capitalize() for w in re.split(r"[-_]+", slug) if w)


def _job_id_from_path(path: str) -> str | None:
    """Extract UUID from /prisma-careers/jobs/0fbcec57-..."""
    parts = [p for p in path.strip("/").split("/") if p]
    for i, part in enumerate(parts):
        if part == "jobs" and i + 1 < len(parts):
            candidate = parts[i + 1].split("?")[0]
            if len(candidate) == 36 and candidate.count("-") == 4:
                return candidate
    return None


def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }


def _fetch_next_data(url: str) -> dict | None:
    """Fetch a Rippling Next.js page and return its __NEXT_DATA__ payload."""
    try:
        resp = requests.get(url, headers=_headers(), timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Rippling: HTTP {resp.status_code} for {url}")
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        tag = soup.find("script", id="__NEXT_DATA__")
        if not tag or not tag.string:
            return None
        return json.loads(tag.string)
    except Exception as e:
        logger.warning(f"   Rippling fetch failed ({url}): {e}")
        return None


# ============================================================================
# GET JOB LINKS — parsed from __NEXT_DATA__ on the board page
# ============================================================================

def get_links_from_rippling(board_url: str, since_dt: datetime = None) -> list:
    """
    Scrape a Rippling ATS job board via Next.js SSR __NEXT_DATA__.

    Board page embeds a React Query dehydrated state with job listings.
    Pagination works via ?page=N URL param (SSR re-renders each page).

    Data per item: id, name (title), url, department.name,
                   locations[].{name, workplaceType}.
    No published date on board items — since_dt not applied here.
    """
    base = _board_base(board_url)
    jobs: list[dict] = []
    seen_ids: set[str] = set()

    page = 0
    total_pages = 1

    while page < total_pages:
        page_url = f"{base}?page={page}"
        data = _fetch_next_data(page_url)
        if not data:
            break

        try:
            queries = data["props"]["pageProps"]["dehydratedState"]["queries"]
        except (KeyError, TypeError):
            logger.warning(f"   Rippling: no dehydratedState in {page_url}")
            break

        job_query = next(
            (q for q in queries if "job-posts" in q.get("queryKey", [])),
            None,
        )
        if not job_query:
            logger.warning(f"   Rippling: no job-posts query in {page_url}")
            break

        result = job_query.get("state", {}).get("data", {})
        total_pages = result.get("totalPages", 1)
        items = result.get("items", [])

        for item in items:
            job_id = item.get("id", "")
            if not job_id or job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            title = (item.get("name") or "").strip()
            if not title:
                continue

            job_url = (item.get("url") or "").strip()
            if not job_url:
                continue

            job: dict = {
                "jobId": job_id,
                "url": job_url,
                "title": title,
                "method": "rippling_next_data",
            }

            dept = (item.get("department") or {}).get("name", "").strip()
            if dept:
                job["department"] = dept

            locations = item.get("locations") or []
            if locations:
                loc_names = [loc.get("name", "") for loc in locations if loc.get("name")]
                if loc_names:
                    job["location"] = ", ".join(loc_names)
                workplace = locations[0].get("workplaceType", "")
                if workplace:
                    job["workplace"] = workplace.replace("_", " ").title()

            jobs.append(job)

        page += 1

    logger.info(f"✅ Rippling: {len(jobs)} jobs from {base}")
    return jobs


# ============================================================================
# JOB DETAIL — parsed from __NEXT_DATA__ on the individual job page
# ============================================================================

def extract_job_with_rippling(job_url: str) -> dict | None:
    """
    Scrape a Rippling ATS job detail page via Next.js SSR __NEXT_DATA__.

    apiData keys used:
      jobPost.{uuid, name, description.{company, role}, createdOn,
               employmentType.id, workLocations, url}
      department.name
      payRangeDetails  (may be empty)
    """
    job_id = _job_id_from_path(urlparse(job_url).path)
    if not job_id:
        logger.warning(f"   Rippling: cannot parse job ID from {job_url}")
        return None

    data = _fetch_next_data(job_url)
    if not data:
        return None

    try:
        api_data = data["props"]["pageProps"]["apiData"]
    except (KeyError, TypeError):
        logger.warning(f"   Rippling: no apiData in {job_url}")
        return None

    job_post = api_data.get("jobPost") or {}
    dept_data = api_data.get("department") or {}

    title = (job_post.get("name") or "").strip()
    if not title:
        return None

    # jobBoard.companyName is the real registered company name (e.g. "Prisma
    # Data, Inc.") — prefer it over the URL-slug guess. No website field
    # exists anywhere in Rippling's apiData.
    job_board    = api_data.get("jobBoard") or {}
    company_name = (job_board.get("companyName") or "").strip()
    slug         = _slug_from_url(job_url)
    job: dict = {
        "jobId": job_post.get("uuid") or job_id,
        "url": job_post.get("url") or job_url,
        "title": title,
        "company": company_name or (_company_name_from_slug(slug) if slug else None),
    }

    # Department
    dept = (dept_data.get("name") or "").strip()
    if dept:
        job["department"] = dept

    # Location
    work_locs = job_post.get("workLocations") or []
    if work_locs:
        job["location"] = ", ".join(str(l) for l in work_locs if l)

    # Employment type
    emp_type = ((job_post.get("employmentType") or {}).get("id") or "").strip()
    if emp_type:
        job["type"] = emp_type

    # Created date (ISO 8601)
    created_on = (job_post.get("createdOn") or "").strip()
    if created_on:
        job["posted"] = created_on[:10]

    # Description — concatenate company + role sections
    desc = job_post.get("description") or {}
    desc_parts = []
    for section_key in ("company", "role"):
        html = (desc.get(section_key) or "").strip()
        if len(html) > 50:
            desc_parts.append(html)
    if desc_parts:
        job["description"] = "\n".join(desc_parts)

    # Pay range
    pay_ranges = api_data.get("payRangeDetails") or []
    if pay_ranges:
        job["compensation"] = "; ".join(
            str(pr) for pr in pay_ranges if pr
        )

    # Apply URL (standard Rippling pattern)
    job["apply_url"] = job_url + "/apply"

    has_content = bool(job.get("description") or job.get("location") or job.get("department"))
    return job if has_content else None
