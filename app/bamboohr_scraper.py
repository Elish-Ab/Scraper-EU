# app/bamboohr_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
import logging
import random
import requests

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]

# locationType values returned by the BambooHR careers API
_LOCATION_TYPE = {
    "0": "In Office",
    "1": "Remote",
    "2": "Hybrid",
}


# ============================================================================
# URL HELPERS
# ============================================================================

def is_bamboohr_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host.endswith(".bamboohr.com") or host == "bamboohr.com"


def _base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _job_id_from_path(path: str) -> str | None:
    """Extract numeric job ID from a BambooHR path like /careers/337."""
    parts = [p for p in path.strip("/").split("/") if p]
    for i, part in enumerate(parts):
        if part == "careers" and i + 1 < len(parts):
            candidate = parts[i + 1].split("?")[0]
            if candidate.isdigit():
                return candidate
    return None


def _headers() -> dict:
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Referer": "",
    }


# ============================================================================
# GET JOB LINKS — public careers/list API
# ============================================================================

def get_links_from_bamboohr(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch BambooHR job listings via the public careers/list API.

    Endpoint: GET https://{company}.bamboohr.com/careers/list
    Returns JSON: { "meta": {"totalCount": N}, "result": [...] }

    Each result item:
      id                   numeric job ID (string)
      jobOpeningName       job title
      departmentLabel      department name
      employmentStatusLabel  e.g. "Full-Time", "Contractor", "Student"
      location             { city, state }
      locationType         "0"=In Office, "1"=Remote, "2"=Hybrid
      isRemote             null (unreliable — use locationType instead)

    BambooHR does not expose a published date on the list endpoint, so
    since_dt is applied in the detail endpoint when fetching each job.
    """
    base = _base_url(board_url)
    api_url = f"{base}/careers/list"

    try:
        resp = requests.get(api_url, headers=_headers(), timeout=20)
        if resp.status_code != 200:
            logger.warning(f"   BambooHR list: HTTP {resp.status_code}")
            return []
        data = resp.json()
    except Exception as e:
        logger.warning(f"   BambooHR list fetch failed: {e}")
        return []

    items = data.get("result", [])
    if not items:
        logger.info("   BambooHR list: no jobs in response")
        return []

    jobs: list[dict] = []
    for item in items:
        job_id = str(item.get("id", "")).strip()
        title = (item.get("jobOpeningName") or "").strip()
        if not job_id or not title:
            continue

        loc_type_raw = str(item.get("locationType") or "")
        loc_type = _LOCATION_TYPE.get(loc_type_raw, "")
        is_remote = loc_type_raw == "1"

        location_obj = item.get("location") or {}
        city = (location_obj.get("city") or "").strip()
        state = (location_obj.get("state") or "").strip()
        location = ", ".join(filter(None, [city, state]))

        job: dict = {
            "url":    f"{base}/careers/{job_id}",
            "jobId":  job_id,
            "title":  title,
            "method": "api",
        }
        if item.get("departmentLabel"):
            job["department"] = item["departmentLabel"].strip()
        if item.get("employmentStatusLabel"):
            job["type"] = item["employmentStatusLabel"].strip()
        if location:
            job["location"] = location
        if loc_type:
            job["workplace"] = loc_type
        if is_remote:
            job["remote"] = True

        jobs.append(job)

    logger.info(f"✅ BambooHR: {len(jobs)} jobs")
    return jobs


# ============================================================================
# JOB DETAIL — careers/{id}/detail API
# ============================================================================

def extract_job_with_bamboohr(job_url: str) -> dict | None:
    """
    Fetch BambooHR job detail via the internal careers/{id}/detail API.

    Endpoint: GET https://{company}.bamboohr.com/careers/{id}/detail
    Returns JSON: { "result": { "jobOpening": {...}, "formFields": {...} } }

    jobOpening fields used:
      jobOpeningName         title
      description            full HTML description
      datePosted             ISO date string (YYYY-MM-DD)
      departmentLabel        department
      employmentStatusLabel  employment type
      location               { city, state, postalCode, addressCountry }
      locationType           "0"/"1"/"2"
      minimumExperience      e.g. "Experienced"
      compensation           salary info (may be null)
      jobOpeningShareUrl     canonical URL
    """
    parsed = urlparse(job_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    job_id = _job_id_from_path(parsed.path)
    if not job_id:
        logger.warning(f"   BambooHR: cannot parse job URL: {job_url}")
        return None

    api_url = f"{base}/careers/{job_id}/detail"
    try:
        resp = requests.get(api_url, headers=_headers(), timeout=20)
        if resp.status_code != 200:
            logger.warning(f"   BambooHR detail: HTTP {resp.status_code}")
            return None
        data = resp.json()
    except Exception as e:
        logger.warning(f"   BambooHR detail fetch failed: {e}")
        return None

    jo = (data.get("result") or {}).get("jobOpening") or {}
    title = (jo.get("jobOpeningName") or "").strip()
    if not title:
        return None

    job: dict = {
        "jobId": job_id,
        "url":   jo.get("jobOpeningShareUrl") or f"{base}/careers/{job_id}",
        "title": title,
    }

    if jo.get("datePosted"):
        job["published"] = jo["datePosted"][:10]

    if jo.get("departmentLabel"):
        job["department"] = jo["departmentLabel"].strip()

    if jo.get("employmentStatusLabel"):
        job["type"] = jo["employmentStatusLabel"].strip()

    loc_type_raw = str(jo.get("locationType") or "")
    loc_type = _LOCATION_TYPE.get(loc_type_raw, "")
    if loc_type:
        job["workplace"] = loc_type
    if loc_type_raw == "1":
        job["remote"] = True

    location_obj = jo.get("location") or {}
    city = (location_obj.get("city") or "").strip()
    state = (location_obj.get("state") or "").strip()
    country = (location_obj.get("addressCountry") or "").strip()
    location = ", ".join(filter(None, [city, state, country]))
    if location:
        job["location"] = location

    if jo.get("minimumExperience"):
        job["experience"] = jo["minimumExperience"]

    if jo.get("compensation"):
        job["compensation"] = jo["compensation"]

    description = (jo.get("description") or "").strip()
    if description:
        job["description"] = description

    job["apply_url"] = f"{base}/careers/{job_id}/detail#apply"

    has_content = bool(job.get("description") or job.get("location") or job.get("department"))
    return job if has_content else None
