# app/smartrecruiters_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
import logging
import random
import re
import time
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]

SR_API_BASE  = "https://api.smartrecruiters.com/v1/companies"
SR_JOBS_BASE = "https://jobs.smartrecruiters.com"


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _job_url(company: str, job_id: str, title: str) -> str:
    slug = _slugify(title)
    return f"{SR_JOBS_BASE}/{company}/{job_id}-{slug}"


# ============================================================================
# URL HELPERS
# ============================================================================

def is_smartrecruiters_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return "smartrecruiters.com" in host


def _parse_sr_board_url(board_url: str) -> str:
    """Extract company slug from careers.smartrecruiters.com/{company}/"""
    parsed = urlparse(board_url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if parts:
        return parts[0]
    raise ValueError(f"Cannot parse SmartRecruiters board URL: {board_url}")


def _parse_sr_job_url(job_url: str) -> tuple[str, str]:
    """
    Extract (company, job_id) from jobs.smartrecruiters.com/{company}/{job_id}-{slug}
    The job_id is the leading numeric segment before the first dash.
    """
    parsed = urlparse(job_url)
    parts = [p for p in parsed.path.strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Cannot parse SmartRecruiters job URL: {job_url}")
    company = parts[0]
    job_slug = parts[1]
    job_id = job_slug.split("-")[0] if "-" in job_slug else job_slug
    return company, job_id


# ============================================================================
# GET JOB LINKS — public API with pagination
# ============================================================================

def get_links_from_smartrecruiters(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch SmartRecruiters job listings via public API.

    API: https://api.smartrecruiters.com/v1/companies/{company}/postings
         ?limit=100&offset=N
    Date field: releasedDate (ISO 8601)

    Returns list of job dicts with:
      url, jobId, title, department, location, type, published, remote
    """
    try:
        company = _parse_sr_board_url(board_url)
    except ValueError as exc:
        logger.error(str(exc))
        return []

    headers = {
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
    }

    jobs = []
    offset = 0
    limit = 100
    total_found = None

    while True:
        api_url = f"{SR_API_BASE}/{company}/postings?limit={limit}&offset={offset}"
        try:
            resp = requests.get(api_url, headers=headers, timeout=20)
            if resp.status_code != 200:
                logger.warning(f"   SR API: HTTP {resp.status_code} at offset={offset}")
                break
            data = resp.json()
        except Exception as e:
            logger.warning(f"   SR API fetch failed at offset={offset}: {e}")
            break

        if total_found is None:
            total_found = data.get("totalFound", 0)
            logger.info(f"   SR API: {total_found} total jobs for '{company}'")

        content = data.get("content", [])
        if not content:
            break

        stop_early = False
        for item in content:
            job_id = item.get("id", "")
            title = item.get("name", "")
            released = item.get("releasedDate", "")
            company_id = (item.get("company") or {}).get("identifier", company)

            if not job_id or not title:
                continue

            published = ""
            if released:
                try:
                    normalized = released.replace("Z", "+00:00") if released.endswith("Z") else released
                    pub_dt = datetime.fromisoformat(normalized)
                    if pub_dt.tzinfo is None:
                        pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                    if since_dt and pub_dt < since_dt:
                        stop_early = True
                        continue
                    published = released[:10]
                except Exception:
                    published = released[:10]

            loc = item.get("location") or {}
            location = loc.get("fullLocation", "").strip()
            if not location:
                city = loc.get("city", "")
                country = loc.get("country", "")
                location = ", ".join(filter(None, [city, country]))
            remote = bool(loc.get("remote", False))

            dept_obj = item.get("department") or {}
            department = dept_obj.get("label", "") if isinstance(dept_obj, dict) else ""

            type_obj = item.get("typeOfEmployment") or {}
            emp_type = type_obj.get("label", "") if isinstance(type_obj, dict) else ""

            jobs.append({
                "url":        _job_url(company_id, job_id, title),
                "jobId":      job_id,
                "title":      title,
                "department": department,
                "location":   location,
                "type":       emp_type,
                "published":  published,
                "remote":     remote,
                "method":     "api",
            })

        offset += limit
        if offset >= (total_found or 0):
            break
        if stop_early:
            logger.info("   SR API: reached date cutoff, stopping pagination")
            break

        time.sleep(random.uniform(0.5, 1.0))

    logger.info(f"✅ SmartRecruiters: {len(jobs)} jobs")
    return jobs


# ============================================================================
# JOB DETAIL — DOM scrape
# ============================================================================

def extract_job_with_smartrecruiters(job_url: str) -> dict | None:
    """
    Scrape SmartRecruiters job detail page.

    Confirmed selectors (jobs.smartrecruiters.com):
      Title           h1.job-title
      Posted date     meta[itemprop="datePosted"]
      Location        spl-job-location[formattedaddress]
      Workplace type  spl-job-location[workplacetype]  (e.g. "remote")
      Employment type li[itemprop="employmentType"]
      Company name    div[itemprop="hiringOrganization"] meta[itemprop="name"]
      Sections        section.job-section  (id → field):
        #st-companyDescription  → company_description
        #st-jobDescription      → description
        #st-qualifications      → requirements
        #st-additionalInformation → additional_info
      Apply URL       a.js-oneclick[href]
    """
    try:
        company, job_id = _parse_sr_job_url(job_url)
    except ValueError as exc:
        logger.error(str(exc))
        return None

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(job_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   SR detail: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   SR detail fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Title ──────────────────────────────────────────────────────────────
    title_el = soup.select_one("h1.job-title") or soup.select_one("h1[itemprop='title']")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title:
        return None

    job: dict = {
        "jobId":   job_id,
        "url":     job_url,
        "account": company,
        "title":   title,
    }

    # ── Posted date ────────────────────────────────────────────────────────
    date_meta = soup.select_one("meta[itemprop='datePosted']")
    if date_meta:
        raw_date = (date_meta.get("content") or "").strip()
        if raw_date:
            job["published"] = raw_date[:10]

    # ── Location + workplace ───────────────────────────────────────────────
    loc_el = soup.select_one("spl-job-location")
    if loc_el:
        addr = (loc_el.get("formattedaddress") or "").strip()
        if addr:
            job["location"] = addr
        wp_type = (loc_el.get("workplacetype") or "").strip()
        if wp_type:
            job["workplace"] = wp_type
            job["remote"] = wp_type.lower() == "remote"

    # ── Employment type ────────────────────────────────────────────────────
    type_el = soup.select_one("li[itemprop='employmentType']")
    if type_el:
        emp_type = type_el.get_text(strip=True)
        if emp_type:
            job["type"] = emp_type

    # ── Company name ───────────────────────────────────────────────────────
    company_meta = soup.select_one("div[itemprop='hiringOrganization'] meta[itemprop='name']")
    if company_meta:
        company_name = (company_meta.get("content") or "").strip()
        if company_name:
            job["company"] = company_name

    # ── Job sections ───────────────────────────────────────────────────────
    section_map = {
        "st-companyDescription":   "company_description",
        "st-jobDescription":       "description",
        "st-qualifications":       "requirements",
        "st-additionalInformation": "additional_info",
    }
    for section_id, field_name in section_map.items():
        section_el = soup.select_one(f"section#{section_id}")
        if not section_el:
            continue
        wysiwyg = section_el.select_one("div.wysiwyg")
        if wysiwyg:
            html_content = str(wysiwyg)
            if len(html_content) > 50:
                job[field_name] = html_content

    # ── Apply URL ──────────────────────────────────────────────────────────
    apply_el = soup.select_one("a.js-oneclick[href]")
    if apply_el:
        job["apply_url"] = apply_el.get("href", "")

    has_content = bool(job.get("description") or job.get("requirements") or job.get("location"))
    return job if has_content else None
