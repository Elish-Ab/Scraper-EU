# app/pinpoint_scraper.py
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

PINPOINT_HOST_SUFFIX = ".pinpointhq.com"


# ============================================================================
# URL HELPERS
# ============================================================================

def is_pinpoint_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return host.endswith(PINPOINT_HOST_SUFFIX) or host == "pinpointhq.com"


def _base_url(url: str) -> str:
    p = urlparse(url)
    return f"{p.scheme}://{p.netloc}"


def _company_name_from_host(netloc: str) -> str:
    slug = netloc.split(".")[0]
    return " ".join(w.capitalize() for w in re.split(r"[-_]+", slug) if w)


def _job_id_from_path(path: str) -> str | None:
    """Extract job UUID from path like /en/postings/1918bec6-3939-47a4-b58b-9733d9875917"""
    parts = [p for p in path.strip("/").split("/") if p]
    for i, part in enumerate(parts):
        if part == "postings" and i + 1 < len(parts):
            candidate = parts[i + 1].split("?")[0]
            if len(candidate) == 36 and candidate.count("-") == 4:
                return candidate
    return None


# ============================================================================
# GET JOB LINKS — /postings.json API (returns full metadata + descriptions)
# ============================================================================

def get_links_from_pinpoint(board_url: str, since_dt: datetime = None) -> list:
    """
    Fetch Pinpoint job board via the /postings.json API endpoint.

    The board page embeds a React component that loads from {base}/postings.json.
    Response: {"data": [{id, title, url, path, employment_type_text,
                          workplace_type_text, compensation, description,
                          key_responsibilities, benefits, skills_knowledge_expertise,
                          job: {department: {name}, division: {name}},
                          location: {name}}, ...]}

    Pinpoint does not expose published dates, so since_dt is not applied.
    """
    base = _base_url(board_url)
    api_url = f"{base}/postings.json"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Referer": board_url,
    }

    try:
        resp = requests.get(api_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Pinpoint API: HTTP {resp.status_code} for {api_url}")
            return []
        data = resp.json().get("data", [])
    except Exception as e:
        logger.warning(f"   Pinpoint API fetch failed: {e}")
        return []

    jobs: list[dict] = []
    seen_ids: set[str] = set()

    for item in data:
        full_url = item.get("url", "")
        path     = item.get("path", "")
        if not full_url and path:
            full_url = urljoin(base, path)
        if not full_url:
            continue

        job_id = _job_id_from_path(urlparse(full_url).path)
        if not job_id or job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title = (item.get("title") or "").strip()
        if not title:
            continue

        job_meta    = item.get("job") or {}
        dept_obj    = job_meta.get("department") or {}
        div_obj     = job_meta.get("division") or {}
        location_obj = item.get("location") or {}

        job: dict = {
            "url":    full_url,
            "jobId":  job_id,
            "title":  title,
            "method": "api",
        }

        location = (location_obj.get("name") or "").strip()
        if location:
            job["location"] = location

        department = (dept_obj.get("name") or "").strip()
        if department:
            job["department"] = department

        division = (div_obj.get("name") or "").strip()
        if division:
            job["division"] = division

        emp_type = (item.get("employment_type_text") or "").strip()
        if emp_type:
            job["type"] = emp_type

        workplace = (item.get("workplace_type_text") or "").strip()
        if workplace:
            job["workplace"] = workplace

        compensation = (item.get("compensation") or "").strip()
        if compensation:
            job["compensation"] = compensation

        jobs.append(job)

    logger.info(f"✅ Pinpoint: {len(jobs)} jobs from {api_url}")
    return jobs


# ============================================================================
# JOB DETAIL — SSR HTML page scrape
# ============================================================================

def extract_job_with_pinpoint(job_url: str) -> dict | None:
    """
    Scrape a Pinpoint job detail page (server-side rendered).

    Confirmed selectors (pinpointhq.com):
      Title         h1.external-panel__title
      Sidebar meta  #external-jobs-show-meta-desktop dl.external-definition-list
                    dd.pinpoint-job-sidebar--department
                    dd.pinpoint-job-sidebar--employment_type
                    dd.pinpoint-job-sidebar--location
                    dd.pinpoint-job-sidebar--workplace-type
                    dd.pinpoint-job-sidebar--compensation
      Description   #about-body, #external-jobs-show-description,
                    #responsibilities-body, #skills-body, #benefits-body
      Apply URL     a[href*="/applications/new"]
    """
    parsed = urlparse(job_url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    job_id = _job_id_from_path(parsed.path)
    if not job_id:
        logger.warning(f"   Pinpoint: cannot parse job URL: {job_url}")
        return None

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(job_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Pinpoint job: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   Pinpoint job fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # ── Title ──────────────────────────────────────────────────────────────
    title_el = soup.select_one("h1.external-panel__title") or soup.select_one("h1")
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    if not title:
        return None

    job: dict = {
        "jobId": job_id,
        "url":   job_url,
        "title": title,
        "company": _company_name_from_host(parsed.netloc),
    }

    # ── Sidebar metadata ───────────────────────────────────────────────────
    sidebar = (
        soup.select_one("#external-jobs-show-meta-desktop") or
        soup.select_one("#external-jobs-show-meta-mobile")
    )
    if sidebar:
        meta_map = {
            "department":   "pinpoint-job-sidebar--department",
            "type":         "pinpoint-job-sidebar--employment_type",
            "location":     "pinpoint-job-sidebar--location",
            "workplace":    "pinpoint-job-sidebar--workplace-type",
            "compensation": "pinpoint-job-sidebar--compensation",
        }
        for field, css_class in meta_map.items():
            el = sidebar.select_one(f"dd.{css_class}")
            if el:
                value = el.get_text(strip=True)
                if value:
                    job[field] = value

    # ── Company website ─────────────────────────────────────────────────────
    # First external footer link is the company's real homepage; the
    # privacy-policy link right after it is always a relative path.
    site_link = soup.select_one("a.external-footer__link[href^='http']")
    if site_link:
        href = site_link.get("href", "").strip()
        if href:
            job["company_website"] = href

    # ── Description — combine all named content sections ──────────────────
    section_ids = [
        "about-body",
        "external-jobs-show-description",
        "responsibilities-body",
        "skills-body",
        "benefits-body",
    ]
    desc_parts = []
    for section_id in section_ids:
        el = soup.select_one(f"#{section_id}")
        if not el:
            continue
        for script in el.select("script"):
            script.decompose()
        html = str(el).strip()
        if len(html) > 50:
            desc_parts.append(html)

    if desc_parts:
        job["description"] = "\n".join(desc_parts)

    # ── Apply URL ──────────────────────────────────────────────────────────
    apply_el = soup.select_one('a[href*="/applications/new"]')
    if apply_el:
        href = apply_el.get("href", "")
        if href:
            job["apply_url"] = urljoin(base, href)

    has_content = bool(job.get("description") or job.get("location") or job.get("department"))
    return job if has_content else None
