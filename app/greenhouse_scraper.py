# app/greenhouse_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
import logging
import random
import time
import requests
from bs4 import BeautifulSoup
import html

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]

# Two Greenhouse board domains
GH_US_BASE = "https://job-boards.greenhouse.io"
GH_EU_BASE = "https://job-boards.eu.greenhouse.io"
GH_API_BASE = "https://boards-api.greenhouse.io/v1/boards"


# ============================================================================
# URL HELPERS
# ============================================================================

def is_greenhouse_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return "greenhouse.io" in host


def _parse_greenhouse_board_url(board_url: str) -> tuple[str, str]:
    """
    Returns (company_slug, base_url).
    e.g. https://job-boards.eu.greenhouse.io/altamiratechnologies
         → ('altamiratechnologies', 'https://job-boards.eu.greenhouse.io')
    """
    parsed = urlparse(board_url)
    host   = parsed.netloc.lower()
    base   = GH_EU_BASE if "eu.greenhouse" in host else GH_US_BASE
    parts  = [p for p in parsed.path.strip("/").split("/") if p]
    if not parts:
        raise ValueError(f"Cannot parse Greenhouse board URL: {board_url}")
    return parts[0], base


def _parse_greenhouse_job_url(job_url: str) -> tuple[str, str, str]:
    """
    Returns (company_slug, job_id, base_url).
    e.g. https://job-boards.eu.greenhouse.io/altamiratechnologies/jobs/4797310101
         → ('altamiratechnologies', '4797310101', 'https://job-boards.eu.greenhouse.io')
    """
    parsed = urlparse(job_url)
    host   = parsed.netloc.lower()
    base   = GH_EU_BASE if "eu.greenhouse" in host else GH_US_BASE
    parts  = [p for p in parsed.path.strip("/").split("/") if p]
    # path: /company/jobs/job_id
    if len(parts) < 3:
        raise ValueError(f"Cannot parse Greenhouse job URL: {job_url}")
    return parts[0], parts[2], base


# ============================================================================
# PUBLIC API — primary source (has first_published date)
# ============================================================================

def _fetch_greenhouse_api(company: str) -> list:
    """
    Fetch all jobs via Greenhouse public API.
    Returns raw job dicts with first_published, title, location, content etc.
    No pagination — API returns all jobs in one response.
    """
    api_url = f"{GH_API_BASE}/{company}/jobs?content=true"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
    }
    try:
        resp = requests.get(api_url, headers=headers, timeout=30)
        if resp.status_code == 404:
            logger.warning(f"   Greenhouse API 404 for '{company}' — company not found")
            return []
        if resp.status_code != 200:
            logger.warning(f"   Greenhouse API status {resp.status_code} for '{company}'")
            return []
        data = resp.json()
        jobs = data.get("jobs", [])
        logger.info(f"   Greenhouse API: {len(jobs)} jobs for '{company}'")
        return jobs
    except Exception as e:
        logger.warning(f"   Greenhouse API failed: {e}")
        return []


def _fetch_greenhouse_departments(company: str) -> dict:
    """
    Fetch department info from Greenhouse API.
    Returns dict of job_id → department_name.
    """
    api_url = f"{GH_API_BASE}/{company}/departments"
    headers = {"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"}
    job_dept_map = {}
    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        for dept in data.get("departments", []):
            dept_name = dept.get("name", "")
            for job in dept.get("jobs", []):
                job_id = str(job.get("id", ""))
                if job_id:
                    job_dept_map[job_id] = dept_name
        return job_dept_map
    except:
        return {}


# ============================================================================
# BOARD PAGE DOM SCRAPER — fallback when API fails
# ============================================================================

def _scrape_greenhouse_board_dom(board_url: str, company: str, base_url: str) -> list:
    """
    Scrape Greenhouse board page HTML.
    Handles pagination via ?page=N query param.

    Selectors (confirmed):
      Job item:       tr.job-post
      URL:            td.cell a[href]
      Title:          p.body.body--medium
      Location:       p.body__secondary.body--metadata
      Department:     h3.section-header (preceding sibling)
      Dept path:      div.job-posts--department-path p
      Pagination:     div.pagination-wrapper button.pagination__link
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }

    all_jobs     = []
    seen_urls    = set()
    current_page = 1

    while True:
        page_url = f"{board_url}?page={current_page}" if current_page > 1 else board_url
        try:
            resp = requests.get(page_url, headers=headers, timeout=30)
            if resp.status_code != 200:
                logger.warning(f"   Greenhouse DOM: HTTP {resp.status_code} on page {current_page}")
                break
        except Exception as e:
            logger.warning(f"   Greenhouse DOM fetch failed: {e}")
            break

        soup = BeautifulSoup(resp.text, "html.parser")
        page_jobs = _parse_board_page(soup, seen_urls)
        all_jobs.extend(page_jobs)

        # Check for more pages
        pagination = soup.select_one("div.pagination-wrapper")
        if not pagination:
            break

        # Find next page button
        next_btn = pagination.select_one("button.pagination__next:not([aria-disabled='true'])")
        if not next_btn or next_btn.get("aria-disabled") == "true":
            break

        current_page += 1
        time.sleep(random.uniform(0.5, 1.5))

    logger.info(f"   Greenhouse DOM: {len(all_jobs)} jobs across {current_page} pages")
    return all_jobs


def _parse_board_page(soup: BeautifulSoup, seen_urls: set) -> list:
    """Parse one board page and return job dicts."""
    jobs = []
    current_dept      = ""
    current_dept_path = ""

    for el in soup.select(
        "div.job-posts--table--department, tr.job-post"
    ):
        if "job-posts--table--department" in el.get("class", []):
            # Extract department name and path from this section container
            dept_path_el = el.select_one("div.job-posts--department-path p")
            current_dept_path = dept_path_el.get_text(strip=True) if dept_path_el else ""
            dept_el = el.select_one("h3.section-header")
            current_dept = dept_el.get_text(strip=True) if dept_el else ""
            continue

        # tr.job-post
        link = el.select_one("td.cell a[href]")
        if not link:
            continue

        job_url = link.get("href", "").strip()
        if not job_url or job_url in seen_urls:
            continue
        seen_urls.add(job_url)

        # Job ID = last URL path segment
        path_parts = [p for p in urlparse(job_url).path.strip("/").split("/") if p]
        job_id = path_parts[-1] if path_parts else ""

        title_el    = link.select_one("p.body.body--medium")
        location_el = link.select_one("p.body__secondary.body--metadata")

        title    = title_el.get_text(strip=True)    if title_el    else ""
        location = location_el.get_text(strip=True) if location_el else ""

        if not title:
            continue

        jobs.append({
            "url":         job_url,
            "jobId":       job_id,
            "title":       title,
            "department":  current_dept,
            "dept_path":   current_dept_path,
            "location":    location,
            "published":   "",   # not in DOM
            "method":      "dom",
        })

    return jobs


# ============================================================================
# GET LINKS — API primary (has dates), DOM fallback
# ============================================================================

def get_links_from_greenhouse(board_url: str, since_dt: datetime = None) -> list:
    """
    Get Greenhouse job listings.

    Step 1 — Public API (boards-api.greenhouse.io):
        Returns all jobs with first_published for date filtering.
        Also fetches departments separately.

    Step 2 — Date filter using first_published.

    Step 3 — DOM fallback if API fails (no dates).
    """
    try:
        company, base_url = _parse_greenhouse_board_url(board_url)
    except ValueError as e:
        logger.error(str(e))
        return []

    # ── STEP 1: Fetch via public API ───────────────────────────────
    api_jobs = _fetch_greenhouse_api(company)

    if api_jobs:
        # Fetch departments in parallel (best effort)
        dept_map = _fetch_greenhouse_departments(company)

        # ── STEP 2: Date filter ────────────────────────────────────
        filtered = []
        for job in api_jobs:
            if since_dt:
                pub_str = job.get("first_published", "") or job.get("updated_at", "")
                if pub_str:
                    try:
                        # Format: "2026-03-10T07:50:45-04:00"
                        pub_dt = datetime.fromisoformat(pub_str)
                        if pub_dt.tzinfo is None:
                            pub_dt = pub_dt.replace(tzinfo=timezone.utc)
                        if pub_dt < since_dt:
                            continue
                    except:
                        pass
            filtered.append(job)

        logger.info(f"   After date filter: {len(filtered)} jobs (from {len(api_jobs)} total)")

        jobs = []
        for job in filtered:
            job_id  = str(job.get("id", ""))
            pub_str = job.get("first_published", "")
            published = pub_str[:10] if pub_str else ""

            # Department from dept_map, fallback to departments array in job
            department = dept_map.get(job_id, "")
            if not department:
                depts = job.get("departments", [])
                if depts:
                    department = depts[0].get("name", "")

            jobs.append({
                "url":        job.get("absolute_url", ""),
                "jobId":      job_id,
                "title":      job.get("title", "").strip(),
                "department": department,
                "location":   job.get("location", {}).get("name", ""),
                "company":    job.get("company_name", ""),
                "published":  published,
                "method":     "api",
            })

        logger.info(f"✅ Greenhouse: {len(jobs)} jobs for '{company}'")
        return jobs

    # ── STEP 3: DOM fallback (no dates) ───────────────────────────
    logger.warning(f"   Greenhouse API failed → DOM fallback for '{company}'")
    return _scrape_greenhouse_board_dom(board_url, company, base_url)


# ============================================================================
# JOB DETAIL — DOM (server-rendered, no JS needed)
# ============================================================================

def extract_job_with_greenhouse(job_url: str) -> dict | None:
    """
    Scrape Greenhouse job detail page.
    Page is server-rendered — plain requests + BeautifulSoup works.

    Confirmed selectors:
      title       h1.section-header.section-header--large
      location    div.job__location div
      description div.job__description
      company     img.logo[alt]  → strip " Logo"
      apply_url   same as job_url (form on same page)
    """
    try:
        company, job_id, base_url = _parse_greenhouse_job_url(job_url)
    except ValueError as e:
        logger.error(str(e))
        return None

    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }

    try:
        resp = requests.get(job_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Greenhouse detail: HTTP {resp.status_code}")
            return None
    except Exception as e:
        logger.warning(f"   Greenhouse detail fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")

    # Title
    title_el = soup.select_one("h1.section-header.section-header--large")
    if not title_el:
        logger.warning(f"   No title found for {job_url}")
        return None
    title = title_el.get_text(strip=True)
    if not title:
        return None

    job_data = {
        "jobId":     job_id,
        "url":       job_url,
        "account":   company,
        "title":     title,
        "apply_url": job_url,  # apply form is on the same page
    }

    # Location
    loc_el = soup.select_one("div.job__location div")
    if loc_el:
        job_data["location"] = loc_el.get_text(strip=True)

    # Company name from logo alt text
    logo_el = soup.select_one("img.logo")
    if logo_el:
        alt = logo_el.get("alt", "")
        # alt is typically "Altamira.ai Logo" — strip " Logo"
        company_name = alt.replace(" Logo", "").strip()
        if company_name:
            job_data["company"] = company_name

    # Description — full HTML
    desc_el = soup.select_one("div.job__description")
    if desc_el:
        desc_html = str(desc_el)
        if len(desc_html) > 100:
            job_data["description"] = desc_html

    # Also try to get description from API (cleaner, decoded HTML)
    try:
        api_url  = f"{GH_API_BASE}/{company}/jobs/{job_id}"
        api_resp = requests.get(
            api_url,
            headers={"User-Agent": random.choice(USER_AGENTS), "Accept": "application/json"},
            timeout=15,
        )
        if api_resp.status_code == 200:
            api_data = api_resp.json()

            # API content is double HTML-encoded — decode it
            raw_content = api_data.get("content", "")
            if raw_content:
                decoded = html.unescape(html.unescape(raw_content))
                if len(decoded) > 100:
                    job_data["description"] = decoded

            # Department from API
            depts = api_data.get("departments", [])
            if depts:
                job_data["department"] = depts[0].get("name", "")

            # Office / location from API
            offices = api_data.get("offices", [])
            if offices and not job_data.get("location"):
                job_data["location"] = offices[0].get("name", "")

            # Published date
            pub = api_data.get("first_published", "")
            if pub:
                job_data["published"] = pub[:10]

    except Exception as e:
        logger.debug(f"   Greenhouse detail API enrichment failed: {e}")

    has_content = bool(job_data.get("description") or job_data.get("location"))
    return job_data if has_content else None