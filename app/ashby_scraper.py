# app/ashby_scraper.py
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse
import logging
import random
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
]

ASHBY_BASE = "https://jobs.ashbyhq.com"
ASHBY_API  = "https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams"

GRAPHQL_QUERY = """
query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) {
  jobBoard: jobBoardWithTeams(
    organizationHostedJobsPageName: $organizationHostedJobsPageName
  ) {
    teams {
      id
      name
      parentTeamId
    }
    jobPostings {
      id
      title
      teamId
      locationId
      locationName
      workplaceType
      employmentType
      secondaryLocations {
        locationId
        locationName
      }
      compensationTierSummary
    }
  }
}
"""


# ============================================================================
# URL HELPERS
# ============================================================================

def is_ashby_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return "ashbyhq.com" in host


def parse_ashby_board_url(board_url: str) -> str:
    """Extract company slug from board URL.
    e.g. https://jobs.ashbyhq.com/wayflyer → wayflyer
    """
    parts = [p for p in urlparse(board_url).path.strip("/").split("/") if p]
    if not parts:
        raise ValueError(f"Cannot parse Ashby board URL: {board_url}")
    return parts[0]


def parse_ashby_job_url(job_url: str) -> tuple[str, str]:
    """Extract company slug and job ID from job URL.
    e.g. https://jobs.ashbyhq.com/tribe-xyz/320651d4-... → (tribe-xyz, 320651d4-...)
    """
    parts = [p for p in urlparse(job_url).path.strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Cannot parse Ashby job URL: {job_url}")
    return parts[0], parts[1]


# ============================================================================
# GRAPHQL API — board listing with published dates
# ============================================================================

def _fetch_ashby_api(company: str) -> list:
    """
    Fetch all job postings via Ashby GraphQL API.
    Returns list of raw job dicts with: id, title, teamId, locationName,
    workplaceType, employmentType, secondaryLocations, publishedAt
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": random.choice(USER_AGENTS),
        "Origin": ASHBY_BASE,
        "Referer": f"{ASHBY_BASE}/{company}",
    }
    payload = {
        "operationName": "ApiJobBoardWithTeams",
        "query": GRAPHQL_QUERY,
        "variables": {"organizationHostedJobsPageName": company},
    }
    try:
        resp = requests.post(ASHBY_API, json=payload, headers=headers, timeout=20)
        if resp.status_code != 200:
            logger.warning(f"   Ashby API status {resp.status_code}")
            return []
        data = resp.json()
        board = data.get("data", {}).get("jobBoard", {})
        teams        = board.get("teams", [])
        job_postings = board.get("jobPostings", [])
        # Build team id → name map
        team_map = {t["id"]: t["name"] for t in teams}
        # Attach team name to each job
        for job in job_postings:
            job["teamName"] = team_map.get(job.get("teamId"), "")
        logger.info(f"   Ashby API: {len(job_postings)} jobs for '{company}'")
        return job_postings
    except Exception as e:
        logger.warning(f"   Ashby API failed: {e}")
        return []


# ============================================================================
# BOARD PAGE — DOM SCRAPER
# ============================================================================

def _scrape_ashby_board_dom(board_url: str, company: str) -> list:
    """
    Scrape board page HTML for job listings.
    Returns list of dicts with: url, jobId, title, department,
    location, type, workplace
    Note: no published date in DOM — always use API for dates.
    """
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    }
    try:
        resp = requests.get(board_url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.warning(f"   Ashby board DOM: HTTP {resp.status_code}")
            return []
    except Exception as e:
        logger.warning(f"   Ashby board DOM fetch failed: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    jobs = []

    # Each department group has an h2 heading followed by job links
    for group in soup.select("div._departments_1cp2r_345 > *"):
        # Track current department as we iterate siblings
        pass

    # Simpler: iterate all job links and find preceding department heading
    current_dept = ""
    for el in soup.select("div._departments_1cp2r_345 h2, a._container_j2da7_1"):
        if el.name == "h2":
            current_dept = el.get_text(strip=True)
            continue

        if el.name != "a":
            continue

        href = el.get("href", "")
        if not href:
            continue

        # Job ID is last path segment (UUID)
        path_parts = [p for p in href.strip("/").split("/") if p]
        if len(path_parts) < 2:
            continue
        job_id = path_parts[-1]

        # Skip /application links
        if job_id == "application":
            continue

        url = f"{ASHBY_BASE}{href}"

        title_el = el.select_one("h3.ashby-job-posting-brief-title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        # Details line: "Department • Location • Type • Workplace"
        details_el = el.select_one("div.ashby-job-posting-brief-details p")
        location  = ""
        job_type  = ""
        workplace = ""

        if details_el:
            parts = [p.strip() for p in details_el.get_text().split("•")]
            # parts[0] = dept/company name (skip — use heading dept instead)
            # parts[1] = location(s)
            # parts[2] = employment type
            # parts[3] = workplace type
            if len(parts) >= 2:
                location = parts[1].strip()
            if len(parts) >= 3:
                job_type = parts[2].strip()
            if len(parts) >= 4:
                workplace = parts[3].strip()

        jobs.append({
            "url":        url,
            "jobId":      job_id,
            "title":      title,
            "department": current_dept,
            "location":   location,
            "type":       job_type,
            "workplace":  workplace,
            "method":     "dom",
        })

    logger.info(f"   Ashby board DOM: {len(jobs)} jobs")
    return jobs


# ============================================================================
# GET LINKS — API PRIMARY (has dates), DOM fallback
# ============================================================================

def get_links_from_ashby(board_url: str, since_dt: datetime = None) -> list:
    try:
        company = parse_ashby_board_url(board_url)
    except ValueError as e:
        logger.error(str(e))
        return []

    if since_dt:
        logger.info(f"   ⚠️  Ashby has no published dates — returning all jobs")

    api_jobs = _fetch_ashby_api(company)

    if not api_jobs:
        logger.warning(f"   Ashby API failed → falling back to DOM for '{company}'")
        return _scrape_ashby_board_dom(board_url, company)

    jobs = []
    for posting in api_jobs:  # ← renamed from 'job' to 'posting' to avoid shadowing
        posting_id = posting.get("id", "")
        if not posting_id:
            continue

        primary_loc    = posting.get("locationName", "")
        secondary_locs = [
            s.get("locationName", "")
            for s in posting.get("secondaryLocations", [])
            if s.get("locationName")
        ]
        all_locations = [l.strip() for l in ([primary_loc] + secondary_locs) if l.strip()]

        jobs.append({
            "url":        f"{ASHBY_BASE}/{company}/{posting_id}",
            "jobId":      posting_id,
            "title":      posting.get("title", ""),
            "department": posting.get("teamName", ""),
            "location":   all_locations[0] if all_locations else "",
            "locations":  all_locations if len(all_locations) > 1 else None,
            "type":       posting.get("employmentType", ""),
            "type": "Full time" if posting.get("employmentType") == "FullTime" else posting.get("employmentType", ""),
            "workplace":  posting.get("workplaceType", ""),
            "published":  "",
            "method":     "api",
        })

    logger.info(f"✅ Ashby: {len(jobs)} jobs for '{company}'")
    return jobs
    # ── STEP 3: DOM fallback (no dates) ───────────────────────────

    logger.warning(f"   Ashby API failed → falling back to DOM for '{company}'")
    dom_jobs = _scrape_ashby_board_dom(board_url, company)
    logger.info(f"✅ Ashby DOM fallback: {len(dom_jobs)} jobs")
    return dom_jobs


# ============================================================================
# JOB DETAIL — DOM PRIMARY
# ============================================================================
def extract_job_with_ashby(job_url: str) -> dict | None:
    try:
        company, job_id = parse_ashby_job_url(job_url)
    except ValueError as e:
        logger.error(str(e))
        return None

    # Ashby renders via React — need a real browser
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
        import threading
        # Import semaphore from main to avoid concurrent browser launches
        try:
            from app.main import playwright_semaphore
        except ImportError:
            playwright_semaphore = threading.Semaphore(1)

        with playwright_semaphore:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox', '--disable-gpu', '--disable-dev-shm-usage',
                ])
                context = browser.new_context(
                    user_agent=random.choice(USER_AGENTS),
                    viewport={'width': 1280, 'height': 800},
                )
                page = context.new_page()

                try:
                    page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                except PlaywrightTimeout:
                    logger.warning(f"   Ashby detail timeout: {job_url}")
                    browser.close()
                    return None

                # Wait for React to render
                try:
                    page.wait_for_selector("h1.ashby-job-posting-heading", timeout=10000)
                except:
                    pass
                page.wait_for_timeout(1000)

                job_data = {
                    "jobId":   job_id,
                    "url":     job_url,
                    "account": company,
                }

                # Title
                try:
                    el = page.query_selector("h1.ashby-job-posting-heading")
                    if el:
                        job_data["title"] = el.inner_text().strip()
                except:
                    pass

                if not job_data.get("title"):
                    browser.close()
                    return None

                # Left pane metadata
                try:
                    for section in page.query_selector_all(".ashby-job-posting-left-pane div"):
                        heading = section.query_selector("h2")
                        value   = section.query_selector("p")
                        if not heading or not value:
                            continue
                        label = heading.inner_text().strip().lower()
                        text  = value.inner_text().strip()
                        if label == "location":
                            job_data["location"] = text
                        elif label == "employment type":
                            job_data["type"] = text
                        elif label == "location type":
                            job_data["workplace"] = text
                        elif label == "department":
    # May have nested spans like "Revenue\nSales" → "Revenue > Sales"
                            job_data["department"] = " > ".join(
                                line.strip() for line in text.splitlines() if line.strip()
                            )
                except:
                    pass

                # Description
                try:
                    el = page.query_selector(".ashby-job-posting-right-pane [role='tabpanel']")
                    if not el:
                        el = page.query_selector(".ashby-job-posting-right-pane")
                    if el:
                        html = el.inner_html().strip()
                        if len(html) > 100:
                            job_data["description"] = html
                except:
                    pass

                # Apply URL
                try:
                    el = page.query_selector('a[href*="/application"]')
                    if el:
                        href = el.get_attribute("href")
                        if href:
                            job_data["apply_url"] = f"{ASHBY_BASE}{href}" if href.startswith("/") else href
                except:
                    pass

                browser.close()

                has_content = bool(job_data.get("description") or job_data.get("location"))
                return job_data if has_content else None

    except Exception as e:
        logger.error(f"   Ashby detail failed: {e}")
        return None