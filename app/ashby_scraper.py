# app/ashby_scraper.py
from datetime import datetime, timezone
from urllib.parse import urlparse
import logging
import random
import re
import requests
import json
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

def _company_name_from_slug(slug: str) -> str:
    return " ".join(w.capitalize() for w in re.split(r"[-_]+", slug) if w)


def is_ashby_url(url: str) -> bool:
    host = urlparse(url).netloc.lower()
    return "ashbyhq.com" in host


def parse_ashby_board_url(board_url: str) -> str:
    parts = [p for p in urlparse(board_url).path.strip("/").split("/") if p]
    if not parts:
        raise ValueError(f"Cannot parse Ashby board URL: {board_url}")
    return parts[0]


def parse_ashby_job_url(job_url: str) -> tuple[str, str]:
    parts = [p for p in urlparse(job_url).path.strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Cannot parse Ashby job URL: {job_url}")
    return parts[0], parts[1]


# ============================================================================
# __appData EXTRACTOR — primary source, has publishedDate
# ============================================================================

def _extract_app_data(html: str) -> dict | None:
    """Extract window.__appData JSON from Ashby board page HTML."""
    marker = "window.__appData = "
    pos = html.find(marker)
    if pos == -1:
        return None
    start = html.find("{", pos)
    if start == -1:
        return None

    depth = 0
    end   = None
    in_str = False
    escape = False
    for i in range(start, len(html)):
        c = html[i]
        if escape:
            escape = False
            continue
        if c == "\\" and in_str:
            escape = True
            continue
        if c == '"' and not escape:
            in_str = not in_str
            continue
        if in_str:
            continue
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                end = i + 1
                break

    if end is None:
        return None

    try:
        return json.loads(html[start:end])
    except Exception as e:
        logger.warning(f"   __appData JSON parse failed: {e}")
        return None


def _fetch_ashby_api(company: str) -> list:
    """
    Fetch job postings from window.__appData in the board page HTML.
    __appData contains publishedDate (e.g. "2026-01-29") which the
    GraphQL API does not expose. Falls back to GraphQL if page parsing fails.
    """
    board_url = f"{ASHBY_BASE}/{company}/"
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    try:
        resp = requests.get(board_url, headers=headers, timeout=30)
        if resp.status_code == 200:
            app_data = _extract_app_data(resp.text)
            if app_data:
                job_postings = app_data.get("jobBoard", {}).get("jobPostings", [])
                if job_postings:
                    logger.info(f"   Ashby __appData: {len(job_postings)} jobs for '{company}'")
                    return job_postings
    except Exception as e:
        logger.warning(f"   Ashby board page fetch failed: {e}")

    logger.warning(f"   Falling back to GraphQL for '{company}'")
    return _fetch_ashby_graphql(company)


def _fetch_ashby_graphql(company: str) -> list:
    """GraphQL fallback — no publishedDate available."""
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
            return []
        data         = resp.json()
        board        = data.get("data", {}).get("jobBoard", {})
        teams        = board.get("teams", [])
        job_postings = board.get("jobPostings", [])
        team_map = {t["id"]: t["name"] for t in teams}
        for job in job_postings:
            job["teamName"] = team_map.get(job.get("teamId"), "")
        logger.info(f"   Ashby GraphQL: {len(job_postings)} jobs for '{company}'")
        return job_postings
    except Exception as e:
        logger.warning(f"   Ashby GraphQL failed: {e}")
        return []


# ============================================================================
# BOARD PAGE — DOM SCRAPER (last resort fallback)
# ============================================================================

def _scrape_ashby_board_dom(board_url: str, company: str) -> list:
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
        path_parts = [p for p in href.strip("/").split("/") if p]
        if len(path_parts) < 2:
            continue
        job_id = path_parts[-1]
        if job_id == "application":
            continue

        title_el = el.select_one("h3.ashby-job-posting-brief-title")
        if not title_el:
            continue
        title = title_el.get_text(strip=True)
        if not title:
            continue

        details_el = el.select_one("div.ashby-job-posting-brief-details p")
        location = job_type = workplace = ""
        if details_el:
            parts = [p.strip() for p in details_el.get_text().split("•")]
            if len(parts) >= 2:
                location = parts[1].strip()
            if len(parts) >= 3:
                job_type = parts[2].strip()
            if len(parts) >= 4:
                workplace = parts[3].strip()

        jobs.append({
            "url":        f"{ASHBY_BASE}{href}",
            "jobId":      job_id,
            "title":      title,
            "department": current_dept,
            "location":   location,
            "type":       job_type,
            "workplace":  workplace,
            "published":  "",
            "method":     "dom",
        })

    logger.info(f"   Ashby board DOM: {len(jobs)} jobs")
    return jobs


# ============================================================================
# GET LINKS — __appData primary, GraphQL fallback, DOM last resort
# ============================================================================

def get_links_from_ashby(board_url: str, since_dt: datetime = None) -> list:
    """
    Get Ashby job listings.

    Step 1 — Parse window.__appData from board page HTML:
        Contains publishedDate (e.g. "2026-01-29") for date filtering.

    Step 2 — Date filter using publishedDate.

    Step 3 — GraphQL API fallback (no dates).

    Step 4 — DOM fallback (no dates, last resort).
    """
    try:
        company = parse_ashby_board_url(board_url)
    except ValueError as e:
        logger.error(str(e))
        return []

    api_jobs = _fetch_ashby_api(company)

    if not api_jobs:
        logger.warning(f"   Ashby all API methods failed → DOM fallback for '{company}'")
        return _scrape_ashby_board_dom(board_url, company)

    # Date filter using publishedDate from __appData
    # GraphQL fallback jobs won't have publishedDate → include all
    filtered = []
    for posting in api_jobs:
        if since_dt:
            pub_str = posting.get("publishedDate", "")
            if pub_str:
                try:
                    pub_date = datetime.fromisoformat(pub_str[:10]).replace(tzinfo=timezone.utc)
                    if pub_date < since_dt:
                        continue
                except:
                    pass  # unparseable → include
        filtered.append(posting)

    logger.info(f"   After date filter: {len(filtered)} jobs (from {len(api_jobs)} total)")

    jobs = []
    for posting in filtered:
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
        pub_raw       = posting.get("publishedDate", "")  # from __appData

        # teamName is already in __appData; GraphQL fallback sets it via team_map
        department = posting.get("teamName", "")

        jobs.append({
            "url":        f"{ASHBY_BASE}/{company}/{posting_id}",
            "jobId":      posting_id,
            "title":      posting.get("title", ""),
            "department": department,
            "location":   all_locations[0] if all_locations else "",
            "locations":  all_locations if len(all_locations) > 1 else None,
            "type":       "Full time" if posting.get("employmentType") == "FullTime" else posting.get("employmentType", ""),
            "workplace":  posting.get("workplaceType", ""),
            "published":  pub_raw[:10] if pub_raw else "",
            "method":     "api",
        })

    logger.info(f"✅ Ashby: {len(jobs)} jobs for '{company}'")
    return jobs


# ============================================================================
# JOB DETAIL — Playwright (React-rendered page)
# ============================================================================

def extract_job_with_ashby(job_url: str) -> dict | None:
    """
    Scrape Ashby job detail page using Playwright (React-rendered).

    Confirmed selectors (tribe-xyz, wayflyer):
      title           h1.ashby-job-posting-heading
      location        .ashby-job-posting-left-pane section h2="Location" → p
      employment_type .ashby-job-posting-left-pane section h2="Employment Type" → p
      workplace       .ashby-job-posting-left-pane section h2="Location Type" → p
      department      .ashby-job-posting-left-pane section h2="Department" → p
      description     .ashby-job-posting-right-pane [role="tabpanel"]
      apply_url       a[href*="/application"]
    """
    try:
        company, job_id = parse_ashby_job_url(job_url)
    except ValueError as e:
        logger.error(str(e))
        return None

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
        import threading
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

                # Wait for React to render title
                try:
                    page.wait_for_selector("h1.ashby-job-posting-heading", timeout=10000)
                except:
                    pass
                page.wait_for_timeout(1000)

                job_data = {
                    "jobId":   job_id,
                    "url":     job_url,
                    "account": company,
                    "company": _company_name_from_slug(company),
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

                # Left pane metadata sections
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
                            # Nested dept headings e.g. "Revenue\nSales" → "Revenue > Sales"
                            job_data["department"] = " > ".join(
                                line.strip() for line in text.splitlines() if line.strip()
                            )
                except:
                    pass

                # Description (right pane overview tab)
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