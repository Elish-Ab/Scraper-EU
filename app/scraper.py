# app/scraper.py
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import logging
import requests
import re
import datetime
from datetime import datetime, timedelta
# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # use INFO in production
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def parse_posted_days(text: str) -> int:
    """Convert 'Posted X days ago' or 'Posted 1 day ago' into an integer."""
    if not text:
        return 9999
    m = re.search(r"(\d+)\s+day", text)
    if m:
        return int(m.group(1))
    if "hour" in text.lower():   # treat 'Posted X hours ago' as 0 days
        return 0
    return 9999


MAX_DAYS = 5

def fetch_from_api(board_url: str):
    """Fetch jobs from Workable API using cursor pagination and cutoff by days."""
    subdomain = board_url.strip("/").split("/")[-1]
    api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"

    cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
    jobs = []

    params = {"limit": 20}
    while True:
        resp = requests.get(api_url, params=params, timeout=15)
        if resp.status_code != 200:
            logger.debug(f"[API] Request failed: {resp.status_code}")
            break

        data = resp.json()
        results = data.get("results", [])
        if not results:
            break

        logger.debug(f"[API] Got {len(results)} jobs in this batch, total so far: {len(jobs)}")

        stop = False
        for job in results:
            published = job.get("published")
            if published:
                pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                if pub_date < cutoff_date:
                    logger.debug(f"[API] Hit cutoff date at {pub_date}, stopping pagination.")
                    stop = True
                    break

            shortcode = job.get("shortcode")
            if shortcode:
                full_link = urljoin(board_url, f"j/{shortcode}/")
                jobs.append(full_link)

        if stop:
            break

        # Follow pagination
        next_page = data.get("nextPage")
        if not next_page:
            break
        params = {"limit": 20, "nextPage": next_page}

    return jobs


def fetch_from_dom(board_url: str):
    """Fallback DOM scraper with cutoff by days (for boards without API)."""
    cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
    jobs = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(board_url, wait_until="networkidle")

        while True:
            # Grab all job items currently loaded
            job_items = page.query_selector_all("li[data-ui='job'], li[data-ui='job-opening']")
            for item in job_items[len(jobs):]:
                # Extract link
                link_el = item.query_selector("a[aria-labelledby], a.styles--1OnOt")
                posted_el = item.query_selector("[data-ui='job-posted']")
                if not link_el or not posted_el:
                    continue

                href = link_el.get_attribute("href")
                text = posted_el.inner_text().strip()

                # Check posted days
                if "Posted" in text and "day" in text:
                    try:
                        days = int(text.split()[1])
                        if days > MAX_DAYS:
                            browser.close()
                            return jobs  # stop when older than cutoff
                    except Exception:
                        pass

                jobs.append(urljoin(board_url, href))

            # Try clicking "Show more"
            try:
                show_more = page.query_selector("button[data-ui='load-more-button']")
                if show_more:
                    show_more.click()
                    page.wait_for_timeout(1500)  # wait for jobs to load
                    continue
            except Exception:
                pass
            break

        browser.close()
    return jobs


def get_job_links(board_url: str):
    """General entry point: try API first, then fallback to DOM."""
    try:
        jobs = fetch_from_api(board_url)
        if jobs:
            return jobs
    except Exception as e:
        print(f"[DEBUG] API fetch failed: {e}")

    return fetch_from_dom(board_url)
def fetch_job_details(job_url: str):
    try:
        # Extract account slug and job id from the URL
        parts = job_url.strip("/").split("/")
        account = parts[-3]  # e.g. constructor-1
        job_id = parts[-1]   # e.g. 5F1B56C10C

        api_url = f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{job_id}"

        resp = requests.get(api_url, headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0"
        })
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        return {"job": {"jobId": job_id, "url": job_url, "status": "error", "error": str(e)}}

def scrape_jobs(board_url: str):
    """Full scrape: first collect links, then fetch job details for each."""
    job_links = get_job_links(board_url)
    logger.info(f"Found {len(job_links)} job links total")
    jobs = [fetch_job_details(link) for link in job_links]
    return jobs
