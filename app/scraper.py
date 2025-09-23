from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import logging
import requests
import re

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

def get_job_links(board_url: str, max_days: int = 5):
    """Scrape job links posted within the last `max_days` days."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(board_url, wait_until="networkidle")

        # Wait for job listings
        page.wait_for_selector("li[data-ui='job-opening']")

        job_items = page.query_selector_all("li[data-ui='job-opening']")
        links = []

        for item in job_items:
            # Posted date text
            posted_el = item.query_selector("small[data-ui='job-posted']")
            posted_text = posted_el.inner_text().strip() if posted_el else ""
            days = parse_posted_days(posted_text)

            # Stop if older than max_days
            if days > max_days:
                break

            # Job link
            link_el = item.query_selector("a[aria-labelledby]")
            if link_el:
                href = link_el.get_attribute("href")
                if href:
                    full_link = urljoin(board_url, href)
                    links.append(full_link)

        browser.close()
        return list(set(links))  # deduplicate

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
