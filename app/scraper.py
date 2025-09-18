from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import logging
import requests

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # use INFO in production
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)


def get_job_links(board_url: str):
    """Scrape all job links from a Workable job board using Playwright."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        logger.info(f"Opening board URL: {board_url}")
        page.goto(board_url, wait_until="networkidle")

        logger.info("Waiting for job listings to load...")
        page.wait_for_selector("li[data-ui='job-opening']")

        job_items = page.query_selector_all("li[data-ui='job-opening']")
        logger.info(f"Found {len(job_items)} job items")

        links = []
        for item in job_items:
            link_el = item.query_selector("a[aria-labelledby]")
            if link_el:
                href = link_el.get_attribute("href")
                if href:
                    full_link = urljoin(board_url, href)
                    links.append(full_link)

        browser.close()
        logger.info(f"Collected {len(links)} job links")
        return list(set(links))


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
