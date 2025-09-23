from fastapi import FastAPI, Query
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import re
app = FastAPI()


# --- Helper Functions ---

def parse_posted_days(text: str) -> int:
    """Convert 'Posted X days ago' into integer days."""
    if not text:
        return 9999
    m = re.search(r"(\d+)\s+day", text)
    if m:
        return int(m.group(1))
    if "hour" in text.lower():   # e.g. "Posted 3 hours ago"
        return 0
    return 9999

def get_job_links(board_url: str, max_days: int = 5):
    """Scrape job links from a Workable job board, filtering jobs posted in last `max_days` days."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(board_url, wait_until="networkidle")

        # Ensure job listings are loaded
        page.wait_for_selector("li[data-ui='job-opening']")

        job_items = page.query_selector_all("li[data-ui='job-opening']")
        links = []

        for item in job_items:
            # Extract posted date
            posted_el = item.query_selector("small[data-ui='job-posted']")
            posted_text = posted_el.inner_text().strip() if posted_el else ""
            days = parse_posted_days(posted_text)

            # Stop if older than max_days
            if days > max_days:
                break

            # Extract job link
            link_el = item.query_selector("a[aria-labelledby]")
            if link_el:
                href = link_el.get_attribute("href")
                if href:
                    full_link = urljoin(board_url, href)
                    links.append(full_link)

        browser.close()
        return list(set(links))  # deduplicate


def fetch_job_details(job_url: str):
    """Fetch details of a single job posting from Workable API"""
    try:
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
        return {
            "job": {
                "jobId": job_id,
                "url": job_url,
                "status": "error",
                "error": str(e)
            }
        }


# --- Endpoints ---

@app.get("/scrape-links")
def scrape_links(url: str = Query(..., description="Workable job board URL")):
    try:
        links = get_job_links(url)
        return {"count": len(links), "links": links}
    except Exception as e:
        return {"error": str(e)}


@app.get("/scrape-job")
def scrape_job(url: str = Query(..., description="Workable job URL")):
    try:
        job_data = fetch_job_details(url)
        return job_data
    except Exception as e:
        return {"error": str(e)}

@app.get("/")
def root():
    return {"message": "Workable Job Scraper API. Use /scrape-links and /scrape-job endpoints."}
