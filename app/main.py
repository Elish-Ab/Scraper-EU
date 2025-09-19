from fastapi import FastAPI, Query
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup

app = FastAPI()


# --- Helper Functions ---

def get_job_links(board_url: str):
    """Scrape all job links from a Workable job board using Playwright."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(board_url, wait_until="networkidle")

        # Ensure job listings are loaded
        page.wait_for_selector("li[data-ui='job-opening']")

        job_items = page.query_selector_all("li[data-ui='job-opening']")
        links = []

        for item in job_items:
            # Reliable selector: anchor with aria-labelledby
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
