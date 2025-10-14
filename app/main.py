# app/main.py - Fixed Version with Correct Slug Handling
from fastapi import FastAPI, Query
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import requests
import datetime
from datetime import datetime, timedelta
import re
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MAX_DAYS = 30  # Increased to 30 days to catch more jobs

# --- Helper Functions ---

def try_json_api(board_url: str):
    """Try to fetch from the JSON API - uses ORIGINAL slug format"""
    try:
        # Extract subdomain WITHOUT converting -dot- to .
        subdomain = board_url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"Trying JSON API: {api_url}")

        cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
        all_jobs = []

        params = {"limit": 20}
        page_count = 0
        max_pages = 20

        while page_count < max_pages:
            logger.info(f"Fetching page {page_count + 1} from JSON API")
            
            resp = requests.get(api_url, params=params, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=15)

            logger.info(f"JSON API response: {resp.status_code}")

            if resp.status_code != 200:
                logger.warning(f"API failed with status {resp.status_code}")
                break

            data = resp.json()
            results = data.get("results", [])
            
            if not results:
                logger.info("No results in response")
                break

            logger.info(f"Found {len(results)} jobs in page {page_count + 1}")

            stop_pagination = False
            for job in results:
                published = job.get("published")
                if published:
                    try:
                        if published.endswith('Z'):
                            pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                        else:
                            pub_date = datetime.fromisoformat(published)

                        if pub_date < cutoff_date:
                            logger.info(f"Hit cutoff date at {pub_date}, stopping pagination")
                            stop_pagination = True
                            break
                    except Exception as e:
                        logger.debug(f"Date parsing error: {e}")

                shortcode = job.get("shortcode")
                if shortcode:
                    job_url = urljoin(board_url, f"j/{shortcode}/")
                    all_jobs.append(job_url)

            if stop_pagination:
                break

            next_page = data.get("nextPage")
            if not next_page:
                logger.info("No nextPage found, ending pagination")
                break

            params = {"limit": 20, "nextPage": next_page}
            page_count += 1

        logger.info(f"JSON API collected {len(all_jobs)} job links total")
        return list(set(all_jobs))

    except Exception as e:
        logger.error(f"JSON API failed: {e}")
        return []

def get_job_links_dom_enhanced(board_url: str):
    """Enhanced DOM scraper - continues even if some jobs are old"""
    job_links = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })

            logger.info(f"DOM: Navigating to: {board_url}")
            page.goto(board_url, wait_until="networkidle", timeout=30000)
            page.wait_for_timeout(3000)

            # Handle cookie consent
            try:
                cookie_selectors = [
                    "[data-ui='cookie-consent'] button",
                    ".cookie-consent button",
                    "button:has-text('Accept')",
                    "button:has-text('OK')"
                ]
                for selector in cookie_selectors:
                    cookie_btn = page.query_selector(selector)
                    if cookie_btn and cookie_btn.is_visible():
                        cookie_btn.click()
                        page.wait_for_timeout(1000)
                        logger.info("Dismissed cookie consent")
                        break
            except Exception as e:
                logger.debug(f"Cookie handling: {e}")

            # Find job elements
            job_selectors = [
                "li[data-ui='job']",
                "li[data-ui='job-opening']",
                ".job-item"
            ]

            all_jobs = []
            for selector in job_selectors:
                try:
                    elements = page.query_selector_all(selector)
                    if elements:
                        logger.info(f"DOM: Found {len(elements)} elements with selector: {selector}")
                        all_jobs = elements
                        break
                except:
                    continue

            # Process all found jobs
            cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
            old_jobs_count = 0
            
            for item in all_jobs:
                try:
                    link_el = None
                    for link_selector in ["a[aria-labelledby]", "a", "a.styles--1OnOt"]:
                        link_el = item.query_selector(link_selector)
                        if link_el:
                            break
                    
                    if not link_el:
                        continue

                    href = link_el.get_attribute("href")
                    if not href or '/j/' not in href:
                        continue

                    # Check posting date
                    posted_el = item.query_selector("[data-ui='job-posted']")
                    if posted_el:
                        text = posted_el.inner_text().strip()
                        if "Posted" in text and "day" in text:
                            m = re.search(r"(\d+)\s+day", text)
                            if m:
                                days = int(m.group(1))
                                if days > MAX_DAYS:
                                    old_jobs_count += 1
                                    logger.debug(f"DOM: Skipping job older than {MAX_DAYS} days: {days} days")
                                    # Don't stop, just skip this job
                                    continue

                    full_url = urljoin(board_url, href)
                    if full_url not in job_links:
                        job_links.append(full_url)
                        logger.debug(f"DOM: Found job link: {full_url}")
                        
                except Exception as e:
                    logger.debug(f"DOM: Error processing job item: {e}")
                    continue

            logger.info(f"DOM: Processed {len(all_jobs)} jobs, found {len(job_links)} valid, skipped {old_jobs_count} old")

            # Try to click "Show more" to get additional jobs
            attempts = 0
            max_attempts = 5
            
            while attempts < max_attempts:
                try:
                    page.wait_for_timeout(2000)
                    
                    show_more = page.query_selector("button[data-ui='load-more-button']")
                    
                    if not show_more or not show_more.is_visible():
                        logger.info("DOM: No more 'Show more' button found")
                        break
                    
                    logger.info(f"DOM: Clicking 'Show more' button (attempt {attempts + 1})")
                    
                    # Dismiss overlays first
                    try:
                        backdrop = page.query_selector("[data-ui='backdrop']")
                        if backdrop:
                            backdrop.click(force=True)
                            page.wait_for_timeout(500)
                    except:
                        pass
                    
                    show_more.click(force=True)
                    page.wait_for_timeout(3000)
                    
                    # Process new jobs
                    new_jobs = page.query_selector_all("li[data-ui='job'], li[data-ui='job-opening']")
                    if len(new_jobs) > len(all_jobs):
                        logger.info(f"DOM: Loaded {len(new_jobs) - len(all_jobs)} more jobs")
                        
                        for item in new_jobs[len(all_jobs):]:
                            try:
                                link_el = item.query_selector("a[aria-labelledby], a")
                                if link_el:
                                    href = link_el.get_attribute("href")
                                    if href and '/j/' in href:
                                        full_url = urljoin(board_url, href)
                                        if full_url not in job_links:
                                            job_links.append(full_url)
                                            logger.debug(f"DOM: Found additional job: {full_url}")
                            except:
                                continue
                        
                        all_jobs = new_jobs
                        attempts += 1
                    else:
                        logger.info("DOM: No new jobs loaded, stopping")
                        break
                        
                except Exception as e:
                    logger.debug(f"DOM: Error with 'Show more' (attempt {attempts + 1}): {e}")
                    attempts += 1
                    break
            
            browser.close()
            
    except Exception as e:
        logger.error(f"DOM scraping failed: {e}")
    
    return list(set(job_links))

def get_job_links(board_url: str):
    """Main function - tries JSON API first, then DOM"""
    logger.info(f"Starting job link extraction for: {board_url}")
    
    links = try_json_api(board_url)
    if links:
        logger.info(f"Successfully got {len(links)} links from JSON API")
        return links
    
    logger.info("JSON API failed, trying DOM scraping")
    links = get_job_links_dom_enhanced(board_url)
    logger.info(f"DOM scraping found {len(links)} links")
    
    return links

def fetch_job_details(job_url: str):
    """Fetch details of a single job - uses ORIGINAL slug format"""
    try:
        parts = job_url.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError("Invalid job URL format")
            
        account = parts[-3]  # e.g. infatica-dot-i-o (keep original)
        job_id = parts[-1]   # e.g. 5F1B56C10C
        
        # Keep original account format (don't convert)
        api_url = f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{job_id}"
        
        logger.debug(f"Fetching job details from: {api_url}")
        
        resp = requests.get(api_url, headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }, timeout=15)
        
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.warning(f"Job detail API failed with status {resp.status_code}")
            return {
                "job": {
                    "jobId": job_id,
                    "url": job_url,
                    "status": "limited_info",
                    "account": account
                }
            }
            
    except Exception as e:
        logger.error(f"Error fetching job details: {e}")
        return {
            "job": {
                "jobId": job_url.split("/")[-1] if "/" in job_url else "unknown",
                "url": job_url,
                "status": "error",
                "error": str(e)
            }
        }

# --- Endpoints ---

@app.get("/scrape-links")
def scrape_links(url: str = Query(..., description="Workable job board URL")):
    """Get job links from a Workable board"""
    try:
        links = get_job_links(url)
        return {"count": len(links), "links": links}
    except Exception as e:
        logger.error(f"Error in scrape_links: {e}")
        return {"error": str(e), "count": 0, "links": []}

@app.get("/scrape-job")
def scrape_job(url: str = Query(..., description="Workable job URL")):
    """Get details for a specific job"""
    try:
        job_data = fetch_job_details(url)
        return job_data
    except Exception as e:
        logger.error(f"Error in scrape_job: {e}")
        return {"error": str(e)}

@app.get("/scrape-all-jobs")
def scrape_all_jobs(url: str = Query(..., description="Workable job board URL")):
    """Get all job details using JSON API with pagination"""
    try:
        # Extract subdomain WITHOUT converting -dot- to .
        subdomain = url.strip("/").split("/")[-1]
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"Scraping all jobs from: {api_url}")
        
        cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
        all_jobs = []
        
        params = {"limit": 20}
        page_count = 0
        max_pages = 20
        
        while page_count < max_pages:
            resp = requests.get(api_url, params=params, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=15)

            if resp.status_code != 200:
                logger.warning(f"API failed with {resp.status_code}, falling back to DOM")
                break

            data = resp.json()
            results = data.get("results", [])
            
            if not results:
                break

            logger.info(f"Processing page {page_count + 1} with {len(results)} jobs")

            stop_pagination = False
            for job in results:
                published = job.get("published")
                if published:
                    try:
                        pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                        if pub_date < cutoff_date:
                            logger.info(f"Cutoff date hit at {pub_date}")
                            stop_pagination = True
                            break
                    except Exception as e:
                        logger.debug(f"Date parsing error: {e}")

                all_jobs.append({
                    "id": job.get("id"),
                    "shortcode": job.get("shortcode"),
                    "title": job.get("title"),
                    "remote": job.get("remote"),
                    "location": job.get("location"),
                    "locations": job.get("locations"),
                    "published": job.get("published"),
                    "type": job.get("type"),
                    "department": job.get("department"),
                    "workplace": job.get("workplace"),
                    "url": urljoin(url, f"j/{job.get('shortcode')}/") if job.get("shortcode") else None
                })

            if stop_pagination:
                break

            next_page = data.get("nextPage")
            if not next_page:
                break

            params = {"limit": 20, "nextPage": next_page}
            page_count += 1

        if all_jobs:
            return {
                "success": True,
                "total": len(all_jobs),
                "jobs": all_jobs,
                "api_endpoint": api_url,
                "pages_processed": page_count + 1
            }

        # Fallback to DOM if API returns nothing
        logger.info("Falling back to DOM parsing")
        links = get_job_links_dom_enhanced(url)
        return {
            "success": True,
            "total": len(links),
            "jobs": [{"url": link, "status": "from_dom"} for link in links],
            "fallback": "DOM"
        }

    except Exception as e:
        logger.error(f"Error in scrape_all_jobs: {e}")
        return {"success": False, "error": str(e), "jobs": []}

@app.get("/test-api-endpoints")
def test_api_endpoints(url: str = Query(..., description="Workable job board URL")):
    """Test different API endpoints"""
    # Extract subdomain WITHOUT converting
    subdomain = url.strip("/").split("/")[-1]
    
    endpoints_to_test = [
        f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs",
        f"https://apply.workable.com/api/v2/accounts/{subdomain}/jobs",
        f"https://apply.workable.com/api/v1/accounts/{subdomain}/jobs",
    ]
    
    results = []
    
    for endpoint in endpoints_to_test:
        try:
            resp = requests.get(endpoint, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=10)
            
            result = {
                "endpoint": endpoint,
                "status_code": resp.status_code,
                "success": resp.status_code == 200
            }
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if "results" in data:
                        result["has_results"] = True
                        result["results_count"] = len(data.get("results", []))
                        result["has_next_page"] = "nextPage" in data
                        if data.get("results"):
                            sample = data.get("results", [{}])[0]
                            result["sample_data"] = {
                                "id": sample.get("id"),
                                "title": sample.get("title"),
                                "shortcode": sample.get("shortcode"),
                                "published": sample.get("published")
                            }
                except Exception as e:
                    result["error"] = str(e)
            
            results.append(result)
            
        except Exception as e:
            results.append({
                "endpoint": endpoint,
                "status_code": None,
                "success": False,
                "error": str(e)
            })
    
    return {
        "subdomain": subdomain,
        "original_url": url,
        "endpoints_tested": len(endpoints_to_test),
        "results": results
    }

@app.get("/")
def root():
    return {
        "message": "Workable Job Scraper API - Fixed JSON API Version", 
        "endpoints": {
            "/scrape-links": "Get job links (JSON API with DOM fallback)",
            "/scrape-job": "Get details for a specific job",
            "/scrape-all-jobs": "Get all job details with pagination",
            "/test-api-endpoints": "Test which API endpoints work"
        },
        "config": {
            "max_days": MAX_DAYS,
            "note": "Uses original slug format (keeps -dot- instead of converting to .)"
        }
    }