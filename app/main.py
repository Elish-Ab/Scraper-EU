# app/main.py - Complete Fixed Version with Working JSON API
from fastapi import FastAPI, Query
from urllib.parse import urljoin
from playwright.sync_api import sync_playwright
import requests
from bs4 import BeautifulSoup
import datetime
from datetime import datetime, timedelta
import re
import logging

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

MAX_DAYS = 5

# --- Helper Functions ---

def extract_account_slug(board_url: str) -> str:
    """Extract workable account slug (handles -dot- cases)."""
    slug = board_url.strip("/").split("/")[-1]
    # Replace "-dot-" with "."
    slug = slug.replace("-dot-", ".")
    return slug

def try_json_api(board_url: str):
    """Try to fetch from the JSON API that we know is working"""
    try:
        subdomain = extract_account_slug(board_url)
        
        # Based on your network tab, this is the working endpoint
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"Trying JSON API: {api_url}")
        
        cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
        all_jobs = []
        
        # Start with first page
        params = {"limit": 20}
        page_count = 0
        max_pages = 20  # Safety limit
        
        while page_count < max_pages:
            logger.debug(f"Fetching page {page_count + 1} from JSON API")
            
            resp = requests.get(api_url, params=params, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }, timeout=15)
            
            logger.debug(f"JSON API response: {resp.status_code}")
            
            if resp.status_code != 200:
                logger.debug(f"API failed with status {resp.status_code}: {resp.text[:200]}")
                break
            
            data = resp.json()
            if "shortcode" in data and data.get("shortcode") == "Required":
                logger.warning("JSON API is not available for this account. Falling back to DOM.")
                return []

            results = data.get("results", [])

            
            if not results:
                logger.debug("No results in response")
                break
            
            logger.info(f"Found {len(results)} jobs in page {page_count + 1}")
            
            # Process jobs from this page
            stop_pagination = False
            for job in results:
                # Check if job is recent enough
                published = job.get("published")
                if published:
                    try:
                        # Handle different date formats
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
            
            # Check for next page
            next_page = data.get("nextPage")
            if not next_page:
                logger.debug("No nextPage found, ending pagination")
                break
            
            # Set up params for next page
            params = {"limit": 20, "nextPage": next_page}
            page_count += 1
        
        logger.info(f"JSON API collected {len(all_jobs)} job links total")
        return list(set(all_jobs))
        
    except Exception as e:
        logger.error(f"JSON API failed: {e}")
        return []

def get_job_links_dom_enhanced(board_url: str):
    """Enhanced DOM scraper with better cookie and modal handling"""
    job_links = []
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            # Set user agent to avoid blocking
            page.set_extra_http_headers({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
            })
            
            logger.debug(f"DOM: Navigating to: {board_url}")
            page.goto(board_url, wait_until="networkidle", timeout=30000)
            
            # Wait for content and handle cookies
            page.wait_for_timeout(3000)
            
            # Try to dismiss cookie consent if it exists
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
                        logger.debug("Dismissed cookie consent")
                        break
            except Exception as e:
                logger.debug(f"Cookie handling failed: {e}")
            
            # Find job elements
            job_selectors = [
                "li[data-ui='job']",
                "li[data-ui='job-opening']", 
                ".job-item",
                "[data-testid*='job']"
            ]
            
            all_jobs = []
            for selector in job_selectors:
                try:
                    elements = page.query_selector_all(selector)
                    if elements:
                        logger.debug(f"DOM: Found {len(elements)} elements with selector: {selector}")
                        all_jobs = elements
                        break
                except:
                    continue
            
            # Process found job elements
            for item in all_jobs:
                try:
                    # Try different ways to get the job link
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
                    
                    # Check posting date if possible
                    posted_el = item.query_selector("[data-ui='job-posted']")
                    if posted_el:
                        text = posted_el.inner_text().strip()
                        if "Posted" in text:
                            m = re.search(r"(\d+)\s+day", text)
                            if m:
                                days = int(m.group(1))
                                if days > MAX_DAYS:
                                    logger.debug(f"DOM: Found job older than {MAX_DAYS} days: {days} days. Stopping.")
                                    browser.close()
                                    return list(set(job_links))

                    
                    full_url = urljoin(board_url, href)
                    if full_url not in job_links:
                        job_links.append(full_url)
                        logger.debug(f"DOM: Found job link: {full_url}")
                        
                except Exception as e:
                    logger.debug(f"DOM: Error processing job item: {e}")
                    continue
            
            # Try to click "Show more" with better handling
            attempts = 0
            max_attempts = 5
            
            seen_links = set(job_links)
            while attempts < max_attempts:
                try:
                    page.wait_for_timeout(1500)

                    # Attempt to find the "Show more" button using multiple strategies
                    show_more_btn = page.query_selector("button[data-ui='load-more-button']")
                    if not show_more_btn or not show_more_btn.is_visible():
                        logger.debug("DOM: No more 'Show more' button visible, stopping.")
                        break

                    logger.debug(f"DOM: Attempting to click 'Show more' (attempt {attempts + 1})")

                    # Scroll into view & click using JS (more robust)
                    page.evaluate("(el) => el.scrollIntoView()", show_more_btn)
                    page.evaluate("(el) => el.click()", show_more_btn)
                    page.wait_for_timeout(3000)  # Wait for jobs to load

                    # Re-check the job list
                    updated_jobs = page.query_selector_all("li[data-ui='job'], li[data-ui='job-opening']")

                    new_found = 0
                    for item in updated_jobs:
                        try:
                            link_el = item.query_selector("a[aria-labelledby], a")
                            if not link_el:
                                continue
                            href = link_el.get_attribute("href")
                            if not href or "/j/" not in href:
                                continue
                            full_url = urljoin(board_url, href)
                            if full_url not in seen_links:
                                job_links.append(full_url)
                                seen_links.add(full_url)
                                new_found += 1
                                logger.debug(f"DOM: (more) Found job link: {full_url}")
                        except:
                            continue

                    if new_found == 0:
                        logger.debug("DOM: No new job links detected, stopping.")
                        break
                    else:
                        attempts += 1

                except Exception as e:
                    logger.debug(f"DOM: Error clicking 'Show more' (attempt {attempts + 1}): {e}")
                    break
            
            browser.close()
            
    except Exception as e:
        logger.error(f"DOM scraping failed: {e}")
    
    return list(set(job_links))

def get_job_links(board_url: str):
    """Main function to get job links - tries JSON API first, then DOM"""
    logger.info(f"Starting job link extraction for: {board_url}")
    
    # First try the JSON API with pagination
    logger.info("Trying JSON API first")
    links = try_json_api(board_url)
    if links:
        logger.info(f"Successfully got {len(links)} links from JSON API")
        return links
    
    # Fallback to enhanced DOM scraping
    logger.info("JSON API failed, trying DOM scraping")
    links = get_job_links_dom_enhanced(board_url)
    logger.info(f"DOM scraping found {len(links)} links")
    
    return links

def fetch_job_details(job_url: str):
    """Fetch details of a single job posting from Workable API"""
    try:
        parts = job_url.strip("/").split("/")
        if len(parts) < 2:
            raise ValueError("Invalid job URL format")
            
        account = parts[-3]  # e.g. infatica-dot-i-o
        job_id = parts[-1]   # e.g. 5F1B56C10C
        
        # Convert account slug back for API call
        api_account = account.replace("-dot-", ".")
        
        # Try the public job detail API
        api_url = f"https://apply.workable.com/api/v2/accounts/{api_account}/jobs/{job_id}"
        
        logger.debug(f"Fetching job details from: {api_url}")
        
        resp = requests.get(api_url, headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }, timeout=15)
        
        if resp.status_code == 200:
            return resp.json()
        else:
            logger.debug(f"Job detail API failed with status {resp.status_code}")
            return {
                "job": {
                    "jobId": job_id,
                    "url": job_url,
                    "status": "limited_info",
                    "account": account
                }
            }
            
    except Exception as e:
        logger.error(f"Error fetching job details for {job_url}: {e}")
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
    try:
        links = get_job_links(url)
        return {"count": len(links), "links": links}
    except Exception as e:
        logger.error(f"Error in scrape_links: {e}")
        return {"error": str(e), "count": 0, "links": []}

@app.get("/scrape-job")
def scrape_job(url: str = Query(..., description="Workable job URL")):
    try:
        job_data = fetch_job_details(url)
        return job_data
    except Exception as e:
        logger.error(f"Error in scrape_job: {e}")
        return {"error": str(e)}

@app.get("/scrape-all-jobs")
def scrape_all_jobs(url: str = Query(..., description="Workable job board URL")):
    """Get all job details in one call using JSON API with fallback to DOM"""
    try:
        subdomain = extract_account_slug(url)
        api_url = f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs"
        
        logger.info(f"Scraping all jobs from: {api_url}")
        
        cutoff_date = datetime.utcnow() - timedelta(days=MAX_DAYS)
        all_jobs = []
        
        # Start with first page
        params = {"limit": 20}
        page_count = 0
        max_pages = 20
        
        while page_count < max_pages:
            resp = requests.get(api_url, params=params, headers={
                "Accept": "application/json",
                "User-Agent": "Mozilla/5.0"
            }, timeout=15)

            if resp.status_code != 200:
                logger.warning(f"API failed with {resp.status_code}, falling back to DOM")
                break

            data = resp.json()

            # Add fallback check here
            if "shortcode" in data and data.get("shortcode") == "Required":
                logger.warning("API returned 'shortcode required', falling back to DOM")
                break

            results = data.get("results", [])
            if not results:
                break

            logger.info(f"Processing page {page_count + 1} with {len(results)} jobs")

            for job in results:
                published = job.get("published")
                if published:
                    try:
                        pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
                        if pub_date < cutoff_date:
                            logger.info(f"Cutoff date hit at {pub_date}")
                            return {
                                "success": True,
                                "total": len(all_jobs),
                                "jobs": all_jobs,
                                "api_endpoint": api_url,
                                "pages_processed": page_count + 1
                            }
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

        # fallback if no jobs
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
    """Test different API endpoints to see which ones work"""
    subdomain = extract_account_slug(url)
    
    endpoints_to_test = [
        f"https://apply.workable.com/api/v3/accounts/{subdomain}/jobs",
        f"https://apply.workable.com/api/v2/accounts/{subdomain}/jobs",
        f"https://apply.workable.com/api/v1/accounts/{subdomain}/jobs",
        f"https://apply.workable.com/api/v1/widget/accounts/{subdomain}",
        f"https://apply.workable.com/{subdomain}.json",
        f"https://apply.workable.com/{subdomain}/jobs.json"
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
                "success": resp.status_code == 200,
                "content_type": resp.headers.get("content-type", ""),
                "response_size": len(resp.text),
                "has_results": False,
                "results_count": 0,
                "has_next_page": False,
                "sample_data": {}
            }
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if isinstance(data, dict):
                        # Check for different data structures
                        if "results" in data:
                            result["has_results"] = True
                            result["results_count"] = len(data.get("results", []))
                            result["has_next_page"] = "nextPage" in data
                            if data.get("results"):
                                result["sample_data"] = {k: v for k, v in list(data.get("results", [{}]))[0].items() if k in ["id", "title", "shortcode", "published"]}
                        elif "jobs" in data:
                            result["has_results"] = True
                            result["results_count"] = len(data.get("jobs", []))
                            if data.get("jobs"):
                                result["sample_data"] = {k: v for k, v in list(data.get("jobs", [{}]))[0].items() if k in ["id", "title", "shortcode", "published"]}
                        else:
                            result["sample_data"] = {k: v for k, v in list(data.items())[:3]}
                except:
                    result["response_preview"] = resp.text[:200]
            
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
        "message": "Workable Job Scraper API - Complete JSON API Version", 
        "endpoints": {
            "/scrape-links": "Get job links using JSON API with pagination fallback to DOM",
            "/scrape-job": "Get details for a specific job",
            "/scrape-all-jobs": "Get all job details using JSON API with pagination",
            "/test-api-endpoints": "Test which API endpoints work for a given board"
        },
        "features": [
            "Primary: JSON API with nextPage pagination support",
            "Fallback: Enhanced DOM scraping with Show More handling", 
            "Date-based filtering (last 5 days)",
            "Cookie consent and modal handling",
            "Force click for stubborn Show More buttons"
        ]
    }