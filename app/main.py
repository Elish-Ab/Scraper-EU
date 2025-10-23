# app/main.py - Fixed Version with Correct Slug Handling
from fastapi import FastAPI, Query
from urllib.parse import urljoin, urlparse
from playwright.sync_api import sync_playwright
import requests
import datetime
from datetime import datetime, timedelta
import re
import logging
import json

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

# ---------------- NEW: robust URL parsing + hardened fetch ---------------- #

def _parse_workable_url(job_url: str):
    """
    Works for:
    - https://apply.workable.com/<account>/j/<shortcode>/
    - https://apply.workable.com/<account>/j/<shortcode>
    - /<account>/j/<shortcode>/
    - <account>/j/<shortcode>
    """
    u = urlparse(job_url)
    path = u.path if u.path else job_url  # allow bare path input
    parts = [p for p in path.strip("/").split("/") if p]

    # find the "j" marker defensively
    try:
        j_idx = len(parts) - 1 - parts[::-1].index("j")
        account = parts[j_idx - 1]
        shortcode = parts[j_idx + 1]
        return account, shortcode
    except Exception:
        # Last-resort heuristic for ".../j/<shortcode>"
        if len(parts) >= 3 and parts[-2] == "j":
            return parts[-3], parts[-1]
        raise ValueError(f"Cannot parse Workable URL: {job_url}")

def _browsery_headers(job_url: str):
    return {
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": job_url,
        "Origin": "https://apply.workable.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }

def fetch_job_details(job_url: str):
    """
    Fetch details of a single Workable job with enhanced DOM scraping.
    
    Order of attempts:
      1) v3 detail API with enhanced headers
      2) v2 detail API  
      3) v3 list API (scan for shortcode)
      4) Enhanced DOM scraping (extracts ALL visible content)
    """
    api_attempts = []
    
    try:
        account, shortcode = _parse_workable_url(job_url)
        v3_detail = f"https://apply.workable.com/api/v3/accounts/{account}/jobs/{shortcode}"
        v2_detail = f"https://apply.workable.com/api/v2/accounts/{account}/jobs/{shortcode}"
        v3_list   = f"https://apply.workable.com/api/v3/accounts/{account}/jobs"
        
        # Enhanced headers with more browser-like behavior
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": f"https://apply.workable.com/{account}/",
            "Origin": "https://apply.workable.com",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
        }

        # --- Attempt 1: v3 detail endpoint ---
        try:
            logger.info(f"Trying v3 detail API: {v3_detail}")
            resp = requests.get(v3_detail, headers=headers, timeout=15)
            api_attempts.append({"endpoint": "v3_detail", "status": resp.status_code})
            
            if resp.status_code == 200:
                d = resp.json()
                logger.info(f"✓ v3 detail API succeeded for {shortcode}")
                return {
                    "job": {
                        "jobId": d.get("shortcode") or shortcode,
                        "id": d.get("id"),
                        "url": job_url,
                        "status": "ok",
                        "account": account,
                        "title": d.get("title"),
                        "department": d.get("department"),
                        "published": d.get("published"),
                        "location": d.get("location"),
                        "locations": d.get("locations"),
                        "type": d.get("type"),
                        "workplace": d.get("workplace"),
                        "remote": d.get("remote"),
                        "approvalStatus": d.get("approvalStatus"),
                        "description_html": d.get("description"),
                        "requirements_html": d.get("requirements"),
                        "benefits_html": d.get("benefits"),
                        "language": d.get("language"),
                        "state": d.get("state"),
                        "isInternal": d.get("isInternal"),
                        "source": "v3_detail",
                    }
                }
            logger.warning(f"✗ v3 detail failed with {resp.status_code}")
        except Exception as e:
            logger.warning(f"✗ v3 detail error: {e}")
            api_attempts.append({"endpoint": "v3_detail", "error": str(e)})

        # --- Attempt 2: v2 detail endpoint ---
        try:
            logger.info(f"Trying v2 detail API: {v2_detail}")
            resp = requests.get(v2_detail, headers=headers, timeout=15)
            api_attempts.append({"endpoint": "v2_detail", "status": resp.status_code})
            
            if resp.status_code == 200:
                d = resp.json()
                logger.info(f"✓ v2 detail API succeeded for {shortcode}")
                return {
                    "job": {
                        "jobId": shortcode,
                        "url": job_url,
                        "status": "ok",
                        "account": account,
                        "title": d.get("title"),
                        "department": d.get("department"),
                        "published": d.get("published"),
                        "location": d.get("location"),
                        "locations": d.get("locations"),
                        "type": d.get("type"),
                        "workplace": d.get("workplace"),
                        "remote": d.get("remote"),
                        "description_html": d.get("description"),
                        "requirements_html": d.get("requirements"),
                        "benefits_html": d.get("benefits"),
                        "source": "v2_detail",
                    }
                }
            logger.warning(f"✗ v2 detail failed with {resp.status_code}")
        except Exception as e:
            logger.warning(f"✗ v2 detail error: {e}")
            api_attempts.append({"endpoint": "v2_detail", "error": str(e)})

        # --- Attempt 3: v3 listing scan ---
        try:
            logger.info(f"Trying v3 list scan: {v3_list}")
            params = {"limit": 50}
            seen_pages = set()
            max_scan_pages = 5
            scan_count = 0
            
            while scan_count < max_scan_pages:
                r = requests.get(v3_list, headers=headers, params=params, timeout=15)
                if r.status_code != 200:
                    logger.warning(f"✗ v3 list failed with {r.status_code}")
                    api_attempts.append({"endpoint": "v3_list", "status": r.status_code})
                    break
                    
                data = r.json()
                for job in data.get("results", []):
                    if job.get("shortcode") == shortcode:
                        logger.info(f"✓ Found job in v3 list (page {scan_count + 1})")
                        return {
                            "job": {
                                "jobId": shortcode,
                                "id": job.get("id"),
                                "url": job_url,
                                "status": "ok",
                                "account": account,
                                "title": job.get("title"),
                                "department": job.get("department"),
                                "published": job.get("published"),
                                "location": job.get("location"),
                                "locations": job.get("locations"),
                                "type": job.get("type"),
                                "workplace": job.get("workplace"),
                                "remote": job.get("remote"),
                                "language": job.get("language"),
                                "state": job.get("state"),
                                "isInternal": job.get("isInternal"),
                                "source": "v3_list_match",
                            }
                        }
                
                nextp = data.get("nextPage")
                if not nextp or nextp in seen_pages:
                    break
                seen_pages.add(nextp)
                params = {"limit": 50, "nextPage": nextp}
                scan_count += 1
                
            logger.warning(f"✗ Job not found in v3 list after {scan_count} pages")
        except Exception as e:
            logger.warning(f"✗ v3 list scan error: {e}")
            api_attempts.append({"endpoint": "v3_list", "error": str(e)})

        # --- Attempt 4: ENHANCED DOM SCRAPING ---
        logger.info(f"All APIs failed, using enhanced DOM scraping for {job_url}")
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page(extra_http_headers={"User-Agent": headers["User-Agent"]})
                page.goto(job_url, wait_until="networkidle", timeout=35000)
                page.wait_for_timeout(2000)

                job_data = {
                    "jobId": shortcode,
                    "url": job_url,
                    "status": "ok",
                    "account": account,
                    "source": "dom_enhanced",
                    "api_attempts": api_attempts,
                }

                # Extract title (multiple methods)
                title = None
                title_selectors = [
                    'h1[data-ui="job-title"]',
                    'h1',
                    '[data-ui="title"]',
                    '.job-title'
                ]
                for sel in title_selectors:
                    el = page.query_selector(sel)
                    if el:
                        title = el.inner_text().strip()
                        if title:
                            break
                
                if not title:
                    title = page.title()
                
                job_data["title"] = title

                # Extract location
                location_selectors = [
                    '[data-ui="job-location"]',
                    '[data-ui="location"]',
                    '.location',
                    'span:has-text("Remote")',
                    'span:has-text("Location")'
                ]
                for sel in location_selectors:
                    el = page.query_selector(sel)
                    if el:
                        loc = el.inner_text().strip()
                        if loc:
                            job_data["location"] = loc
                            break

                # Extract department
                dept_el = page.query_selector('[data-ui="job-department"]')
                if dept_el:
                    job_data["department"] = dept_el.inner_text().strip()

                # Extract job type (Full-time, Part-time, etc.)
                type_el = page.query_selector('[data-ui="job-type"]')
                if type_el:
                    job_data["type"] = type_el.inner_text().strip()

                # Extract workplace type (Remote, Hybrid, On-site)
                workplace_el = page.query_selector('[data-ui="job-workplace"]')
                if workplace_el:
                    job_data["workplace"] = workplace_el.inner_text().strip()

                # Extract description section
                desc_selectors = [
                    '[data-ui="job-description"]',
                    'section:has-text("Description")',
                    'div[class*="description"]',
                    '.job-description'
                ]
                for sel in desc_selectors:
                    el = page.query_selector(sel)
                    if el:
                        job_data["description_html"] = el.inner_html()
                        break

                # Extract requirements section  
                req_selectors = [
                    '[data-ui="job-requirements"]',
                    'section:has-text("Requirements")',
                    'div[class*="requirements"]'
                ]
                for sel in req_selectors:
                    el = page.query_selector(sel)
                    if el:
                        job_data["requirements_html"] = el.inner_html()
                        break

                # Extract benefits section
                benefits_selectors = [
                    '[data-ui="job-benefits"]',
                    'section:has-text("Benefits")',
                    'div[class*="benefits"]'
                ]
                for sel in benefits_selectors:
                    el = page.query_selector(sel)
                    if el:
                        job_data["benefits_html"] = el.inner_html()
                        break

                # Try JSON-LD for additional data
                script = page.query_selector('script[type="application/ld+json"]')
                if script:
                    try:
                        ld = json.loads(script.inner_text())
                        if isinstance(ld, dict):
                            if not job_data.get("title"):
                                job_data["title"] = ld.get("title") or ld.get("name")
                            
                            if "hiringOrganization" in ld:
                                org = ld["hiringOrganization"]
                                if isinstance(org, dict):
                                    job_data["company"] = org.get("name")
                            
                            if "datePosted" in ld:
                                job_data["published"] = ld.get("datePosted")
                            
                            if "employmentType" in ld:
                                job_data["employment_type"] = ld.get("employmentType")
                                
                            if "jobLocation" in ld and not job_data.get("location"):
                                jl = ld["jobLocation"]
                                if isinstance(jl, list) and jl:
                                    jl = jl[0]
                                if isinstance(jl, dict):
                                    addr = jl.get("address", {})
                                    if isinstance(addr, dict):
                                        job_data["location"] = addr.get("addressLocality")
                    except Exception as e:
                        logger.debug(f"JSON-LD parsing error: {e}")

                browser.close()
                
                logger.info(f"✓ DOM scraping extracted: {list(job_data.keys())}")
                return {"job": job_data}
                
        except Exception as e2:
            logger.error(f"✗ DOM fallback failed: {e2}")
            return {
                "job": {
                    "jobId": shortcode,
                    "url": job_url,
                    "status": "limited_info",
                    "account": account,
                    "error": f"All methods failed. DOM error: {str(e2)}",
                    "api_attempts": api_attempts,
                }
            }

    except Exception as e:
        logger.error(f"Critical error fetching job details: {e}")
        return {
            "job": {
                "jobId": job_url.split("/")[-1] if "/" in job_url else "unknown",
                "url": job_url,
                "status": "error",
                "error": str(e),
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
